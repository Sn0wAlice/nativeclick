use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{Stream, StreamExt, stream};
use indexmap::IndexMap;
use protocol::CompressionMethod;
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
    select,
    sync::{
        broadcast,
        mpsc::{self, Receiver},
        oneshot,
    },
};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::{
    NativeclickError, ParsedQuery, RawRow, Result, Type,
    block::{Block, BlockInfo},
    convert::Row,
    internal_client_in::InternalClientIn,
    internal_client_out::{
        CLIENT_NAME, ClientHello, ClientInfo, InternalClientOut, Query, QueryKind as OutQueryKind,
        QueryProcessingStage,
    },
    io::{ClickhouseRead, ClickhouseWrite},
    progress::Progress,
    protocol::{self, ServerPacket},
};
use log::*;

/// Settings sent with every query: `JSON` columns as text and `Dynamic` columns in the flattened
/// layout, the forms this client decodes.
const FORMAT_SETTINGS: [(&str, &str); 2] = [
    ("output_format_native_write_json_as_string", "1"),
    (
        "output_format_native_use_flattened_dynamic_and_json_serialization",
        "1",
    ),
];

// Maximum number of progress statuses to keep in memory. New statuses evict old ones.
const PROGRESS_CAPACITY: usize = 100;

struct InnerClient<R: ClickhouseRead, W: ClickhouseWrite> {
    input: Option<InternalClientIn<R>>,
    output: InternalClientOut<W>,
    options: ClientOptions,
    pending_queries: VecDeque<PendingQuery>,
    executing_query: Option<ExecutingQuery>,
    progress: broadcast::Sender<(Uuid, Progress)>,
    closed: ClosedReason,
    /// Reports the handshake result to [`Client::start`].
    ready: Option<oneshot::Sender<Result<ServerInfo>>>,
}

/// Why the connection task stopped, shared with every [`Client`] handle so later calls report
/// the real cause instead of a bare "channel closed".
type ClosedReason = Arc<Mutex<Option<NativeclickError>>>;

struct PendingQuery {
    query: String,
    kind: QueryKind,
    options: Arc<QueryOptions>,
    response: oneshot::Sender<(Uuid, mpsc::Receiver<Result<Block>>)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueryKind {
    Select,
    Insert,
    Ping,
}

struct ExecutingQuery {
    id: Uuid,
    sender: mpsc::Sender<Result<Block>>,
    kind: QueryKind,
    /// An INSERT whose header block has not arrived yet.
    awaiting_header: bool,
    /// An INSERT whose header block arrived and whose data is not terminated yet: the server
    /// reads `Data` packets until an empty one, even after it failed the query.
    awaiting_data: bool,
    /// The caller is gone or timed out: the rest of the answer is drained and discarded.
    cancelled: bool,
    deadline: Option<tokio::time::Instant>,
}

fn empty_block() -> Block {
    Block {
        info: BlockInfo::default(),
        rows: 0,
        column_types: IndexMap::new(),
        column_data: IndexMap::new(),
    }
}

impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> InnerClient<R, W> {
    pub fn new(reader: R, writer: W, options: ClientOptions, closed: ClosedReason) -> Self {
        Self {
            input: Some(InternalClientIn::new(reader)),
            output: InternalClientOut::new(writer),
            options,
            pending_queries: VecDeque::new(),
            executing_query: None,
            progress: broadcast::channel(PROGRESS_CAPACITY).0,
            closed,
            ready: None,
        }
    }

    async fn dispatch_query(&mut self, query: PendingQuery) -> Result<()> {
        let (sender, receiver) = mpsc::channel(32);
        let id = query.options.query_id.unwrap_or_else(Uuid::new_v4);
        let deadline = query
            .options
            .timeout
            .map(|timeout| tokio::time::Instant::now() + timeout);
        if query.kind == QueryKind::Ping {
            self.output.send_ping().await?;
            query.response.send((id, receiver)).ok();
            self.executing_query = Some(ExecutingQuery {
                id,
                sender,
                kind: QueryKind::Ping,
                awaiting_header: false,
                awaiting_data: false,
                cancelled: false,
                deadline,
            });
            return Ok(());
        }
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|x| x.as_micros() as i64)
            .unwrap_or_default();
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_default();
        let hostname = std::env::var("HOSTNAME").unwrap_or_default();
        // Formats this client reads; the caller's settings come after and can override them.
        let settings = FORMAT_SETTINGS
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .chain(query.options.settings.iter().cloned())
            .collect::<Vec<_>>();
        self.output
            .send_query(Query {
                id: &id.to_string(),
                info: ClientInfo {
                    kind: OutQueryKind::InitialQuery,
                    initial_user: "",
                    initial_query_id: "",
                    initial_address: "0.0.0.0:0",
                    initial_query_start_time: start_time,
                    os_user: &os_user,
                    client_hostname: &hostname,
                    client_name: CLIENT_NAME,
                    client_version_major: crate::VERSION_MAJOR,
                    client_version_minor: crate::VERSION_MINOR,
                    client_tcp_protocol_version: protocol::DBMS_TCP_PROTOCOL_VERSION,
                    quota_key: &self.options.quota_key,
                    distributed_depth: 0,
                    client_version_patch: 0,
                    open_telemetry: None,
                    client_agent: "",
                },
                settings: &settings,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::default(),
                query: &query.query,
                parameters: &query.options.parameters,
            })
            .await?;

        query.response.send((id, receiver)).ok();
        self.executing_query = Some(ExecutingQuery {
            id,
            sender,
            kind: query.kind,
            awaiting_header: query.kind == QueryKind::Insert,
            awaiting_data: false,
            cancelled: false,
            deadline,
        });
        // Terminates the (empty) list of external tables.
        self.output
            .send_data(empty_block(), CompressionMethod::default(), "", false)
            .await?;
        Ok(())
    }

    /// Stops listening to the executing query: the caller dropped it or it timed out. The
    /// server still answers until EndOfStream, which is drained before the next query.
    async fn cancel_executing(&mut self) -> Result<()> {
        let Some(current) = self.executing_query.as_mut() else {
            return Ok(());
        };
        if current.cancelled {
            return Ok(());
        }
        current.cancelled = true;
        match current.kind {
            // The server answers Cancel with EndOfStream.
            QueryKind::Select => self.output.send_cancel().await?,
            // A Cancel racing the server's own failure of an INSERT makes it drop the
            // connection; terminating the data is safe in every state.
            QueryKind::Insert => {
                if current.awaiting_data {
                    current.awaiting_data = false;
                    self.output
                        .send_data(empty_block(), CompressionMethod::default(), "", false)
                        .await?;
                }
            }
            QueryKind::Ping => {}
        }
        Ok(())
    }

    async fn on_timeout(&mut self) -> Result<()> {
        if let Some(current) = self.executing_query.as_ref() {
            current
                .sender
                .send(Err(NativeclickError::Timeout))
                .await
                .ok();
        }
        self.cancel_executing().await
    }

    async fn dispatch_next(&mut self) -> Result<()> {
        if let Some(query) = self.pending_queries.pop_front() {
            self.dispatch_query(query).await?;
        }
        Ok(())
    }

    async fn handle_request(&mut self, request: ClientRequest) -> Result<()> {
        match request.data {
            ClientRequestData::Query {
                query,
                kind,
                options,
                response,
            } => {
                let query = PendingQuery {
                    query,
                    kind,
                    options,
                    response,
                };
                if self.pending_queries.is_empty() && self.executing_query.is_none() {
                    self.dispatch_query(query).await?;
                } else {
                    self.pending_queries.push_back(query);
                }
            }
            ClientRequestData::SendData {
                query_id,
                block,
                response,
            } => {
                // Data belongs to one INSERT: never write it into another query, nor after the
                // server ended this one (it would read it as a stray packet).
                let Some(current) = self
                    .executing_query
                    .as_mut()
                    .filter(|x| x.id == query_id && x.awaiting_data && !x.cancelled)
                else {
                    response.send(Err(DataRejected::NotRunning)).ok();
                    return Ok(());
                };
                if block.rows == 0 && block.column_types.is_empty() {
                    current.awaiting_data = false;
                }
                self.output
                    .send_data(block, CompressionMethod::default(), "", false)
                    .await?;
                response.send(Ok(())).ok();
            }
        }
        Ok(())
    }

    async fn receive_packet(&mut self, packet: ServerPacket) -> Result<()> {
        match packet {
            ServerPacket::Hello(_) => {
                return Err(NativeclickError::ProtocolError(
                    "unexpected retransmission of server hello".to_string(),
                ));
            }
            ServerPacket::Data(block) => {
                let Some(current) = self.executing_query.as_mut() else {
                    return Err(NativeclickError::ProtocolError(
                        "received data block, but no pending queries".to_string(),
                    ));
                };
                // The first block of an INSERT is the table header: from now on the server
                // expects data until an empty block.
                if current.awaiting_header {
                    current.awaiting_header = false;
                    if current.cancelled {
                        self.output
                            .send_data(empty_block(), CompressionMethod::default(), "", false)
                            .await?;
                        return Ok(());
                    }
                    current.awaiting_data = true;
                }
                if !current.cancelled && current.sender.send(Ok(block.block)).await.is_err() {
                    // Dropped by the caller: no need to compute the rest.
                    self.cancel_executing().await?;
                }
            }
            ServerPacket::Exception(e) => {
                let Some(current) = self.executing_query.take() else {
                    return Err(e.emit());
                };
                if current.awaiting_data {
                    // The server skips the remaining data of a failed INSERT up to the empty
                    // block; send it now so it is ready for the next query.
                    self.output
                        .send_data(empty_block(), CompressionMethod::default(), "", false)
                        .await?;
                }
                current.sender.send(Err(e.emit())).await.ok();
                self.dispatch_next().await?;
            }
            ServerPacket::Progress(progress) => {
                if let Some(current) = &self.executing_query {
                    let _ = self.progress.send((current.id, progress));
                }
            }
            ServerPacket::Pong => {
                if self
                    .executing_query
                    .as_ref()
                    .is_some_and(|x| x.kind == QueryKind::Ping)
                {
                    self.executing_query = None;
                    self.dispatch_next().await?;
                }
            }
            ServerPacket::EndOfStream => {
                if self.executing_query.take().is_none() {
                    return Err(NativeclickError::ProtocolError(
                        "received end of stream, but no executing query".to_string(),
                    ));
                }
                self.dispatch_next().await?;
            }
            ServerPacket::ProfileInfo(_) => {}
            ServerPacket::Totals(_) => {}
            ServerPacket::Extremes(_) => {}
            ServerPacket::TablesStatusResponse(_) => {}
            ServerPacket::Log(_) => {}
            ServerPacket::TableColumns(_) => {}
            ServerPacket::PartUUIDs(_) => {}
            ServerPacket::ReadTaskRequest => {}
            ServerPacket::ProfileEvents(_) => {}
            ServerPacket::TimezoneUpdate(_) => {}
        }
        Ok(())
    }

    async fn run_inner(&mut self, input: &mut Receiver<ClientRequest>) -> Result<()> {
        let mut reader = self.input.take().expect("connection task started twice");
        self.output
            .send_hello(ClientHello {
                default_database: &self.options.default_database,
                username: &self.options.username,
                password: &self.options.password,
            })
            .await?;
        let hello = match reader.receive_hello().await {
            Ok(hello) => hello,
            Err(e) => {
                if let Some(ready) = self.ready.take() {
                    ready.send(Err(e.clone())).ok();
                }
                return Err(e);
            }
        };
        self.output.revision = reader.revision;
        self.output.send_addendum(&self.options.quota_key).await?;
        if let Some(ready) = self.ready.take() {
            ready.send(Ok(ServerInfo::from(hello))).ok();
        }

        // Packets are read in their own task: reading one takes many awaits, and dropping that
        // future halfway (as a `select!` against new requests would) loses the bytes already read.
        let (packet_sender, mut packets) = mpsc::channel(4);
        let reader_task = AbortOnDrop(tokio::spawn(async move {
            loop {
                let packet = reader.receive_packet().await;
                let failed = packet.is_err();
                if packet_sender.send(packet).await.is_err() || failed {
                    break;
                }
            }
        }));

        let result = loop {
            let deadline = self
                .executing_query
                .as_ref()
                .filter(|x| !x.cancelled)
                .and_then(|x| x.deadline);
            select! {
                _ = sleep_until_opt(deadline) => self.on_timeout().await?,
                request = input.recv() => match request {
                    Some(request) => self.handle_request(request).await?,
                    None => break Ok(()),
                },
                packet = packets.recv() => match packet {
                    Some(packet) => self.receive_packet(packet?).await?,
                    None => break Err(NativeclickError::ProtocolError(
                        "connection reader stopped".to_string(),
                    )),
                },
            }
        };
        drop(reader_task);
        result
    }

    /// Hands `error` to every caller still waiting on this connection.
    async fn fail(&mut self, error: NativeclickError, input: &mut Receiver<ClientRequest>) {
        *self.closed.lock().unwrap() = Some(error.clone());
        input.close();
        if let Some(current) = self.executing_query.take() {
            current.sender.send(Err(error.clone())).await.ok();
        }
        let failed_query = |response: oneshot::Sender<(Uuid, mpsc::Receiver<Result<Block>>)>| {
            let (sender, receiver) = mpsc::channel(1);
            sender.try_send(Err(error.clone())).ok();
            response.send((Uuid::nil(), receiver)).ok();
        };
        for query in self.pending_queries.drain(..) {
            failed_query(query.response);
        }
        while let Ok(request) = input.try_recv() {
            match request.data {
                ClientRequestData::Query { response, .. } => failed_query(response),
                ClientRequestData::SendData { response, .. } => {
                    response.send(Err(DataRejected::Error(error.clone()))).ok();
                }
            }
        }
    }

    pub async fn run(mut self, mut input: Receiver<ClientRequest>) {
        if let Err(e) = self.run_inner(&mut input).await {
            error!("clickhouse client failed: {:?}", e);
            self.fail(e, &mut input).await;
        }
    }
}

async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Aborts the spawned task when dropped, so the reader never outlives its connection.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum ClientRequestData {
    Query {
        query: String,
        kind: QueryKind,
        options: Arc<QueryOptions>,
        response: oneshot::Sender<(Uuid, mpsc::Receiver<Result<Block>>)>,
    },
    SendData {
        query_id: Uuid,
        block: Block,
        response: oneshot::Sender<std::result::Result<(), DataRejected>>,
    },
}

/// Why the connection task did not send a block of INSERT data.
enum DataRejected {
    /// The query ended (the server failed it) before this block: its error is in the query's
    /// response stream.
    NotRunning,
    Error(NativeclickError),
}

struct ClientRequest {
    data: ClientRequestData,
}

/// Client handle for a Clickhouse connection, has internal reference to connection, and can be freely cloned and sent across threads.
#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<ClientRequest>,
    progress: broadcast::Sender<(Uuid, Progress)>,
    closed: ClosedReason,
    options: Arc<QueryOptions>,
    server: Arc<ServerInfo>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("server", &self.server)
            .field("options", &self.options)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// What the server told about itself when connecting.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServerInfo {
    /// Usually `ClickHouse`.
    pub name: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    /// The server's own protocol revision.
    pub revision: u64,
    /// Protocol revision used on this connection: the lower of the server's and this client's.
    pub negotiated_revision: u64,
    pub timezone: Option<String>,
    pub display_name: Option<String>,
    /// Settings of the user's profile that differ from the server defaults.
    pub settings: Vec<(String, String)>,
}

impl From<protocol::ServerHello> for ServerInfo {
    fn from(hello: protocol::ServerHello) -> Self {
        Self {
            negotiated_revision: hello.negotiated_revision(),
            name: hello.server_name,
            version_major: hello.major_version,
            version_minor: hello.minor_version,
            version_patch: hello.patch_version,
            revision: hello.revision_version,
            timezone: hello.timezone,
            display_name: hello.display_name,
            settings: hello.settings,
        }
    }
}

/// Options set for a Clickhouse connection.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub username: String,
    pub password: String,
    pub default_database: String,
    pub tcp_nodelay: bool,
    /// Quota key of this connection (`quota_key` in ClickHouse quotas).
    pub quota_key: String,
    /// Limit for opening the connection: TCP connect, TLS and handshake. None waits forever.
    pub connect_timeout: Option<Duration>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            username: "default".to_string(),
            password: String::new(),
            default_database: String::new(),
            tcp_nodelay: true,
            quota_key: String::new(),
            connect_timeout: None,
        }
    }
}

/// Options applied to queries, see [`Client::with_options`].
///
/// ```no_run
/// # async fn f(client: nativeclick::Client) -> nativeclick::Result<()> {
/// use std::time::Duration;
/// use nativeclick::QueryOptions;
///
/// let client = client.with_options(
///     QueryOptions::new()
///         .setting("max_threads", 2)
///         .param("min_id", 10)
///         .timeout(Duration::from_secs(30)),
/// );
/// client.execute("SELECT * FROM t WHERE id >= {min_id:UInt64}").await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryOptions {
    /// Query id sent to the server (`system.query_log`, `KILL QUERY`, progress events).
    /// A random one is generated for each query when unset.
    pub query_id: Option<Uuid>,
    /// Settings for each query: (name, value as text, as in `SETTINGS name = value`).
    pub settings: Vec<(String, String)>,
    /// Values of server-side `{name:Type}` query parameters: (name, raw text of the value).
    pub parameters: Vec<(String, String)>,
    /// Limit for each query, until its last block. On expiry the query fails with
    /// [`NativeclickError::Timeout`] and is cancelled on the server.
    pub timeout: Option<Duration>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the query id. With several queries per handle, prefer one handle per query.
    pub fn query_id(mut self, id: Uuid) -> Self {
        self.query_id = Some(id);
        self
    }

    /// Adds a setting, e.g. `setting("max_execution_time", 10)`.
    pub fn setting(mut self, name: impl Into<String>, value: impl ToString) -> Self {
        self.settings.push((name.into(), value.to_string()));
        self
    }

    /// Adds the value of a `{name:Type}` parameter as text, exactly like
    /// `clickhouse-client --param_name=...`: the server parses it in the escaped (TSV) text format
    /// of the parameter's type, e.g. `param("n", 42)`, `param("ids", "[1,2]")`,
    /// `param("names", "['a','b']")`.
    ///
    /// For a `String` parameter holding arbitrary text (user input, backslashes, newlines), use
    /// [`QueryOptions::param_string`], which escapes it.
    pub fn param(mut self, name: impl Into<String>, value: impl ToString) -> Self {
        self.parameters.push((name.into(), value.to_string()));
        self
    }

    /// Adds the value of a `{name:String}` parameter (or `FixedString`, `Enum`...): any text,
    /// escaped so the server reads it back unchanged.
    pub fn param_string(mut self, name: impl Into<String>, value: &str) -> Self {
        let mut escaped = String::with_capacity(value.len());
        crate::internal_client_out::escape_into(&mut escaped, value, false);
        self.parameters.push((name.into(), escaped));
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Client {
    /// Consumes a reader and writer to connect to Nativeclick. To be used for exotic setups or TLS. Generally prefer [`Client::connect()`]
    pub async fn connect_stream(
        read: impl AsyncRead + Unpin + Send + Sync + 'static,
        writer: impl AsyncWrite + Unpin + Send + Sync + 'static,
        options: ClientOptions,
    ) -> Result<Self> {
        Self::start(InnerClient::new(
            BufReader::new(read),
            BufWriter::new(writer),
            options,
            ClosedReason::default(),
        ))
        .await
    }

    /// Connects to a specific socket address over plaintext TCP for Clickhouse.
    pub async fn connect<A: ToSocketAddrs>(destination: A, options: ClientOptions) -> Result<Self> {
        let timeout = options.connect_timeout;
        with_timeout(timeout, async move {
            let stream = TcpStream::connect(destination).await?;
            stream.set_nodelay(options.tcp_nodelay)?;
            let (read, writer) = stream.into_split();
            Self::connect_stream(read, writer, options).await
        })
        .await
    }

    /// Connects to a specific socket address over TLS (rustls) for Clickhouse.
    #[cfg(feature = "tls")]
    pub async fn connect_tls<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: rustls_pki_types::ServerName<'static>,
        connector: &tokio_rustls::TlsConnector,
    ) -> Result<Self> {
        let timeout = options.connect_timeout;
        with_timeout(timeout, async move {
            let stream = TcpStream::connect(destination).await?;
            stream.set_nodelay(options.tcp_nodelay)?;
            let tls_stream = connector.connect(name, stream).await?;
            let (read, writer) = tokio::io::split(tls_stream);
            Self::connect_stream(read, writer, options).await
        })
        .await
    }

    async fn start<R: ClickhouseRead + 'static, W: ClickhouseWrite>(
        mut inner: InnerClient<R, W>,
    ) -> Result<Self> {
        let progress = inner.progress.clone();
        let closed = inner.closed.clone();
        let (sender, receiver) = mpsc::channel(1024);
        let (ready, server) = oneshot::channel();
        inner.ready = Some(ready);

        tokio::spawn(inner.run(receiver));
        let server = match server.await {
            Ok(server) => server?,
            Err(_) => {
                return Err(closed.lock().unwrap().clone().unwrap_or_else(|| {
                    NativeclickError::ProtocolError("connection closed during handshake".into())
                }));
            }
        };
        let client = Client {
            sender,
            progress,
            closed,
            options: Arc::default(),
            server: Arc::new(server),
        };
        client
            .execute("SET date_time_input_format='best_effort'")
            .await?;
        Ok(client)
    }

    /// The error that stopped the connection, or a generic one if it is still being reported.
    fn closed_error(&self) -> NativeclickError {
        self.closed
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| NativeclickError::ProtocolError("connection closed".to_string()))
    }

    /// A handle on the same connection whose queries use `options` (settings, parameters,
    /// query id, timeout). The original handle is unchanged.
    pub fn with_options(&self, options: QueryOptions) -> Client {
        Client {
            options: Arc::new(options),
            ..self.clone()
        }
    }

    /// The options used by this handle's queries.
    pub fn options(&self) -> &QueryOptions {
        &self.options
    }

    /// What the server reported when connecting: version, timezone, profile settings...
    pub fn server_info(&self) -> &ServerInfo {
        &self.server
    }

    /// Round trip to the server, after the queries already queued on this connection.
    pub async fn ping(&self) -> Result<()> {
        let (_, mut receiver) = self.start_query(String::new(), QueryKind::Ping).await?;
        match drain_error(&mut receiver).await {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn start_query(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
        kind: QueryKind,
    ) -> Result<(Uuid, mpsc::Receiver<Result<Block>>)> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    query: query.try_into()?.0.trim().to_string(),
                    kind,
                    options: self.options.clone(),
                    response: sender,
                },
            })
            .await
            .map_err(|_| self.closed_error())?;
        receiver.await.map_err(|_| self.closed_error())
    }

    /// Sends a query string and read column blocks over a stream.
    /// You probably want [`Client::query()`]
    pub async fn query_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<ReceiverStream<Result<Block>>> {
        let (_, receiver) = self.start_query(query, QueryKind::Select).await?;
        Ok(ReceiverStream::new(receiver))
    }

    async fn send_data(
        &self,
        query_id: Uuid,
        block: Block,
    ) -> std::result::Result<(), DataRejected> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::SendData {
                    query_id,
                    block,
                    response: sender,
                },
            })
            .await
            .map_err(|_| DataRejected::Error(self.closed_error()))?;
        receiver
            .await
            .map_err(|_| DataRejected::Error(self.closed_error()))?
    }

    /// Sends the blocks of an INSERT, then its terminating empty block.
    ///
    /// When the server stops the INSERT early, its exception is returned rather than a
    /// "query no longer running" from the next block.
    async fn send_insert_data(
        &self,
        query_id: Uuid,
        receiver: &mut mpsc::Receiver<Result<Block>>,
        mut blocks: impl Stream<Item = Result<Block>> + Unpin,
    ) -> Result<()> {
        let mut failure = None;
        let mut not_running = false;
        while let Some(block) = blocks.next().await {
            let sent = match block {
                Ok(block) => self.send_data(query_id, block).await,
                Err(e) => Err(DataRejected::Error(e)),
            };
            match sent {
                Ok(()) => continue,
                Err(DataRejected::NotRunning) => not_running = true,
                Err(DataRejected::Error(e)) => failure = Some(e),
            }
            break;
        }
        // Always terminate the data, even after a client-side error: the server would wait
        // for more of it otherwise.
        if !not_running {
            match self.send_data(query_id, empty_block()).await {
                Ok(()) => {}
                Err(DataRejected::NotRunning) => not_running = true,
                Err(DataRejected::Error(e)) => {
                    failure.get_or_insert(e);
                }
            }
        }
        if let Some(e) = failure {
            drain_error(receiver).await;
            return Err(e);
        }
        if not_running {
            return Err(drain_error(receiver).await.unwrap_or_else(|| {
                NativeclickError::ProtocolError(
                    "the query is no longer running on the server".to_string(),
                )
            }));
        }
        Ok(())
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read.
    /// You probably want [`Client::insert_native`].
    pub async fn insert_native_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<impl Stream<Item = Result<Block>>> {
        let (id, mut receiver) = self.start_query(query, QueryKind::Insert).await?;
        let header = receiver.recv().await.ok_or_else(|| self.closed_error())??;
        self.send_insert_data(id, &mut receiver, blocks.map(Ok))
            .await?;
        Ok(stream::iter([Ok(header)]).chain(ReceiverStream::new(receiver)))
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), waits for Clickhouse to
    /// confirm the insert and returns its error, if any. Response blocks are DISCARDED.
    ///
    /// A row that fails to serialize stops the insert with an error. Blocks sent before it
    /// (earlier items of `blocks`) may already be inserted, as with any streamed insert.
    /// Make sure any query you send native data with has a `format native` suffix.
    pub async fn insert_native<T: Row + Send + Sync + 'static>(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
        blocks: impl Stream<Item = Vec<T>> + Send + Sync + Unpin + 'static,
    ) -> Result<()> {
        let (id, mut receiver) = self.start_query(query, QueryKind::Insert).await?;
        let header = receiver.recv().await.ok_or_else(|| self.closed_error())??;
        let blocks = blocks
            .filter(|rows| std::future::ready(!rows.is_empty()))
            .map(|rows| rows_to_block(rows, &header.column_types));
        self.send_insert_data(id, &mut receiver, blocks).await?;
        match drain_error(&mut receiver).await {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Wrapper over [`Client::insert_native`] to send a single block.
    /// Make sure any query you send native data with has a `format native` suffix.
    pub async fn insert_native_block<T: Row + Send + Sync + 'static>(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
        blocks: Vec<T>,
    ) -> Result<()> {
        let blocks = Box::pin(async move { blocks });
        let stream = futures_util::stream::once(blocks);
        self.insert_native(query, stream).await
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends more than one block).
    pub async fn query<T: Row, I: TryInto<ParsedQuery, Error = NativeclickError>>(
        &self,
        query: I,
    ) -> Result<impl Stream<Item = Result<T>> + use<T, I>> {
        let raw = self.query_raw(query).await?;
        Ok(raw.flat_map(|block| match block {
            Ok(mut block) => stream::iter(
                block
                    .take_iter_rows()
                    .filter(|x| !x.is_empty())
                    .map(|m| T::deserialize_row(m))
                    .collect::<Vec<_>>(),
            ),
            Err(e) => stream::iter(vec![Err(e)]),
        }))
    }

    /// Same as `query`, but collects all rows into a `Vec`
    pub async fn query_collect<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<Vec<T>> {
        let mut out = vec![];
        let mut stream = self.query::<T, _>(query).await?;
        while let Some(next) = stream.next().await {
            out.push(next?);
        }
        Ok(out)
    }

    /// Same as `query`, but returns the first row and discards the rest.
    pub async fn query_one<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<T> {
        self.query(query)
            .await?
            .next()
            .await
            .unwrap_or_else(|| Err(NativeclickError::MissingRow))
    }

    /// Same as `query`, but returns the first row, if any, and discards the rest.
    pub async fn query_opt<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<Option<T>> {
        self.query(query).await?.next().await.transpose()
    }

    /// Same as `query`, but discards all returns blocks. Waits until the first block returns from the server to check for errors.
    /// Waiting for the first response block or EOS also prevents the server from aborting the query potentially due to client disconnection.
    pub async fn execute(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<()> {
        let mut stream = self.query::<RawRow, _>(query).await?;
        while let Some(next) = stream.next().await {
            next?;
        }
        Ok(())
    }

    /// Same as `execute`, but doesn't wait for a server response. The query could get aborted if the connection is closed quickly.
    pub async fn execute_now(
        &self,
        query: impl TryInto<ParsedQuery, Error = NativeclickError>,
    ) -> Result<()> {
        let _ = self.query::<RawRow, _>(query).await?;
        Ok(())
    }

    /// true if the Client is closed. The cause is returned by every later call.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Receive progress on the queries as they execute.
    ///
    /// TODO: There is currently no way to retrieve the ID of a query launched
    ///       with `query` or `execute.`
    ///       The signature of these functions should be modified to also return
    ///       an ID (and possibly directly the streaming broadcast).
    pub fn subscribe_progress(&self) -> broadcast::Receiver<(Uuid, Progress)> {
        self.progress.subscribe()
    }
}

/// Runs `future`, failing with [`NativeclickError::Timeout`] after `timeout` if set.
async fn with_timeout<T>(
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NativeclickError::Timeout)?,
        None => future.await,
    }
}

/// Waits for the end of a query and returns the first error it reported.
async fn drain_error(receiver: &mut mpsc::Receiver<Result<Block>>) -> Option<NativeclickError> {
    let mut error = None;
    while let Some(block) = receiver.recv().await {
        if let Err(e) = block {
            error.get_or_insert(e);
        }
    }
    error
}

/// Serializes `rows` into one block for columns `column_types`, failing on the first bad row.
fn rows_to_block<T: Row>(rows: Vec<T>, column_types: &IndexMap<String, Type>) -> Result<Block> {
    let mut block = Block {
        info: BlockInfo::default(),
        rows: rows.len() as u64,
        column_types: column_types.clone(),
        column_data: IndexMap::new(),
    };
    for row in rows {
        for (key, value) in row.serialize_row(column_types)? {
            let type_ = column_types.get(&*key).ok_or_else(|| {
                NativeclickError::ProtocolError(format!("missing type for data, column: {key}"))
            })?;
            type_.validate_value(&value)?;
            if let Some(column) = block.column_data.get_mut(&*key) {
                column.push(value);
            } else {
                block.column_data.insert(key.into_owned(), vec![value]);
            }
        }
    }
    Ok(block)
}
