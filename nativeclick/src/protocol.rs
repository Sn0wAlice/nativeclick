use indexmap::IndexMap;
use uuid::Uuid;

use crate::{NativeclickError, Result, block::Block, progress::Progress};

// Revisions of the native protocol (ClickHouse src/Core/ProtocolDefines.h). Every revision-gated
// field is read and written against the NEGOTIATED revision, min(client, server).
pub const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
pub const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
pub const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME: u64 = 54449;
pub const DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub const DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES: u64 = 54461;
pub const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2: u64 = 54462;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS: u64 = 54463;
pub const DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK: u64 = 54467;
pub const DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION: u64 = 54469;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS: u64 = 54470;
pub const DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL: u64 = 54471;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES: u64 = 54472;
pub const DBMS_MIN_REVISION_WITH_SERVER_SETTINGS: u64 = 54474;
pub const DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS: u64 = 54475;
pub const DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER: u64 = 54476;
pub const DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION: u64 = 54477;
pub const DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL: u64 = 54479;
pub const DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION: u64 = 54480;
pub const DBMS_MIN_REVISION_WITH_COMPRESSED_LOGS_PROFILE_EVENTS_COLUMNS: u64 = 54481;
pub const DBMS_MIN_REVISION_WITH_CLIENT_AGENT_IN_CLIENT_INFO: u64 = 54485;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERNAL_QUERY_FLAG: u64 = 54486;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_CURRENT_ROLES: u64 = 54488;
pub const DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION: u64 = 54492;

/// Revision announced by this client (ClickHouse master; a 26.9 server answers 54492).
///
/// Column formats this implies, all decoded: custom serialization kinds (54454), sparse columns
/// (54465, Nullable at 54483), replicated columns (54482), size-stream Strings (54492).
pub const DBMS_TCP_PROTOCOL_VERSION: u64 = 54493;

/// Parallel replicas protocol version sent in the addendum (irrelevant for a plain client).
pub const DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION: u64 = 8;

pub const MAX_STRING_SIZE: usize = 1 << 30;
/// Cap on strings and lists in the server Hello, as in the reference client.
pub const MAX_HELLO_STRING_SIZE: usize = 4096;
pub const MAX_PASSWORD_COMPLEXITY_RULES: u64 = 256;

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum ClientPacketId {
    Hello,
    Query,
    Data,
    Cancel,
    Ping,
    TablesStatusRequest,
    KeepAlive,
    Scalar,
    IgnoredPartUUIDs,
    ReadTaskResponse,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub enum ServerPacketId {
    Hello,
    Data,
    Exception,
    Progress,
    Pong,
    EndOfStream,
    ProfileInfo,
    Totals,
    Extremes,
    TablesStatusResponse,
    Log,
    TableColumns,
    PartUUIDs,
    ReadTaskRequest,
    ProfileEvents,
    MergeTreeAllRangesAnnouncement,
    MergeTreeReadTaskRequest,
    TimezoneUpdate,
    SshChallenge,
}

impl ServerPacketId {
    pub fn from_u64(i: u64) -> Result<Self> {
        Ok(match i {
            0 => ServerPacketId::Hello,
            1 => ServerPacketId::Data,
            2 => ServerPacketId::Exception,
            3 => ServerPacketId::Progress,
            4 => ServerPacketId::Pong,
            5 => ServerPacketId::EndOfStream,
            6 => ServerPacketId::ProfileInfo,
            7 => ServerPacketId::Totals,
            8 => ServerPacketId::Extremes,
            9 => ServerPacketId::TablesStatusResponse,
            10 => ServerPacketId::Log,
            11 => ServerPacketId::TableColumns,
            12 => ServerPacketId::PartUUIDs,
            13 => ServerPacketId::ReadTaskRequest,
            14 => ServerPacketId::ProfileEvents,
            15 => ServerPacketId::MergeTreeAllRangesAnnouncement,
            16 => ServerPacketId::MergeTreeReadTaskRequest,
            17 => ServerPacketId::TimezoneUpdate,
            18 => ServerPacketId::SshChallenge,
            x => {
                return Err(NativeclickError::ProtocolError(format!(
                    "invalid packet id from server: {x}"
                )));
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ServerHello {
    pub server_name: String,
    pub major_version: u64,
    pub minor_version: u64,
    /// The server's own protocol revision.
    pub revision_version: u64,
    pub timezone: Option<String>,
    pub display_name: Option<String>,
    pub patch_version: u64,
    /// `proto_send_chunked` / `proto_recv_chunked` capabilities of the server.
    pub chunked_send: Option<String>,
    pub chunked_recv: Option<String>,
    /// Password complexity rules: (pattern, message).
    pub password_complexity_rules: Vec<(String, String)>,
    /// Settings of the user's profile that differ from the defaults: (name, value).
    pub settings: Vec<(String, String)>,
}

impl ServerHello {
    /// Revision both sides speak: every gated field follows this, never the server's own number.
    pub fn negotiated_revision(&self) -> u64 {
        self.revision_version.min(DBMS_TCP_PROTOCOL_VERSION)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerData {
    pub table_name: String,
    pub block: Block,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerException {
    pub code: i32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
    pub has_nested: bool,
}

impl ServerException {
    pub fn emit(&self) -> NativeclickError {
        NativeclickError::ServerException {
            code: self.code,
            name: self.name.clone(),
            message: self.message.clone(),
            stack_trace: self.stack_trace.clone(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BlockStreamProfileInfo {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
    pub applied_aggregation: bool,
    pub rows_before_aggregation: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TableColumns {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TableStatus {
    pub is_replicated: bool,
    pub absolute_delay: u32,
    pub is_readonly: bool,
}

#[derive(Debug, Clone)]
pub struct TablesStatusResponse {
    pub database_tables: IndexMap<String, IndexMap<String, TableStatus>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ServerPacket {
    Hello(ServerHello),
    Data(ServerData),
    Exception(ServerException),
    Progress(Progress),
    Pong,
    EndOfStream,
    ProfileInfo(BlockStreamProfileInfo),
    Totals(ServerData),
    Extremes(ServerData),
    TablesStatusResponse(TablesStatusResponse),
    Log(ServerData),
    TableColumns(TableColumns),
    PartUUIDs(Vec<Uuid>),
    ReadTaskRequest,
    ProfileEvents(ServerData),
    TimezoneUpdate(String),
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(unused)]
pub enum CompressionMethod {
    #[cfg_attr(not(feature = "compression"), default)]
    None,
    #[cfg_attr(feature = "compression", default)]
    LZ4,
}

impl CompressionMethod {
    pub fn byte(&self) -> u8 {
        match self {
            CompressionMethod::None => 0x02,
            CompressionMethod::LZ4 => 0x82,
        }
    }
}
