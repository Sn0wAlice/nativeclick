use crate::Result;
use crate::{
    NativeclickError,
    block::Block,
    io::ClickhouseRead,
    progress::Progress,
    protocol::{self, *},
};
use indexmap::IndexMap;
use log::trace;
use protocol::ServerPacketId;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[cfg(feature = "compression")]
pub(crate) const MAX_COMPRESSION_SIZE: u32 = 0x40000000;

pub struct InternalClientIn<R: ClickhouseRead> {
    reader: R,
    /// Negotiated revision: 0 until the server Hello is read.
    pub revision: u64,
    /// Whether the queries of this connection ask for compressed data.
    pub compression: CompressionMethod,
}

impl<R: ClickhouseRead + 'static> InternalClientIn<R> {
    pub fn new(reader: R) -> Self {
        InternalClientIn {
            reader,
            revision: 0,
            compression: CompressionMethod::default(),
        }
    }

    /// A string from the server Hello, capped like the reference client does.
    async fn read_hello_string(&mut self) -> Result<String> {
        let len = self.reader.read_var_uint().await?;
        if len as usize > MAX_HELLO_STRING_SIZE {
            return Err(NativeclickError::ProtocolError(format!(
                "server hello string too long: {len} > {MAX_HELLO_STRING_SIZE}"
            )));
        }
        let mut buf = vec![0u8; len as usize];
        self.reader.read_exact(&mut buf).await?;
        Ok(String::from_utf8(buf)?)
    }

    /// Settings in the `STRINGS_WITH_FLAGS` format: (name, flags, value)*, then an empty name.
    async fn read_settings(&mut self) -> Result<Vec<(String, String)>> {
        let mut out = vec![];
        loop {
            let name = self.reader.read_utf8_string().await?;
            if name.is_empty() {
                return Ok(out);
            }
            let _flags = self.reader.read_var_uint().await?;
            let value = self.reader.read_utf8_string().await?;
            out.push((name, value));
        }
    }

    async fn read_hello(&mut self) -> Result<ServerHello> {
        let server_name = self.read_hello_string().await?;
        let major_version = self.reader.read_var_uint().await?;
        let minor_version = self.reader.read_var_uint().await?;
        let revision_version = self.reader.read_var_uint().await?;
        let revision = revision_version.min(DBMS_TCP_PROTOCOL_VERSION);
        if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
            let _parallel_replicas_version = self.reader.read_var_uint().await?;
        }
        let timezone = if revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            Some(self.read_hello_string().await?)
        } else {
            None
        };
        let display_name = if revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            Some(self.read_hello_string().await?)
        } else {
            None
        };
        let patch_version = if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            self.reader.read_var_uint().await?
        } else {
            revision_version
        };
        let (chunked_send, chunked_recv) =
            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
                (
                    Some(self.read_hello_string().await?),
                    Some(self.read_hello_string().await?),
                )
            } else {
                (None, None)
            };
        let mut password_complexity_rules = vec![];
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
            let count = self.reader.read_var_uint().await?;
            if count > MAX_PASSWORD_COMPLEXITY_RULES {
                return Err(NativeclickError::ProtocolError(format!(
                    "too many password complexity rules: {count}"
                )));
            }
            for _ in 0..count {
                let pattern = self.read_hello_string().await?;
                let message = self.read_hello_string().await?;
                password_complexity_rules.push((pattern, message));
            }
        }
        if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            let _nonce = self.reader.read_u64_le().await?;
        }
        let settings = if revision >= DBMS_MIN_REVISION_WITH_SERVER_SETTINGS {
            self.read_settings().await?
        } else {
            vec![]
        };
        if revision >= DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION {
            let _query_plan_version = self.reader.read_var_uint().await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL {
            let _cluster_function_version = self.reader.read_var_uint().await?;
        }
        self.revision = revision;
        Ok(ServerHello {
            server_name,
            major_version,
            minor_version,
            revision_version,
            timezone,
            display_name,
            patch_version,
            chunked_send,
            chunked_recv,
            password_complexity_rules,
            settings,
        })
    }

    async fn read_exception(&mut self) -> Result<ServerException> {
        let code = self.reader.read_i32_le().await?;
        let name = self.reader.read_utf8_string().await?;
        let message = self.reader.read_utf8_string().await?;
        let stack_trace = self.reader.read_utf8_string().await?;
        let mut has_nested = self.reader.read_u8().await? != 0;
        let mut message = message;
        // Old servers chain nested exceptions; keep their messages instead of desyncing.
        while has_nested {
            let _code = self.reader.read_i32_le().await?;
            let _name = self.reader.read_utf8_string().await?;
            let nested = self.reader.read_utf8_string().await?;
            let _stack_trace = self.reader.read_utf8_string().await?;
            has_nested = self.reader.read_u8().await? != 0;
            message.push_str("\nCaused by: ");
            message.push_str(&nested);
        }

        Ok(ServerException {
            code,
            name,
            message,
            stack_trace,
            has_nested: false,
        })
    }

    #[cfg(feature = "compression")]
    async fn decompress_data(&mut self) -> Result<Block> {
        let mut reader = crate::compression::DecompressionReader::new(&mut self.reader);

        let block = Block::read(&mut reader, self.revision).await?;

        Ok(block)
    }

    #[cfg(not(feature = "compression"))]
    async fn decompress_data(&mut self) -> Result<Block> {
        panic!(
            "attempted to use compression when not compiled with `compression` feature in nativeclick"
        );
    }

    async fn receive_data(&mut self, compression: CompressionMethod) -> Result<ServerData> {
        let table_name = self.reader.read_utf8_string().await?;

        let block = match compression {
            CompressionMethod::None => Block::read(&mut self.reader, self.revision).await?,
            _ => self.decompress_data().await?,
        };

        Ok(ServerData { table_name, block })
    }

    /// Log and ProfileEvents blocks: compressed like Data only since revision 54481.
    fn side_channel_compression(&self) -> CompressionMethod {
        if self.revision >= DBMS_MIN_REVISION_WITH_COMPRESSED_LOGS_PROFILE_EVENTS_COLUMNS {
            self.compression
        } else {
            CompressionMethod::None
        }
    }

    async fn receive_table_columns(&mut self) -> Result<TableColumns> {
        #[cfg(feature = "compression")]
        if !matches!(self.side_channel_compression(), CompressionMethod::None) {
            // Both strings are inside one compressed frame.
            let mut reader = crate::compression::DecompressionReader::new(&mut self.reader);
            let name = reader.read_utf8_string().await?;
            let description = reader.read_utf8_string().await?;
            return Ok(TableColumns { name, description });
        }
        let name = self.reader.read_utf8_string().await?;
        let description = self.reader.read_utf8_string().await?;
        Ok(TableColumns { name, description })
    }

    pub async fn receive_packet(&mut self) -> Result<ServerPacket> {
        let packet_id = ServerPacketId::from_u64(self.reader.read_var_uint().await?)?;
        let packet: Result<ServerPacket> = match packet_id {
            ServerPacketId::Hello => Ok(ServerPacket::Hello(self.read_hello().await?)),
            ServerPacketId::Data => Ok(ServerPacket::Data(
                self.receive_data(self.compression).await?,
            )),
            ServerPacketId::Exception => Ok(ServerPacket::Exception(self.read_exception().await?)),
            ServerPacketId::Progress => {
                let revision = self.revision;
                let read_rows = self.reader.read_var_uint().await?;
                let read_bytes = self.reader.read_var_uint().await?;
                let new_total_rows_to_read = self.reader.read_var_uint().await?;
                let new_total_bytes_to_read =
                    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
                        Some(self.reader.read_var_uint().await?)
                    } else {
                        None
                    };
                let (new_written_rows, new_written_bytes) =
                    if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                        (
                            Some(self.reader.read_var_uint().await?),
                            Some(self.reader.read_var_uint().await?),
                        )
                    } else {
                        (None, None)
                    };
                let elapsed_ns =
                    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
                        Some(self.reader.read_var_uint().await?)
                    } else {
                        None
                    };
                Ok(ServerPacket::Progress(Progress {
                    read_rows,
                    read_bytes,
                    new_total_rows_to_read,
                    new_written_rows,
                    new_written_bytes,
                    new_total_bytes_to_read,
                    elapsed_ns,
                }))
            }
            ServerPacketId::Pong => Ok(ServerPacket::Pong),
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                let rows = self.reader.read_var_uint().await?;
                let blocks = self.reader.read_var_uint().await?;
                let bytes = self.reader.read_var_uint().await?;
                let applied_limit = self.reader.read_u8().await? != 0;
                let rows_before_limit = self.reader.read_var_uint().await?;
                let calculated_rows_before_limit = self.reader.read_u8().await? != 0;
                let (applied_aggregation, rows_before_aggregation) =
                    if self.revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
                        (
                            self.reader.read_u8().await? != 0,
                            self.reader.read_var_uint().await?,
                        )
                    } else {
                        (false, 0)
                    };
                Ok(ServerPacket::ProfileInfo(BlockStreamProfileInfo {
                    rows,
                    blocks,
                    bytes,
                    applied_limit,
                    rows_before_limit,
                    calculated_rows_before_limit,
                    applied_aggregation,
                    rows_before_aggregation,
                }))
            }
            ServerPacketId::Totals => Ok(ServerPacket::Totals(
                self.receive_data(self.compression).await?,
            )),
            ServerPacketId::Extremes => Ok(ServerPacket::Extremes(
                self.receive_data(self.compression).await?,
            )),
            ServerPacketId::TablesStatusResponse => {
                let mut response = TablesStatusResponse {
                    database_tables: IndexMap::new(),
                };
                let size = self.reader.read_var_uint().await?;
                if size as usize > MAX_STRING_SIZE {
                    return Err(NativeclickError::ProtocolError(format!(
                        "table status response size too large. {size} > {MAX_STRING_SIZE}"
                    )));
                }
                for _ in 0..size {
                    let database_name = self.reader.read_utf8_string().await?;
                    let table_name = self.reader.read_utf8_string().await?;
                    let is_replicated = self.reader.read_u8().await? != 0;
                    let (absolute_delay, is_readonly) = if is_replicated {
                        let delay = self.reader.read_var_uint().await? as u32;
                        let readonly = self.revision
                            >= DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK
                            && self.reader.read_var_uint().await? != 0;
                        (delay, readonly)
                    } else {
                        (0, false)
                    };
                    response
                        .database_tables
                        .entry(database_name)
                        .or_default()
                        .insert(
                            table_name,
                            TableStatus {
                                is_replicated,
                                absolute_delay,
                                is_readonly,
                            },
                        );
                }
                Ok(ServerPacket::TablesStatusResponse(response))
            }
            ServerPacketId::Log => Ok(ServerPacket::Log(
                self.receive_data(self.side_channel_compression()).await?,
            )),
            ServerPacketId::ProfileEvents => Ok(ServerPacket::ProfileEvents(
                self.receive_data(self.side_channel_compression()).await?,
            )),
            ServerPacketId::TableColumns => Ok(ServerPacket::TableColumns(
                self.receive_table_columns().await?,
            )),
            ServerPacketId::TimezoneUpdate => Ok(ServerPacket::TimezoneUpdate(
                self.reader.read_utf8_string().await?,
            )),
            ServerPacketId::MergeTreeAllRangesAnnouncement
            | ServerPacketId::MergeTreeReadTaskRequest
            | ServerPacketId::SshChallenge => Err(NativeclickError::ProtocolError(format!(
                "unsupported packet from server: {packet_id:?}"
            ))),
            ServerPacketId::PartUUIDs => {
                let len = self.reader.read_var_uint().await?;
                if len as usize > MAX_STRING_SIZE {
                    return Err(NativeclickError::ProtocolError(format!(
                        "PartUUIDs response size too large. {len} > {MAX_STRING_SIZE}"
                    )));
                }
                let mut out = Vec::with_capacity(len as usize);
                let mut bytes = [0u8; 16];
                for _ in 0..len {
                    self.reader.read_exact(&mut bytes[..]).await?;

                    out.push(Uuid::from_bytes(bytes));
                }
                Ok(ServerPacket::PartUUIDs(out))
            }
            ServerPacketId::ReadTaskRequest => Ok(ServerPacket::ReadTaskRequest),
        };
        let packet = packet?;

        trace!("clickhouse packet received: {packet:?}");
        Ok(packet)
    }

    pub async fn receive_hello(&mut self) -> Result<ServerHello> {
        match self.receive_packet().await? {
            ServerPacket::Hello(hello) => Ok(hello),
            ServerPacket::Exception(e) => Err(e.emit()),
            packet => Err(NativeclickError::ProtocolError(format!(
                "unexpected packet {packet:?}, expected server hello"
            ))),
        }
    }
}
