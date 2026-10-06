use crate::{
    Result,
    block::Block,
    io::ClickhouseWrite,
    protocol::{self, *},
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Client name sent in the Hello and in every query's client info: servers with
/// `validate_tcp_client_information` require both to match.
pub const CLIENT_NAME: &str = "ClickHouse nativeclick";

pub struct InternalClientOut<W: ClickhouseWrite> {
    writer: W,
    /// Negotiated revision: 0 until the server Hello is read.
    pub revision: u64,
}

pub struct ClientHello<'a> {
    pub default_database: &'a str,
    pub username: &'a str,
    pub password: &'a str,
}

#[repr(u8)]
#[derive(PartialEq, Clone, Copy)]
#[allow(unused, clippy::enum_variant_names)]
pub enum QueryKind {
    NoQuery,
    InitialQuery,
    SecondaryQuery,
}

pub struct ClientInfo<'a> {
    pub kind: QueryKind,
    pub initial_user: &'a str,
    pub initial_query_id: &'a str,
    pub initial_address: &'a str,
    /// Microseconds since the Unix epoch.
    pub initial_query_start_time: i64,
    // interface = TCP = 1
    pub os_user: &'a str,
    pub client_hostname: &'a str,
    pub client_name: &'a str,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_tcp_protocol_version: u64,
    pub quota_key: &'a str,
    pub distributed_depth: u64,
    pub client_version_patch: u64,
    pub open_telemetry: Option<OpenTelemetry<'a>>,
    pub client_agent: &'a str,
}

impl ClientInfo<'_> {
    pub async fn write<W: ClickhouseWrite>(&self, to: &mut W, revision: u64) -> Result<()> {
        to.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(());
        }
        to.write_string(self.initial_user).await?;
        to.write_string(self.initial_query_id).await?;
        to.write_string(self.initial_address).await?;
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
            to.write_i64_le(self.initial_query_start_time).await?;
        }
        to.write_u8(1).await?;
        to.write_string(self.os_user).await?;
        to.write_string(self.client_hostname).await?;
        to.write_string(self.client_name).await?;
        to.write_var_uint(self.client_version_major).await?;
        to.write_var_uint(self.client_version_minor).await?;
        to.write_var_uint(self.client_tcp_protocol_version).await?;
        if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            to.write_string(self.quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            to.write_var_uint(self.distributed_depth).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            to.write_var_uint(self.client_version_patch).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            if let Some(telemetry) = &self.open_telemetry {
                to.write_u8(1u8).await?;
                let (high, low) = telemetry.trace_id.as_u64_pair();
                to.write_u64_le(high).await?;
                to.write_u64_le(low).await?;
                to.write_u64_le(telemetry.span_id).await?;
                to.write_string(telemetry.tracestate).await?;
                to.write_u8(telemetry.trace_flags).await?;
            } else {
                to.write_u8(0u8).await?;
            }
        }
        if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
            to.write_var_uint(0).await?; // collaborate_with_initiator
            to.write_var_uint(0).await?; // count_participating_replicas
            to.write_var_uint(0).await?; // number_of_current_replica
        }
        if revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
            to.write_var_uint(0).await?; // script_query_number
            to.write_var_uint(0).await?; // script_line_number
        }
        if revision >= DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER {
            to.write_u8(0).await?; // no JWT
        }
        if revision >= DBMS_MIN_REVISION_WITH_CLIENT_AGENT_IN_CLIENT_INFO {
            to.write_string(self.client_agent).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERNAL_QUERY_FLAG {
            to.write_u8(0).await?; // is_internal
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_CURRENT_ROLES {
            to.write_u8(0).await?; // no current roles
        }

        Ok(())
    }
}

#[allow(dead_code)]
pub struct OpenTelemetry<'a> {
    trace_id: Uuid,
    span_id: u64,
    tracestate: &'a str,
    trace_flags: u8,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum QueryProcessingStage {
    FetchColumns,
    WithMergeableState,
    Complete,
    WithMergableStateAfterAggregation,
}

pub struct Query<'a> {
    pub id: &'a str,
    pub info: ClientInfo<'a>,
    /// Builtin settings: (name, value as text).
    pub settings: &'a [(String, String)],
    pub stage: QueryProcessingStage,
    pub compression: CompressionMethod,
    pub query: &'a str,
    /// Server-side `{name:Type}` parameters: (name, raw value text).
    pub parameters: &'a [(String, String)],
}

/// `SettingsWriteFormat::STRINGS_WITH_FLAGS` flags.
const SETTING_FLAG_CUSTOM: u64 = 0x02;

/// Appends `value` with ClickHouse backslash escapes; `quote` also escapes `'`
/// (`writeAnyQuotedString`), otherwise it is the TSV escaping (`writeEscapedString`).
pub(crate) fn escape_into(out: &mut String, value: &str, quote: bool) {
    for c in value.chars() {
        match c {
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            '\\' => out.push_str("\\\\"),
            '\'' if quote => out.push_str("\\'"),
            c => out.push(c),
        }
    }
}

/// Encodes the text of a query parameter for the wire, as `Field::dump()` of a String:
/// single-quoted with backslash escapes. The server unquotes it, then parses the text in the
/// escaped format of the parameter's type.
pub fn quote_parameter(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    escape_into(&mut out, value, true);
    out.push('\'');
    out
}

impl<W: ClickhouseWrite> InternalClientOut<W> {
    pub fn new(writer: W) -> Self {
        InternalClientOut {
            writer,
            revision: 0,
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_query<'a>(&mut self, params: Query<'a>) -> Result<()> {
        let revision = self.revision;
        if !params.parameters.is_empty() && revision < DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
            return Err(crate::NativeclickError::Unsupported(
                "query parameters need a server with protocol revision 54459 or later".to_string(),
            ));
        }
        self.writer
            .write_var_uint(protocol::ClientPacketId::Query as u64)
            .await?;
        self.writer.write_string(params.id).await?;
        if revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            params.info.write(&mut self.writer, revision).await?;
        }
        for (name, value) in params.settings {
            self.writer.write_string(name).await?;
            // Flags 0: an unknown setting is ignored with a warning instead of failing.
            self.writer.write_var_uint(0).await?;
            self.writer.write_string(value).await?;
        }
        self.writer.write_string("").await?;
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
            self.writer.write_string("").await?; // external roles
        }
        if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            self.writer.write_string("").await?; // interserver secret hash
        }
        self.writer.write_var_uint(params.stage as u64).await?;
        self.writer
            .write_u8(if matches!(params.compression, CompressionMethod::None) {
                0
            } else {
                1
            })
            .await?;
        self.writer.write_string(params.query).await?;
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
            for (name, value) in params.parameters {
                self.writer.write_string(name).await?;
                self.writer.write_var_uint(SETTING_FLAG_CUSTOM).await?;
                self.writer.write_string(&quote_parameter(value)).await?;
            }
            self.writer.write_string("").await?;
        }

        self.writer.flush().await?;
        Ok(())
    }

    /// Asks the server to stop the running query. Ignored by the server when idle; the query
    /// then ends with EndOfStream.
    pub async fn send_cancel(&mut self) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Cancel as u64)
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn send_ping(&mut self) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Ping as u64)
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Sent right after the server Hello since revision 54458. This client never asks for
    /// chunked packets, which every server accepts unless explicitly configured otherwise.
    pub async fn send_addendum(&mut self, quota_key: &str) -> Result<()> {
        let revision = self.revision;
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
            self.writer.write_string(quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
            self.writer.write_string("notchunked").await?;
            self.writer.write_string("notchunked").await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
            self.writer
                .write_var_uint(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
                .await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    #[cfg(feature = "compression")]
    async fn compress_data(&mut self, byte: u8, block: Block) -> Result<()> {
        let (out, decompressed_size) =
            crate::compression::compress_block(block, self.revision).await?;
        let mut new_out = Vec::with_capacity(out.len() + 5);
        new_out.push(byte);
        new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
        new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
        new_out.extend(out);

        let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
        self.writer.write_u64_le((hash >> 64) as u64).await?;
        self.writer.write_u64_le(hash as u64).await?;
        self.writer.write_all(&new_out[..]).await?;
        Ok(())
    }

    #[cfg(not(feature = "compression"))]
    async fn compress_data(&mut self, _byte: u8, _block: Block) -> Result<()> {
        panic!(
            "attempted to use compression when not compiled with `compression` feature in nativeclick"
        );
    }

    pub async fn send_data(
        &mut self,
        block: Block,
        compression: CompressionMethod,
        name: &str,
        scalar: bool,
    ) -> Result<()> {
        if scalar {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Scalar as u64)
                .await?;
        } else {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Data as u64)
                .await?;
        }
        self.writer.write_string(name).await?;
        match compression {
            CompressionMethod::None => {
                block.write(&mut self.writer, self.revision).await?;
            }
            CompressionMethod::LZ4 => {
                self.compress_data(CompressionMethod::LZ4.byte(), block)
                    .await?;
            }
        }

        self.writer.flush().await?;

        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_hello<'a>(&mut self, params: ClientHello<'a>) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Hello as u64)
            .await?;
        self.writer.write_string(CLIENT_NAME).await?;
        self.writer.write_var_uint(crate::VERSION_MAJOR).await?;
        self.writer.write_var_uint(crate::VERSION_MINOR).await?;
        self.writer
            .write_var_uint(protocol::DBMS_TCP_PROTOCOL_VERSION)
            .await?;
        self.writer.write_string(params.default_database).await?;
        self.writer.write_string(params.username).await?;
        self.writer.write_string(params.password).await?;
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::quote_parameter;

    #[test]
    fn parameters_are_quoted() {
        assert_eq!(quote_parameter("42"), "'42'");
        assert_eq!(quote_parameter("it's"), "'it\\'s'");
        assert_eq!(quote_parameter("a\\b"), "'a\\\\b'");
        assert_eq!(quote_parameter("a\nb"), "'a\\nb'");
        assert_eq!(quote_parameter("['x']"), "'[\\'x\\']'");
    }
}
