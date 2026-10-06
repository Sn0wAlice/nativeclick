# nativeclick

> Fork of [Protryon/klickhouse](https://github.com/Protryon/klickhouse) (`master` branch), renamed to `nativeclick`.

Nativeclick is a pure Rust client for [ClickHouse](https://clickhouse.com/docs) over the **native TCP protocol**, for async (tokio) applications, with derive-based rows and minimal boilerplate.

## Versioning and compatibility

- **Version scheme:** the crate version follows the ClickHouse version it targets, `0.MAJOR.MINOR`. `0.26.9` targets ClickHouse **26.9**.
- **Supported servers:** each release supports the targeted version and **the 10 releases before it**. For `0.26.9` that is ClickHouse **25.11 to 26.9**, and CI runs the whole test suite against each of them.
- **Protocol:** the client speaks native protocol revision **54493**, and each connection negotiates `min(client, server)`. Every revision-dependent field and column format follows the negotiated revision, so older servers keep working.
- **Rust:** MSRV is **1.89**, tested in CI.

See [CHANGELOG.md](https://github.com/Sn0wAlice/nativeclick/blob/master/CHANGELOG.md) for the changes since klickhouse and how to migrate.

## nativeclick vs klickhouse

Compared with klickhouse 0.15 (the fork point, protocol revision 54448):

| | klickhouse 0.15 | nativeclick 0.26.9 |
|---|---|---|
| **Compatibility** | | |
| Protocol revision | 54448 (ClickHouse 22.x era) | 54493, negotiated per connection |
| ClickHouse 26.9 (ZSTD answers by default) | ❌ empty results, then the connection dies | ✅ |
| Versions tested in CI | 25.8 | 25.11 → 26.9 (11 versions) |
| Compressed answers read | LZ4 only | LZ4, ZSTD, none |
| Sparse / replicated columns, size-stream strings | ❌ (revision too old to receive them) | ✅ |
| **Reliability** | | |
| Unsupported column type | ❌ "OK, 0 rows", then the connection silently dies | ✅ clear error naming the type |
| Concurrent use of a cloned `Client` | ❌ can corrupt the stream | ✅ |
| INSERT row that fails to serialize | ❌ silently skipped | ✅ error, nothing sent for that block |
| Server-side INSERT errors (constraints…) | ❌ lost | ✅ returned |
| `DateTime64` before 1970 | ❌ panics | ✅ |
| `Decimal(P, 0)`, `DateTime64(0)` inserts | ❌ rejected | ✅ |
| Server log packets (`send_logs_level`) | ❌ panic the connection | ✅ |
| Malformed server data | panics (`unimplemented!`, asserts) | errors for offsets, maps, kinds, packets |
| **Features** | | |
| Per-query settings | ❌ | ✅ `QueryOptions::setting` |
| Server-side `{name:Type}` parameters | ❌ | ✅ `param`, `param_string` |
| Query id | random, hidden | ✅ settable, used in progress events |
| Query timeout | ❌ | ✅ cancels on the server |
| Cancel a running query | ❌ keeps running | ✅ on stream drop or timeout |
| Connect timeout, quota key | ❌ | ✅ `ClientOptions` |
| Ping, server info (version, timezone, profile settings) | ❌ | ✅ `ping()`, `server_info()` |
| TLS in the bb8 pool | ❌ | ✅ `ConnectionManager::with_tls` |
| **Types** | | |
| `Bool` | as `UInt8` | ✅ own type (`u8` still reads it) |
| `Date32`, `Time`, `Time64`, `Interval*` | ❌ | ✅ |
| `Nothing` (`SELECT NULL`, `[]`) | ❌ | ✅ |
| Named tuples | ❌ | ✅ |
| `Enum` names with `,` `'` `(` | ❌ misparsed | ✅, read/write by value or by name |
| `LineString`, `MultiLineString`, `MultiPoint`, `Geometry` | ❌ | ✅ |
| `Variant`, `Dynamic`, `JSON` | ❌ | ✅ |
| `SimpleAggregateFunction` | ❌ | ✅ |
| `AggregateFunction` | ❌ | ❌ by design, explicit error pointing to `finalizeAggregation` |
| **Code health** | | |
| Unmaintained / dead dependencies | `paste`, `compiler-tools`, `rustc_version` + dead `build.rs` | removed |
| Error kinds | catch-all `ProtocolError(String)` | `ConnectionClosed`, `Timeout`, `Compression`, `Unsupported`, … |
| Tests | 13 integration tests, run on one version | 116 unit + 61 integration, on 11 versions |

Decoding speed of this crate's decoder before and after the 0.26.9 optimizations. Measured with `examples/throughput.rs` (2M rows × 5 columns, ClickHouse 26.9 on localhost, ZSTD answers):

| | before | after |
|---|---|---|
| SELECT, blocks | 4.0 M rows/s | 6.1 M rows/s |
| SELECT, derived rows | 2.8 M rows/s | 3.6 M rows/s |
| INSERT, derived rows | 0.94 M rows/s | 1.5 M rows/s |

## Getting started

```toml
[dependencies]
nativeclick = "0.26.9"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use nativeclick::{Client, ClientOptions, Row};

#[derive(Row, Debug, Clone)]
struct Event {
    id: u64,
    name: String,
    tags: Vec<String>,
    score: Option<f64>,
}

#[tokio::main]
async fn main() -> nativeclick::Result<()> {
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default()).await?;

    client
        .execute("CREATE TABLE IF NOT EXISTS events (id UInt64, name String, tags Array(String), score Nullable(Float64)) ENGINE = MergeTree ORDER BY id")
        .await?;

    client
        .insert_native_block(
            "INSERT INTO events FORMAT Native",
            vec![Event { id: 1, name: "start".into(), tags: vec!["a".into()], score: None }],
        )
        .await?;

    let events: Vec<Event> = client.query_collect("SELECT * FROM events").await?;
    println!("{events:?}");
    Ok(())
}
```

More in [the examples](https://github.com/Sn0wAlice/nativeclick/tree/master/nativeclick/examples).

## Queries

| Method | Use |
|---|---|
| `query::<T>` | stream of rows `T: Row` |
| `query_collect`, `query_one`, `query_opt` | all rows, the first one, or the first one if any |
| `query_raw` | stream of raw `Block`s |
| `execute` | run a statement and wait for it; `execute_now` does not wait |
| `insert_native`, `insert_native_block` | insert rows; the query must end with `FORMAT Native` |
| `insert_native_raw` | insert raw `Block`s |

Rows are any struct with `#[derive(Row)]`, `RawRow` (dynamic access by index or name), or `UnitValue<T>` for a single column. Field names match column names, and can be adjusted with `#[nativeclick(rename = "...")]`, `rename_all`, `default`, `flatten`, `nested`.

### Per-query options

`Client::with_options` returns a handle on the same connection whose queries use the given options. The original handle is unchanged.

```rust,no_run
# async fn f(client: nativeclick::Client) -> nativeclick::Result<()> {
use std::time::Duration;
use nativeclick::{QueryOptions, Uuid};

let id = Uuid::new_v4();
let tuned = client.with_options(
    QueryOptions::new()
        .query_id(id)                          // visible in system.query_log, KILL QUERY
        .setting("max_threads", 4)             // any ClickHouse setting
        .setting("max_execution_time", 60)
        .param("min_id", 10)                   // {min_id:UInt64}
        .param("ids", "[1, 2, 3]")             // {ids:Array(UInt8)}, as clickhouse-client --param_ids
        .param_string("name", "it's \"quoted\"") // any text for a {name:String}, escaped for you
        .timeout(Duration::from_secs(30)),     // cancels the query on the server when exceeded
);
tuned
    .execute("SELECT count() FROM events WHERE id >= {min_id:UInt64} AND name != {name:String}")
    .await?;
# Ok(()) }
```

- **Parameters** are values sent separately from the SQL, so they cannot inject SQL.
  - `param` takes the text exactly as `clickhouse-client --param_x=...` does: numbers, dates, `[1,2]`, `['a','b']`.
  - `param_string` takes any text for a `String` parameter and escapes it.
- **Timeouts** fail the query with `NativeclickError::Timeout` and cancel it on the server, so the connection stays usable.
- **Dropping a result stream** before its end also cancels the query on the server.

### Connection

```rust,no_run
# async fn f() -> nativeclick::Result<()> {
use std::time::Duration;
use nativeclick::{Client, ClientOptions};

let client = Client::connect(
    "127.0.0.1:9000",
    ClientOptions {
        username: "default".into(),
        password: String::new(),
        default_database: "default".into(),
        quota_key: "tenant-42".into(),
        connect_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    },
)
.await?;

let server = client.server_info();
println!("{} {}.{} ({:?}), protocol {}", server.name, server.version_major, server.version_minor, server.timezone, server.negotiated_revision);
client.ping().await?;
# Ok(()) }
```

A `Client` is cheap to clone and can be used from many tasks; queries on one connection run one after the other. For parallelism, use several connections, for example with the bb8 pool:

```rust,ignore
// features = ["bb8"] (and "tls" for with_tls)
let manager = nativeclick::ConnectionManager::new("127.0.0.1:9000", ClientOptions::default()).await?;
let pool = nativeclick::bb8::Pool::builder().max_size(8).build(manager).await?;
let client = pool.get().await?;
```

Connection-level failures are reported to every waiting call, and later calls on the same `Client` return the error that closed it. `Client::is_closed()` tells when to reconnect, and the bb8 pool does it for you.

### Errors

`NativeclickError` is `#[non_exhaustive]` and `Clone`. The main variants:

| Variant | Meaning |
|---|---|
| `ServerException { code, name, message, .. }` | ClickHouse rejected the query |
| `Timeout` | `QueryOptions::timeout` or `ClientOptions::connect_timeout` expired |
| `ConnectionClosed` | the connection is gone |
| `DeserializeError`, `SerializeError`, `UnexpectedType*`, `TypeParseError` | a value or type does not match |
| `Compression`, `ProtocolError` | corrupt frame or unexpected server data |
| `Unsupported` | the server is too old for a requested feature |

## Types

| ClickHouse | Rust |
|---|---|
| `Int8`…`Int256`, `UInt8`…`UInt256`, `Float32/64`, `BFloat16` | integers, floats, `i256`/`u256`, `bf16` |
| `Bool` | `bool` (`u8` also reads it) |
| `Decimal(P, S)`, `Decimal32/64/128/256` | `FixedPoint32/64/128/256<S>`, `rust_decimal::Decimal` |
| `String`, `FixedString(N)` | `String`, `Bytes`; `Vec<u8>` writes |
| `UUID`, `IPv4`, `IPv6` | `Uuid`, `Ipv4`, `Ipv6` |
| `Date`, `Date32` | `Date`, `Date32`, `chrono::NaiveDate` |
| `DateTime`, `DateTime64(p)` | `DateTime`, `DateTime64<P>` (`from_ticks`/`ticks` for dates before 1970), `chrono::DateTime` |
| `Time`, `Time64(p)` | `chrono::TimeDelta` |
| `Interval*` | `i64` |
| `Enum8`, `Enum16` | `i8`/`i16` (value) or `String` (name) |
| `Nullable(T)`, `Nothing` | `Option<T>` |
| `Array(T)` | `Vec<T>`, `[T; N]` |
| `Map(K, V)` | `HashMap`, `BTreeMap`, `IndexMap` |
| `Tuple(...)`, named tuples | Rust tuples |
| `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` | as `T` |
| `Point`, `Ring`, `LineString`, `MultiPoint`, `Polygon`, `MultiLineString`, `MultiPolygon` | `Point`, `Ring`, `Polygon`, `MultiPolygon` (and `geo-types` with the feature) |
| `Variant(...)`, `Dynamic`, `Geometry` | `Value` / `DynamicValue` (value plus actual type), or `T` / `Option<T>` when rows have that type |
| `JSON` | `String` (JSON text), `Json<T>` (serde) |
| `AggregateFunction(...)` | not readable: states have no generic format. Select `finalizeAggregation(col)` instead. |

Notes:
- **JSON and Dynamic formats:** the client asks the server for `JSON` columns as text and for the flattened `Dynamic` layout, with two settings sent before the caller's own (which can override them).
- **Writing `Variant` / `Dynamic` / `Geometry`:**
  - a `DynamicValue` chooses the type explicitly;
  - a plain value goes to the first variant that accepts it (`Variant`) or to its natural type (`Dynamic`).
- **Recent column formats:** sparse and replicated columns, and the size-stream string layout (revision 54492), are decoded transparently.

## Feature flags

- `derive`: the `#[derive(Row)]` macro. Default.
- `compression`: compression of client/server traffic: writes `lz4`, reads `lz4` and `zstd`. Default.
- `serde`: `serde` support on values, and the `Json<T>` wrapper. Default.
- `bfloat16`: the `BFloat16` type. Default.
- `tls`: TLS via [tokio-rustls](https://crates.io/crates/tokio-rustls), for `Client::connect_tls` and the bb8 pool.
- `bb8`: a [bb8](https://crates.io/crates/bb8) `ConnectionManager`.
- `refinery`: migrations via [refinery](https://crates.io/crates/refinery).
- `geo-types`: conversions with the [geo-types](https://crates.io/crates/geo-types) crate.
- `rust_decimal`: conversions with [rust_decimal](https://crates.io/crates/rust_decimal).

## Running the tests

The integration tests need a ClickHouse server:

```sh
docker run -d --name clickhouse -p 19000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 --ulimit nofile=262144:262144 clickhouse/clickhouse-server:26.9
export NATIVECLICK_TEST_ADDR=127.0.0.1:19000
# export NATIVECLICK_TEST_USER=default NATIVECLICK_TEST_PASSWORD= NATIVECLICK_TEST_DATABASE=default
cargo test --all-features -- --test-threads=1
```

The tests share one server and its tables, so run them on one thread.

To measure throughput:

```sh
NATIVECLICK_TEST_ADDR=127.0.0.1:19000 cargo run --release --example throughput
```

## Credit

Nativeclick started as a fork of [klickhouse](https://github.com/Protryon/klickhouse) by Protryon. `nativeclick_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at the lowest time cost. In a prototype, `serde` was used directly, but this was abandoned because of the lock-in of `serde`'s data model.
