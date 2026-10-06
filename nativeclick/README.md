# Nativeclick

> Fork of [Protryon/klickhouse](https://github.com/Protryon/klickhouse) (`master` branch).

Nativeclick is a pure Rust SDK for working with [ClickHouse](https://clickhouse.com/docs) over the native TCP protocol, in async (tokio) environments, with minimal boilerplate.

## Versioning and compatibility

The crate version follows the ClickHouse version it targets: **`0.MAJOR.MINOR`**, so `0.26.9` targets ClickHouse **26.9**.

Each release supports the targeted version **and the 10 releases before it**: `0.26.9` supports ClickHouse **25.11 to 26.9**. The CI runs the whole test suite against each of them.

The client speaks native protocol revision 54493, and the server negotiates down to its own revision. Every revision-dependent field and column format follows that negotiated revision, so older servers keep working.

## Example

```rust,no_run
use nativeclick::{Client, ClientOptions, QueryOptions, Row};

#[derive(Row, Debug)]
struct Event {
    id: u64,
    name: String,
    tags: Vec<String>,
}

# async fn run() -> nativeclick::Result<()> {
let client = Client::connect("127.0.0.1:9000", ClientOptions::default()).await?;

client
    .insert_native_block(
        "INSERT INTO events FORMAT Native",
        vec![Event { id: 1, name: "start".into(), tags: vec![] }],
    )
    .await?;

// Server-side parameters, settings, a timeout and a query id, per handle.
let events: Vec<Event> = client
    .with_options(
        QueryOptions::new()
            .param("min_id", 1)
            .param_string("name", "start")
            .setting("max_threads", 2)
            .timeout(std::time::Duration::from_secs(30)),
    )
    .query_collect("SELECT * FROM events WHERE id >= {min_id:UInt64} AND name = {name:String}")
    .await?;
# Ok(()) }
```

More in [the examples](https://github.com/Sn0wAlice/nativeclick/blob/master/nativeclick/examples/basic.rs).

## Client features

- Queries stream blocks (`query`, `query_raw`) or collect rows (`query_collect`, `query_one`, `query_opt`). `execute` runs a statement.
- Inserts stream rows (`insert_native`, `insert_native_block`, `insert_native_raw`). Errors are reported: rows that fail to serialize, and server-side failures such as constraints.
- Per-query options with `Client::with_options(QueryOptions)`:
  - settings;
  - server-side `{name:Type}` parameters (`param`, `param_string`);
  - query id;
  - timeout, which cancels the query on the server.
- Dropping a result stream cancels the query on the server.
- `Client::ping`, and `Client::server_info` for the server version, timezone and profile settings.
- `ClientOptions`: credentials, default database, quota key, connect timeout.
- Connections are shared: a `Client` can be cloned and used from many tasks. Queries on one connection run one after the other.
- Pooling with bb8 (`bb8` feature), over TCP or TLS (`ConnectionManager::with_tls`).
- LZ4 compression on writes. On reads it decompresses whatever the server sends: LZ4, ZSTD (the default since ClickHouse 26.9) or none.

## Types

| ClickHouse | Rust |
|---|---|
| `Int8`…`Int256`, `UInt8`…`UInt256`, `Float32/64`, `BFloat16` | integers, floats, `i256`/`u256`, `bf16` |
| `Bool` | `bool` (`u8` also reads it) |
| `Decimal(P, S)`, `Decimal32/64/128/256` | `FixedPoint*`, `rust_decimal::Decimal` |
| `String`, `FixedString(N)` | `String`, `Bytes`, `Vec<u8>` (writes) |
| `UUID`, `IPv4`, `IPv6` | `Uuid`, `Ipv4`, `Ipv6` |
| `Date`, `Date32` | `Date`, `Date32`, `chrono::NaiveDate` |
| `DateTime`, `DateTime64(p)` | `DateTime`, `DateTime64<P>`, `chrono::DateTime` |
| `Time`, `Time64(p)` | `chrono::TimeDelta` |
| `Interval*` | `i64` |
| `Enum8`, `Enum16` | `i8`/`i16` (value) or `String` (name) |
| `Nullable(T)`, `Nothing` | `Option<T>` |
| `Array(T)`, `Map(K, V)`, `Tuple(...)` incl. named tuples | `Vec`, arrays, `HashMap`/`BTreeMap`/`IndexMap`, tuples |
| `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` | as `T` |
| `Point`, `Ring`, `LineString`, `MultiPoint`, `Polygon`, `MultiLineString`, `MultiPolygon` | `Point`, `Ring`, `Polygon`, `MultiPolygon`, `geo-types` |
| `Variant(...)`, `Dynamic`, `Geometry` | `Value` / `DynamicValue` (value and actual type), or any `T` when the row's type matches |
| `JSON` | `String` (JSON text), `Json<T>` (serde) |
| `AggregateFunction(...)` | not readable: the states have no generic format. Select `finalizeAggregation(col)` instead. |

Recent column formats are decoded at the negotiated revision:
- sparse columns;
- replicated columns (JOIN / ARRAY JOIN results);
- strings with a separate size stream (revision 54492).

The client asks the server for `JSON` as text and for the flattened `Dynamic` layout. It does this with two settings, sent before the caller's own settings.

## Running the tests

The integration tests need a ClickHouse server. Start one in Docker:

```sh
docker run --rm --name clickhouse -p 19000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server
export NATIVECLICK_TEST_ADDR=127.0.0.1:19000
# export NATIVECLICK_TEST_USER=default
# export NATIVECLICK_TEST_PASSWORD=default
# export NATIVECLICK_TEST_DATABASE=default
cargo test --all-features -- --test-threads=1
```

The tests share one server and its tables, so run them on one thread.

## Feature flags

- `derive`: Enable [nativeclick_derive], providing a derive macro for the [Row] trait. Default.
- `compression`: compression of client/server communication: writes `lz4`, reads `lz4` and `zstd`. Default.
- `serde`: Derivation of [serde::Serialize] and [serde::Deserialize] on various objects, and JSON support. Default.
- `tls`: TLS support via [tokio-rustls](https://crates.io/crates/tokio-rustls), for connections and the bb8 pool.
- `refinery`: Migrations via [refinery](https://crates.io/crates/refinery).
- `geo-types`: Conversion of geo types to/from the [geo-types](https://crates.io/crates/geo-types) crate.
- `bb8`: Enables a `ConnectionManager` managed by bb8.
- `bfloat16`: The `BFloat16` type. Default.

## Credit

`nativeclick_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at lowest time-cost. In a prototype, `serde` was directly used, but this was abandoned due to lock-in of `serde`'s data model.
