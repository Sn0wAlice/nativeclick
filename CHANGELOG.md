# Changelog

## 0.26.9

The first release under the new versioning: `0.MAJOR.MINOR` follows the targeted ClickHouse version. It targets **ClickHouse 26.9** and supports **25.11 to 26.9**, the targeted version and the 10 releases before it, all tested in CI.

### Protocol

- Native protocol revision **54493** (was 54448, from the ClickHouse 22.x era). Every revision-gated field now follows the revision negotiated with the server, so older servers keep working.
- Reads every column format a recent server sends:
  - sparse columns, including Nullable and tuple elements;
  - replicated columns (JOIN / ARRAY JOIN results);
  - strings with a separate size stream (revision 54492). Strings are also written that way.
- New server packets: ProfileEvents, TimezoneUpdate, Log. A Log packet used to panic the connection.
- Progress reports `elapsed_ns` and `new_total_bytes_to_read`.
- The connection reads `out_of_order_buckets` in block info.

### Client

- `Client::with_options(QueryOptions)` sets, for the queries of a handle:
  - settings;
  - server-side `{name:Type}` parameters (`param`, `param_string`);
  - query id;
  - timeout, which cancels the query on the server.
- Dropping a result stream cancels the query on the server.
- `Client::ping`, `Client::server_info`, and `impl Debug for Client`.
- `ClientOptions::quota_key`, `ClientOptions::connect_timeout`.
- TLS for the bb8 pool: `ConnectionManager::with_tls`.
- Fixes:
  - a request arriving while a large result was being read could corrupt the stream;
  - an unsupported type returned an empty result instead of an error and silently closed the connection;
  - inserts skipped rows that failed to serialize and never reported server-side errors;
  - `DateTime64` before 1970 panicked;
  - `Decimal(P, 0)` / `DateTime64(0)` could not be inserted;
  - ZSTD-compressed answers (the server default since 26.9) could not be read.

### Types

- New: `Bool`, `Date32`, `Time`, `Time64`, `Interval*`, `Nothing` (`SELECT NULL`, `[]`), named tuples, `LineString`, `MultiLineString`, `MultiPoint`, `Geometry`, `Variant`, `Dynamic`, `JSON`, `SimpleAggregateFunction`.
- `Enum8`/`Enum16`:
  - names with commas, quotes or parentheses now parse;
  - values can be written and read as `i8`/`i16` or by name (`String`).
- `chrono::DateTime` is written at the precision of its `DateTime64` column.
- `NaiveDate` is supported, written as `Date` or `Date32` depending on the column.
- `AggregateFunction` columns fail with a clear error pointing to `finalizeAggregation`.

### Breaking changes and how to migrate

- **`Type`, `Value`, `NativeclickError` are `#[non_exhaustive]`, and `Type`/`Value` have new variants.** An exhaustive `match` on them needs a `_ =>` arm.
- **`Bool` columns now read as `Value::Bool`, not `Value::UInt8`.** A `u8` field still reads them, a `bool` field reads both, and `bool` values are written as `Bool` into `Bool` columns.
- **`ClientOptions` has new fields.** A struct literal needs `..Default::default()`.
- **`Progress` and `BlockInfo` are `#[non_exhaustive]` and have new fields.** Build them with `Default::default()`.
- **`DateTime64`/`DynDateTime64` keep their `u64` field.** It holds the raw bits of the signed value; use `from_ticks()`/`ticks()` for dates before 1970.
- **New error variant `NativeclickError::Timeout`.**
- **Type names are printed as the server prints them,** e.g. `DateTime64(3, 'UTC')`, `Map(K, V)`.
- **`FromSql` has a new provided method, `from_sql_column`,** which converts a value as read from a column. Existing implementations need no change. Code that calls `FromSql::from_sql` directly on a column type should call `nativeclick::from_sql_resolved` instead, to look through `LowCardinality`, `Variant`/`Dynamic` rows and similar wrappers.
- **Each query now sends two settings:** `output_format_native_write_json_as_string` and `output_format_native_use_flattened_dynamic_and_json_serialization`. They select the JSON and Dynamic formats this client reads, and a caller's own settings override them.
- **The reported client name and version are now `ClickHouse nativeclick` 26.9** (was 22.9), and `distributed_depth` is 0.

## 0.15.x

Fork of [Protryon/klickhouse](https://github.com/Protryon/klickhouse), renamed `nativeclick`.
