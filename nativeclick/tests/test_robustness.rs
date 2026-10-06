//! Regression tests for failures that used to be silent or fatal:
//! 1. an unsupported type answered "OK, 0 rows" and killed the connection without telling anyone,
//! 2. a request arriving while a packet was half read corrupted the stream,
//! 3. INSERT dropped rows that failed to serialize and never reported server-side errors,
//! 4. DateTime64 before 1970 panicked, and `Decimal(P, 0)` / `DateTime64(0)` could not be inserted.

use std::{borrow::Cow, future::Future, time::Duration};

use futures_util::{StreamExt, stream};
use nativeclick::{
    Client, DateTime64, FixedPoint32, FixedPoint64, FixedPoint128, FixedPoint256, IndexMap,
    NativeclickError, RawRow, Result, Row, Type, Value, i256,
};

/// Every call below must finish: a hang is a failure, not a slow test.
async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .unwrap_or_else(|_| panic!("{what}: timed out"))
}

async fn count(client: &Client, table: &str) -> u64 {
    #[derive(nativeclick::Row)]
    struct Count {
        c: u64,
    }
    let row: Count = bounded(
        "count",
        client.query_one(format!("SELECT count() AS c FROM {table}").as_str()),
    )
    .await
    .unwrap();
    row.c
}

// ---------------------------------------------------------------------------------------------
// 1. Unsupported types
// ---------------------------------------------------------------------------------------------

/// Queries whose result type nativeclick cannot read (yet). Each must be an error, never rows.
const UNSUPPORTED: &[&str] = &[
    "SELECT NULL AS x",
    "SELECT [] AS x",
    "SELECT CAST((1, 'a') AS Tuple(a Int32, b String)) AS x",
    "SELECT toDate32('2020-01-01') AS x",
    "SELECT toTime('12:00:00') AS x",
    "SELECT 1::Variant(UInt8, String) AS x",
    "SELECT '{\"a\":1}'::JSON AS x",
    "SELECT CAST('a,b' AS Enum8('a,b' = 1)) AS x",
];

#[tokio::test]
async fn unsupported_type_is_an_error_not_an_empty_result() {
    for query in UNSUPPORTED {
        let client = super::get_client().await;
        let result = bounded(query, client.query_collect::<RawRow>(*query)).await;
        assert!(
            result.is_err(),
            "{query}: expected an error, got {result:?}"
        );

        // The same failure reaches `execute` and `query_one`.
        let client = super::get_client().await;
        assert!(
            bounded(query, client.execute(*query)).await.is_err(),
            "{query}: execute"
        );
        let client = super::get_client().await;
        assert!(
            bounded(query, client.query_one::<RawRow>(*query))
                .await
                .is_err(),
            "{query}: query_one"
        );
    }
}

#[tokio::test]
async fn unsupported_type_error_names_the_type() {
    let client = super::get_client().await;
    let error = bounded(
        "SELECT NULL",
        client.query_collect::<RawRow>("SELECT NULL AS x"),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("Nothing"),
        "error should name the type: {error}"
    );
}

#[tokio::test]
async fn later_calls_report_why_the_connection_closed() {
    let client = super::get_client().await;
    let first = bounded("first", client.query_collect::<RawRow>("SELECT NULL AS x"))
        .await
        .unwrap_err();

    // Every later call fails at once, with the same cause, on this handle and its clones.
    for _ in 0..3 {
        let clone = client.clone();
        let next = bounded("next", clone.query_collect::<RawRow>("SELECT 1 AS x"))
            .await
            .unwrap_err();
        assert_eq!(next.to_string(), first.to_string());
    }
    assert!(client.is_closed());

    // Inserts fail the same way instead of hanging.
    let insert = bounded(
        "insert",
        client.insert_native_block("INSERT INTO nowhere FORMAT Native", vec![RawRow::default()]),
    )
    .await
    .unwrap_err();
    assert_eq!(insert.to_string(), first.to_string());

    // The server is fine: a new connection works.
    let fresh = super::get_client().await;
    bounded("fresh", fresh.execute("SELECT 1")).await.unwrap();
}

#[tokio::test]
async fn queries_queued_behind_a_fatal_one_get_the_error() {
    let client = super::get_client().await;
    // Started together, so most of them are queued when the first one kills the connection.
    let mut queries = vec![tokio::spawn({
        let client = client.clone();
        async move { client.query_collect::<RawRow>("SELECT NULL AS x").await }
    })];
    for _ in 0..20 {
        let client = client.clone();
        queries.push(tokio::spawn(async move {
            client
                .query_collect::<RawRow>("SELECT number FROM numbers(10)")
                .await
        }));
    }
    let mut errors = 0;
    for query in queries {
        match bounded("queued", query).await.unwrap() {
            Ok(rows) => assert_eq!(rows.len(), 10, "a query that ran must be complete"),
            Err(_) => errors += 1,
        }
    }
    assert!(errors >= 1, "the fatal query must fail");
}

#[tokio::test]
async fn supported_types_still_work_after_the_change() {
    let client = super::get_client().await;
    let rows = bounded(
        "select",
        client.query_collect::<RawRow>("SELECT number, toString(number) FROM numbers(1000)"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1000);
    assert!(!client.is_closed());
}

// ---------------------------------------------------------------------------------------------
// 2. Concurrent use of one connection
// ---------------------------------------------------------------------------------------------

#[derive(nativeclick::Row)]
struct NumberRow {
    n: u64,
    s: String,
}

/// One clone streams a large, multi-block result while others keep sending queries on the same
/// connection. Every request used to cancel the half-read packet and desync the stream.
#[tokio::test]
async fn concurrent_queries_on_one_connection_do_not_corrupt_results() {
    const ROWS: u64 = 2_000_000;
    let client = super::get_client().await;

    for round in 0..3 {
        let big = tokio::spawn({
            let client = client.clone();
            async move {
                let query =
                    format!("SELECT number AS n, toString(number) AS s FROM numbers({ROWS})");
                let mut stream = client.query::<NumberRow, _>(query.as_str()).await?;
                let (mut rows, mut sum) = (0u64, 0u64);
                while let Some(row) = stream.next().await {
                    let row = row?;
                    assert_eq!(row.s, row.n.to_string());
                    rows += 1;
                    sum += row.n;
                    // A slow consumer makes the connection task wait while requests pile up.
                    if rows % 100_000 == 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                Ok::<_, NativeclickError>((rows, sum))
            }
        });

        let small: Vec<_> = (0..8)
            .map(|task| {
                let client = client.clone();
                tokio::spawn(async move {
                    for i in 0..25u64 {
                        let rows = client
                            .query_collect::<NumberRow>(
                                format!(
                                    "SELECT number AS n, toString(number) AS s FROM numbers({})",
                                    task * 100 + i
                                )
                                .as_str(),
                            )
                            .await?;
                        assert_eq!(rows.len() as u64, task * 100 + i);
                        for (expected, row) in rows.iter().enumerate() {
                            assert_eq!(row.n, expected as u64);
                            assert_eq!(row.s, expected.to_string());
                        }
                    }
                    Ok::<_, NativeclickError>(())
                })
            })
            .collect();

        let (rows, sum) = bounded("big query", big).await.unwrap().unwrap();
        assert_eq!(rows, ROWS, "round {round}");
        assert_eq!(sum, ROWS * (ROWS - 1) / 2, "round {round}");
        for task in small {
            bounded("small queries", task).await.unwrap().unwrap();
        }
    }
    assert!(!client.is_closed());
}

/// Requests arrive continuously while a large result is being read: each one used to cancel
/// the half-read packet and desync the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_arriving_mid_packet_do_not_corrupt_the_stream() {
    const ROWS: u64 = 1_000_000;
    let client = super::get_client().await;

    for round in 0..2 {
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let spammer = tokio::spawn({
            let client = client.clone();
            let done = done.clone();
            async move {
                let mut queued = vec![];
                let mut i = 0u64;
                while !done.load(std::sync::atomic::Ordering::Relaxed) && i < 3_000 {
                    let client = client.clone();
                    let n = i % 50;
                    queued.push(tokio::spawn(async move {
                        let rows = client
                            .query_collect::<NumberRow>(
                                format!(
                                    "SELECT number AS n, toString(number) AS s FROM numbers({n})"
                                )
                                .as_str(),
                            )
                            .await?;
                        assert_eq!(rows.len() as u64, n);
                        Ok::<_, NativeclickError>(())
                    }));
                    i += 1;
                    tokio::task::yield_now().await;
                    if i.is_multiple_of(64) {
                        tokio::time::sleep(Duration::from_micros(200)).await;
                    }
                }
                queued
            }
        });

        let query = format!("SELECT number AS n, toString(number) AS s FROM numbers({ROWS})");
        let big = async {
            let mut stream = client.query::<NumberRow, _>(query.as_str()).await?;
            let (mut rows, mut sum) = (0u64, 0u64);
            while let Some(row) = stream.next().await {
                let row = row?;
                assert_eq!(row.s, row.n.to_string());
                rows += 1;
                sum += row.n;
            }
            Ok::<_, NativeclickError>((rows, sum))
        };
        let result = bounded("big query", big).await;
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        let (rows, sum) = result.unwrap_or_else(|e| panic!("round {round}: {e}"));
        assert_eq!(rows, ROWS, "round {round}");
        assert_eq!(sum, ROWS * (ROWS - 1) / 2, "round {round}");
        let queued = bounded("spammer", spammer).await.unwrap();
        assert!(
            queued.len() > 100,
            "only {} requests during the stream",
            queued.len()
        );
        for task in queued {
            bounded("queued query", task).await.unwrap().unwrap();
        }
    }
    assert!(!client.is_closed());
}

/// Inserts and selects from many clones of one client interleave without corrupting either.
#[tokio::test]
async fn concurrent_inserts_and_selects_on_one_connection() {
    #[derive(nativeclick::Row, Clone)]
    struct Item {
        id: u64,
        label: String,
    }

    let client = super::get_client().await;
    super::prepare_table("test_robust_concurrent", "id UInt64, label String", &client).await;

    let tasks: Vec<_> = (0..10u64)
        .map(|task| {
            let client = client.clone();
            tokio::spawn(async move {
                for batch in 0..10u64 {
                    let rows: Vec<Item> = (0..500)
                        .map(|i| {
                            let id = task * 1_000_000 + batch * 1_000 + i;
                            Item {
                                id,
                                label: format!("item-{id}"),
                            }
                        })
                        .collect();
                    client
                        .insert_native_block(
                            "INSERT INTO test_robust_concurrent FORMAT Native",
                            rows,
                        )
                        .await?;
                    client
                        .query_collect::<NumberRow>(
                            "SELECT number AS n, toString(number) AS s FROM numbers(5000)",
                        )
                        .await?;
                }
                Ok::<_, NativeclickError>(())
            })
        })
        .collect();
    for task in tasks {
        bounded("insert task", task).await.unwrap().unwrap();
    }

    assert_eq!(
        count(&client, "test_robust_concurrent").await,
        10 * 10 * 500
    );
    let bad: Vec<Item> = bounded(
        "check",
        client.query_collect(
            "SELECT id, label FROM test_robust_concurrent WHERE label != concat('item-', toString(id))",
        ),
    )
    .await
    .unwrap();
    assert_eq!(bad.len(), 0);
}

// ---------------------------------------------------------------------------------------------
// 3. INSERT errors
// ---------------------------------------------------------------------------------------------

/// A row type that fails to serialize when its value is 13.
struct Flaky(u32);

impl Row for Flaky {
    const COLUMN_COUNT: Option<usize> = Some(1);

    fn column_names() -> Option<Vec<Cow<'static, str>>> {
        Some(vec!["x".into()])
    }

    fn deserialize_row(_map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        unreachable!("only inserted")
    }

    fn serialize_row(
        self,
        _type_hints: &IndexMap<String, Type>,
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
        if self.0 == 13 {
            return Err(NativeclickError::SerializeError("unlucky row".to_string()));
        }
        Ok(vec![("x".into(), Value::UInt32(self.0))])
    }
}

#[tokio::test]
async fn insert_with_a_bad_row_fails_and_inserts_nothing() {
    let client = super::get_client().await;
    super::prepare_table("test_robust_bad_row", "x UInt32", &client).await;

    let rows = (0..100).map(Flaky).collect::<Vec<_>>();
    let error = bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_robust_bad_row FORMAT Native", rows),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unlucky row"), "{error}");

    // Nothing from the failed block is inserted, and the connection is still usable.
    assert_eq!(count(&client, "test_robust_bad_row").await, 0);
    bounded(
        "next insert",
        client.insert_native_block(
            "INSERT INTO test_robust_bad_row FORMAT Native",
            (20..30).map(Flaky).collect(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(count(&client, "test_robust_bad_row").await, 10);
}

#[tokio::test]
async fn insert_stream_stops_at_the_first_bad_block() {
    let client = super::get_client().await;
    super::prepare_table("test_robust_bad_stream", "x UInt32", &client).await;

    // Batch 1 is fine, batch 2 has the bad row, batch 3 must never be sent.
    let batches = vec![
        (0..10).map(Flaky).collect::<Vec<_>>(),
        (10..20).map(Flaky).collect(),
        (20..30).map(Flaky).collect(),
    ];
    let error = bounded(
        "insert",
        client.insert_native(
            "INSERT INTO test_robust_bad_stream FORMAT Native",
            stream::iter(batches),
        ),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unlucky row"), "{error}");

    // Blocks sent before the bad one may be inserted, the rest never are.
    let inserted = count(&client, "test_robust_bad_stream").await;
    assert!(inserted == 0 || inserted == 10, "inserted {inserted}");
    bounded("next query", client.execute("SELECT 1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn server_side_insert_error_is_returned() {
    #[derive(nativeclick::Row)]
    struct Checked {
        x: u32,
    }

    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_constraint",
        "x UInt32, CONSTRAINT small CHECK x < 100",
        &client,
    )
    .await;

    // Rejected by the server after it received the data: this error used to be lost.
    let error = bounded(
        "insert",
        client.insert_native_block(
            "INSERT INTO test_robust_constraint FORMAT Native",
            vec![Checked { x: 1 }, Checked { x: 500 }],
        ),
    )
    .await
    .unwrap_err();
    match &error {
        NativeclickError::ServerException { message, .. } => {
            assert!(message.contains("small"), "{message}")
        }
        other => panic!("expected a server exception, got {other:?}"),
    }
    assert_eq!(count(&client, "test_robust_constraint").await, 0);

    // The connection survives, and so do valid inserts.
    bounded(
        "valid insert",
        client.insert_native_block(
            "INSERT INTO test_robust_constraint FORMAT Native",
            vec![Checked { x: 1 }, Checked { x: 2 }],
        ),
    )
    .await
    .unwrap();
    assert_eq!(count(&client, "test_robust_constraint").await, 2);
}

#[tokio::test]
async fn server_side_error_in_the_middle_of_a_stream() {
    #[derive(nativeclick::Row)]
    struct Checked {
        x: u32,
    }

    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_constraint_stream",
        "x UInt32, CONSTRAINT small CHECK x < 100",
        &client,
    )
    .await;

    // Many blocks after the bad one: the client must stop sending and not desync.
    let batches = (0..50u32).map(|batch| {
        (0..100u32)
            .map(|i| Checked {
                x: if batch == 5 && i == 50 { 1000 } else { i % 100 },
            })
            .collect::<Vec<_>>()
    });
    let error = bounded(
        "insert",
        client.insert_native(
            "INSERT INTO test_robust_constraint_stream FORMAT Native",
            stream::iter(batches.collect::<Vec<_>>()),
        ),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );

    // Still usable for both reads and inserts.
    bounded("select", client.execute("SELECT 1")).await.unwrap();
    super::prepare_table("test_robust_after_error", "x UInt32", &client).await;
    bounded(
        "insert after",
        client.insert_native_block(
            "INSERT INTO test_robust_after_error FORMAT Native",
            (0..5).map(Flaky).collect(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(count(&client, "test_robust_after_error").await, 5);
}

#[tokio::test]
async fn insert_into_missing_table_is_an_error() {
    let client = super::get_client().await;
    let error = bounded(
        "insert",
        client.insert_native_block(
            "INSERT INTO test_robust_does_not_exist FORMAT Native",
            vec![Flaky(1)],
        ),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );
    bounded("next", client.execute("SELECT 1")).await.unwrap();
}

#[tokio::test]
async fn insert_native_raw_reports_server_errors() {
    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_raw",
        "x UInt32, CONSTRAINT small CHECK x < 100",
        &client,
    )
    .await;
    // Header first, to build blocks with the right column types.
    let mut header = client
        .query_raw("SELECT x FROM test_robust_raw LIMIT 0")
        .await
        .unwrap();
    let header = header.next().await.unwrap().unwrap();
    let mut block = header.clone();
    block.rows = 1;
    block
        .column_data
        .insert("x".to_string(), vec![Value::UInt32(1000)]);

    let result = bounded(
        "raw insert",
        client.insert_native_raw(
            "INSERT INTO test_robust_raw FORMAT Native",
            stream::iter(vec![block]),
        ),
    )
    .await;
    let error = match result {
        Err(e) => e,
        Ok(mut responses) => loop {
            match bounded("responses", responses.next()).await {
                Some(Err(e)) => break e,
                Some(Ok(_)) => continue,
                None => panic!("the server error was lost"),
            }
        },
    };
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );
    bounded("next", client.execute("SELECT 1")).await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// 4. DateTime64 before 1970, scale 0
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn datetime64_before_1970_round_trips() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Dates {
        id: u32,
        d3: DateTime64<3>,
        d0: DateTime64<0>,
        d9: DateTime64<9>,
        text: String,
    }

    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_dt64",
        "id UInt32, d3 DateTime64(3, 'UTC'), d0 DateTime64(0, 'UTC'), d9 DateTime64(9, 'UTC'), text String",
        &client,
    )
    .await;

    // Seconds (with millis) around and well before the epoch, within the DateTime64 range.
    let cases: &[(i64, &str)] = &[
        (-1, "1969-12-31 23:59:59.999"),
        (-1000, "1969-12-31 23:59:59.000"),
        (-315_619_199_750, "1960-01-01 00:00:00.250"),
        (-2_208_988_800_000, "1900-01-01 00:00:00.000"),
        (0, "1970-01-01 00:00:00.000"),
        (1_700_000_000_123, "2023-11-14 22:13:20.123"),
    ];
    let rows: Vec<Dates> = cases
        .iter()
        .enumerate()
        .map(|(id, (millis, text))| Dates {
            id: id as u32,
            d3: DateTime64::from_ticks(nativeclick::Tz::UTC, *millis),
            d0: DateTime64::from_ticks(nativeclick::Tz::UTC, millis.div_euclid(1000)),
            d9: DateTime64::from_ticks(nativeclick::Tz::UTC, millis * 1_000_000),
            text: text.to_string(),
        })
        .collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_robust_dt64 FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();

    // The server agrees on what was written…
    #[derive(nativeclick::Row)]
    struct Check {
        text: String,
        server: String,
    }
    let checks: Vec<Check> = bounded(
        "check",
        client
            .query_collect("SELECT text, toString(d3) AS server FROM test_robust_dt64 ORDER BY id"),
    )
    .await
    .unwrap();
    for check in checks {
        assert_eq!(check.server, check.text);
    }

    // …and reading it back gives the same values, without panicking.
    let back: Vec<Dates> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_robust_dt64 ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);

    // Values produced by the server (not by this client) read fine too.
    let read: Vec<RawRow> = bounded(
        "server values",
        client.query_collect(
            "SELECT toDateTime64('1955-05-05 05:05:05.555', 3, 'UTC') AS a, toDateTime64('1900-01-01 00:00:00', 9, 'UTC') AS b",
        ),
    )
    .await
    .unwrap();
    assert_eq!(read.len(), 1);
}

#[tokio::test]
async fn chrono_dates_before_1970_round_trip_at_column_precision() {
    use chrono::{TimeZone, Utc};

    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Chrono {
        id: u32,
        d3: chrono::DateTime<Utc>,
        d9: chrono::DateTime<Utc>,
    }

    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_chrono",
        "id UInt32, d3 DateTime64(3, 'UTC'), d9 DateTime64(9, 'UTC')",
        &client,
    )
    .await;

    let dates = [
        Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 59).unwrap()
            + chrono::Duration::milliseconds(999),
        Utc.with_ymd_and_hms(1923, 4, 5, 6, 7, 8).unwrap() + chrono::Duration::milliseconds(9),
        Utc.with_ymd_and_hms(2024, 2, 29, 12, 0, 0).unwrap(),
    ];
    let rows: Vec<Chrono> = dates
        .iter()
        .enumerate()
        .map(|(id, date)| Chrono {
            id: id as u32,
            d3: *date,
            d9: *date + chrono::Duration::nanoseconds(123),
        })
        .collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_robust_chrono FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Chrono> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_robust_chrono ORDER BY id"),
    )
    .await
    .unwrap();
    // DateTime64(9) keeps the nanoseconds: it used to be written as DateTime64(6).
    assert_eq!(back, rows);
}

#[tokio::test]
async fn decimal_and_datetime64_with_scale_zero_can_be_inserted() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Scales {
        d9_0: FixedPoint32<0>,
        d18_0: FixedPoint64<0>,
        d38_0: FixedPoint128<0>,
        d76_0: FixedPoint256<0>,
        d9_9: FixedPoint32<9>,
        dt0: DateTime64<0>,
    }

    let client = super::get_client().await;
    super::prepare_table(
        "test_robust_scale0",
        "d9_0 Decimal(9, 0), d18_0 Decimal(18, 0), d38_0 Decimal(38, 0), d76_0 Decimal(76, 0), \
         d9_9 Decimal(9, 9), dt0 DateTime64(0, 'UTC')",
        &client,
    )
    .await;

    let rows = vec![
        Scales {
            d9_0: FixedPoint32(-999_999_999),
            d18_0: FixedPoint64(123_456_789_012_345_678),
            d38_0: FixedPoint128(-12_345_678_901_234_567_890_123_456_789),
            d76_0: FixedPoint256(i256::from((0u128, 42u128))),
            d9_9: FixedPoint32(-999_999_999),
            dt0: DateTime64::from_ticks(nativeclick::Tz::UTC, -86_400),
        },
        Scales {
            d9_0: FixedPoint32(0),
            d18_0: FixedPoint64(0),
            d38_0: FixedPoint128(0),
            d76_0: FixedPoint256(i256::from((0u128, 0u128))),
            d9_9: FixedPoint32(1),
            dt0: DateTime64::from_ticks(nativeclick::Tz::UTC, 1_700_000_000),
        },
    ];
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_robust_scale0 FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();

    #[derive(nativeclick::Row)]
    struct Text {
        a: String,
        b: String,
        e: String,
        f: String,
    }
    let text: Vec<Text> = bounded(
        "text",
        client.query_collect(
            "SELECT toString(d9_0) AS a, toString(d18_0) AS b, toString(d9_9) AS e, toString(dt0) AS f \
             FROM test_robust_scale0 ORDER BY d9_0",
        ),
    )
    .await
    .unwrap();
    assert_eq!(text[0].a, "-999999999");
    assert_eq!(text[0].b, "123456789012345678");
    assert_eq!(text[0].e, "-0.999999999");
    assert_eq!(text[0].f, "1969-12-31 00:00:00");

    let back: Vec<Scales> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_robust_scale0 ORDER BY d9_0"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
}
