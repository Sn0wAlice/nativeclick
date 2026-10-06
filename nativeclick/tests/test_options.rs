//! Per-query options (settings, parameters, query id, timeout), cancellation, ping, server info
//! and connection options.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use nativeclick::{Client, ClientOptions, NativeclickError, QueryOptions, RawRow, Uuid};

async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .unwrap_or_else(|_| panic!("{what}: timed out"))
}

#[derive(nativeclick::Row, Debug)]
struct Text {
    v: String,
}

async fn text(client: &Client, query: &str) -> String {
    bounded(query, client.query_one::<Text>(query))
        .await
        .unwrap()
        .v
}

#[tokio::test]
async fn settings_apply_to_the_queries_of_the_handle() {
    let client = super::get_client().await;
    let with = client.with_options(QueryOptions::new().setting("max_block_size", 1234));
    assert_eq!(
        text(&with, "SELECT toString(getSetting('max_block_size')) AS v").await,
        "1234"
    );
    // The original handle is unchanged.
    assert_ne!(
        text(
            &client,
            "SELECT toString(getSetting('max_block_size')) AS v"
        )
        .await,
        "1234"
    );
    // Settings change behaviour, not just reported values.
    let limited = client.with_options(
        QueryOptions::new()
            .setting("max_result_rows", 10)
            .setting("result_overflow_mode", "throw"),
    );
    let error = bounded(
        "limited",
        limited.query_collect::<RawRow>("SELECT number FROM numbers(1000)"),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn unknown_settings_are_ignored_not_fatal() {
    let client = super::get_client().await;
    let with = client.with_options(QueryOptions::new().setting("no_such_setting_at_all", 1));
    assert_eq!(text(&with, "SELECT 'ok' AS v").await, "ok");
}

/// The server rejects the value while reading the query, before the query exists: it sends the
/// error and closes the connection (server behaviour). The caller gets that error, and later
/// calls fail fast instead of hanging.
#[tokio::test]
async fn invalid_setting_value_is_a_server_error() {
    let client = super::get_client().await;
    let with = client.with_options(QueryOptions::new().setting("max_threads", "not a number"));
    let error = bounded("bad", with.execute("SELECT 1")).await.unwrap_err();
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );
    assert!(bounded("next", client.execute("SELECT 1")).await.is_err());
    let fresh = super::get_client().await;
    bounded("fresh", fresh.execute("SELECT 1")).await.unwrap();
}

#[tokio::test]
async fn server_side_parameters() {
    #[derive(nativeclick::Row, Debug)]
    struct Params {
        n: u64,
        s: String,
        ids: Vec<u8>,
        d: String,
        names: Vec<String>,
    }

    let client = super::get_client().await;
    let tricky = "it's a \\ test\nwith 'quotes', tabs\tand {braces} \\n not a newline \0 nul";
    let with = client.with_options(
        QueryOptions::new()
            .param("n", 41)
            .param_string("s", tricky)
            .param("ids", "[1,2,3]")
            .param("names", "['a','b\\'c']")
            .param("d", "2024-02-29"),
    );
    let row: Params = bounded(
        "params",
        with.query_one(
            "SELECT {n:UInt32} + 1 AS n, {s:String} AS s, {ids:Array(UInt8)} AS ids, \
             toString({d:Date}) AS d, {names:Array(String)} AS names",
        ),
    )
    .await
    .unwrap();
    assert_eq!(row.n, 42);
    assert_eq!(row.s, tricky);
    assert_eq!(row.ids, vec![1, 2, 3]);
    assert_eq!(row.d, "2024-02-29");
    assert_eq!(row.names, vec!["a".to_string(), "b'c".to_string()]);

    // Parameters are values, never SQL: no injection.
    let injected = client.with_options(QueryOptions::new().param_string("s", "x' OR 1=1 --"));
    assert_eq!(
        text(&injected, "SELECT {s:String} AS v").await,
        "x' OR 1=1 --"
    );

    // A missing parameter is a server error.
    assert!(
        bounded("missing", client.execute("SELECT {absent:UInt8}"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn parameters_in_inserts() {
    #[derive(nativeclick::Row, Debug)]
    struct Item {
        id: u32,
        name: String,
    }

    let client = super::get_client().await;
    super::prepare_table("test_options_params", "id UInt32, name String", &client).await;
    let with = client.with_options(QueryOptions::new().param("id", 7).param("name", "seven"));
    bounded(
        "insert select",
        with.execute("INSERT INTO test_options_params SELECT {id:UInt32}, {name:String}"),
    )
    .await
    .unwrap();
    let rows: Vec<Item> = bounded(
        "select",
        client.query_collect("SELECT id, name FROM test_options_params"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].id, rows[0].name.as_str()), (7, "seven"));
}

#[tokio::test]
async fn query_id_reaches_the_server() {
    let client = super::get_client().await;
    let id = Uuid::new_v4();
    let with = client.with_options(QueryOptions::new().query_id(id));
    assert_eq!(text(&with, "SELECT queryID() AS v").await, id.to_string());
}

#[tokio::test]
async fn timeout_fails_the_query_and_cancels_it_on_the_server() {
    let client = super::get_client().await;
    let id = Uuid::new_v4();
    let slow = client.with_options(
        QueryOptions::new()
            .query_id(id)
            .setting("max_block_size", 1)
            .timeout(Duration::from_millis(500)),
    );
    let started = Instant::now();
    let error = bounded(
        "slow",
        slow.query_collect::<RawRow>("SELECT sleepEachRow(0.1) FROM numbers(300)"),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, NativeclickError::Timeout), "{error:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // The connection is usable right away: the server stopped the query.
    let started = Instant::now();
    assert_eq!(text(&client, "SELECT 'after' AS v").await, "after");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        text(
            &client,
            &format!("SELECT toString(count()) AS v FROM system.processes WHERE query_id = '{id}'")
        )
        .await,
        "0"
    );
}

#[tokio::test]
async fn fast_queries_are_not_affected_by_a_timeout() {
    let client = super::get_client().await;
    let with = client.with_options(QueryOptions::new().timeout(Duration::from_secs(10)));
    for _ in 0..20 {
        assert_eq!(text(&with, "SELECT 'quick' AS v").await, "quick");
    }
}

#[tokio::test]
async fn dropping_a_stream_cancels_the_query() {
    let client = super::get_client().await;
    let id = Uuid::new_v4();
    let with = client.with_options(
        QueryOptions::new()
            .query_id(id)
            .setting("max_block_size", 1),
    );
    let mut stream = with
        .query::<RawRow, _>("SELECT sleepEachRow(0.1), number FROM numbers(1000)")
        .await
        .unwrap();
    // Read one row, then give up on the 100s query.
    bounded("first row", stream.next()).await.unwrap().unwrap();
    drop(stream);

    let started = Instant::now();
    assert_eq!(text(&client, "SELECT 'next' AS v").await, "next");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the dropped query kept the connection busy for {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn ping_works_alone_and_between_queries() {
    let client = super::get_client().await;
    bounded("ping", client.ping()).await.unwrap();
    let pings: Vec<_> = (0..10)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                client.ping().await?;
                client
                    .query_collect::<RawRow>("SELECT number FROM numbers(100)")
                    .await
            })
        })
        .collect();
    for ping in pings {
        assert_eq!(bounded("ping", ping).await.unwrap().unwrap().len(), 100);
    }
}

#[tokio::test]
async fn server_info_is_reported() {
    let client = super::get_client().await;
    let info = client.server_info();
    assert_eq!(info.name, "ClickHouse");
    #[derive(nativeclick::Row)]
    struct Version {
        v: String,
    }
    let version: Version = client.query_one("SELECT version() AS v").await.unwrap();
    assert!(
        version
            .v
            .starts_with(&format!("{}.{}.", info.version_major, info.version_minor)),
        "{} vs {info:?}",
        version.v
    );
    assert!(info.negotiated_revision <= info.revision);
    assert!(info.negotiated_revision >= 54459, "{info:?}");
    assert!(info.timezone.is_some());
}

#[tokio::test]
async fn progress_reports_server_time() {
    let client = super::get_client().await;
    let mut progress = client.subscribe_progress();
    bounded(
        "query",
        client.execute("SELECT count() FROM numbers(10000000) WHERE sleepEachRow(0) = 0"),
    )
    .await
    .unwrap();
    let mut total = nativeclick::Progress::default();
    while let Ok((_, update)) = progress.try_recv() {
        total += update;
    }
    assert!(total.read_rows > 0, "{total:?}");
    assert!(total.elapsed_ns.is_some(), "{total:?}");
}

#[tokio::test]
async fn connect_timeout() {
    let options = ClientOptions {
        connect_timeout: Some(Duration::from_millis(300)),
        ..Default::default()
    };
    let started = Instant::now();
    // Non-routable address: the TCP connect never completes.
    let error = Client::connect("10.255.255.1:9000", options)
        .await
        .unwrap_err();
    assert!(matches!(error, NativeclickError::Timeout), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn wrong_password_fails_at_connect() {
    let address =
        std::env::var("NATIVECLICK_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let options = ClientOptions {
        password: "definitely wrong".to_string(),
        connect_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let error = Client::connect(address, options).await.unwrap_err();
    assert!(
        matches!(error, NativeclickError::ServerException { .. }),
        "{error:?}"
    );
}

/// With `send_logs_level`, the server interleaves Log packets with the answer: they used to
/// panic the connection task.
#[tokio::test]
async fn server_logs_are_received() {
    let client = super::get_client().await;
    let with = client.with_options(QueryOptions::new().setting("send_logs_level", "trace"));
    for _ in 0..3 {
        let rows = bounded(
            "logs",
            with.query_collect::<RawRow>("SELECT number, toString(number) FROM numbers(10000)"),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 10000);
    }
    bounded("insert", with.execute("SELECT 1")).await.unwrap();
    assert!(!client.is_closed());
}
