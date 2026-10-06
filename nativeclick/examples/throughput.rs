//! Read and write throughput against a local server.
//!
//! ```sh
//! NATIVECLICK_TEST_ADDR=127.0.0.1:9000 cargo run --release --example throughput
//! ```

use std::time::Instant;

use futures_util::StreamExt;
use nativeclick::{Client, ClientOptions, Row};

#[derive(Row, Clone)]
struct Item {
    id: u64,
    name: String,
    score: f64,
    tags: Vec<String>,
    flag: Option<u8>,
}

const ROWS: u64 = 2_000_000;
const QUERY: &str = "SELECT number AS id, concat('name-', toString(number)) AS name, \
    number / 3 AS score, [toString(number % 7), 'x'] AS tags, \
    if(number % 2 = 0, NULL, toUInt8(number % 200)) AS flag FROM numbers(2000000)";

fn report(what: &str, started: Instant, rows: u64) {
    let seconds = started.elapsed().as_secs_f64();
    println!(
        "{what:<28} {:>8.0} ms  {:>10.0} rows/s",
        seconds * 1000.0,
        rows as f64 / seconds
    );
}

#[tokio::main]
async fn main() -> nativeclick::Result<()> {
    let address =
        std::env::var("NATIVECLICK_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let client = Client::connect(address, ClientOptions::default()).await?;

    let rounds = std::env::var("THROUGHPUT_ROUNDS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(2);
    for _ in 0..rounds {
        // Blocks only: network, decompression and column decoding.
        let started = Instant::now();
        let mut blocks = client.query_raw(QUERY).await?;
        let mut rows = 0;
        while let Some(block) = blocks.next().await {
            rows += block?.rows;
        }
        assert_eq!(rows, ROWS);
        report("select blocks", started, rows);

        // Blocks then rows of a derived struct.
        let started = Instant::now();
        let mut stream = client.query::<Item, _>(QUERY).await?;
        let mut rows = 0;
        while let Some(row) = stream.next().await {
            row?;
            rows += 1;
        }
        assert_eq!(rows, ROWS);
        report("select rows (derive)", started, rows);
    }

    client
        .execute("CREATE TABLE IF NOT EXISTS throughput (id UInt64, name String, score Float64, tags Array(String), flag Nullable(UInt8)) ENGINE = Null")
        .await?;
    let items: Vec<Item> = (0..ROWS)
        .map(|i| Item {
            id: i,
            name: format!("name-{i}"),
            score: i as f64 / 3.0,
            tags: vec![(i % 7).to_string(), "x".into()],
            flag: (i % 2 == 1).then_some((i % 200) as u8),
        })
        .collect();
    let insert_rounds = std::env::var("THROUGHPUT_INSERT_ROUNDS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(1);
    for _ in 0..insert_rounds {
        let started = Instant::now();
        let batches = futures_util::stream::iter(
            items
                .chunks(100_000)
                .map(|x| x.to_vec())
                .collect::<Vec<_>>(),
        );
        client
            .insert_native("INSERT INTO throughput FORMAT Native", batches)
            .await?;
        report("insert rows (derive)", started, ROWS);
    }
    Ok(())
}
