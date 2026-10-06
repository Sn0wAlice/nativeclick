//! Types added for recent ClickHouse versions, and the column formats of recent protocol
//! revisions (sparse, replicated, size-stream strings), checked against a real server.

use std::time::Duration;

use chrono::{NaiveDate, TimeDelta};
use nativeclick::{Client, Date32, DynamicValue, IndexMap, QueryOptions, RawRow, Type, Value};

async fn bounded<T>(what: &str, future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .unwrap_or_else(|_| panic!("{what}: timed out"))
}

/// Server-side text of each row of `query` (one String column named `v`).
async fn texts(client: &Client, query: &str) -> Vec<String> {
    #[derive(nativeclick::Row)]
    struct Text {
        v: String,
    }
    bounded(query, client.query_collect::<Text>(query))
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .into_iter()
        .map(|x| x.v)
        .collect()
}

fn dynamic(type_: &str, value: Value) -> Value {
    Value::Dynamic(Box::new(DynamicValue::new(type_.parse().unwrap(), value)))
}

#[tokio::test]
async fn null_and_empty_array_literals() {
    #[derive(nativeclick::Row, Debug)]
    struct Literals {
        n: Option<u8>,
        a: Vec<u8>,
        na: Vec<Option<String>>,
    }
    let client = super::get_client().await;
    let row: Literals = bounded(
        "literals",
        client.query_one("SELECT NULL AS n, [] AS a, [NULL] AS na"),
    )
    .await
    .unwrap();
    assert_eq!(row.n, None);
    assert!(row.a.is_empty());
    assert_eq!(row.na, vec![None]);

    let raw: Vec<RawRow> = bounded("raw", client.query_collect("SELECT NULL AS x"))
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
}

#[tokio::test]
async fn date32() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u8,
        d: NaiveDate,
        raw: Date32,
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_date32",
        "id UInt8, d Date32, raw Date32",
        &client,
    )
    .await;
    let dates = [(1900, 1, 1), (1969, 12, 31), (2024, 2, 29), (2299, 12, 31)];
    let rows: Vec<Row> = dates
        .iter()
        .enumerate()
        .map(|(id, (y, m, d))| {
            let date = NaiveDate::from_ymd_opt(*y, *m, *d).unwrap();
            Row {
                id: id as u8,
                d: date,
                raw: date.into(),
            }
        })
        .collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_date32 FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        texts(
            &client,
            "SELECT toString(d) AS v FROM test_types_date32 ORDER BY id"
        )
        .await,
        ["1900-01-01", "1969-12-31", "2024-02-29", "2299-12-31"]
    );
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_date32 ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
}

#[tokio::test]
async fn bool_columns() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Flags {
        id: u8,
        b: bool,
        nb: Option<bool>,
    }
    #[derive(nativeclick::Row, Debug, PartialEq)]
    struct Legacy {
        b: u8,
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_bool",
        "id UInt8, b Bool, nb Nullable(Bool)",
        &client,
    )
    .await;
    let rows = vec![
        Flags {
            id: 0,
            b: true,
            nb: None,
        },
        Flags {
            id: 1,
            b: false,
            nb: Some(true),
        },
    ];
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_bool FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Flags> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_bool ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(b) AS v FROM test_types_bool ORDER BY id"
        )
        .await,
        ["true", "false"]
    );
    // Code written when Bool was read as UInt8 keeps working.
    let legacy: Vec<Legacy> = bounded(
        "legacy",
        client.query_collect("SELECT b FROM test_types_bool ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(legacy, [Legacy { b: 1 }, Legacy { b: 0 }]);
}

#[tokio::test]
async fn time_and_time64() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Times {
        id: u8,
        t: TimeDelta,
        t3: TimeDelta,
    }
    let client = super::get_client().await;
    // Experimental before 26.x.
    let client = client.with_options(QueryOptions::new().setting("enable_time_time64_type", 1));
    super::prepare_table("test_types_time", "id UInt8, t Time, t3 Time64(3)", &client).await;
    let rows = vec![
        Times {
            id: 0,
            t: TimeDelta::seconds(3600 + 2),
            t3: TimeDelta::milliseconds(1500),
        },
        Times {
            id: 1,
            t: TimeDelta::seconds(-30),
            t3: TimeDelta::seconds(100 * 3600) + TimeDelta::milliseconds(7),
        },
    ];
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_time FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Times> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_time ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(t3) AS v FROM test_types_time ORDER BY id"
        )
        .await,
        ["00:00:01.500", "100:00:00.007"]
    );
}

#[tokio::test]
async fn intervals() {
    #[derive(nativeclick::Row, Debug)]
    struct Intervals {
        d: i64,
        s: i64,
        q: i64,
    }
    let client = super::get_client().await;
    let row: Intervals = bounded(
        "intervals",
        client.query_one(
            "SELECT toIntervalDay(3) AS d, toIntervalSecond(-5) AS s, toIntervalQuarter(2) AS q",
        ),
    )
    .await
    .unwrap();
    assert_eq!((row.d, row.s, row.q), (3, -5, 2));
}

#[tokio::test]
async fn named_tuples() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u8,
        t: (u8, String),
        nested: (u8, (String, Vec<u32>)),
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_named_tuple",
        "id UInt8, t Tuple(a UInt8, `b c` String), nested Tuple(x UInt8, y Tuple(`select` String, z Array(UInt32)))",
        &client,
    )
    .await;
    let rows = vec![Row {
        id: 1,
        t: (7, "seven".into()),
        nested: (1, ("in".into(), vec![1, 2])),
    }];
    bounded(
        "insert",
        client.insert_native_block(
            "INSERT INTO test_types_named_tuple FORMAT Native",
            rows.clone(),
        ),
    )
    .await
    .unwrap();
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_named_tuple"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(&client, "SELECT t.`b c` AS v FROM test_types_named_tuple").await,
        ["seven"]
    );
}

#[tokio::test]
async fn enums_with_special_names() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u8,
        e: i8,
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_enum",
        "id UInt8, e Enum8('a,b' = 1, 'it\\'s' = 2, '(x)' = 3, 'back\\\\slash' = 4)",
        &client,
    )
    .await;
    let rows: Vec<Row> = (1..=4).map(|e| Row { id: e as u8, e }).collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_enum FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_enum ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(e) AS v FROM test_types_enum ORDER BY id"
        )
        .await,
        ["a,b", "it's", "(x)", "back\\slash"]
    );
    // By name, both ways.
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Named {
        id: u8,
        e: String,
    }
    super::prepare_table(
        "test_types_enum_named",
        "id UInt8, e Enum8('a,b' = 1, 'it\\'s' = 2)",
        &client,
    )
    .await;
    let named = vec![
        Named {
            id: 0,
            e: "it's".into(),
        },
        Named {
            id: 1,
            e: "a,b".into(),
        },
    ];
    bounded(
        "insert named",
        client.insert_native_block(
            "INSERT INTO test_types_enum_named FORMAT Native",
            named.clone(),
        ),
    )
    .await
    .unwrap();
    let back: Vec<Named> = bounded(
        "named",
        client.query_collect("SELECT * FROM test_types_enum_named ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, named);
    let unknown = client
        .insert_native_block(
            "INSERT INTO test_types_enum_named FORMAT Native",
            vec![Named {
                id: 2,
                e: "nope".into(),
            }],
        )
        .await
        .unwrap_err();
    assert!(unknown.to_string().contains("nope"), "{unknown}");

    // The type round-trips through our parser with the names unescaped.
    let raw: Vec<RawRow> = bounded("raw", client.query_collect("SELECT e FROM test_types_enum"))
        .await
        .unwrap();
    assert_eq!(raw.len(), 4);
}

#[tokio::test]
async fn geo_types() {
    let client = super::get_client().await;
    let raw: Vec<RawRow> = bounded(
        "geo",
        client.query_collect(
            "SELECT [(0., 0.), (1., 1.)]::LineString AS l, [[(0., 0.)], [(2., 2.)]]::MultiLineString AS ml",
        ),
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 1);
    // MultiPoint exists since ClickHouse 26.8.
    let info = client.server_info();
    if (info.version_major, info.version_minor) >= (26, 8) {
        let raw: Vec<RawRow> = bounded(
            "multipoint",
            client.query_collect("SELECT [(3., 4.)]::MultiPoint AS mp"),
        )
        .await
        .unwrap();
        assert_eq!(raw.len(), 1);
    }

    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Geo {
        id: u8,
        l: nativeclick::Ring,
        g: Value,
    }
    let ring = nativeclick::Ring(vec![
        nativeclick::Point([1.0, 2.0]),
        nativeclick::Point([3.0, 4.0]),
    ]);
    let geometry_client = client.with_options(
        QueryOptions::new()
            .setting("allow_suspicious_variant_types", 1)
            .setting("allow_experimental_geo_types", 1),
    );
    super::prepare_table(
        "test_types_geo",
        "id UInt8, l LineString, g Geometry",
        &geometry_client,
    )
    .await;
    let rows = vec![
        Geo {
            id: 0,
            l: ring.clone(),
            g: dynamic("Point", Value::Point(nativeclick::Point([5.0, 6.0]))),
        },
        Geo {
            id: 1,
            l: ring.clone(),
            g: dynamic("LineString", Value::Ring(ring.clone())),
        },
        Geo {
            id: 2,
            l: ring.clone(),
            g: Value::Null,
        },
    ];
    bounded(
        "insert",
        geometry_client
            .insert_native_block("INSERT INTO test_types_geo FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Geo> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_geo ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(variantType(g)) AS v FROM test_types_geo ORDER BY id"
        )
        .await,
        ["Point", "LineString", "None"]
    );
}

#[tokio::test]
async fn variant() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u8,
        v: Value,
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_variant",
        "id UInt8, v Variant(String, UInt64, Array(UInt8))",
        &client,
    )
    .await;
    let rows = vec![
        Row {
            id: 0,
            v: dynamic("UInt64", Value::UInt64(42)),
        },
        Row {
            id: 1,
            v: dynamic("String", Value::string("abc")),
        },
        Row {
            id: 2,
            v: Value::Null,
        },
        Row {
            id: 3,
            v: dynamic(
                "Array(UInt8)",
                Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
            ),
        },
    ];
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_variant FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_variant ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(variantType(v)) AS v FROM test_types_variant ORDER BY id"
        )
        .await,
        ["UInt64", "String", "None", "Array(UInt8)"]
    );

    // Rows convert to Rust types through their actual type.
    #[derive(nativeclick::Row, Debug)]
    struct Typed {
        v: Option<String>,
    }
    let strings: Vec<Typed> = bounded(
        "typed",
        client.query_collect("SELECT v FROM test_types_variant WHERE id IN (1, 2) ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(strings[0].v.as_deref(), Some("abc"));
    assert_eq!(strings[1].v, None);
}

#[tokio::test]
async fn dynamic_columns() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u8,
        d: Value,
    }
    let client = super::get_client().await;
    super::prepare_table("test_types_dynamic", "id UInt8, d Dynamic", &client).await;
    let mut rows = vec![
        Row {
            id: 0,
            d: dynamic("Int32", Value::Int32(-7)),
        },
        Row {
            id: 1,
            d: dynamic("String", Value::string("text")),
        },
        Row {
            id: 2,
            d: Value::Null,
        },
        Row {
            id: 3,
            d: dynamic(
                "Array(Nullable(String))",
                Value::Array(vec![Value::Null, Value::string("x")]),
            ),
        },
        Row {
            id: 4,
            d: dynamic(
                "Map(String, UInt64)",
                Value::Map(vec![Value::string("k")], vec![Value::UInt64(9)]),
            ),
        },
    ];
    // More distinct types than a small max_types would hold.
    for i in 0..40u8 {
        rows.push(Row {
            id: 10 + i,
            d: dynamic(&format!("FixedString({})", i + 1), Value::string("f")),
        });
    }
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_dynamic FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_dynamic ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
    assert_eq!(
        texts(
            &client,
            "SELECT toString(dynamicType(d)) AS v FROM test_types_dynamic WHERE id < 5 ORDER BY id"
        )
        .await,
        [
            "Int32",
            "String",
            "None",
            "Array(Nullable(String))",
            "Map(String, UInt64)"
        ]
    );
    // Values computed by the server.
    let raw: Vec<RawRow> = bounded(
        "literals",
        client.query_collect("SELECT 1::Dynamic AS a, [1, 2]::Dynamic AS b, NULL::Dynamic AS c, [1::Dynamic, 'x'::Dynamic] AS d"),
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 1);
}

#[tokio::test]
async fn json_columns() {
    #[derive(nativeclick::Row, Debug, Clone)]
    struct Row {
        id: u8,
        j: String,
    }
    let client = super::get_client().await;
    super::prepare_table("test_types_json", "id UInt8, j JSON", &client).await;
    let rows = vec![
        Row {
            id: 0,
            j: r#"{"a":1,"b":{"c":"x"}}"#.to_string(),
        },
        Row {
            id: 1,
            j: r#"{"arr":[1,2,3],"s":"it's \"quoted\""}"#.to_string(),
        },
        Row {
            id: 2,
            j: "{}".to_string(),
        },
    ];
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_json FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        texts(
            &client,
            "SELECT toString(j.b.c) AS v FROM test_types_json WHERE id = 0"
        )
        .await,
        ["x"]
    );
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_json ORDER BY id"),
    )
    .await
    .unwrap();
    for (row, original) in back.iter().zip(&rows) {
        let read: serde_json::Value = serde_json::from_str(&row.j).unwrap();
        let written: serde_json::Value = serde_json::from_str(&original.j).unwrap();
        assert_eq!(read, written, "row {}", row.id);
    }

    // Through the serde helper too.
    #[derive(nativeclick::Row, Debug)]
    struct Typed {
        j: nativeclick::Json<serde_json::Value>,
    }
    let typed: Vec<Typed> = bounded(
        "typed",
        client.query_collect("SELECT j FROM test_types_json WHERE id = 0"),
    )
    .await
    .unwrap();
    assert_eq!(typed[0].j.0["b"]["c"], "x");
}

#[tokio::test]
async fn simple_aggregate_function() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        k: u8,
        total: u64,
        last: Option<String>,
    }
    let client = super::get_client().await;
    client
        .execute("DROP TABLE IF EXISTS test_types_saf")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_types_saf (k UInt8, total SimpleAggregateFunction(sum, UInt64), \
             last SimpleAggregateFunction(anyLast, Nullable(String))) ENGINE = AggregatingMergeTree ORDER BY k",
        )
        .await
        .unwrap();
    for (total, last) in [(1, "a"), (2, "b")] {
        bounded(
            "insert",
            client.insert_native_block(
                "INSERT INTO test_types_saf FORMAT Native",
                vec![Row {
                    k: 1,
                    total,
                    last: Some(last.into()),
                }],
            ),
        )
        .await
        .unwrap();
    }
    client
        .execute("OPTIMIZE TABLE test_types_saf FINAL")
        .await
        .unwrap();
    let rows: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_saf"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows,
        [Row {
            k: 1,
            total: 3,
            last: Some("b".into())
        }]
    );
}

#[tokio::test]
async fn sparse_columns() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u32,
        n: u64,
        s: String,
        ns: Option<String>,
        t: (u32, String),
        f: f64,
    }
    let client = super::get_client().await;
    client
        .execute("DROP TABLE IF EXISTS test_types_sparse")
        .await
        .unwrap();
    let columns = "id UInt32, n UInt64, s String, ns Nullable(String), t Tuple(a UInt32, b String), f Float64";
    let create = format!(
        "CREATE TABLE test_types_sparse ({columns}) ENGINE = MergeTree ORDER BY id \
         SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5"
    );
    // Sparse Nullable columns need a table setting that older servers do not know.
    if client
        .execute(format!("{create}, nullable_serialization_version = 'allow_sparse'").as_str())
        .await
        .is_err()
    {
        let client = super::get_client().await;
        client.execute(create.as_str()).await.unwrap();
    }
    let client = super::get_client().await;
    let rows: Vec<Row> = (0..10_000u32)
        .map(|id| {
            let rare = id % 97 == 0;
            Row {
                id,
                n: if rare { id as u64 } else { 0 },
                s: if rare {
                    format!("s{id}")
                } else {
                    String::new()
                },
                ns: if rare {
                    Some(format!("n{id}"))
                } else if id % 89 == 0 {
                    Some(String::new())
                } else {
                    None
                },
                t: (
                    if rare { id } else { 0 },
                    if id % 83 == 0 {
                        "t".into()
                    } else {
                        String::new()
                    },
                ),
                f: if rare { -0.0 } else { 0.0 },
            }
        })
        .collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_sparse FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    client
        .execute("OPTIMIZE TABLE test_types_sparse FINAL")
        .await
        .unwrap();
    let kinds = texts(
        &client,
        "SELECT concat(column, ':', serialization_kind) AS v FROM system.parts_columns \
         WHERE table = 'test_types_sparse' AND active AND database = currentDatabase() ORDER BY column",
    )
    .await;
    assert!(
        kinds.iter().any(|x| x == "n:Sparse"),
        "column n should be stored sparse: {kinds:?}"
    );

    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_sparse ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back.len(), rows.len());
    for (read, written) in back.iter().zip(&rows) {
        assert_eq!(read.id, written.id);
        assert_eq!(
            (read.n, &read.s, &read.ns, &read.t),
            (written.n, &written.s, &written.ns, &written.t),
            "row {}",
            read.id
        );
        // Compared as numbers: servers up to 26.4 store -0.0 as a sparse default (+0.0).
        assert_eq!(read.f, written.f, "row {}", read.id);
    }
}

#[tokio::test]
async fn replicated_columns() {
    #[derive(nativeclick::Row, Debug, PartialEq)]
    struct Row {
        s: String,
        n: u8,
        t: (String, u64),
    }
    let client = super::get_client().await;
    let client =
        client.with_options(QueryOptions::new().setting("enable_lazy_columns_replication", 1));
    let rows: Vec<Row> = bounded(
        "array join",
        client.query_collect(
            "SELECT s, n, t FROM (SELECT concat('row', toString(number)) AS s, (s, number) AS t \
             FROM numbers(1000)) ARRAY JOIN [1, 2, 3] AS n ORDER BY s, n",
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 3000);
    let mut expected: Vec<(String, u64)> = (0..1000u64).map(|i| (format!("row{i}"), i)).collect();
    expected.sort();
    for (i, row) in rows.iter().enumerate() {
        let (s, number) = &expected[i / 3];
        assert_eq!(&row.s, s);
        assert_eq!(row.n as usize, i % 3 + 1);
        assert_eq!(&row.t, &(s.clone(), *number));
    }

    #[derive(nativeclick::Row, Debug)]
    struct Joined {
        k: u64,
        v: String,
    }
    let joined: Vec<Joined> = bounded(
        "join",
        client.query_collect(
            "SELECT l.k AS k, r.v AS v FROM (SELECT toUInt64(number % 10) AS k FROM numbers(5000)) AS l \
             INNER JOIN (SELECT number AS k, repeat('v', number) AS v FROM numbers(10)) AS r ON l.k = r.k \
             ORDER BY k",
        ),
    )
    .await
    .unwrap();
    assert_eq!(joined.len(), 5000);
    assert!(joined.iter().all(|x| x.v == "v".repeat(x.k as usize)));
}

#[tokio::test]
async fn strings_survive_every_layout() {
    #[derive(nativeclick::Row, Debug, PartialEq, Clone)]
    struct Row {
        id: u32,
        s: String,
        b: nativeclick::Bytes,
        a: Vec<String>,
        m: IndexMap<String, String>,
        lc: String,
        fs: String,
    }
    let client = super::get_client().await;
    super::prepare_table(
        "test_types_strings",
        "id UInt32, s String, b String, a Array(String), m Map(String, String), \
         lc LowCardinality(String), fs FixedString(4)",
        &client,
    )
    .await;
    let rows: Vec<Row> = (0..2000u32)
        .map(|id| Row {
            id,
            s: "é\u{0}".repeat(id as usize % 50),
            b: nativeclick::Bytes((0..id % 300).map(|x| x as u8).collect()),
            a: (0..id % 4)
                .map(|x| x.to_string().repeat(x as usize))
                .collect(),
            m: IndexMap::from_iter([(format!("k{id}"), "v".repeat(id as usize % 7))]),
            lc: format!("lc{}", id % 5),
            fs: "ab".to_string(),
        })
        .collect();
    bounded(
        "insert",
        client.insert_native_block("INSERT INTO test_types_strings FORMAT Native", rows.clone()),
    )
    .await
    .unwrap();
    let lengths = texts(
        &client,
        "SELECT toString(sum(length(s))) AS v FROM test_types_strings",
    )
    .await;
    assert_eq!(
        lengths[0],
        rows.iter().map(|x| x.s.len()).sum::<usize>().to_string()
    );
    let back: Vec<Row> = bounded(
        "select",
        client.query_collect("SELECT * FROM test_types_strings ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(back, rows);
}

#[tokio::test]
async fn type_names_from_the_server_parse() {
    let client = super::get_client().await;
    let raw: Vec<RawRow> = bounded(
        "types",
        client.query_collect(
            "SELECT toTypeName(x) AS name FROM (SELECT CAST(1, 'Dynamic(max_types=3)') AS x)",
        ),
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 1);
    let name: Type = "Dynamic(max_types=3)".parse().unwrap();
    assert_eq!(name.to_string(), "Dynamic(max_types=3)");
}
