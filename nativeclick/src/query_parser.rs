//! Minimal SQL lexing for client-side `$N` arguments and for splitting statements: it only needs
//! to know where strings, quoted identifiers, comments and heredocs start and end.

use crate::Value;
use std::fmt::Write;

#[derive(Debug, PartialEq)]
enum Piece<'a> {
    /// Copied as is: SQL text, strings, identifiers, comments, heredocs.
    Text(&'a str),
    /// `$N`: the N-th client-side argument (the text is kept for out-of-range indexes).
    Argument(usize, &'a str),
    /// `$$`, written as a single `$`.
    EscapedDollar,
    Semicolon,
}

/// Length of a quoted section starting at `bytes[0]`, up to its unescaped closing quote (or the end).
fn quoted_len(bytes: &[u8]) -> usize {
    let quote = bytes[0];
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            c if c == quote => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Length of a `$tag$...$tag$` heredoc starting at `input[0]`, if it is one.
fn heredoc_len(input: &str) -> Option<usize> {
    let close = input[1..].find('$')? + 1;
    let tag = &input[..=close];
    if close == 1
        || !tag[1..close]
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return None;
    }
    let end = input[tag.len()..].find(tag)?;
    Some(tag.len() * 2 + end)
}

/// Splits `query` into pieces; concatenating their text gives the query back.
fn lex<'a>(query: &'a str) -> Vec<Piece<'a>> {
    let bytes = query.as_bytes();
    let mut pieces = vec![];
    let mut text_start = 0;
    let mut i = 0;
    let flush = |pieces: &mut Vec<Piece<'a>>, start: usize, end: usize| {
        if end > start {
            pieces.push(Piece::Text(&query[start..end]));
        }
    };
    while i < bytes.len() {
        let rest = &query[i..];
        let skip = match bytes[i] {
            b'\'' | b'"' | b'`' => quoted_len(&bytes[i..]),
            b'-' if rest.starts_with("--") => rest.find('\n').unwrap_or(rest.len()),
            b'#' => rest.find('\n').unwrap_or(rest.len()),
            b'/' if rest.starts_with("/*") => rest.find("*/").map(|x| x + 2).unwrap_or(rest.len()),
            b'$' => {
                if let Some(len) = heredoc_len(rest) {
                    len
                } else {
                    flush(&mut pieces, text_start, i);
                    if rest.starts_with("$$") {
                        pieces.push(Piece::EscapedDollar);
                        i += 2;
                    } else {
                        let digits = rest[1..].bytes().take_while(u8::is_ascii_digit).count();
                        match rest[1..=digits].parse() {
                            Ok(index) => pieces.push(Piece::Argument(index, &rest[..=digits])),
                            Err(_) => pieces.push(Piece::Text("$")),
                        }
                        i += digits + 1;
                    }
                    text_start = i;
                    continue;
                }
            }
            b';' => {
                flush(&mut pieces, text_start, i);
                pieces.push(Piece::Semicolon);
                i += 1;
                text_start = i;
                continue;
            }
            _ => 1,
        };
        i += skip;
    }
    flush(&mut pieces, text_start, bytes.len());
    pieces
}

/// Parses a query and replaces arguments with values
pub fn parse_query_arguments(query: &str, arguments: &[Value]) -> String {
    let mut out = String::with_capacity(query.len() + 100);
    for piece in lex(query) {
        match piece {
            Piece::Text(text) => out.push_str(text),
            Piece::Argument(index, _) if index > 0 && index <= arguments.len() => {
                write!(&mut out, "{}", arguments[index - 1]).unwrap()
            }
            Piece::Argument(_, text) => out.push_str(text),
            Piece::EscapedDollar => out.push('$'),
            Piece::Semicolon => out.push(';'),
        }
    }
    out
}

/// Splits a series of semicolon-delimited queries into individual queries
pub fn split_query_statements(query: &str) -> Vec<String> {
    let mut out = vec![String::new()];
    for piece in lex(query) {
        let current = out.last_mut().unwrap();
        match piece {
            Piece::Text(text) | Piece::Argument(_, text) => current.push_str(text),
            Piece::EscapedDollar => current.push('$'),
            Piece::Semicolon => {
                current.push(';');
                out.push(String::new());
            }
        }
    }
    out.into_iter()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tests() {
        let tests = &[
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * APPLY(x)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * REPLACE(y)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY db.a, db.b, db.c",
            "MODIFY COMMENT ''",
            "MODIFY COMMENT ''",
            "MODIFY COMMENT 'some comment value'",
            "MODIFY COMMENT 'some comment value'",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE `tbl`\n(PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db ENGINE=Foo TABLE OVERRIDE `tbl` (), TABLE OVERRIDE a (COLUMNS (_created DateTime MATERIALIZED now())), TABLE OVERRIDE b (PARTITION BY rand())",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE `tbl`,\nTABLE OVERRIDE `a`\n(\n    COLUMNS\n    (\n        `_created` DateTime MATERIALIZED now()\n    )\n),\nTABLE OVERRIDE `b`\n(\n    PARTITION BY rand()\n)",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE tbl (COLUMNS (id UUID) PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        `id` UUID\n    )\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db TABLE OVERRIDE tbl (COLUMNS (INDEX foo foo TYPE minmax GRANULARITY 1) PARTITION BY if(_staged = 1, 'staging', toYYYYMM(created)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        INDEX foo `foo` TYPE minmax GRANULARITY 1\n    )\n    PARTITION BY if(`_staged` = 1, 'staging', toYYYYMM(`created`))\n)",
            "CREATE DATABASE db TABLE OVERRIDE t1 (TTL inserted + INTERVAL 1 MONTH DELETE), TABLE OVERRIDE t2 (TTL `inserted` + INTERVAL 2 MONTH DELETE)",
            "CREATE DATABASE db\nTABLE OVERRIDE `t1`\n(\n    TTL `inserted` + toIntervalMonth(1)\n),\nTABLE OVERRIDE `t2`\n(\n    TTL `inserted` + toIntervalMonth(2)\n)",
            "CREATE DATABASE db ENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw') SETTINGS allows_query_when_mysql_lost = 1 TABLE OVERRIDE tab3 (COLUMNS (_staged UInt8 MATERIALIZED 1) PARTITION BY (c3) TTL c3 + INTERVAL 10 minute), TABLE OVERRIDE tab5 (PARTITION BY (c3) TTL c3 + INTERVAL 10 minute)",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw')\nSETTINGS allows_query_when_mysql_lost = 1\nTABLE OVERRIDE `tab3`\n(\n    COLUMNS\n    (\n        `_staged` UInt8 MATERIALIZED 1\n    )\n    PARTITION BY `c3`\n    TTL `c3` + toIntervalMinute(10)\n),\nTABLE OVERRIDE `tab5`\n(\n    PARTITION BY `c3`\n    TTL `c3` + toIntervalMinute(10)\n)",
            "CREATE DATABASE db TABLE OVERRIDE tbl (PARTITION BY toYYYYMM(created) COLUMNS (created DateTime CODEC(Delta)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        `created` DateTime CODEC(Delta)\n    )\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE `a`\n(\n    ORDER BY (`id`, `version`)\n)",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 COMMENT 'db comment' TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE `a`\n(\n    ORDER BY (`id`, `version`)\n)\nCOMMENT 'db comment'",
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '[A-Za-z0-9]{64}' SALT '[A-Za-z0-9]{64}'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "ALTER USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '[A-Za-z0-9]{64}' SALT '[A-Za-z0-9]{64}'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264'",
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264';",
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF'",
            "$HTEST$SS$HTEST$",
            "$2$3$$$",
            "$$2$3",
            "1 - 2 --sdfsdfsdf",
        ];

        // Without arguments, queries come back unchanged ($$ aside).
        for test in tests.iter().filter(|x| !x.contains("$$")) {
            assert_eq!(&parse_query_arguments(test, &[]), test);
        }
        assert_eq!(parse_query_arguments("$$2$3", &[]), "$2$3");
    }

    #[test]
    fn arg_tests() {
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $2",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = 3232"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $3",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = $3"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $1",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = 'te\\'st'"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $0",
                &[Value::string("te'st")]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = $0"
        );
    }

    #[test]
    fn split_tests() {
        assert_eq!(split_query_statements("X;B",), vec!["X;", "B"]);
        assert_eq!(split_query_statements("X;B;",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;B;\n",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;B;\n\n\n",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;\n\n\n",), vec!["X;"]);
        assert_eq!(split_query_statements("X\n\n\n",), vec!["X"]);
        assert_eq!(split_query_statements("",), Vec::<&str>::new());
    }

    #[test]
    fn arguments_are_not_replaced_in_quoted_text_or_comments() {
        let args = [Value::UInt32(7)];
        for (query, expected) in [
            (
                "SELECT '$1', `$1`, \"$1\", $1",
                "SELECT '$1', `$1`, \"$1\", 7",
            ),
            ("SELECT 'it\\'s $1', $1", "SELECT 'it\\'s $1', 7"),
            ("SELECT $1 -- $1\n, $1", "SELECT 7 -- $1\n, 7"),
            ("SELECT /* $1 */ $1 # $1", "SELECT /* $1 */ 7 # $1"),
            ("SELECT $tag$ $1 ; $tag$, $1", "SELECT $tag$ $1 ; $tag$, 7"),
            ("SELECT $1$1", "SELECT 77"),
            ("SELECT $", "SELECT $"),
            ("SELECT 'unterminated $1", "SELECT 'unterminated $1"),
        ] {
            assert_eq!(parse_query_arguments(query, &args), expected, "{query}");
        }
    }

    #[test]
    fn semicolons_in_quoted_text_do_not_split() {
        assert_eq!(
            split_query_statements("SELECT ';'; SELECT `a;b` -- ;\n; /* ; */ SELECT 1"),
            vec!["SELECT ';';", "SELECT `a;b` -- ;\n;", "/* ; */ SELECT 1"]
        );
    }
}
