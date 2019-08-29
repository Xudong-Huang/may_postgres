use futures::TryStreamExt;
use std::collections::HashMap;
use std::error::Error;
use std::f32;
use std::f64;
use std::fmt;
use std::net::IpAddr;
use std::result;
use std::time::{Duration, UNIX_EPOCH};
use tokio_postgres::to_sql_checked;
use tokio_postgres::types::{FromSql, FromSqlOwned, IsNull, Kind, ToSql, Type, WrongType};

use crate::connect;

#[cfg(feature = "with-bit-vec-0_7")]
mod bit_vec_07;
#[cfg(feature = "with-chrono-0_4")]
mod chrono_04;
#[cfg(feature = "with-eui48-0_4")]
mod eui48_04;
#[cfg(feature = "with-geo-0_10")]
mod geo_010;
#[cfg(feature = "with-serde_json-1")]
mod serde_json_1;
#[cfg(feature = "with-uuid-0_7")]
mod uuid_07;

async fn test_type<T, S>(sql_type: &str, checks: Vec<(T, S)>)
where
    T: PartialEq + for<'a> FromSqlOwned + ToSql,
    S: fmt::Display,
{
    let mut client = connect("user=postgres").await;

    for (val, repr) in checks {
        let stmt = client
            .prepare(&format!("SELECT {}::{}", repr, sql_type))
            .await
            .unwrap();
        let rows = client
            .query(&stmt, &[])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = rows[0].get(0);
        assert_eq!(val, result);

        let stmt = client
            .prepare(&format!("SELECT $1::{}", sql_type))
            .await
            .unwrap();
        let rows = client
            .query(&stmt, &[&val])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = rows[0].get(0);
        assert_eq!(val, result);
    }
}

#[tokio::test]
async fn test_bool_params() {
    test_type(
        "BOOL",
        vec![(Some(true), "'t'"), (Some(false), "'f'"), (None, "NULL")],
    )
    .await;
}

#[tokio::test]
async fn test_i8_params() {
    test_type("\"char\"", vec![(Some('a' as i8), "'a'"), (None, "NULL")]).await;
}

#[tokio::test]
async fn test_name_params() {
    test_type(
        "NAME",
        vec![
            (Some("hello world".to_owned()), "'hello world'"),
            (
                Some("イロハニホヘト チリヌルヲ".to_owned()),
                "'イロハニホヘト チリヌルヲ'",
            ),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_i16_params() {
    test_type(
        "SMALLINT",
        vec![
            (Some(15001i16), "15001"),
            (Some(-15001i16), "-15001"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_i32_params() {
    test_type(
        "INT",
        vec![
            (Some(2_147_483_548i32), "2147483548"),
            (Some(-2_147_483_548i32), "-2147483548"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_oid_params() {
    test_type(
        "OID",
        vec![
            (Some(2_147_483_548u32), "2147483548"),
            (Some(4_000_000_000), "4000000000"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_i64_params() {
    test_type(
        "BIGINT",
        vec![
            (Some(9_223_372_036_854_775_708i64), "9223372036854775708"),
            (Some(-9_223_372_036_854_775_708i64), "-9223372036854775708"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_f32_params() {
    test_type(
        "REAL",
        vec![
            (Some(f32::INFINITY), "'infinity'"),
            (Some(f32::NEG_INFINITY), "'-infinity'"),
            (Some(1000.55), "1000.55"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_f64_params() {
    test_type(
        "DOUBLE PRECISION",
        vec![
            (Some(f64::INFINITY), "'infinity'"),
            (Some(f64::NEG_INFINITY), "'-infinity'"),
            (Some(10000.55), "10000.55"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_varchar_params() {
    test_type(
        "VARCHAR",
        vec![
            (Some("hello world".to_owned()), "'hello world'"),
            (
                Some("イロハニホヘト チリヌルヲ".to_owned()),
                "'イロハニホヘト チリヌルヲ'",
            ),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_text_params() {
    test_type(
        "TEXT",
        vec![
            (Some("hello world".to_owned()), "'hello world'"),
            (
                Some("イロハニホヘト チリヌルヲ".to_owned()),
                "'イロハニホヘト チリヌルヲ'",
            ),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_borrowed_text() {
    let mut client = connect("user=postgres").await;

    let stmt = client.prepare("SELECT 'foo'").await.unwrap();
    let rows = client
        .query(&stmt, &[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let s: &str = rows[0].get(0);
    assert_eq!(s, "foo");
}

#[tokio::test]
async fn test_bpchar_params() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL PRIMARY KEY,
                b CHAR(5)
            )",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO foo (b) VALUES ($1), ($2), ($3)")
        .await
        .unwrap();
    client
        .execute(&stmt, &[&"12345", &"123", &None::<&'static str>])
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT b FROM foo ORDER BY id")
        .await
        .unwrap();
    let rows = client
        .query(&stmt, &[])
        .map_ok(|row| row.get(0))
        .try_collect::<Vec<Option<String>>>()
        .await
        .unwrap();

    assert_eq!(
        vec![Some("12345".to_owned()), Some("123  ".to_owned()), None],
        rows,
    );
}

#[tokio::test]
async fn test_citext_params() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL PRIMARY KEY,
                b CITEXT
            )",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO foo (b) VALUES ($1), ($2), ($3)")
        .await
        .unwrap();
    client
        .execute(&stmt, &[&"foobar", &"FooBar", &None::<&'static str>])
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT b FROM foo WHERE b = 'FOOBAR' ORDER BY id")
        .await
        .unwrap();
    let rows = client
        .query(&stmt, &[])
        .map_ok(|row| row.get(0))
        .try_collect::<Vec<String>>()
        .await
        .unwrap();

    assert_eq!(vec!["foobar".to_string(), "FooBar".to_string()], rows,);
}

#[tokio::test]
async fn test_bytea_params() {
    test_type(
        "BYTEA",
        vec![
            (Some(vec![0u8, 1, 2, 3, 254, 255]), "'\\x00010203feff'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_borrowed_bytea() {
    let mut client = connect("user=postgres").await;
    let stmt = client.prepare("SELECT 'foo'::BYTEA").await.unwrap();
    let rows = client
        .query(&stmt, &[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let s: &[u8] = rows[0].get(0);
    assert_eq!(s, b"foo");
}

macro_rules! make_map {
    ($($k:expr => $v:expr),+) => ({
        let mut map = HashMap::new();
        $(map.insert($k, $v);)+
        map
    })
}

#[tokio::test]
async fn test_hstore_params() {
    test_type(
        "hstore",
        vec![
            (
                Some(make_map!("a".to_owned() => Some("1".to_owned()))),
                "'a=>1'",
            ),
            (
                Some(make_map!("hello".to_owned() => Some("world!".to_owned()),
                               "hola".to_owned() => Some("mundo!".to_owned()),
                               "what".to_owned() => None)),
                "'hello=>world!,hola=>mundo!,what=>NULL'",
            ),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_array_params() {
    test_type(
        "integer[]",
        vec![
            (Some(vec![1i32, 2i32]), "ARRAY[1,2]"),
            (Some(vec![1i32]), "ARRAY[1]"),
            (Some(vec![]), "ARRAY[]"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[allow(clippy::eq_op)]
async fn test_nan_param<T>(sql_type: &str)
where
    T: PartialEq + ToSql + FromSqlOwned,
{
    let mut client = connect("user=postgres").await;

    let stmt = client
        .prepare(&format!("SELECT 'NaN'::{}", sql_type))
        .await
        .unwrap();
    let rows = client
        .query(&stmt, &[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let val: T = rows[0].get(0);
    assert!(val != val);
}

#[tokio::test]
async fn test_f32_nan_param() {
    test_nan_param::<f32>("REAL").await;
}

#[tokio::test]
async fn test_f64_nan_param() {
    test_nan_param::<f64>("DOUBLE PRECISION").await;
}

#[tokio::test]
async fn test_pg_database_datname() {
    let mut client = connect("user=postgres").await;
    let stmt = client
        .prepare("SELECT datname FROM pg_database")
        .await
        .unwrap();
    let rows = client
        .query(&stmt, &[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "postgres");
}

#[tokio::test]
async fn test_slice() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL PRIMARY KEY,
                f TEXT
            );
            INSERT INTO foo (f) VALUES ('a'), ('b'), ('c'), ('d');",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT f FROM foo WHERE id = ANY($1)")
        .await
        .unwrap();
    let rows = client
        .query(&stmt, &[&&[1i32, 3, 4][..]])
        .map_ok(|r| r.get(0))
        .try_collect::<Vec<String>>()
        .await
        .unwrap();

    assert_eq!(vec!["a".to_owned(), "c".to_owned(), "d".to_owned()], rows);
}

#[tokio::test]
async fn test_slice_wrong_type() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL PRIMARY KEY
            )",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT * FROM foo WHERE id = ANY($1)")
        .await
        .unwrap();
    let err = client
        .query(&stmt, &[&&[&"hi"][..]])
        .try_collect::<Vec<_>>()
        .await
        .err()
        .unwrap();
    match err.source() {
        Some(e) if e.is::<WrongType>() => {}
        _ => panic!("Unexpected error {:?}", err),
    };
}

#[tokio::test]
async fn test_slice_range() {
    let mut client = connect("user=postgres").await;

    let stmt = client.prepare("SELECT $1::INT8RANGE").await.unwrap();
    let err = client
        .query(&stmt, &[&&[&1i64][..]])
        .try_collect::<Vec<_>>()
        .await
        .err()
        .unwrap();
    match err.source() {
        Some(e) if e.is::<WrongType>() => {}
        _ => panic!("Unexpected error {:?}", err),
    };
}

#[tokio::test]
async fn domain() {
    #[derive(Debug, PartialEq)]
    struct SessionId(Vec<u8>);

    impl ToSql for SessionId {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut Vec<u8>,
        ) -> result::Result<IsNull, Box<dyn Error + Sync + Send>> {
            let inner = match *ty.kind() {
                Kind::Domain(ref inner) => inner,
                _ => unreachable!(),
            };
            self.0.to_sql(inner, out)
        }

        fn accepts(ty: &Type) -> bool {
            ty.name() == "session_id"
                && match *ty.kind() {
                    Kind::Domain(_) => true,
                    _ => false,
                }
        }

        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for SessionId {
        fn from_sql(ty: &Type, raw: &[u8]) -> result::Result<Self, Box<dyn Error + Sync + Send>> {
            Vec::<u8>::from_sql(ty, raw).map(SessionId)
        }

        fn accepts(ty: &Type) -> bool {
            // This is super weird!
            <Vec<u8> as FromSql>::accepts(ty)
        }
    }

    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "
            CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16);
            CREATE TABLE pg_temp.foo (id pg_temp.session_id);
            ",
        )
        .await
        .unwrap();

    let id = SessionId(b"0123456789abcdef".to_vec());

    let stmt = client
        .prepare("INSERT INTO pg_temp.foo (id) VALUES ($1)")
        .await
        .unwrap();
    client.execute(&stmt, &[&id]).await.unwrap();

    let stmt = client.prepare("SELECT id FROM pg_temp.foo").await.unwrap();
    let rows = client
        .query(&stmt, &[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(id, rows[0].get(0));
}

#[tokio::test]
async fn composite() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC
            )",
        )
        .await
        .unwrap();

    let stmt = client.prepare("SELECT $1::inventory_item").await.unwrap();
    let type_ = &stmt.params()[0];
    assert_eq!(type_.name(), "inventory_item");
    match *type_.kind() {
        Kind::Composite(ref fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::TEXT);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::INT4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::NUMERIC);
        }
        ref t => panic!("bad type {:?}", t),
    }
}

#[tokio::test]
async fn enum_() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .unwrap();

    let stmt = client.prepare("SELECT $1::mood").await.unwrap();
    let type_ = &stmt.params()[0];
    assert_eq!(type_.name(), "mood");
    match *type_.kind() {
        Kind::Enum(ref variants) => {
            assert_eq!(
                variants,
                &["sad".to_owned(), "ok".to_owned(), "happy".to_owned()]
            );
        }
        _ => panic!("bad type"),
    }
}

#[tokio::test]
async fn system_time() {
    test_type(
        "TIMESTAMP",
        vec![
            (
                Some(UNIX_EPOCH + Duration::from_millis(1_010)),
                "'1970-01-01 00:00:01.01'",
            ),
            (
                Some(UNIX_EPOCH - Duration::from_millis(1_010)),
                "'1969-12-31 23:59:58.99'",
            ),
            (
                Some(UNIX_EPOCH + Duration::from_millis(946_684_800 * 1000 + 1_010)),
                "'2000-01-01 00:00:01.01'",
            ),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn inet() {
    test_type(
        "INET",
        vec![
            (Some("127.0.0.1".parse::<IpAddr>().unwrap()), "'127.0.0.1'"),
            (
                Some("127.0.0.1".parse::<IpAddr>().unwrap()),
                "'127.0.0.1/32'",
            ),
            (
                Some(
                    "2001:4f8:3:ba:2e0:81ff:fe22:d1f1"
                        .parse::<IpAddr>()
                        .unwrap(),
                ),
                "'2001:4f8:3:ba:2e0:81ff:fe22:d1f1'",
            ),
            (
                Some(
                    "2001:4f8:3:ba:2e0:81ff:fe22:d1f1"
                        .parse::<IpAddr>()
                        .unwrap(),
                ),
                "'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128'",
            ),
        ],
    )
    .await;
}
