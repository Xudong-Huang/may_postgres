//! Row, SimpleQueryRow, Statement and Column accessors, and their errors.

use crate::connect;
use may_postgres::types::Type;
use may_postgres::SimpleQueryMessage;

#[test]
fn row_accessors() {
    let client = connect("user=postgres");
    let row = client
        .query_one(
            "SELECT 1::INT AS one, 'x'::TEXT AS \"Name\", NULL::TEXT AS nothing",
            &[],
        )
        .unwrap();
    assert_eq!(row.len(), 3);
    assert!(!row.is_empty());
    let names: Vec<_> = row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["one", "Name", "nothing"]);
    assert_eq!(row.columns()[0].type_(), &Type::INT4);
    assert!(format!("{:?}", row.columns()[1]).contains("Name"));

    assert_eq!(row.get::<_, i32>("one"), 1);
    assert_eq!(
        row.get::<_, &str>("name"),
        "x",
        "names match case-insensitively"
    );
    assert_eq!(row.get::<_, Option<&str>>(&"nothing"), None);
    assert_eq!(row.get::<_, &str>(&1usize), "x");

    let missing = row
        .try_get::<_, i32>("absent")
        .map(drop)
        .expect_err("an error");
    assert!(missing.to_string().contains("invalid column `absent`"));
    assert!(row.try_get::<_, i32>(7usize).is_err());
    let wrong = row
        .try_get::<_, &str>(0usize)
        .map(drop)
        .expect_err("an error");
    assert!(wrong.to_string().contains("error deserializing column 0"));
    let null = row
        .try_get::<_, &str>(2usize)
        .map(drop)
        .expect_err("an error");
    assert!(null.to_string().contains("column 2"));

    let empty = client.query_one("SELECT", &[]).unwrap();
    assert!(empty.is_empty());
}

#[test]
#[should_panic(expected = "error retrieving column")]
fn row_get_panics_on_a_missing_column() {
    let client = connect("user=postgres");
    let row = client.query_one("SELECT 1::INT", &[]).unwrap();
    let _: i32 = row.get("absent");
}

#[test]
fn simple_query_rows() {
    let client = connect("user=postgres");
    let messages = client
        .simple_query(
            "SELECT 1 AS a, NULL AS b; SELECT; UPDATE pg_class SET relname = relname WHERE false",
        )
        .unwrap();
    let rows: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    let first = rows[0];
    assert_eq!(first.len(), 2);
    assert!(!first.is_empty());
    assert_eq!(first.get("a"), Some("1"));
    assert_eq!(first.get("A"), Some("1"));
    assert_eq!(first.try_get(1usize).unwrap(), None);
    assert!(first.try_get("zzz").is_err());
    assert!(rows[1].is_empty());
    let completions = messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
        .count();
    assert_eq!(completions, 3);
}

#[test]
#[should_panic(expected = "error retrieving column")]
fn simple_query_row_get_panics_on_a_missing_column() {
    let client = connect("user=postgres");
    let messages = client.simple_query("SELECT 1 AS a").unwrap();
    if let SimpleQueryMessage::Row(row) = &messages[0] {
        row.get("zzz");
    }
}

#[test]
fn statement_metadata() {
    let client = connect("user=postgres");
    let stmt = client
        .prepare("SELECT $1::INT8 AS big, $2::TEXT AS t")
        .unwrap();
    assert_eq!(stmt.params(), &[Type::INT8, Type::TEXT]);
    assert_eq!(stmt.columns().len(), 2);
    assert_eq!(stmt.columns()[1].name(), "t");
    let no_rows = client.prepare("SELECT 1 WHERE false").unwrap();
    assert!(client.query_opt(&no_rows, &[]).unwrap().is_none());
    let e = client
        .query_one(&no_rows, &[])
        .map(drop)
        .expect_err("an error");
    assert!(e.to_string().contains("unexpected number of rows"));
    let e = client
        .query_opt("SELECT generate_series(1, 2)", &[])
        .map(drop)
        .expect_err("an error");
    assert!(e.to_string().contains("unexpected number of rows"));
}
