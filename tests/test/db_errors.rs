//! Errors the server reports, with the fields DbError exposes.

use crate::connect;
use may_postgres::error::{ErrorPosition, Severity, SqlState};

#[test]
fn a_unique_violation_names_its_table_and_constraint() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE uv (id INT CONSTRAINT uv_pkey PRIMARY KEY); INSERT INTO uv VALUES (1)")
        .unwrap();
    let e = client
        .execute("INSERT INTO uv VALUES (1)", &[])
        .map(drop)
        .expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::UNIQUE_VIOLATION));
    let db = e.as_db_error().unwrap();
    assert_eq!(db.severity(), "ERROR");
    assert_eq!(db.parsed_severity(), Some(Severity::Error));
    assert_eq!(db.table(), Some("uv"));
    assert_eq!(db.constraint(), Some("uv_pkey"));
    assert!(db.schema().unwrap().starts_with("pg_temp"));
    assert!(db.detail().unwrap().contains("(id)=(1)"));
    assert!(db.file().is_some() && db.line().is_some() && db.routine().is_some());
    assert!(e.to_string().starts_with("db error: ERROR: duplicate key"));
    assert!(e.to_string().contains("\nDETAIL: "));
}

#[test]
fn a_not_null_violation_names_its_column() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE nn (name TEXT NOT NULL)")
        .unwrap();
    let e = client
        .execute("INSERT INTO nn VALUES ($1)", &[&None::<&str>])
        .map(drop)
        .expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::NOT_NULL_VIOLATION));
    assert_eq!(e.as_db_error().unwrap().column(), Some("name"));
}

#[test]
fn a_domain_check_names_its_datatype() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE DOMAIN pg_temp.posint AS INT CHECK (VALUE > 0)")
        .unwrap();
    let e = client
        .query("SELECT (-1)::pg_temp.posint", &[])
        .map(drop)
        .expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::CHECK_VIOLATION));
    assert_eq!(e.as_db_error().unwrap().datatype(), Some("posint"));
}

#[test]
fn a_syntax_error_points_into_the_query() {
    let client = connect("user=postgres");
    let e = client
        .batch_execute("SELECT 1 FROMM nowhere")
        .map(drop)
        .expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::SYNTAX_ERROR));
    assert!(matches!(
        e.as_db_error().unwrap().position(),
        Some(ErrorPosition::Original(_))
    ));
}

#[test]
fn a_raised_exception_carries_detail_hint_and_context() {
    let client = connect("user=postgres");
    let e = client
        .batch_execute(
            "DO $$ BEGIN RAISE EXCEPTION 'custom failure' USING DETAIL = 'the detail', \
             HINT = 'the hint', ERRCODE = 'P0001'; END $$",
        )
        .map(drop)
        .expect_err("an error");
    let db = e.as_db_error().unwrap();
    assert_eq!(db.code(), &SqlState::RAISE_EXCEPTION);
    assert_eq!(db.message(), "custom failure");
    assert_eq!(db.detail(), Some("the detail"));
    assert_eq!(db.hint(), Some("the hint"));
    assert!(db.where_().unwrap().contains("PL/pgSQL"));
    assert!(db.to_string().ends_with("\nHINT: the hint"));
}

#[test]
fn an_error_in_an_internal_query_reports_its_position_there() {
    let client = connect("user=postgres");
    client
        .batch_execute(
            "CREATE FUNCTION pg_temp.broken() RETURNS INT AS $$ \
             BEGIN RETURN (SELECT no_such_column FROM pg_class LIMIT 1); END $$ LANGUAGE plpgsql",
        )
        .unwrap();
    let e = client
        .query("SELECT pg_temp.broken()", &[])
        .map(drop)
        .expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::UNDEFINED_COLUMN));
    match e.as_db_error().unwrap().position() {
        Some(ErrorPosition::Internal { position, query }) => {
            assert!(*position > 0);
            assert!(query.contains("no_such_column"));
        }
        other => panic!("expected an internal position, got {other:?}"),
    }
}

#[test]
fn notices_do_not_disturb_the_query() {
    let client = connect("user=postgres");
    client
        .batch_execute("SET client_min_messages = notice; DO $$ BEGIN RAISE NOTICE 'hello'; RAISE WARNING 'careful'; END $$")
        .unwrap();
    let row = client.query_one("SELECT 1::INT", &[]).unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[test]
fn errors_leave_the_connection_usable() {
    let client = connect("user=postgres");
    for _ in 0..3 {
        assert!(client.query("SELECT 1/0", &[]).is_err());
        assert!(client.prepare("SELECT nope FROM nowhere").is_err());
        assert_eq!(
            client
                .query_one("SELECT 2::INT", &[])
                .unwrap()
                .get::<_, i32>(0),
            2
        );
    }
}
