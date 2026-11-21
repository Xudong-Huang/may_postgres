//! Edge paths against a real server: empty statements, misused copies, binary copy decoding
//! errors, asynchronous messages.

use crate::connect;
use bytes::Bytes;
use may_postgres::binary_copy::{BinaryCopyInWriter, BinaryCopyOutStream};
use may_postgres::error::SqlState;
use may_postgres::types::Type;
use may_postgres::SimpleQueryMessage;

#[test]
fn empty_statements_affect_no_rows() {
    let client = connect("user=postgres");
    assert_eq!(client.execute("", &[]).unwrap(), 0);

    let mut rows = client.query_raw("", &[]).unwrap();
    assert!(rows.next().is_none());
    assert_eq!(rows.rows_affected(), Some(0));

    let messages = client.simple_query("").unwrap();
    assert!(matches!(
        messages.as_slice(),
        [SimpleQueryMessage::CommandComplete(0)]
    ));
}

#[test]
fn notifications_are_queued() {
    let client = connect("user=postgres");
    client
        .batch_execute("LISTEN may_postgres_edges; NOTIFY may_postgres_edges, 'hello'")
        .unwrap();
    // The notification arrives with the batch's responses, before its ReadyForQuery.
    let notification = client.notifications().pop().expect("a notification");
    assert_eq!(notification.channel(), "may_postgres_edges");
    assert_eq!(notification.payload(), "hello");
    assert!(notification.process_id() > 0);
}

#[test]
fn parameter_changes_are_tracked() {
    let client = connect("user=postgres");
    client
        .batch_execute("SET application_name = 'may_postgres_edges'")
        .unwrap();
    let rows = client.simple_query("SHOW application_name").unwrap();
    match &rows[0] {
        SimpleQueryMessage::Row(row) => assert_eq!(row.get(0), Some("may_postgres_edges")),
        _ => panic!("expected a row"),
    }
}

#[test]
fn copy_out_of_a_query_that_does_not_copy() {
    let client = connect("user=postgres");
    assert!(client.copy_out("SELECT 1").is_err());
    for _ in 0..3 {
        assert_eq!(client.execute("SELECT 1", &[]).unwrap(), 1);
    }
}

#[test]
fn copy_in_mixes_small_and_large_chunks() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE chunks (id INT, body TEXT)")
        .unwrap();

    let mut sink = client.copy_in("COPY chunks FROM STDIN").unwrap();
    // A small chunk is buffered; a large one following it goes out chained to the buffer.
    sink.send(Bytes::from_static(b"1\tsmall\n")).unwrap();
    let mut large = format!("2\t{}\n", "x".repeat(8192));
    large.push_str("3\tafter\n");
    sink.send(Bytes::from(large)).unwrap();
    // A large chunk with nothing buffered goes out on its own.
    sink.send(Bytes::from(format!("4\t{}\n", "y".repeat(8192))))
        .unwrap();
    assert_eq!(sink.finish().unwrap(), 4);

    let rows = client
        .query("SELECT id, length(body) FROM chunks ORDER BY id", &[])
        .unwrap();
    let got: Vec<(i32, i32)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, 5), (2, 8192), (3, 5), (4, 8192)]);
}

#[test]
fn copy_in_reports_bad_data_on_finish() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE numbers (n INT)")
        .unwrap();
    let mut sink = client.copy_in("COPY numbers FROM STDIN").unwrap();
    sink.send(Bytes::from_static(b"not a number\n")).unwrap();
    let error = sink.finish().map(drop).expect_err("a copy error");
    assert_eq!(error.code(), Some(&SqlState::INVALID_TEXT_REPRESENTATION));

    // The connection is usable afterwards.
    assert_eq!(client.execute("SELECT 1", &[]).unwrap(), 1);
}

fn binary_rows(client: &may_postgres::Client) {
    client
        .batch_execute(
            "CREATE TEMPORARY TABLE pairs (id INT, label TEXT);
             INSERT INTO pairs VALUES (1, 'one'), (2, NULL);",
        )
        .unwrap();
}

#[test]
fn binary_copy_out_row_errors() {
    let client = connect("user=postgres");
    binary_rows(&client);
    let stream = client.copy_out("COPY pairs TO STDOUT BINARY").unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let row = &rows[0];
    assert_eq!(row.try_get::<i32>(0).unwrap(), 1);
    // out of range
    assert!(row.try_get::<i32>(2).is_err());
    // wrong Rust type for the column
    assert!(row.try_get::<String>(0).is_err());
    // NULL into a non-Option
    assert!(rows[1].try_get::<String>(1).is_err());
    assert_eq!(rows[1].try_get::<Option<String>>(1).unwrap(), None);
}

#[test]
#[should_panic]
fn binary_copy_out_get_panics_on_error() {
    let client = connect("user=postgres");
    binary_rows(&client);
    let stream = client.copy_out("COPY pairs TO STDOUT BINARY").unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let _: String = rows[0].get(0);
}

#[test]
fn binary_copy_out_with_the_wrong_column_count() {
    let client = connect("user=postgres");
    binary_rows(&client);
    let stream = client.copy_out("COPY pairs TO STDOUT BINARY").unwrap();
    let mut rows = BinaryCopyOutStream::new(stream, &[Type::INT4]);
    let error = rows
        .next()
        .unwrap()
        .map(drop)
        .expect_err("a column count error");
    assert!(
        error.to_string().contains("expected 1 values but got 2"),
        "{error}"
    );
}

#[test]
fn binary_copy_out_of_a_text_copy() {
    let client = connect("user=postgres");
    binary_rows(&client);
    // Text format is not the binary format: the header check fails.
    let stream = client.copy_out("COPY pairs TO STDOUT").unwrap();
    let mut rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT]);
    assert!(rows.next().unwrap().is_err());
}

#[test]
fn binary_copy_out_of_an_empty_table_ends() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE nothing (id INT)")
        .unwrap();
    let stream = client.copy_out("COPY nothing TO STDOUT BINARY").unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
#[should_panic(expected = "expected 2 values but got 1")]
fn binary_copy_in_with_the_wrong_value_count_panics() {
    let client = connect("user=postgres");
    binary_rows(&client);
    let sink = client
        .copy_in("COPY pairs (id, label) FROM STDIN BINARY")
        .unwrap();
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]);
    let _ = writer.write(&[&1i32]);
}

#[test]
fn binary_copy_in_with_a_value_of_the_wrong_type() {
    let client = connect("user=postgres");
    binary_rows(&client);
    let sink = client
        .copy_in("COPY pairs (id, label) FROM STDIN BINARY")
        .unwrap();
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]);
    assert!(writer.write(&[&"one", &"label"]).is_err());
}

#[test]
fn copy_in_buffers_small_chunks() {
    let client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE small (n INT)")
        .unwrap();
    let mut sink = client.copy_in("COPY small FROM STDIN").unwrap();
    // Small chunks are gathered into 4 KiB writes.
    for n in 0..2000 {
        sink.send(Bytes::from(format!("{n}\n"))).unwrap();
    }
    assert_eq!(sink.finish().unwrap(), 2000);
}

#[test]
fn type_lookups_are_cached_per_kind() {
    let client = connect("user=postgres");
    client
        .batch_execute(
            "CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok');
             CREATE TYPE pg_temp.size AS ENUM ('s', 'm');
             CREATE TYPE pg_temp.point2 AS (x INT, y INT);
             CREATE TYPE pg_temp.span AS (lo INT, hi INT);",
        )
        .unwrap();
    // The second enum and the second composite reuse the lookup statements prepared for the first.
    for query in [
        "SELECT 'ok'::pg_temp.mood",
        "SELECT 'm'::pg_temp.size",
        "SELECT ROW(1, 2)::pg_temp.point2",
        "SELECT ROW(1, 2)::pg_temp.span",
    ] {
        let statement = client.prepare(query).unwrap();
        assert_eq!(statement.columns().len(), 1);
    }
}
