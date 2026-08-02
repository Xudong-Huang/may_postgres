use crate::connect;
use may_postgres::binary_copy::{BinaryCopyInWriter, BinaryCopyOutStream};
use may_postgres::types::Type;

#[test]
fn write_basic() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .unwrap();
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]);
    writer.write(&[&1i32, &"foobar"]).unwrap();
    writer.write(&[&2i32, &None::<&str>]).unwrap();
    writer.finish().unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, Option<&str>>(1), Some("foobar"));
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, Option<&str>>(1), None);
}

#[test]
fn write_many_rows() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .unwrap();
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]);

    for i in 0..10_000i32 {
        writer
            .write(&[&i, &format!("the value for {}", i)])
            .unwrap();
    }

    writer.finish().unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &str>(1), format!("the value for {}", i));
    }
}

#[test]
fn write_big_rows() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar BYTEA)")
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .unwrap();
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::BYTEA]);

    for i in 0..2i32 {
        writer.write(&[&i, &vec![i as u8; 128 * 1024]]).unwrap();
    }

    writer.finish().unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &[u8]>(1), &*vec![i as u8; 128 * 1024]);
    }
}

#[test]
fn read_basic() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "
            CREATE TEMPORARY TABLE foo (id INT, bar TEXT);
            INSERT INTO foo (id, bar) VALUES (1, 'foobar'), (2, NULL);
            ",
        )
        .unwrap();

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].get::<i32>(0), 1);
    assert_eq!(rows[0].get::<Option<&str>>(1), Some("foobar"));
    assert_eq!(rows[1].get::<i32>(0), 2);
    assert_eq!(rows[1].get::<Option<&str>>(1), None);
}

#[test]
fn read_many_rows() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "
            CREATE TEMPORARY TABLE foo (id INT, bar TEXT);
            INSERT INTO foo (id, bar) SELECT i, 'the value for ' || i FROM generate_series(0, 9999) i;"
        )
        .unwrap();

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 10_000);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<i32>(0), i as i32);
        assert_eq!(row.get::<&str>(1), format!("the value for {}", i));
    }
}

#[test]
fn read_big_rows() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar BYTEA)")
        .unwrap();
    for i in 0..2i32 {
        client
            .execute(
                "INSERT INTO foo (id, bar) VALUES ($1, $2)",
                &[&i, &vec![i as u8; 128 * 1024]],
            )
            .unwrap();
    }

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::BYTEA])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<i32>(0), i as i32);
        assert_eq!(row.get::<&[u8]>(1), &vec![i as u8; 128 * 1024][..]);
    }
}

/// Run `f` on a worker thread, failing rather than hanging if it does not finish.
///
/// A deadlock in the connection loop would otherwise block the whole test binary
/// indefinitely, which in CI looks like an infrastructure problem rather than a
/// bug. Bounding it turns the hang into a legible failure.
fn with_deadline<F>(what: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    use std::sync::mpsc;
    use std::time::Duration;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });

    if rx.recv_timeout(Duration::from_secs(20)).is_err() {
        panic!("{what} did not complete within 20s — the connection loop is deadlocked");
    }
}

/// Regression: a COPY IN must complete on an otherwise idle connection.
///
/// `process_req` used to block in `rcv.recv()` once the copy channel drained,
/// parking the only coroutine that reads the socket. The client was meanwhile
/// waiting for `BindComplete`, which that coroutine had to decode — a circular
/// wait between the two halves of one connection.
///
/// The existing `write_basic` test did not catch this: it passes when a busy
/// test suite happens to generate the concurrent activity that unsticks the
/// loop. Isolation is the case that matters, so this test creates its own
/// connection and does nothing else.
#[test]
fn copy_in_completes_without_concurrent_activity() {
    with_deadline("COPY IN on an idle connection", || {
        let client = crate::connect("user=postgres");
        client
            .batch_execute("CREATE TEMPORARY TABLE idle_copy (id INT, bar TEXT)")
            .unwrap();

        let sink = client
            .copy_in("COPY idle_copy (id, bar) FROM STDIN BINARY")
            .unwrap();
        let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]);
        writer.write(&[&1i32, &"alpha"]).unwrap();
        writer.write(&[&2i32, &"beta"]).unwrap();
        let rows_written = writer.finish().unwrap();

        assert_eq!(rows_written, 2, "COPY should report the rows it inserted");

        let rows = client
            .query("SELECT id, bar FROM idle_copy ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
        assert_eq!(rows[0].get::<_, &str>(1), "alpha");
    });
}

/// The connection must stay usable for ordinary queries after a copy finishes.
///
/// If the copy left the loop in a partial state, the next request would hang
/// rather than fail — the same deadlock one step later.
#[test]
fn connection_is_reusable_after_a_copy() {
    with_deadline("query after COPY IN", || {
        let client = crate::connect("user=postgres");
        client
            .batch_execute("CREATE TEMPORARY TABLE reuse_copy (id INT)")
            .unwrap();

        let sink = client
            .copy_in("COPY reuse_copy (id) FROM STDIN BINARY")
            .unwrap();
        let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4]);
        writer.write(&[&7i32]).unwrap();
        writer.finish().unwrap();

        // Several round trips, to catch a loop that services one more request
        // and then stalls.
        for expected in 1..=3 {
            let row = client.query_one("SELECT $1::INT", &[&expected]).unwrap();
            assert_eq!(row.get::<_, i32>(0), expected);
        }

        let row = client
            .query_one("SELECT count(*) FROM reuse_copy", &[])
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
    });
}

/// An empty copy must still complete rather than block waiting for data.
///
/// This is the boundary case of the deadlock: the channel is empty from the
/// outset, so a loop that blocks when it finds nothing to write never returns.
#[test]
fn an_empty_copy_completes() {
    with_deadline("empty COPY IN", || {
        let client = crate::connect("user=postgres");
        client
            .batch_execute("CREATE TEMPORARY TABLE empty_copy (id INT)")
            .unwrap();

        let sink = client
            .copy_in("COPY empty_copy (id) FROM STDIN BINARY")
            .unwrap();
        let writer = BinaryCopyInWriter::new(sink, &[Type::INT4]);
        let rows_written = {
            let mut w = writer;
            w.finish().unwrap()
        };

        assert_eq!(rows_written, 0, "an empty copy inserts nothing");
    });
}

/// A copy that is abandoned without `finish()` must not wedge the connection.
///
/// Dropping the sink closes the channel, which the receiver reports as an
/// error; the loop has to treat that as "copy over" and carry on rather than
/// waiting for data that will never arrive.
#[test]
fn abandoning_a_copy_does_not_wedge_the_connection() {
    with_deadline("abandoned COPY IN", || {
        let client = crate::connect("user=postgres");
        client
            .batch_execute("CREATE TEMPORARY TABLE abandoned_copy (id INT)")
            .unwrap();

        {
            let sink = client
                .copy_in("COPY abandoned_copy (id) FROM STDIN BINARY")
                .unwrap();
            let mut writer = BinaryCopyInWriter::new(sink, &[Type::INT4]);
            writer.write(&[&1i32]).unwrap();
            // Deliberately no finish() — the sink is dropped here.
        }

        // The copy is aborted, so the row must not be visible, and crucially
        // the connection must still answer.
        let row = client
            .query_one("SELECT count(*) FROM abandoned_copy", &[])
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0, "an aborted copy inserts nothing");
    });
}
