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
