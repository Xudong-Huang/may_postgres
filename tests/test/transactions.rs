//! Transactions: nested savepoints, and every Transaction method.

use crate::connect;
use may_postgres::types::Type;
use may_postgres::{IsolationLevel, SimpleQueryMessage};

fn count(client: &may_postgres::Client, table: &str) -> i64 {
    client
        .query_one(&*format!("SELECT count(*) FROM {table}"), &[])
        .unwrap()
        .get(0)
}

#[test]
fn nested_transactions_are_savepoints() {
    let mut client = connect("user=postgres");
    client
        .batch_execute("CREATE TEMPORARY TABLE nest (id INT)")
        .unwrap();

    let mut outer = client.transaction().unwrap();
    outer.execute("INSERT INTO nest VALUES (1)", &[]).unwrap();
    {
        let mut inner = outer.transaction().unwrap();
        inner.execute("INSERT INTO nest VALUES (2)", &[]).unwrap();
        {
            let deeper = inner.transaction().unwrap();
            deeper.execute("INSERT INTO nest VALUES (3)", &[]).unwrap();
            // dropped: ROLLBACK TO sp2
        }
        inner.commit().unwrap(); // RELEASE sp1
    }
    {
        let inner = outer.transaction().unwrap();
        inner.execute("INSERT INTO nest VALUES (4)", &[]).unwrap();
        inner.rollback().unwrap(); // ROLLBACK TO sp1
    }
    outer.commit().unwrap();

    let ids: Vec<i32> = client
        .query("SELECT id FROM nest ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, [1, 2]);
}

#[test]
fn every_transaction_method() {
    let mut client = may_postgres::connect("host=localhost port=5433 user=postgres").unwrap();
    client
        .batch_execute("CREATE TEMPORARY TABLE txm (id INT, name TEXT)")
        .unwrap();
    let tx = client.transaction().unwrap();

    let insert = tx
        .prepare_typed("INSERT INTO txm VALUES ($1, $2)", &[Type::INT4, Type::TEXT])
        .unwrap();
    tx.execute(&insert, &[&1i32, &"a"]).unwrap();
    assert_eq!(tx.execute_raw(&insert, &[&2i32, &"b"]).unwrap(), 1);
    let select = tx.prepare("SELECT name FROM txm WHERE id = $1").unwrap();
    assert_eq!(tx.query(&select, &[&1i32]).unwrap().len(), 1);
    assert_eq!(
        tx.query_one(&select, &[&2i32]).unwrap().get::<_, &str>(0),
        "b"
    );
    assert!(tx.query_opt(&select, &[&9i32]).unwrap().is_none());
    assert_eq!(tx.query_raw(&select, &[&1i32]).unwrap().count(), 1);

    let all = tx.prepare("SELECT id FROM txm ORDER BY id").unwrap();
    let portal = tx.bind_raw(&all, &[]).unwrap();
    let first: Vec<_> = tx.query_portal_raw(&portal, 1).unwrap().collect();
    assert_eq!(first.len(), 1);

    let mut sink = tx.copy_in("COPY txm FROM STDIN").unwrap();
    sink.send(bytes::Bytes::from_static(b"3\tc\n")).unwrap();
    assert_eq!(sink.finish().unwrap(), 1);
    let copied: Vec<u8> = tx
        .copy_out("COPY (SELECT name FROM txm ORDER BY id) TO STDOUT")
        .unwrap()
        .map(|chunk| chunk.unwrap().to_vec())
        .collect::<Vec<_>>()
        .concat();
    assert_eq!(copied, b"a\nb\nc\n");

    let messages = tx.simple_query("SELECT count(*) FROM txm").unwrap();
    assert!(matches!(&messages[0], SimpleQueryMessage::Row(r) if r.get(0) == Some("3")));
    tx.batch_execute("DELETE FROM txm WHERE id = 3").unwrap();

    let _token = tx.cancel_token();
    tx.cancel_query().unwrap(); // nothing running: a no-op for the server
    tx.commit().unwrap();
    assert_eq!(count(&client, "txm"), 2);
}

#[test]
fn builder_combinations() {
    let mut client = connect("user=postgres");
    for level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ] {
        let tx = client
            .build_transaction()
            .isolation_level(level)
            .start()
            .unwrap();
        tx.commit().unwrap();
    }
    let tx = client
        .build_transaction()
        .read_only(false)
        .deferrable(false)
        .start()
        .unwrap();
    tx.rollback().unwrap();
    let tx = client.build_transaction().deferrable(true).start().unwrap();
    tx.commit().unwrap();

    let tx = client.build_transaction().read_only(true).start().unwrap();
    let e = tx.batch_execute("CREATE TEMPORARY TABLE ro (id INT); INSERT INTO ro VALUES (1)");
    assert!(e.is_err(), "a read-only transaction refuses writes");
}
