//! GenericClient: one function body over a Client and over a Transaction.

use crate::connect;
use may_postgres::types::Type;
use may_postgres::GenericClient;

fn exercise<C: GenericClient>(client: &mut C, table: &str) {
    client
        .execute(
            &*format!("CREATE TEMPORARY TABLE {table} (id INT, name TEXT)"),
            &[],
        )
        .unwrap();
    let insert = client
        .prepare_typed(
            &format!("INSERT INTO {table} VALUES ($1, $2)"),
            &[Type::INT4, Type::TEXT],
        )
        .unwrap();
    assert_eq!(client.execute(&insert, &[&1i32, &"a"]).unwrap(), 1);
    assert_eq!(client.execute_raw(&insert, &[&2i32, &"b"]).unwrap(), 1);

    let select = client
        .prepare(&format!("SELECT name FROM {table} WHERE id = $1"))
        .unwrap();
    assert_eq!(client.query(&select, &[&1i32]).unwrap().len(), 1);
    assert_eq!(
        client
            .query_one(&select, &[&2i32])
            .unwrap()
            .get::<_, &str>(0),
        "b"
    );
    assert!(client.query_opt(&select, &[&3i32]).unwrap().is_none());
    let rows: Vec<_> = client
        .query_raw(&*format!("SELECT id FROM {table} ORDER BY id"), &[])
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);

    let inner = client.transaction().unwrap();
    inner
        .execute(&*format!("DELETE FROM {table}"), &[])
        .unwrap();
    inner.rollback().unwrap();
    assert_eq!(
        client
            .query_one(&*format!("SELECT count(*) FROM {table}"), &[])
            .unwrap()
            .get::<_, i64>(0),
        2
    );
}

#[test]
fn a_client_through_the_trait() {
    let mut client = connect("user=postgres");
    exercise(&mut client, "gc_client");
}

#[test]
fn a_transaction_through_the_trait() {
    let mut client = connect("user=postgres");
    let mut tx = client.transaction().unwrap();
    exercise(&mut tx, "gc_tx");
    tx.commit().unwrap();
}
