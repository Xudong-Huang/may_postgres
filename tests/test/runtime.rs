// use may::{go, join};
// use may_postgres::error::SqlState;
use may_postgres::Client;
// use std::time::Duration;

fn connect(s: &str) -> Client {
    may_postgres::connect(s).unwrap()
}

fn smoke_test(s: &str) {
    let client = connect(s);

    let stmt = client.prepare("SELECT $1::INT").unwrap();
    let rows = client.query(&stmt, &[&1i32]).unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1i32);
}

#[test]
fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres")
}

#[test]
fn multiple_hosts_one_port() {
    smoke_test("host=foobar.invalid,localhost port=5433 user=postgres")
}

#[test]
fn multiple_hosts_multiple_ports() {
    smoke_test("host=foobar.invalid,localhost port=5432,5433 user=postgres")
}

#[test]
fn wrong_port_count() {
    may_postgres::connect("host=localhost port=5433,5433 user=postgres")
        .err()
        .unwrap();
}

#[test]
fn target_session_attrs_ok() {
    smoke_test("host=localhost port=5433 user=postgres target_session_attrs=read-write")
}

#[test]
fn target_session_attrs_err() {
    may_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write
         options='-c default_transaction_read_only=on'",
    )
    .err()
    .unwrap();
}

#[test]
fn cancel_query() {
    // let client = connect("host=localhost port=5433 user=postgres");

    // join! {
    //     {
    //         let ret = client.batch_execute("SELECT pg_sleep(100)");
    //         assert!(ret.is_err());
    //         let err = ret.err().unwrap();
    //         let err = err.code();
    //         assert_eq!(err, Some(&SqlState::QUERY_CANCELED));
    //     },
    //     {
    //         may::coroutine::sleep(Duration::from_millis(100));
    //         let ret = client.cancel_query();
    //         assert!(ret.is_ok());
    //     }
    // }
}
