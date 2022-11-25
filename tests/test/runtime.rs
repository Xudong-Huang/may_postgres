use may::{go, join};
use may_postgres::error::SqlState;
use may_postgres::{Client, NoTls};
use std::time::Duration;

fn connect(s: &str) -> Client {
    may_postgres::connect(s, NoTls).unwrap()
}

fn smoke_test(s: &str) {
    let client = connect(s);

    let stmt = client.prepare("SELECT $1::INT").unwrap();
    let rows = client.query(&stmt, &[&1i32]).unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1i32);
}

#[test]
#[ignore] // FIXME doesn't work with our docker-based tests :(
fn unix_socket() {
    smoke_test("host=/var/run/postgresql port=5433 user=postgres");
}

#[test]
fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres");
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
    may_postgres::connect("host=localhost port=5433,5433 user=postgres", NoTls)
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
        NoTls,
    )
    .err()
    .unwrap();
}

// #[test]
// fn cancel_query() {
//     let client = connect("host=localhost port=5433 user=postgres");

//     let cancel_token = client.cancel_token();
//     let cancel = cancel_token.cancel_query(NoTls);
//     let cancel = std::thread::sleep(Duration::from_millis(100)).then(|()| cancel);

//     let sleep = client.batch_execute("SELECT pg_sleep(100)");

//     match join!(sleep, cancel) {
//         (Err(ref e), Ok(())) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
//         t => panic!("unexpected return: {:?}", t),
//     }
// }
