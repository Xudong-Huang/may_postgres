//! Cancelling a running query.

use crate::{connect, connect_raw};
use may_postgres::error::SqlState;
use std::time::Duration;

fn assert_cancels(cancel: impl FnOnce(&may_postgres::Client)) {
    let client = may_postgres::connect("host=localhost port=5433 user=postgres").unwrap();
    let sleeper = client.clone();
    let query = may::go!(move || sleeper.batch_execute("SELECT pg_sleep(30)"));
    may::coroutine::sleep(Duration::from_millis(300));
    cancel(&client);
    let e = query.join().unwrap().map(drop).expect_err("an error");
    assert_eq!(e.code(), Some(&SqlState::QUERY_CANCELED), "{e}");
    assert_eq!(
        client
            .query_one("SELECT 1::INT", &[])
            .unwrap()
            .get::<_, i32>(0),
        1
    );
}

#[test]
fn client_cancel_query() {
    assert_cancels(|c| c.cancel_query().unwrap());
}

#[test]
fn cancel_token() {
    assert_cancels(|c| c.cancel_token().cancel_query().unwrap());
}

#[test]
fn cancel_token_over_a_raw_stream() {
    assert_cancels(|c| {
        let socket = may::net::TcpStream::connect("127.0.0.1:5433").unwrap();
        c.cancel_token().cancel_query_raw(socket).unwrap();
    });
}

#[test]
fn a_client_from_connect_raw_cannot_cancel_by_itself() {
    // connect_raw has no host to open the cancel connection to.
    let client = connect_raw("user=postgres").unwrap();
    let e = client.cancel_query().map(drop).expect_err("an error");
    assert!(e.to_string().contains("unknown host"), "{e}");
    assert!(client.cancel_token().cancel_query().is_err());
    let _ = connect("user=postgres").id();
}
