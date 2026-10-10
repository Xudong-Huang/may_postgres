//! Regression tests for the pending-response starvation defect (2026-08-29
//! lifeguard pool worker wedge, PriceWhisperer /tickers outage).
//!
//! Mechanism under test: when a connection's I/O loop exits (error / EOF /
//! shutdown) every pending in-flight request must be FAILED — the caller's
//! `Responses::next` must return `Err`, never block forever — and subsequent
//! sends on the dead client must fail fast instead of queueing onto a channel
//! no coroutine will ever drain.
//!
//! The tests sever the TCP path abruptly through a local one-shot proxy, so
//! the server never gets a chance to send a clean ErrorResponse first (that
//! cleaner path already worked; the abrupt cut is the wedge).
//!
//! Requires a PostgreSQL at 127.0.0.1:$TEST_PG_PORT (default 5432) accepting
//! user=postgres password=postgres.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use may_postgres::{Client, Config};

/// How long a failed pending query may take to surface as `Err`. Generous for
/// CI; the point is "bounded", versus the pre-fix behavior of forever.
const FAIL_BOUND: Duration = Duration::from_secs(5);

fn pg_port() -> u16 {
    std::env::var("TEST_PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5432)
}

/// One-shot TCP proxy: accepts a single client connection, pipes it to the
/// real server, and severs both directions abruptly on command.
struct SeverableProxy {
    addr: std::net::SocketAddr,
    conns: mpsc::Receiver<(TcpStream, TcpStream)>,
}

impl SeverableProxy {
    fn start(backend_port: u16) -> SeverableProxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let (client_sock, _) = listener.accept().unwrap();
            let server_sock =
                TcpStream::connect(("127.0.0.1", backend_port)).expect("backend postgres");
            client_sock.set_nodelay(true).ok();
            server_sock.set_nodelay(true).ok();
            // Hand clones to the test so it can sever; pump bytes with the originals.
            tx.send((
                client_sock.try_clone().unwrap(),
                server_sock.try_clone().unwrap(),
            ))
            .unwrap();
            let c2 = client_sock.try_clone().unwrap();
            let s2 = server_sock.try_clone().unwrap();
            thread::spawn(move || pump(client_sock, server_sock));
            thread::spawn(move || pump(s2, c2));
        });
        SeverableProxy { addr, conns: rx }
    }

    /// Abruptly closes both sides of the proxied connection.
    fn sever(&self) {
        let (c, s) = self
            .conns
            .recv_timeout(Duration::from_secs(10))
            .expect("proxy accepted a connection");
        c.shutdown(Shutdown::Both).ok();
        s.shutdown(Shutdown::Both).ok();
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream) {
    let mut buf = [0u8; 8192];
    loop {
        match from.read(&mut buf) {
            Ok(0) | Err(_) => {
                to.shutdown(Shutdown::Both).ok();
                return;
            }
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    from.shutdown(Shutdown::Both).ok();
                    return;
                }
            }
        }
    }
}

fn connect_via(addr: std::net::SocketAddr) -> Client {
    let socket = may::net::TcpStream::connect(addr).unwrap();
    let config = "user=postgres password=postgres dbname=postgres"
        .parse::<Config>()
        .unwrap();
    config.connect_raw(socket).expect("connect through proxy")
}

/// A query that is in flight when the transport dies must return `Err` within
/// a bound — not hang forever (the lifeguard pool-worker wedge).
#[test]
fn pending_query_fails_when_connection_severed_mid_flight() {
    let proxy = SeverableProxy::start(pg_port());
    let client = connect_via(proxy.addr);

    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        // In flight for 30s unless the severance fails it early.
        let result = client.query_one("SELECT pg_sleep(30)", &[]);
        done_tx.send(result.map(|_| ())).ok();
        client // keep the client alive for the second phase
    });

    // Let the query hit the wire, then cut the transport abruptly.
    thread::sleep(Duration::from_millis(500));
    let start = Instant::now();
    proxy.sever();

    let result = done_rx
        .recv_timeout(FAIL_BOUND)
        .expect("pending query must fail within a bound after severance, not hang");
    let waited = start.elapsed();
    let err = result.expect_err("severed mid-flight query cannot succeed");
    assert!(
        waited < FAIL_BOUND,
        "query failed but only after {waited:?}"
    );
    // The error must read as a dead-connection error (lifeguard's heal
    // classifier keys on is_closed / io kinds / SQLSTATE 08***).
    assert!(
        err.is_closed() || err.to_string().contains("io error"),
        "expected a connection-death error, got: {err:?}"
    );

    // Phase 2: the client is now dead — new queries fail fast, not block.
    let client = worker.join().unwrap();
    assert!(
        client.is_closed(),
        "client must report closed after severance"
    );
    let start = Instant::now();
    let again = client.query_one("SELECT 1", &[]);
    assert!(again.is_err(), "queries on a dead client must error");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "queries on a dead client must fail fast, took {:?}",
        start.elapsed()
    );
}

/// Same wedge, but for a request QUEUED (not yet written) when the loop dies:
/// the exit drain must fail queued requests too.
#[test]
fn queued_request_fails_after_connection_death() {
    let proxy = SeverableProxy::start(pg_port());
    let client = connect_via(proxy.addr);

    // Prove liveness, then kill the transport with nothing in flight.
    client.query_one("SELECT 1", &[]).expect("warmup");
    proxy.sever();

    // Give the I/O loop a moment to observe the death.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.is_closed() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(client.is_closed(), "loop must mark the connection dead");

    let start = Instant::now();
    let result = client.query_one("SELECT 1", &[]);
    assert!(result.is_err(), "query on dead connection must error");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "must fail fast, took {:?}",
        start.elapsed()
    );
}
