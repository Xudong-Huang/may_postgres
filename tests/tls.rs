//! TLS against real servers, set up by tests/tls/run.sh (two containers and a throwaway CA):
//!
//! * TLS_PG_PORT: ssl=on, certificate for DNS:localhost only (so 127.0.0.1 fails verify-full), pg_hba
//!   `hostssl ... certuser ... clientcert=verify-full`, `hostssl all all scram-sha-256`, plaintext rejected.
//! * PLAIN_PG_PORT: ssl=off.
//! * TLS_TEST_DIR: ca.crt, client.crt + client.key (CN=certuser), other.crt + other.key (CN=certuser, from
//!   other-ca.crt, another CA).
//!
//! Skipped (passes) when TLS_PG_PORT is not set.

use may_postgres::{config::SslMode, Client, Config};

struct Env {
    tls: u16,
    plain: u16,
    dir: String,
}

fn env() -> Option<Env> {
    let tls = std::env::var("TLS_PG_PORT").ok()?.parse().ok()?;
    let plain = std::env::var("PLAIN_PG_PORT").ok()?.parse().ok()?;
    let dir = std::env::var("TLS_TEST_DIR").ok()?;
    Some(Env { tls, plain, dir })
}

fn connect(s: &str) -> Result<Client, may_postgres::Error> {
    may_postgres::connect(s)
}

fn one(c: &Client) -> i32 {
    c.query_one("SELECT 1", &[]).unwrap().get(0)
}

#[test]
fn require_encrypts_and_queries() {
    let Some(e) = env() else { return };
    let c = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require",
        e.tls
    ))
    .unwrap();
    assert_eq!(one(&c), 1);
    let ssl: bool = c
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(ssl, "pg_stat_ssl says the connection is not encrypted");
}

#[test]
fn large_results_and_many_round_trips_over_tls() {
    let Some(e) = env() else { return };
    let c = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require",
        e.tls
    ))
    .unwrap();
    // Many TLS records per response: 200k rows, and one 4 MB value.
    let rows = c
        .query(
            "SELECT g, md5(g::text) FROM generate_series(1, 200000) g",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 200000);
    let big: String = c
        .query_one("SELECT repeat('x', 4 * 1024 * 1024)", &[])
        .unwrap()
        .get(0);
    assert_eq!(big.len(), 4 * 1024 * 1024);
    // Large request: a 2 MB parameter.
    let payload = "y".repeat(2 * 1024 * 1024);
    let n: i32 = c
        .query_one("SELECT length($1::text)", &[&payload])
        .unwrap()
        .get(0);
    assert_eq!(n as usize, payload.len());
    for i in 0..500 {
        let v: i32 = c.query_one("SELECT $1::int4", &[&i]).unwrap().get(0);
        assert_eq!(v, i);
    }
}

#[test]
fn concurrent_coroutines_share_a_tls_connection() {
    let Some(e) = env() else { return };
    let c = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require",
        e.tls
    ))
    .unwrap();
    let handles: Vec<_> = (0..32)
        .map(|i| {
            let c = c.clone();
            may::go!(move || {
                for j in 0..50 {
                    let v: i32 = c
                        .query_one("SELECT $1::int4", &[&(i * 1000 + j)])
                        .unwrap()
                        .get(0);
                    assert_eq!(v, i * 1000 + j);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn verify_full_checks_the_name() {
    let Some(e) = env() else { return };
    let ca = format!("{}/ca.crt", e.dir);
    let ok = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=verify-full sslrootcert={ca}",
        e.tls
    ))
    .unwrap();
    assert_eq!(one(&ok), 1);
    // The certificate names localhost only.
    let err = connect(&format!(
        "host=127.0.0.1 port={} user=pw password=pw dbname=postgres sslmode=verify-full sslrootcert={ca}",
        e.tls
    ))
    .err()
    .expect("verify-full must reject a certificate for another name");
    assert!(err.to_string().contains("TLS"), "{err}");
    // verify-ca does not check the name.
    let c = connect(&format!(
        "host=127.0.0.1 port={} user=pw password=pw dbname=postgres sslmode=verify-ca sslrootcert={ca}",
        e.tls
    ))
    .unwrap();
    assert_eq!(one(&c), 1);
}

#[test]
fn verify_ca_rejects_another_ca() {
    let Some(e) = env() else { return };
    let other = format!("{}/other-ca.crt", e.dir);
    assert!(connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=verify-ca sslrootcert={other}",
        e.tls
    ))
    .is_err());
    // require + sslrootcert behaves as verify-ca (libpq).
    assert!(connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require sslrootcert={other}",
        e.tls
    ))
    .is_err());
}

#[test]
fn verify_modes_need_a_root_certificate() {
    let Some(e) = env() else { return };
    let err = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=verify-full",
        e.tls
    ))
    .err()
    .unwrap();
    assert!(
        err.to_string().contains("sslrootcert") || format!("{err:?}").contains("sslrootcert"),
        "{err:?}"
    );
}

#[test]
fn client_certificate() {
    let Some(e) = env() else { return };
    let d = &e.dir;
    let c = connect(&format!(
        "host=localhost port={} user=certuser password=cu dbname=postgres sslmode=verify-full \
         sslrootcert={d}/ca.crt sslcert={d}/client.crt sslkey={d}/client.key",
        e.tls
    ))
    .unwrap();
    assert_eq!(one(&c), 1);
    // Without a certificate the server refuses (clientcert=verify-full) even with the password.
    assert!(connect(&format!(
        "host=localhost port={} user=certuser password=cu dbname=postgres sslmode=verify-full sslrootcert={d}/ca.crt",
        e.tls
    ))
    .is_err());
    // A certificate from another CA is refused.
    assert!(connect(&format!(
        "host=localhost port={} user=certuser password=cu dbname=postgres sslmode=verify-full \
         sslrootcert={d}/ca.crt sslcert={d}/other.crt sslkey={d}/other.key",
        e.tls
    ))
    .is_err());
    // Config builder API.
    let mut cfg = Config::new();
    cfg.host("localhost")
        .port(e.tls)
        .user("certuser")
        .password("cu")
        .dbname("postgres")
        .ssl_mode(SslMode::VerifyFull)
        .ssl_root_cert(format!("{d}/ca.crt"))
        .ssl_cert(format!("{d}/client.crt"))
        .ssl_key(format!("{d}/client.key"));
    assert_eq!(one(&cfg.connect().unwrap()), 1);
}

#[test]
fn plaintext_is_refused_by_the_tls_server() {
    let Some(e) = env() else { return };
    assert!(connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=disable",
        e.tls
    ))
    .is_err());
}

#[test]
fn prefer_falls_back_and_require_fails_on_a_plain_server() {
    let Some(e) = env() else { return };
    let c = connect(&format!(
        "host=localhost port={} user=postgres password=postgres dbname=postgres",
        e.plain
    ))
    .unwrap();
    assert_eq!(one(&c), 1);
    let c = connect(&format!(
        "host=localhost port={} user=postgres password=postgres dbname=postgres sslmode=disable",
        e.plain
    ))
    .unwrap();
    assert_eq!(one(&c), 1);
    let err = connect(&format!(
        "host=localhost port={} user=postgres password=postgres dbname=postgres sslmode=require",
        e.plain
    ))
    .err()
    .expect("require against a server without TLS must fail");
    assert!(err.to_string().contains("TLS"), "{err}");
}

#[test]
fn drop_closes_the_tls_connection_cleanly() {
    let Some(e) = env() else { return };
    let admin = connect(&format!(
        "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require application_name=admin",
        e.tls
    ))
    .unwrap();
    for _ in 0..20 {
        let c = connect(&format!(
            "host=localhost port={} user=pw password=pw dbname=postgres sslmode=require application_name=churn",
            e.tls
        ))
        .unwrap();
        assert_eq!(one(&c), 1);
        drop(c);
    }
    std::thread::sleep(std::time::Duration::from_millis(500));
    let left: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'churn'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(left, 0, "backends left behind after drop");
}
