//! Diagnostic: does dropping a Client actually release its resources?
//!
//! Two independent measurements per iteration:
//!   - server side: backends in pg_stat_activity for our database
//!   - client side: open socket fds in /proc/self/fd
//!
//! If BOTH climb one-per-iteration, the process itself is holding the socket
//! after drop - a genuine client-side leak. If fds return to baseline but
//! backends linger, the client is fine and the server is just slow to reap.

fn socket_fds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            std::fs::read_link(e.path())
                .map(|t| t.to_string_lossy().starts_with("socket:"))
                .unwrap_or(false)
        })
        .count()
}

fn main() {
    let url = std::env::var("TEST_URL").expect("set TEST_URL");

    let observer = may_postgres::connect(&url).expect("observer connect");
    let backends = |o: &may_postgres::Client| -> i64 {
        o.query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
            &[],
        )
        .unwrap()
        .get(0)
    };

    let base_be = backends(&observer);
    let base_fd = socket_fds();
    println!("BASELINE backends={base_be} socket_fds={base_fd}");

    let iters: u32 = std::env::var("ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let verbose = iters <= 20;
    for i in 1..=iters {
        {
            let client = may_postgres::connect(&url).expect("connect");
            let rows = client.query("SELECT 1", &[]).expect("query");
            assert_eq!(rows.len(), 1);
            drop(client);
        }
        std::thread::sleep(std::time::Duration::from_millis(if verbose {
            300
        } else {
            5
        }));
        if !verbose && i % 100 != 0 {
            continue;
        }
        println!(
            "ITER {i} backends={} socket_fds={}",
            backends(&observer),
            socket_fds()
        );
    }

    // Give the server every chance to reap on its own.
    println!("WAITING 15s...");
    std::thread::sleep(std::time::Duration::from_secs(15));
    println!(
        "FINAL backends={} socket_fds={} (baseline {base_be}/{base_fd})",
        backends(&observer),
        socket_fds()
    );
}
