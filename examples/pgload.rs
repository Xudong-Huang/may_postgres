//! pgload: a load generator for may_postgres, running pgbench's built-in workloads.
//!
//! pgbench measures the server through libpq; this runs the same transactions through
//! may_postgres, against the same `pgbench -i` schema, so the two can be compared side by side
//! (`hack/load-test.sh` does that and writes the report).
//!
//! ```text
//! cargo run --release --example pgload -- \
//!     --url 'host=127.0.0.1 port=5433 user=postgres dbname=postgres' \
//!     --workload select-only --connections 8 --per-connection 1 --duration 30 --scale 10 \
//!     --json target/load/pgload-select-only.json
//! ```
//!
//! Workloads (as `pgbench -b`):
//! - `select-only`: `SELECT abalance FROM pgbench_accounts WHERE aid = $1` (`-S`);
//! - `simple-update`: update an account, read its balance, insert history, in a transaction (`-N`);
//! - `tpcb-like`: the TPC-B-like transaction, also updating tellers and branches (the default).
//!
//! `--per-connection N` runs N coroutines on each connection. may_postgres pipelines their
//! statements on the one socket, which pgbench cannot do across clients; it is only allowed for
//! `select-only`, since interleaved transactions on a shared connection would be wrong.
//!
//! Exit status: 0 when every transaction committed; 1 when any failed or none ran.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use may_postgres::Client;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Workload {
    SelectOnly,
    SimpleUpdate,
    TpcbLike,
}

impl Workload {
    fn parse(s: &str) -> Workload {
        match s {
            "select-only" => Workload::SelectOnly,
            "simple-update" => Workload::SimpleUpdate,
            "tpcb-like" => Workload::TpcbLike,
            other => usage(&format!("unknown workload {other:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Workload::SelectOnly => "select-only",
            Workload::SimpleUpdate => "simple-update",
            Workload::TpcbLike => "tpcb-like",
        }
    }
}

struct Args {
    url: String,
    workload: Workload,
    connections: usize,
    per_connection: usize,
    duration: Duration,
    warmup: Duration,
    scale: i32,
    workers: usize,
    json: Option<String>,
}

fn usage(error: &str) -> ! {
    eprintln!(
        "pgload: {error}\n\nusage: pgload --url CONNINFO [--workload select-only|simple-update|tpcb-like] \
         [--connections N] [--per-connection N] [--duration SECS] [--warmup SECS] [--scale N] \
         [--workers N] [--json PATH]"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut args = Args {
        url: std::env::var("PGLOAD_URL").unwrap_or_default(),
        workload: Workload::SelectOnly,
        connections: 8,
        per_connection: 1,
        duration: Duration::from_secs(30),
        warmup: Duration::from_secs(5),
        scale: 10,
        workers: std::thread::available_parallelism().map_or(4, |n| n.get()),
        json: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .unwrap_or_else(|| usage(&format!("{flag} needs a value")))
        };
        let number = |v: String| -> u64 {
            v.parse()
                .unwrap_or_else(|_| usage(&format!("{v:?} is not a number")))
        };
        match flag.as_str() {
            "--url" => args.url = value(),
            "--workload" => args.workload = Workload::parse(&value()),
            "--connections" => args.connections = number(value()) as usize,
            "--per-connection" => args.per_connection = number(value()) as usize,
            "--duration" => args.duration = Duration::from_secs(number(value())),
            "--warmup" => args.warmup = Duration::from_secs(number(value())),
            "--scale" => args.scale = number(value()) as i32,
            "--workers" => args.workers = number(value()) as usize,
            "--json" => args.json = Some(value()),
            "-h" | "--help" => usage("help"),
            other => usage(&format!("unknown flag {other:?}")),
        }
    }
    if args.url.is_empty() {
        usage("--url (or PGLOAD_URL) is required");
    }
    if args.connections == 0 || args.per_connection == 0 || args.scale <= 0 {
        usage("--connections, --per-connection and --scale must be at least 1");
    }
    if args.per_connection > 1 && args.workload != Workload::SelectOnly {
        usage("--per-connection above 1 pipelines statements on a shared connection: select-only only");
    }
    args
}

/// xorshift64*: cheap, per-coroutine, no shared state.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: i32) -> i32 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as i32 + 1
    }
}

struct Statements {
    select: may_postgres::Statement,
    update_account: may_postgres::Statement,
    update_teller: may_postgres::Statement,
    update_branch: may_postgres::Statement,
    insert_history: may_postgres::Statement,
}

fn prepare(client: &Client) -> Result<Statements, may_postgres::Error> {
    Ok(Statements {
        select: client.prepare("SELECT abalance FROM pgbench_accounts WHERE aid = $1")?,
        update_account: client
            .prepare("UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2")?,
        update_teller: client
            .prepare("UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2")?,
        update_branch: client
            .prepare("UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2")?,
        insert_history: client.prepare(
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) \
             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)",
        )?,
    })
}

/// One transaction of `workload`, as pgbench's built-in script.
fn transaction(
    client: &mut Client,
    s: &Statements,
    workload: Workload,
    scale: i32,
    rng: &mut Rng,
) -> Result<(), may_postgres::Error> {
    let aid = rng.below(100_000 * scale);
    if workload == Workload::SelectOnly {
        client.query_one(&s.select, &[&aid])?;
        return Ok(());
    }
    let bid = rng.below(scale);
    let tid = rng.below(10 * scale);
    let delta = rng.below(10_001) - 5_001;
    let tx = client.transaction()?;
    tx.execute(&s.update_account, &[&delta, &aid])?;
    tx.query_one(&s.select, &[&aid])?;
    if workload == Workload::TpcbLike {
        tx.execute(&s.update_teller, &[&delta, &tid])?;
        tx.execute(&s.update_branch, &[&delta, &bid])?;
    }
    tx.execute(&s.insert_history, &[&tid, &bid, &aid, &delta])?;
    tx.commit()
}

#[derive(Default)]
struct Totals {
    latency_us: Option<Histogram<u64>>,
    transactions: u64,
    failed: u64,
    first_error: Option<String>,
}

fn main() {
    let args = parse_args();
    may::config().set_workers(args.workers);

    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let totals = Arc::new(Mutex::new(Totals::default()));

    let mut coroutines = Vec::new();
    for c in 0..args.connections {
        let client = match may_postgres::connect(&args.url) {
            Ok(client) => client,
            Err(e) => {
                eprintln!("pgload: connect: {e}");
                std::process::exit(1);
            }
        };
        for p in 0..args.per_connection {
            let mut client = client.clone();
            let (measuring, stop, totals) = (measuring.clone(), stop.clone(), totals.clone());
            let (workload, scale) = (args.workload, args.scale);
            let seed = 0x9E37_79B9_7F4A_7C15_u64
                ^ ((c * 1024 + p) as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407);
            coroutines.push(may::go!(move || {
                let mut rng = Rng(seed);
                let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
                let (mut transactions, mut failed, mut first_error) = (0_u64, 0_u64, None);
                let statements = match prepare(&client) {
                    Ok(s) => s,
                    Err(e) => {
                        totals
                            .lock()
                            .unwrap()
                            .first_error
                            .get_or_insert(format!("prepare: {e}"));
                        totals.lock().unwrap().failed += 1;
                        return;
                    }
                };
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let result = transaction(&mut client, &statements, workload, scale, &mut rng);
                    if !measuring.load(Ordering::Relaxed) {
                        continue;
                    }
                    match result {
                        Ok(()) => {
                            transactions += 1;
                            let us = started.elapsed().as_micros().max(1) as u64;
                            histogram.saturating_record(us);
                        }
                        Err(e) => {
                            failed += 1;
                            first_error.get_or_insert_with(|| e.to_string());
                        }
                    }
                }
                let mut t = totals.lock().unwrap();
                t.transactions += transactions;
                t.failed += failed;
                if t.first_error.is_none() {
                    t.first_error = first_error;
                }
                match t.latency_us.as_mut() {
                    Some(h) => h.add(&histogram).unwrap(),
                    None => t.latency_us = Some(histogram),
                }
            }));
        }
    }

    std::thread::sleep(args.warmup);
    measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    std::thread::sleep(args.duration);
    measuring.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    for c in coroutines {
        let _ = c.join();
    }

    let t = totals.lock().unwrap();
    let empty = Histogram::<u64>::new(3).unwrap();
    let h = t.latency_us.as_ref().unwrap_or(&empty);
    let ms = |q: f64| h.value_at_quantile(q) as f64 / 1000.0;
    let tps = t.transactions as f64 / elapsed.as_secs_f64();
    let report = format!(
        "{{\"tool\":\"may_postgres\",\"workload\":\"{}\",\"connections\":{},\"per_connection\":{},\
         \"scale\":{},\"duration_s\":{:.3},\"transactions\":{},\"failed\":{},\"tps\":{:.1},\
         \"latency_ms\":{{\"mean\":{:.3},\"p50\":{:.3},\"p95\":{:.3},\"p99\":{:.3},\"max\":{:.3}}}}}",
        args.workload.name(),
        args.connections,
        args.per_connection,
        args.scale,
        elapsed.as_secs_f64(),
        t.transactions,
        t.failed,
        tps,
        h.mean() / 1000.0,
        ms(0.50),
        ms(0.95),
        ms(0.99),
        h.max() as f64 / 1000.0,
    );
    println!(
        "pgload {} connections={} per_connection={}: {} transactions, {} failed, {:.1} tps, \
         latency ms p50={:.3} p95={:.3} p99={:.3} max={:.3}",
        args.workload.name(),
        args.connections,
        args.per_connection,
        t.transactions,
        t.failed,
        tps,
        ms(0.50),
        ms(0.95),
        ms(0.99),
        h.max() as f64 / 1000.0
    );
    if let Some(e) = &t.first_error {
        eprintln!("pgload: first error: {e}");
    }
    if let Some(path) = &args.json {
        if let Some(dir) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        std::fs::write(path, report).unwrap_or_else(|e| eprintln!("pgload: {path}: {e}"));
    }
    if t.failed > 0 || t.transactions == 0 {
        std::process::exit(1);
    }
}
