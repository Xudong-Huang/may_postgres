# may_postgres

Porting from [rust-postgres](https://github.com/sfackler/rust-postgres) for stackful coroutine.

## Tests

The suite needs a PostgreSQL with the fixtures in `ci/initdb` (one role per password method):

```sh
docker run -d --name pg -p 127.0.0.1:5433:5432 -e POSTGRES_HOST_AUTH_METHOD=trust \
  -v "$PWD/ci/initdb:/docker-entrypoint-initdb.d:ro" postgres:17
cargo nextest run --workspace     # or cargo test --all
```

## Load tests

`hack/load-test.sh` runs pgbench's built-in workloads (select-only, TPC-B-like, simple-update) twice
against the same server: through libpq with `pgbench`, and through may_postgres with
`examples/pgload.rs`, a pgbench-style load generator (connections, coroutines per connection,
warm-up, latency percentiles). It also runs a pipelined select-only load (2 connections x 16
coroutines) that pgbench cannot. The report (`target/load/load-report.md`) puts the two side by side:

```sh
PGPORT=5433 hack/load-test.sh            # LOAD_DURATION, LOAD_CLIENTS, LOAD_SCALE, LOAD_MIN_RATIO...
cargo run --release --example pgload -- --url 'host=127.0.0.1 port=5433 user=postgres' \
  --workload tpcb-like --connections 16 --duration 30
```

It fails on any failed transaction, or when may_postgres reaches less than `LOAD_MIN_RATIO` (0.5)
of pgbench's throughput on a shared scenario: the ratio of two tools measured back to back on one
runner is stable where absolute numbers are not. CI runs it against PostgreSQL 17 on every push and
pull request, after the tests, and posts the report with the test results.

## License
Licensed under either of the following, at your option:

 * The Apache License v2.0.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0);
 * The MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT).