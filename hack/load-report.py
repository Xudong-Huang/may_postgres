#!/usr/bin/env python3
"""Turn hack/load-test.sh's output into a Markdown report (stdout) and a JSON summary.

Reads, from --dir:
  pgbench-<scenario>.out      pgbench's summary (tps, failed transactions)
  pgbench-<scenario>.log.*    pgbench's per-transaction log (3rd field: latency in microseconds)
  pgload-<scenario>.json      examples/pgload.rs's summary

Exit status 1 when a run failed transactions, produced none, is missing, or may_postgres reached
less than --min-ratio of pgbench's tps on a scenario both ran.
"""
import argparse
import glob
import json
import os
import re
import sys

SCENARIOS = [
    ("select-only", "select-only", "`SELECT abalance ... WHERE aid = $1` (pgbench -S)"),
    ("tpcb-like", "tpcb-like", "TPC-B-like transaction (pgbench default)"),
    ("simple-update", "simple-update", "update, select, insert history (pgbench -N)"),
    ("select-only-pipelined", None, "select-only, 2 connections x 16 coroutines pipelining (may_postgres only)"),
]


def percentile(sorted_values, q):
    if not sorted_values:
        return None
    i = min(len(sorted_values) - 1, max(0, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[i]


def pgbench_result(directory, name):
    out = os.path.join(directory, f"pgbench-{name}.out")
    if not os.path.exists(out):
        return None
    text = open(out).read()
    tps = re.search(r"^tps = ([0-9.]+)", text, re.M)
    failed = re.search(r"number of failed transactions: (\d+)", text)
    processed = re.search(r"number of transactions actually processed: (\d+)", text)
    latencies = []
    for path in glob.glob(os.path.join(directory, f"pgbench-{name}.log*")):
        with open(path) as f:
            for line in f:
                fields = line.split()
                if len(fields) >= 3 and fields[2].isdigit():
                    latencies.append(int(fields[2]) / 1000.0)
    latencies.sort()
    return {
        "tool": "pgbench (libpq)",
        "tps": float(tps.group(1)) if tps else None,
        "transactions": int(processed.group(1)) if processed else 0,
        "failed": int(failed.group(1)) if failed else (0 if tps else None),
        "p50": percentile(latencies, 0.50),
        "p95": percentile(latencies, 0.95),
        "p99": percentile(latencies, 0.99),
    }


def pgload_result(directory, name):
    path = os.path.join(directory, f"pgload-{name}.json")
    if not os.path.exists(path):
        return None
    data = json.load(open(path))
    lat = data.get("latency_ms", {})
    return {
        "tool": "may_postgres",
        "tps": data.get("tps"),
        "transactions": data.get("transactions", 0),
        "failed": data.get("failed"),
        "p50": lat.get("p50"),
        "p95": lat.get("p95"),
        "p99": lat.get("p99"),
    }


def fmt(value, digits=3):
    return "–" if value is None else f"{value:,.{digits}f}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True)
    parser.add_argument("--min-ratio", type=float, default=0.5)
    parser.add_argument("--output-json")
    args = parser.parse_args()

    problems, summary = [], []
    print("#### Postgres load tests (pgbench workloads: libpq vs may_postgres)\n")
    print("| Scenario | Client | TPS | p50 ms | p95 ms | p99 ms | Failed | may_postgres / pgbench |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for name, pgbench_name, description in SCENARIOS:
        rows = []
        bench = pgbench_result(args.dir, pgbench_name) if pgbench_name else None
        load = pgload_result(args.dir, name)
        if pgbench_name and bench is None:
            problems.append(f"{name}: pgbench output missing")
        if load is None:
            problems.append(f"{name}: may_postgres output missing")
        ratio = None
        if bench and load and bench.get("tps") and load.get("tps") is not None:
            ratio = load["tps"] / bench["tps"]
            if ratio < args.min_ratio:
                problems.append(f"{name}: may_postgres at {ratio:.2f}x pgbench's tps (floor {args.min_ratio})")
        for result in (bench, load):
            if result is None:
                continue
            if result["failed"]:
                problems.append(f"{name}: {result['tool']} failed {result['failed']} transactions")
            if not result["transactions"]:
                problems.append(f"{name}: {result['tool']} completed no transactions")
            rows.append(result)
        for i, r in enumerate(rows):
            print(f"| {name if i == 0 else ''} | {r['tool']} | {fmt(r['tps'], 0)} | {fmt(r['p50'])} | "
                  f"{fmt(r['p95'])} | {fmt(r['p99'])} | {r['failed'] if r['failed'] is not None else '–'} | "
                  f"{(f'{ratio:.2f}x' if ratio is not None and r['tool'] == 'may_postgres' else '')} |")
        summary.append({"scenario": name, "description": description, "ratio": ratio, "results": rows})

    print()
    for name, _, description in SCENARIOS:
        print(f"- **{name}**: {description}")
    print()
    if problems:
        print("**Load test failures:**\n")
        for p in problems:
            print(f"- {p}")
    else:
        print(f"All scenarios completed with no failed transactions; may_postgres at or above "
              f"{args.min_ratio}x pgbench's tps on every shared scenario.")
    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump({"min_ratio": args.min_ratio, "scenarios": summary, "problems": problems}, f, indent=2)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
