#!/usr/bin/env python3
"""Markdown coverage report from `cargo llvm-cov report --json --summary-only`.

    cargo llvm-cov report --json --summary-only --output-path summary.json
    python3 hack/coverage-report.py summary.json --min-lines 95 > coverage-report.md

Lists every source file with its line, region and function coverage, lowest first, and the
totals. Exits 1 when total line coverage is below --min-lines (the report is still written).
"""

import argparse
import json
import os
import sys


def pct(part):
    return f"{part['percent']:.2f}%" if part["count"] else "–"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("summary")
    parser.add_argument("--min-lines", type=float, default=0.0)
    parser.add_argument("--title", default="Coverage")
    args = parser.parse_args()

    with open(args.summary) as f:
        data = json.load(f)["data"][0]

    def name(path):
        return path.split("/src/", 1)[-1] if "/src/" in path else os.path.basename(path)

    files = sorted(data["files"], key=lambda f: f["summary"]["lines"]["percent"])
    totals = data["totals"]
    lines = totals["lines"]
    ok = lines["percent"] >= args.min_lines

    out = [f"#### {args.title}", ""]
    out.append(
        f"**{lines['percent']:.2f}%** of lines ({lines['covered']}/{lines['count']}), "
        f"{pct(totals['regions'])} of regions, {pct(totals['functions'])} of functions"
        + (f"; minimum {args.min_lines:g}% of lines {'✅' if ok else '❌'}" if args.min_lines else "")
    )
    out += ["", "<details><summary>Per file</summary>", ""]
    out.append("| File | Lines | Missed lines | Regions | Functions |")
    out.append("|------|------:|-------------:|--------:|----------:|")
    for f in files:
        s = f["summary"]
        missed = s["lines"]["count"] - s["lines"]["covered"]
        out.append(
            f"| `{name(f['filename'])}` | {pct(s['lines'])} | {missed} | "
            f"{pct(s['regions'])} | {pct(s['functions'])} |"
        )
    out += ["", "</details>", ""]
    print("\n".join(out))
    if not ok:
        print(
            f"line coverage {lines['percent']:.2f}% is below the minimum {args.min_lines:g}%",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
