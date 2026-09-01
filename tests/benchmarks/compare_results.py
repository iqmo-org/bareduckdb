#!/usr/bin/env python3
"""Generate a benchmark comparison table from JSONL results and gate on regressions."""

import argparse
import json
import sys
from pathlib import Path

import bareduckdb

DEFAULT_ALLOWLIST = Path(__file__).parent / "regression_allowlist.json"
DEFAULT_THRESHOLD = 1.5
DEFAULT_MIN_BASELINE_MS = 5.0

# One statement per execute()
SETUP_STATEMENTS = [
    """
    create or replace table all_results_raw as
    select *
    from read_json('RESULTS_DIR/*.jsonl', filename=True, ignore_errors=true)
    """,
    """
    create or replace table all_results as
    select * exclude (timestamp, nodeid),
        coalesce(test_run, 1) as test_run,
        case when bench is not null then bench
            when library = 'duckdb' then library
            when library = 'bareduckdb' and 'dev' in lib_version then 'bareduckdb_dev'
            else library
        end as lib,
    from all_results_raw
    """,
    """
    create or replace table latest_results as
    select * from all_results where filename in
    (select max(filename) from all_results group by lib, lib_version)
    """,
    """
    create or replace table result_stats as
    select
        lib,
        test_name,
        mode,
        avg(wall_time_s)*1000 as time_ms_avg,
        median(wall_time_s)*1000 as time_ms_median,
        -- try_cast: a metric that was unavailable for a whole run is all-null, which
        -- read_json infers as JSON rather than a numeric type.
        avg(try_cast(rusage_maxrss_delta_kb as double)) as memory_kb_delta,
        avg(try_cast(rusage_maxrss_peak_kb as double)) as memory_kb_peak,
        avg(try_cast(rss_peak_delta_kb as double)) as memory_kb_query_delta,
        count(*) num_tests
     from latest_results
     group by lib, test_name, mode
    """,
    "create or replace table baseline as (select * from result_stats where lib='duckdb')",
    # Every (test, mode) the baseline measured, crossed with every library, so a
    # library with no results for a case stays in the table as an explicit gap
    # instead of being dropped by an inner join.
    """
    create or replace table expected_cells as
    select b.test_name, b.mode, l.lib
    from baseline b
    cross join (select distinct lib from result_stats where lib != 'duckdb') l
    """,
    """
    create or replace table result_vs_baseline as
    select e.test_name, e.mode, e.lib,
        r.time_ms_avg,
        r.time_ms_median,
        b.time_ms_median as base_time_ms_median,
        r.num_tests,
        r.time_ms_avg/b.time_ms_avg as ms_ratio,
        -- median ratio: immune to a single cold-cache run, unlike ms_ratio
        r.time_ms_median/b.time_ms_median as ms_median_ratio,
     -- r.memory_kb_delta/b.memory_kb_delta as mem_delta_ratio,
        r.memory_kb_peak/b.memory_kb_peak as mem_peak_ratio,
    -- mem_query_ratio: per-query RSS delta, vs. mem_peak_ratio's process-wide peak
        r.memory_kb_query_delta/b.memory_kb_query_delta as mem_query_ratio,
        r.time_ms_avg is null as missing
    from expected_cells e
    join baseline b on b.test_name=e.test_name and b.mode=e.mode
    left join result_stats r
        on r.test_name=e.test_name and r.mode=e.mode and r.lib=e.lib
    order by e.test_name, e.mode, e.lib
    """,
    # Libraries that produced results for a case the duckdb baseline never measured.
    """
    create or replace table missing_baseline as
    select r.lib, r.test_name, r.mode, r.num_tests
    from result_stats r
    left join baseline b on b.test_name=r.test_name and b.mode=r.mode
    where r.lib != 'duckdb' and b.test_name is null
    order by r.test_name, r.mode, r.lib
    """,
]

def build_report_query(libs):
    """Build the per-library report columns explicitly, since PIVOT expands into multiple engine statements."""
    columns = []
    for lib in libs:
        safe = lib.replace("'", "''")
        columns.append(f"round(max(case when lib = '{safe}' then time_ms_avg end), 1) as \"{lib}_time_ms_avg\"")
        columns.append(f"round(max(case when lib = '{safe}' then ms_ratio end), 2) as \"{lib}_time\"")
        columns.append(f"round(max(case when lib = '{safe}' then ms_median_ratio end), 2) as \"{lib}_time_med\"")
    for lib in libs:
        safe = lib.replace("'", "''")
        # mem_peak: process-wide rusage high-water mark. mem_query: per-query RSS delta.
        columns.append(f"round(max(case when lib = '{safe}' then mem_peak_ratio end), 1) as \"{lib}_mem_peak\"")
        columns.append(f"round(max(case when lib = '{safe}' then mem_query_ratio end), 1) as \"{lib}_mem_query\"")

    column_sql = ",\n        ".join(columns)
    return f"""
    with pivoted as (
        select test_name, mode,
        {column_sql}
        from result_vs_baseline
        group by test_name, mode
    ),
    gaps as (
        select test_name, mode, string_agg(lib, ',' order by lib) as no_data
        from result_vs_baseline where missing group by test_name, mode
    )
    select b.test_name as test,
        b.mode,
        round(b.time_ms_avg,1) base_ms,
        round(b.time_ms_median,1) base_ms_med,
        p.* exclude (test_name, mode),
        coalesce(g.no_data, '') as no_data
    from baseline b
    join pivoted p on p.test_name=b.test_name and p.mode=b.mode
    left join gaps g on g.test_name=b.test_name and g.mode=b.mode
    order by b.test_name, b.mode
    """


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", nargs="?", default="benchmark-results", help="Directory of benchmark JSONL files")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Fail when a case's median time ratio exceeds this (default {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--allowlist",
        default=str(DEFAULT_ALLOWLIST),
        help="JSON file of known regressions that do not fail the gate",
    )
    parser.add_argument(
        "--allow",
        action="append",
        default=[],
        metavar="TEST[:MODE[:LIB]]",
        help="Allowlist one case inline, repeatable; MODE and LIB default to '*'",
    )
    parser.add_argument(
        "--min-baseline-ms",
        type=float,
        default=DEFAULT_MIN_BASELINE_MS,
        help=f"Do not gate cases whose duckdb baseline median is below this, where noise dominates (default {DEFAULT_MIN_BASELINE_MS})",
    )
    parser.add_argument("--no-gate", action="store_true", help="Report only, never exit non-zero on a regression")
    parser.add_argument("--fail-on-missing", action="store_true", help="Also fail when a library has no data for a baseline case")
    return parser.parse_args(argv)


def load_allowlist(path, inline_entries):
    """Read the allowlist file and merge in any --allow entries."""
    entries = []
    allowlist_path = Path(path)
    if allowlist_path.exists():
        raw = json.loads(allowlist_path.read_text())
        for item in raw:
            entries.append(
                {
                    "test": item["test"],
                    "mode": item.get("mode", "*"),
                    "lib": item.get("lib", "*"),
                    "reason": item.get("reason", ""),
                    "max_ratio": item.get("max_ratio"),
                    "source": str(allowlist_path),
                }
            )

    for spec in inline_entries:
        parts = spec.split(":")
        entries.append(
            {
                "test": parts[0],
                "mode": parts[1] if len(parts) > 1 else "*",
                "lib": parts[2] if len(parts) > 2 else "*",
                "reason": "--allow on the command line",
                "max_ratio": None,
                "source": "--allow",
            }
        )
    return entries


def _matches(entry, row):
    return (
        entry["test"] == row["test_name"]
        and entry["mode"] in ("*", row["mode"])
        and entry["lib"] in ("*", row["lib"])
    )


def _entry_label(entry):
    return f"{entry['test']}:{entry['mode']}:{entry['lib']}"


def _is_number(value):
    return value is not None and value == value


def report_gate(df_ratios, args):
    """Print the regression gate outcome and return the process exit code."""
    entries = load_allowlist(args.allowlist, args.allow)

    print("\n## Regression gate\n")
    print(f"- median time ratio threshold: **{args.threshold}**")
    print(f"- cases with a duckdb baseline median below {args.min_baseline_ms} ms are not gated, noise dominates there")
    print(f"- allowlist file: `{args.allowlist}`" + ("" if Path(args.allowlist).exists() else " (absent)"))

    if entries:
        print("\n**Allowlisted regressions** (exempt from the gate, listed so they are never silent):\n")
        print("| case | max_ratio | observed_median_ratio | matched | reason | source |")
        print("| --- | --- | --- | --- | --- | --- |")
    breaches = []

    above_threshold = df_ratios[df_ratios["ms_median_ratio"].notna() & (df_ratios["ms_median_ratio"] > args.threshold)]
    too_short = above_threshold["base_time_ms_median"] < args.min_baseline_ms
    over = above_threshold[~too_short]
    ungated = above_threshold[too_short]

    for entry in entries:
        observed = []
        for _, row in df_ratios.iterrows():
            if _matches(entry, row) and _is_number(row["ms_median_ratio"]):
                observed.append(row["ms_median_ratio"])
        worst = max(observed) if observed else None
        worst_txt = "n/a" if worst is None else f"{worst:.2f}"
        ceiling = "none" if entry["max_ratio"] is None else f"{entry['max_ratio']}"
        matched = "yes" if observed else "NO (stale entry)"
        print(f"| {_entry_label(entry)} | {ceiling} | {worst_txt} | {matched} | {entry['reason']} | {entry['source']} |")

    for _, row in over.iterrows():
        allowed_by = None
        ceiling_breach = None
        for entry in entries:
            if _matches(entry, row):
                if entry["max_ratio"] is not None and row["ms_median_ratio"] > entry["max_ratio"]:
                    ceiling_breach = entry
                else:
                    allowed_by = entry
                    break
        if allowed_by is not None:
            continue
        breaches.append((row, ceiling_breach))

    if breaches:
        print("\n**REGRESSION GATE FAILED.** These cases exceed the median ratio threshold:\n")
        print("| lib | case | mode | median_ratio | mean_ratio | note |")
        print("| --- | --- | --- | --- | --- | --- |")
        for row, ceiling_breach in breaches:
            note = ""
            if ceiling_breach is not None:
                note = f"allowlisted but above its max_ratio {ceiling_breach['max_ratio']}"
            mean_txt = f"{row['ms_ratio']:.2f}" if _is_number(row["ms_ratio"]) else "n/a"
            print(f"| {row['lib']} | {row['test_name']} | {row['mode']} | {row['ms_median_ratio']:.2f} | {mean_txt} | {note} |")
    else:
        print(f"\nNo case exceeds the {args.threshold}x median ratio threshold outside the allowlist.")

    if len(ungated) > 0:
        ungated = ungated[[not any(_matches(e, row) for e in entries) for _, row in ungated.iterrows()]]
    if len(ungated) > 0:
        print(f"\n**Over threshold but not gated** (duckdb baseline median below {args.min_baseline_ms} ms):\n")
        print(ungated[["lib", "test_name", "mode", "base_time_ms_median", "ms_median_ratio"]].to_markdown(index=False))

    exit_code = 1 if breaches else 0

    if args.fail_on_missing:
        missing = df_ratios[df_ratios["missing"]]
        if len(missing) > 0:
            print("\n**REGRESSION GATE FAILED.** --fail-on-missing and these cases have no data:\n")
            print(missing[["lib", "test_name", "mode"]].to_markdown(index=False))
            exit_code = 1

    if args.no_gate and exit_code:
        print("\n_--no-gate given: reporting the breach but exiting 0._")
        return 0
    return exit_code


def main(argv=None):
    args = parse_args(argv)
    results_dir = Path(args.results_dir)

    with bareduckdb.connect() as conn:
        for statement in SETUP_STATEMENTS:
            conn.execute(statement.replace("RESULTS_DIR", str(results_dir).replace("\\", "/")))

        libs = [row[0] for row in conn.execute("select distinct lib from result_vs_baseline order by lib").fetchall()]
        if not libs:
            print("No non-duckdb results found in", results_dir)
            return 1

        df = conn.execute(build_report_query(libs)).df()
        df_ratios = conn.execute("select * from result_vs_baseline").df()
        df_check = conn.execute("select filename, pid, count(*) c from latest_results group by filename, pid having c > 1").df()
        df_gaps = conn.execute("select lib, count(*) cases from result_vs_baseline where missing group by lib order by lib").df()
        df_no_baseline = conn.execute("select * from missing_baseline").df()
        df_metrics = conn.execute(
            """
            select lib, count(*) records,
                sum(case when rss_peak_delta_kb is null then 1 else 0 end) as rss_null,
                sum(case when rusage_maxrss_peak_kb is null then 1 else 0 end) as rusage_null
            from latest_results group by lib order by lib
            """
        ).df()

    print("## Benchmark Results\n")
    print(df.to_markdown(index=False))
    print("\n_time_ratio < 1 means bareduckdb is faster_")
    print("_`time` is the mean-of-N ratio, `time_med` the median-of-N ratio; a regression in one but not the other is run-to-run noise_")
    print("_`no_data` names any library with no results for that case, so gaps are visible rather than dropped_")

    if len(df_gaps) > 0:
        print("\n**Cases with no data** (still listed above, with blank ratio columns):\n")
        print(df_gaps.to_markdown(index=False))

    if len(df_no_baseline) > 0:
        print("\n**Cases with no duckdb baseline** (excluded from the table above):\n")
        print(df_no_baseline.to_markdown(index=False))

    if df_metrics[["rss_null", "rusage_null"]].to_numpy().sum() > 0:
        print("\n**WARNING: memory metrics missing.** psutil or the resource module was unavailable for these runs:\n")
        print(df_metrics.to_markdown(index=False))

    if len(df_check) > 0:
        print("\n**WARNING: Fork isolation issue detected!** Multiple tests ran in same process:\n")
        print(df_check.to_markdown(index=False))

    return report_gate(df_ratios, args)


if __name__ == "__main__":
    sys.exit(main())
