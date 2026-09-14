#!/usr/bin/env python3
"""Generate a benchmark comparison table from JSONL results"""

import argparse
import sys
from pathlib import Path

import bareduckdb


# One statement per execute()
SETUP_STATEMENTS = [
    """
    create or replace table all_results_raw as
    select *
    from read_json('RESULTS_DIR/*.jsonl', filename=True, ignore_errors=true)
    """,
    """
    create or replace table all_results as
    select * exclude (timestamp, nodeid, cache),
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
    # One baseline row per (test_name, mode).
    "create or replace table baseline as (select * from result_stats where lib='duckdb')",
    # Baseline (test, mode) crossed with every library present, so a gap stays explicit.
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
        b.time_ms_avg as base_time_ms_avg,
        b.time_ms_median as base_time_ms_median,
        r.num_tests,
        r.time_ms_avg/b.time_ms_avg as ms_ratio,
        -- median ratio: immune to a single cold-cache run, unlike ms_ratio
        r.time_ms_median/b.time_ms_median as ms_median_ratio,
    -- mem_delta_ratio: rusage high-water RISE across the call phase,
    -- mem_peak_ratio: high-water ABSOLUTE
        r.memory_kb_delta/b.memory_kb_delta as mem_delta_ratio,
        r.memory_kb_peak/b.memory_kb_peak as mem_peak_ratio,
    -- mem_query_ratio: per-query RSS delta
        r.memory_kb_query_delta/b.memory_kb_query_delta as mem_query_ratio,
        b.memory_kb_delta as base_mem_kb_delta,
        b.memory_kb_peak as base_mem_kb_peak,
        r.memory_kb_peak as mem_kb_peak,
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

def _ratio_cell(lib, ratio_col, ours_col, theirs_col, unit_divisor=1, places=0):
    """One cell reading `ratio (ours / theirs)`, so the ratio is never read without its magnitudes."""
    safe = lib.replace("'", "''")
    pick = f"max(case when lib = '{safe}' then {{}} end)"
    ratio = f"round({pick.format(ratio_col)}, 2)"
    cast = "::bigint" if places == 0 else ""
    ours = f"round({pick.format(ours_col)}/{unit_divisor}, {places}){cast}"
    theirs = f"round(max({theirs_col})/{unit_divisor}, {places}){cast}"
    return f"{ratio}::varchar || ' (' || {ours}::varchar || ' / ' || {theirs}::varchar || ')'"


def build_report_query(libs):
    """Build the per-library report columns explicitly, since PIVOT expands into multiple engine statements"""
    columns = []
    for lib in libs:
        time_cell = _ratio_cell(lib, "ms_ratio", "time_ms_avg", "base_time_ms_avg", 1, 1)
        mem_cell = _ratio_cell(lib, "mem_peak_ratio", "mem_kb_peak", "base_mem_kb_peak", 1024, 0)
        columns.append(f'{time_cell} as "{lib} time (ms)"')
        columns.append(f'{mem_cell} as "{lib} mem (MB)"')

    diags = []
    for lib in libs:
        safe = lib.replace("'", "''")
        diags.append(f"round(max(case when lib = '{safe}' then ms_median_ratio end), 2) as \"{lib}_time_med\"")
        diags.append(f"round(max(case when lib = '{safe}' then mem_delta_ratio end), 1) as \"{lib}_mem_delta\"")
        diags.append(f"round(max(case when lib = '{safe}' then mem_query_ratio end), 1) as \"{lib}_mem_query\"")

    column_sql = ",\n        ".join(columns)
    diag_sql = ",\n        ".join(diags)
    return f"""
    with pivoted as (
        select test_name, mode,
        {column_sql},
        {diag_sql}
        from result_vs_baseline
        group by test_name, mode
    ),
    gaps as (
        select test_name, mode, string_agg(lib, ',' order by lib) as no_data
        from result_vs_baseline where missing group by test_name, mode
    )
    select b.test_name as test,
        b.mode,
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
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    results_dir = Path(args.results_dir)

    with bareduckdb.connect() as conn:
        prefix = str(results_dir).replace("\\", "/")
        try:
            conn.execute(SETUP_STATEMENTS[0].replace("RESULTS_DIR", prefix))
        except Exception as exc:
            print("# No benchmark data")
            print()
            print(f"No readable `*.jsonl` was found in `{results_dir}`.")
            print(f"\n```\n{exc}\n```")
            return 1

        records = conn.execute("select count(*) from all_results_raw").fetchall()[0][0]
        if not records:
            print("# No benchmark data")
            print()
            print(f"No records were read from `{results_dir}`.")
            print("The benchmark arms produced no output, so there is nothing to compare.")
            return 1

        present = {row[0] for row in conn.execute("describe all_results_raw").fetchall()}
        absent = sorted({"timestamp", "nodeid"} - present)
        if absent:
            print("# Malformed benchmark data")
            print()
            print(f"Records in `{results_dir}` are missing: {', '.join(absent)}.")
            print()
            print(f"Columns found: {', '.join(sorted(present))}")
            return 1

        for statement in SETUP_STATEMENTS[1:]:
            conn.execute(statement.replace("RESULTS_DIR", prefix))

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

    # Split the wide diagnostic columns out so the headline table stays readable.
    diag_cols = [c for c in df.columns if c.endswith(("_time_med", "_mem_delta", "_mem_query"))]
    key_cols = ["test", "mode"]
    main = df[[c for c in df.columns if c not in diag_cols]]

    print("## Benchmark Results\n")
    print(main.to_markdown(index=False))
    print("\n_each cell is `ratio (ours / theirs)`; ratio < 1 means bareduckdb is better_")
    print("_time is the mean of N reps in ms; mem is ABSOLUTE peak RSS in MB, which is what shows whether a source is materialized_")
    print("_each test is forked, so a peak includes the parent RSS it inherited: compare across rows and sizes, not against zero_")
    print("_`no_data` names any library with no results for that case, so gaps are visible rather than dropped_")

    if diag_cols:
        print("\n<details><summary>Diagnostics (median time ratio, rusage delta, sampled per-query RSS)</summary>\n")
        print(df[key_cols + diag_cols].to_markdown(index=False))
        print("\n_`time_med` disagreeing with the mean ratio above is run-to-run noise, not a regression_")
        print("_`mem_delta` is the rusage high-water RISE across the timed call; it hides work done outside that call, which is why it is not the headline_")
        print("\n</details>")

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
