#!/usr/bin/env python3
"""Generate benchmark comparison table from JSONL results."""

import sys
from pathlib import Path

import bareduckdb


def main():
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("benchmark-results")

    regexp = r"(.*)::([^\[]+?)(\[((.*)-)?(\d+)-(\d+)\])?$"

    query = rf"""
    create or replace table all_results as
    select * exclude (test, timestamp),
        if(test like '%[%', regexp_extract(test, '{regexp}', 2), test) as pytest,
        if(test like '%[%', regexp_extract(test, '{regexp}', 5), test) as test_name_raw,
        -- Strip mode prefix from test_name (e.g., "parquet-limits_topn_small" -> "limits_topn_small")
        if(test_name_raw is not null and test_name_raw like mode || '-%',
           regexp_replace(test_name_raw, '^' || mode || '-', ''),
           test_name_raw) as test_name,
        if(test_name is not null and len(test_name)>0, test_name, pytest) as test,
        if(test like '%[%', regexp_extract(test, '{regexp}', 6)::int, 1) as test_run,
        case when bench is not null then bench
            when library = 'duckdb' then library
            when library = 'bareduckdb' and 'dev' in lib_version then 'bareduckdb_dev'
            else library
        end as lib,
    from
    read_json('RESULTS_DIR/*.jsonl', filename=True);

    create or replace table latest_results as
    select * from all_results where filename in
    (select max(filename) from all_results group by lib, lib_version)
    ;
    create or replace table result_stats as
    select
        lib,
        test,
        mode,
        avg(wall_time_s)*1000 as time_ms_avg,
        avg(rusage_maxrss_delta_kb) as memory_kb_delta,
        count(*) num_tests
     from latest_results
     group by lib, test, mode
    ;

    create or replace table baseline as (select * from result_stats where lib='duckdb');

    create or replace table result_vs_baseline as
    select r.*, r.time_ms_avg/b.time_ms_avg as ms_ratio, r.memory_kb_delta/b.memory_kb_delta as mem_ratio
    from result_stats r
    join baseline b 
    on b.test=r.test
        and b.mode=r.mode
    order by r.test, r.mode, r.lib
    ;
    

    with time_pivoted as (
    pivot result_vs_baseline on lib using round(last(ms_ratio),2) as time group by test
    ),
    mem_pivoted as (
    pivot result_vs_baseline on lib using round(last(mem_ratio),1) as mem group by test
    )
    select b.test,
        b.mode,
        round(b.time_ms_avg,1) base_ms,
        round(b.memory_kb_delta,1) base_kb,
        t.* exclude (test, duckdb_time),
        m.* exclude (test, duckdb_mem) 
    from baseline b
    join mem_pivoted m on m.test=b.test
    join time_pivoted t on t.test=b.test 
    order by b.test, b.mode
    """.replace("RESULTS_DIR", str(results_dir))

    with bareduckdb.connect() as conn:
        df = conn.execute(query).df()

        df_check = conn.execute("select filename, pid, count(*) c from latest_results group by filename, pid having c > 1").df()

    print("## Benchmark Results\n")
    print(df.to_markdown(index=False))
    print("\n_time_ratio < 1 means bareduckdb is faster_")

    if len(df_check) > 0:
        print("\n**WARNING: Fork isolation issue detected!** Multiple tests ran in same process:\n")
        print(df_check.to_markdown(index=False))


if __name__ == "__main__":
    main()
