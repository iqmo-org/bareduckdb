#!/usr/bin/env python3

import re
from pathlib import Path

DATA_DIR = Path("testdata")
CASES_DIR = Path(__file__).parent / "cases"

# Mapping of SQL placeholder names to actual parquet files
DATA_FILE_MAP = {
    "DATA_CATEGORY_DATE_PRICE": DATA_DIR / "category_date_price.parquet",
    "DATA_STRINGS": DATA_DIR / "strings_1m.parquet",
    "DATA_RANGE": DATA_DIR / "range_100m.parquet",
}


def replace_data_placeholders(sql: str) -> str:
    """Replace DATA_* placeholders with actual file paths."""
    for placeholder, filepath in DATA_FILE_MAP.items():
        sql = sql.replace(placeholder, f"'{filepath}'")
    return sql


def parse_sql_case(path: Path) -> tuple[str, str | None]:
    """Parse SQL file, return (sql, expected_len expression or None)."""
    content = path.read_text()
    lines = content.strip().split("\n")

    expected = None
    if lines and lines[0].startswith("--"):
        match = re.match(r"--\s*expected_len\s*([=<>!]+\s*\d+)", lines[0], re.IGNORECASE)
        if match:
            expected = match.group(1).strip()
            lines = lines[1:]

    sql = "\n".join(lines).strip()
    return replace_data_placeholders(sql), expected


def discover_sql_cases() -> list[tuple[str, Path]]:
    """Discover all SQL case files, return list of (test_id, path)."""
    cases = []
    for sql_file in sorted(CASES_DIR.rglob("*.sql")):
        # test_id: category/name (without .sql)
        rel = sql_file.relative_to(CASES_DIR)
        test_id = str(rel.with_suffix("")).replace("/", "_")
        cases.append((test_id, sql_file))
    return cases


PARQUET_DEFINITIONS = {
    # t1 and t2 are meant to be used together
    "t1.parquet": f"""
        SELECT i AS value, i % 1000 AS t2_id
        FROM range(0, 1000000) AS r(i)
    """,
    "t2.parquet": f"""
        SELECT i AS id, lpad(CAST(i AS VARCHAR), 6, '0') AS code
        FROM range(0, 1000) AS r(i)
    """,
    "range_100m.parquet": """
        SELECT * FROM range(100000000) t(i)
    """,
    "category_date_price.parquet": """
        SELECT
            category || '_cat' AS category,
            date,
            price
        FROM range(today()-interval 1 year, today(), interval 1 day) z(date),
             range(10) t(category),
             range(10000) t(price)
    """,
    "strings_1m.parquet": """
        SELECT
            i as id,
            'user_' || (i % 1000)::VARCHAR as username,
            CASE
                WHEN i % 10 = 0 THEN 'admin'
                WHEN i % 5 = 0 THEN 'moderator'
                ELSE 'user'
            END as role,
            'email_' || i::VARCHAR || '@example.com' as email,
            repeat('x', 50 + (i % 100)) as description,
            CASE (i % 5)
                WHEN 0 THEN 'United States'
                WHEN 1 THEN 'United Kingdom'
                WHEN 2 THEN 'Germany'
                WHEN 3 THEN 'France'
                ELSE 'Japan'
            END as country
        FROM generate_series(1, 1000000) t(i)
    """,
}


def setup_data(data_dir: Path | None = None, force: bool = False):
    import bareduckdb

    if data_dir is None:
        data_dir = DATA_DIR

    data_dir.mkdir(exist_ok=True)

    with bareduckdb.connect() as conn:
        for filename, query in PARQUET_DEFINITIONS.items():
            filepath = data_dir / filename

            if filepath.exists() and not force:
                continue

            print(f"Creating {filepath}...")

            conn.execute(f"COPY ({query}) TO '{filepath}' (FORMAT PARQUET)")


def clean_data(data_dir: Path | None = None):
    """Remove all benchmark data files."""
    if data_dir is None:
        data_dir = DATA_DIR

    for filename in PARQUET_DEFINITIONS.keys():
        filepath = data_dir / filename
        if filepath.exists():
            print(f"Removing {filepath}")
            filepath.unlink()


if __name__ == "__main__":
    import sys

    if "--clean" in sys.argv:
        clean_data()
        setup_data(force=True)
    elif "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        print("Data files to create:")
        for filename, query in PARQUET_DEFINITIONS.items():
            print(f"  {DATA_DIR / filename}")
    else:
        setup_data()
