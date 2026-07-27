import bareduckdb

# UDTFs and replacement scans require SQL parsing
if not bareduckdb.features["sql_parsing"]:
    collect_ignore_glob = ["test_*.py"]
