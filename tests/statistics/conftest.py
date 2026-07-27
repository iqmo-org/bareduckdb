import bareduckdb

# Statistics injection requires the holder_scan experimental feature
if not bareduckdb.features["holder_scan"]:
    collect_ignore_glob = ["test_*.py"]
