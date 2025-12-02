-- TIMESTAMP with interval calculations benchmark
-- Expected: ~8.6 seconds for 5M rows
-- Tests TIMESTAMP arithmetic and conversion performance
SELECT '2024-01-01'::TIMESTAMP + INTERVAL (i) SECOND as value
FROM DATA_RANGE
