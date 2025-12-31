-- expected_len > 0
WITH data AS (
    SELECT
        i as id,
        (i % 1000) as group_id,
        (i * 1.5) as value1,
        (i * 2.3) as value2,
        (i % 3) as category
    FROM DATA_RANGE t(i)
)
SELECT
    group_id,
    category,
    COUNT(*) as cnt,
    SUM(value1) as sum_v1,
    AVG(value2) as avg_v2,
    MIN(value1) as min_v1,
    MAX(value2) as max_v2
FROM data
GROUP BY group_id, category
HAVING COUNT(*) > 100
ORDER BY cnt DESC
