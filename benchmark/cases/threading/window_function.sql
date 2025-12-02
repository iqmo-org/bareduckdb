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
    id,
    group_id,
    value1,
    ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY value1) as rn,
    SUM(value1) OVER (PARTITION BY group_id ORDER BY id) as running_sum,
    AVG(value2) OVER (PARTITION BY group_id) as group_avg
FROM data
WHERE group_id < 500
