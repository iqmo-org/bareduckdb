WITH data AS (
    SELECT
        i as id,
        (i % 1000) as group_id,
        (i * 1.5) as value1,
        (i * 2.3) as value2,
        (i % 3) as category
    FROM DATA_RANGE t(i)
),
data2 AS (SELECT * FROM data)
SELECT
    a.id,
    a.value1,
    b.value2,
    a.value1 + b.value2 as combined
FROM data2 a
JOIN data2 b ON a.id = b.id
WHERE a.group_id % 100 = 0
