WITH data AS (
    SELECT
        i as id,
        (i * 2654435761) % 1000000000 as shuffled_value,
        i * 1.5 as price,
        'category_' || (i % 100)::VARCHAR as category
    FROM DATA_RANGE t(i)
)
SELECT * FROM data
ORDER BY shuffled_value DESC
LIMIT 10000
