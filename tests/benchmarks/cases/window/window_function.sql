-- expected_len = 10000
SELECT date, category, avg(price) OVER (PARTITION BY category ORDER BY date) as running_avg
FROM DATA_CATEGORY_DATE_PRICE
LIMIT 10000
