-- expected_len > 0
SELECT category, count(*) as cnt, avg(price) as avg_price
FROM DATA_CATEGORY_DATE_PRICE
GROUP BY category
