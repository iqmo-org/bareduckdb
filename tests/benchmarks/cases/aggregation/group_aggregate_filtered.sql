-- expected_len > 0
SELECT category,
       count(*) as cnt,
       avg(price) as avg_price,
       sum(price) as total_price
FROM DATA_CATEGORY_DATE_PRICE
WHERE price > 5000
GROUP BY category
