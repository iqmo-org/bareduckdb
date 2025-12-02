SELECT
    category,
    COUNT(*) as cnt,
    SUM(price) as total_price,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM DATA_CATEGORY_DATE_PRICE
GROUP BY category
ORDER BY total_price DESC
