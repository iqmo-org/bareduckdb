SELECT
    COUNT(*) as total_count,
    SUM(f.amount) as total_revenue,
    AVG(f.amount) as avg_amount
FROM fact f
INNER JOIN products p ON f.product_id = p.product_id
INNER JOIN customers c ON f.customer_id = c.customer_id
INNER JOIN stores s ON f.store_id = s.store_id
WHERE f.amount > 100
