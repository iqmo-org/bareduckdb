-- expected_len > 0
SELECT * FROM (
    SELECT category,
           date,
           price,
           row_number() OVER (PARTITION BY category ORDER BY price DESC) as rank_in_category
    FROM DATA_CATEGORY_DATE_PRICE
) WHERE rank_in_category <= 100
