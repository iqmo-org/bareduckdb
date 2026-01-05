-- expected_len > 0
SELECT *
FROM DATA_CATEGORY_DATE_PRICE
WHERE price BETWEEN 3000 AND 7000
  AND date >= CURRENT_DATE - INTERVAL 180 DAY
  AND category IN ('0_cat', '1_cat', '2_cat', '3_cat', '4_cat')
