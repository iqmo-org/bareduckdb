-- expected_len > 0
SELECT count(*) FROM DATA_STRINGS
WHERE country IN ('United States', 'Germany', 'Japan')
