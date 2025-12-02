SELECT
    'This is a longer string value with some repeated content: ' ||
    repeat('data ', 10) || i::VARCHAR as value
FROM DATA_RANGE t(i)
WHERE i%3 = 0
