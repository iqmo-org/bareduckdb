SELECT
    i::HUGEINT as huge_val,
    uuid() as uuid_val,
    (i * 1.5)::DECIMAL(28,6) as decimal_val,
    'string_' || i::VARCHAR as str_val
FROM DATA_RANGE t(i)
