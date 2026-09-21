-- expected_len = 1
-- Four window functions over two different specs, so the engine sorts the input twice and cannot
-- share a single window state. The heaviest window shape in the suite; aggregated to one row so
-- the window operator is what is timed, not the output.
SELECT sum(rn + rk + prev + running) FROM (
    SELECT row_number() OVER w_price AS rn,
           rank() OVER w_price AS rk,
           coalesce(lag(price, 1) OVER w_date, 0) AS prev,
           avg(price) OVER w_date AS running
    FROM DATA_CATEGORY_DATE_PRICE
    WINDOW w_price AS (PARTITION BY category ORDER BY price),
           w_date AS (PARTITION BY date ORDER BY price)
)
