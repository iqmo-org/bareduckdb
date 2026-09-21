-- expected_len = 1
-- A moving frame, not a cumulative one: a separate code path from the running aggregates above,
-- and not reducible to a top-N. Aggregated to one row so the window operator is what is timed.
SELECT sum(running) FROM (
    SELECT avg(price) OVER (
        PARTITION BY category ORDER BY date
        ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) AS running
    FROM DATA_CATEGORY_DATE_PRICE
)
