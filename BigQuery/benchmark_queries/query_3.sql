-- Query 3
-- Description: Selects line item details including the next line's extended price, the previous line's ship date, and the first line's extended price within each order, using window functions.
-- Difficulty: Medium
SELECT
    L_ORDERKEY,
    L_LINENUMBER,
    L_SHIPDATE,
    L_EXTENDEDPRICE,
    LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
    LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
    FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
FROM `@full_dataset.LINEITEM`
ORDER BY L_ORDERKEY, L_LINENUMBER
LIMIT 1000