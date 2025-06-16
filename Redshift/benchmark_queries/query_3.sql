-- Query 3
-- Description: Selects line item details including the next line's extended price, the previous line's ship date, and the first line's extended price within each order using window functions.
-- Difficulty: Medium
SELECT
        l_orderkey,
        l_linenumber,
        l_shipdate,
        l_extendedprice,
        LEAD(l_extendedprice)
            OVER (PARTITION BY l_orderkey
                ORDER BY l_linenumber)                     AS next_line_price,
        LAG(l_shipdate)
            OVER (PARTITION BY l_orderkey
                ORDER BY l_linenumber)                     AS prev_ship_date,
        FIRST_VALUE(l_extendedprice)
            OVER (PARTITION BY l_orderkey
                ORDER BY l_linenumber
                ROWS BETWEEN UNBOUNDED PRECEDING
                        AND UNBOUNDED FOLLOWING)         AS first_line_price
        FROM
        lineitem
        ORDER BY
        l_orderkey,
        l_linenumber
        LIMIT 1000;