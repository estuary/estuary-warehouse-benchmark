-- Query 3
-- Description: This query retrieves order details from the lineitem table, including the current, next, and first extended prices, as well as the previous shipping date, using window functions for analysis.
-- Difficulty: Easy

SELECT 
    L_ORDERKEY, 
    L_LINENUMBER, 
    L_SHIPDATE, 
    L_EXTENDEDPRICE, 
    LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
    LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
    FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
FROM snowflake_sample_data.tpch_sf1000.lineitem
ORDER BY L_ORDERKEY, L_LINENUMBER;