-- Query 4
-- Description: Aggregates line item and order data by ship year/month, ship mode, and order priority, then ranks the results using row numbering based on sum and average within partitions.
-- Difficulty: Medium
WITH base_table AS (
    SELECT
        TO_CHAR(l_shipdate, 'YYYY-MM') AS ship_year_month,
        l_shipmode AS L_SHIPMODE,
        o_orderpriority AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(l_extendedprice) AS sum,
        AVG(l_discount) AS avg
    FROM lineitem li
    LEFT JOIN orders ord
        ON li.l_orderkey = ord.o_orderkey
    GROUP BY ship_year_month, l_shipmode, o_orderpriority
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table
LIMIT 1000;