-- Query 4
-- Description: Calculates aggregated line item data by shipping year/month, ship mode, and order priority, then applies row numbering based on sum and average within partitions.
-- Difficulty: Medium
WITH base_table AS (
    SELECT
        FORMAT_DATE('%Y-%m', DATE(L_SHIPDATE)) AS ship_year_month,
        L_SHIPMODE,
        O_ORDERPRIORITY AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(L_EXTENDEDPRICE) AS sum,
        AVG(L_DISCOUNT) AS avg
    FROM `@full_dataset.LINEITEM` li
    LEFT JOIN `@full_dataset.ORDERS` ord
        ON li.L_ORDERKEY = ord.O_ORDERKEY
    GROUP BY ship_year_month, L_SHIPMODE, order_priority
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table
LIMIT 1000