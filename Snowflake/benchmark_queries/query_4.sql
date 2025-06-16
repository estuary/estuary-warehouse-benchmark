-- Query 4
-- Description: Left joins Orders and Lineitem tables to aggregate metrics as CTE and rank them using ROW_NUMBER() window function
-- Difficulty: Medium(JSON)  


WITH base_table AS (
    SELECT 
        TO_CHAR(lineitem:l_shipdate::DATE, 'YYYY-MM') AS ship_year_month,
        lineitem:l_shipmode::STRING AS L_SHIPMODE,
        orders:o_orderpriority::STRING AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(lineitem:l_extendedprice::FLOAT) AS sum,
        AVG(lineitem:l_discount::FLOAT) AS avg
    FROM snowflake_sample_data.tpch_sf1000.jlineitem li
    LEFT JOIN snowflake_sample_data.tpch_sf1000.jorders ord 
        ON li.lineitem:l_orderkey::INT = ord.orders:o_orderkey::INT
    GROUP BY ship_year_month, L_SHIPMODE, order_priority
)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table;

