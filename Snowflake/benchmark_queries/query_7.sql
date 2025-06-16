-- Query 7
-- Description: Take Query 5 as base query and add a filter to only include customers with an odd sum of digits in their name and get the top 3 customers for each month by total spent or total quantity
-- Difficulty: Hard(JSON)

WITH customer_sales AS (
    SELECT 
        TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') AS order_month,  -- Formatting Year and Month Name
        customer:c_name::STRING AS c_name,
        SUM(orders:o_totalprice::FLOAT) AS total_spent,
        SUM(lineitem:l_quantity::INT) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') ORDER BY SUM(orders:o_totalprice::FLOAT) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') ORDER BY SUM(lineitem:l_quantity::INT) DESC) AS quantity_rank
    FROM snowflake_sample_data.tpch_sf1000.jorders o
    JOIN snowflake_sample_data.tpch_sf1000.jlineitem l 
        ON o.orders:o_orderkey::INT = l.lineitem:l_orderkey::INT
    JOIN snowflake_sample_data.tpch_sf1000.jcustomer c 
        ON o.orders:o_custkey::INT = c.customer:c_custkey::INT
    GROUP BY order_month, c_name
),
number_extraction AS (
    SELECT *,
           REGEXP_REPLACE(c_name, '[^0-9]', '') AS customer_number  
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT 
        order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        SUM(value::INT) AS number_sum 
    FROM number_extraction,
         LATERAL FLATTEN(input => SPLIT(customer_number, ''))  
    GROUP BY order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)  
AND MOD(number_sum, 2) = 1  
ORDER BY order_month, price_rank, quantity_rank;
