-- Query 5
-- Description: This query joins 3 tables & retrieves the top 3 customers based on total spent and total quantity for each month, ranking them using RANK() window function
-- Difficulty: Medium 
WITH customer_sales AS (
    SELECT 
        TO_CHAR(o.o_orderdate, 'YYYY - Month') AS order_month,  -- Formatting Year and Month Name
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Month') ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Month') ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM snowflake_sample_data.tpch_sf1000.orders o
    JOIN snowflake_sample_data.tpch_sf1000.lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN snowflake_sample_data.tpch_sf1000.customer c ON o.o_custkey = c.c_custkey
    GROUP BY order_month, c.c_name
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank;
