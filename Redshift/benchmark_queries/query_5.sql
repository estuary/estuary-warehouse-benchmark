-- Query 5
-- Description: Ranks customers by total spent and total quantity within each month and filters for the top 3 customers based on either ranking.
-- Difficulty: Medium
WITH customer_sales AS (
    SELECT
        TO_CHAR(o.o_orderdate, 'YYYY - Mon') AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Mon') ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Mon') ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM orders o
    JOIN  lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN customer c ON o.o_custkey = c.c_custkey
    GROUP BY TO_CHAR(o.o_orderdate, 'YYYY - Mon'), c.c_name
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;