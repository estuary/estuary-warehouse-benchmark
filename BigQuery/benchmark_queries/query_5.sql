-- Query 5
-- Description: Ranks customers by total spent and total quantity ordered within each month and filters for the top 3 customers in either ranking.
-- Difficulty: Medium
WITH customer_sales AS (
    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM `@full_dataset.ORDERS` o
    JOIN `@full_dataset.LINEITEM` l ON o.o_orderkey = l.l_orderkey
    JOIN `@full_dataset.CUSTOMER` c ON o.o_custkey = c.c_custkey
    GROUP BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)), c.c_name, o.o_orderdate
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000