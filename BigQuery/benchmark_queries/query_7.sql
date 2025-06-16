-- Query 7
-- Description: Identifies customers who rank in the top 3 by total spent or total quantity each month and whose customer number digits sum to an odd number.
-- Difficulty: Hard
WITH customer_sales AS (
    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM `@full_dataset.ORDERS` o
    JOIN `@full_dataset.LINEITEM` l
        ON o.o_orderkey = l.l_orderkey
    JOIN `@full_dataset.CUSTOMER` c
        ON o.o_custkey = c.c_custkey
    GROUP BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)), c.c_name, o.o_orderdate
),
number_extraction AS (
    SELECT *,
        REGEXP_REPLACE(c_name, r'[^0-9]', '') AS customer_number
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT
        order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        (SELECT SUM(CAST(digit AS INT64))
        FROM UNNEST(SPLIT(customer_number, '')) digit
        ) AS number_sum
    FROM number_extraction
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)
AND MOD(number_sum, 2) = 1
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000