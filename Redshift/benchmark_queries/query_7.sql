-- Query 7
-- Description: Finds customers who are in the top 3 by either total spent or total quantity per month and whose numeric digits in their name sum to an odd number.
-- Difficulty: Hard
WITH customer_sales AS (
    SELECT
        TO_CHAR(o.o_orderdate, 'YYYY - Mon') AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Mon') ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Mon') ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM orders o
    JOIN lineitem l
        ON o.o_orderkey = l.l_orderkey
    JOIN customer c
        ON o.o_custkey = c.c_custkey
    GROUP BY TO_CHAR(o.o_orderdate, 'YYYY - Mon'), c.c_name
),
number_extraction AS (
    SELECT
        order_month,
        c_name,
        total_spent,
        total_quantity,
        price_rank,
        quantity_rank,
        REGEXP_REPLACE(c_name, '[^0-9]', '') AS customer_number
    FROM customer_sales
    WHERE REGEXP_REPLACE(c_name, '[^0-9]', '') != ''
),
digit_sum_calc AS (
    SELECT
        order_month,
        c_name,
        total_spent,
        total_quantity,
        price_rank,
        quantity_rank,
        customer_number,
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 1, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 2, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 3, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 4, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 5, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 6, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 7, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 8, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 9, 1) AS INTEGER), 0) +
        COALESCE(TRY_CAST(SUBSTRING(customer_number, 10, 1) AS INTEGER), 0) AS number_sum
    FROM number_extraction
    WHERE LENGTH(customer_number) <= 10  -- Adjust based on max expected digits
    )
    SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
    FROM digit_sum_calc
    WHERE (price_rank <= 3 OR quantity_rank <= 3)
    AND number_sum % 2 = 1
    ORDER BY order_month, price_rank, quantity_rank
    LIMIT 1000;