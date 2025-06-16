-- Query 7
-- Description: Filters for top customers based on spending or quantity per month and then identifies those whose numeric digits within their name sum to an odd number using string manipulation and TRY_CAST.
-- Difficulty: Hard
WITH customer_sales AS (
    SELECT
        DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') AS order_month,
        C.C_NAME AS C_NAME,
        SUM(CAST(O.O_TOTALPRICE AS FLOAT)) AS total_spent,
        SUM(CAST(L.L_QUANTITY AS INT)) AS total_quantity,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(CAST(O.O_TOTALPRICE AS FLOAT)) DESC) AS price_rank,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(CAST(L.L_QUANTITY AS INT)) DESC) AS quantity_rank
    FROM {database}.{schema}.orders O
    JOIN {database}.{schema}.lineitem L
        ON O.O_ORDERKEY = L.L_ORDERKEY
    JOIN {database}.{schema}.customer C
        ON O.O_CUSTKEY = C.C_CUSTKEY
    GROUP BY order_month, C.C_NAME
),
number_extraction AS (
    SELECT *,
        REGEXP_REPLACE(C_NAME, '[^0-9]', '') AS customer_number
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT
        order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        SUM(try_cast(word AS INT)) AS number_sum
    FROM number_extraction
    LATERAL VIEW EXPLODE(SPLIT(customer_number, '')) AS word
    GROUP BY order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank, customer_number
)
SELECT order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)
AND MOD(number_sum, 2) = 1
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;

