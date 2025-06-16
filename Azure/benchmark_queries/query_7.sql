
-- Query-7
WITH customer_sales AS (
    SELECT
        FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM') AS order_month,
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_name') AS c_name,
        SUM(TRY_CONVERT(FLOAT, JSON_VALUE(o.ORDERS_JSON, '$.o_totalprice'))) AS total_spent,
        SUM(TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_quantity'))) AS total_quantity,
        RANK() OVER (PARTITION BY FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM')
                    ORDER BY SUM(TRY_CONVERT(FLOAT, JSON_VALUE(o.ORDERS_JSON, '$.o_totalprice'))) DESC) AS price_rank,
        RANK() OVER (PARTITION BY FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM')
                    ORDER BY SUM(TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_quantity'))) DESC) AS quantity_rank
    FROM [your_schema].JORDERS o
    JOIN [your_schema].JLINEITEM l
        ON TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_orderkey')) = TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_orderkey'))
    JOIN [your_schema].JCUSTOMER c
        ON TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_custkey')) = TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey'))
    GROUP BY FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM'),
            JSON_VALUE(c.CUSTOMER_JSON, '$.c_name')
),
number_extraction AS (
    SELECT *,
        REPLACE(REPLACE(REPLACE(c_name, ' ', ''), '-', ''), '[^0-9]', '') AS customer_number
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT
        order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        SUM(TRY_CAST(value AS INT)) AS number_sum
    FROM number_extraction
    CROSS APPLY STRING_SPLIT(customer_number, '')
    WHERE ISNUMERIC(value) = 1
    GROUP BY order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number
)
SELECT TOP 1000 order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)
AND number_sum % 2 = 1
ORDER BY order_month, price_rank, quantity_rank;