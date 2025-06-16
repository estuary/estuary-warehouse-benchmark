
-- Query-10
WITH customer_orders AS (
    SELECT
        TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')) AS c_custkey,
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_name') AS c_name,
        TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')) AS o_orderdate,
        COUNT_BIG(*) AS total_orders,
        SUM(TRY_CONVERT(FLOAT, JSON_VALUE(l.LINEITEM_JSON, '$.l_extendedprice')) *
            (1 - TRY_CONVERT(FLOAT, JSON_VALUE(l.LINEITEM_JSON, '$.l_discount')))) AS total_revenue,
        SUM(CASE WHEN TRY_CONVERT(DATE, JSON_VALUE(l.LINEITEM_JSON, '$.l_shipdate')) <=
                    DATEADD(day, 30, TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')))
                THEN 1 ELSE 0 END) AS shipped_orders
    FROM [your_schema].JCUSTOMER c
    JOIN [your_schema].JORDERS o
        ON TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')) = TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_custkey'))
    JOIN [your_schema].JLINEITEM l
        ON TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_orderkey')) = TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_orderkey'))
    WHERE TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')) >= DATEADD(year, -30, GETDATE())
    GROUP BY TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')),
            JSON_VALUE(c.CUSTOMER_JSON, '$.c_name'),
            TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate'))
),
customer_metrics AS (
    SELECT
        c_custkey,
        c_name,
        SUM(total_orders) AS total_orders_per_customer,
        SUM(total_revenue) AS total_revenue_per_customer,
        SUM(shipped_orders) AS total_shipped_orders_per_customer,
        AVG(total_revenue / NULLIF(total_orders, 0)) AS avg_revenue_per_order
    FROM customer_orders
    GROUP BY c_custkey, c_name
),
customer_ratios AS (
    SELECT
        c_custkey,
        c_name,
        total_orders_per_customer,
        total_revenue_per_customer,
        total_shipped_orders_per_customer,
        avg_revenue_per_order,
        CAST(total_shipped_orders_per_customer AS FLOAT) / NULLIF(total_orders_per_customer, 0) AS shipped_ratio
    FROM customer_metrics
)
SELECT TOP 1000
    c_custkey,
    c_name,
    total_orders_per_customer,
    total_revenue_per_customer,
    avg_revenue_per_order,
    shipped_ratio * 100 AS shipped_percentage
FROM customer_ratios
WHERE shipped_ratio > 0.5
ORDER BY avg_revenue_per_order DESC;
