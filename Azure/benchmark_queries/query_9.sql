
-- Query-9
WITH customer_orders AS (
    SELECT
        TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')) AS c_custkey,
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_name') AS c_name,
        TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')) AS o_orderdate,
        SUM(TRY_CONVERT(FLOAT, JSON_VALUE(l.LINEITEM_JSON, '$.l_extendedprice')) *
            (1 - TRY_CONVERT(FLOAT, JSON_VALUE(l.LINEITEM_JSON, '$.l_discount')))) AS total_revenue,
        COUNT_BIG(*) AS order_count,
        ROW_NUMBER() OVER (PARTITION BY TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey'))
                        ORDER BY TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')) ASC) AS order_rank
    FROM [your_schema].JCUSTOMER c
    JOIN [your_schema].JORDERS o
        ON TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')) = TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_custkey'))
    JOIN [your_schema].JLINEITEM l
        ON TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_orderkey')) = TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_orderkey'))
    WHERE TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')) >= DATEADD(year, -30, GETDATE())
    AND TRY_CONVERT(DATE, JSON_VALUE(l.LINEITEM_JSON, '$.l_shipdate')) > TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate'))
    GROUP BY TRY_CONVERT(INT, JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')),
            JSON_VALUE(c.CUSTOMER_JSON, '$.c_name'),
            TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate'))
),
cumulative_revenue AS (
    SELECT
        c_custkey,
        c_name,
        order_rank,
        o_orderdate,
        total_revenue,
        SUM(total_revenue) OVER (PARTITION BY c_custkey ORDER BY order_rank ASC) AS cumulative_revenue
    FROM customer_orders
),
monthly_analysis AS (
    SELECT
        c_custkey,
        c_name,
        DATEFROMPARTS(YEAR(o_orderdate), MONTH(o_orderdate), 1) AS order_month,
        COUNT_BIG(*) AS monthly_orders,
        SUM(total_revenue) AS monthly_revenue
    FROM cumulative_revenue
    GROUP BY c_custkey, c_name, DATEFROMPARTS(YEAR(o_orderdate), MONTH(o_orderdate), 1)
)
SELECT TOP 1000
    m.c_custkey,
    m.c_name,
    STRING_AGG(
        'Month: ' + FORMAT(order_month, 'yyyy-MM') +
        ', Orders: ' + CAST(monthly_orders AS VARCHAR) +
        ', Revenue: ' + FORMAT(monthly_revenue, 'N2'),
        ' | '
    ) WITHIN GROUP (ORDER BY order_month) AS monthly_summary,
    MAX(cumulative_revenue) AS lifetime_revenue
FROM monthly_analysis m
JOIN cumulative_revenue c ON m.c_custkey = c.c_custkey
GROUP BY m.c_custkey, m.c_name
ORDER BY lifetime_revenue DESC;
