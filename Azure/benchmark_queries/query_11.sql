
-- Query-11
SELECT TOP 1000
    TRY_CONVERT(INT, JSON_VALUE(orders.ORDERS_JSON, '$.o_orderkey')) AS orderkey,
    TRY_CONVERT(DATE, JSON_VALUE(orders.ORDERS_JSON, '$.o_orderdate')) AS orderdate,
    TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(orders.ORDERS_JSON, '$.o_totalprice')) AS totalprice,
    lineitems.total_revenue,
    lineitems.avg_discount
FROM (
    SELECT
        TRY_CONVERT(INT, JSON_VALUE(LINEITEM_JSON, '$.l_orderkey')) AS orderkey,
        SUM(TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(LINEITEM_JSON, '$.l_extendedprice')) *
            (1 - TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(LINEITEM_JSON, '$.l_discount')))) AS total_revenue,
        AVG(TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(LINEITEM_JSON, '$.l_discount'))) AS avg_discount
    FROM [your_schema].JLINEITEM
    GROUP BY TRY_CONVERT(INT, JSON_VALUE(LINEITEM_JSON, '$.l_orderkey'))
) AS lineitems
JOIN [your_schema].JORDERS AS orders
    ON lineitems.orderkey = TRY_CONVERT(INT, JSON_VALUE(orders.ORDERS_JSON, '$.o_orderkey'))
WHERE lineitems.total_revenue > 50000
AND lineitems.avg_discount < 0.05
ORDER BY lineitems.total_revenue DESC;