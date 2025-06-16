
-- Query-4
WITH base_table AS (
    SELECT
        FORMAT(TRY_CONVERT(DATE, JSON_VALUE(LI.LINEITEM_JSON, '$.l_shipdate')), 'yyyy-MM') AS ship_year_month,
        JSON_VALUE(LI.LINEITEM_JSON, '$.l_shipmode') AS L_SHIPMODE,
        JSON_VALUE(ORD.ORDERS_JSON, '$.o_orderpriority') AS order_priority,
        COUNT_BIG(*) AS count_of_line_items,
        SUM(TRY_CONVERT(FLOAT, JSON_VALUE(LI.LINEITEM_JSON, '$.l_extendedprice'))) AS sum,
        AVG(TRY_CONVERT(FLOAT, JSON_VALUE(LI.LINEITEM_JSON, '$.l_discount'))) AS avg
    FROM [your_schema].JLINEITEM LI
    LEFT JOIN [your_schema].JORDERS ORD
        ON TRY_CONVERT(INT, JSON_VALUE(LI.LINEITEM_JSON, '$.l_orderkey')) = TRY_CONVERT(INT, JSON_VALUE(ORD.ORDERS_JSON, '$.o_orderkey'))
    GROUP BY FORMAT(TRY_CONVERT(DATE, JSON_VALUE(LI.LINEITEM_JSON, '$.l_shipdate')), 'yyyy-MM'),
            JSON_VALUE(LI.LINEITEM_JSON, '$.l_shipmode'),
            JSON_VALUE(ORD.ORDERS_JSON, '$.o_orderpriority')
)
SELECT TOP 1000
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table
ORDER BY ship_year_month, L_SHIPMODE, order_priority;
