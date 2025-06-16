-- Query 11
-- Description: Joins orders and line item aggregates to find orders with total revenue over 50000 and average discount less than 0.05.
-- Difficulty: Medium
SELECT
    orders.O_ORDERKEY AS orderkey,
    orders.O_ORDERDATE AS orderdate,
    orders.O_TOTALPRICE AS totalprice,
    lineitems.total_revenue,
    lineitems.avg_discount
FROM (
    SELECT
        L_ORDERKEY AS orderkey,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue,
        AVG(L_DISCOUNT) AS avg_discount
    FROM `@full_dataset.LINEITEM`
    GROUP BY L_ORDERKEY
) AS lineitems
JOIN `@full_dataset.ORDERS` AS orders
    ON lineitems.orderkey = orders.O_ORDERKEY
WHERE lineitems.total_revenue > 50000
AND lineitems.avg_discount < 0.05
ORDER BY lineitems.total_revenue DESC
LIMIT 1000