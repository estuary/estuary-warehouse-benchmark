-- Query 11
-- Description: Joins orders and line item aggregates to find orders with total revenue over 50000 and average discount less than 0.05.
-- Difficulty: Medium
SELECT
        o.o_orderkey AS orderkey,
        o.o_orderdate AS orderdate,
        o.o_totalprice AS totalprice,
        lineitems.total_revenue,
        lineitems.avg_discount
    FROM (
        SELECT
            l_orderkey AS orderkey,
            SUM(l_extendedprice * (1 - l_discount)) AS total_revenue,
            AVG(l_discount) AS avg_discount
        FROM lineitem
        GROUP BY l_orderkey
    ) AS lineitems
    JOIN orders o
        ON lineitems.orderkey = o.o_orderkey
    WHERE lineitems.total_revenue > 50000
    AND lineitems.avg_discount < 0.05
    ORDER BY lineitems.total_revenue DESC
    LIMIT 1000;