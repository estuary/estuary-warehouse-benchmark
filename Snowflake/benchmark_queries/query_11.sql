-- Query 11
-- Description: This query retrieves orders and their aggregated line item details
-- Difficulty: Medium(JSON)

SELECT 
    orders.orders:o_orderkey::INT AS orderkey, 
    orders.orders:o_orderdate::DATE AS orderdate, 
    orders.orders:o_totalprice::DECIMAL AS totalprice, 
    lineitems.total_revenue, 
    lineitems.avg_discount
FROM (
      SELECT 
        lineitem:l_orderkey::INT AS orderkey, 
        SUM(lineitem:l_extendedprice::DECIMAL * (1 - lineitem:l_discount::DECIMAL)) AS total_revenue,
        AVG(lineitem:l_discount::DECIMAL) AS avg_discount
    FROM snowflake_sample_data.tpch_sf1000.jlineitem
    GROUP BY lineitem:l_orderkey
) AS lineitems
JOIN snowflake_sample_data.tpch_sf1000.jorders AS orders
    ON lineitems.orderkey = orders.orders:o_orderkey::INT
WHERE lineitems.total_revenue > 50000
AND lineitems.avg_discount < 0.05
ORDER BY lineitems.total_revenue DESC;
