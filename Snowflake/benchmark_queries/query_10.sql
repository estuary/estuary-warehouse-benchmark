-- Query 10
-- Description: This query retrieves the top customers based on the average revenue per order and shipped order ratio, filtering out customers with a shipped order ratio below 50%
-- Difficulty: Hard
WITH customer_orders AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        COUNT(*) AS total_orders,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        SUM(CASE WHEN l.l_shipdate <= DATEADD(day, 30, o.o_orderdate) THEN 1 ELSE 0 END) AS shipped_orders 
    FROM snowflake_sample_data.tpch_sf1000.customer c
    JOIN snowflake_sample_data.tpch_sf1000.orders o ON c.c_custkey = o.o_custkey
    JOIN snowflake_sample_data.tpch_sf1000.lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= DATEADD(year, -30, CURRENT_DATE) 
    GROUP BY c.c_custkey, c.c_name, o.o_orderdate
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
SELECT 
    c_custkey,
    c_name,
    total_orders_per_customer,
    total_revenue_per_customer,
    avg_revenue_per_order,
    shipped_ratio * 100 AS shipped_percentage 
FROM customer_ratios
WHERE shipped_ratio > 0.5 
ORDER BY avg_revenue_per_order DESC;
