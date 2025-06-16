-- Query 10
-- Description: Calculates various customer metrics including total orders, total revenue, average revenue per order, and the percentage of orders shipped within 30 days, filtering for customers with a shipped ratio above 50%.
-- Difficulty: Hard
WITH customer_orders AS (
    SELECT
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        COUNT(*) AS total_orders,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        SUM(CASE WHEN l.l_shipdate <= DATE_ADD(o.o_orderdate, INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS shipped_orders
    FROM `@full_dataset.CUSTOMER` c
    JOIN `@full_dataset.ORDERS` o ON c.c_custkey = o.o_custkey
    JOIN `@full_dataset.LINEITEM` l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 YEAR)
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
        CAST(total_shipped_orders_per_customer AS FLOAT64) / NULLIF(total_orders_per_customer, 0) AS shipped_ratio
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
ORDER BY avg_revenue_per_order DESC
LIMIT 1000