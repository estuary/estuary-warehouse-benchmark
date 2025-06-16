-- Query 9
-- Description: Analyzes customer order history over the last 30 years, calculating monthly order counts and revenue, cumulative revenue, and lifetime revenue for each customer.
-- Difficulty: Hard
WITH customer_orders AS (
    SELECT
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        COUNT(*) AS order_count,
        ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate ASC) AS order_rank
    FROM `@full_dataset.CUSTOMER` c
    JOIN `@full_dataset.ORDERS` o ON c.c_custkey = o.o_custkey
    JOIN `@full_dataset.LINEITEM` l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 YEAR)
      AND l.l_shipdate > o.o_orderdate
    GROUP BY c.c_custkey, c.c_name, o.o_orderdate
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
        DATE_TRUNC(o_orderdate, MONTH) AS order_month,
        COUNT(*) AS monthly_orders,
        SUM(total_revenue) AS monthly_revenue
    FROM cumulative_revenue
    GROUP BY c_custkey, c_name, DATE_TRUNC(o_orderdate, MONTH)
)
SELECT
    m.c_custkey,
    m.c_name,
    STRING_AGG(
        CONCAT(
            'Month: ', FORMAT_DATE('%Y-%m', order_month),
            ', Orders: ', CAST(monthly_orders AS STRING),
            ', Revenue: ', FORMAT('%.2f', monthly_revenue)
        ),
        ' | '
        ORDER BY order_month
    ) AS monthly_summary,
    MAX(cumulative_revenue) AS lifetime_revenue
FROM monthly_analysis m
    JOIN cumulative_revenue c ON m.c_custkey = c.c_custkey
    GROUP BY m.c_custkey, m.c_name
    ORDER BY lifetime_revenue DESC
    LIMIT 1000