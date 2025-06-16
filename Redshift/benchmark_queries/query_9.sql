-- Query 9
-- Description: Analyzes customer order history over the last 30 years, calculating monthly and cumulative revenue and order counts, and presenting a monthly summary.
-- Difficulty: Hard
WITH customer_orders AS (
        SELECT
            c.c_custkey,
            c.c_name,
            o.o_orderdate,
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
            COUNT(*) AS order_count,
            ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate ASC) AS order_rank
        FROM customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATEADD(year, -30, CURRENT_DATE)
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
                SUM(total_revenue) OVER (
                    PARTITION BY c_custkey
                    ORDER BY order_rank ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_revenue
            FROM customer_orders
        ),
        monthly_analysis AS (
            SELECT
                c_custkey,
                c_name,
                DATE_TRUNC('month', o_orderdate) AS order_month,
                COUNT(*) AS monthly_orders,
                SUM(total_revenue) AS monthly_revenue
            FROM cumulative_revenue
            GROUP BY c_custkey, c_name, DATE_TRUNC('month', o_orderdate)
        )
        SELECT
            m.c_custkey,
            m.c_name,
            LISTAGG(
                'Month: ' || TO_CHAR(order_month, 'YYYY-MM') ||
                ', Orders: ' || monthly_orders ||
                ', Revenue: ' || TO_CHAR(monthly_revenue, 'FM999,999.99'),
                ' | '
            ) WITHIN GROUP (ORDER BY order_month) AS monthly_summary,
            MAX(cumulative_revenue) AS lifetime_revenue
        FROM monthly_analysis m
        JOIN cumulative_revenue c ON m.c_custkey = c.c_custkey
        GROUP BY m.c_custkey, m.c_name
        ORDER BY lifetime_revenue DESC
        LIMIT 1000;