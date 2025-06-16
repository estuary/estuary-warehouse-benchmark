-- Query 1
-- Description: Basic query to sum the L_EXTENDEDPRICE column.
-- Difficulty: Easy
SELECT
    SUM(L_EXTENDEDPRICE) AS sum
FROM {database}.{schema}.lineitem;-- Query 2
-- Description: Calculates fundamental statistics for line items: count, sum of extended price, average discount, minimum ship date, and maximum receipt date.
-- Difficulty: Easy
SELECT
    COUNT(*) AS count_of_line_items,
    SUM(CAST(L_EXTENDEDPRICE AS FLOAT)) AS sum,
    AVG(CAST(L_DISCOUNT AS FLOAT)) AS avg,
    MIN(CAST(L_SHIPDATE AS DATE)) AS min,
    MAX(CAST(L_RECEIPTDATE AS DATE)) AS max
FROM {database}.{schema}.lineitem;

-- Query 3
-- Description: Uses window functions to fetch line item details along with the extended price of the next item, the ship date of the previous item, and the extended price of the first item within the same order.
-- Difficulty: Medium
SELECT
    L_ORDERKEY,
    L_LINENUMBER,
    L_SHIPDATE,
    L_EXTENDEDPRICE,
    LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
    LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
    FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
FROM {database}.{schema}.lineitem
ORDER BY L_ORDERKEY, L_LINENUMBER
LIMIT 1000;

-- Query 4
-- Description: Joins lineitem and orders tables, aggregates data by shipping year/month, ship mode, and order priority, and then applies row numbering based on aggregated sums and averages within partitions.
-- Difficulty: Medium
WITH base_table AS (
    SELECT
        DATE_FORMAT(L_SHIPDATE, 'yyyy-MM') AS ship_year_month,
        L_SHIPMODE AS L_SHIPMODE,
        O_ORDERPRIORITY AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(CAST(L_EXTENDEDPRICE AS FLOAT)) AS sum,
        AVG(CAST(L_DISCOUNT AS FLOAT)) AS avg
    FROM {database}.{schema}.lineitem LI
    LEFT JOIN {database}.{schema}.orders ORD
        ON LI.L_ORDERKEY = ORD.O_ORDERKEY
    GROUP BY ship_year_month, L_SHIPMODE, order_priority
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table
LIMIT 1000;

-- Query 5
-- Description: Ranks customers by total revenue and total quantity ordered within each month using window functions and filters for the top 3 customers in either ranking.
-- Difficulty: Medium
WITH customer_sales AS (
    SELECT
        DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') AS order_month,
        C.C_NAME,
        SUM(O.O_TOTALPRICE) AS total_spent,
        SUM(L.L_QUANTITY) AS total_quantity,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(O.O_TOTALPRICE) DESC) AS price_rank,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(L.L_QUANTITY) DESC) AS quantity_rank
    FROM {database}.{schema}.orders O
    JOIN {database}.{schema}.lineitem L ON O.O_ORDERKEY = L.L_ORDERKEY
    JOIN {database}.{schema}.customer C ON O.O_CUSTKEY = C.C_CUSTKEY
    GROUP BY order_month, C.C_NAME
)
SELECT order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;

-- Query 6
-- Description: Analyzes combined comments from orders and line items to find the top 5 most frequent words each month, excluding common stop words, using string manipulation and tokenization via LATERAL VIEW EXPLODE.
-- Difficulty: Hard
WITH combined_comments AS (
    SELECT
        DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(O.O_COMMENT, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM {database}.{schema}.orders O

    UNION ALL

    SELECT
        DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(L.L_COMMENT, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM {database}.{schema}.lineitem L
    JOIN {database}.{schema}.orders O
        ON L.L_ORDERKEY = O.O_ORDERKEY
),
tokenized_words AS (
    SELECT
        order_month,
        word
    FROM combined_comments
    LATERAL VIEW EXPLODE(SPLIT(cleaned_comment, ' ')) AS word
),
word_counts AS (
    SELECT
        order_month,
        word,
        COUNT(*) AS word_count,
        RANK() OVER (PARTITION BY order_month ORDER BY COUNT(*) DESC) AS rank
    FROM tokenized_words
    WHERE word NOT IN ('the', 'is', 'and', 'or', 'a', 'an', 'of', 'to', 'in', 'for', 'on', 'with', 'at')
    GROUP BY order_month, word
)
SELECT order_month, word, word_count
FROM word_counts
WHERE rank <= 5
ORDER BY order_month, word_count DESC
LIMIT 1000;

-- Query 7
-- Description: Filters for top customers based on spending or quantity per month and then identifies those whose numeric digits within their name sum to an odd number using string manipulation and TRY_CAST.
-- Difficulty: Hard
WITH customer_sales AS (
    SELECT
        DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') AS order_month,
        C.C_NAME AS C_NAME,
        SUM(CAST(O.O_TOTALPRICE AS FLOAT)) AS total_spent,
        SUM(CAST(L.L_QUANTITY AS INT)) AS total_quantity,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(CAST(O.O_TOTALPRICE AS FLOAT)) DESC) AS price_rank,
        RANK() OVER (PARTITION BY DATE_FORMAT(O.O_ORDERDATE, 'yyyy - MMMM') ORDER BY SUM(CAST(L.L_QUANTITY AS INT)) DESC) AS quantity_rank
    FROM {database}.{schema}.orders O
    JOIN {database}.{schema}.lineitem L
        ON O.O_ORDERKEY = L.L_ORDERKEY
    JOIN {database}.{schema}.customer C
        ON O.O_CUSTKEY = C.C_CUSTKEY
    GROUP BY order_month, C.C_NAME
),
number_extraction AS (
    SELECT *,
        REGEXP_REPLACE(C_NAME, '[^0-9]', '') AS customer_number
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT
        order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        SUM(try_cast(word AS INT)) AS number_sum
    FROM number_extraction
    LATERAL VIEW EXPLODE(SPLIT(customer_number, '')) AS word
    GROUP BY order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank, customer_number
)
SELECT order_month, C_NAME, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)
AND MOD(number_sum, 2) = 1
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;

-- Query 8
-- Description: Identifies customers with comments containing a specific term ('final'), counts their total comments, and aggregates their top 5 longest comments using ARRAY_AGG.
-- Difficulty: Hard
WITH order_comments AS (
    SELECT
        C.C_CUSTKEY,
        C.C_NAME,
        LOWER(TRIM(REGEXP_REPLACE(O.O_COMMENT, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE
            WHEN LENGTH(O.O_COMMENT) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM {database}.{schema}.orders O
    JOIN {database}.{schema}.customer C ON O.O_CUSTKEY = C.C_CUSTKEY
),
lineitem_comments AS (
    SELECT
        C.C_CUSTKEY,
        C.C_NAME,
        LOWER(TRIM(REGEXP_REPLACE(L.L_COMMENT, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE
            WHEN LENGTH(L.L_COMMENT) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM {database}.{schema}.lineitem L
    JOIN {database}.{schema}.orders O ON L.L_ORDERKEY = O.O_ORDERKEY
    JOIN {database}.{schema}.customer C ON O.O_CUSTKEY = C.C_CUSTKEY
),
combined_comments AS (
    SELECT * FROM order_comments
    UNION ALL
    SELECT * FROM lineitem_comments
),
comment_counts AS (
    SELECT
        C_CUSTKEY,
        COUNT(*) AS total_comments
    FROM combined_comments
    GROUP BY C_CUSTKEY
),
filtered_comments AS (
    SELECT
        CC.C_CUSTKEY,
        CC.C_NAME,
        CC.CLEANED_COMMENT,
        CC.COMMENT_TYPE,
        COALESCE(CCC.TOTAL_COMMENTS, 0) AS total_comments
    FROM combined_comments CC
    LEFT JOIN comment_counts CCC ON CC.C_CUSTKEY = CCC.C_CUSTKEY
    WHERE CC.CLEANED_COMMENT LIKE '%final%'
),
ranked_comments AS (
    SELECT
        C_CUSTKEY,
        C_NAME,
        CLEANED_COMMENT,
        COMMENT_TYPE,
        TOTAL_COMMENTS,
        ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY LENGTH(CLEANED_COMMENT) DESC) AS comment_rank
    FROM filtered_comments
)
SELECT
    RC.C_CUSTKEY,
    RC.C_NAME,
    ARRAY_AGG(CONCAT(RC.CLEANED_COMMENT, ' (', RC.COMMENT_TYPE, ')')) AS customer_comments,
    MAX(RC.TOTAL_COMMENTS) AS total_comments_per_customer
FROM ranked_comments RC
WHERE RC.COMMENT_RANK <= 5
GROUP BY RC.C_CUSTKEY, RC.C_NAME
ORDER BY total_comments_per_customer DESC
LIMIT 1000;

-- Query 9
-- Description: Analyzes customer order history over the last 30 years, calculating monthly order counts and revenue, cumulative revenue, and lifetime revenue using date functions and ARRAY_AGG for monthly summary.
-- Difficulty: Hard
WITH customer_orders AS (
    SELECT
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        COUNT(*) AS order_count,
        ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate ASC) AS order_rank
    FROM {database}.{schema}.customer c
    JOIN {database}.{schema}.orders o ON c.c_custkey = o.o_custkey
    JOIN {database}.{schema}.lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= date_sub(CURRENT_DATE, 30 * 365)
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
        DATE_TRUNC('MONTH', o_orderdate) AS order_month,
        COUNT(*) AS monthly_orders,
        SUM(total_revenue) AS monthly_revenue
    FROM cumulative_revenue
    GROUP BY c_custkey, c_name, DATE_TRUNC('MONTH', o_orderdate)
)
SELECT
    m.c_custkey,
    m.c_name,
    ARRAY_AGG(
        CONCAT('Month: ', DATE_FORMAT(order_month, 'yyyy-MM'),
            ', Orders: ', monthly_orders,
            ', Revenue: ', ROUND(monthly_revenue, 2))
    ) AS monthly_summary,
    MAX(cumulative_revenue) AS lifetime_revenue
FROM monthly_analysis m
JOIN cumulative_revenue c ON m.c_custkey = c.c_custkey
GROUP BY m.c_custkey, m.c_name
ORDER BY lifetime_revenue DESC
LIMIT 1000;

-- Query 10
-- Description: Calculates customer total orders, revenue, average revenue per order, and the percentage of orders shipped within 30 days, filtering for customers with a shipped ratio above 50% using date arithmetic.
-- Difficulty: Hard
WITH customer_orders AS (
    SELECT
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        COUNT(*) AS total_orders,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        SUM(CASE WHEN l.l_shipdate <= DATE_ADD(o.o_orderdate, 30) THEN 1 ELSE 0 END) AS shipped_orders
    FROM {database}.{schema}.customer c
    JOIN {database}.{schema}.orders o ON c.c_custkey = o.o_custkey
    JOIN {database}.{schema}.lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= date_sub(CURRENT_DATE, 30 * 365)
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
ORDER BY avg_revenue_per_order DESC
LIMIT 1000;

-- Query 11
-- Description: Joins orders and line item aggregates to find orders with total revenue over 50000 and average discount less than 0.05.
-- Difficulty: Medium
SELECT
    O.O_ORDERKEY AS orderkey,
    O.O_ORDERDATE AS orderdate,
    O.O_TOTALPRICE AS totalprice,
    lineitems.total_revenue,
    lineitems.avg_discount
FROM (
    SELECT
        L_ORDERKEY AS orderkey,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue,
        AVG(L_DISCOUNT) AS avg_discount
    FROM {database}.{schema}.lineitem
    GROUP BY L_ORDERKEY
) AS lineitems
JOIN {database}.{schema}.orders AS O
    ON lineitems.orderkey = O.O_ORDERKEY
WHERE lineitems.total_revenue > 50000
AND lineitems.avg_discount < 0.05
ORDER BY lineitems.total_revenue DESC
LIMIT 1000;

-- Query F
-- Description: This query analyzes supplier delivery performance, transaction frequency patterns, seasonal ordering patterns, and customer satisfaction metrics. It also calculates risk scores for each supplier based on various factors such as delivery delays, revenue volatility, and customer loyalty.
-- Difficulty: Hard
-- Step 1: Analyze Supplier Delivery Performance
        WITH supplier_delivery_metrics AS (
            SELECT
                s.s_suppkey,
                s.s_name,
                s.s_nationkey,
                s.s_acctbal,
                s.s_address,
                s.s_phone,
                COUNT(DISTINCT l.l_orderkey) AS total_orders,
                AVG(DATEDIFF(l.l_receiptdate, l.l_commitdate)) AS avg_delivery_delay,
                PERCENTILE_APPROX(DATEDIFF(l.l_receiptdate, l.l_commitdate), 0.5) AS median_delivery_delay,
                PERCENTILE_APPROX(DATEDIFF(l.l_receiptdate, l.l_commitdate), 0.90) AS p90_delivery_delay,
                SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END) AS late_deliveries,
                SUM(CASE WHEN l.l_receiptdate <= l.l_commitdate THEN 1 ELSE 0 END) AS on_time_deliveries,
                SUM(CASE WHEN DATEDIFF(l.l_receiptdate, l.l_commitdate) > 7 THEN 1 ELSE 0 END) AS severely_late_deliveries,
                SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
                SUM(l.l_extendedprice * (1 - l.l_discount) * l.l_tax) AS total_tax,
                SUM(l.l_quantity) AS total_quantity,
                AVG(l.l_discount) AS avg_discount_rate,
                MAX(l.l_discount) AS max_discount_offered,
                STDDEV(l.l_extendedprice * (1 - l.l_discount)) AS revenue_volatility,
                VARIANCE(l.l_extendedprice * (1 - l.l_discount)) AS revenue_variance,
                MIN(l.l_shipdate) AS first_shipment_date,
                MAX(l.l_shipdate) AS last_shipment_date,
                DATEDIFF(MAX(l.l_shipdate), MIN(l.l_shipdate)) AS active_days,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_shipped
            FROM {database}.{schema}.supplier s
            JOIN {database}.{schema}.lineitem l ON s.s_suppkey = l.l_suppkey
            GROUP BY s.s_suppkey, s.s_name, s.s_nationkey, s.s_acctbal, s.s_address, s.s_phone
        ),

        -- Step 2: Calculate Supplier Transaction Frequency Patterns
        supplier_transaction_frequency AS (
            SELECT
                l_suppkey,
                DATE_TRUNC('MONTH', l_shipdate) AS ship_month,
                COUNT(*) AS transactions_per_month,
                SUM(l_quantity) AS quantity_per_month,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue_per_month,
                LAG(COUNT(*)) OVER (PARTITION BY l_suppkey ORDER BY DATE_TRUNC('MONTH', l_shipdate)) AS prev_month_transactions,
                LAG(SUM(l_quantity)) OVER (PARTITION BY l_suppkey ORDER BY DATE_TRUNC('MONTH', l_shipdate)) AS prev_month_quantity,
                LAG(SUM(l_extendedprice * (1 - l_discount))) OVER (PARTITION BY l_suppkey ORDER BY DATE_TRUNC('MONTH', l_shipdate)) AS prev_month_revenue,
                DATEDIFF(DATE_TRUNC('MONTH', l_shipdate), LAG(DATE_TRUNC('MONTH', l_shipdate)) OVER (PARTITION BY l_suppkey ORDER BY DATE_TRUNC('MONTH', l_shipdate))) AS months_since_last_activity
            FROM {database}.{schema}.lineitem
            GROUP BY l_suppkey, ship_month
        ),

        -- Step 3: Calculate Seasonal Ordering Patterns (Quarterly)
        seasonal_patterns_quarterly AS (
            SELECT
                l.l_suppkey,
                QUARTER(o.o_orderdate) AS quarter,
                YEAR(o.o_orderdate) AS year,
                COUNT(DISTINCT o.o_orderkey) AS quarterly_orders,
                COUNT(DISTINCT MONTH(o.o_orderdate)) AS active_months_in_quarter,
                SUM(l.l_quantity) AS quarterly_quantity,
                SUM(l.l_extendedprice * (1 - l.l_discount)) AS quarterly_revenue,
                AVG(l.l_extendedprice * (1 - l.l_discount)) AS avg_order_value_in_quarter,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_in_quarter,
                LAG(SUM(l.l_quantity)) OVER (PARTITION BY l.l_suppkey, QUARTER(o.o_orderdate) ORDER BY YEAR(o.o_orderdate)) AS prev_year_quarterly_quantity,
                LAG(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (PARTITION BY l.l_suppkey, QUARTER(o.o_orderdate) ORDER BY YEAR(o.o_orderdate)) AS prev_year_quarterly_revenue
            FROM {database}.{schema}.lineitem l
            JOIN {database}.{schema}.orders o ON l.l_orderkey = o.o_orderkey
            GROUP BY l.l_suppkey, quarter, year
        ),

        -- Step 4: Calculate Seasonal Ordering Patterns (Monthly)
        seasonal_patterns_monthly AS (
            SELECT
                l.l_suppkey,
                MONTH(o.o_orderdate) AS month,
                YEAR(o.o_orderdate) AS year,
                COUNT(DISTINCT o.o_orderkey) AS monthly_orders,
                SUM(l.l_quantity) AS monthly_quantity,
                SUM(l.l_extendedprice * (1 - l.l_discount)) AS monthly_revenue,
                LAG(SUM(l.l_quantity)) OVER (PARTITION BY l.l_suppkey, MONTH(o.o_orderdate) ORDER BY YEAR(o.o_orderdate)) AS prev_year_monthly_quantity,
                LAG(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (PARTITION BY l.l_suppkey, MONTH(o.o_orderdate) ORDER BY YEAR(o.o_orderdate)) AS prev_year_monthly_revenue
            FROM {database}.{schema}.lineitem l
            JOIN {database}.{schema}.orders o ON l.l_orderkey = o.o_orderkey
            GROUP BY l.l_suppkey, month, year
        ),

        -- Step 5: Calculate YoY Growth and Seasonality Scores (Quarterly)
        supplier_seasonality_quarterly AS (
            SELECT
                l_suppkey,
                quarter,
                year,
                quarterly_quantity,
                quarterly_revenue,
                unique_parts_in_quarter,
                active_months_in_quarter,
                CASE
                    WHEN prev_year_quarterly_quantity > 0 THEN (quarterly_quantity - prev_year_quarterly_quantity) / prev_year_quarterly_quantity * 100
                    ELSE NULL
                END AS yoy_quantity_growth,
                CASE
                    WHEN prev_year_quarterly_revenue > 0 THEN (quarterly_revenue - prev_year_quarterly_revenue) / prev_year_quarterly_revenue * 100
                    ELSE NULL
                END AS yoy_revenue_growth,
                DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY quarterly_quantity DESC) AS quantity_quarter_rank,
                DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY quarterly_revenue DESC) AS revenue_quarter_rank,
                quarterly_quantity / NULLIF(quarterly_orders, 0) AS avg_quantity_per_order,
                quarterly_revenue / NULLIF(quarterly_orders, 0) AS avg_revenue_per_order
            FROM seasonal_patterns_quarterly
        ),

        -- Step 6: Calculate YoY Growth and Seasonality Scores (Monthly)
        supplier_seasonality_monthly AS (
            SELECT
                l_suppkey,
                month,
                year,
                monthly_quantity,
                monthly_revenue,
                CASE
                    WHEN prev_year_monthly_quantity > 0 THEN (monthly_quantity - prev_year_monthly_quantity) / prev_year_monthly_quantity * 100
                    ELSE NULL
                END AS yoy_quantity_growth_monthly,
                CASE
                    WHEN prev_year_monthly_revenue > 0 THEN (monthly_revenue - prev_year_monthly_revenue) / prev_year_monthly_revenue * 100
                    ELSE NULL
                END AS yoy_revenue_growth_monthly,
                DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY monthly_quantity DESC) AS quantity_month_rank,
                DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY monthly_revenue DESC) AS revenue_month_rank
            FROM seasonal_patterns_monthly
        ),

        -- Step 7: Analyze Parts Supply Chain Diversification
        part_supplier_diversity AS (
            SELECT
                p.p_partkey,
                p.p_name,
                p.p_mfgr,
                p.p_brand,
                p.p_type,
                p.p_size,
                p.p_container,
                p.p_retailprice,
                COUNT(DISTINCT ps.ps_suppkey) AS supplier_count,
                MAX(ps.ps_supplycost) / NULLIF(MIN(ps.ps_supplycost), 0) AS cost_ratio,
                MAX(ps.ps_supplycost) - MIN(ps.ps_supplycost) AS cost_spread,
                STDDEV(ps.ps_supplycost) AS cost_volatility,
                AVG(ps.ps_supplycost) AS avg_cost,
                MIN(ps.ps_supplycost) AS min_cost,
                MAX(ps.ps_supplycost) AS max_cost,
                AVG(ps.ps_availqty) AS avg_availability,
                SUM(ps.ps_availqty) AS total_availability,
                MIN(ps.ps_availqty) AS min_availability,
                MAX(ps.ps_availqty) AS max_availability,
                VARIANCE(ps.ps_availqty) AS availability_variance
            FROM {database}.{schema}.part p
            JOIN {database}.{schema}.partsupp ps ON p.p_partkey = ps.ps_partkey
            GROUP BY p.p_partkey, p.p_name, p.p_mfgr, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_retailprice
            HAVING COUNT(DISTINCT ps.ps_suppkey) > 1
        ),

        -- Step 8: Analyze Part Categories and Their Supply Chain Characteristics
        part_category_analysis AS (
            SELECT
                SPLIT(p.p_type, ' ')[0] AS part_category,
                COUNT(DISTINCT p.p_partkey) AS category_part_count,
                AVG(psd.supplier_count) AS avg_suppliers_per_part,
                MIN(psd.supplier_count) AS min_suppliers_per_part,
                MAX(psd.supplier_count) AS max_suppliers_per_part,
                AVG(psd.cost_ratio) AS avg_cost_ratio,
                AVG(psd.cost_volatility) AS avg_cost_volatility,
                SUM(p.p_retailprice * psd.total_availability) AS category_inventory_value,
                COUNT(DISTINCT ps.ps_suppkey) AS unique_suppliers_in_category
            FROM {database}.{schema}.part p
            JOIN part_supplier_diversity psd ON p.p_partkey = psd.p_partkey
            JOIN {database}.{schema}.partsupp ps ON p.p_partkey = ps.ps_partkey
            GROUP BY part_category
        ),

        -- Step 9: Geographic Performance Analysis (Nation Level)
        nation_performance AS (
            SELECT
                n.n_nationkey,
                n.n_name AS nation,
                n.n_regionkey,
                COUNT(DISTINCT s.s_suppkey) AS supplier_count,
                AVG(sdm.avg_delivery_delay) AS nation_avg_delay,
                PERCENTILE_APPROX(sdm.avg_delivery_delay, 0.5) AS nation_median_delay,
                AVG(sdm.p90_delivery_delay) AS nation_p90_delay,
                SUM(sdm.total_revenue) AS nation_revenue,
                SUM(sdm.total_quantity) AS nation_quantity,
                SUM(sdm.late_deliveries) / NULLIF(SUM(sdm.total_orders), 0) * 100 AS nation_late_delivery_pct,
                SUM(sdm.severely_late_deliveries) / NULLIF(SUM(sdm.total_orders), 0) * 100 AS nation_severely_late_pct,
                AVG(sdm.revenue_volatility) AS nation_avg_rev_volatility,
                SUM(sdm.unique_parts_shipped) AS nation_unique_parts,
                COUNT(DISTINCT l.l_partkey) AS nation_distinct_parts
            FROM {database}.{schema}.supplier s
            JOIN supplier_delivery_metrics sdm ON s.s_suppkey = sdm.s_suppkey
            JOIN {database}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
            JOIN {database}.{schema}.lineitem l ON s.s_suppkey = l.l_suppkey
            GROUP BY n.n_nationkey, n.n_name, n.n_regionkey
        ),

        -- Step 10: Geographic Performance Analysis (Region Level)
        region_performance AS (
            SELECT
                r.r_regionkey,
                r.r_name AS region,
                COUNT(DISTINCT s.s_suppkey) AS supplier_count,
                COUNT(DISTINCT n.n_nationkey) AS nations_count,
                AVG(np.nation_avg_delay) AS region_avg_delay,
                AVG(np.nation_median_delay) AS region_median_delay,
                SUM(np.nation_revenue) AS region_revenue,
                SUM(np.nation_quantity) AS region_quantity,
                SUM(np.nation_unique_parts) AS region_unique_parts,
                AVG(np.nation_late_delivery_pct) AS region_late_delivery_pct,
                AVG(np.nation_severely_late_pct) AS region_severely_late_pct,
                AVG(np.nation_avg_rev_volatility) AS region_avg_rev_volatility,
                MAX(np.nation_revenue) / NULLIF(MIN(np.nation_revenue), 0) AS nation_revenue_disparity,
                STDDEV(np.nation_late_delivery_pct) AS late_delivery_pct_stddev
            FROM {database}.{schema}.region r
            JOIN nation_performance np ON r.r_regionkey = np.n_regionkey
            JOIN {database}.{schema}.nation n ON np.n_nationkey = n.n_nationkey
            JOIN {database}.{schema}.supplier s ON n.n_nationkey = s.s_nationkey
            GROUP BY r.r_regionkey, r.r_name
        ),

        -- Step 11: Customer Satisfaction Metrics via Order Lifecycle Analysis
        customer_satisfaction AS (
            SELECT
                l.l_suppkey,
                o.o_custkey,
                COUNT(DISTINCT o.o_orderkey) AS order_count,
                AVG(DATEDIFF(l.l_receiptdate, o.o_orderdate)) AS avg_order_to_receipt,
                AVG(o.o_totalprice) AS avg_order_value,
                COUNT(DISTINCT CASE WHEN l.l_returnflag = 'R' THEN l.l_orderkey END) AS returned_orders,
                COUNT(DISTINCT CASE WHEN l.l_returnflag = 'R' THEN l.l_orderkey END) / NULLIF(COUNT(DISTINCT o.o_orderkey), 0) * 100 AS return_rate,
                AVG(CASE WHEN l.l_returnflag = 'R' THEN l.l_extendedprice * (1 - l.l_discount) ELSE NULL END) AS avg_return_value,
                COUNT(DISTINCT CASE WHEN o.o_orderstatus = 'F' THEN o.o_orderkey END) / NULLIF(COUNT(DISTINCT o.o_orderkey), 0) * 100 AS fulfillment_rate,
                COUNT(DISTINCT CASE WHEN o.o_orderpriority LIKE '1-%' OR o.o_orderpriority LIKE '2-%' THEN o.o_orderkey END) AS high_priority_orders,
                COUNT(DISTINCT CASE WHEN o.o_orderpriority LIKE '1-%' OR o.o_orderpriority LIKE '2-%' THEN o.o_orderkey END) / NULLIF(COUNT(DISTINCT o.o_orderkey), 0) * 100 AS high_priority_percentage
            FROM {database}.{schema}.lineitem l
            JOIN {database}.{schema}.orders o ON l.l_orderkey = o.o_orderkey
            GROUP BY l.l_suppkey, o.o_custkey
        ),

        -- Step 12: Supplier Customer Diversity and Loyalty Analysis
        supplier_customer_analysis AS (
            SELECT
                cs.l_suppkey,
                COUNT(DISTINCT cs.o_custkey) AS unique_customers,
                AVG(cs.order_count) AS avg_orders_per_customer,
                MAX(cs.order_count) AS max_orders_from_customer,
                MIN(cs.order_count) AS min_orders_from_customer,
                STDDEV(cs.order_count) AS order_count_stddev,
                COUNT(DISTINCT CASE WHEN cs.order_count > 5 THEN cs.o_custkey ELSE NULL END) AS loyal_customers,
                COUNT(DISTINCT CASE WHEN cs.order_count > 5 THEN cs.o_custkey ELSE NULL END) / NULLIF(COUNT(DISTINCT cs.o_custkey), 0) * 100 AS loyal_customer_percentage,
                AVG(cs.return_rate) AS avg_return_rate,
                MAX(cs.return_rate) AS max_return_rate,
                AVG(cs.avg_order_value) AS avg_customer_order_value,
                MAX(cs.avg_order_value) AS max_customer_order_value,
                MIN(cs.avg_order_value) AS min_customer_order_value,
                COUNT(DISTINCT CASE WHEN cs.high_priority_percentage > 50 THEN cs.o_custkey ELSE NULL END) AS high_priority_customers,
                COUNT(DISTINCT CASE WHEN cs.high_priority_percentage > 50 THEN cs.o_custkey ELSE NULL END) / NULLIF(COUNT(DISTINCT cs.o_custkey), 0) * 100 AS high_priority_customer_percentage
            FROM customer_satisfaction cs
            GROUP BY cs.l_suppkey
        ),

        -- Step 13: Calculate Risk Scores for Each Supplier
        supplier_risk_scores AS (
            SELECT
                sdm.s_suppkey,
                sdm.s_name,
                sdm.s_acctbal,
                sdm.total_orders,
                sdm.total_revenue,
                sdm.total_quantity,
                sdm.late_deliveries / NULLIF(sdm.total_orders, 0) * 100 AS late_delivery_pct,
                sdm.severely_late_deliveries / NULLIF(sdm.total_orders, 0) * 100 AS severely_late_pct,
                sdm.on_time_deliveries / NULLIF(sdm.total_orders, 0) * 100 AS on_time_delivery_pct,
                sdm.avg_delivery_delay,
                sdm.median_delivery_delay,
                sdm.p90_delivery_delay,
                sdm.revenue_volatility,
                sdm.revenue_volatility / NULLIF(sdm.total_revenue / sdm.total_orders, 0) AS normalized_volatility,
                sdm.active_days,
                sdm.unique_parts_shipped,
                sdm.avg_discount_rate,
                sdm.max_discount_offered,
                sca.unique_customers,
                sca.loyal_customer_percentage,
                sca.avg_return_rate,

                -- Risk Components:

                -- Delivery Risk Score (higher is worse): 0-100
                (sdm.late_deliveries / NULLIF(sdm.total_orders, 0) * 40) +
                (sdm.severely_late_deliveries / NULLIF(sdm.total_orders, 0) * 60) +
                (CASE WHEN sdm.avg_delivery_delay > 0 THEN LOG10(sdm.avg_delivery_delay + 1) * 10 ELSE 0 END) +
                (CASE WHEN sdm.p90_delivery_delay > 10 THEN LOG10(sdm.p90_delivery_delay) * 5 ELSE 0 END) AS delivery_risk_score,

                -- Financial Risk Score (higher is worse): 0-100
                (sdm.revenue_volatility / NULLIF(sdm.total_revenue / sdm.total_orders, 0) * 20) +
                (CASE WHEN sdm.s_acctbal < 0 THEN ABS(sdm.s_acctbal) / 1000 * 5 ELSE 0 END) +
                (CASE WHEN sdm.total_orders < 10 THEN 20 ELSE 0 END) +
                (CASE WHEN sdm.active_days < 365 THEN (365 - sdm.active_days) / 3.65 ELSE 0 END) +
                (CASE WHEN stf.months_since_last_activity IS NULL OR stf.months_since_last_activity > 6 THEN 30 ELSE stf.months_since_last_activity * 5 END) AS financial_risk_score,

                -- Diversification Risk Score (higher is worse): 0-100
                (CASE WHEN sdm.unique_parts_shipped < 5 THEN (5 - sdm.unique_parts_shipped) * 10 ELSE 0 END) +
                (CASE WHEN sca.unique_customers < 5 THEN (5 - sca.unique_customers) * 10 ELSE 0 END) +
                (CASE WHEN sca.loyal_customer_percentage < 30 THEN (30 - sca.loyal_customer_percentage) ELSE 0 END) AS diversification_risk_score,

                -- Quality Risk Score (higher is worse): 0-100
                (sca.avg_return_rate * 5) +
                (CASE WHEN sca.max_return_rate > 30 THEN (sca.max_return_rate - 30) * 2 ELSE 0 END) AS quality_risk_score
            FROM supplier_delivery_metrics sdm
            JOIN supplier_customer_analysis sca ON sdm.s_suppkey = sca.l_suppkey
            LEFT JOIN (
                SELECT
                    l_suppkey,
                    MIN(months_since_last_activity) AS months_since_last_activity
                FROM supplier_transaction_frequency
                WHERE months_since_last_activity IS NOT NULL
                GROUP BY l_suppkey
            ) stf ON sdm.s_suppkey = stf.l_suppkey
        ),

        -- Step 14: Aggregate Seasonal Insights per Supplier (Quarterly)
        supplier_seasonality_quarterly_agg AS (
            SELECT
                ssq.l_suppkey,
                MAX(CASE WHEN ssq.quantity_quarter_rank = 1 THEN ssq.quarter ELSE NULL END) AS peak_quantity_quarter,
                MAX(CASE WHEN ssq.revenue_quarter_rank = 1 THEN ssq.quarter ELSE NULL END) AS peak_revenue_quarter,
                AVG(CASE WHEN ssq.yoy_quantity_growth IS NOT NULL THEN ABS(ssq.yoy_quantity_growth) ELSE NULL END) AS avg_quantity_volatility,
                AVG(CASE WHEN ssq.yoy_revenue_growth IS NOT NULL THEN ABS(ssq.yoy_revenue_growth) ELSE NULL END) AS avg_revenue_volatility,
                MAX(ssq.yoy_revenue_growth) AS max_revenue_growth,
                MIN(ssq.yoy_revenue_growth) AS min_revenue_growth,
                AVG(ssq.unique_parts_in_quarter) AS avg_unique_parts_per_quarter,
                MAX(ssq.active_months_in_quarter) AS max_active_months_in_quarter,
                AVG(ssq.avg_quantity_per_order) AS overall_avg_quantity_per_order,
                AVG(ssq.avg_revenue_per_order) AS overall_avg_revenue_per_order,
                STDDEV(ssq.quarterly_revenue) / NULLIF(AVG(ssq.quarterly_revenue), 0) * 100 AS revenue_coefficient_of_variation
            FROM supplier_seasonality_quarterly ssq
            GROUP BY ssq.l_suppkey
        ),

        -- Step 15: Aggregate Seasonal Insights per Supplier (Monthly)
        supplier_seasonality_monthly_agg AS (
            SELECT
                ssm.l_suppkey,
                MAX(CASE WHEN ssm.quantity_month_rank = 1 THEN ssm.month ELSE NULL END) AS peak_quantity_month,
                MAX(CASE WHEN ssm.revenue_month_rank = 1 THEN ssm.month ELSE NULL END) AS peak_revenue_month,
                AVG(CASE WHEN ssm.yoy_quantity_growth_monthly IS NOT NULL THEN ABS(ssm.yoy_quantity_growth_monthly) ELSE NULL END) AS avg_monthly_quantity_volatility,
                AVG(CASE WHEN ssm.yoy_revenue_growth_monthly IS NOT NULL THEN ABS(ssm.yoy_revenue_growth_monthly) ELSE NULL END) AS avg_monthly_revenue_volatility,
                MAX(ssm.yoy_revenue_growth_monthly) AS max_monthly_revenue_growth,
                MIN(ssm.yoy_revenue_growth_monthly) AS min_monthly_revenue_growth,
                STDDEV(ssm.monthly_revenue) / NULLIF(AVG(ssm.monthly_revenue), 0) * 100 AS monthly_revenue_coefficient_of_variation
            FROM supplier_seasonality_monthly ssm
            GROUP BY ssm.l_suppkey
        ),

        -- Step 16: Parts Supplied Analysis
        supplier_parts_profile AS (
                SELECT
                ps.ps_suppkey,
                COUNT(DISTINCT ps.ps_partkey) AS unique_parts_supplied,
                COUNT(DISTINCT p.p_mfgr) AS manufacturer_count,
                COUNT(DISTINCT p.p_brand) AS brand_count,
                COUNT(DISTINCT SPLIT(p.p_type, ' ')[0]) AS part_category_count,
                AVG(ps.ps_supplycost) AS avg_supply_cost,
                MIN(ps.ps_supplycost) AS min_supply_cost,
                MAX(ps.ps_supplycost) AS max_supply_cost,
                STDDEV(ps.ps_supplycost) AS supply_cost_stddev,
                SUM(ps.ps_availqty) AS total_availability,
                MIN(ps.ps_availqty) AS min_availability,
                MAX(ps.ps_availqty) AS max_availability,
                AVG(ps.ps_availqty) AS avg_availability,
                STDDEV(ps.ps_availqty) AS availability_stddev,
                AVG(ps.ps_availqty * p.p_retailprice) AS avg_inventory_value,
                SUM(ps.ps_availqty * p.p_retailprice) AS total_inventory_value,
                COUNT(DISTINCT SPLIT(p.p_type, ' ')[0]) AS category_diversity,
                AVG(CASE WHEN psd.supplier_count IS NOT NULL THEN psd.supplier_count ELSE 1 END) AS avg_supply_chain_redundancy,
                SUM(CASE WHEN p.p_retailprice > 1000 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS premium_part_percentage,
                SUM(ps.ps_availqty) / NULLIF(SUM(COALESCE(l.l_quantity, 0)), 0) AS inventory_turnover_ratio
            FROM {database}.{schema}.partsupp ps
            JOIN {database}.{schema}.part p ON ps.ps_partkey = p.p_partkey
            LEFT JOIN part_supplier_diversity psd ON p.p_partkey = psd.p_partkey
            LEFT JOIN {database}.{schema}.lineitem l ON ps.ps_suppkey = l.l_suppkey
            GROUP BY ps.ps_suppkey
        ),

        -- Step 17: Text Analysis of Supplier Comments
        supplier_comment_analysis AS (
            SELECT
                s_suppkey,
                s_comment,
                LENGTH(s_comment) AS comment_length,
                LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior)]', '')) AS positive_mentions,
                LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed)]', '')) AS negative_mentions,
                CASE
                    WHEN LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior)]', '')) >
                        LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed)]', '')) THEN 'Positive'
                    WHEN LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior)]', '')) <
                        LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed)]', '')) THEN 'Negative'
                    ELSE 'Neutral'
                END AS sentiment,
                LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(price|cost|discount|rate|value|expense)]', '')) AS financial_mentions,
                LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(ship|deliver|transport|carry|move|logistics)]', '')) AS logistics_mentions,
                LENGTH(REGEXP_REPLACE(LOWER(s_comment), '[^(part|item|product|stock|component|material)]', '')) AS product_mentions
            FROM {database}.{schema}.supplier
        ),

        -- Step 18: Final Supplier Performance Report
        final_supplier_report AS (
            SELECT
                sdm.s_suppkey,
                sdm.s_name,
                sdm.s_nationkey,
                sdm.s_acctbal,
                sdm.s_address,
                sdm.s_phone,
                sdm.total_orders,
                sdm.avg_delivery_delay,
                sdm.median_delivery_delay,
                sdm.p90_delivery_delay,
                sdm.late_deliveries,
                sdm.on_time_deliveries,
                sdm.severely_late_deliveries,
                sdm.total_revenue,
                sdm.total_tax,
                sdm.total_quantity,
                sdm.avg_discount_rate,
                sdm.max_discount_offered,
                sdm.revenue_volatility,
                sdm.revenue_variance,
                sdm.first_shipment_date,
                sdm.last_shipment_date,
                sdm.active_days,
                sdm.unique_parts_shipped,
                stf.transactions_per_month,
                stf.quantity_per_month,
                stf.revenue_per_month,
                stf.prev_month_transactions,
                stf.prev_month_quantity,
                stf.prev_month_revenue,
                stf.months_since_last_activity,
                ssq_agg.peak_quantity_quarter,
                ssq_agg.peak_revenue_quarter,
                ssq_agg.avg_quantity_volatility,
                ssq_agg.avg_revenue_volatility,
                ssq_agg.max_revenue_growth,
                ssq_agg.min_revenue_growth,
                ssq_agg.avg_unique_parts_per_quarter,
                ssq_agg.max_active_months_in_quarter,
                ssq_agg.overall_avg_quantity_per_order,
                ssq_agg.overall_avg_revenue_per_order,
                ssq_agg.revenue_coefficient_of_variation,
                ssm_agg.peak_quantity_month,
                ssm_agg.peak_revenue_month,
                ssm_agg.avg_monthly_quantity_volatility,
                ssm_agg.avg_monthly_revenue_volatility,
                ssm_agg.max_monthly_revenue_growth,
                ssm_agg.min_monthly_revenue_growth,
                ssm_agg.monthly_revenue_coefficient_of_variation,
                srs.delivery_risk_score,
                srs.financial_risk_score,
                srs.diversification_risk_score,
                srs.quality_risk_score,
                spp.unique_parts_supplied,
                spp.manufacturer_count,
                spp.brand_count,
                spp.part_category_count,
                spp.avg_supply_cost,
                spp.min_supply_cost,
                spp.max_supply_cost,
                spp.supply_cost_stddev,
                spp.total_availability,
                spp.min_availability,
                spp.max_availability,
                spp.avg_availability,
                spp.availability_stddev,
                spp.avg_inventory_value,
                spp.total_inventory_value,
                spp.category_diversity,
                spp.avg_supply_chain_redundancy,
                spp.premium_part_percentage,
                spp.inventory_turnover_ratio,
                sca.sentiment,
                sca.comment_length,
                sca.positive_mentions,
                sca.negative_mentions,
                sca.financial_mentions,
                sca.logistics_mentions,
                sca.product_mentions
            FROM supplier_delivery_metrics sdm
            LEFT JOIN supplier_transaction_frequency stf ON sdm.s_suppkey = stf.l_suppkey
            LEFT JOIN supplier_seasonality_quarterly_agg ssq_agg ON sdm.s_suppkey = ssq_agg.l_suppkey
            LEFT JOIN supplier_seasonality_monthly_agg ssm_agg ON sdm.s_suppkey = ssm_agg.l_suppkey
            LEFT JOIN supplier_risk_scores srs ON sdm.s_suppkey = srs.s_suppkey
            LEFT JOIN supplier_parts_profile spp ON sdm.s_suppkey = spp.ps_suppkey
            LEFT JOIN supplier_comment_analysis sca ON sdm.s_suppkey = sca.s_suppkey
        )

        SELECT * FROM final_supplier_report LIMIT 1000;