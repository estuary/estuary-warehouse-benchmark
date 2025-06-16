-- Query 1
-- Description: Basic query to sum the L_EXTENDEDPRICE column in the lineitem table
-- Difficulty: Easy
SELECT
    SUM(L_EXTENDEDPRICE) AS sum
FROM `@full_dataset.LINEITEM`;

-- Query 2
-- Description: Calculates the count, sum of extended price, average discount, minimum ship date, and maximum receipt date from the lineitem table.
-- Difficulty: Easy
SELECT
    COUNT(*) AS count_of_line_items,
    SUM(CAST(l_extendedprice AS FLOAT64)) AS sum,
    AVG(CAST(l_discount AS FLOAT64)) AS avg,
    MIN(CAST(l_shipdate AS DATE)) AS min,
    MAX(CAST(l_receiptdate AS DATE)) AS max
FROM `@full_dataset.LINEITEM`;

-- Query 3
-- Description: Selects line item details including the next line's extended price, the previous line's ship date, and the first line's extended price within each order, using window functions.
-- Difficulty: Medium
SELECT
    L_ORDERKEY,
    L_LINENUMBER,
    L_SHIPDATE,
    L_EXTENDEDPRICE,
    LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
    LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
    FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
FROM `@full_dataset.LINEITEM`
ORDER BY L_ORDERKEY, L_LINENUMBER
LIMIT 1000;

-- Query 4
-- Description: Calculates aggregated line item data by shipping year/month, ship mode, and order priority, then applies row numbering based on sum and average within partitions.
-- Difficulty: Medium
WITH base_table AS (
    SELECT
        FORMAT_DATE('%Y-%m', DATE(L_SHIPDATE)) AS ship_year_month,
        L_SHIPMODE,
        O_ORDERPRIORITY AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(L_EXTENDEDPRICE) AS sum,
        AVG(L_DISCOUNT) AS avg
    FROM `@full_dataset.LINEITEM` li
    LEFT JOIN `@full_dataset.ORDERS` ord
        ON li.L_ORDERKEY = ord.O_ORDERKEY
    GROUP BY ship_year_month, L_SHIPMODE, order_priority
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table
LIMIT 1000;

-- Query 5
-- Description: Ranks customers by total spent and total quantity ordered within each month and filters for the top 3 customers in either ranking.
-- Difficulty: Medium
WITH customer_sales AS (
    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM `@full_dataset.ORDERS` o
    JOIN `@full_dataset.LINEITEM` l ON o.o_orderkey = l.l_orderkey
    JOIN `@full_dataset.CUSTOMER` c ON o.o_custkey = c.c_custkey
    GROUP BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)), c.c_name, o.o_orderdate
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;

-- Query 6
-- Description: Finds the top 5 most frequent words in combined order and line item comments for each month, excluding common stop words.
-- Difficulty: Hard
WITH combined_comments AS (
    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(o.o_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM `@full_dataset.ORDERS` o

    UNION ALL

    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(l.l_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM `@full_dataset.LINEITEM` l
    JOIN `@full_dataset.ORDERS` o
    ON l.l_orderkey = o.o_orderkey
),
tokenized_words AS (
    SELECT
        order_month,
        word
    FROM combined_comments,
    UNNEST(SPLIT(cleaned_comment, ' ')) as word
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
-- Description: Identifies customers who rank in the top 3 by total spent or total quantity each month and whose customer number digits sum to an odd number.
-- Difficulty: Hard
WITH customer_sales AS (
    SELECT
        FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM `@full_dataset.ORDERS` o
    JOIN `@full_dataset.LINEITEM` l
        ON o.o_orderkey = l.l_orderkey
    JOIN `@full_dataset.CUSTOMER` c
        ON o.o_custkey = c.c_custkey
    GROUP BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)), c.c_name, o.o_orderdate
),
number_extraction AS (
    SELECT *,
        REGEXP_REPLACE(c_name, r'[^0-9]', '') AS customer_number
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT
        order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        (SELECT SUM(CAST(digit AS INT64))
        FROM UNNEST(SPLIT(customer_number, '')) digit
        ) AS number_sum
    FROM number_extraction
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)
AND MOD(number_sum, 2) = 1
ORDER BY order_month, price_rank, quantity_rank
LIMIT 1000;

-- Query 8
-- Description: Finds customers with comments containing 'final' in orders or line items, counts their total comments, and concatenates their top 5 longest comments.
-- Difficulty: Hard
WITH order_comments AS (
    SELECT
        c.c_custkey,
        c.c_name,
        LOWER(TRIM(REGEXP_REPLACE(o.o_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE
            WHEN LENGTH(o.o_comment) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM `@full_dataset.ORDERS` o
    JOIN `@full_dataset.CUSTOMER` c ON o.o_custkey = c.c_custkey
),
lineitem_comments AS (
    SELECT
        c.c_custkey,
        c.c_name,
        LOWER(TRIM(REGEXP_REPLACE(l.l_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE
            WHEN LENGTH(l.l_comment) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM `@full_dataset.LINEITEM` l
    JOIN `@full_dataset.ORDERS` o ON l.l_orderkey = o.o_orderkey
    JOIN `@full_dataset.CUSTOMER` c ON o.o_custkey = c.c_custkey
),
combined_comments AS (
    SELECT * FROM order_comments
    UNION ALL
    SELECT * FROM lineitem_comments
),
comment_counts AS (
    SELECT
        c_custkey,
        COUNT(*) AS total_comments
    FROM combined_comments
    GROUP BY c_custkey
),
filtered_comments AS (
    SELECT
        cc.c_custkey,
        cc.c_name,
        cc.cleaned_comment,
        cc.comment_type,
        COALESCE(ccc.total_comments, 0) AS total_comments
    FROM combined_comments cc
    LEFT JOIN comment_counts ccc ON cc.c_custkey = ccc.c_custkey
    WHERE cc.cleaned_comment LIKE '%final%'
),
ranked_comments AS (
    SELECT
        c_custkey,
        c_name,
        cleaned_comment,
        comment_type,
        total_comments,
        ROW_NUMBER() OVER (PARTITION BY c_custkey ORDER BY LENGTH(cleaned_comment) DESC) AS comment_rank
    FROM filtered_comments
)
SELECT
    rc.c_custkey,
    rc.c_name,
    STRING_AGG(CONCAT(rc.cleaned_comment, ' (', rc.comment_type, ')'), ' | ' ORDER BY rc.comment_rank) AS customer_comments,
    MAX(rc.total_comments) AS total_comments_per_customer
FROM ranked_comments rc
WHERE rc.comment_rank <= 5
GROUP BY rc.c_custkey, rc.c_name
ORDER BY total_comments_per_customer DESC
LIMIT 1000;

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
    LIMIT 1000;

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
LIMIT 1000;

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
LIMIT 1000;


-- Query F
-- Description: Comprehensive analysis of supplier performance including delivery metrics, transaction frequency, seasonal ordering patterns, part supply chain diversification, geographic performance, customer satisfaction insights, supplier customer diversity, calculated risk scores across multiple dimensions (delivery, financial, diversification, quality), detailed parts supplied profiles, and sentiment analysis of supplier comments. This query uses multiple CTEs to build a detailed supplier performance report by integrating various aspects of their interactions within the database.
-- Difficulty: Hard
WITH supplier_delivery_metrics AS (
        SELECT
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        s.s_acctbal,
        s.s_address,
        s.s_phone,
        COUNT(DISTINCT l.l_orderkey) AS total_orders,
        AVG(DATE_DIFF(l.l_receiptdate, l.l_commitdate, DAY)) AS avg_delivery_delay,
        -- Fix for median calculation in BigQuery
        PERCENTILE_CONT(DATE_DIFF(l.l_receiptdate, l.l_commitdate, DAY), 0.5)
            OVER(PARTITION BY s.s_suppkey) AS median_delivery_delay,
        -- Fix for p90 calculation
        PERCENTILE_CONT(DATE_DIFF(l.l_receiptdate, l.l_commitdate, DAY), 0.90)
            OVER(PARTITION BY s.s_suppkey) AS p90_delivery_delay,
        COUNTIF(l.l_receiptdate > l.l_commitdate) AS late_deliveries,
        COUNTIF(l.l_receiptdate <= l.l_commitdate) AS on_time_deliveries,
        COUNTIF(DATE_DIFF(l.l_receiptdate, l.l_commitdate, DAY) > 7) AS severely_late_deliveries,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        SUM(l.l_extendedprice * (1 - l.l_discount) * l.l_tax) AS total_tax,
        SUM(l.l_quantity) AS total_quantity,
        AVG(l.l_discount) AS avg_discount_rate,
        MAX(l.l_discount) AS max_discount_offered,
        STDDEV(l.l_extendedprice * (1 - l.l_discount)) AS revenue_volatility,
        VAR_POP(l.l_extendedprice * (1 - l.l_discount)) AS revenue_variance,
        MIN(l.l_shipdate) AS first_shipment_date,
        MAX(l.l_shipdate) AS last_shipment_date,
        DATE_DIFF(MAX(l.l_shipdate), MIN(l.l_shipdate), DAY) AS active_days,
        COUNT(DISTINCT l.l_partkey) AS unique_parts_shipped
    FROM `@full_dataset.SUPPLIER` s
    JOIN `@full_dataset.LINEITEM` l ON s.s_suppkey = l.l_suppkey
    WHERE l.l_receiptdate IS NOT NULL
        AND l.l_commitdate IS NOT NULL
        AND l.l_shipdate IS NOT NULL
    GROUP BY
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        s.s_acctbal,
        s.s_address,
        s.s_phone,
        l.l_receiptdate,
        l.l_commitdate
    HAVING COUNT(DISTINCT l.l_orderkey) > 0
),

-- Step 2: Calculate Supplier Transaction Frequency Patterns
supplier_transaction_frequency AS (
    SELECT
  l_suppkey,
  DATE_TRUNC(l_shipdate, MONTH) AS ship_month,
  COUNT(*) AS transactions_per_month,
  SUM(l_quantity) AS quantity_per_month,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue_per_month,
  LAG(COUNT(*)) OVER ( PARTITION BY l_suppkey ORDER BY MIN(l_shipdate)) AS prev_month_transactions,
  LAG(SUM(l_quantity)) OVER (PARTITION BY l_suppkey ORDER BY MIN(l_shipdate)) AS prev_month_quantity,
  LAG(SUM(l_extendedprice * (1 - l_discount))) OVER (PARTITION BY l_suppkey ORDER BY MIN(l_shipdate)) AS prev_month_revenue,
  DATE_DIFF(
    DATE_TRUNC(MIN(l_shipdate), MONTH),
    LAG(DATE_TRUNC(MIN(l_shipdate), MONTH)) OVER (
      PARTITION BY l_suppkey
      ORDER BY MIN(l_shipdate)
    ),
    MONTH
  ) AS months_since_last_activity
FROM
  `@full_dataset.LINEITEM`
GROUP BY
  l_suppkey,
  DATE_TRUNC(l_shipdate, MONTH)
),

-- Step 3: Calculate Seasonal Ordering Patterns (Quarterly)
seasonal_patterns_quarterly AS (
    SELECT
        l.l_suppkey,
        quarter,
        year,
        COUNT(DISTINCT o.o_orderkey) AS quarterly_orders,
        COUNT(DISTINCT EXTRACT(MONTH FROM o.o_orderdate)) AS active_months_in_quarter,
        SUM(l.l_quantity) AS quarterly_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS quarterly_revenue,
        AVG(l.l_extendedprice * (1 - l.l_discount)) AS avg_order_value_in_quarter,
        COUNT(DISTINCT l.l_partkey) AS unique_parts_in_quarter,
        LAG(SUM(l.l_quantity)) OVER (
            PARTITION BY l.l_suppkey, quarter
            ORDER BY year
        ) AS prev_year_quarterly_quantity,
        LAG(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (
            PARTITION BY l.l_suppkey, quarter
            ORDER BY year
        ) AS prev_year_quarterly_revenue
    FROM `@full_dataset.LINEITEM` l
    JOIN `@full_dataset.ORDERS` o ON l.l_orderkey = o.o_orderkey,
    UNNEST([STRUCT(
        EXTRACT(QUARTER FROM o.o_orderdate) AS quarter,
        EXTRACT(YEAR FROM o.o_orderdate) AS year
    )])
    GROUP BY l.l_suppkey, quarter, year
),

-- Step 4: Calculate Seasonal Ordering Patterns (Monthly)
seasonal_patterns_monthly AS (
    SELECT
        l.l_suppkey,
        month,
        year,
        COUNT(DISTINCT o.o_orderkey) AS monthly_orders,
        SUM(l.l_quantity) AS monthly_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS monthly_revenue,
        LAG(SUM(l.l_quantity)) OVER (
            PARTITION BY l.l_suppkey, month
            ORDER BY year
        ) AS prev_year_monthly_quantity,
        LAG(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (
            PARTITION BY l.l_suppkey, month
            ORDER BY year
        ) AS prev_year_monthly_revenue
    FROM `@full_dataset.LINEITEM` l
    JOIN `@full_dataset.ORDERS` o ON l.l_orderkey = o.o_orderkey,
    UNNEST([STRUCT(
        EXTRACT(MONTH FROM o.o_orderdate) AS month,
        EXTRACT(YEAR FROM o.o_orderdate) AS year
    )])
    GROUP BY l.l_suppkey, month, year
),

-- Step 5: Calculate YoY Growth and Seasonality Scores (Quarterly)
supplier_seasonality_quarterly AS (
    SELECT
        ssq.l_suppkey,
        quarter,
        year,
        SUM(quarterly_quantity) as quarterly_quantity,
        SUM(quarterly_revenue) as quarterly_revenue,
        COUNT(DISTINCT unique_parts_in_quarter) as unique_parts_in_quarter,
        MAX(active_months_in_quarter) as active_months_in_quarter,
        CASE
            WHEN LAG(SUM(quarterly_quantity)) OVER (
                PARTITION BY l_suppkey, quarter ORDER BY year
            ) > 0 THEN
                (SUM(quarterly_quantity) - LAG(SUM(quarterly_quantity)) OVER (
                    PARTITION BY l_suppkey, quarter ORDER BY year
                )) / LAG(SUM(quarter_quantity)) OVER (
                    PARTITION BY l_suppkey, quarter ORDER BY year
                ) * 100
            ELSE NULL
        END AS yoy_quantity_growth,
        CASE
            WHEN LAG(SUM(quarterly_revenue)) OVER (
                PARTITION BY l_suppkey, quarter ORDER BY year
            ) > 0 THEN
                (SUM(quarterly_revenue) - LAG(SUM(quarterly_revenue)) OVER (
                    PARTITION BY l_suppkey, quarter ORDER BY year
                )) / LAG(SUM(quarterly_revenue)) OVER (
                    PARTITION BY l_suppkey, quarter ORDER BY year
                ) * 100
            ELSE NULL
        END AS yoy_revenue_growth,
        DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY SUM(quarterly_quantity) DESC) AS quantity_quarter_rank,
        DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY SUM(quarterly_revenue) DESC) AS revenue_quarter_rank,
        SAFE_DIVIDE(SUM(quarterly_quantity), COUNT(DISTINCT quarterly_orders)) AS avg_quantity_per_order,
        SAFE_DIVIDE(SUM(quarterly_revenue), COUNT(DISTINCT quarterly_orders)) AS avg_revenue_per_order
    FROM seasonal_patterns_quarterly ssq
    GROUP BY l_suppkey, quarter, year
),

-- Step 6: Calculate YoY Growth and Seasonality Scores (Monthly)
supplier_seasonality_monthly AS (
    SELECT
        ssm.l_suppkey,
        month,
        year,
        SUM(monthly_quantity) as monthly_quantity,
        SUM(monthly_revenue) as monthly_revenue,
        CASE
            WHEN LAG(SUM(monthly_quantity)) OVER (
                PARTITION BY l_suppkey, month ORDER BY year
            ) > 0 THEN
                (SUM(monthly_quantity) - LAG(SUM(monthly_quantity)) OVER (
                    PARTITION BY l_suppkey, month ORDER BY year
                )) / LAG(SUM(monthly_quantity)) OVER (
                    PARTITION BY l_suppkey, month ORDER BY year
                ) * 100
            ELSE NULL
        END AS yoy_quantity_growth_monthly,
        CASE
            WHEN LAG(SUM(monthly_revenue)) OVER (
                PARTITION BY l_suppkey, month ORDER BY year
            ) > 0 THEN
                (SUM(monthly_revenue) - LAG(SUM(monthly_revenue)) OVER (
                    PARTITION BY l_suppkey, month ORDER BY year
                )) / LAG(SUM(monthly_revenue)) OVER (
                    PARTITION BY l_suppkey, month ORDER BY year
                ) * 100
            ELSE NULL
        END AS yoy_revenue_growth_monthly,
        DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY SUM(monthly_quantity) DESC) AS quantity_month_rank,
        DENSE_RANK() OVER (PARTITION BY l_suppkey ORDER BY SUM(monthly_revenue) DESC) AS revenue_month_rank
    FROM seasonal_patterns_monthly ssm
    GROUP BY l_suppkey, month, year
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
            -- Basic supplier metrics
            COUNT(DISTINCT ps.ps_suppkey) AS supplier_count,

            -- Cost analysis metrics with NULL handling
            SAFE_DIVIDE(
                NULLIF(MAX(ps.ps_supplycost), 0),
                NULLIF(MIN(ps.ps_supplycost), 0)
            ) AS cost_ratio,
            NULLIF(MAX(ps.ps_supplycost) - MIN(ps.ps_supplycost), 0) AS cost_spread,
            COALESCE(STDDEV(ps.ps_supplycost), 0) AS cost_volatility,
            COALESCE(AVG(ps.ps_supplycost), 0) AS avg_cost,
            NULLIF(MIN(ps.ps_supplycost), 0) AS min_cost,
            MAX(ps.ps_supplycost) AS max_cost,

            -- Availability metrics with NULL handling
            COALESCE(AVG(ps.ps_availqty), 0) AS avg_availability,
            COALESCE(SUM(ps.ps_availqty), 0) AS total_availability,
            NULLIF(MIN(ps.ps_availqty), 0) AS min_availability,
            MAX(ps.ps_availqty) AS max_availability,
            COALESCE(VAR_POP(ps.ps_availqty), 0) AS availability_variance,

            -- Additional metrics for supply chain analysis
            COALESCE(STDDEV(ps.ps_availqty), 0) AS availability_stddev,
            SAFE_DIVIDE(
                STDDEV(ps.ps_supplycost),
                AVG(ps.ps_supplycost)
            ) * 100 AS cost_coefficient_variation,
            SAFE_DIVIDE(
                MAX(ps.ps_availqty) - MIN(ps.ps_availqty),
                NULLIF(AVG(ps.ps_availqty), 0)
            ) * 100 AS availability_range_pct
        FROM `@full_dataset.PART` p
        JOIN `@full_dataset.PARTSUPP` ps ON p.p_partkey = ps.ps_partkey
        WHERE ps.ps_supplycost > 0  -- Filter out invalid supply costs
        GROUP BY
            p.p_partkey,
            p.p_name,
            p.p_mfgr,
            p.p_brand,
            p.p_type,
            p.p_size,
            p.p_container,
            p.p_retailprice
        HAVING COUNT(DISTINCT ps.ps_suppkey) > 1  -- Only parts with multiple suppliers
),

-- Step 8: Analyze Part Categories and Their Supply Chain Characteristics
part_category_analysis AS (
    SELECT
        SPLIT(p.p_type, ' ')[SAFE_OFFSET(0)] AS part_category,
        COUNT(DISTINCT p.p_partkey) AS category_part_count,
        AVG(psd.supplier_count) AS avg_suppliers_per_part,
        MIN(psd.supplier_count) AS min_suppliers_per_part,
        MAX(psd.supplier_count) AS max_suppliers_per_part,
        AVG(psd.cost_ratio) AS avg_cost_ratio,
        AVG(psd.cost_volatility) AS avg_cost_volatility,
        SUM(p.p_retailprice * psd.total_availability) AS category_inventory_value,
        COUNT(DISTINCT ps.ps_suppkey) AS unique_suppliers_in_category
    FROM `@full_dataset.PART` p
    JOIN part_supplier_diversity psd ON p.p_partkey = psd.p_partkey
    JOIN `@full_dataset.PARTSUPP` ps ON p.p_partkey = ps.ps_partkey
    GROUP BY part_category
),

-- Step 9: Geographic Performance Analysis (Nation Level)
nation_performance AS (
    SELECT
        n.n_nationkey,
        n.n_name AS nation,
        n.n_regionkey,
        -- Basic supplier metrics with NULL handling
        COUNT(DISTINCT s.s_suppkey) AS supplier_count,
        COUNT(DISTINCT CASE WHEN sdm.active_days > 365 THEN s.s_suppkey END) AS active_suppliers,

        -- Delivery performance metrics
        COALESCE(AVG(sdm.avg_delivery_delay), 0) AS nation_avg_delay,
        PERCENTILE_CONT(NULLIF(sdm.avg_delivery_delay, 0), 0.5) OVER() AS nation_median_delay,
        COALESCE(AVG(sdm.p90_delivery_delay), 0) AS nation_p90_delay,

        -- Revenue and quantity metrics
        SUM(COALESCE(sdm.total_revenue, 0)) AS nation_revenue,
        SUM(COALESCE(sdm.total_quantity, 0)) AS nation_quantity,

        -- Delivery quality metrics with safe division
        SAFE_DIVIDE(
            SUM(COALESCE(sdm.late_deliveries, 0)),
            NULLIF(SUM(COALESCE(sdm.total_orders, 0)), 0)
        ) * 100 AS nation_late_delivery_pct,
        SAFE_DIVIDE(
            SUM(COALESCE(sdm.severely_late_deliveries, 0)),
            NULLIF(SUM(COALESCE(sdm.total_orders, 0)), 0)
        ) * 100 AS nation_severely_late_pct,

        -- Volatility and diversity metrics
        COALESCE(AVG(sdm.revenue_volatility), 0) AS nation_avg_rev_volatility,
        COALESCE(STDDEV(sdm.total_revenue), 0) AS nation_revenue_stddev,
        SUM(COALESCE(sdm.unique_parts_shipped, 0)) AS nation_unique_parts,
        COUNT(DISTINCT l.l_partkey) AS nation_distinct_parts,

        -- Additional performance indicators
        SAFE_DIVIDE(
            SUM(COALESCE(sdm.total_revenue, 0)),
            COUNT(DISTINCT s.s_suppkey)
        ) AS avg_revenue_per_supplier,
        SAFE_DIVIDE(
            COUNT(DISTINCT l.l_partkey),
            COUNT(DISTINCT s.s_suppkey)
        ) AS avg_parts_per_supplier,

        -- Market concentration metrics
        MAX(sdm.total_revenue) AS highest_supplier_revenue,
        MIN(sdm.total_revenue) AS lowest_supplier_revenue,
        SAFE_DIVIDE(
            MAX(sdm.total_revenue) - MIN(sdm.total_revenue),
            NULLIF(AVG(sdm.total_revenue), 0)
        ) * 100 AS revenue_spread_pct

    FROM `@full_dataset.SUPPLIER` s
    JOIN supplier_delivery_metrics sdm
        ON s.s_suppkey = sdm.s_suppkey
    JOIN `@full_dataset.NATION` n
        ON s.s_nationkey = n.n_nationkey
    JOIN `@full_dataset.LINEITEM` l
        ON s.s_suppkey = l.l_suppkey
    WHERE s.s_suppkey IS NOT NULL
        AND sdm.total_orders > 0  -- Filter out inactive suppliers
    GROUP BY
        n.n_nationkey,
        n.n_name,
        n.n_regionkey,
        sdm.avg_delivery_delay
    HAVING
        COUNT(DISTINCT s.s_suppkey) > 0  -- Ensure we have active suppliers
    ORDER BY
        nation_revenue DESC
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
        SAFE_DIVIDE(MAX(np.nation_revenue), MIN(np.nation_revenue)) AS nation_revenue_disparity,
        STDDEV(np.nation_late_delivery_pct) AS late_delivery_pct_stddev
    FROM `@full_dataset.REGION` r
    JOIN nation_performance np ON r.r_regionkey = np.n_regionkey
    JOIN `@full_dataset.NATION` n ON np.n_nationkey = n.n_nationkey
    JOIN `@full_dataset.SUPPLIER` s ON n.n_nationkey = s.s_nationkey
    GROUP BY r.r_regionkey, r.r_name
),

-- Step 11: Customer Satisfaction Metrics via Order Lifecycle Analysis
customer_satisfaction AS (
    SELECT
        l.l_suppkey,
        o.o_custkey,
        COUNT(DISTINCT o.o_orderkey) AS order_count,
        AVG(DATE_DIFF(l.l_receiptdate, o.o_orderdate, DAY)) AS avg_order_to_receipt,
        AVG(o.o_totalprice) AS avg_order_value,
        COUNTIF(l.l_returnflag = 'R') AS returned_orders,
        SAFE_DIVIDE(COUNTIF(l.l_returnflag = 'R'), COUNT(DISTINCT o.o_orderkey)) * 100 AS return_rate,
        AVG(CASE WHEN l.l_returnflag = 'R' THEN l.l_extendedprice * (1 - l.l_discount) END) AS avg_return_value,
        SAFE_DIVIDE(COUNTIF(o.o_orderstatus = 'F'), COUNT(DISTINCT o.o_orderkey)) * 100 AS fulfillment_rate,
        COUNTIF(o.o_orderpriority LIKE '1-%' OR o.o_orderpriority LIKE '2-%') AS high_priority_orders,
        SAFE_DIVIDE(COUNTIF(o.o_orderpriority LIKE '1-%' OR o.o_orderpriority LIKE '2-%'), COUNT(DISTINCT o.o_orderkey)) * 100 AS high_priority_percentage
    FROM `@full_dataset.LINEITEM` l
    JOIN `@full_dataset.ORDERS` o ON l.l_orderkey = o.o_orderkey
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
        COUNTIF(cs.order_count > 5) AS loyal_customers,
        SAFE_DIVIDE(COUNTIF(cs.order_count > 5), COUNT(DISTINCT cs.o_custkey)) * 100 AS loyal_customer_percentage,
        AVG(cs.return_rate) AS avg_return_rate,
        MAX(cs.return_rate) AS max_return_rate,
        AVG(cs.avg_order_value) AS avg_customer_order_value,
        MAX(cs.avg_order_value) AS max_customer_order_value,
        MIN(cs.avg_order_value) AS min_customer_order_value,
        COUNTIF(cs.high_priority_percentage > 50) AS high_priority_customers,
        SAFE_DIVIDE(COUNTIF(cs.high_priority_percentage > 50), COUNT(DISTINCT cs.o_custkey)) * 100 AS high_priority_customer_percentage
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
        SAFE_DIVIDE(sdm.late_deliveries, sdm.total_orders) * 100 AS late_delivery_pct,
        SAFE_DIVIDE(sdm.severely_late_deliveries, sdm.total_orders) * 100 AS severely_late_pct,
        SAFE_DIVIDE(sdm.on_time_deliveries, sdm.total_orders) * 100 AS on_time_delivery_pct,
        sdm.avg_delivery_delay,
        sdm.median_delivery_delay,
        sdm.p90_delivery_delay,
        sdm.revenue_volatility,
        SAFE_DIVIDE(sdm.revenue_volatility, SAFE_DIVIDE(sdm.total_revenue, sdm.total_orders)) AS normalized_volatility,
        sdm.active_days,
        sdm.unique_parts_shipped,
        sdm.avg_discount_rate,
        sdm.max_discount_offered,
        sca.unique_customers,
        sca.loyal_customer_percentage,
        sca.avg_return_rate,

        -- Risk Components (adapted for BigQuery)
        (SAFE_DIVIDE(sdm.late_deliveries, sdm.total_orders) * 40) +
        (SAFE_DIVIDE(sdm.severely_late_deliveries, sdm.total_orders) * 60) +
        (IF(sdm.avg_delivery_delay > 0, LOG(sdm.avg_delivery_delay + 1, 10) * 10, 0)) +
        (IF(sdm.p90_delivery_delay > 10, LOG(sdm.p90_delivery_delay, 10) * 5, 0)) AS delivery_risk_score,

        -- Financial Risk Score
        (SAFE_DIVIDE(sdm.revenue_volatility, SAFE_DIVIDE(sdm.total_revenue, sdm.total_orders)) * 20) +
        (IF(sdm.s_acctbal < 0, ABS(sdm.s_acctbal) / 1000 * 5, 0)) +
        (IF(sdm.total_orders < 10, 20, 0)) +
        (IF(sdm.active_days < 365, (365 - sdm.active_days) / 3.65, 0)) +
        (IF(stf.months_since_last_activity IS NULL OR stf.months_since_last_activity > 6,
            30, stf.months_since_last_activity * 5)) AS financial_risk_score,

        -- Diversification Risk Score
        (IF(sdm.unique_parts_shipped < 5, (5 - sdm.unique_parts_shipped) * 10, 0)) +
        (IF(sca.unique_customers < 5, (5 - sca.unique_customers) * 10, 0)) +
        (IF(sca.loyal_customer_percentage < 30, 30 - sca.loyal_customer_percentage, 0)) AS diversification_risk_score,

        -- Quality Risk Score
        (sca.avg_return_rate * 5) +
        (IF(sca.max_return_rate > 30, (sca.max_return_rate - 30) * 2, 0)) AS quality_risk_score
    FROM supplier_delivery_metrics sdm
    JOIN supplier_customer_analysis sca ON sdm.s_suppkey = sca.l_suppkey
    LEFT JOIN supplier_transaction_frequency stf ON sdm.s_suppkey = stf.l_suppkey
),

-- Step 14: Aggregate Seasonal Insights per Supplier (Quarterly)
supplier_seasonality_quarterly_agg AS (
    SELECT
        ssq.l_suppkey,
        -- Use NULLIF to handle edge cases where quarter might be 0 or invalid
        MAX(NULLIF(IF(ssq.quantity_quarter_rank = 1, ssq.quarter, NULL), 0)) AS peak_quantity_quarter,
        MAX(NULLIF(IF(ssq.revenue_quarter_rank = 1, ssq.quarter, NULL), 0)) AS peak_revenue_quarter,
        -- Use SAFE functions for division operations in volatility calculations
        AVG(CASE
            WHEN ssq.yoy_quantity_growth IS NOT NULL THEN ABS(ssq.yoy_quantity_growth)
            ELSE NULL
        END) AS avg_quantity_volatility,
        AVG(CASE
            WHEN ssq.yoy_revenue_growth IS NOT NULL THEN ABS(ssq.yoy_revenue_growth)
            ELSE NULL
        END) AS avg_revenue_volatility,
        -- Add NULLIF to prevent issues with empty groups
        MAX(NULLIF(ssq.yoy_revenue_growth, 0)) AS max_revenue_growth,
        MIN(NULLIF(ssq.yoy_revenue_growth, 0)) AS min_revenue_growth,
        -- Handle NULL values in averages
        COALESCE(AVG(ssq.unique_parts_in_quarter), 0) AS avg_unique_parts_per_quarter,
        COALESCE(MAX(ssq.active_months_in_quarter), 0) AS max_active_months_in_quarter,
        -- Use SAFE_DIVIDE for averages involving potential zero denominators
        SAFE_DIVIDE(
            SUM(ssq.avg_quantity_per_order),
            COUNT(ssq.avg_quantity_per_order)
        ) AS overall_avg_quantity_per_order,
        SAFE_DIVIDE(
            SUM(ssq.avg_revenue_per_order),
            COUNT(ssq.avg_revenue_per_order)
        ) AS overall_avg_revenue_per_order,
        -- Add NULL handling for coefficient of variation
        CASE
            WHEN AVG(ssq.quarterly_revenue) > 0 THEN
                SAFE_DIVIDE(
                    STDDEV(ssq.quarterly_revenue),
                    AVG(ssq.quarterly_revenue)
                ) * 100
            ELSE NULL
        END AS revenue_coefficient_of_variation
    FROM supplier_seasonality_quarterly ssq
    WHERE ssq.l_suppkey IS NOT NULL  -- Ensure we don't group by NULL values
    GROUP BY ssq.l_suppkey
),

-- Step 15: Aggregate Seasonal Insights per Supplier (Monthly)
supplier_seasonality_monthly_agg AS (
    SELECT
        ssm.l_suppkey,
        -- Use NULLIF to handle edge cases where month might be 0 or invalid
        MAX(NULLIF(IF(ssm.quantity_month_rank = 1, ssm.month, NULL), 0)) AS peak_quantity_month,
        MAX(NULLIF(IF(ssm.revenue_month_rank = 1, ssm.month, NULL), 0)) AS peak_revenue_month,
        -- Use CASE for clearer NULL handling in volatility calculations
        AVG(CASE
            WHEN ssm.yoy_quantity_growth_monthly IS NOT NULL THEN ABS(ssm.yoy_quantity_growth_monthly)
            ELSE NULL
        END) AS avg_monthly_quantity_volatility,
        AVG(CASE
            WHEN ssm.yoy_revenue_growth_monthly IS NOT NULL THEN ABS(ssm.yoy_revenue_growth_monthly)
            ELSE NULL
        END) AS avg_monthly_revenue_volatility,
        -- Add NULLIF to prevent issues with empty groups
        MAX(NULLIF(ssm.yoy_revenue_growth_monthly, 0)) AS max_monthly_revenue_growth,
        MIN(NULLIF(ssm.yoy_revenue_growth_monthly, 0)) AS min_monthly_revenue_growth,
        -- Add NULL handling for coefficient of variation
        CASE
            WHEN AVG(ssm.monthly_revenue) > 0 THEN
                SAFE_DIVIDE(
                    STDDEV(ssm.monthly_revenue),
                    AVG(ssm.monthly_revenue)
                ) * 100
            ELSE NULL
        END AS monthly_revenue_coefficient_of_variation
    FROM supplier_seasonality_monthly ssm
    WHERE ssm.l_suppkey IS NOT NULL  -- Ensure we don't group by NULL values
    GROUP BY ssm.l_suppkey
),

-- Step 16: Parts Supplied Analysis
supplier_parts_profile AS (
    SELECT
        ps.ps_suppkey,
        COUNT(DISTINCT ps.ps_partkey) AS unique_parts_supplied,
        COUNT(DISTINCT p.p_mfgr) AS manufacturer_count,
        COUNT(DISTINCT p.p_brand) AS brand_count,
        COUNT(DISTINCT SPLIT(p.p_type, ' ')[SAFE_OFFSET(0)]) AS part_category_count,
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
        COUNT(DISTINCT SUBSTR(p.p_type, 1, STRPOS(p.p_type, ' '))) AS category_diversity,
        AVG(IF(psd.supplier_count IS NOT NULL, psd.supplier_count, 1)) AS avg_supply_chain_redundancy,
        SAFE_DIVIDE(COUNTIF(p.p_retailprice > 1000), COUNT(*)) * 100 AS premium_part_percentage
    FROM `@full_dataset.PARTSUPP` ps
    JOIN `@full_dataset.PART` p ON ps.ps_partkey = p.p_partkey
    LEFT JOIN part_supplier_diversity psd ON p.p_partkey = psd.p_partkey
    GROUP BY ps.ps_suppkey
),

-- Step 17: Text Analysis of Supplier Comments
supplier_comment_analysis AS (
-- Supplier Comment Analysis with Enhanced Text Processing
    SELECT
        s.s_suppkey,
        s.s_comment,
        -- Basic metrics with NULL handling
        COALESCE(LENGTH(s.s_comment), 0) AS comment_length,

        -- Positive sentiment analysis
        (
            SELECT COUNT(1)
            FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
            WHERE REGEXP_CONTAINS(word, r'quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior')
        ) AS positive_mentions,

        -- Negative sentiment analysis
        (
            SELECT COUNT(1)
            FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
            WHERE REGEXP_CONTAINS(word, r'delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed')
        ) AS negative_mentions,

        -- Enhanced sentiment calculation
        CASE
            WHEN s.s_comment IS NULL THEN 'Unknown'
            WHEN LENGTH(TRIM(s.s_comment)) = 0 THEN 'No Comment'
            WHEN (
                SELECT COUNT(1)
                FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                WHERE REGEXP_CONTAINS(word, r'quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior')
            ) > (
                SELECT COUNT(1)
                FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                WHERE REGEXP_CONTAINS(word, r'delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed')
            ) THEN 'Positive'
            WHEN (
                SELECT COUNT(1)
                FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                WHERE REGEXP_CONTAINS(word, r'quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior')
            ) < (
                SELECT COUNT(1)
                FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                WHERE REGEXP_CONTAINS(word, r'delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed')
            ) THEN 'Negative'
            ELSE 'Neutral'
        END AS sentiment,

        -- Topic-specific mention counting
        (
            SELECT COUNT(1)
            FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
            WHERE REGEXP_CONTAINS(word, r'price|cost|discount|rate|value|expense')
        ) AS financial_mentions,

        (
            SELECT COUNT(1)
            FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
            WHERE REGEXP_CONTAINS(word, r'ship|deliver|transport|carry|move|logistics')
        ) AS logistics_mentions,

        (
            SELECT COUNT(1)
            FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
            WHERE REGEXP_CONTAINS(word, r'part|item|product|stock|component|material')
        ) AS product_mentions

    FROM `@full_dataset.SUPPLIER` s
    WHERE s.s_suppkey IS NOT NULL
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
SELECT *
FROM final_supplier_report
LIMIT 1000;