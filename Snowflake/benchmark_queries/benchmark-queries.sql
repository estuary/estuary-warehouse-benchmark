-- Query 1 
-- Description: Basic query to sum the L_EXTENDEDPRICE column in the lineitem table
-- Difficulty: Easy

Select 
       sum(L_EXTENDEDPRICE) as sum
from snowflake_sample_data.tpch_sf1000.lineitem;

-- Query 2 
-- Description: Basic aggregating query to plug foundational aggregate functions on the lineitem table
-- Difficulty: Easy(JSON)

SELECT 
    COUNT(*) AS count_of_line_items,
    SUM(lineitem:l_extendedprice::FLOAT) AS sum,
    AVG(lineitem:l_discount::FLOAT) AS avg,
    MIN(lineitem:l_shipdate::DATE) AS min,
    MAX(lineitem:l_receiptdate::DATE) AS max
FROM snowflake_sample_data.tpch_sf1000.jlineitem;

-- Query 3
-- Description: This query retrieves order details from the lineitem table, including the current, next, and first extended prices, as well as the previous shipping date, using window functions for analysis.
-- Difficulty: Easy

SELECT 
    L_ORDERKEY, 
    L_LINENUMBER, 
    L_SHIPDATE, 
    L_EXTENDEDPRICE, 
    LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
    LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
    FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
FROM snowflake_sample_data.tpch_sf1000.lineitem
ORDER BY L_ORDERKEY, L_LINENUMBER;


-- Query 4
-- Description: Left joins Orders and Lineitem tables to aggregate metrics as CTE and rank them using ROW_NUMBER() window function
-- Difficulty: Medium(JSON)  


WITH base_table AS (
    SELECT 
        TO_CHAR(lineitem:l_shipdate::DATE, 'YYYY-MM') AS ship_year_month,
        lineitem:l_shipmode::STRING AS L_SHIPMODE,
        orders:o_orderpriority::STRING AS order_priority,
        COUNT(*) AS count_of_line_items,
        SUM(lineitem:l_extendedprice::FLOAT) AS sum,
        AVG(lineitem:l_discount::FLOAT) AS avg
    FROM snowflake_sample_data.tpch_sf1000.jlineitem li
    LEFT JOIN snowflake_sample_data.tpch_sf1000.jorders ord 
        ON li.lineitem:l_orderkey::INT = ord.orders:o_orderkey::INT
    GROUP BY ship_year_month, L_SHIPMODE, order_priority
)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
    ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
FROM base_table;




-- Query 5
-- Description: This query joins 3 tables & retrieves the top 3 customers based on total spent and total quantity for each month, ranking them using RANK() window function
-- Difficulty: Medium 
WITH customer_sales AS (
    SELECT 
        TO_CHAR(o.o_orderdate, 'YYYY - Month') AS order_month,  -- Formatting Year and Month Name
        c.c_name,
        SUM(o.o_totalprice) AS total_spent,
        SUM(l.l_quantity) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Month') ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(o.o_orderdate, 'YYYY - Month') ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
    FROM snowflake_sample_data.tpch_sf1000.orders o
    JOIN snowflake_sample_data.tpch_sf1000.lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN snowflake_sample_data.tpch_sf1000.customer c ON o.o_custkey = c.c_custkey
    GROUP BY order_month, c.c_name
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM customer_sales
WHERE price_rank <= 3 OR quantity_rank <= 3
ORDER BY order_month, price_rank, quantity_rank;

-- Query 6
-- Description: This query combines comments from the Orders and Lineitem tables, cleans them, tokenizes them, and ranks the top 5 words for each month based on frequency
-- Difficulty: Medium
WITH combined_comments AS (
    SELECT 
        TO_CHAR(o.o_orderdate, 'YYYY - Month') AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(o.o_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM snowflake_sample_data.tpch_sf1000.orders o

    UNION ALL

    SELECT 
        TO_CHAR(o.o_orderdate, 'YYYY - Month') AS order_month,
        LOWER(TRIM(REGEXP_REPLACE(l.l_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
    FROM snowflake_sample_data.tpch_sf1000.lineitem l
    JOIN snowflake_sample_data.tpch_sf1000.orders o 
    ON l.l_orderkey = o.o_orderkey
),
tokenized_words AS (
    SELECT 
        order_month,
        value AS word
    FROM combined_comments,
         LATERAL FLATTEN(input => SPLIT(cleaned_comment, ' '))
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
ORDER BY order_month, word_count DESC;




-- Query 7
-- Description: Take Query 5 as base query and add a filter to only include customers with an odd sum of digits in their name and get the top 3 customers for each month by total spent or total quantity
-- Difficulty: Hard(JSON)

WITH customer_sales AS (
    SELECT 
        TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') AS order_month,  -- Formatting Year and Month Name
        customer:c_name::STRING AS c_name,
        SUM(orders:o_totalprice::FLOAT) AS total_spent,
        SUM(lineitem:l_quantity::INT) AS total_quantity,
        RANK() OVER (PARTITION BY TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') ORDER BY SUM(orders:o_totalprice::FLOAT) DESC) AS price_rank,
        RANK() OVER (PARTITION BY TO_CHAR(orders:o_orderdate::DATE, 'YYYY - Month') ORDER BY SUM(lineitem:l_quantity::INT) DESC) AS quantity_rank
    FROM snowflake_sample_data.tpch_sf1000.jorders o
    JOIN snowflake_sample_data.tpch_sf1000.jlineitem l 
        ON o.orders:o_orderkey::INT = l.lineitem:l_orderkey::INT
    JOIN snowflake_sample_data.tpch_sf1000.jcustomer c 
        ON o.orders:o_custkey::INT = c.customer:c_custkey::INT
    GROUP BY order_month, c_name
),
number_extraction AS (
    SELECT *,
           REGEXP_REPLACE(c_name, '[^0-9]', '') AS customer_number  
    FROM customer_sales
),
digit_sum_calc AS (
    SELECT 
        order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number,
        SUM(value::INT) AS number_sum 
    FROM number_extraction,
         LATERAL FLATTEN(input => SPLIT(customer_number, ''))  
    GROUP BY order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank, customer_number
)
SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
FROM digit_sum_calc
WHERE (price_rank <= 3 OR quantity_rank <= 3)  
AND MOD(number_sum, 2) = 1  
ORDER BY order_month, price_rank, quantity_rank;

-- Query 8
-- Description: This query combines comments from the Orders and Lineitem tables, cleans them, tokenizes them, and ranks the top 5 words for each month based on frequency
-- Difficulty: Hard

WITH order_comments AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        LOWER(TRIM(REGEXP_REPLACE(o.o_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE 
            WHEN LENGTH(o.o_comment) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM snowflake_sample_data.tpch_sf1000.orders o
    JOIN snowflake_sample_data.tpch_sf1000.customer c ON o.o_custkey = c.c_custkey
),
lineitem_comments AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        LOWER(TRIM(REGEXP_REPLACE(l.l_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
        CASE 
            WHEN LENGTH(l.l_comment) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM snowflake_sample_data.tpch_sf10.lineitem l
    JOIN snowflake_sample_data.tpch_sf10.orders o ON l.l_orderkey = o.o_orderkey
    JOIN snowflake_sample_data.tpch_sf10.customer c ON o.o_custkey = c.c_custkey
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
    LISTAGG(rc.cleaned_comment || ' (' || rc.comment_type || ')', ' | ') 
        WITHIN GROUP (ORDER BY rc.comment_rank) AS customer_comments,
    MAX(rc.total_comments) AS total_comments_per_customer
FROM ranked_comments rc
WHERE rc.comment_rank <= 5 
GROUP BY rc.c_custkey, rc.c_name
ORDER BY total_comments_per_customer DESC;


-- Query 9
-- Description: This query retrieves the monthly summary of orders and revenue for customers in the last 30 years, including a lifetime revenue metric
-- Difficulty: Hard

WITH customer_orders AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        o.o_orderdate,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,
        COUNT(*) AS order_count,
        ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate ASC) AS order_rank
    FROM snowflake_sample_data.tpch_sf1000.customer c
    JOIN snowflake_sample_data.tpch_sf1000.orders o ON c.c_custkey = o.o_custkey
    JOIN snowflake_sample_data.tpch_sf1000.lineitem l ON o.o_orderkey = l.l_orderkey
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
        SUM(total_revenue) OVER (PARTITION BY c_custkey ORDER BY order_rank ASC) AS cumulative_revenue 
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
        ', Revenue: ' || TO_CHAR(monthly_revenue, '999,999.99'),
        ' | '
    ) WITHIN GROUP (ORDER BY order_month) AS monthly_summary,
    MAX(cumulative_revenue) AS lifetime_revenue 
FROM monthly_analysis m
JOIN cumulative_revenue c ON m.c_custkey = c.c_custkey
GROUP BY m.c_custkey, m.c_name
ORDER BY lifetime_revenue DESC;


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
