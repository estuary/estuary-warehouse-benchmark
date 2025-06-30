from dotenv import load_dotenv
import os

load_dotenv()

# Get environment variables
project_id = os.getenv("BIGQUERY_PROJECT_ID")
dataset = os.getenv("BIGQUERY_DATASET")

# Construct the full dataset path
full_dataset = f"{project_id}.{dataset}"

queries = [
    ("Query-1-Basic-Selection", f"""
    SELECT 
        SUM(L_EXTENDEDPRICE) AS sum
    FROM `{full_dataset}.LINEITEM`
    """),
    ("Query-2", f"""
    SELECT 
        COUNT(*) AS count_of_line_items,
        SUM(CAST(JSON_VALUE(json_data, '$.L_EXTENDEDPRICE') AS FLOAT64)) AS sum,
        AVG(CAST(JSON_VALUE(json_data, '$.L_DISCOUNT') AS FLOAT64)) AS avg,
        MIN(CAST(JSON_VALUE(json_data, '$.L_SHIPDATE') AS DATE)) AS min,
        MAX(CAST(JSON_VALUE(json_data, '$.L_RECEIPTDATE') AS DATE)) AS max
    FROM `{full_dataset}.jLINEITEM`
    """),
    ("Query-3", f"""
    SELECT 
        L_ORDERKEY, 
        L_LINENUMBER, 
        L_SHIPDATE, 
        L_EXTENDEDPRICE, 
        LEAD(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS next_line_price,
        LAG(L_SHIPDATE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS prev_ship_date,
        FIRST_VALUE(L_EXTENDEDPRICE) OVER (PARTITION BY L_ORDERKEY ORDER BY L_LINENUMBER) AS first_line_price
    FROM `{full_dataset}.LINEITEM`
    ORDER BY L_ORDERKEY, L_LINENUMBER 
    LIMIT 1000
    """),
    ("Query-4", f"""
    WITH base_table AS (
        SELECT 
                FORMAT_DATE('%Y-%m', CAST(JSON_VALUE(li.json_data, '$.L_SHIPDATE') AS DATE)) AS ship_year_month,
                JSON_VALUE(li.json_data, '$.L_SHIPMODE') AS L_SHIPMODE,
                JSON_VALUE(ord.json_data, '$.O_ORDERPRIORITY') AS order_priority,
                COUNT(*) AS count_of_line_items,
                SUM(CAST(JSON_VALUE(li.json_data, '$.L_EXTENDEDPRICE') AS FLOAT64)) AS sum,
                AVG(CAST(JSON_VALUE(li.json_data, '$.L_DISCOUNT') AS FLOAT64)) AS avg
            FROM `{full_dataset}.jLINEITEM` li
            LEFT JOIN `{full_dataset}.jORDERS` ord 
                ON CAST(JSON_VALUE(li.json_data, '$.L_ORDERKEY') AS INT64) = CAST(JSON_VALUE(ord.json_data, '$.O_ORDERKEY') AS INT64)
            GROUP BY ship_year_month, L_SHIPMODE, order_priority
        )
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY sum) AS row_number_by_order_priority,
            ROW_NUMBER() OVER (PARTITION BY L_SHIPMODE ORDER BY avg) AS row_number_by_ship_mode
        FROM base_table 
        LIMIT 1000;
    """),
    ("Query-5", f"""
        WITH customer_sales AS (
            SELECT 
                FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
                c.c_name,
                SUM(o.o_totalprice) AS total_spent,
                SUM(l.l_quantity) AS total_quantity,
                RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(o.o_totalprice) DESC) AS price_rank,
                RANK() OVER (PARTITION BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) ORDER BY SUM(l.l_quantity) DESC) AS quantity_rank
            FROM `{full_dataset}.ORDERS` o
            JOIN `{full_dataset}.LINEITEM` l ON o.o_orderkey = l.l_orderkey
            JOIN `{full_dataset}.CUSTOMER` c ON o.o_custkey = c.c_custkey
            GROUP BY FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)), c.c_name, o.o_orderdate
        )
        SELECT order_month, c_name, total_spent, total_quantity, price_rank, quantity_rank
        FROM customer_sales
        WHERE price_rank <= 3 OR quantity_rank <= 3
        ORDER BY order_month, price_rank, quantity_rank 
        LIMIT 1000
    """),
    ("Query-6", f"""
    WITH combined_comments AS (
        SELECT 
            FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
            LOWER(TRIM(REGEXP_REPLACE(o.o_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
        FROM `{full_dataset}.ORDERS` o

        UNION ALL

        SELECT 
            FORMAT_DATE('%Y - %B', DATE(o.o_orderdate)) AS order_month,
            LOWER(TRIM(REGEXP_REPLACE(l.l_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
        FROM `{full_dataset}.LINEITEM` l
        JOIN `{full_dataset}.ORDERS` o 
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
        LIMIT 1000
    """),
    ("Query-7", f"""
        WITH customer_sales AS (
            SELECT 
                FORMAT_DATE('%Y - %B', CAST(JSON_VALUE(o.json_data, '$.O_ORDERDATE') AS DATE)) AS order_month,
                JSON_VALUE(c.json_data, '$.C_NAME') AS c_name,
                SUM(CAST(JSON_VALUE(o.json_data, '$.O_TOTALPRICE') AS FLOAT64)) AS total_spent,
                SUM( CAST(CAST(JSON_VALUE(l.json_data, '$.L_QUANTITY') AS FLOAT64) AS INT64)) AS total_quantity
            FROM `{full_dataset}.jORDERS` o
            JOIN `{full_dataset}.jLINEITEM` l 
                ON CAST(JSON_VALUE(o.json_data, '$.O_ORDERKEY') AS INT64) = CAST(JSON_VALUE(l.json_data, '$.L_ORDERKEY') AS INT64)
            JOIN `{full_dataset}.jCUSTOMER` c 
                ON CAST(JSON_VALUE(o.json_data, '$.O_CUSTKEY') AS INT64) = CAST(JSON_VALUE(c.json_data, '$.C_CUSTKEY') AS INT64)
            GROUP BY order_month, c_name
        ),
        ranked_sales AS (
            SELECT 
                order_month,
                c_name,
                total_spent,
                total_quantity,
                RANK() OVER (PARTITION BY order_month ORDER BY total_spent DESC) AS price_rank,
                RANK() OVER (PARTITION BY order_month ORDER BY total_quantity DESC) AS quantity_rank
            FROM customer_sales
        ),
        number_extraction AS (
            SELECT *,
                REGEXP_REPLACE(c_name, r'[^0-9]', '') AS customer_number  
            FROM ranked_sales
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
        LIMIT 1000

    """),
    ("Query-8", f"""
        WITH order_comments AS (
            SELECT 
                c.c_custkey,
                c.c_name,
                LOWER(TRIM(REGEXP_REPLACE(o.o_comment, r'[^a-zA-Z0-9 ]', ''))) AS cleaned_comment,
                CASE 
                    WHEN LENGTH(o.o_comment) > 100 THEN 'LONG_COMMENT'
                    ELSE 'SHORT_COMMENT'
                END AS comment_type
            FROM `{full_dataset}.ORDERS` o
            JOIN `{full_dataset}.CUSTOMER` c ON o.o_custkey = c.c_custkey
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
            FROM `{full_dataset}.LINEITEM` l
            JOIN `{full_dataset}.ORDERS` o ON l.l_orderkey = o.o_orderkey
            JOIN `{full_dataset}.CUSTOMER` c ON o.o_custkey = c.c_custkey
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
            WHERE ccc.total_comments > 5
        ),
        word_analysis AS (
            SELECT 
                c_custkey,
                c_name,
                comment_type,
                total_comments,
                word,
                COUNT(*) AS word_frequency
            FROM filtered_comments,
            UNNEST(SPLIT(cleaned_comment, ' ')) as word
            WHERE LENGTH(word) > 3
            GROUP BY c_custkey, c_name, comment_type, total_comments, word
        ),
        top_words AS (
            SELECT 
                c_custkey,
                c_name,
                comment_type,
                total_comments,
                word,
                word_frequency,
                RANK() OVER (PARTITION BY c_custkey ORDER BY word_frequency DESC) AS word_rank
            FROM word_analysis
        )
        SELECT 
            c_custkey,
            c_name,
            comment_type,
            total_comments,
            STRING_AGG(word, ', ' ORDER BY word_rank) AS top_words
        FROM top_words
        WHERE word_rank <= 3
        GROUP BY c_custkey, c_name, comment_type, total_comments
        ORDER BY total_comments DESC, c_custkey
        LIMIT 1000
    """),
    ("Query-9", f"""
        WITH customer_order_analysis AS (
            SELECT 
                c.c_custkey,
                c.c_name,
                c.c_nationkey,
                COUNT(DISTINCT o.o_orderkey) AS total_orders,
                SUM(o.o_totalprice) AS total_spent,
                AVG(o.o_totalprice) AS avg_order_value,
                MIN(o.o_orderdate) AS first_order_date,
                MAX(o.o_orderdate) AS last_order_date,
                COUNT(DISTINCT DATE_TRUNC(o.o_orderdate, MONTH)) AS active_months
            FROM `{full_dataset}.CUSTOMER` c
            LEFT JOIN `{full_dataset}.ORDERS` o ON c.c_custkey = o.o_custkey
            GROUP BY c.c_custkey, c.c_name, c.c_nationkey
        ),
        customer_lineitem_analysis AS (
            SELECT 
                c.c_custkey,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_ordered,
                SUM(l.l_quantity) AS total_quantity_ordered,
                AVG(l.l_discount) AS avg_discount_received,
                COUNT(DISTINCT l.l_suppkey) AS unique_suppliers
            FROM `{full_dataset}.CUSTOMER` c
            LEFT JOIN `{full_dataset}.ORDERS` o ON c.c_custkey = o.o_custkey
            LEFT JOIN `{full_dataset}.LINEITEM` l ON o.o_orderkey = l.l_orderkey
            GROUP BY c.c_custkey
        ),
        customer_ranking AS (
            SELECT 
                coa.c_custkey,
                coa.c_name,
                coa.c_nationkey,
                coa.total_orders,
                coa.total_spent,
                coa.avg_order_value,
                coa.first_order_date,
                coa.last_order_date,
                coa.active_months,
                cla.unique_parts_ordered,
                cla.total_quantity_ordered,
                cla.avg_discount_received,
                cla.unique_suppliers,
                RANK() OVER (ORDER BY coa.total_spent DESC) AS spending_rank,
                RANK() OVER (ORDER BY coa.total_orders DESC) AS order_count_rank,
                RANK() OVER (ORDER BY cla.unique_parts_ordered DESC) AS variety_rank
            FROM customer_order_analysis coa
            LEFT JOIN customer_lineitem_analysis cla ON coa.c_custkey = cla.c_custkey
        )
        SELECT 
            c_custkey,
            c_name,
            c_nationkey,
            total_orders,
            total_spent,
            avg_order_value,
            first_order_date,
            last_order_date,
            active_months,
            unique_parts_ordered,
            total_quantity_ordered,
            avg_discount_received,
            unique_suppliers,
            spending_rank,
            order_count_rank,
            variety_rank,
            CASE 
                WHEN spending_rank <= 100 AND order_count_rank <= 100 THEN 'HIGH_VALUE_HIGH_FREQUENCY'
                WHEN spending_rank <= 100 THEN 'HIGH_VALUE'
                WHEN order_count_rank <= 100 THEN 'HIGH_FREQUENCY'
                ELSE 'STANDARD'
            END AS customer_segment
        FROM customer_ranking
        WHERE spending_rank <= 500 OR order_count_rank <= 500
        ORDER BY spending_rank, order_count_rank
        LIMIT 1000
    """),
    ("Query-10", f"""
        WITH supplier_performance AS (
            SELECT 
                s.s_suppkey,
                s.s_name,
                s.s_nationkey,
                s.s_acctbal,
                COUNT(DISTINCT l.l_orderkey) AS total_orders_supplied,
                SUM(l.l_quantity) AS total_quantity_supplied,
                SUM(l.l_extendedprice) AS total_revenue_generated,
                AVG(l.l_discount) AS avg_discount_offered,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_supplied,
                MIN(l.l_shipdate) AS first_shipment_date,
                MAX(l.l_shipdate) AS last_shipment_date
            FROM `{full_dataset}.SUPPLIER` s
            LEFT JOIN `{full_dataset}.LINEITEM` l ON s.s_suppkey = l.l_suppkey
            GROUP BY s.s_suppkey, s.s_name, s.s_nationkey, s.s_acctbal
        ),
        supplier_ranking AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY total_revenue_generated DESC) AS revenue_rank,
                RANK() OVER (ORDER BY total_quantity_supplied DESC) AS quantity_rank,
                RANK() OVER (ORDER BY unique_parts_supplied DESC) AS variety_rank,
                RANK() OVER (ORDER BY total_orders_supplied DESC) AS order_rank
            FROM supplier_performance
        )
        SELECT 
            s_suppkey,
            s_name,
            s_nationkey,
            s_acctbal,
            total_orders_supplied,
            total_quantity_supplied,
            total_revenue_generated,
            avg_discount_offered,
            unique_parts_supplied,
            first_shipment_date,
            last_shipment_date,
            revenue_rank,
            quantity_rank,
            variety_rank,
            order_rank,
            CASE 
                WHEN revenue_rank <= 50 AND quantity_rank <= 50 THEN 'TOP_PERFORMER'
                WHEN revenue_rank <= 100 OR quantity_rank <= 100 THEN 'HIGH_PERFORMER'
                WHEN revenue_rank <= 500 OR quantity_rank <= 500 THEN 'MEDIUM_PERFORMER'
                ELSE 'LOW_PERFORMER'
            END AS performance_category
        FROM supplier_ranking
        WHERE revenue_rank <= 1000 OR quantity_rank <= 1000
        ORDER BY revenue_rank, quantity_rank
        LIMIT 1000
    """),
    ("Query-11", f"""
        WITH supplier_delivery_metrics AS (
            SELECT
                s.s_suppkey,
                s.s_name,
                s.s_nationkey,
                s.s_acctbal,
                s.s_address,
                s.s_phone,
                COUNT(DISTINCT l.l_orderkey) AS total_orders,
                AVG(DATE_DIFF(l.l_receiptdate, l.l_shipdate, DAY)) AS avg_delivery_delay,
                PERCENTILE_CONT(l.l_discount, 0.5) OVER (PARTITION BY s.s_suppkey) AS median_delivery_delay,
                PERCENTILE_CONT(l.l_discount, 0.9) OVER (PARTITION BY s.s_suppkey) AS p90_delivery_delay,
                COUNTIF(DATE_DIFF(l.l_receiptdate, l.l_shipdate, DAY) > 7) AS late_deliveries,
                COUNTIF(DATE_DIFF(l.l_receiptdate, l.l_shipdate, DAY) <= 7) AS on_time_deliveries,
                COUNTIF(DATE_DIFF(l.l_receiptdate, l.l_shipdate, DAY) > 14) AS severely_late_deliveries,
                SUM(l.l_extendedprice) AS total_revenue,
                SUM(l.l_extendedprice * l.l_tax) AS total_tax,
                SUM(l.l_quantity) AS total_quantity,
                AVG(l.l_discount) AS avg_discount_rate,
                MAX(l.l_discount) AS max_discount_offered,
                STDDEV(l.l_extendedprice) AS revenue_volatility,
                VARIANCE(l.l_extendedprice) AS revenue_variance,
                MIN(l.l_shipdate) AS first_shipment_date,
                MAX(l.l_shipdate) AS last_shipment_date,
                DATE_DIFF(MAX(l.l_shipdate), MIN(l.l_shipdate), DAY) AS active_days,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_shipped
            FROM `{full_dataset}.SUPPLIER` s
            LEFT JOIN `{full_dataset}.LINEITEM` l ON s.s_suppkey = l.l_suppkey
            WHERE l.l_suppkey IS NOT NULL
            GROUP BY s.s_suppkey, s.s_name, s.s_nationkey, s.s_acctbal, s.s_address, s.s_phone
        ),
        supplier_transaction_frequency AS (
            SELECT
                l.l_suppkey,
                COUNT(DISTINCT DATE_TRUNC(l.l_shipdate, MONTH)) AS transactions_per_month,
                AVG(l.l_quantity) AS quantity_per_month,
                AVG(l.l_extendedprice) AS revenue_per_month,
                LAG(COUNT(DISTINCT DATE_TRUNC(l.l_shipdate, MONTH))) OVER (PARTITION BY l.l_suppkey ORDER BY DATE_TRUNC(l.l_shipdate, MONTH)) AS prev_month_transactions,
                LAG(AVG(l.l_quantity)) OVER (PARTITION BY l.l_suppkey ORDER BY DATE_TRUNC(l.l_shipdate, MONTH)) AS prev_month_quantity,
                LAG(AVG(l.l_extendedprice)) OVER (PARTITION BY l.l_suppkey ORDER BY DATE_TRUNC(l.l_shipdate, MONTH)) AS prev_month_revenue,
                DATE_DIFF(CURRENT_DATE(), MAX(l.l_shipdate), MONTH) AS months_since_last_activity
            FROM `{full_dataset}.LINEITEM` l
            GROUP BY l.l_suppkey, DATE_TRUNC(l.l_shipdate, MONTH)
        ),
        supplier_seasonality_quarterly AS (
            SELECT
                l.l_suppkey,
                EXTRACT(QUARTER FROM l.l_shipdate) AS quarter,
                EXTRACT(YEAR FROM l.l_shipdate) AS year,
                COUNT(DISTINCT l.l_orderkey) AS orders_in_quarter,
                SUM(l.l_quantity) AS quantity_in_quarter,
                SUM(l.l_extendedprice) AS revenue_in_quarter,
                COUNT(DISTINCT l.l_partkey) AS unique_parts_in_quarter,
                COUNT(DISTINCT DATE_TRUNC(l.l_shipdate, MONTH)) AS active_months_in_quarter
            FROM `{full_dataset}.LINEITEM` l
            GROUP BY l.l_suppkey, EXTRACT(QUARTER FROM l.l_shipdate), EXTRACT(YEAR FROM l.l_shipdate)
        ),
        supplier_seasonality_quarterly_agg AS (
            SELECT
                l_suppkey,
                ARRAY_AGG(STRUCT(quarter, year, orders_in_quarter, quantity_in_quarter, revenue_in_quarter) ORDER BY year, quarter) AS quarterly_data,
                MAX(quantity_in_quarter) AS peak_quantity_quarter,
                MAX(revenue_in_quarter) AS peak_revenue_quarter,
                STDDEV(quantity_in_quarter) AS avg_quantity_volatility,
                STDDEV(revenue_in_quarter) AS avg_revenue_volatility,
                MAX(revenue_in_quarter) - MIN(revenue_in_quarter) AS max_revenue_growth,
                MIN(revenue_in_quarter) - MAX(revenue_in_quarter) AS min_revenue_growth,
                AVG(unique_parts_in_quarter) AS avg_unique_parts_per_quarter,
                MAX(active_months_in_quarter) AS max_active_months_in_quarter,
                AVG(quantity_in_quarter / NULLIF(orders_in_quarter, 0)) AS overall_avg_quantity_per_order,
                AVG(revenue_in_quarter / NULLIF(orders_in_quarter, 0)) AS overall_avg_revenue_per_order,
                CASE 
                    WHEN AVG(revenue_in_quarter) > 0 THEN STDDEV(revenue_in_quarter) / AVG(revenue_in_quarter)
                    ELSE NULL
                END AS revenue_coefficient_of_variation
            FROM supplier_seasonality_quarterly
            GROUP BY l_suppkey
        ),
        supplier_seasonality_monthly AS (
            SELECT
                l.l_suppkey,
                EXTRACT(MONTH FROM l.l_shipdate) AS month,
                EXTRACT(YEAR FROM l.l_shipdate) AS year,
                COUNT(DISTINCT l.l_orderkey) AS orders_in_month,
                SUM(l.l_quantity) AS quantity_in_month,
                SUM(l.l_extendedprice) AS revenue_in_month
            FROM `{full_dataset}.LINEITEM` l
            GROUP BY l.l_suppkey, EXTRACT(MONTH FROM l.l_shipdate), EXTRACT(YEAR FROM l.l_shipdate)
        ),
        supplier_seasonality_monthly_agg AS (
            SELECT
                l_suppkey,
                ARRAY_AGG(STRUCT(month, year, orders_in_month, quantity_in_month, revenue_in_month) ORDER BY year, month) AS monthly_data,
                MAX(quantity_in_month) AS peak_quantity_month,
                MAX(revenue_in_month) AS peak_revenue_month,
                STDDEV(quantity_in_month) AS avg_monthly_quantity_volatility,
                STDDEV(revenue_in_month) AS avg_monthly_revenue_volatility,
                MAX(revenue_in_month) - MIN(revenue_in_month) AS max_monthly_revenue_growth,
                MIN(revenue_in_month) - MAX(revenue_in_month) AS min_monthly_revenue_growth,
                CASE 
                    WHEN AVG(revenue_in_month) > 0 THEN STDDEV(revenue_in_month) / AVG(revenue_in_month)
                    ELSE NULL
                END AS monthly_revenue_coefficient_of_variation
            FROM supplier_seasonality_monthly
            GROUP BY l_suppkey
        ),
        supplier_risk_scores AS (
            SELECT
                sdm.s_suppkey,
                CASE 
                    WHEN sdm.total_orders > 0 THEN (sdm.late_deliveries / sdm.total_orders) * 100
                    ELSE 0
                END AS delivery_risk_score,
                CASE 
                    WHEN sdm.revenue_volatility > 0 AND sdm.total_revenue > 0 THEN (sdm.revenue_volatility / sdm.total_revenue) * 100
                    ELSE 0
                END AS financial_risk_score,
                CASE 
                    WHEN sdm.unique_parts_shipped > 0 THEN (100 - (sdm.unique_parts_shipped / 100))
                    ELSE 100
                END AS diversification_risk_score,
                CASE 
                    WHEN sdm.avg_discount_rate > 0.1 THEN 100
                    WHEN sdm.avg_discount_rate > 0.05 THEN 50
                    ELSE 0
                END AS quality_risk_score
            FROM supplier_delivery_metrics sdm
        ),
        part_supplier_diversity AS (
            SELECT
                p.p_partkey,
                COUNT(DISTINCT ps.ps_suppkey) AS supplier_count
            FROM `{full_dataset}.PART` p
            LEFT JOIN `{full_dataset}.PARTSUPP` ps ON p.p_partkey = ps.ps_partkey
            GROUP BY p.p_partkey
        ),
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
            FROM `{full_dataset}.PARTSUPP` ps
            JOIN `{full_dataset}.PART` p ON ps.ps_partkey = p.p_partkey
            LEFT JOIN part_supplier_diversity psd ON p.p_partkey = psd.p_partkey
            GROUP BY ps.ps_suppkey
        ),
        supplier_comment_analysis AS (
            SELECT
                s.s_suppkey,
                s.s_comment,
                COALESCE(LENGTH(s.s_comment), 0) AS comment_length,
                (
                    SELECT COUNT(1) 
                    FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                    WHERE REGEXP_CONTAINS(word, r'quality|reliable|good|excellent|best|quick|fast|prompt|timely|superior')
                ) AS positive_mentions,
                (
                    SELECT COUNT(1)
                    FROM UNNEST(SPLIT(LOWER(s.s_comment), ' ')) word
                    WHERE REGEXP_CONTAINS(word, r'delay|issue|problem|complaint|late|bad|slow|poor|dissatisfied|disappointed')
                ) AS negative_mentions,
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
            FROM `{full_dataset}.SUPPLIER` s
            WHERE s.s_suppkey IS NOT NULL
        ),
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
        SELECT * FROM final_supplier_report LIMIT 1000
    """)
]
