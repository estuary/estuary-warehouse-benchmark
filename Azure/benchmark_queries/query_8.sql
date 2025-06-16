
-- Query-8
WITH order_comments AS (
    SELECT
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey') AS c_custkey,
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_name') AS c_name,
        LOWER(TRIM(REPLACE(REPLACE(REPLACE(JSON_VALUE(o.ORDERS_JSON, '$.o_comment'), '!', ' '), '@', ' '), '#', ' '))) AS cleaned_comment,
        CASE
            WHEN LEN(JSON_VALUE(o.ORDERS_JSON, '$.o_comment')) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM [your_schema].JORDERS o
    JOIN [your_schema].JCUSTOMER c ON JSON_VALUE(o.ORDERS_JSON, '$.o_custkey') = JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')
),
lineitem_comments AS (
    SELECT
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey') AS c_custkey,
        JSON_VALUE(c.CUSTOMER_JSON, '$.c_name') AS c_name,
        LOWER(TRIM(REPLACE(REPLACE(REPLACE(JSON_VALUE(l.LINEITEM_JSON, '$.l_comment'), '!', ' '), '@', ' '), '#', ' '))) AS cleaned_comment,
        CASE
            WHEN LEN(JSON_VALUE(l.LINEITEM_JSON, '$.l_comment')) > 100 THEN 'LONG_COMMENT'
            ELSE 'SHORT_COMMENT'
        END AS comment_type
    FROM [your_schema].JLINEITEM l
    JOIN [your_schema].JORDERS o ON JSON_VALUE(l.LINEITEM_JSON, '$.l_orderkey') = JSON_VALUE(o.ORDERS_JSON, '$.o_orderkey')
    JOIN [your_schema].JCUSTOMER c ON JSON_VALUE(o.ORDERS_JSON, '$.o_custkey') = JSON_VALUE(c.CUSTOMER_JSON, '$.c_custkey')
),
combined_comments AS (
    SELECT * FROM order_comments
    UNION ALL
    SELECT * FROM lineitem_comments
),
comment_counts AS (
    SELECT
        c_custkey,
        COUNT_BIG(*) AS total_comments
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
        ROW_NUMBER() OVER (PARTITION BY c_custkey ORDER BY LEN(cleaned_comment) DESC) AS comment_rank
    FROM filtered_comments
)
SELECT TOP 1000
    rc.c_custkey,
    rc.c_name,
    STRING_AGG(rc.cleaned_comment + ' (' + rc.comment_type + ')', ' | ')
        WITHIN GROUP (ORDER BY rc.comment_rank) AS customer_comments,
    MAX(rc.total_comments) AS total_comments_per_customer
FROM ranked_comments rc
WHERE rc.comment_rank <= 5
GROUP BY rc.c_custkey, rc.c_name
ORDER BY total_comments_per_customer DESC;
