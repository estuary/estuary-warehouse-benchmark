
-- Query-6
WITH combined_comments AS (
    SELECT
        FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM') AS order_month,
        LOWER(TRIM(REPLACE(REPLACE(REPLACE(JSON_VALUE(o.ORDERS_JSON, '$.o_comment'),
            '!@#$%^&*()_+-=[]{};":<>,.?/|', ' '), '''', ' '), '\', ' '))) AS cleaned_comment
    FROM [your_schema].JORDERS o

    UNION ALL

    SELECT
        FORMAT(TRY_CONVERT(DATE, JSON_VALUE(o.ORDERS_JSON, '$.o_orderdate')), 'yyyy - MMMM') AS order_month,
        LOWER(TRIM(REPLACE(REPLACE(REPLACE(JSON_VALUE(l.LINEITEM_JSON, '$.l_comment'),
            '!@#$%^&*()_+-=[]{};":<>,.?/|', ' '), '''', ' '), '\', ' '))) AS cleaned_comment
    FROM [your_schema].JLINEITEM l
    JOIN [your_schema].JORDERS o
    ON TRY_CONVERT(INT, JSON_VALUE(l.LINEITEM_JSON, '$.l_orderkey')) = TRY_CONVERT(INT, JSON_VALUE(o.ORDERS_JSON, '$.o_orderkey'))
),
tokenized_words AS (
    SELECT
        order_month,
        value AS word
    FROM combined_comments
    CROSS APPLY STRING_SPLIT(cleaned_comment, ' ')
),
word_counts AS (
    SELECT
        order_month,
        word,
        COUNT_BIG(*) AS word_count,
        RANK() OVER (PARTITION BY order_month ORDER BY COUNT_BIG(*) DESC) AS rank
    FROM tokenized_words
    WHERE LEN(word) > 0
    AND word NOT IN ('the', 'is', 'and', 'or', 'a', 'an', 'of', 'to', 'in', 'for', 'on', 'with', 'at')
    GROUP BY order_month, word
)
SELECT TOP 1000 order_month, word, word_count
FROM word_counts
WHERE rank <= 5
ORDER BY order_month, word_count DESC;
