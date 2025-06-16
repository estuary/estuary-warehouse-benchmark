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

