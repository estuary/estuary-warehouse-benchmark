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
LIMIT 1000