-- Query 6
-- Description: Identifies the top 5 most frequent non-stop words in combined order and line item comments for each month using string manipulation and ranking.
-- Difficulty: Hard
WITH combined_comments AS (
        SELECT
            TO_CHAR(o.o_orderdate, 'YYYY - Mon') AS order_month,
            LOWER(TRIM(REGEXP_REPLACE(o.o_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
        FROM orders o
        UNION ALL
        SELECT
            TO_CHAR(o.o_orderdate, 'YYYY - Mon') AS order_month,
            LOWER(TRIM(REGEXP_REPLACE(l.l_comment, '[^a-zA-Z0-9 ]', ''))) AS cleaned_comment
        FROM lineitem l
        JOIN orders o
            ON l.l_orderkey = o.o_orderkey
    ),
    numbers AS (
        SELECT n
        FROM (SELECT ROW_NUMBER() OVER () AS n
            FROM orders
            LIMIT 50) nums
        WHERE n <= 50  -- Adjust based on max expected words
    ),
    tokenized_words AS (
        SELECT
            cc.order_month,
            TRIM(SPLIT_PART(cc.cleaned_comment, ' ', CAST(n.n AS INTEGER))) AS word
        FROM combined_comments cc
        CROSS JOIN numbers n
        WHERE TRIM(SPLIT_PART(cc.cleaned_comment, ' ', CAST(n.n AS INTEGER))) != ''
        AND n.n <= (LENGTH(cc.cleaned_comment) - LENGTH(REPLACE(cc.cleaned_comment, ' ', '')) + 1)
        AND cc.cleaned_comment IS NOT NULL
    ),
    word_counts AS (
        SELECT
            order_month,
            word,
            COUNT(*) AS word_count,
            RANK() OVER (PARTITION BY order_month ORDER BY COUNT(*) DESC) AS rank
        FROM tokenized_words
        WHERE word NOT IN ('the', 'is', 'and', 'or', 'a', 'an', 'of', 'to', 'in', 'for', 'on', 'with', 'at')
        AND word IS NOT NULL
        AND word != ''
        GROUP BY order_month, word
    )
    SELECT order_month, word, word_count
    FROM word_counts
    WHERE rank <= 5
    ORDER BY order_month, word_count DESC
    LIMIT 1000;