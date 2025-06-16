-- Query 2
-- Description: Calculates the count, sum of extended price, average discount, minimum ship date, and maximum receipt date from the lineitem table.
-- Difficulty: Easy
SELECT
    COUNT(*) AS count_of_line_items,
    SUM(CAST(l_extendedprice AS FLOAT64)) AS sum,
    AVG(CAST(l_discount AS FLOAT64)) AS avg,
    MIN(CAST(l_shipdate AS DATE)) AS min,
    MAX(CAST(l_receiptdate AS DATE)) AS max
FROM `@full_dataset.LINEITEM`