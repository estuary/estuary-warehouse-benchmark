-- Query 2
-- Description: Calculates the count of line items, sum of extended price, average discount, minimum ship date, and maximum receipt date from the lineitem table.
-- Difficulty: Easy
SELECT
    COUNT(*)                                  AS count_of_line_items,
    SUM(lineitem.l_extendedprice::FLOAT)      AS sum_price,
    AVG(lineitem.l_discount::FLOAT)           AS avg_discount,
    MIN(lineitem.l_shipdate::DATE)            AS min_shipdate,
    MAX(lineitem.l_receiptdate::DATE)         AS max_receiptdate
    FROM
    lineitem;