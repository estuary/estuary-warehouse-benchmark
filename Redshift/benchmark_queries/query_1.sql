-- Query 1
-- Description: Basic query to sum the l_extendedprice column from the lineitem table.
-- Difficulty: Easy
SELECT
    SUM(l_extendedprice) AS total_extended_price
    FROM
    lineitem;