-- Query 1
-- Description: Basic query to sum the L_EXTENDEDPRICE column in the lineitem table
-- Difficulty: Easy
SELECT
    SUM(L_EXTENDEDPRICE) AS sum
FROM `@full_dataset.LINEITEM`