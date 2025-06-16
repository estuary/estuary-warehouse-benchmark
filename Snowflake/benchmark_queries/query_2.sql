-- Query 2 
-- Description: Basic aggregating query to plug foundational aggregate functions on the lineitem table
-- Difficulty: Easy(JSON)

SELECT 
    COUNT(*) AS count_of_line_items,
    SUM(lineitem:l_extendedprice::FLOAT) AS sum,
    AVG(lineitem:l_discount::FLOAT) AS avg,
    MIN(lineitem:l_shipdate::DATE) AS min,
    MAX(lineitem:l_receiptdate::DATE) AS max
FROM snowflake_sample_data.tpch_sf1000.jlineitem;