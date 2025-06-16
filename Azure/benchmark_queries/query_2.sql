-- Query-2
SELECT
    COUNT_BIG(*) AS count_of_line_items,
    SUM(CAST(JSON_VALUE(LINEITEM_JSON, '$.l_extendedprice') AS FLOAT)) AS sum,
    AVG(CAST(JSON_VALUE(LINEITEM_JSON, '$.l_discount') AS FLOAT)) AS avg,
    MIN(CAST(JSON_VALUE(LINEITEM_JSON, '$.l_shipdate') AS DATE)) AS min,
    MAX(CAST(JSON_VALUE(LINEITEM_JSON, '$.l_receiptdate') AS DATE)) AS max
FROM [your_schema].[JLINEITEM];

