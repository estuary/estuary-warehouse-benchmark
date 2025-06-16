-- Query 1 
-- Description: Basic query to sum the L_EXTENDEDPRICE column in the lineitem table
-- Difficulty: Easy

Select 
       sum(L_EXTENDEDPRICE) as sum
from snowflake_sample_data.tpch_sf1000.lineitem;