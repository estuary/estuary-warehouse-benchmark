# Snowflake Query Performance Monitoring Executing Script

## Overview

This repo has one Python script to execute the SQL queries linearly and an environment file to plug in credentials.

| Sno | File Name   | Comment          |
|-----|------------|------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Takes in your credentials |



## Getting Started

To run the Snowflake benchmark, plug in your own credentials by setting the following environment variables:

```bash
# Snowflake credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT= which will look like ->  ejbgmce-zy85181

# Snowflake configuration
SNOWFLAKE_DATABASE=SNOWFLAKE_SAMPLE_DATA or your own database
SNOWFLAKE_SCHEMA=TPCH_SF1000 or your own schema
SNOWFLAKE_WAREHOUSE=COMPUTE_MEDIUM or your own warehouse

# Benchmark configuration
QUERY_TAG=linear (not mandatory but will be helpful for tagging results)
```

## Setup

Install the required dependencies mentioned inside the Snowflake folder.

```bash
pip install -r requirements.txt

```
## Running the code 

Inside the Snowflake folder run

```bash
python main.py
```




Queries used for the Benchmark Report can be found [here](queries.py).


Benchmark report response time will be recorded in `query_stats.csv` file.


