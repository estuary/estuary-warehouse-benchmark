# Snowflake Query Performance Monitoring Script

## Overview

This repository contains a Python script to execute SQL queries linearly and an environment file for credentials configuration.

| No. | File Name   | Description          |
|-----|------------|---------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Contains your credentials |

## Getting Started

To run the Snowflake benchmark, configure your credentials by setting the following environment variables:

```bash
# Snowflake credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_id  # e.g., ejbgmce-zy85181

# Snowflake configuration
SNOWFLAKE_DATABASE=SNOWFLAKE_SAMPLE_DATA  # or your own database
SNOWFLAKE_SCHEMA=TPCH_SF1000  # or your own schema
SNOWFLAKE_WAREHOUSE=COMPUTE_MEDIUM  # or your own warehouse

# Benchmark configuration
QUERY_TAG=linear  # optional but helpful for tagging results
```

## Setup

Install the required dependencies in the Snowflake folder:

```bash
pip install -r requirements.txt
```

## Running the Code

Inside the Snowflake folder, run:

```bash
python main.py
```

## Output

- Queries used for the benchmark report can be found in [`queries.py`](queries.py).
- Benchmark response times will be recorded in the `query_stats.csv` file.


