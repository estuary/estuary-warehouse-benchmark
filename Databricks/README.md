# Databricks Query Performance Monitoring Script

## Overview

This repository contains a Python script to execute SQL queries linearly and an environment file for credentials configuration.

| No. | File Name   | Description          |
|-----|------------|---------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Contains your credentials |

## Getting Started

To run the Databricks benchmark, configure your credentials by setting the following environment variables:

```bash
SERVER_HOSTNAME=your_hostname  # e.g., cdc-649a22e0-78e0.cloud.databricks.com
HTTP_PATH=your_http_path  # e.g., /sql/1.0/warehouses/cdc7c76b0bd70a9f
ACCESS_TOKEN=your_access_token  # e.g., cdcib3ba4213eb685c578a5547d02cee1413
DATABASE=workspace
SCHEMA=tcph_1000
WAREHOUSE=small
```

## Setup

Install the required dependencies in the Databricks folder:

```bash
pip install -r requirements.txt
```

## Running the Code

Inside the Databricks folder, run:

```bash
python main.py
```

## Output

- Queries used for the benchmark report can be found in [`queries.py`](queries.py).
- Benchmark response times will be recorded in the `query_stats.csv` file.


