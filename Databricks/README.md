# Databricks Query Performance Monitoring Executing Script

## Overview

This repo has one Python script to execute the SQL queries linearly and an environment file to plug in credentials.

| Sno | File Name   | Comment          |
|-----|------------|------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Takes in your credentials |



## Getting Started

To run the Databricks benchmark, plug in your own credentials by setting the following environment variables:

```bash
SERVER_HOSTNAME='' looks like -> cdc-649a22e0-78e0.cloud.databricks.com
HTTP_PATH='' looks like -> /sql/1.0/warehouses/cdc7c76b0bd70a9f
ACCESS_TOKEN='' looks like cdcib3ba4213eb685c578a5547d02cee1413
DATABASE=workspace
SCHEMA=tcph_1000
WAREHOUSE=small


```

## Setup

Install the required dependencies mentioned inside the Databricks folder.

```bash
pip install -r requirements.txt

```
## Running the code 

Inside the Databricks folder run

```bash
python main.py
```




Queries used for the Benchmark Report can be found [here](queries.py).


Benchmark report response time will be recorded in `query_stats.csv` file.


