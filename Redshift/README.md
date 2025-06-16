# Redshift Query Performance Monitoring Executing Script

## Overview

This repo has one Python script to execute the SQL queries linearly and an environment file to plug in credentials.

| Sno | File Name   | Comment          |
|-----|------------|------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Takes in your credentials |



## Getting Started

To run the Redshift benchmark, plug in your own credentials by setting the following environment variables:

```bash
# Redshift Credentials
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_HOST=your_host # e.g., redshift-cluster-1.abc123xyz.us-west-2.redshift.amazonaws.com
REDSHIFT_DATABASE=your_database
REDSHIFT_PORT=5439 # Default port for Redshift
QUERY_TAG=redshift_benchmark 

```

## Setup

Install the required dependencies mentioned inside the Redshift folder.

```bash
pip install -r requirements.txt

```
## Running the code 

Inside the Redshift folder run

```bash
python main.py
```


Queries used for the Benchmark Report can be found [here](queries.py).


Benchmark report response time will be recorded in `query_stats.csv` file.


