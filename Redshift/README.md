# Redshift Query Performance Monitoring Script

## Overview

This repository contains a Python script to execute SQL queries linearly and an environment file for credentials configuration.

| No. | File Name   | Description          |
|-----|------------|---------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Contains your credentials |

## Getting Started

To run the Redshift benchmark, configure your credentials by setting the following environment variables:

```bash
# Redshift credentials
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_HOST=your_host  # e.g., redshift-cluster-1.abc123xyz.us-west-2.redshift.amazonaws.com
REDSHIFT_DATABASE=your_database
REDSHIFT_PORT=5439  # Default port for Redshift
QUERY_TAG=redshift_benchmark
```

## Setup

Install the required dependencies in the Redshift folder:

```bash
pip install -r requirements.txt
```

## Running the Code

Inside the Redshift folder, run:

```bash
python main.py
```

## Output

- Queries used for the benchmark report can be found in [`queries.py`](queries.py).
- Benchmark response times will be recorded in the `query_stats.csv` file.


