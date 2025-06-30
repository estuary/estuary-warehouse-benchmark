# BigQuery Query Performance Monitoring Script

## Overview

This repository contains a Python script to execute SQL queries linearly and an environment file for credentials configuration.

| No. | File Name   | Description          |
|-----|------------|---------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Contains your credentials |

## Getting Started

To run the BigQuery benchmark, configure your credentials by setting the following environment variables:

```bash
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-file.json
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_DATASET=your_dataset
BIGQUERY_LOCATION=your_location
QUERY_TAG=benchmark_testing
```

## Setup

Install the required dependencies in the BigQuery folder:

```bash
pip install -r requirements.txt
```

## Running the Code

Inside the BigQuery folder, run:

```bash
python main.py
```

## Output

- Queries used for the benchmark report can be found in [`queries.py`](queries.py).
- Benchmark response times will be recorded in the `query_stats.csv` file.


