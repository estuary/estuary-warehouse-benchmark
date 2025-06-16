# BigQuery Query Performance Monitoring Executing Script

## Overview

This repo has one Python script to execute the SQL queries linearly and an environment file to plug in credentials.

| Sno | File Name   | Comment          |
|-----|------------|------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Takes in your credentials |



## Getting Started

To run the BigQuery benchmark, plug in your own credentials by setting the following environment variables:

```bash
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-file.json
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_DATASET=your_dataset
BIGQUERY_LOCATION=your_location
QUERY_TAG=benchmark_testing 
```

## Setup

Install the required dependencies mentioned inside the BigQuery folder.

```bash
pip install -r requirements.txt

```
## Running the code 

Inside the BigQuery folder run

```bash
python main.py
```




Queries used for the Benchmark Report can be found [here](queries.py).
<br>

Benchmark report response time will be recorded in `query_stats.csv` file.


