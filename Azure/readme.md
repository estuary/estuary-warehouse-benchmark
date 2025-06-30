# Fabric Query Performance Monitoring Script

## Overview

This repository contains a Python script to execute SQL queries linearly and an environment file for credentials configuration.

| No. | File Name   | Description          |
|-----|------------|---------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Contains your credentials |

## Getting Started

To use this Azure Python script, you need a Microsoft driver. On Ubuntu, follow these steps to deploy the driver:

1. Import the public repository GPG keys:
    ```bash
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    ```

2. Register the Microsoft Ubuntu repository:
    ```bash
    sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
    ```

3. Update the package list:
    ```bash
    sudo apt-get update
    ```

4. Install the Microsoft ODBC driver:
    ```bash
    sudo apt-get install -y msodbcsql18
    ```

Refer to the [official Microsoft documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) for more details.

To run the Fabric benchmark, configure your credentials by setting the following environment variables:

```bash
server=your_server  # e.g., your_account-ondemand.sql.azuresynapse.net
database=xsmall
schema=tpch
username=your_username  # e.g., sqladminuser@your_account
password=your_password
driver={ODBC Driver 17 for SQL Server}
query_tag=Dw100c
```

## Setup

Install the required dependencies in the Fabric folder:

```bash
pip install -r requirements.txt
```

## Running the Code

Inside the Fabric folder, run:

```bash
python main.py
```

## Output

- Queries used for the benchmark report can be found in [`queries.py`](queries.py).
- Benchmark response times will be recorded in the `query_stats.csv` file.


