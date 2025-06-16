# Fabric Query Performance Monitoring Executing Script

## Overview

This repo has one Python script to execute the SQL queries linearly and an environment file to plug in credentials.

| Sno | File Name   | Comment          |
|-----|------------|------------------|
| 1   | `main.py`  | Runs all SQL queries linearly |
| 2   | `.env`  | Takes in your credentials |

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

To run the Fabric benchmark, plug in your own credentials by setting the following environment variables:

```bash
server = 'your_server' -> looks like your_account-ondemand.sql.azuresynapse.net
database = 'xsmall'
schema = 'tpch'
username = 'your_username' -> looks like sqladminuser@your_account
password = 'yourpassword'
driver = '{ODBC Driver 17 for SQL Server}'
query_tag = 'Dw100c'

```

## Setup

Install the required dependencies mentioned inside the Fabric folder.

```bash
pip install -r requirements.txt

```
## Running the code 

Inside the Fabric folder run

```bash
python main.py
```



Queries used for the Benchmark Report can be found [here](queries.py).


Benchmark report response time will be recorded in `query_stats.csv` file.


