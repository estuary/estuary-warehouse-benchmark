# **Estuary Benchmark Report**  

Our goal with this benchmark report is to equip you with the technical tools and insights needed to run your own tests and determine which data warehouse best fits your specific needs.<br><br>
We’ve designed this repo so that even semi-technical users can easily run the benchmarks and draw meaningful insights.<br><br>
If you have any questions about running the benchmark, don’t hesitate to get in touch. We’re here to help.<br><br>

This repository contains Python  code to run and measure SQL queries on the following data warehouses:  

- [**Snowflake**](Snowflake/)  
- [**Databricks**](Databricks/)  
- [**Big Query**](BigQuery/)  
- [**Microsoft Fabric**](Azure/)  
- [**Redshift**](Redshift/)  


For instructions on running Python  code for a specific data warehouse, refer to its respective `README` file.

## **Implementation Details:**

- To ensure accurate performance measurement, the code bypasses cached results from previous queries, guaranteeing a fresh execution each time.
- We initially attempted to extract query execution times from system query history logs, where such metadata was programmatically accessible.
- For data warehouses that did not support programmatic access to query metadata, we measured elapsed time using Python logic.

## **Architecutre:**
<img width="779" alt="image" src="https://github.com/user-attachments/assets/b824fcf3-438e-46d7-9b90-721a56a1f156" />


## **Deployed Environment:**

- The Python code was deployed in a Upcloud Ubutu Server hosted in Sweden. 
- The server had 2 Core, 8 GB of memory, and 10GB of storage.
- We pulled the code from Github, activated a virtual Python environment, and installed only the necessary modules to run the code.
- We ran the code using `tmux` logic and did not modify or alter the code during or after runtime.
- We waited 24 hours to retrieve the cost to run queries.

## **Repository Tree:**
```
.
├── Azure/
│   ├── .env
│   ├── benchmark_queries/
│   ├── Azure Python Code Flow.png
│   ├── main.py
│   ├── queries.py
│   ├── readme.md
│   └── requirements.txt
│
│
├── BigQuery/
│   ├── .env
│   ├── Big Query Python Code Flow.png
│   ├── README.md
│   ├── benchmark_queries/
│   ├── main.py
│   ├── queries.py
│   └── requirements.txt
│
│
├── Databricks/
│   ├── .env
│   ├── Databricks Python Code Flow.png
│   ├── README.md
│   ├── benchmark_queries/
│   ├── main.py
│   ├── queries.py
│   └── requirements.txt
│
│
├── Redshift/
│   ├── .env
│   ├── README.md
│   ├── Redshift Python Code Flow.png
│   ├── benchmark_queries/
│   ├── main.py
│   ├── queries.py
│   └── requirements.txt
│
│
├── Snowflake/
│   ├── Python Code Flow.png
│   ├── .env
│   ├── README.md
│   ├── benchmark_queries/
│   ├── main.py
│   ├── queries.py
│   └── requirements.txt
└── README.md
```

# **About Estuary**  

* Estuary is one of the fastest and the most reliable realtime streaming and ETL tool in the market
* Estuary has 200+ built-in connectors to cater to almost all popular needs
* Estuary moves 1 petabyte per month while maintaining 99.9% uptime and less than 100 milliseconds of latency

## We have a best in class enterprise grade architecture 


<img width="572" alt="image" src="https://github.com/user-attachments/assets/be04d71c-0cc5-4973-993d-6795bdc44646" />

## Interested in trying out Estuary?

[Get a 30-day credit card free trial here](https://estuary.dev/) 

