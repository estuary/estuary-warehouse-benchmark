"""
BigQuery Query Performance Monitoring Script

This script executes SQL queries on BigQuery and measures their performance metrics,
including response time, bytes scanned, and rows produced. Results are saved to a CSV file.
"""

from google.cloud import bigquery
import time
from datetime import datetime
import csv
import os
import json
from dotenv import load_dotenv
from queries import queries

# Load environment variables
load_dotenv()


def run_query_and_save_metrics(client, query_description, query, project_id, dataset, query_tag):
    """
    Execute a query and save performance metrics to CSV.
    
    Args:
        client: BigQuery client instance
        query_description: Human-readable description of the query
        query: SQL query string to execute
        project_id: BigQuery project ID
        dataset: BigQuery dataset name
        query_tag: Tag for categorizing results
    """
    try:
        print(f"\nRunning query: {query_description}\n")

        # Configure the job with query configuration
        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,  # Don't use cached results
            labels={"query_tag": query_tag.replace("-", "_").lower()}  # BigQuery labels can't contain dashes
        )
        
        # Record query start time in milliseconds
        start_time = time.time() * 1000
        
        # Execute the query
        query_job = client.query(query, job_config=job_config)

        # Record query end time in milliseconds
        end_time = time.time() * 1000

        # Calculate response time
        response_time = end_time - start_time

        job_id = query_job.job_id

        # Get query performance metrics
        job = client.get_job(job_id)
        
        # Extract relevant metrics
        bytes_scanned = job.total_bytes_processed
        mb_scanned = bytes_scanned / 1024 / 1024 if bytes_scanned else 0
        rows_produced = job.total_rows if hasattr(job, 'total_rows') else 0
        job_duration = (job.ended.timestamp() * 1000 - job.started.timestamp() * 1000) if job.ended and job.started else None

        # Prepare data to append
        query_metrics = {
            'query_description': query_description,
            'response_time_ms': response_time,
            'bigquery_official_time_in_milli_sec': job_duration,
            'mb_scanned': round(mb_scanned, 4),
            'rows_produced': rows_produced,
            'project_id': project_id,
            'job_id': job_id,
            'run_type': 'Linear',
            'query_tag': query_tag
        }

        # Define CSV file path
        output_file = 'query_stats.csv'

        # Check if the file exists
        file_exists = os.path.isfile(output_file)

        # Open the CSV file in append mode and write the data
        with open(output_file, mode='a', newline='') as file:
            fieldnames = [
                'query_description', 'response_time_ms', 'bigquery_official_time_in_milli_sec', 
                'mb_scanned', 'rows_produced', 'project_id', 'job_id', 'run_type', 'query_tag'
            ]
            
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # If the file doesn't exist, write the header row
            if not file_exists:
                writer.writeheader()

            # Write the query metrics to the CSV
            writer.writerow(query_metrics)

        print(f"Metrics saved to {output_file}")

    except Exception as e:
        print(f"Unexpected error in 'run_query_and_save_metrics': {e}")


def main():
    """
    Main function to execute all benchmark queries.
    
    Retrieves environment variables, establishes BigQuery connection,
    and executes all queries in the queries list.
    """
    try:
        # Get environment variables
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        dataset = os.getenv("BIGQUERY_DATASET")
        query_tag = os.getenv("QUERY_TAG")
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Validate required environment variables
        if not all([project_id, dataset, credentials_path]):
            raise ValueError("Missing required environment variables. Please check BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, and GOOGLE_APPLICATION_CREDENTIALS.")

        # Create BigQuery client with service account
        client = bigquery.Client.from_service_account_json(
            credentials_path,
            project=project_id
        )
        
        print(f"Connected to BigQuery project: {project_id}")
        print(f"Using dataset: {dataset}")
        print(f"Query tag: {query_tag}")
        
        try:
            # Iterate through the queries and execute them
            for query_description, query in queries:
                run_query_and_save_metrics(client, query_description, query, project_id, dataset, query_tag)
        
        except Exception as e:
            print(f"Error during query execution loop: {e}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main() 
