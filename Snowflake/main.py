import snowflake.connector
import pandas as pd
import time
from datetime import datetime
import csv
import os
from dotenv import load_dotenv
from queries import queries

load_dotenv()


def run_query_and_save_metrics(cur,query_description,query,warehouse,snowflake_database,query_tag):

    try:

        cur.execute(f""" Use WAREHOUSE {warehouse};""")

        print(f"\n\nRunning this query = {query_description}\n")

        # Record query start time
        start_time = time.time()

        # Execute and fetch query
        cur.execute(query)
        result = cur.fetchall()

        # Record query end time
        end_time = time.time()

        # Converting to milliseconds
        start_time = start_time * 1000
        end_time = end_time * 1000
        response_time = end_time - start_time

        query_id = cur.sfqid

        # Get query performance metrics
        cur.execute(f"""
            SELECT 
                QUERY_ID as query_id,
                TOTAL_ELAPSED_TIME as snowflake_official_time_in_milli_sec,  
                BYTES_SCANNED/1024/1024 as mb_scanned,
                ROWS_PRODUCED as rows_produced,
                CREDITS_USED_CLOUD_SERVICES as credits_used
            FROM TABLE({snowflake_database}.INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID = '{query_id}'
        """)
        metrics = cur.fetchone()

        # Prepare data to append
        query_metrics = {
            'query_description': query_description,
            'response_time_ms': response_time,
            'snowflake_official_time_in_milli_sec': metrics[1] if metrics else None,
            'mb_scanned': round(metrics[2], 4) if metrics and metrics[2] else 0,
            'rows_produced': metrics[3] if metrics else None,
            'credits_used': metrics[4] if metrics else None,
            'warehouse':warehouse,
            'query_id':query_id,
            'run_type':'Linear',
            'query_tag':query_tag
        }

        # Define CSV file path
        output_file = 'query_stats.csv'

        # Check if the file exists
        file_exists = os.path.isfile(output_file)

        # Open the CSV file in append mode and write the data
        with open(output_file, mode='a', newline='') as file:
            fieldnames = ['query_description', 'response_time_ms', 'snowflake_official_time_in_milli_sec', 
                        'mb_scanned', 'rows_produced', 'credits_used','warehouse','query_id','run_type','query_tag']
            
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # If the file doesn't exist, write the header row
            if not file_exists:
                writer.writeheader()

            # Write the query metrics to the CSV
            writer.writerow(query_metrics)

        print(f"\nMetrics saved to {output_file}")

    except Exception as e:
        print(f"Unexpected error in 'run_query_and_save_metrics': {e}")

    



def main():

    try:

        # Get env variables
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        snowflake_database=os.getenv("SNOWFLAKE_DATABASE")
        query_tag=os.getenv("QUERY_TAG")

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            session_parameters={
                                'QUERY_TAG': query_tag
                                }
        )
        
        # Establish Connection
        cur = conn.cursor()

        try:
            # This ensures that Snowflake does not use the cached results.
            cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")

            # Iterate through the queries and execute them
            for query_description, query in queries:
                run_query_and_save_metrics(cur,query_description,query,warehouse,snowflake_database,query_tag)
        

        except Exception as e:
                print(f"Error during query execution loop: {e}")
        
        finally:
            # Close cursor
            cur.close()
            print("\nCursor closed.")
        

    except snowflake.connector.errors.DatabaseError as db_err:
        print(f"Database connection error: {db_err}")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    finally:
        if 'conn' in locals() and conn:
            # Close connectiona
            conn.close()
            print("Connection closed.")


if __name__ == "__main__":
    main()