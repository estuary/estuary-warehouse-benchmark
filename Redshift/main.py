import pyodbc
import pandas as pd
import time
from datetime import datetime
import csv
import os
from dotenv import load_dotenv
from queries import queries

load_dotenv()

def run_query_and_save_metrics(cur, query_description, query, query_tag):
    try:
        print(f"\n\nRunning this query = {query_description}\n")
        
        # Clear cache before execution
        try:
            print("Clearing cache...")
            cur.execute("SET enable_result_cache_for_session TO OFF")
            print("Cache cleared successfully")
        except Exception as e:
            print(f"Error clearing cache: {e}")
        
        # Record query start time
        start_time = time.time()

        # Execute and fetch query
        cur.execute(query)
        result = cur.fetchall()

        # Record query end time
        end_time = time.time()

        # Converting to milliseconds
        response_time =round((end_time - start_time) * 1000, 2)
    

        # Add a small delay to ensure system table is updated
        time.sleep(1)

        # Get the query ID using PG_LAST_QUERY_ID()
        try:
            cur.execute("SELECT PG_LAST_QUERY_ID()")
            query_id_result = cur.fetchone()
            query_id = str(query_id_result[0]) if query_id_result else "unknown"
            
            print(f"Last executed query ID: {query_id}")
            
            # Attempt to retrieve execution time with a delay to allow system tables to update
            redshift_official_time = None
            max_attempts = 3
            
            for attempt in range(max_attempts):
                try:
                    print(f"Attempt {attempt+1} to retrieve execution time...")
                    
                    # Try the SYS_QUERY_HISTORY view
                    cur.execute(f"""
                        SELECT 
                            query_id,
                            execution_time,
                            elapsed_time,
                            queue_time,
                            planning_time
                        FROM 
                            SYS_QUERY_HISTORY
                        WHERE 
                            query_id = '{query_id}'
                    """)
                    
                    time_result = cur.fetchone()
                    if time_result and time_result[1] is not None:
                        # Convert seconds to milliseconds
                        execution_time_microsec = float(time_result[1]) 
                        elapsed_time_microsec = float(time_result[2]) if time_result[2] is not None else 0
                        
                        # Use elapsed_time if available, otherwise use execution_time
                        redshift_official_time =elapsed_time_microsec /1000   # Convert to milliseconds
                        
                        print(f"Query ID: {time_result[0]}")
                        print(f"Execution time: {execution_time_microsec/1000 } ms")
                        print(f"Elapsed time: {elapsed_time_microsec/1000 } ms")
                        print(f"Queue time: {float(time_result[3]) / 1000 if time_result[3] is not None else 0} ms")
                        print(f"Planning time: {float(time_result[4]) / 1000 if time_result[4] is not None else 0} ms")
                        
                        if redshift_official_time > 0:
                            break
                        else:
                            print("Retrieved 0.0ms execution time, trying again...")
                            time.sleep(1)  # Wait a bit longer for system tables to update
                    else:
                        print(f"No execution data found for query ID {query_id} in SYS_QUERY_HISTORY")
                        time.sleep(1)  # Wait before retrying
                
                except Exception as e:
                    print(f"Could not retrieve execution time from SYS_QUERY_HISTORY: {e}")
                    time.sleep(1)  # Wait before retrying
                
        except Exception as e:
            print(f"Could not retrieve query stats: {e}")
            query_id = "unknown"
            redshift_official_time = None

        # Prepare data to append
        query_metrics = {
            'query_description': query_description,
            'response_time_ms': response_time ,
            'redshift_official_time_in_milli_sec': redshift_official_time ,
            'rows_produced': len(result) if result else 0,
            'run_type': 'Linear',
            'query_tag': query_tag,
            'database': os.getenv("REDSHIFT_DATABASE"),
            'query_id': query_id
        }

        # Define CSV file path
        output_file = 'Redshift/query_stats.csv'

        # Check if the file exists
        file_exists = os.path.isfile(output_file)

        # Open the CSV file in append mode and write the data
        with open(output_file, mode='a', newline='') as file:
            fieldnames = ['query_description', 'response_time_ms', 'redshift_official_time_in_milli_sec', 
                        'rows_produced', 'run_type', 'query_tag', 'database', 'query_id']
            
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
        query_tag = os.getenv("QUERY_TAG")

        # Build connection string
        conn_str = f'''Driver={{Amazon Redshift (x64)}}; 
        Server={os.getenv("REDSHIFT_HOST")}; 
        Database={os.getenv("REDSHIFT_DATABASE")};
        UID={os.getenv("REDSHIFT_USER")};
        PWD={os.getenv("REDSHIFT_PASSWORD")};
        PORT={os.getenv("REDSHIFT_PORT")};
        '''

        # Connect to Redshift
        conn = pyodbc.connect(conn_str)
        
        # Establish Connection
        cur = conn.cursor()

        try:
            # Iterate through the queries and execute them
            for query_description, query in queries:
                run_query_and_save_metrics(cur, query_description, query, query_tag)
        
        except Exception as e:
            print(f"Error during query execution loop: {e}")
        
        finally:
            # Close cursor
            cur.close()
            print("\nCursor closed.")
        
    except pyodbc.Error as db_err:
        print(f"Database connection error: {db_err}")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    finally:
        if 'conn' in locals() and conn:
            # Close connection
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()