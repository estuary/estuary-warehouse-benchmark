import pyodbc
import os
import time
import csv
from dotenv import load_dotenv
from queries import queries  

def run_query_and_save_metrics(conn, description, query, database_name, csv_file_name, query_tag):
    print(f"Running query: {description}")
    start = time.time()
    duration_milliseconds = -1
    file_exists = os.path.isfile(csv_file_name)  

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        # Calculate duration in seconds, then convert to milliseconds
        duration_seconds = time.time() - start
        duration_milliseconds = round(duration_seconds * 1000, 2)

        print(f"{description}: completed in {duration_milliseconds}ms")
        print(f"First row: {result}")

        # Write results to CSV
        with open(csv_file_name, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if not file_exists:
                csv_writer.writerow(["Query Description", "Response Time (ms)", "Database Name", "Query Tag"])
                print(f"Created '{csv_file_name}' and wrote headers.")
            csv_writer.writerow([description, duration_milliseconds, database_name, query_tag])
            print(f"Metrics recorded for '{description}'")

    except Exception as e:
        print(f"Error in '{description}': {e}")
        with open(csv_file_name, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if not file_exists:
                csv_writer.writerow(["Query Description", "Response Time (ms)", "Database Name", "Query Tag", "Error"])
                file_exists = True  # Ensure headers are marked as written
            csv_writer.writerow([description, duration_milliseconds, database_name, query_tag, str(e)])

def main():
    """Main entrypoint for connecting and running queries."""
    load_dotenv()  

    
    driver = os.getenv('driver')
    server = os.getenv('server')
    database = os.getenv('database')
    username = os.getenv('username')
    password = os.getenv('password')
    query_tag = os.getenv('query_tag')

    
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )

    conn = None
    csv_file_name = "query_stats.csv"

    try:
        conn = pyodbc.connect(conn_str)
        print("Successfully connected to the database.")

        
        for description, query in queries:
            try:
                run_query_and_save_metrics(conn, description, query, database, csv_file_name, query_tag)
            except Exception as inner_e:
                print(f"Unexpected error during query '{description}': {inner_e}")
            time.sleep(3)  

        print("All queries completed")

    except pyodbc.Error as db_e:
        print(f"Database connection error: {db_e}")
    except Exception as e:
        print(f"An unexpected error occurred in main: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed")

if __name__ == "__main__":
    main()
