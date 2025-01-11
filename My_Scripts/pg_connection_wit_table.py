import pandas as pd
from datetime import datetime as dtime
import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Add custom path to sys
add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)
from init import *

# Test PostgreSQL connection
def test_pg_connection():
    # Define the Postgres connection ID
    conn_id = 'postgres_postgres_public'
    
    try:
        # Create a PostgresHook instance using the connection ID
        hook = PostgresHook(postgres_conn_id=conn_id)
        
        # Attempt to get the connection
        conn = hook.get_conn()
        
        # If connection is successful, print a success message
        if conn:
            print(f"Successfully connected to PostgreSQL using connection ID '{conn_id}'")
        else:
            print(f"Failed to connect to PostgreSQL using connection ID '{conn_id}'")
        
    except Exception as e:
        # If there is any error, print the error message
        print(f"Error while testing PostgreSQL connection: {e}")

# Extract function to retrieve data from PostgreSQL database
def extract_fn(conn_obj):
    select_qry = "SELECT * FROM employees"
    # Get the data as a pandas DataFrame using the PostgresHook
    df = conn_obj.get_pandas_df(select_qry)
    print("Extracted data:")
    print(df)
    return df

# Transform function to modify the data
def transform_fn(df):
    # Add a new column that checks if salary is above a threshold
    df["High_Salary"] = df["salary"].apply(lambda x: "Yes" if x > 5000 else "No")
    
    # Filter to get only high-salary employees
    high_salary_df = df[df["High_Salary"] == "Yes"]
    
    print("Transformed data:")
    print(high_salary_df)
    
    return high_salary_df  # Return the filtered DataFrame

# Load function to save only high salary people to a CSV file
def load_fn(df):
    # Save the filtered DataFrame (only high-salary people) to a CSV file
    df.to_csv(f'{log_dir}/high_salary_employees.csv', index=False)
    
    print("High salary data saved to CSV.")
    return 1

# ETL Wrapper function
def etl():
    conn_obj = get_pg_hook_conn()

    # Test the connection before proceeding
    test_pg_connection()

    # Extract data
    e_df = extract_fn(conn_obj)
    
    # Transform data
    transformed_df = transform_fn(e_df)
    
    # Load the data into the database and save to CSV
    load_fn(transformed_df)
    return None

# Wrapper to get the PostgresHook connection object
def get_pg_hook_conn():
    conn = PostgresHook(postgres_conn_id='postgres_postgres_public')
    return conn

# Running the ETL process
if __name__ == "__main__":
    etl()
