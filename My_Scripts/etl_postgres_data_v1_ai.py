import pandas as pd
import sys
from init import * 
from postgres_conn_v1 import *

add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)
from init import *
from postgres_conn_v1 import *

# Define the wrapper function
def etl():
    conn_obj = get_pg_hook_conn()

    e_df = extract_fn(conn_obj)
    
    transformed_df = transform_fn(e_df)
    
    # Load the transformed data into a CSV file
    load_fn(transformed_df)
    return None

# Extract function to retrieve data from the PostgreSQL database
def extract_fn(conn_obj):
    select_qry = "SELECT * FROM employees"
    # Get the data as a pandas DataFrame using the PostgresHook
    df = conn_obj.get_pandas_df(select_qry)
    print("Extracted data:")
    print(df)
    return df

# Transform function to modify the data
def transform_fn(df):
    # Add a transformation: Create a new column that checks if salary is above a threshold
    df["High_Salary"] = df["salary"].apply(lambda x: "Yes" if x > 10000 else "No")
    
    # Filter for high salary employees
    high_salary_df = df[df["High_Salary"] == "Yes"]
    
    print("Transformed data:")
    print(high_salary_df)
    
    return high_salary_df  # Return only high salary employees

# Load function to save the high salary employees data into a CSV file
def load_fn(df):
    # Save the filtered high salary employees DataFrame to a CSV file
    df.to_csv(f'{log_dir}/high_salary_employees.csv', index=False)
    
    print("High salary data saved to CSV.")
    return 1
