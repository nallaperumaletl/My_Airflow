from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from datetime import datetime
import os
from init import log_dir_mysql  # Assuming this is still required for logging

def get_mysql_connection():
    """
    Initializes and returns a MySqlHook connection object.
    """
    return MySqlHook(mysql_conn_id='MySQL_bigdata_ID')  # Replace with your actual connection ID

def fetch_data_from_mysql():
    """
    Fetches data from the MySQL database using a connection obtained from get_mysql_connection.
    """
    mysql_hook = get_mysql_connection()
    query = 'SELECT * FROM sample_data'
    df = mysql_hook.get_pandas_df(sql=query)
    return df

def transform_data(df):
    """
    Transforms the data by filtering rows where 'age' is greater than 30.
    """
    df_transformed = df[df['age'] > 30]
    return df_transformed

def write_data_to_file(df):
    """
    Writes the transformed DataFrame to a CSV file.
    """
    output_dir = '/home/nalla/airflow/Nalla-test-outfile/mysql_out'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f'etl_output_{timestamp}.csv'
    file_path = os.path.join(output_dir, file_name)
    df.to_csv(file_path, index=False)
    print(f'Data written to {file_path}')

def etl_process():
    """
    Executes the ETL process: fetch, transform, and write data.
    """
    df = fetch_data_from_mysql()
    df_transformed = transform_data(df)
    write_data_to_file(df_transformed)

if __name__ == "__main__":
    etl_process()
