# import psycopg2
# import pandas as pd

# # Example pandas DataFrame
# data = {
#     "id": [1, 2, 3],
#     "name": ["Alice", "Bob", "Charlie"],
#     "age": [25, 30, 35]
# }
# df = pd.DataFrame(data)

# # PostgreSQL connection details
# connection = psycopg2.connect(
#     dbname="bigdata",
#     user="postgres",
#     password="postgres",
#     host="localhost",
#     port="5432",
# )
# table_name = "sample_df"
# # Insert DataFrame into PostgreSQL
# try:
#     with connection.cursor() as cursor:
#         # Define the SQL INSERT statement
#         insert_query = f"""
#         INSERT INTO {table_name} (id, name, age)
#         VALUES (%s, %s, %s)
#         """
#         # Convert DataFrame rows to a list of tuples
#         rows = df.itertuples(index=False, name=None)
        
#         # Execute the query for all rows
#         cursor.executemany(insert_query, rows)
        
#         # Commit the transaction
#         connection.commit()
#         print("Data inserted successfully!")
# except Exception as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()

###/********************************************************/
## DYNAMIC COLUMN ALLOCATION
###*********************************************************/

import psycopg2
import pandas as pd

# PostgreSQL connection function
def get_postgres_connection(db_params):
    return psycopg2.connect(**db_params)

def extract_data():

    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 20, 40]
    }
    extract_df = pd.DataFrame(data)
    return extract_df

def transform_data(dataframe):

    transform =  dataframe[dataframe['age'] >= 30]
    return transform

# Load function: Insert the transformed data into PostgreSQL
def load_data_to_postgres(dataframe, table_name, db_params):
    """
    Load data into PostgreSQL from a pandas DataFrame.
    """
    try:
        # Establish the PostgreSQL connection
        connection = get_postgres_connection(db_params)
        
        with connection.cursor() as cursor:
            # Dynamically create the INSERT query
            columns = ", ".join(dataframe.columns)  # Get column names dynamically
            placeholders = ", ".join(["%s"] * len(dataframe.columns))  # Create placeholders for query values
            insert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            """
            
            # Convert DataFrame rows to a list of tuples
            rows = dataframe.itertuples(index=False, name=None)
            
            # Execute the query for all rows
            cursor.executemany(insert_query, rows)
            
            # Commit the transaction
            connection.commit()
            print(f"Data inserted successfully into table '{table_name}'!")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if 'connection' in locals():
            connection.close()

# ETL pipeline function
def run_etl_pipeline():
    db_params = {
        "dbname": "bigdata",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"
    }

    # Extract data
    df = extract_data()
    print("Data Extracted:")
    print(df)

    # Transform data (filter rows where age >= 30)
    transformed_df = transform_data(df)
    print("Data Transformed:")
    print(transformed_df)

    # Load data into PostgreSQL
    load_data_to_postgres(transformed_df, table_name="sample_df", db_params=db_params)

# Run the ETL pipeline
if __name__ == "__main__":
    run_etl_pipeline()


