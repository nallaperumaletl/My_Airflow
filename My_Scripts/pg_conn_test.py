from airflow.providers.postgres.hooks.postgres import PostgresHook

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

# Run the connection test
test_pg_connection()
