# from sqlalchemy import create_engine

# def get_db_conn():
#     engine = create_engine("postgres+pyscopg2://{pg_user}:{pg_passwd}@{ip}:{pg_port}/{pg_db}".format(
#         pg_user=DB_USER, pg_passwd=DB_PWD, ip=HOST_IP, pg_port=DB_PORT, pg_db=DB_NAME),
#         connect_args = {'options': '-csearch_path={}'.format(DB_SCHEMA)}
                           
#     )
#     conn = engine.connect()
#     return conn

# Airflow concepts:
############# CONNECTION & HOOKS #############

from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_pg_hook_conn():
    conn = PostgresHook(postgres_conn_id='postgres_postgres_public')
    return conn