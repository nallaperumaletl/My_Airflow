# Imports
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import sys
#NOTE: we should add this sys path in our business_logic script file also..

add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)

from init import *  
#NOTE: we should call this init modular in our business_logic script file also..
from etl_postgres_data_v1_ai import *
from postgres_conn_v1 import *

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,    
    'start_date': datetime(2025,1,10),
    'retries': 0,
}

with DAG(
    "postgres_connection_v1",
    default_args=default_args,
    catchup=False,
    description="This is a dummy ETL for Xcom understanding concept of postgres conn test",
    tags=["ETL", "DE"],
    max_active_runs=1
)as dag:
    
    start = EmptyOperator(task_id='START')
    
    e_t_l = PythonOperator(
    task_id = "EXTRACT_TRANSFORM_LOAD",
    python_callable= etl
    )

    end = EmptyOperator(task_id='END')
    
start >> e_t_l >> end