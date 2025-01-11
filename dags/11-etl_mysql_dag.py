from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

import sys
add_path_to_sys = '/home/nalla/airflow/My_Scripts'
sys.path.append(add_path_to_sys)
from etl_mysql_v1 import *

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'MySQL_ETL_dag',  # DAG name
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 11),
    catchup=False,
    tags=["ETL", "DE"],
    max_active_runs=1
) as dag:


    start = EmptyOperator(task_id="START")

    Run_E_T_L_MySQL = BashOperator(
    task_id='ETL_MySQL',
    bash_command='bash /home/nalla/airflow/My_Scripts/etl_mysql_wrapper.sh ',#give a space after the path
    dag=dag,
    )

    end = EmptyOperator(task_id="END")


start >> Run_E_T_L_MySQL >> end