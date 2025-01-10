# Imports
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import os
import sys

#for os module:
root_folder_path = "/home/nalla/airflow/My_Scripts"
os.chdir(root_folder_path)

exec(open('.init.py').read())
exec(open('etl_business_logic_v1.py').read())

# for sys module:
# add_path_to_sys = "/home/nalla/airflow/My_Scripts"
# sys.path.append(add_path_to_sys)

# from init import *
# from etl_business_logic_v1 import *

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,    
    'start_date': datetime(2025,1,10),
    'retries': 0,
}

with DAG(
    "modular_code_approach_v1",
    default_args=default_args,
    catchup=False,
    description="This is a dummy ETL for Xcom understanding concept!",
    tags=["ETL", "DE"]
)as dag:
    
    start = EmptyOperator(task_id='START')

#do_xcom_push=True     ## By default This parameter is set to True   

    e = PythonOperator(
        task_id = 'EXTRACT',
        python_callable= extract_fn,
        do_xcom_push=True     ## By default This parameter is set to True
        )
    
    t = PythonOperator(
        task_id = 'TRANSFORM',
        python_callable= transform_fn,
        op_args= ["Learning Airflow from the scratch, as part of DE! Its Great!"],
        do_xcom_push = True
    )
    
    l = PythonOperator(
        task_id = 'LOAD',
        python_callable= load_fn,
        op_kwargs = {"p1":"NALLA PERUMAL", "p2": "DATA ENGINEER"}
    )
    
    end = EmptyOperator(task_id='END')
    
start >> e >> t >> l >> end
