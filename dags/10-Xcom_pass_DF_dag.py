# Imports
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import os
import sys

# for sys module:
#NOTE: we should add this sys path in our business_logic script file also..
add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)

from init import *  
#NOTE: we should call this init modular in our business_logic script file also..
from etl_business_logic_v3_DF import *

#for os module:
# root_folder_path = "/home/nalla/airflow/My_Scripts"
# os.chdir(root_folder_path)

# exec(open('./init.py').read())
# exec(open('./etl_business_logic_v1.py').read())



default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,    
    'start_date': datetime(2025,1,10),
    'retries': 0,
}

with DAG(
    "ETL_wrappering_dataframe_single_taskinstance_v1",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,  # Limit to 1 active run
    description="A dummy ETL DAG demonstrating the use of a DataFrame wrapper function, showcasing how to execute it within a single callable function for better modularity and clarity.",
    tags=["ETL", "DE"]
)as dag:
    
    start = EmptyOperator(task_id='START')

# Here instead of seperate operator; pull all the function in a single operator
# by calling the wrapper function:
    
    e_t_l = PythonOperator(
        task_id = "EXTRACT_TRANSFORM_LOAD_DF_wrapper",
        python_callable= etl,
        op_args=["Learning Data Engineering with Airflow","DE","NALLA PERUMAL"],
        do_xcom_push=True
    )
    
    end = EmptyOperator(task_id='END')
    
start >> e_t_l >> end
