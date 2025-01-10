# Imports
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import pandas as pd

# All constansts be declared as Global Variables.
TITLE = "DATA ENGINEER"

def extract_fn():
    print("Logic to extract data!")
    print("Value of Global variable is: ", TITLE)
    rtn_val = "Hello Nalla!"
    return rtn_val

# Creating Dataframe
# details = {
#   'cust_id' : [1, 2, 3, 4],
#   'Name' : ['Nalla', 'Raju', 'Anu', 'Mega']
#    }
# df = pd.DataFrame(details)
# return df


# XCOM : Cross-Communication
# ti stands for Task Instance (Each execution of DAG creates its own task instance)
    # By using the ti, Xcom pull function from which fn you are trying to get the value - i.e:  Here its from extract_fn()
def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =['EXTRACT'])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_obj = xcom_pull_obj[0]
    print(f"The value of xcom pull object is {extract_rtn_obj}")
    
    print("The value of a1 is :", a1)
    print("Logic to Transform Data")
    return 10

def load_fn(p1, p2, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =['EXTRACT'])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    print(f"The value of xcom pull object is {xcom_pull_obj[0]}")
    
    print("The value of p1 is :", p1)
    print("The value of p2 is :", p2)
    print("Logic to Load Data!")
    
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'start_date': datetime(2025,1,10),
    'retries': 1,
}

with DAG(
    "xcom_push_pull_01",
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
