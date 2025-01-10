#NOTE:
# from ablove 2.0 Airflow version DummyOperator replaces with EmptyOperator

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def extract_fn():
    print("Logic to extract Data!!")
    
def transform_fn():
    print("The value of a1 is ", a1)
    print("Logic to Transform Data!!")
    
def load_fn():
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data!!")
    
def etl_fn(a1, p1, p2):
    print("Logic to extract Data!!")
    print("The value of a1 is ", a1)
    print("Logic to Transform Data!!")
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data!!")

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'start_date': datetime(2025,1,10),
    'retries': 1,
}

with DAG(
    "Simple_PyOperator_demo",
    default_args=default_args,
    catchup=False,
    description="This is a dummy ETL for execution Method visualize..!",
    tags=["ETL"]
)as dag:
    
    start = EmptyOperator(task_id = "START")
    
    e = PythonOperator(
        task_id = "EXTRACT",
        python_callable=extract_fn
    )
 
# op_args : operation arguments   
    t = PythonOperator(
        task_id = "TRANSFORM",
        python_callable= transform_fn,
        op_args= ["Learning Data Engineering with Airflow"]        
    )
    
    l = PythonOperator(
        task_id = "LOAD",
        python_callable= load_fn,
        op_kwargs = ["NALLA PERUMAL", "DATA ENGINEER"],
        #op_args = {p1="NALLA PERUMAL" , p2="DATA ENGINEER"}
    )
    
    end = EmptyOperator(
        task_id = "END"
    )
    
start >> e >> t >> l >> end