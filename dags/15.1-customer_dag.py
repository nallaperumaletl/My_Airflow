from datetime import datetime 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


def fn_customer_dag():
    print("Logic for Customer Dag..!")
    
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'retries': 0,
}

with DAG( 'customer_dag',  default_args=default_args, 
         description='Dag dependency example', 
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 12), catchup=False, tags=["ETL", "DE"] ) as dag:
    
    start = EmptyOperator(task_id="START")
    customer_task = PythonOperator(
        task_id = "cust_dag",
        python_callable= fn_customer_dag
        )
    bash = BashOperator(task_id = 'bash_command_sleep',
                        bash_command='sleep 2')
    end = EmptyOperator(task_id="END")
    
start >> customer_task >>  bash >> end