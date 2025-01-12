from datetime import datetime 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def fn_account_dag():
    print("Logic for Accountmember Dag..!")
    
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'retries': 0,
}

with DAG( 'accountmember_dag',  default_args=default_args, 
         description='Dag dependency example', 
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 12), catchup=False, tags=["ETL", "DE"] ) as dag:
    
    start = EmptyOperator(task_id="START")
    accountmember_task = PythonOperator(
        task_id = "acc_dag",
        python_callable= fn_account_dag
        )
    end = EmptyOperator(task_id="END")
    
start >> accountmember_task >> end