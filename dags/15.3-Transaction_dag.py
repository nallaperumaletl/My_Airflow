from datetime import datetime 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def fn_transaction_dag():
    print("Logic for Transaction Dag..!")
    
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'retries': 0,
}

with DAG( 'transaction_dag',  default_args=default_args, 
         description='Dag dependency example', 
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 12), catchup=False, tags=["ETL", "DE"] ) as dag:
    
    start = EmptyOperator(task_id="START")
    transaction_task = PythonOperator(
        task_id = "trasn_dag",
        python_callable= fn_transaction_dag
        )
    end = EmptyOperator(task_id="END")
    
start >> transaction_task >> end