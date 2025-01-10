from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'start_date': datetime(2025,1,9),
    'retries': 1,
}

with DAG(
    "ETL_simple",
    default_args=default_args,
    catchup=False,
    description="This is a dummy ETL for execution Method visualize..!",
    tags=["ETL"]
)as dag:
    start = DummyOperator(task_id = "START")
    e = DummyOperator(task_id = "EXTRACTION")
    t = DummyOperator(task_id = "TRANSFORMATION")
    l = DummyOperator(task_id = "LOAD")
    end = DummyOperator(task_id = "END")
    
start >> e >> t >> l >> end