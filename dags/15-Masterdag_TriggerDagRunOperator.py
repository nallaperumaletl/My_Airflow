from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default arguments for the tasks
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'retries': 0,
}

# def trigger_dag(context, dag_run_obj):
#     """ Custom function to trigger another DAG """
#     dag_run_obj.payload = {'message': 'Triggered by Master DAG'}
#     return dag_run_obj

# Define the master DAG
with DAG(
    'master_dag',
    default_args=default_args,
    description='Master DAG to trigger Customer, AccountMember, and Transaction DAGs',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 12),
    catchup=False,  # To prevent backfilling if you don't want it
    tags=["ETL", "Master"]
) as master_dag:

    start = EmptyOperator(task_id="START")
    
    # Trigger Customer DAG
    trigger_customer = TriggerDagRunOperator(
        task_id="CUSTOMER_MASTER_DAG",
        trigger_dag_id="customer_dag",  # The DAG ID to trigger
        execution_date= '{{ ds }}',
        reset_dag_run = True,     
        wait_for_completion = True,
        
    )

    # Trigger AccountMember DAG
    trigger_accountmember = TriggerDagRunOperator(
        task_id="ACCOUNT_MASTER_DAG",
        trigger_dag_id="accountmember_dag",
        execution_date= '{{ ds }}',
        reset_dag_run = True,     
        wait_for_completion = True,
        
    )

    # Trigger Transaction DAG
    trigger_transaction = TriggerDagRunOperator(
        task_id="TRANSACTION_DAG",
        trigger_dag_id="transaction_dag",
        execution_date= '{{ ds }}',
        reset_dag_run = True,     
        wait_for_completion = True,
        poke_interval = 3
    )

    end = EmptyOperator(task_id="END")

# Task Dependencies
start >> [trigger_customer, trigger_accountmember] >> trigger_transaction >> end

