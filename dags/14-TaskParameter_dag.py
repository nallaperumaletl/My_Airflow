from datetime import datetime 
from datetime import timedelta
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
#NOTE: get_current_context Module is Important to acheive this param retrival.

def get_config_param(**kwargs):
    context = get_current_context()
    print(f"AIRFLOW DEFAULT CONTEXT VALUES : {context}")   
    #NOTE By default Many context parameteres are available in airflow:
    
    logical_date = kwargs["logical_date"]
    dag_run_context = kwargs["dag_run"]
    
    print(f"AIRFLOW DEFAULT CONTEXT VALUES 'DAG_RUN': {dag_run_context}") 
    
    #NOTE 'logical_date': DateTime(2025, 1, 12, 14, 20, 0, tzinfo=Timezone('UTC'))      # Outcome format default
    '''(airflow_venv)nalla@nallaperumal-aspire:~/airflow$ python3 -c "from datetime import datetime; logical_date = datetime(2025, 1, 12, 14, 20, 0); print(logical_date.date())"
        2025-01-12'''
    # {"custom_parameter_nalla":"LEARNING_APACHE_AIRFLOW_ORCHESTRATION_IN-DETAIL"} 
    custom_param = kwargs['dag_run'].conf.get('custom_parameter_nalla')
    
    todays_date = datetime.now().date()
    
    '''(airflow_venv)nalla@nallaperumal-aspire:~/airflow$ python3 -c "from datetime import datetime; print(datetime.now().date())"
        2025-01-12'''
    
    if logical_date.date() == todays_date:
        print('Normal Execution')
        print(f'custom paramter value is : {custom_param}')
    else:
        print('Backed-dated Execution')
        if custom_param is not None:
            print(f'custom paramter value is : {custom_param}')
    time.sleep(20) ## sleep for 20 seconds
    
    
default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'retries': 0,
}

with DAG( 'Ex_custom_parameter',  default_args=default_args, 
         description='custom paramter example', 
         #schedule_interval=timedelta(minutes=5), 
         start_date=datetime(2024, 1, 12), catchup=False, tags=["ETL", "DE"] ) as dag:
    
    start = EmptyOperator(task_id="START")
    config_params = PythonOperator(
        task_id = "Dag_config_params",
        python_callable= get_config_param
        )
    end = EmptyOperator(task_id="END")
    

start  >> config_params >> end