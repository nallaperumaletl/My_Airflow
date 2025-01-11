from datetime import datetime 
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

#task group related Modules:
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Nalla Perumal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'Nalla_Taskgroup_orchestration',  # DAG name
    default_args=default_args,
    description='Example: Demonstration of subdag & taskgroup',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 11),
    catchup=False,
    tags=["ETL", "DE"],
    max_active_runs=1
) as dag:
    
    start = EmptyOperator(task_id="START")
    with TaskGroup("A-D", tooltip="Task group from A-D") as grp_A_to_D:
        a = EmptyOperator(task_id="Task_A")
        a1 = EmptyOperator(task_id="Task_A1")
        b = EmptyOperator(task_id="Task_B")
        b1 = EmptyOperator(task_id="Task_B1")
        c = EmptyOperator(task_id="Task_C")
        d = EmptyOperator(task_id="Task_D")
        
        a >> a1 >> c 
        a >> b >> b1
        [c, b1] >> d 
    
    
    
#start >> grp_A_to_D >> end
    with TaskGroup("E-F-G-H", tooltip="Task group from E to H") as group_E_H:
        e = EmptyOperator(task_id="Task_E")
        f = EmptyOperator(task_id="Task_F")
        g = EmptyOperator(task_id="Task_G")
        h = EmptyOperator(task_id="Task_H")
        e >> g 
        e >> f
        [g, f] >> h
        
        with TaskGroup("I-K", tooltip="Taskgroup from I to K") as sub_grp_2:
            i = EmptyOperator(task_id="Task_I")
            j = EmptyOperator(task_id="Task_J")
            k = EmptyOperator(task_id="Task_K")
            f >> i
            f >> j
            [i, j] >> k
           
    end = EmptyOperator(task_id="END")

    
start >> grp_A_to_D >> group_E_H >> end