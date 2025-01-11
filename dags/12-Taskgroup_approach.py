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
    'eg_subdag_taskgroup',  # DAG name
    default_args=default_args,
    description='Example: Demonstration of subdag & taskgroup',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 11),
    catchup=False,
    tags=["ETL", "DE"],
    max_active_runs=1
) as dag:
    

### FIXME: UNLOCK ANY ONE METHOD AND FIND THE TASK EXECUTION MANNER IN DAG :)
###/*************************************************************
### NOTE: First Method: LINEAR Method :
###*************************************************************/    

#     start = EmptyOperator(task_id = "START")
#     a = EmptyOperator(task_id = "Task_a")
#     a1 = EmptyOperator(task_id = "Task_a1")
#     b = EmptyOperator(task_id = "Task_b")
#     c = EmptyOperator(task_id = "Task_c")
#     d = EmptyOperator(task_id = "Task_d")
#     e = EmptyOperator(task_id = "Task_e")
#     f = EmptyOperator(task_id = "Task_f")
#     g = EmptyOperator(task_id = "Task_g")
#     end = EmptyOperator(task_id = "END")

# ###NOTE: Normal Linear Manner:   
# start >> a >> a1 >> b >> c >> d >> e >> f >> g >> end
        
#/*************************************************************
# second Method: sub group (sequential):
#*************************************************************/
    start = EmptyOperator(task_id = "START")
    d = EmptyOperator(task_id = "Task_d")
    e = EmptyOperator(task_id = "Task_e")
    f = EmptyOperator(task_id = "Task_f")
    g = EmptyOperator(task_id = "Task_g")
    end = EmptyOperator(task_id = "END")

#NOTE:
#### Simple SEQUENTIAL TASK GROUP ######(from above comment a - c)
    with TaskGroup("A-A1", tooltip="Task group for A & A1") as group_1:        
        a = EmptyOperator(task_id = "Task_a")
        a1 = EmptyOperator(task_id = "Task_a1")
        b = EmptyOperator(task_id = "Task_b")
        c = EmptyOperator(task_id = "Task_c")
        a >> a1
    
start >> group_1 >> d >> e >> f >> g >> end

#/*************************************************************
## NOTE: Third Method: sub group with Nested Group :
#*************************************************************/

#     start = EmptyOperator(task_id = "START")
#     end = EmptyOperator(task_id = "END")

# #### Simple SEQUENTIAL TASK GROUP ######(from above comment a - c)
#     with TaskGroup("A-A1", tooltip="Task group for A & A1") as group_1:        
#         a = EmptyOperator(task_id = "Task_a")
#         a1 = EmptyOperator(task_id = "Task_a1")
#         b = EmptyOperator(task_id = "Task_b")
#         c = EmptyOperator(task_id = "Task_c")
#         #a >> a1
#         #a >> c
#         a.set_downstream(a1)
#         a.set_downstream(c)

        
# #NOTE:
# ######## NESTED TASK GROUP #######
#     with TaskGroup("D-E-F-G", tooltip="Nested Task Group") as group_2:
#         d = EmptyOperator(task_id = "Task_d")
        
#         with TaskGroup("E-F-G", tooltip="Inner Nested Task Group") as sub_group_2:
#                 e = EmptyOperator(task_id = "Task_e")
#                 f = EmptyOperator(task_id = "Task_f")
#                 g = EmptyOperator(task_id = "Task_g")
#                 e >> f 
#                 e >> g
                
                
# start >> group_1 >> group_2 >> end
        