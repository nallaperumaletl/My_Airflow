from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

json_obj = Variable.get("MYSQL_DB_CONN_VARIABLES")
dict_obj = Variable.get("MYSQL_DB_CONN_VARIABLES", deserialize_json=True)
pwd = Variable.get("MYSQL_ROOT_PASSWORD")

def print_airflow_variables():
    print(f"the value of variable json_obj is {json_obj} and its datatype is {type(json_obj)}")
    print(f"the value of variable dict_obj is {dict_obj} and its datatype is {type(dict_obj)}")
    print(f"the value of variable dict_obj['MYSQL_DB_HOST_NAME'] is {dict_obj['MYSQL_DB_HOST_NAME']}")
    print(f"the value of variable dict_obj['MYSQL_DB_NAME'] is {dict_obj['MYSQL_DB_NAME']}")
    print(f"the value of variable dict_obj['MYSQL_DB_USER'] is {dict_obj['MYSQL_DB_USER']}")
    print(f"the value of variable dict_obj['MYSQL_DB_PORT'] is {dict_obj['MYSQL_DB_PORT']}")
    print(f"the value of variable dict_obj['MYSQL_DB_SCHEMA'] is {dict_obj['MYSQL_DB_SCHEMA']}")
    print(f"the value of variable pwd is {pwd} and its datatype is {type(pwd)}")
    return None

def_args = {
    "owner": "Nalla Perumal",
    "retries": 0,
    "start_date": datetime(2025, 1, 1),
}

with DAG("nalla_airflow_variables", default_args=def_args, catchup=False) as dag:
    start = DummyOperator(task_id="START")
    airflow_variables = PythonOperator(
        task_id="AIRFLOW_VARIABLES",
        python_callable=print_airflow_variables
    )
    end = DummyOperator(task_id="END")

    start >> airflow_variables >> end

