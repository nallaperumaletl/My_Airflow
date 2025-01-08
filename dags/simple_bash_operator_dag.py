from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the output file path
output_file = "/home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/output.txt"

# Define the DAG
with DAG(
    "simple_bash_operator_dag_with_file_output",
    description="A simple DAG using BashOperator to write output to a file",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Do not run past scheduled runs
    tags=["example", "bash_operator"],
) as dag:

    # Task 1: Print "Hello World" and write to file
    hello_task = BashOperator(
        task_id="print_hello",
        bash_command=f'echo "Hello, World!" >> {output_file}',
    )

    # Task 2: List files in /tmp and append output to the file
    list_files_task = BashOperator(
        task_id="list_files",
        bash_command=f'echo "Listing files in /tmp:" >> {output_file} && ls -l /tmp >> {output_file}',
    )

    # Task 3: Print the current date and append output to the file
    print_date_task = BashOperator(
        task_id="print_date",
        bash_command=f'echo "Current date and time:" >> {output_file} && date >> {output_file}',
    )

    # Set task dependencies
    hello_task >> list_files_task >> print_date_task

