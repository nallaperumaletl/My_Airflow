#IMPORTS:
from datetime import timedelta , datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator

output_file = "/home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/Nalla_etl_tolldata.log"


with DAG(
    "Nalla_etl_tolldata",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'owner': 'Nalla Perumal',
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    # [END default_args]
    description="Apache Airflow Project on local simplified ETL Approach",
    schedule=timedelta(days=1),
    start_date=datetime(2025,1,8),
    catchup=False,
    tags=["example"],
) as dag:
#schedule_interval=timedelta(days=1)


# Task 1 "Unzipping the data..."

    unzip_tar = BashOperator(
        task_id="unzipping_tarfile",
        bash_command='tar -xzvf /home/nalla/bigdata/AIRFLOW-Workouts-guides/workouts/tolldata.tgz -C /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area',
    )

# Task 2: Extracting data from the CSV file...

    extract_csv = BashOperator(
        task_id="extract_data_from_csv",
        bash_command='cut -d "," -f1-4 /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/vehicle-data.csv > /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/csv_data.csv',
    )
    
# Task 3: Extracting data from the TSV file...

    extract_tsv = BashOperator(
        task_id="extract_data_from_tsv",
        bash_command="cut -f5-7 /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/tollplaza-data.tsv | awk '{gsub(/\\r/,\"\"); print}' | tr '\t' ',' > /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/tsv_data.csv",
    )
    
# Task 4 Extracting data from the fixed-width file...

    extract_fixed_width = BashOperator(
        task_id="extract_data_from_fixed_file",
        bash_command='cut -c 59- /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/payment-data.txt | tr " " "," > /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/fixed_width_data.csv',
    )
    
# Task 5: Consolidating data from previous tasks...

    consolidate_data = BashOperator(
        task_id="consolidating_extracted_file",
        bash_command='paste -d "," /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/csv_data.csv \
    /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/tsv_data.csv \
    /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/fixed_width_data.csv \
    > /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/extracted_data.csv',
    )
# Task 6 Transforming and loading the data...

    transform_load = BashOperator(
        task_id="transforming_and_loading",
        bash_command=r"sed 's/[^,]*/\U&/4' /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/extracted_data.csv > /home/nalla/bigdata/AIRFLOW-Workouts-guides/work-area/transformed_data.csv",

    )

# Task 7: Print current date:
    print_date_task = BashOperator(
        task_id="print_date",
        bash_command=f'echo "Current date and time:" >> {output_file} && date >> {output_file} && echo "Successfully completed!"  >> {output_file}',
    )
    
# Set task dependencies
    unzip_tar >> extract_csv >> extract_tsv >> extract_fixed_width >> consolidate_data >> transform_load >> print_date_task

