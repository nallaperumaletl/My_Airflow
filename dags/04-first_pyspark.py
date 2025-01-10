from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession


# Function to run PySpark job
def run_pyspark_job():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Simple PySpark Job in Airflow") \
        .master("local[*]") \
        .getOrCreate()

    # Create a simple DataFrame
    data = [("John", 25), ("Doe", 30), ("Alice", 28)]
    columns = ["Name", "Age"]

    df = spark.createDataFrame(data, columns)

    # Define output path
    #output_path = "/home/nalla/airflow/Nalla-test-outfile/first_pyspark.out"
    output_path = "file:///home/nalla/airflow/Nalla-test-outfile/first_pyspark.out"

    # Write DataFrame to the specified path(coalesce use to write into a single file)
    df.coalesce(1).write.mode("overwrite").csv(output_path)

    # Stop SparkSession
    spark.stop()


# Default arguments for the DAG
default_args = {
    'owner': 'Nalla perumal',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="pyspark_dag",
    default_args=default_args,
    schedule_interval=None,  # Run on demand
    catchup=False,
) as dag:
    # PythonOperator to run the PySpark job
    pyspark_task = PythonOperator(
        task_id="run_pyspark_job",
        python_callable=run_pyspark_job,
    )
