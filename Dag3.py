
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


# Default arguments
default_args = {
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

# Create DAG
with DAG(
    dag_id="shri2000airflowdag",
    default_args=default_args,
    description="Create GCS bucket and load CSV data into BigQuery",
    schedule="*/4 * * * *",
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    # Task 1: Create GCS bucket
    t1 = BashOperator(
        task_id="create_bucket",
        bash_command="""
        gcloud storage buckets describe gs://dag_airflow_bucket ||
        gcloud storage buckets create gs://dag_airflow_bucket
        --location=asia-south1
        """,
    )

    # Task 2: Create BigQuery dataset
    t2 = BashOperator(
        task_id="create_dataset",
        bash_command="""
        bq --location=asia-south1 mk --dataset \
        --if_not_exists \
        proj_airflow_ds
        """,
    )

    # Task 3: Create BigQuery table
    t3 = BashOperator(
        task_id="create_table",
        bash_command="""
        bq mk --table --if_not_exists \
        proj_airflow_ds.customers_details \
        user_id:STRING,age:STRING,gender:STRING,occupation:STRING,zip_code:STRING
        """,
    )

    # Task 4: Load CSV data into BigQuery
    t4 = BashOperator(
        task_id="load_data",
        bash_command="""
        bq load \
        --location=asia-south1 \
        --source_format=CSV \
        --skip_leading_rows=1 \
        proj_airflow_ds.customers_details \
        gs://dag_airflow_bucket/customers_details.csv \
        user_id:STRING,age:STRING,gender:STRING,occupation:STRING,zip_code:STRING
        """,
    )

    # Task dependencies
    t1 >> t2 >> t3 >> t4
