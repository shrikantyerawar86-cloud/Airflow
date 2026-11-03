from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 10, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}


# Create Child DAG
with DAG(
    dag_id='Transaction_dag',
    default_args=default_args,
    description='Child DAG for copying Auction CSV file',
    schedule=None,
    catchup=False
) as dag:

    # Task 1: Start
    dummy_start = EmptyOperator(
        task_id='dummy_start'
    )

    # Task 2: Copy file from source bucket to destination bucket
    copy_file = BashOperator(
        task_id='copy_file',
        bash_command='gcloud storage cp gs://ssy20/Auction.csv gs://operator_airflow/'
    )

    # Task 3: Stop
    dummy_stop = EmptyOperator(
        task_id='dummy_stop'
    )

    # Dependencies
    dummy_start >> copy_file >> dummy_stop
