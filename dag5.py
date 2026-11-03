from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 10, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=12)
}


# Create Parent DAG
with DAG(
    dag_id='Orders_dag',
    default_args=default_args,
    description='Parent DAG to trigger Transaction DAG',
    schedule='*/20 * * * *',
    catchup=False
) as dag:

    # Task 1: Start
    dummy_start = EmptyOperator(
        task_id='dummy_start'
    )

    # Task 2: Create GCS bucket
    create_bucket = BashOperator(
        task_id='create_bucket',
        bash_command='gcloud storage buckets create gs://operator_airflow'
    )

    # Task 3: Trigger Child DAG
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_transaction_dag',
        trigger_dag_id='Transaction_dag',
        wait_for_completion=True,
        reset_dag_run=False
    )

    # Task 4: Stop
    dummy_stop = EmptyOperator(
        task_id='dummy_stop'
    )

    # Dependencies
    dummy_start >> create_bucket >> trigger_dag >> dummy_stop
