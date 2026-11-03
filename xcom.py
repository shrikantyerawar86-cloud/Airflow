from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Task 1: Push data into XCom
def push_data(**kwargs):

    kwargs['ti'].xcom_push(
        key='ord_msg',
        value='weekly_task'
    )

    print("Data successfully pushed to XCom")


# Task 2: Pull data from XCom
def pull_data(**kwargs):

    message = kwargs['ti'].xcom_pull(
        task_ids='push_task',
        key='ord_msg'
    )

    if message == 'weekly_task':
        print(f"Pulled Data Message: {message}")
    else:
        print("Invalid message")


# Define DAG
with DAG(
    dag_id='xcom_example',
    start_date=datetime(2024, 4, 2),
    schedule='@daily',
    catchup=False
) as dag:

    # Push Task
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_data
    )

    # Pull Task
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_data
    )

    # Task dependencies
push_task >> pull_task
