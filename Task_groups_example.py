
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Define DAG
with DAG(
    dag_id='order_processing_dag',
    start_date=datetime(2025, 1, 1),
    schedule="*/4 * * * *",
    catchup=False
) as dag:

    # Starting task
    start = EmptyOperator(
        task_id='start'
    )

    # GROUP 1: Data Extraction
    with TaskGroup('data_extraction') as tg1:

        extract_orders = EmptyOperator(
            task_id='extract_orders'
        )

        extract_customers = EmptyOperator(
            task_id='extract_customers'
        )

        extract_products = EmptyOperator(
            task_id='extract_products'
        )

        combine_data = EmptyOperator(
            task_id='combine_data'
        )

        # Internal dependencies
        [extract_orders, extract_customers, extract_products] >> combine_data

    # GROUP 2: Data Processing
    with TaskGroup('data_processing') as tg2:

        validate_data = EmptyOperator(
            task_id='validate_data'
        )

        clean_data = EmptyOperator(
            task_id='clean_data'
        )

        load_data = EmptyOperator(
            task_id='load_data'
        )

        # Internal dependencies
        validate_data >> clean_data >> load_data

    # Ending task
    stop = EmptyOperator(
        task_id='stop'
    )

    # Cross-group dependencies
    [start,tg1,tg2] >> stop
