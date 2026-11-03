from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 
from google.cloud import bigquery

try:
  from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
  from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'shri2000airflowdag',
    default_args=default_args,
    description='airflowdag',
    schedule='*/4 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

t1 = BashOperator(
    task_id='create_bucket',
    bash_command='gcloud storage buckets create gs://shri_airflow200018',
    dag=dag
)

t2 = BashOperator(
    task_id='create_dataset',
    bash_command='bq mk -d dag18_airflow_ds',
    dag=dag
)

t3 = BashOperator(
    task_id='create_table',
    bash_command= 'bq mk -t dag18_airflow_ds.airflow1 user_id:STRING,age:STRING,gender:STRING,occupation:STRING,zip_code:STRING',
    dag=dag
)


t4 = BashOperator(
    task_id='load_Data',
    bash_command='bq load --autodetect --source_format=CSV --skip_leading_rows=1 --schema=user_id:STRING,age:STRING,gender:STRING,occupation:STRING,zip_code:STRING dag18_airflow_ds.airflow1 gs://shri_airflow_bucket/customer_details.csv',
    dag=dag
)

t5 = BashOperator(
    task_id='copydata',
    bash_command= 'gcloud storge cp gs://shri_airflow_bucket/customer_details.csv gs://shri_airflow200018/customer_details.csv',
    dag=dag
)


t1 >> t2 >> t3 >> t4 >> t5