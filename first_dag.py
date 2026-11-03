import datetime

from airflow import DAG

# pylint: disable=g-import-not-at-top
try:
  from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
  from airflow.operators.bash_operator import BashOperator
# pylint: enable=g-import-not-at-top

default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=3),
}

dag = DAG(
    'shri2000airflowdag',
    default_args=default_args,
    description='airflowdag',
    schedule='*/4 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='create_bucket',
    bash_command='gcloud storage buckets create gs://dag_airflow_bucket',
    dag=dag
    )
    
t2 = BashOperator(
    task_id='create_dataset',
    bash_command='bq mk -d proj_airflow_ds',
    dag=dag
    )
    
t3 = BashOperator(
    task_id='create_table',
    bash_command='bq mk -t --schema shri_airflow_ds.customers_details "user_id:String,age:String,gender:String,occupation:String,zip_code:String",
    dag=dag
    ) 
 
 t4 = BashOperator(
    task_id='load_Data',
    bash_command='bq laod --source_format=CSV --skip_leading_rows=1 \proj_airflow_ds_airflow_ds.customers_details gs://shri_airflow_bucket/customers_details.csv',
    dag=dag
    )
 
    
t1 >> t2 >> t3 >> t4