from datetime import datetime, timedelta# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import BigQuery

email = ["satish.bigdata9@gmail.com","arun@gmail.com","Venu@gmail.com"]
proj_id = "b49_project_test"
taskid = 101

default_args={
        'start_date': airflow.utils.dates.days_ago(0),
        'depends_on_past': False,
        'email': email,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=10)
    }           
    
dag = DAG(
    'Training_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    sle_interval=timedelta(days=1)
 )
 
 
  # t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='create_bucket',
    bash_command='gcloud storage buckets create gs://b49bucket',
    dag = dag)

t3 = BashOperator(
        task_id='create_table',
        bash_command = 'bq mk -t sample_dag',
        dag = dag
)

t2 = BashOperator(
    task_id='copydata',
    bash_command='gsutil cp sourcepath dest_path',
    retries=3,
    dag = dag
 )

t4 = PythonOperator(
    task_id='create_dataset',
    python_callable = createds(),
    dag = dag
)


#below python function
def createds():
    client = bigquery.Client()
    dataset_id = "vaarahi-b15.testpythonds"
    dataset = bigquery.Dataset(dataset_id)
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset sucess {}.{}".format(client.project, dataset.dataset_id))
    
t1 >> t2 >> t4 >> t3
 
