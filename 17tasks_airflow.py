from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import airflow.utils.dates


#Function to create BigQuery Table using Python
def create_staging_table():
    client = bigquery.Client()
    table_id = "shri2000.ipl_data_ds.ipl_auction_stg_table"
    schema = [
        bigquery.SchemaField("jersey_num", "STRING"),
        bigquery.SchemaField("Country", "STRING"),
        bigquery.SchemaField("Player", "STRING"),
        bigquery.SchemaField("Team", "STRING"),
        bigquery.SchemaField("Base_price", "STRING"),
        bigquery.SchemaField("Winning_bid", "STRING"),
        bigquery.SchemaField("Year", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)  
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

#Default arguments for the DAG
default_args={
    'start_date':datetime(2025, 1, 1),
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
}

#Defining Dag
dag=DAG(
    '12_steps_airflow_dag',
    default_args=default_args,
    description='This is my first build pipeline in airflow',
    schedule_interval='*/4 * * * *',
    catchup=False,
    tags=['twen_one_steps']
)

#Task Initialisation

#Task 1:Empty Task at the start
start=EmptyOperator(
    task_id='start_empty',
        dag=dag
)
    
#Task 2:Create Dataset
create_dataset = BashOperator(
    task_id='create_dataset',
    bash_command='bq mk --d ipl_data',
    dag=dag
)

    
#Task 3:Create Bucket
create_bucket=BashOperator(
    task_id='create_bucket',
    bash_command='gcloud storage buckets create gs://twelve_step_airflow_bucket',
    dag=dag
)
    
#Task 4:Copy file from one bucket to created bucket
copy_files=BashOperator(
    task_id='copy_file_to_new_bucket',
    bash_command='gcloud storage cp gs://ssy20/Auction.csv gs://twelve_step_airflow_bucket/Auction.csv',
    dag=dag
)
    
#Task 5:To create BigQuery Staging Table with python operator
create_staging_table=PythonOperator(
    task_id='create_staging_table',
    python_callable=create_staging_table,
    dag=dag
)
 
    
#Task 6:Load Staging Table with a file which is in bucket
load_staging_table=BashOperator(
    task_id='load_staging_table',
    bash_command='bq load --source_format=CSV --skip_leading_rows=1 --schema=jersey_num:STRING,Country:STRING,Player:STRING,Team:STRING,Base_price:STRING,Winning_bid:STRING,Year:STRING ipl_data_ds.twen_one_step_cus_table gs://://twelve_step_airflow_bucket/Auction.csv',
    dag=dag
)

#Task 7:Create Audit Dataset
create_audit_dataset = BashOperator(
    task_id='create_audit_dataset',
    bash_command='bq mk --dataset twen_one_step_audit_airflow_ds',
    dag=dag
)

# Task 8: Create Staging Audit Table
create_stg_audit_table = BashOperator(
    task_id='create_stg_audit_table',
    bash_command='bq mk -t ipl_audit_ds.ipl_auction_stg_audit_table dataset_name:STRING,table_name:STRING,date_audit:DATE,total_record_count:INT64,audited_by:STRING',
    dag=dag,
)


# Task 9: Insert Staging Audit Data into Table
insert_stg_audit_data = BashOperator(
    task_id='insert_stg_audit_data',
    bash_command="bq query --use_legacy_sql=false 'INSERT INTO ipl_audit_ds.ipl_auction_stg_audit_table (dataset_name, table_name, date_audit, total_record_count, audited_by) SELECT \"ipl_data_ds\" AS dataset_name, \"ipl_auction_stg_table\" AS table_name, CURRENT_DATE() AS date_audit, COUNT(*) AS total_record_count,  AS audited_by FROM ipl_data_ds.ipl_auction_stg_table'",
    dag=dag,
    depends_on_past=False,
)


#Task 10:Create History Dataset
create_hist_dataset = BashOperator(
    task_id='create_hist_dataset',
    bash_command='bq mk --dataset demodb-462015:ipl_hist_ds',
    dag=dag
)

# Task 11: Create History Table
create_hist_table = BashOperator(
    task_id='create_hist_table',
    bash_command='bq mk -t ipl_hist_ds.ipl_auction_hist_table jersey_num:INT64,Country:STRING,Player:STRING,Team:STRING,Base_price:FLOAT64,Winning_bid:FLOAT64,Year:INT64',
    dag=dag,
)


# Task 12: Insert Data into history table
insert_data_to_hist = BashOperator(
    task_id='insert_data_to_hist',
    bash_command="bq query --use_legacy_sql=false 'INSERT INTO ipl_hist_ds.ipl_auction_hist_table (jersey_num, Country, Player, Team, Base_price, Winning_bid, Year) SELECT SAFE_CAST(jersey_num AS INT64), Country, Player, Team, SAFE_CAST(Base_price AS FLOAT64), SAFE_CAST(Winning_bid AS FLOAT64), SAFE_CAST(Year AS INT64) FROM ipl_data_ds.ipl_auction_stg_table'",
    dag=dag,
    depends_on_past=False,
)


# Task 13: Create History Audit Table
create_hist_audit_table = BashOperator(
    task_id='create_hist_audit_table',
    bash_command='bq mk -t ipl_audit_ds.ipl_auction_hist_audit_table dataset_name:STRING,table_name:STRING,date_audit:DATE,total_record_count:INT64,audited_by:STRING',
    dag=dag,
)


# Task 14: Insert history Audit Data into Table
insert_hist_audit_data = BashOperator(
    task_id='insert_hist_audit_data',
    bash_command="bq query --use_legacy_sql=false 'INSERT INTO ipl_audit_ds.ipl_auction_hist_audit_table (dataset_name, table_name, date_audit, total_record_count, audited_by) SELECT \"ipl_hist_ds\" AS dataset_name, \"ipl_auction_hist_table\" AS table_name, CURRENT_DATE() AS date_audit, COUNT(*) AS total_record_count, AS audited_by FROM ipl_hist_ds.ipl_auction_hist_table'",
    dag=dag,
    depends_on_past=False,
)


#Task 15:Create views Dataset
create_views_dataset = BashOperator(
    task_id='create_views_dataset',
    bash_command='bq mk --dataset demodb-462015:twen_one_step_views_airflow_ds',
    dag=dag
)


# Task 16: Create Views table
create_views_table = BashOperator(
    task_id='create_views_table',
    bash_command="bq query --use_legacy_sql=false 'CREATE OR REPLACE VIEW ipl_views_ds.ipl_auction_views_table AS SELECT jersey_num, Country, Player, Team, Base_price, Winning_bid, Year FROM ipl_hist_ds.ipl_auction_hist_table'",
    dag=dag,
)

#Task 17:Empty task at the end 
end = EmptyOperator(
    task_id='end_empty',
    dag=dag
)
    
#setting task dependencies as DAG Flow
start >> create_dataset >> create_bucket >> copy_files >> create_staging_table >> load_staging_table >> create_audit_dataset >> create_stg_audit_table >> insert_stg_audit_data >> create_hist_dataset >> create_hist_table >> insert_data_to_hist >> create_hist_audit_table >> insert_hist_audit_data >> create_views_dataset >> create_views_table >> end
                                                                                 
                                                                                 
                                                                                 
                                                                                 
                                                                                 