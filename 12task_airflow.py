from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from google.cloud import bigquery


# --------------------------------------------------
# GCP PROJECT AND GCS CONFIGURATION
# --------------------------------------------------

PROJECT_ID = "shri2000"

SOURCE_BUCKET = "ssy20"
DEST_BUCKET = "twelve_step_airflow_bucket"

SOURCE_FILE = f"gs://{SOURCE_BUCKET}/Auction.csv"
DEST_FILE = f"gs://{DEST_BUCKET}/Auction.csv"


# --------------------------------------------------
# BIGQUERY DATASET AND TABLE NAMES
# --------------------------------------------------

STG_DATASET = "ipl_data_ds"
STG_TABLE = "ipl_auction_stg_table"

AUDIT_DATASET = "ipl_audit_ds"
STG_AUDIT_TABLE = "ipl_auction_stg_audit_table"
HIST_AUDIT_TABLE = "ipl_auction_hist_audit_table"

HIST_DATASET = "ipl_hist_ds"
HIST_TABLE = "ipl_auction_hist_table"

VIEWS_DATASET = "ipl_views_ds"
VIEWS_TABLE = "ipl_auction_views_table"


# --------------------------------------------------
# FUNCTION TO CREATE STAGING TABLE
# --------------------------------------------------

def create_auction_staging_table():

    client = bigquery.Client(project=PROJECT_ID)

    table_id = (
        f"{PROJECT_ID}.{STG_DATASET}.{STG_TABLE}"
    )

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

    client.create_table(table, exists_ok=True)

    print(f"Staging table created: {table_id}")


# --------------------------------------------------
# DEFAULT ARGUMENTS
# --------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# --------------------------------------------------
# DEFINE DAG
# --------------------------------------------------

dag = DAG(
    dag_id="17tasks_auction_airflow_dag",
    default_args=default_args,
    description="Auction CSV GCS to BigQuery ETL Pipeline",
    schedule=None,
    catchup=False,
    tags=["GCP", "BigQuery", "Auction"],
)


# --------------------------------------------------
# TASK 1: START
# --------------------------------------------------

start = EmptyOperator(
    task_id="start_empty",
    dag=dag
)


# --------------------------------------------------
# TASK 2: CREATE STAGING DATASET
# --------------------------------------------------

create_dataset = BashOperator(
    task_id="create_dataset",
    bash_command=(
        "bq --project_id=shri2000 show --dataset "
        "shri2000:ipl_data_ds >/dev/null 2>&1 || "
        "bq --project_id=shri2000 mk --dataset "
        "shri2000:ipl_data_ds"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 3: CREATE GCS BUCKET
# --------------------------------------------------

create_bucket = BashOperator(
    task_id="create_bucket",
    bash_command=(
        "gcloud storage buckets describe "
        "gs://twelve_step_airflow_bucket >/dev/null 2>&1 || "
        "gcloud storage buckets create "
        "gs://twelve_step_airflow_bucket"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 4: COPY FILE FROM SOURCE TO DESTINATION
# --------------------------------------------------

copy_files = BashOperator(
    task_id="copy_files",
    bash_command=(
        "gcloud storage cp "
        "gs://ssy20/Auction.csv "
        "gs://twelve_step_airflow_bucket/Auction.csv"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 5: CREATE STAGING TABLE
# --------------------------------------------------

create_staging_table_task = PythonOperator(
    task_id="create_staging_table",
    python_callable=create_auction_staging_table,
    dag=dag
)


# --------------------------------------------------
# TASK 6: LOAD CSV DATA INTO STAGING TABLE
# --------------------------------------------------

load_staging_table = BashOperator(
    task_id="load_staging_table",
    bash_command=(
        "bq --project_id=shri2000 load "
        "--replace "
        "--source_format=CSV "
        "--skip_leading_rows=1 "
        "shri2000:ipl_data_ds.ipl_auction_stg_table "
        "gs://twelve_step_airflow_bucket/Auction.csv"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 7: CREATE AUDIT DATASET
# --------------------------------------------------

create_audit_dataset = BashOperator(
    task_id="create_audit_dataset",
    bash_command=(
        "bq --project_id=shri2000 show --dataset "
        "shri2000:ipl_audit_ds >/dev/null 2>&1 || "
        "bq --project_id=shri2000 mk --dataset "
        "shri2000:ipl_audit_ds"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 8: CREATE STAGING AUDIT TABLE
# --------------------------------------------------

create_stg_audit_table = BashOperator(
    task_id="create_stg_audit_table",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'CREATE TABLE IF NOT EXISTS "
        "`shri2000.ipl_audit_ds.ipl_auction_stg_audit_table` "
        "(dataset_name STRING, "
        "table_name STRING, "
        "date_audit DATE, "
        "total_record_count INT64, "
        "audited_by STRING)'"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 9: INSERT STAGING AUDIT DATA
# --------------------------------------------------

insert_stg_audit_data = BashOperator(
    task_id="insert_stg_audit_data",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'INSERT INTO "
        "`shri2000.ipl_audit_ds.ipl_auction_stg_audit_table` "
        "(dataset_name, table_name, date_audit, "
        "total_record_count, audited_by) "
        "SELECT "
        "'\"ipl_data_ds\"', "
        "'\"ipl_auction_stg_table\"', "
        "CURRENT_DATE(), "
        "COUNT(*), "
        "'\"Airflow\"' "
        "FROM `shri2000.ipl_data_ds.ipl_auction_stg_table`'"
    ),
    dag=dag,
    depends_on_past=False
)


# --------------------------------------------------
# TASK 10: CREATE HISTORY DATASET
# --------------------------------------------------

create_hist_dataset = BashOperator(
    task_id="create_hist_dataset",
    bash_command=(
        "bq --project_id=shri2000 show --dataset "
        "shri2000:ipl_hist_ds >/dev/null 2>&1 || "
        "bq --project_id=shri2000 mk --dataset "
        "shri2000:ipl_hist_ds"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 11: CREATE HISTORY TABLE
# --------------------------------------------------

create_hist_table = BashOperator(
    task_id="create_hist_table",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'CREATE TABLE IF NOT EXISTS "
        "`shri2000.ipl_hist_ds.ipl_auction_hist_table` "
        "(jersey_num INT64, "
        "Country STRING, "
        "Player STRING, "
        "Team STRING, "
        "Base_price FLOAT64, "
        "Winning_bid FLOAT64, "
        "Year INT64)'"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 12: INSERT DATA INTO HISTORY TABLE
# --------------------------------------------------

insert_data_to_hist = BashOperator(
    task_id="insert_data_to_hist",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'TRUNCATE TABLE "
        "`shri2000.ipl_hist_ds.ipl_auction_hist_table`; "
        "INSERT INTO "
        "`shri2000.ipl_hist_ds.ipl_auction_hist_table` "
        "(jersey_num, Country, Player, Team, "
        "Base_price, Winning_bid, Year) "
        "SELECT "
        "SAFE_CAST(jersey_num AS INT64), "
        "Country, Player, Team, "
        "SAFE_CAST(Base_price AS FLOAT64), "
        "SAFE_CAST(Winning_bid AS FLOAT64), "
        "SAFE_CAST(Year AS INT64) "
        "FROM `shri2000.ipl_data_ds.ipl_auction_stg_table`'"
    ),
    dag=dag,
    depends_on_past=False
)


# --------------------------------------------------
# TASK 13: CREATE HISTORY AUDIT TABLE
# --------------------------------------------------

create_hist_audit_table = BashOperator(
    task_id="create_hist_audit_table",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'CREATE TABLE IF NOT EXISTS "
        "`shri2000.ipl_audit_ds.ipl_auction_hist_audit_table` "
        "(dataset_name STRING, "
        "table_name STRING, "
        "date_audit DATE, "
        "total_record_count INT64, "
        "audited_by STRING)'"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 14: INSERT HISTORY AUDIT DATA
# --------------------------------------------------

insert_hist_audit_data = BashOperator(
    task_id="insert_hist_audit_data",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'INSERT INTO "
        "`shri2000.ipl_audit_ds.ipl_auction_hist_audit_table` "
        "(dataset_name, table_name, date_audit, "
        "total_record_count, audited_by) "
        "SELECT "
        "'\"ipl_hist_ds\"', "
        "'\"ipl_auction_hist_table\"', "
        "CURRENT_DATE(), "
        "COUNT(*), "
        "'\"Airflow\"' "
        "FROM `shri2000.ipl_hist_ds.ipl_auction_hist_table`'"
    ),
    dag=dag,
    depends_on_past=False
)


# --------------------------------------------------
# TASK 15: CREATE VIEWS DATASET
# --------------------------------------------------

create_views_dataset = BashOperator(
    task_id="create_views_dataset",
    bash_command=(
        "bq --project_id=shri2000 show --dataset "
        "shri2000:ipl_views_ds >/dev/null 2>&1 || "
        "bq --project_id=shri2000 mk --dataset "
        "shri2000:ipl_views_ds"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 16: CREATE BIGQUERY VIEW
# --------------------------------------------------

create_views_table = BashOperator(
    task_id="create_views_table",
    bash_command=(
        "bq --project_id=shri2000 query --use_legacy_sql=false "
        "'CREATE OR REPLACE VIEW "
        "`shri2000.ipl_views_ds.ipl_auction_views_table` "
        "AS SELECT jersey_num, Country, Player, Team, "
        "Base_price, Winning_bid, Year "
        "FROM `shri2000.ipl_hist_ds.ipl_auction_hist_table`'"
    ),
    dag=dag
)


# --------------------------------------------------
# TASK 17: END
# --------------------------------------------------

end = EmptyOperator(
    task_id="end_empty",
    dag=dag
)


# --------------------------------------------------
# DAG TASK DEPENDENCIES
# --------------------------------------------------

(
    start
    >> create_dataset
    >> create_bucket
    >> copy_files
    >> create_staging_table_task
    >> load_staging_table
    >> create_audit_dataset
    >> create_stg_audit_table
    >> insert_stg_audit_data
    >> create_hist_dataset
    >> create_hist_table
    >> insert_data_to_hist
    >> create_hist_audit_table
    >> insert_hist_audit_data
    >> create_views_dataset
    >> create_views_table
    >> end
)
