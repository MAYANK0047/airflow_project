from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mayank',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'metadata_driven_etl_pipeline',
    default_args=default_args,
    description='3-Stage PySpark Metadata Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['databricks', 'metadata', 'pyspark'],
) as dag:

    load_bronze = DatabricksRunNowOperator(
        task_id='1_ingest_bronze_layer',
        databricks_conn_id='databricks_default',
        job_id=551469014133047 
    )

    transform_silver = DatabricksRunNowOperator(
        task_id='2_process_silver_layer',
        databricks_conn_id='databricks_default',
        job_id=1110687934608296
    )

    load_gold = DatabricksRunNowOperator(
        task_id='3_publish_gold_layer',
        databricks_conn_id='databricks_default',
        job_id=335899002610864
    )

    load_bronze >> transform_silver >> load_gold
