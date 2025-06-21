# Airflow DAG for telemetry pipeline
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_events():
    df = pd.read_json('/opt/airflow/data/sample_events.json')
    df.to_csv('/opt/airflow/data/feature_usage_hourly_sample.csv', index=False)

with DAG(
    dag_id='telemetry_ingestion_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='ingest_event_data',
        python_callable=ingest_events
    )

    t2 = BashOperator(
        task_id='run_spark_etl',
        bash_command='spark-submit /opt/airflow/pipelines/spark_jobs/etl_transform.py'
    )

    t1 >> t2
