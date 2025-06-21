from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'telemetry_spark_dbt_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='Spark processing → dbt transformation pipeline'
)

spark_hourly_agg = BashOperator(
    task_id='spark_hourly_aggregation',
    bash_command='python spark/jobs/hourly_aggregation.py',
    dag=dag
)

spark_user_enrichment = BashOperator(
    task_id='spark_user_enrichment', 
    bash_command='python spark/jobs/user_enrichment.py',
    dag=dag
)

dbt_staging = BashOperator(
    task_id='dbt_staging_models',
    bash_command='cd dbt && dbt run --select staging',
    dag=dag
)

dbt_marts = BashOperator(
    task_id='dbt_mart_models',
    bash_command='cd dbt && dbt run --select marts',
    dag=dag
)

[spark_hourly_agg, spark_user_enrichment] >> dbt_staging >> dbt_marts
