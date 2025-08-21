from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

default_args = {
    "owner": "Manthan Solanki",
    "depends_on_past": False,
    "retries": 0,
}
    

with DAG(
    dag_id='spark_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = SparkSubmitOperator(
        task_id='extract_data',
        conn_id='spark_default',
        application='/opt/airflow/jobs/extract.py',
        jars='/opt/airflow/jars/postgresql-42.7.3.jar',
        verbose=True
    )

    transform_task = SparkSubmitOperator(
        task_id='transform_data',
        conn_id='spark_default',
        application='/opt/airflow/jobs/transform.py',
        verbose=True
    )

    load_task = SparkSubmitOperator(
        task_id='load_data',
        conn_id='spark_default',
        application='/opt/airflow/jobs/load.py',
        verbose=True
    )

    trigger_bigquery = TriggerDagRunOperator(
        task_id='trigger_load_to_bigquery',
        trigger_dag_id='load_to_bigquery'
    )

    extract_task >> transform_task >> load_task >> trigger_bigquery
