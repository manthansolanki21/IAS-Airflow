from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from datetime import datetime
from utils.files_to_gcs import upload_to_gcs
from utils.load_to_bq_table import load_to_bq
from utils.create_bq_table import create_bq_table
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


default_args = {
    "owner": "Manthan Solanki",
    "depends_on_past": False,
    "retries": 0,
}

GCP_CONN_ID = '<GCP_CONN_ID>'
GCS_BUCKET_NAME = '<GCS_BUCKET_NAME>'
GCS_DATASET_NAME = '<GCS_DATASET_NAME>'
GCS_TABLE_NAME = '<GCS_TABLE_NAME>'


with DAG(
    dag_id='load_to_bigquery',
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    try:

        create_bucket = GCSCreateBucketOperator(
            task_id='create_gcs_bucket',
            bucket_name=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
            location='<region-name>',
            storage_class='STANDARD',
            resource={"ifNotExists": True}
        )

        create_database = BigQueryCreateEmptyDatasetOperator(
            task_id='create_my_dataset',
            gcp_conn_id=GCP_CONN_ID,
            dataset_id=GCS_DATASET_NAME,
            location='<region-name>',
            exists_ok=True
        )

        load_to_gcs= PythonOperator(
            task_id='upload_to_gcs',
            python_callable=upload_to_gcs,
            op_args=['/opt/airflow/data/warehouse/', 'files', GCS_BUCKET_NAME, GCP_CONN_ID],
            provide_context=True,
        )

        create_table = create_bq_table(
            task_id="create_partitioned_table",
            gcp_conn_id=GCP_CONN_ID,
            dataset_id=GCS_DATASET_NAME,
            table_id=GCS_TABLE_NAME
        )

        insert_into_table = load_to_bq(
            task_id="insert_into_partitioned_table",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            dataset_id=GCS_DATASET_NAME,
            table_id=GCS_TABLE_NAME
        )
    
    except Exception as e:
        log.error(e)
    
[create_bucket, create_database] >> create_table
[create_bucket, create_database] >> load_to_gcs
[load_to_gcs, create_table] >> insert_into_table