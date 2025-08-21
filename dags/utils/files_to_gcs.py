
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def upload_to_gcs(data_folder, gcs_path, bucket_name, gcs_conn_id, **kwargs):

    try:

        log.info(f'...........Trying to upload to GCS.............')

        local_file_path = f"{data_folder}officers.csv"
        gcs_file_path = f"{gcs_path}/officers.csv"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)

        log.info(f'...........File saved...........')

    except Exception as e:
        log.error(e)
