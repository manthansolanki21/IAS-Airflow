from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def load_to_bq(task_id, gcp_conn_id, bucket_name, dataset_id, table_id):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        location='<region-name>',
        configuration={
            "load": {
                "sourceUris": [f"gs://{bucket_name}/files/officers.csv"],
                "destinationTable": {
                    "projectId": "<gcp-project-id>",
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "skipLeadingRows": 1,
            }
        }
    )
