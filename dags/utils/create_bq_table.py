
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

def create_bq_table(task_id, gcp_conn_id, dataset_id, table_id):
    return BigQueryCreateEmptyTableOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        location='<region-name>',
        dataset_id=dataset_id,
        table_id=table_id,
        table_resource={
            "schema": {
                "fields": [
                    {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'gender', 'type': 'STRING', 'mode': 'REQUIRED'}, 
                    {'name': 'date_of_birth', 'type': 'DATE', 'mode': 'REQUIRED'},
                    {'name': 'date_of_appointment', 'type': 'DATE', 'mode': 'REQUIRED'},
                    {'name': 'job_status', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'wef_date', 'type': 'DATE', 'mode': 'NULLABLE'},
                    {'name': 'qual_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'source', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'post_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'dept_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'domicile', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'cadre', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'pay_level', 'type': 'STRING', 'mode': 'REQUIRED'},
                ]
            },
            "timePartitioning": {
                "type": "YEAR",
                "field": "date_of_appointment"
            },
            "clustering": {
                "fields": ["pay_level", "cadre"]
            }
        }
    )
