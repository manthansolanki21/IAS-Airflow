from airflow import settings
from airflow.models import Connection
from airflow.utils.session import provide_session

GCP_CONN_ID = "ias_officers_etl"

@provide_session
def create_gcp_connection(session=None):
    conn = session.query(Connection).filter(Connection.conn_id == GCP_CONN_ID).first()
    if not conn:
        new_conn = Connection(
            conn_id=GCP_CONN_ID,
            conn_type="google_cloud_platform",
            extra={"key_path": "/opt/airflow/plugins/gcp_cred.json"}
        )
        session.add(new_conn)
        session.commit()

create_gcp_connection()
