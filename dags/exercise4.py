
from datetime import date, datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator

args = {
"owner": "Prakhar",
'start_date': datetime(2019, 9, 20),
}

dag = DAG(
dag_id="exercise4",
default_args=args,
schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
  task_id="getData",
  sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
  bucket="airflow-training-data-11",
  filename="{{ ds }}/properties_{}.json",
  postgres_conn_id="Postgres_Cnxn",
  dag=dag,
)
