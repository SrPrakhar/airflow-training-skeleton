
from datetime import date, datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs_operator import HttpToGcsOperator

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


http_to_gcs = HttpToGcsOperator(
  task_id="get_currency_" + "EUR",
  method="GET",
  endpoint=f"/history?start_at='{{ yesterday_ds }}'&end_at='{{ ds }}'&symbols=EUR&base=GBP",
  #http_conn_id="airflow-training-currency-http",
  http_conn_id="http-default",
  gcs_path="EUR/{{ ds }}-" + "EUR" + ".json",
  gcs_bucket="airflow-training-data-11",
  dag=dag,
)
