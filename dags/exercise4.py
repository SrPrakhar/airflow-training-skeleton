
from datetime import date, datetime, timedelta
import airflow
from airflow.models import DAG

args = {
"owner": "Prakhar",
'start_date': datetime(2019, 09, 20),
}

dag = DAG(
dag_id="exercise4",
default_args=args,
schedule_interval="0 0 * * *",
)
