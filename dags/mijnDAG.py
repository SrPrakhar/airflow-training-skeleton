from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Prakhar',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='lekker_hello_world',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

def print_awesome_string(**context):
    print('Hello, world!')

# [START howto_operator_bash]
print_hello_world = PythonOperator(
    task_id="print_awesome_string",
    python_callable=print_awesome_string,
    provide_context=True,
    dag=dag,
)
# [END howto_operator_bash]
