from datetime import date, datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Prakhar',
    #'start_date': datetime(2019, 11, 17),
}

dag = DAG(
    dag_id='excercise3',
    default_args=args,
    schedule_interval="@daily",
)



def _get_weekday(execution_date, **context):
  return execution_date.strftime("%a")

branching = BranchPythonOperator(
  task_id="branching", 
  python_callable=_get_weekday, 
  provide_context=True, 
  dag=dag
)

days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
for day in days:
    branching >> DummyOperator(task_id=day, dag=dag)
