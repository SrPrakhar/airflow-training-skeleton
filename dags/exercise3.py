from datetime import date, datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

args = {
    'owner': 'Prakhar',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='excercise3',
    default_args=args,
    schedule_interval="@daily",
)

def print_weekday(execution_date, **context):
    value = execution_date.strftime("%a")
    return value

def _get_email_name(execution_date, **context):
    value = execution_date.strftime("%a")
    if Value in ["Mon"]:
        return 'email_bob'
    elif Value in ["Tue", "Thu"]:
        return 'email_joe'
    else:
        return 'email_alice'
    
print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=print_weekday,
    provide_context=True,
    dag=dag,
)

branching = BranchPythonOperator(
  task_id="branching", 
  python_callable=_get_weekday, 
  provide_context=True, 
  dag=dag
)

print_weekday >> branching
