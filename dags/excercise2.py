from datetime import date

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Prakhar',
    'start_date': '2019-11-17',
}

dag = DAG(
    dag_id='excercise2',
    default_args=args,
    schedule_interval="@daily",
)

def print_execution_date(**context):
    today = date.today()
    print("Today's date:", today)

# [START howto_operator_bash]
print_execution_date = PythonOperator(
    task_id="print_execution_date",
    python_callable=print_execution_date,
    provide_context=True,
    dag=dag,
)
# [END howto_operator_bash]



wait_5 = BashOperator(task_id="wait_5", bash_command="sleep 5", dag=dag)

wait_1 = BashOperator(task_id="wait_1", bash_command="sleep 1", dag=dag)

wait_10 = BashOperator(task_id="wait_10", bash_command="sleep 10", dag=dag)

the_end = DummyOperator(task_id="the_end", dag=dag)

print_execution_date >> [wait_5, wait_1, wait_10]

[wait_5, wait_1, wait_10] >> the_end
