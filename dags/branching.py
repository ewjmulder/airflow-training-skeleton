import airflow
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(12)
}

dag = DAG(
    dag_id='example_branch_operator',
    default_args=args,
    schedule_interval="@daily")

cmd = 'ls -l'
run_this_first = DummyOperator(task_id='run_this_first', dag=dag)

options = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']


def return_current_day(**context):
    return options.__getitem__(context["execution_date"].weekday() - 1)


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=return_current_day,
    provide_context=True,
    dag=dag)
branching.set_upstream(run_this_first)

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)

for option in options:
    t = DummyOperator(task_id=option, dag=dag)
    t.set_upstream(branching)
    t.set_downstream(join)
