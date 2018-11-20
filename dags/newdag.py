import datetime

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_days_till_christmas(execution_date, **context):
    christmas_date = datetime.date(2018, 12, 24)
    print("Days til christmas: {}".format(christmas_date - execution_date))


args = {"owner": "Harmen", "start_date": airflow.utils.dates.days_ago(14)}

p = print_days_till_christmas
from airflow.operators.bash_operator import BashOperator





with DAG(
        dag_id="exercise2",
        default_args=args
):
    print_days_till_christmas = PythonOperator(task_id="print_days_till_christmas",
                                               python_callable=print_days_till_christmas,
                                               default_args=args)
    wait_5 = BashOperator(task_id="wait_1", bash_command="sleep 1")
    wait_1 = BashOperator(task_id="wait_5", bash_command="sleep 5")
    wait_10 = BashOperator(task_id="wait_10", bash_command="sleep 10")
    the_end = DummyOperator(task_id="the_end")

print_days_till_christmas >> [wait_1, wait_5, wait_10]
[wait_1, wait_5, wait_10] >> the_end

