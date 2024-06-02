from pendulum import datetime, duration
import itertools
from airflow import XComArg
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as py_datetime


# Определение параметров DAG
default_args = {
    "owner": "Peter",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(seconds=5),
    # 'email_on_failure': True,
    # 'email_on_success': True,
    # 'email': EMAIL_DEVELOP,
}


def print_hello():
    print('Hello, Airflow!')

def print_current_time():
    print(f'Current time is: {py_datetime.now()}')



@dag(
    tags=["test","helloworld"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    start_date=datetime(2023,12,1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/include',        
    description='hello world',
    doc_md=__doc__,
)

def print_welcome():
    print_hello_task1 = PythonOperator(task_id="print_hello", python_callable=print_hello)
    print_current_time_task2 = PythonOperator(task_id="print_current_time", python_callable=print_current_time)
    print_hello_task1 >> print_current_time_task2

print_welcome()