from pendulum import datetime, duration
import itertools
from airflow import XComArg
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as py_datetime
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import pandas as pd
from clickhouse_driver import Client

CLICKHOUSE_CONN_ID = 'clickhouse'

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


def extract_data():
   conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
   engine = create_engine(f'clickhouse://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
   with engine.connect as connection:
      df = pd.read_sql('''
        SELECT *
        FROM default.smokers
        LIMIT 1
                        ''', connection)
   return df

def transform_data(ti):
   df = pd.DataFrame(ti.xcom_pull(task_ids='get_from'))
   return df.to_json()

# def load_data(ti):
#     df = pd.from_json(ti.xcom_pull(task_ids='transform_data'))
#     conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
#     client = Client(port=conn.port, host=conn.host, user=conn.login, password=conn.password, schema=conn.schema)
#     client.execute('insert into имятаблицы values',df) # [tuple(row)]  df.iterrows() итерировать построчно

def insert_data(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='transform_data'))
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    client = Client(port=conn.port, host=conn.host, user=conn.login, password=conn.password, database=conn.schema)
    client.execute('INSERT INTO input_test_table VALUES', df.to_records(index=False).tolist())


@dag(
    tags=["test","table"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    start_date=datetime(2023,12,1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/include',        
    description='test table input',
    doc_md=__doc__,
)

def get_data_from_table():
#   extract_data_task1 = PythonOperator(task_id="extract_data", python_callable=extract_data)
    ch_list_count_rows_start_of_month = ClickHouseOperatorExtended(
        task_id='get_from',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='get_from.sql'
    )
    transform_data_task2 = PythonOperator(task_id="transform_data", python_callable=transform_data)

    write_data_task3 = PythonOperator(task_id="insert_data", python_callable=insert_data)

    ch_list_count_rows_start_of_month >> transform_data_task2 >> write_data_task3


get_data_from_table()