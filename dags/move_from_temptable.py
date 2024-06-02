from pendulum import duration
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from clickhouse_driver import Client
from datetime import datetime as py_datetime, timedelta
import pandas as pd
import pendulum

CLICKHOUSE_CONN_ID = 'clickhouse'

# Определение параметров DAG
default_args = {
    "owner": "Peter",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": duration(seconds=5),
}

def extract_data():
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    client = Client(host=conn.host, port=conn.port, user=conn.login, password=conn.password, database=conn.schema)
    
    query = "SELECT Localtime, Temperature_fact_C, Temperature_xgb_C, Temperature_catb_C FROM first_database.temp_table"
    data = client.execute(query)
    
    df = pd.DataFrame(data, columns=['Localtime', 'Temperature_fact_C', 'Temperature_xgb_C', 'Temperature_catb_C'])
    return df

def transform_data(df):
    # Преобразуем столбец Localtime из строки в datetime
    df['Localtime'] = pd.to_datetime(df['Localtime'], format='%Y-%m-%d %H:%M:%S')
    return df

def load_data(df):
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    client = Client(host=conn.host, port=conn.port, user=conn.login, password=conn.password, database=conn.schema)
    
    data_to_insert = df.to_dict('records')
    client.execute('INSERT INTO first_database.WeatherComparsion (Localtime, Temperature_fact_C, Temperature_xgb_C, Temperature_catb_C) VALUES', data_to_insert, types_check=True)

def delete_data(df):
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    client = Client(host=conn.host, port=conn.port, user=conn.login, password=conn.password, database=conn.schema)
    
    localtimes = df['Localtime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    localtimes_str = "', '".join(localtimes)
    query = f"ALTER TABLE first_database.temp_table DELETE WHERE Localtime IN ('{localtimes_str}')"
    
    client.execute(query)

def extract_transform_load():
    df = extract_data()
    df_transformed = transform_data(df)
    load_data(df_transformed)
    delete_data(df_transformed)

@dag(
    tags=["copyfrom_temptable"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2023, 12, 1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/include',        
    description='Copy data from temp table to WeatherComparison',
)
def weather_table_copy():
    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_load,
    )

    etl_task

dag = weather_table_copy()
