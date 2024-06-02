from pendulum import datetime, duration
import itertools
from airflow import XComArg
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime as py_datetime
from clickhouse_driver import Client
import requests
import pandas as pd
import pendulum
from pendulum.parsing.exceptions import ParserError





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


def api_request():
        # Установка базового URL и ключа API
    base_url = "http://api.weatherapi.com/v1"
    api_key = "ae03964b347b4881afe205155242404"  # Замените YOUR_API_KEY на ваш ключ API

    # Параметры запроса
    params = {
        "key": api_key,
        "q": "Moscow"  # Название города, для которого мы хотим получить погоду
    }

    # Формирование запроса
    response = requests.get(f"{base_url}/current.json", params=params)

 # Проверка успешности запроса
    if response.status_code == 200:
        # Возвращаем данные о погоде из ответа
        return response.json()
    else:
        # Обработка случая неуспешного запроса
        raise Exception(f"API request failed with status code {response.status_code}")

def write_request(weather_data):
    # df = pd.DataFrame.from_dict(weather_data)
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    location_data = weather_data.get('location', {})
    current_data = weather_data.get('current', {})
    localtime_str = location_data.get('localtime')
     # Ensure there's a leading zero for the hour part
    # parts = localtime_str.split()
    # hour_part = parts[1]
    # if len(hour_part.split(':')[0]) == 1:
    #     parts[1] = '0' + hour_part
    #     localtime_str = ' '.join(parts)
    
    data = {
        'name': location_data.get('name'),
        'region': location_data.get('region'),
        'country': location_data.get('country'),
        'lat': location_data.get('lat'),
        'lon': location_data.get('lon'),
        #'tz_id': location_data.get('tz_id'),
        #'localtime_epoch': location_data.get('localtime_epoch'),
        #'localtime': location_data.get('localtime'),
        'localtime': pendulum.parse(location_data.get('localtime'),strict=False),  # Преобразование строки в DateTime
        #'last_updated_epoch': current_data.get('last_updated_epoch'),
        #'last_updated': current_data.get('last_updated'),
        'temp_c': current_data.get('temp_c'),
        #'temp_f': current_data.get('temp_f'),
        #'is_day': current_data.get('is_day'),
        'condition_text': current_data['condition'].get('text'),
        #'condition_icon': current_data['condition'].get('icon'),
        #'condition_code': current_data['condition'].get('code'),
        #'wind_mph': current_data.get('wind_mph'),
        'wind_kph': current_data.get('wind_kph'),
        #'wind_degree': current_data.get('wind_degree'),
        'wind_dir': current_data.get('wind_dir'),
        #'pressure_mb': current_data.get('pressure_mb'),
        #'pressure_in': current_data.get('pressure_in'),
        'precip_mm': current_data.get('precip_mm'),
        #'precip_in': current_data.get('precip_in'),
        'humidity': current_data.get('humidity'),
        'cloud': current_data.get('cloud'),
        'feelslike_c': current_data.get('feelslike_c'),
        #'feelslike_f': current_data.get('feelslike_f'),
        'vis_km': current_data.get('vis_km'),
        #'vis_miles': current_data.get('vis_miles'),
        'uv': current_data.get('uv'),
        #'gust_mph': current_data.get('gust_mph'),
        #'gust_kph': current_data.get('gust_kph')
    }
    
    # Преобразование в DataFrame
    df = pd.DataFrame([data])
    # Преобразование строки времени в объект DateTime

    client = Client(port=conn.port, host=conn.host, user=conn.login, password=conn.password, database=conn.schema)
    client.execute('INSERT INTO first_database.WeatherData VALUES', df.to_records(index=False).tolist(), types_check=True)

@dag(
    tags=["weatherapi"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    start_date=datetime(2023,12,1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/include',        
    description='weather request',
    doc_md=__doc__,
)
def updating_weather_table():
    weather_request_task1 = PythonOperator(task_id="api_request", python_callable=api_request)
    write_request_task2 = PythonOperator(
        task_id="write_request",
        python_callable=write_request,
        op_args=["{{ task_instance.xcom_pull(task_ids='api_request') }}"]
        )
    weather_request_task1 >> write_request_task2

updating_weather_table()