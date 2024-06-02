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
from sqlalchemy import create_engine
from io import StringIO
from airflow.operators.dagrun_operator import TriggerDagRunOperator






CLICKHOUSE_CONN_ID = 'clickhouse'


# Определение параметров DAG
default_args = {
    "owner": "Peter",
    "depends_on_past": False,
    "retries": 0,
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


def extract_data():
   conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
   engine = create_engine(f'clickhouse://{conn.login}:{conn.password}@{conn.host}:8123/{conn.schema}')
   with engine.connect() as connection:
      df = pd.read_sql("""
        SELECT Localtime, Temperature_C FROM first_database.WeatherData 
        ORDER BY Localtime 
        DESC LIMIT 1""", connection)
   return df

def make_prediction_and_write_comparison(ti):
    
    df = pd.DataFrame(ti.xcom_pull(task_ids='extract_data'))
   
    # Получите временную метку и температуру из DataFrame
    timestamp, temperature = df.iloc[0,0], df.iloc[0,1]


    # Преобразуйте временную метку в нужный формат строки
    formatted_date = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    # Создайте словарь для отправки запроса модели
    X_dict = {'date': [formatted_date]}
    
    # Отправьте запрос к модели для прогнозирования
    predicted_data_response_xgb = requests.post('https://weatherimage-ma4ayonvha-lm.a.run.app/model/predict_xgb_t', json=X_dict)
    predicted_data_response_catb = requests.post('https://weatherimage-ma4ayonvha-lm.a.run.app/model/predict_catb_t', json=X_dict)
    
    # Проверьте успешность запроса
    if predicted_data_response_xgb.status_code == 200 and predicted_data_response_catb.status_code == 200:
        # Прочтите предсказанные данные из JSON-ответа и округлите до двух знаков после запятой
        predicted_results_xgb = pd.read_json(StringIO(predicted_data_response_xgb.json()))
        predicted_results_catb = pd.read_json(StringIO(predicted_data_response_catb.json()))
        
        # Создайте DataFrame для сравнения данных
        data_comparison = pd.DataFrame({
            'Localtime': [formatted_date],
            'Temperature_C': [temperature],
            'Temperature_xgb_C': [round(predicted_results_xgb.iloc[0,0],2)],
            'Temperature_catb_C': [round(predicted_results_catb.iloc[0,0],2)]
        })
        
        print(data_comparison['Localtime'])

        #data_comparison['Localtime'] = pd.to_datetime(data_comparison['Localtime'],format='%Y-%m-%d %H:%M:%S')
        data_comparison['Temperature_C'] = data_comparison['Temperature_C'].astype('float32')
        data_comparison['Temperature_xgb_C'] = data_comparison['Temperature_xgb_C'].astype('float32')
        data_comparison['Temperature_catb_C'] = data_comparison['Temperature_catb_C'].astype('float32')
    
        # Запись данных в таблицу first_database.WeatherComparison
        conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
        client = Client(port=conn.port, host=conn.host, user=conn.login, password=conn.password, database=conn.schema)
        #return data_comparison.to_json()
        client.execute('INSERT INTO first_database.temp_table VALUES', data_comparison.to_records(index=False).tolist(), types_check=True)
    else:
        logging.error(f"API request failed with status code: XGB: {predicted_data_response_xgb.status_code}, CatB: {predicted_data_response_catb.status_code}")







@dag(
    tags=["weatherapi","xgb","catboost"],
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

def weather_table_prediction():
    weather_request_task1 = PythonOperator(task_id="api_request", python_callable=api_request)
    write_request_task2 = PythonOperator(
        task_id="write_request",
        python_callable=write_request,
        op_args=["{{ task_instance.xcom_pull(task_ids='api_request') }}"]
        )
    
    extract_data_task3 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    make_prediction_and_write_comparison_task4 = PythonOperator(
        task_id="make_prediction_and_write_comparison",
        python_callable=make_prediction_and_write_comparison,
    )

    trigger_dag_task5 = TriggerDagRunOperator(
        task_id='trigger_next_dag',
        trigger_dag_id='weather_table_copy',  # Replace with your second DAG ID
        wait_for_completion=True,
    )

    weather_request_task1 >> write_request_task2 >> extract_data_task3 >> make_prediction_and_write_comparison_task4 >> trigger_dag_task5

weather_table_prediction()