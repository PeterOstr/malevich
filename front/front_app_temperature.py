import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import networkx as nx
import matplotlib.pyplot as plt
import streamlit.components.v1 as components


# URL переменные local/url

#url_clilckhouse= ('https://weatherimage-ma4ayonvha-lm.a.run.app')
url_clilckhouse = 'http://localhost:8000'

# url_airflow = '34.118.108.23'
# url_airflow = 'http://localhost:8090'


# Функция для получения данных из API
def fetch_weather_data(limit=None):
    params = {}
    if limit:
        params['limit'] = limit
    response = requests.get(f"{url_clilckhouse}/weatherdata/", params=params)
    if response.status_code != 200:
        st.error("Failed to fetch data from the server.")
        return []
    return response.json()

def fetch_weather_data_comparison(limit=None):
    params = {}
    if limit:
        params['limit'] = limit
    response = requests.get("https://weatherimage-ma4ayonvha-lm.a.run.app/weathercomparison/", params=params)
    if response.status_code != 200:
        st.error("Failed to fetch data from the server.")
        return []
    return response.json()

def get_airflow_dags(username, password):
    airflow_url = "http://localhost:8090/api/v1/dags"
    response = requests.get(
        airflow_url,
        headers={"Content-Type": "application/json"},
        auth=HTTPBasicAuth(username, password)
    )
    if response.status_code == 200:
        dags = response.json()['dags']
        return [dag['dag_id'] for dag in dags]
    else:
        st.error("Failed to fetch DAGs from Airflow.")
        return []

def trigger_airflow_dag(dag_id, username, password):
    airflow_url = f"http://localhost:8090/api/v1/dags/{dag_id}/dagRuns"
    response = requests.post(
        airflow_url,
        json={},
        headers={"Content-Type": "application/json"},
        auth=HTTPBasicAuth(username, password)
    )
    return response.status_code, response.json()

def get_dag_structure(dag_id, username, password):
    airflow_url = f"http://localhost:8090/api/v1/dags/{dag_id}/tasks"
    response = requests.get(
        airflow_url,
        headers={"Content-Type": "application/json"},
        auth=HTTPBasicAuth(username, password)
    )
    if response.status_code == 200:
        tasks = response.json()['tasks']
        return tasks
    else:
        st.error(f"Failed to fetch tasks for DAG {dag_id} from Airflow.")
        return []

def draw_dag(dag_id, tasks):
    G = nx.DiGraph()
    for task in tasks:
        G.add_node(task['task_id'])
        if 'downstream_task_ids' in task:
            for downstream_task_id in task['downstream_task_ids']:
                G.add_edge(task['task_id'], downstream_task_id)

    plt.figure(figsize=(10, 7))
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=3000, node_color='skyblue', font_size=10, font_weight='bold', arrows=True, arrowstyle='-|>', arrowsize=12)
    plt.title(f"Graph for DAG: {dag_id}")
    plt.show()

# Основная функция для страницы данных погоды
def weather_data_page():
    st.title("Weather Data Dashboard")
    st.write("This dashboard displays weather data retrieved from a ClickHouse database.")

    # Получение данных
    limit = st.sidebar.number_input("Limit records", min_value=1, max_value=1000, value=10, step=1)
    data = fetch_weather_data(limit=limit)
    # data_comparison = fetch_weather_data_comparison(limit=limit)

    # Преобразование данных в DataFrame
    df = pd.DataFrame(data)
    # df = pd.read_json(data, orient='split')
    # df_comparison = pd.DataFrame(data_comparison)

    # Отображение данных в виде таблицы
    st.write("### Weather Data Table")
    st.dataframe(df)

    st.write("### Weather Data Table Comparison")
    # st.dataframe(df_comparison)

# Основная функция для страницы Airflow
def airflow_page():
    st.title("Airflow DAGs Dashboard")

    # Ввод учетных данных для Airflow
    st.sidebar.title("Airflow Credentials")
    username = st.sidebar.text_input("Username", value="airflow")
    password = st.sidebar.text_input("Password", value="airflow", type="password")

    # Получение списка DAG из Airflow
    dags = get_airflow_dags(username, password)
    selected_dag = st.sidebar.selectbox("Select a DAG to trigger and view", dags)

    # Кнопка для запуска DAG в Airflow
    if st.button("Trigger Selected Airflow DAG"):
        if selected_dag:
            status_code, response = trigger_airflow_dag(selected_dag, username, password)
            if status_code == 200:
                st.success(f"DAG {selected_dag} was triggered successfully.")
            else:
                st.error(f"Failed to trigger DAG {selected_dag}. Response: {response}")
        else:
            st.warning("Please select a DAG to trigger.")

    # Получение и отображение структуры DAG
    if selected_dag:
        st.write(f"### DAG Structure for {selected_dag}")
        tasks = get_dag_structure(selected_dag, username, password)
        if tasks:
            draw_dag(selected_dag, tasks)

# Основная функция для страницы профилирования данных
def profiling_page():
    st.title("Data Profiling Report")
    st.write("This page displays the data profiling report generated by ydata-profiling.")

    # Чтение HTML файла и отображение его содержимого
    with open("data_profile_report.html", "r", encoding='utf-8') as f:
        html_content = f.read()

    components.html(html_content, height=800, scrolling=True)

# Основная функция приложения
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Weather Data", "Airflow DAGs", "Profiling Report"])

    if page == "Weather Data":
        weather_data_page()
    elif page == "Airflow DAGs":
        airflow_page()
    elif page == "Profiling Report":
        profiling_page()

    # Дополнительная информация и ссылки
    st.sidebar.title("About")
    st.sidebar.info(
        """
        This app displays weather data from a ClickHouse database.
        """
    )

    st.sidebar.info("Feel free to contact me\n"
                    "[My GitHub](https://github.com/PeterOstr)\n"
                    "[My Linkedin](https://www.linkedin.com/in/ostrikpeter/)\n"
                    "[Or just text me in Telegram](https://t.me/Politejohn)\n"
                    ".")

if __name__ == "__main__":
    main()
