import streamlit as st
import pandas as pd
import requests

# Заголовок и описание
st.title("Weather Data Dashboard")
st.write("This dashboard displays weather data retrieved from a ClickHouse database.")

# Функция для получения данных из API
def fetch_weather_data():
    response = requests.get("http://localhost:8000/weatherdata/")
    if response.status_code != 200:
        st.error("Failed to fetch data from the server.")
        return []
    return response.json()

# Получение данных
data = fetch_weather_data()

# Преобразование данных в DataFrame
df = pd.DataFrame(data)

# Отображение данных в виде таблицы
st.write("### Weather Data Table")
st.dataframe(df)

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
