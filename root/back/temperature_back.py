from fastapi import FastAPI, HTTPException
import requests
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Конфигурация подключения к ClickHouse
clickhouse_config = {
    'host': '34.118.21.172',
    'port': '8123',
    'user': 'default',
    'password': '1234',
    'database': 'first_database'
}

class WeatherData(BaseModel):
    Localtime: str
    Temperature_C: float
    Wind_Speed_kph: float
    Wind_Direction: str
    Pressure_mb: float
    Humidity: float
    Cloud: str
    Visibility_km: float

@app.get("/weatherdata/", response_model=List[WeatherData])
def get_weatherdata():
    query = """
    SELECT 
        Localtime, 
        Temperature_C, 
        Wind_Speed_kph, 
        Wind_Direction, 
        Pressure_mb, 
        Humidity, 
        Cloud, 
        Visibility_km 
    FROM WeatherData
    FORMAT TSV
    """
    url = f"http://{clickhouse_config['host']}:{clickhouse_config['port']}/"
    params = {
        'user': clickhouse_config['user'],
        'password': clickhouse_config['password'],
        'database': clickhouse_config['database'],
        'query': query
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        result = response.text.splitlines()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    weather_data = [WeatherData(
        Localtime=row.split('\t')[0],
        Temperature_C=float(row.split('\t')[1]),
        Wind_Speed_kph=float(row.split('\t')[2]),
        Wind_Direction=row.split('\t')[3],
        Pressure_mb=float(row.split('\t')[4]),
        Humidity=float(row.split('\t')[5]),
        Cloud=row.split('\t')[6],
        Visibility_km=float(row.split('\t')[7])
    ) for row in result]

    return weather_data

@app.get("/")
def read_root():
    return {"message": "Welcome to the Weather Data API"}
