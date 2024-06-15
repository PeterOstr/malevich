from fastapi import FastAPI, HTTPException, Query
import requests
from pydantic import BaseModel
from typing import List, Optional
from requests.auth import HTTPBasicAuth

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
def get_weatherdata(limit: Optional[int] = Query(None, description="Limit the number of returned records")):
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
    """
    if limit:
        query += f" ORDER BY Localtime DESC LIMIT {limit}"
    query += " FORMAT TSV"

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

class WeatherComparison(BaseModel):
    Localtime: str
    Temperature_C: float
    Temperature_xgb_C: float
    Temperature_catb_C: float

@app.get("/weathercomparison/", response_model=List[WeatherComparison])
def get_weathercomparison(limit: Optional[int] = Query(None, description="Limit the number of returned records")):
    query = """
    SELECT 
        Localtime, 
        Temperature_fact_C, 
        Temperature_xgb_C,
        Temperature_catb_C
    FROM first_database.WeatherComparsion
    """
    if limit:
        query += f" ORDER BY Localtime DESC LIMIT {limit}"
    query += " FORMAT TSV"

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

    weather_comparison_data = [WeatherComparison(
        Localtime=row.split('\t')[0],
        Temperature_C=float(row.split('\t')[1]),
        Temperature_xgb_C=float(row.split('\t')[2]),
        Temperature_catb_C=float(row.split('\t')[3])
    ) for row in result]

    return weather_comparison_data

@app.get("/")
def read_root():
    return {"message": "Welcome to the Weather Data API"}
