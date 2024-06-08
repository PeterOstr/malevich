from fastapi import FastAPI, UploadFile, File
import pandas as pd
import numpy as np
from typing import List
from pydantic import BaseModel
from io import StringIO



import joblib

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

class Date_for_prediction_data(BaseModel):
    #index: List
    date: List

# models
model_xgb_t = joblib.load('my_xgb_model.joblib')
model_catb_t = joblib.load('my_catb_model.joblib')




@app.post("/model/predict_xgb_t")
async def predict_xgb_t(data: Date_for_prediction_data):

    data_str = StringIO(data.model_dump_json())
    data_df = pd.read_json(data_str, orient='values')


    data_df.columns = ['Local time in Moscow']

    # Извлеките признаки из новых данных
    data_features = pd.DataFrame()
    data_features['year'] = data_df['Local time in Moscow'].dt.year
    data_features['month'] = data_df['Local time in Moscow'].dt.month
    data_features['day'] = data_df['Local time in Moscow'].dt.day
    data_features['hour'] = data_df['Local time in Moscow'].dt.hour


    model_prediction = np.round(model_xgb_t.predict(data_features),2)

    model_prediction_df = pd.DataFrame(data=model_prediction, index=np.arange(len(model_prediction)), columns=['pred'])

    model_prediction_df = model_prediction_df.to_json()

    return model_prediction_df



@app.post("/model/predict_catb_t")
async def predict_catb_t(data: Date_for_prediction_data):

    data_str = StringIO(data.model_dump_json())
    data_df = pd.read_json(data_str, orient='values')


    data_df.columns = ['Local time in Moscow']

    # Извлеките признаки из новых данных
    data_features = pd.DataFrame()
    data_features['year'] = data_df['Local time in Moscow'].dt.year
    data_features['month'] = data_df['Local time in Moscow'].dt.month
    data_features['day'] = data_df['Local time in Moscow'].dt.day
    data_features['hour'] = data_df['Local time in Moscow'].dt.hour


    model_prediction = np.round(model_catb_t.predict(data_features),2)

    model_prediction_df = pd.DataFrame(data=model_prediction, index=np.arange(len(model_prediction)), columns=['pred'])

    model_prediction_df = model_prediction_df.to_json()

    return model_prediction_df

