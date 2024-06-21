FROM apache/spark:3.4.2-scala2.12-java11-python3-ubuntu
USER root

# Установите cmake
RUN apt-get update && apt-get install -y cmake

COPY model_predict.py .
COPY my_xgb_model.joblib .
COPY my_catb_model.joblib .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["uvicorn","model_predict:app","--host","0.0.0.0","--port","8080"]