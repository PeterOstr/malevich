FROM apache/spark:3.4.2-scala2.12-java11-python3-ubuntu
USER root


# Установите необходимые пакеты, включая cmake 3.18 или выше
RUN apt-get update && apt-get install -y \
    build-essential \
    libgomp1 \
    git \
    wget \
    curl \
    vim \
    unzip \
    libomp-dev

# Установите cmake 3.18 или выше
RUN apt-get remove -y cmake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.21.0/cmake-3.21.0-linux-x86_64.sh
RUN sh cmake-3.21.0-linux-x86_64.sh --skip-license --prefix=/usr/local

COPY model_predict.py .
COPY my_xgb_model.joblib .
COPY my_catb_model.joblib .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["uvicorn","model_predict:app","--host","0.0.0.0","--port","8080"]