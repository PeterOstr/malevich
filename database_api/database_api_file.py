import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from fastapi import FastAPI, UploadFile, File
import pandas as pd
import numpy as np
from typing import List
from pydantic import BaseModel
from io import StringIO



packages = [
    "com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3",
    "com.clickhouse:clickhouse-jdbc:0.6.0-patch4",
    "com.clickhouse:clickhouse-http-client:0.6.0-patch4",
    "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    "com.github.housepower:clickhouse-native-jdbc:2.7.1"
]
ram = 12
cpu = 12*3
# Define the application name and setup session
appName = "Connect To ClickHouse via PySpark"
spark = (SparkSession.builder
         .appName(appName)
         .config("spark.jars.packages", ','.join(packages))
         .config("spark.sql.catalog.clickhouse", "xenon.clickhouse.ClickHouseCatalog")
         .config("spark.sql.catalog.clickhouse.host", "34.118.21.172") # from GCP VM instances or other server ip
         .config("spark.sql.catalog.clickhouse.protocol", "http")
         .config("spark.sql.catalog.clickhouse.http_port", "8123")
         .config("spark.sql.catalog.clickhouse.user", "default")
         .config("spark.sql.catalog.clickhouse.password", "1234")
         .config("spark.sql.catalog.clickhouse.database", "default")
         .config("spark.executor.memory", f"{ram}g")
         .config("spark.driver.maxResultSize", f"{ram}g")
         .config("spark.driver.memory", f"{ram}g")
         .config("spark.executor.memoryOverhead", f"{ram}g")
         .getOrCreate()
         )

spark.sql("use clickhouse")

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/weatherdata")
async def weatherdata():
    df = spark.sql("""
        SELECT * FROM first_database.Weather_moscow_archive
        """)
    model_prediction_df = df.toPandas().to_dict(orient='records')

    return model_prediction_df


