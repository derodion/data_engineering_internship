import dotenv
import json
import os
import requests

from pyspark.sql import SparkSession

dotenv.load_dotenv()


user = os.getenv("USER_GP")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
schema = os.getenv("SCHEMA")


def extract_data(**kwargs):
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    json_data = requests.get(url=url).json()

    kwargs['ti'].xcom_push(key='currency_json', value=json_data)

    print("Данные извлечены успешно.")


def transform_data(**kwargs):
    json_data = kwargs['ti'].xcom_pull(key='currency_json')

    if json_data is None:
        print("Ошибка: данные не найдены в XCom.")
        raise ValueError("Не удалось получить данные из XCom.")
    
    usd = json_data["Valute"]["USD"]
    usd["current_date"] = json_data["Date"]
   
    spark = (SparkSession
            .builder
            .appName('currency')
            .getOrCreate())

    currency = spark.createDataFrame([usd])

    new_cols_name = ["char_code", "id", "name", "nominal", "num_code", "previous", "value", "current_date"]

    for old_name, new_name in zip(currency.columns, new_cols_name):
        currency = currency.withColumnRenamed(old_name, new_name)

    currency = currency.withColumn("current_date", currency["current_date"].cast("date"))

    kwargs['ti'].xcom_push(key='currency_df', value=currency.toPandas().to_dict(orient="records"))

    print("Данные трансформированы успешно.")

    spark.stop()


def load_to_gp(**kwargs):
   
    spark = (SparkSession
            .builder
            .appName('currency')
            .config("spark.jars", "/opt/airflow/dags/r_dekhtiarev/postgresql-42.6.0.jar")
            .getOrCreate())
    
    data = kwargs['ti'].xcom_pull(key='currency_df')
    df = spark.createDataFrame(data)
  

    greenplum_url = f"jdbc:postgresql://{host}:{port}/{database}"

    greenplum_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=greenplum_url,
        table=f"{schema}.currency",
        mode="append",
        properties=greenplum_properties
    )
    print("Данные успешно записаны в GreenPlum.")

    spark.stop()