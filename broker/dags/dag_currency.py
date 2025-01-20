from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from r_dekhtiarev.currency_load_transform import extract_data, transform_data, load_to_gp

args = {
    "owner": "r.dekhtiarev",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="currency",
    schedule_interval="0 12 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["r_dekhtiarev"],
    default_args=args
) as dag:
    extract_data = PythonOperator(task_id="extract", python_callable=extract_data)
    transform_data = PythonOperator(task_id="transform", python_callable=transform_data)
    load_data = PythonOperator(task_id="load", python_callable=load_to_gp)

    extract_data >> transform_data >> load_data

