from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from r_dekhtiarev.gtl_clients import generate_clients, transform_clients, load_clients


args = {
    'owner': 'r.dekhtiarev',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id="gtl_clients",
    description="GenerateTransformLoad clients",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["r_dekhtiarev"],
    default_args=args
)


generate_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_clients,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_clients,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_clients,
    dag=dag
)


generate_task >> transform_task >> load_task