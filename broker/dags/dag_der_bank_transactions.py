from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from r_dekhtiarev.gtl_bank_transactions import generate_bank_transactions, transform_bank_transactions, load_bank_transactions


args = {
    'owner': 'r.dekhtiarev',
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'email_on failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id="gtl_bank_transactions",
    description="GenerateTransormLoad bank_transactions",
    schedule_interval="0 12 * * 2",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["r_dekhtiarev"],
    default_args=args
)


generate_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_bank_transactions,
    dag=dag
)
    
transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_bank_transactions,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_bank_transactions,
    dag=dag
)


generate_task >> transform_task >> load_task