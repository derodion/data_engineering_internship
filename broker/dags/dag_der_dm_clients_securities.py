from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


args = {
    'owner': 'r.dekhtiarev',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id="update_dm_clients_securities",
    description= "Update Data Mart Top 10 Clients Securities",
    schedule_interval="30 12 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["r_dekhtiarev"],
    default_args=args
)


upd_dm_cs = SQLExecuteQueryOperator(
    task_id="update_dm_clients_securities",
    conn_id='der_gp',
    sql='r_dekhtiarev/top_10_clients_securities.sql',
    dag=dag
)


upd_dm_cs