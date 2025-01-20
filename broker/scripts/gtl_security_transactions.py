import random
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
from faker import Faker
from airflow.providers.ssh.hooks.ssh import SSHHook

from r_dekhtiarev.connection import engine, path_st, path_tr_st, schema


fake = Faker()


def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

def upload_data(df: pd.DataFrame, upload_path: str) -> None:
    data = StringIO()
    df.to_csv(data, index=False)
    data.seek(0) 

    ssh_hook = SSHHook(ssh_conn_id='der_hdfs')
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        with sftp.open(upload_path, 'w') as remote_file:
            remote_file.write(data.getvalue())
        sftp.close()


def download_data(download_path: str):
    ssh_hook = SSHHook(ssh_conn_id='der_hdfs')
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        with sftp.open(download_path, 'r') as remote_file:
            content = remote_file.read().decode('utf-8')
        sftp.close()
    
    data = StringIO(content)
    return data


def generate_security_transactions():

    num_clients = 10000
    max_securities_per_client = 5

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    security_data = []
    for client_id in range(1, num_clients + 1):
        num_securities = random.randint(1, max_securities_per_client)
        for _ in range(num_securities):
            transaction_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            price = round(np.random.uniform(50, 500), 2) if currency == 'USD' else round(np.random.uniform(4500, 45000), 2)
            
            security_data.append({
                'client_id': client_id,
                'security_id': np.random.randint(100, 200),
                'transaction_date': transaction_date,
                'transaction_type': np.random.choice(['buy', 'sell']),
                'quantity': np.random.randint(1, 100),
                'currency': currency,
                'price': price
            })

    security_transactions_df = pd.DataFrame(security_data)
    print(security_transactions_df.head())    

    upload_data(security_transactions_df, path_st)

    print("Данные успешно сгенерированы и загружены в HDFS.")


def transform_security_transactions():
    
    df = download_data(path_st)
    security_transactions = pd.read_csv(df)
    if security_transactions is None:
        print("Ошибка чтения данных.")
        raise ValueError("Не удалось получить данные из файла.")
    
    security_transactions['transaction_date'] = pd.to_datetime(security_transactions['transaction_date'])
    upload_data(security_transactions, path_tr_st)
    print("Данные успешно трансформированы и загружены в HDFS.")


def load_security_transactions():
    df = download_data(path_tr_st)
    security_transactions = pd.read_csv(df)
    if security_transactions is None:
        print("Ошибка чтения трансформированных данных.")
        raise ValueError("Не удалось получить трансформированые данные из файла.")
    
    security_transactions.to_sql(name='security_transactions',
                                 con=engine,
                                 schema=schema,
                                 if_exists='append',
                                 index=False)
    print("Данные успешно переданы в GP.")