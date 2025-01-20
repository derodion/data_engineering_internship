from io import StringIO

import numpy as np
import pandas as pd
from faker import Faker
from airflow.providers.ssh.hooks.ssh import SSHHook

from r_dekhtiarev.connection import engine, path_s, path_tr_s, schema


fake = Faker()


def upload_data(df: pd.DataFrame, upload_path: str) -> None:
    data = StringIO()
    df.to_csv(data, index=False)

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


def generate_securitiies():
    num_securities = 100
    security_data = []
    security_types = ['stock', 'bond']
    markets = ['NYSE', 'NASDAQ', 'LSE']

    for security_id in range(100, 100 + num_securities):
        security_data.append({
            'security_id': security_id,
            'security_name': fake.company(),
            'security_type': np.random.choice(security_types),
            'market': np.random.choice(markets)
        })

    security_df = pd.DataFrame(security_data)
    print(security_df.head())

    upload_data(security_df, path_s)
    
    print("Данные успешно сгенерированы и загружены в HDFS.")

def transform_securities():
    df = download_data(path_s)
    securities = pd.read_csv(df)
    if securities is None:
        print("Ошибка чтения данных.")
        raise ValueError("Не удалось получить данные из файла.")
    
    upload_data(securities, path_tr_s)
    print("Данные успешно трансформированы и загружены в HDFS.")

def load_securities():
    df = download_data(path_tr_s)
    securities = pd.read_csv(df)
    if securities is None:
        print("Ошибка чтения трансформированных данных.")
        raise ValueError("Не удалось получить трансформированные данные из файла.")
    
    securities.to_sql(
        name='securities',
        con=engine,
        schema=schema,
        if_exists='append',
        index=False
    )
    print("Данные успешно переданы в GP.")
