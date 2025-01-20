import re
from io import StringIO

import pandas as pd
from faker import Faker
from airflow.providers.ssh.hooks.ssh import SSHHook

from r_dekhtiarev.connection import engine, path_c, path_tr_c, schema


fake = Faker()


def format_name(column: pd.Series) -> pd.Series:
    
    cnt = 0
    deleted_words = ["DDS", "DVM", "Dr.", "MD", "Miss", "Mr.", "Mrs.", "Ms.", "PhD"]
    
    for name in column:
        for word in name.split():
            if word in deleted_words:
                column[cnt] = column[cnt].replace(word, '').strip()
        cnt += 1
    return column


def format_phone_number(phone:str) -> str:
    
    pattern = r'(?:[01\+]*)(?:[\D]?)([\d]{3})(?:[\D]?)([\d]{3})(?:[\D]?)([\d]{4})([x]?[\d]*)'
    replace = r'+1-\1-\2-\3\4'
    
    return re.sub(pattern, replace, phone)


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


def generate_clients():
    num_clients = 10000
    client_data = []
    for client_id in range(1, num_clients + 1):
        client_data.append({
            'client_id': client_id,
            'client_name': fake.name(),
            'client_email': fake.email(),
            'client_phone': fake.phone_number(),
            'client_address': fake.address()
        })
    
    client_df = pd.DataFrame(client_data)
    print(client_df.head())

    upload_data(client_df, path_c)

    print("Данные успешно сгенерированы и загружены в HDFS.")

def transform_clients():

    df = download_data(path_c)
    clients = pd.read_csv(df)
    if clients is None:
        print("Ошибка чтения данных.")
        raise ValueError("Не удалось получить данные из файла.")    

    format_name(clients['client_name'])
    clients['client_phone'] = clients['client_phone'].apply(format_phone_number)
    clients[['client_address', 'client_address_opt']] = clients['client_address'].str.split('\n', expand=True)

    upload_data(clients, path_tr_c)
    print("Данные успешно трансформированы и загружены в HDFS.")

def load_clients():
    df = download_data(path_tr_c)
    clients = pd.read_csv(df)
    if clients is None:
        print("Ошибка чтения трансформированных данных.")
        raise ValueError("Не удалось получить трансформированные данные из файла.")
    
    clients.to_sql(name='clients',
                   con=engine,
                   schema=schema,
                   if_exists='append',
                   index=False
                   )
    print("Данные успешно переданы в GP.")