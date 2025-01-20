import random
from datetime import datetime, timedelta 

from faker import Faker
import numpy as np
import pandas as pd

from r_dekhtiarev.connection import engine, schema


fake = Faker()


def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

def generate_bank_transactions(**kwargs):

    num_clients = 10000
    max_transactions_per_client = 10
    
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    transaction_data = []
    for client_id in range(1, num_clients + 1):
        num_transactions = random.randint(1, max_transactions_per_client)
        for _ in range(num_transactions):
            transaction_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            amount = round(np.random.uniform(100, 10000), 2) if currency == 'USD' else round(np.random.uniform(9000, 900000), 2)
            
            # Добавление аномально больших сумм
            if random.random() < 0.01:
                amount *= 10

            transaction_data.append({
                'client_id': client_id,
                'transaction_id': np.random.randint(1000, 10000),
                'transaction_date': transaction_date,
                'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                'account_number': fake.iban(),
                'currency': currency,
                'amount': amount
            })

    # Добавление аномально большого количества транзакций в один из дней для 3-4 клиентов
    anomalous_clients = random.sample(range(1, num_clients + 1), 4)
    for client_id in anomalous_clients:
        anomalous_dates = [random_date(start_date, end_date) for _ in range(2)]
        for date in anomalous_dates:
            for _ in range(100):  # Аномально большое количество транзакций в один день
                transaction_data.append({
                    'client_id': client_id,
                    'transaction_id': np.random.randint(1000, 10000),
                    'transaction_date': date,
                    'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                    'account_number': fake.iban(),
                    'currency': np.random.choice(['USD', 'RUB']),
                    'amount': round(np.random.uniform(100, 10000), 2)
                })

    transaction_df = pd.DataFrame(transaction_data)
    print(transaction_df.head())
    kwargs['ti'].xcom_push(key='transaction_df', value=transaction_df)
    print("Данные успешно сгенерированы.")


def transform_bank_transactions(**kwargs):
    bank_transactions = kwargs['ti'].xcom_pull(key='transaction_df')
    if bank_transactions is None:
        print("Ошибка чтения данных.")
        raise ValueError("Не удалось получить данные из файла.")

    bank_transactions['transaction_date'] = pd.to_datetime(bank_transactions['transaction_date'])
    kwargs['ti'].xcom_push(key='bank_transactions', value=bank_transactions)
    print("Данные успешно трансформированы.")

def load_bank_transactions(**kwargs):
    bank_transactions = kwargs['ti'].xcom_pull(key='bank_transactions')
    if bank_transactions is None:
        print("Ошибка чтения трансформированных данных.")
        raise ValueError("Не удалось получить трансформированные данные из файла.")
    
    
    bank_transactions.to_sql(name='bank_transactions',
                             con=engine,
                             schema=schema,
                             if_exists='append',
                             index=False)
    print("Данные успешно переданы в GP.")