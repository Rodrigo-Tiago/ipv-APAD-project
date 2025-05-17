import os
import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker()
NUM_SESSIONS = 100  # Número de sessões a gerar
MAX_USER_ID = 100   # Máximo user_id permitido
MAX_CONTENT_ID = 100  # Máximo content_id permitido

# Diretório de saída dinâmico
output_dir = os.path.join(os.path.dirname(__file__), 'csv_csv')
os.makedirs(output_dir, exist_ok=True)
os.chdir(output_dir)

# Valores fixos
DEVICE_TYPES = ['Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Smart TV', 'Game Console']
APP_VERSIONS = ['v1.0.0', 'v1.2.3', 'v2.0.1', 'v2.1.0', 'v3.0.5']
OS_NAMES = ['Windows', 'macOS', 'Linux', 'Android', 'iOS', 'tvOS', 'FireOS']

# Gerar SESSIONS.csv
with open('SESSIONS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['session_id', 'user_id', 'content_id', 'device_type', 'app_version', 'os_name', 'time', 'watched_duration'])
    for i in range(1, NUM_SESSIONS + 1):
        writer.writerow([
            i,
            random.randint(1, MAX_USER_ID),
            random.randint(1, MAX_CONTENT_ID),
            random.choice(DEVICE_TYPES),
            random.choice(APP_VERSIONS),
            random.choice(OS_NAMES),
            fake.date_time_between(start_date='-2y', end_date='now'),
            random.randint(5, 180)
        ])

print("CSV gerado com sucesso para a tabela!")