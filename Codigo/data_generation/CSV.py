import os
import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker()
NUM_SESSIONS = 100  # Número sessões a gerar
MAX_USER_ID = 100   # Máximo users permitido
MAX_CONTENT_ID = 100  # Máximo contents permitido

# Diretório de saída dinâmico
output_dir = os.path.join(os.path.dirname(__file__), 'csv_csv')
os.makedirs(output_dir, exist_ok=True)
os.chdir(output_dir)

# Valores fixos
PLATFORMS = ['Mobile', 'Computer', 'TV', 'Tablet', 'Console']
DEVICE_TYPES = ['Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Smart TV', 'Game Console']
OS_FAMILIES = ['Windows', 'macOS', 'Linux', 'Android', 'iOS', 'tvOS', 'FireOS', 'Playstation', 'Nintendo', 'Xbox']
OS_NAMES = ['Android 13', 'iOS 16', 'iPadOS 15', 'FireOS 6', 'tvOS 14', 'Windows 11', 'macOS Ventura', 'Ubuntu 22.04', 'PS5', 'Switch', 'Xbox']
APP_VERSIONS = ['v1.0.0', 'v1.2.3', 'v2.0.1', 'v2.1.0', 'v3.0.5']

# Função para gerar códigos
def generate_code(prefix, num, total_length=16):
    num_str = str(num).zfill(total_length - len(prefix) - 1)  # -1 para o underscore
    return f"{prefix}_{num_str}"

# Gerar SESSIONS.csv
with open('SESSIONS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['SESSION_CODE', 'USER_CODE', 'CONTENT_CODE', 'TIME', 'WATCHED_DURATION', 'PLATFORM', 'DEVICE_TYPE', 'OS_FAMILY', 'OS_NAME', 'APP_VERSION'])
    for i in range(1, NUM_SESSIONS + 1):
        writer.writerow([
            generate_code('SESSION', i),
            generate_code('USER', random.randint(1, MAX_USER_ID)),
            generate_code('CONTENT', random.randint(1, MAX_CONTENT_ID)),
            fake.date_time_between(start_date='-2y', end_date='now'),
            random.randint(5, 180),
            random.choice(PLATFORMS),
            random.choice(DEVICE_TYPES),
            random.choice(OS_FAMILIES),
            random.choice(OS_NAMES),
            random.choice(APP_VERSIONS)
        ])

print("CSV gerado com sucesso para a tabela!")