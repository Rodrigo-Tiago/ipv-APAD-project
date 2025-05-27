import os
import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker()
NUM_USERS = 100  # Utilizadores a gerar

# Diretório de saída dinâmico
output_dir = os.path.join(os.path.dirname(__file__), 'csv_postgresql1')
os.makedirs(output_dir, exist_ok=True)
os.chdir(output_dir)

# Valores fixos
AGE_GROUPS_LIST = ['0-9', '10-14', '15-19', '20-24', '25-34', '35-44', '45-54', '55-64', '65+']
GENDERS_LIST = ['Male', 'Female', 'Other']
COUNTRIES_LIST = ['Portugal', 'Spain', 'France', 'Germany', 'Italy', 'Netherlands', 'United Kingdom', 'United States', 'Brazil', 'Canada', 'Venezuela']
SUBSCRIPTION_STATUS_LIST = ['Active', 'Cancelled', 'Expired']

# Função para gerar códigos
def generate_code(prefix, num, total_length=16):
    num_str = str(num).zfill(total_length - len(prefix) - 1)  # -1 para o underscore
    return f"{prefix}_{num_str}"

# AGE_GROUPS
with open('AGE_GROUPS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['AGE_GROUP_ID', 'AGE_GROUP_DESIGNATION'])
    for idx, group in enumerate(AGE_GROUPS_LIST, 1):
        writer.writerow([idx, group])

# GENDERS
with open('GENDERS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['GENDER_ID', 'GENDER_DESIGNATION'])
    for idx, gender in enumerate(GENDERS_LIST, 1):
        writer.writerow([idx, gender])

# COUNTRIES
with open('COUNTRIES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['COUNTRY_ID', 'COUNTRY_DESIGNATION'])
    for idx, country in enumerate(COUNTRIES_LIST, 1):
        writer.writerow([idx, country])

# SUBSCRIPTION_STATUS
with open('SUBSCRIPTION_STATUS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['SUBSCRIPTION_STATUS_ID', 'SUBSCRIPTION_STATUS_DESIGNATION'])
    for idx, status in enumerate(SUBSCRIPTION_STATUS_LIST, 1):
        writer.writerow([idx, status])

# USERS
with open('USERS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['USER_CODE', 'AGE_GROUP_ID', 'GENDER_ID', 'COUNTRY_ID', 'SUBSCRIPTION_STATUS_ID', 'NAME', 'EMAIL', 'SIGNUP_DATE', 'DISTRICT', 'CITY', 'POSTAL_CODE', 'STREET_ADDRESS'])
    for i in range(1, NUM_USERS + 1):
        writer.writerow([
            generate_code('USER', i),
            random.randint(1, len(AGE_GROUPS_LIST)),
            random.randint(1, len(GENDERS_LIST)),
            random.randint(1, len(COUNTRIES_LIST)),
            random.randint(1, len(SUBSCRIPTION_STATUS_LIST)),
            fake.name(),
            fake.email(),
            fake.date_between(start_date='-5y', end_date='today'),
            fake.state(),
            fake.city(),
            fake.postcode(),
            fake.street_address()
        ])

print("CSVs gerados com sucesso para todas as tabelas!")