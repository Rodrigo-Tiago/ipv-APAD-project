import os
import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker()
NUM_RECORDS = 1000  # Registos a gerar por tabela

# Definir o diretório para os arquivos CSV
output_dir = os.path.join(os.path.dirname(__file__), '3_1_4_PostgreSQL2')
os.makedirs(output_dir, exist_ok=True)
os.chdir(output_dir)

# Valores fixos
AGE_GROUPS_LIST = ['0-9', '10-14', '15-19', '20-24', '25-34', '35-44', '45-54', '55-64', '65+']
GENDERS_LIST = ['Man', 'Woman', 'Prefer not to say']
COUNTRIES_LIST = ['Portugal', 'Spain', 'France', 'Germany', 'Italy', 'Netherlands', 'United Kingdom', 'United States', 'Brazil', 'Canada', 'Venezuela']
SUBSCRIPTION_STATUS_LIST = ['Subscribed', 'Terminated', 'Lapsed']

CATEGORIES_LIST = ['Adventure', 'Humor', 'Melodrama', 'Suspense', 'Terror', 'Love Story', 'Nonfiction', 'Cartoon', 'Science Fiction', 'Fiction']
TYPES_LIST = ['Movie', 'TV Show', 'Mini Movie', 'Docuseries', 'One-Off']
AGE_RESTRICTIONS_LIST = ['6+', '12+', '16+', '18+']

PLATFORMS = ['Mobile', 'Computer', 'TV', 'Tablet', 'Console']
DEVICE_TYPES = ['Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Smart TV', 'Game Console']
OS_FAMILIES = ['Windows', 'macOS', 'Linux', 'Android', 'iOS', 'tvOS', 'FireOS', 'Playstation', 'Nintendo', 'Xbox']
OS_NAMES = ['Android 13', 'iOS 16', 'iPadOS 15', 'FireOS 6', 'tvOS 14', 'Windows 11', 'macOS Ventura', 'Ubuntu 22.04', 'PS5', 'Switch', 'Xbox']
APP_VERSIONS = ['version 1.0.0', 'version 1.2.3', 'version 2.0.1', 'version 2.1.0', 'version 3.0.5']

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

# CATEGORIES
with open('CATEGORIES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['CATEGORY_ID', 'CATEGORY_DESIGNATION'])
    for idx, category in enumerate(CATEGORIES_LIST, 1):
        writer.writerow([idx, category])

# TYPES
with open('TYPES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['TYPE_ID', 'TYPE_DESIGNATION'])
    for idx, t in enumerate(TYPES_LIST, 1):
        writer.writerow([idx, t])

# AGE_RESTRICTIONS
with open('AGE_RESTRICTIONS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['AGE_RESTRICTION_ID', 'AGE_RESTRICTION_DESIGNATION'])
    for idx, ar in enumerate(AGE_RESTRICTIONS_LIST, 1):
        writer.writerow([idx, ar])

# DIRECTORS
with open('DIRECTORS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DIRECTOR_ID', 'NAME'])
    for i in range(1, NUM_RECORDS + 1):
        writer.writerow([i, fake.name()])

# USERS
with open('USERS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['USER_CODE', 'AGE_GROUP_ID', 'GENDER_ID', 'COUNTRY_ID', 'SUBSCRIPTION_STATUS_ID', 'NAME', 'EMAIL', 'SIGNUP_DATE', 'DISTRICT', 'CITY', 'POSTAL_CODE', 'STREET_ADDRESS'])
    for i in range(1, NUM_RECORDS + 1):
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

# CONTENTS
with open('CONTENTS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['CONTENT_CODE', 'TYPE_ID', 'AGE_RESTRICTION_ID', 'DIRECTOR_ID', 'TITLE', 'RELEASE_DATE', 'DURATION'])
    for i in range(1, NUM_RECORDS + 1):
        writer.writerow([
            generate_code('CONTENT', i),
            random.randint(1, len(TYPES_LIST)),
            random.randint(1, len(AGE_RESTRICTIONS_LIST)),
            random.randint(1, NUM_RECORDS),
            fake.sentence(nb_words=3),
            fake.date_between(start_date='-10y', end_date='today'),
            180 #random.randint(60, 180)
        ])

with open('CONTENT_CATEGORIES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['CATEGORY_ID', 'CONTENT_CODE'])
    for i in range(1, NUM_RECORDS + 1):
        content_code = generate_code('CONTENT', i)
        category_ids = random.sample(range(1, len(CATEGORIES_LIST) + 1), k=random.randint(1, 3))
        for category_id in category_ids:
            writer.writerow([category_id, content_code])

# SESSIONS
with open('SESSIONS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['SESSION_CODE', 'USER_CODE', 'CONTENT_CODE', 'TIME_', 'WATCHED_DURATION', 'PLATFORM', 'DEVICE_TYPE', 'OS_FAMILY', 'OS_NAME', 'APP_VERSION'])
    for i in range(1, NUM_RECORDS + 1):
        writer.writerow([
            generate_code('SESSION', i),
            generate_code('USER', random.randint(1, NUM_RECORDS)),
            generate_code('CONTENT', random.randint(1, NUM_RECORDS)),
            fake.date_time_between(start_date='-2y', end_date='now'),
            random.randint(10, 180),
            random.choice(PLATFORMS),
            random.choice(DEVICE_TYPES),
            random.choice(OS_FAMILIES),
            random.choice(OS_NAMES),
            random.choice(APP_VERSIONS)
        ])

print("CSVs gerados com sucesso para todas as tabelas!")