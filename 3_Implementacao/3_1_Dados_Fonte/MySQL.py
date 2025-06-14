import os
import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker()
NUM_CONTENTS = 1000  # Conteúdos a gerar

# Diretório de saída dinâmico
output_dir = os.path.join(os.path.dirname(__file__), '3_1_2_MySQL')
os.makedirs(output_dir, exist_ok=True)
os.chdir(output_dir)

# Valores fixos
GENRES_LIST = ['Action', 'Comedy', 'Drama', 'Thriller', 'Horror', 'Romance', 'Documentary', 'Animation', 'Sci-Fi', 'Fantasy']
TYPES_LIST = ['Movie', 'Series', 'Short Film', 'Documentary', 'Special']
AGE_RATINGS_LIST = ['G', 'PG', 'PG-13', 'R', 'NC-17']

# Função para gerar códigos
def generate_code(prefix, num, total_length=16):
    num_str = str(num).zfill(total_length - len(prefix) - 1)  # -1 para o underscore
    return f"{prefix}_{num_str}"

# AGE_RATINGS
with open('AGE_RATINGS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['AGE_RATING_ID', 'AGE_RATING_DESIGNATION'])
    for idx, rating in enumerate(AGE_RATINGS_LIST, 1):
        writer.writerow([idx, rating])

# GENRES
with open('GENRES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['GENRE_ID', 'GENRE_DESIGNATION'])
    for idx, genre in enumerate(GENRES_LIST, 1):
        writer.writerow([idx, genre])

# TYPES
with open('TYPES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['TYPE_ID', 'TYPE_DESIGNATION'])
    for idx, t in enumerate(TYPES_LIST, 1):
        writer.writerow([idx, t])

# DIRECTORS
with open('DIRECTORS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DIRECTOR_ID', 'NAME'])
    for i in range(1, NUM_CONTENTS + 1):
        writer.writerow([i, fake.name()])

# CONTENTS
with open('CONTENTS.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['CONTENT_CODE', 'TYPE_ID', 'AGE_RATING_ID', 'DIRECTOR_ID', 'TITLE', 'RELEASE_DATE', 'DURATION'])
    for i in range(1, NUM_CONTENTS + 1):
        writer.writerow([
            generate_code('CONTENT', i),
            random.randint(1, len(TYPES_LIST)),
            random.randint(1, len(AGE_RATINGS_LIST)),
            random.randint(1, NUM_CONTENTS),
            fake.sentence(nb_words=4),
            fake.date_between(start_date='-10y', end_date='today'),
            180 #random.randint(60, 180)
        ])

# CONTENT_GENRES
with open('CONTENT_GENRES.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['GENRE_ID', 'CONTENT_CODE'])
    for i in range(1, NUM_CONTENTS + 1):
        content_code = generate_code('CONTENT', i)
        genre_ids = random.sample(range(1, len(GENRES_LIST) + 1), k=random.randint(1, 3))
        for genre_id in genre_ids:
            writer.writerow([genre_id, content_code])

print("CSVs gerados com sucesso para todas as tabelas!")