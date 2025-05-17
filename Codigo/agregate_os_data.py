import os
import pandas as pd
from datetime import datetime
import numpy as np

base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, 'data_generation/csv_dimensional_model')
os.makedirs(output_dir, exist_ok=True)

# --- Mapeamentos ---
def map_genders(value):
    mapping = {
        'Male': 'Male', 'Man': 'Male',
        'Female': 'Female', 'Woman': 'Female',
        'Other': 'Other', 'Prefer not to say': 'Other',
    }
    return mapping.get(value, 'Unknown')

def map_subscription_status(value):
    mapping = {
        'Active': 'Active', 'Subscribed': 'Active',
        'Cancelled': 'Cancelled', 'Terminated': 'Cancelled',
        'Expired': 'Expired', 'Lapsed': 'Expired',
    }
    return mapping.get(value, 'Unknown')

def map_app_versions(value):
    mapping = {
        'v1.0.0': 'v1.0.0', 'version 1.0.0': 'v1.0.0',
        'v1.2.3': 'v1.2.3', 'version 1.2.3': 'v1.2.3',
        'v2.0.1': 'v2.0.1', 'version 2.0.1': 'v2.0.1',
        'v2.1.0': 'v2.1.0', 'version 2.1.0': 'v2.1.0',
        'v3.0.5': 'v3.0.5', 'version 3.0.5': 'v3.0.5',
    }
    return mapping.get(value, 'Unknown')

def map_genres(value):
    mapping = {
        'Action': 'Action', 'Adventure': 'Action',
        'Comedy': 'Comedy', 'Humor': 'Comedy',
        'Drama': 'Drama', 'Melodrama': 'Drama',
        'Thriller': 'Thriller', 'Suspense': 'Thriller',
        'Horror': 'Horror', 'Terror': 'Horror',
        'Romance': 'Romance', 'Love Story': 'Romance',
        'Documentary': 'Documentary', 'Nonfiction': 'Documentary',
        'Animation': 'Animation', 'Cartoon': 'Animation',
        'Sci-Fi': 'Sci-Fi', 'Science Fiction': 'Sci-Fi',
        'Fantasy': 'Fantasy', 'Fiction': 'Fantasy',
    }
    return mapping.get(value, 'Unknown')

def normalize_genres(genres_str):
    if pd.isna(genres_str):
        return 'Unknown'
    genres = [g.strip() for g in genres_str.split(',')]
    mapped_genres = [map_genres(g) for g in genres]
    # Remove duplicados e junta de novo
    return ','.join(sorted(set(mapped_genres)))

def map_types(value):
    mapping = {
        'Movie': 'Movie',
        'Series': 'Series', 'TV Show': 'Series',
        'Short Film': 'Short Film', 'Mini Movie': 'Short Film',
        'Documentary': 'Documentary', 'Docuseries': 'Documentary',
        'Special': 'Special', 'One-Off': 'Special',
    }
    return mapping.get(value, 'Unknown')

def map_age_ratings(value):
    mapping = {
        'G': 'G',
        'PG': 'PG', '6+': 'PG',
        'PG-13': 'PG-13', ' 12+': 'PG-13',
        'R': 'R', '16+': 'R',
        'NC-17': 'NC-17', '18+': 'NC-17',
    }
    return mapping.get(value, 'Unknown')

def map_age_groups(value):
    valid_groups = ['0-9', '10-14', '15-19', '20-24', '25-34', '35-44', '45-54', '55-64', '65+']
    return value if value in valid_groups else 'Unknown'

def map_countries(value):
    valid_countries = ['Portugal', 'Spain', 'France', 'Germany', 'Italy', 'Netherlands', 'United Kingdom', 'United States', 'Brazil', 'Canada', 'Venezuela']
    return value if value in valid_countries else 'Unknown'

def map_device_types(value):
    valid_devices = ['Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Smart TV', 'Game Console']
    return value if value in valid_devices else 'Unknown'

def map_os_names(value):
    valid_devices = ['Windows', 'macOS', 'Linux', 'Tablet', 'Android', 'iOS', 'tvOS', 'FireOS']
    return value if value in valid_devices else 'Unknown'

# --- USERS ---
def process_users():
    # Ler os CSVs dos users e das tabelas auxiliares
    df_pg1_users = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql1/USERS.csv'))
    df_pg1_age = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql1/AGE_GROUPS.csv'))
    df_pg1_gender = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql1/GENDERS.csv'))
    df_pg1_country = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql1/COUNTRIES.csv'))
    df_pg1_subs = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql1/SUBSCRIPTION_STATUS.csv'))

    df_pg2_users = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/USERS.csv'))
    df_pg2_age = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/AGE_GROUPS.csv'))
    df_pg2_gender = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/GENDERS.csv'))
    df_pg2_country = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/COUNTRIES.csv'))
    df_pg2_subs = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/SUBSCRIPTION_STATUS.csv'))

    def expand_users(df_users, df_age, df_gender, df_country, df_subs):
        df = df_users.copy()

        # Fazer merge para trazer os valores descritivos para os IDs
        df = df.merge(df_age, left_on='AGE_GROUP_ID', right_on='AGE_GROUP_ID', how='left')
        df = df.merge(df_gender, left_on='GENDER_ID', right_on='GENDER_ID', how='left')
        df = df.merge(df_country, left_on='COUNTRY_ID', right_on='COUNTRY_ID', how='left')
        df = df.merge(df_subs, left_on='SUBSCRIPTION_STATUS_ID', right_on='SUBSCRIPTION_STATUS_ID', how='left')

        # Normalizar os valores
        df['GENDER'] = df['GENDER_DESIGNATION'].apply(map_genders)
        df['SUBSCRIPTION_STATUS'] = df['SUBSCRIPTION_STATUS_DESIGNATION'].apply(map_subscription_status)
        df['AGE_GROUP'] = df['AGE_GROUP_DESIGNATION'].apply(map_age_groups)
        df['COUNTRY'] = df['COUNTRY_DESIGNATION'].apply(map_countries)

        return df

    df_pg1_expanded = expand_users(df_pg1_users, df_pg1_age, df_pg1_gender, df_pg1_country, df_pg1_subs)
    df_pg2_expanded = expand_users(df_pg2_users, df_pg2_age, df_pg2_gender, df_pg2_country, df_pg2_subs)

    # Concatenar e limpar duplicados
    df_users = pd.concat([df_pg1_expanded, df_pg2_expanded], ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Gerar USER_ID sequencial
    df_users['USER_ID'] = df_users.index + 1

    # Selecionar colunas finais para o CSV
    df_users_final = df_users[['USER_ID', 'NAME', 'AGE_GROUP', 'GENDER', 'COUNTRY', 'SIGNUP_DATE', 'SUBSCRIPTION_STATUS']]

    output_path = os.path.join(output_dir, 'USERS.csv')
    df_users_final.to_csv(output_path, index=False)
    print(f"USERS.csv criado em {output_path}")

    return df_users_final

# --- DEVICES ---
def process_devices():
    # Ler os CSVs dos devices
    df_pg2_sessions = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/SESSIONS.csv'))
    df_csv_sesions = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_csv/SESSIONS.csv'))

    # Normalizar os nomes de colunas
    df_pg2_sessions.rename(columns={
        'OS_NAME_': 'OS_NAME'
    }, inplace=True)

    df_csv_sesions.rename(columns={
        'device_type': 'DEVICE_TYPE',
        'app_version': 'APP_VERSION',
        'os_name': 'OS_NAME',
    }, inplace=True)

    def expand_devices(df_devices):
            df = df_devices.copy()

            # Normalizar os valores
            df['APP_VERSION'] = df['APP_VERSION'].apply(map_app_versions)

            return df
    
    df_pg2_expanded = expand_devices(df_pg2_sessions)
    df_csv_expanded = expand_devices(df_csv_sesions)

    # Concatenar e limpar duplicados
    df_devices = pd.concat([df_pg2_expanded, df_csv_expanded], ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Gerar DEVICE_ID sequencial
    df_devices['DEVICE_ID'] = df_devices.index + 1

    # Selecionar colunas finais para o CSV
    df_devices_final = df_devices[['DEVICE_ID', 'DEVICE_TYPE', 'APP_VERSION', 'OS_NAME']]

    output_path = os.path.join(output_dir, 'DEVICES.csv')
    df_devices_final.to_csv(output_path, index=False)
    print(f"DEVICES.csv criado em {output_path}")

    return df_devices_final

# --- CONTENTS ---
def process_contents():
    # Ler os CSVs dos contents e das tabelas auxiliares
    df_pg2_contents = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/CONTENTS.csv'))
    df_pg2_categories = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/CATEGORIES.csv'))
    df_pg2_types = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/TYPES.csv'))
    df_pg2_age_restrictions = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/AGE_RESTRICTIONS.csv'))
    df_pg2_directors = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/DIRECTORS.csv'))
    df_pg2_content_categories = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_postgresql2/CONTENT_CATEGORIES.csv'))

    df_mysql_contents = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/CONTENTS.csv'))
    df_mysql_genres = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/GENRES.csv'))
    df_mysql_types = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/TYPES.csv'))
    df_mysql_age_ratings = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/AGE_RATINGS.csv'))
    df_mysql_directors = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/DIRECTORS.csv'))
    df_mysql_content_genres = pd.read_csv(os.path.join(base_dir, 'data_generation/csv_mysql/CONTENT_GENRES.csv'))

    def expand_contents(df_contents, df_cat_or_genres, df_content_cat_or_genres, df_types, df_age_ratings, df_directors, is_postgres):
        df = df_contents.copy()

        # Agregar géneros ou categorias numa única string separada por vírgulas
        if is_postgres:
            # PostgreSQL usa CONTENT_CATEGORIES, CATEGORIES e AGE_RESTRICTIONS
            df_cat_names = df_content_cat_or_genres.merge(df_cat_or_genres, left_on='CATEGORY_ID', right_on='CATEGORY_ID', how='left')
            df_agg = df_cat_names.groupby('CONTENT_ID')['CATEGORY_DESIGNATION'].apply(lambda x: ','.join(sorted(set(x)))).reset_index()
            df = df.merge(df_agg, on='CONTENT_ID', how='left')
            df = df.merge(df_age_ratings[['AGE_RESTRICTION_ID', 'AGE_RESTRICTION_DESIGNATION']], on='AGE_RESTRICTION_ID', how='left')
        else:
            # MySQL usa CONTENT_GENRES, GENRES e AGE_RATINGS
            df_genre_names = df_content_cat_or_genres.merge(df_cat_or_genres, left_on='GENRE_ID', right_on='GENRE_ID', how='left')
            df_agg = df_genre_names.groupby('CONTENT_ID')['GENRE_DESIGNATION'].apply(lambda x: ','.join(sorted(set(x)))).reset_index()
            df = df.merge(df_agg, on='CONTENT_ID', how='left')
            df = df.merge(df_age_ratings[['AGE_RATING_ID', 'AGE_RATING_DESIGNATION']], on='AGE_RATING_ID', how='left')

        # Fazer merge para trazer os valores descritivos para os IDs
        df = df.merge(df_types, left_on='TYPE_ID', right_on='TYPE_ID', how='left')
        df = df.merge(df_directors[['DIRECTOR_ID', 'NAME']], on='DIRECTOR_ID', how='left')

        # Renomear colunas para nomes padrão no modelo dimensional
        df.rename(columns={
            'CATEGORY_DESIGNATION': 'GENRES',
            'GENRE_DESIGNATION': 'GENRES',
            'AGE_RESTRICTION_DESIGNATION': 'AGE_RATING',
            'AGE_RATING_DESIGNATION': 'AGE_RATING',
            'NAME': 'DIRECTOR',
            'TYPE_DESIGNATION': 'TYPE'
        }, inplace=True)

        # Normalizar os valores
        df['GENRES'] = df['GENRES'].apply(normalize_genres)
        df['TYPE'] = df['TYPE'].apply(map_types)

        return df

    df_pg2_expanded = expand_contents(df_pg2_contents, df_pg2_categories, df_pg2_content_categories, df_pg2_types, df_pg2_age_restrictions, df_pg2_directors, is_postgres=True)
    df_mysql_expanded = expand_contents(df_mysql_contents, df_mysql_genres, df_mysql_content_genres, df_mysql_types, df_mysql_age_ratings, df_mysql_directors, is_postgres=False)

    # Concatenar e limpar duplicados
    df_contents = pd.concat([df_pg2_expanded.reset_index(drop=True), df_mysql_expanded.reset_index(drop=True)
], ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Gerar CONTENT_ID sequencial
    df_contents['CONTENT_ID'] = df_contents.index + 1

    # Selecionar colunas finais para o CSV
    df_contents_final = df_contents[['CONTENT_ID', 'TITLE', 'GENRES', 'RELEASE_DATE', 'TYPE', 'DURATION', 'AGE_RATING', 'DIRECTOR']]

    output_path = os.path.join(output_dir, 'CONTENTS.csv')
    df_contents_final.to_csv(output_path, index=False)
    print(f"CONTENTS.csv criado em {output_path}")

    return df_contents_final

# --- TIMES ---
def process_times(start_date='2023-01-01', end_date='2025-12-31'):
    # Gerar uma série temporal por minuto no intervalo desejado
    times = pd.date_range(start=start_date, end=end_date + ' 23:59', freq='min')

    df_times = pd.DataFrame()
    df_times['DATETIME'] = times

    # Preencher colunas
    df_times['TIME_ID'] = np.arange(1, len(df_times) + 1)
    df_times['DAY'] = df_times['DATETIME'].dt.day
    df_times['WEEK'] = df_times['DATETIME'].dt.isocalendar().week.astype(int)
    df_times['MONTH'] = df_times['DATETIME'].dt.month
    df_times['YEAR'] = df_times['DATETIME'].dt.year
    df_times['HOUR'] = df_times['DATETIME'].dt.hour
    df_times['MINUTE'] = df_times['DATETIME'].dt.minute
    df_times['DAY_NAME'] = df_times['DATETIME'].dt.day_name()
    df_times['MONTH_NAME'] = df_times['DATETIME'].dt.month_name()

    # Selecionar colunas finais
    df_times_final = df_times[['TIME_ID', 'DAY', 'WEEK', 'MONTH', 'YEAR', 'HOUR', 'MINUTE', 'DAY_NAME', 'MONTH_NAME']]

    # Guardar CSV
    output_path = os.path.join(output_dir, 'TIMES.csv')
    df_times_final.to_csv(output_path, index=False)
    print(f"TIMES.csv criado em {output_path}")

    return df_times_final

# --- SESSIONS ---

# --- Chamar para testar ---
# process_users()
# process_devices()
# process_contents()
# process_times()
# process_sessions()