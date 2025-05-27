import os
import pandas as pd
import numpy as np
import mysql.connector
import pyodbc
import csv
from datetime import datetime

# Caminhos relativos ao repositório
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, '..', '3_3_DM')
os.makedirs(output_dir, exist_ok=True)
csv_dimensional_model = os.path.join(base_dir, '..', '3_3_DM') # Dados
sql_dimensional_model = os.path.join(base_dir, 'scripts_sql/DimensionalModel.sql') # Tabelas

ordered_tables_dimensional_model = [
    'CONTENTS',
    'DEVICES',
    'TIMES',
    'USERS',
    'SESSIONS',
]

# Criar a BD no SQL Server
def executar_script_sql(path_script, conn):
    with open(path_script, 'r', encoding='utf-8') as f:
        sql = f.read()

    cur = conn.cursor()
    # Separar os comandos (cada um deve terminar com ;)
    statements = [stmt.strip() + ';' for stmt in sql.split(';') if stmt.strip()]
    for stmt in statements:
        try:
            cur.execute(stmt)
        except mysql.connector.Error as err:
            print(f"Erro ao executar: {stmt}\n {err}")
    conn.commit()
    cur.close()

def create_sqlserver_db(server, user, password, dbname, script_path):
    conn_str_master = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE=master;"
        f"UID={user};"
        f"PWD={password}"
    )
    conn_master = pyodbc.connect(conn_str_master, autocommit=True)
    cursor_master = conn_master.cursor()

    # Verificar se a BD já existe
    cursor_master.execute(f"SELECT DB_ID(N'{dbname}')")
    db_exists = cursor_master.fetchone()[0] is not None

    if not db_exists:
        # Criar a BD
        cursor_master.execute(f"CREATE DATABASE {dbname}")
        print(f"Base de dados '{dbname}' criada com sucesso.")
        cursor_master.close()
        conn_master.close()

        # Criar as tabelas
        conn_str_target = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={dbname};"
            f"UID={user};"
            f"PWD={password}"
        )
        conn_target = pyodbc.connect(conn_str_target, autocommit=True)
        print(f"Criar tabelas na BD: {dbname}")
        executar_script_sql(script_path, conn_target)
        conn_target.close()
    else:
        cursor_master.close()
        conn_master.close()

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
    genres = [g.strip() for g in genres_str.split(';')]
    mapped_genres = [map_genres(g) for g in genres]
    # Remove duplicados e junta de novo
    return ';'.join(sorted(set(mapped_genres)))

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

# --- USERS ---
def process_users():
    # Ler os CSVs dos users e das tabelas auxiliares
    df_pg1_users = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_3_PostgreSQL1', 'USERS.csv'))
    df_pg1_users['source'] = 'postgresql1'
    df_pg1_age = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_3_PostgreSQL1', 'AGE_GROUPS.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg1_gender = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_3_PostgreSQL1', 'GENDERS.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg1_country = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_3_PostgreSQL1', 'COUNTRIES.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg1_subs = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_3_PostgreSQL1', 'SUBSCRIPTION_STATUS.csv')).drop(columns=['is_up_to_date'], errors='ignore')

    df_pg2_users = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'USERS.csv'))
    df_pg2_users['source'] = 'postgresql2'
    df_pg2_age = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'AGE_GROUPS.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg2_gender = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'GENDERS.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg2_country = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'COUNTRIES.csv')).drop(columns=['is_up_to_date'], errors='ignore')
    df_pg2_subs = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'SUBSCRIPTION_STATUS.csv')).drop(columns=['is_up_to_date'], errors='ignore')

    def expand_users(df_users, df_age, df_gender, df_country, df_subs):
        df = df_users.copy()

        # Fazer merge para trazer os valores descritivos para os IDs
        df = df.merge(df_age, left_on='age_group_id', right_on='age_group_id', how='left')
        df = df.merge(df_gender, left_on='gender_id', right_on='gender_id', how='left')
        df = df.merge(df_country, left_on='country_id', right_on='country_id', how='left')
        df = df.merge(df_subs, left_on='subscription_status_id', right_on='subscription_status_id', how='left')

        # Normalizar os valores
        df['gender'] = df['gender_designation'].apply(map_genders)
        df['subscription_status'] = df['subscription_status_designation'].apply(map_subscription_status)

        df.rename(columns={
            'age_group_designation': 'age_group',
            'country_designation': 'country',
        }, inplace=True)

        return df

    df_pg1_expanded = expand_users(df_pg1_users, df_pg1_age, df_pg1_gender, df_pg1_country, df_pg1_subs)
    df_pg2_expanded = expand_users(df_pg2_users, df_pg2_age, df_pg2_gender, df_pg2_country, df_pg2_subs)

    # Concatenar e limpar duplicados
    df_users = pd.concat([df_pg1_expanded, df_pg2_expanded], ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Ordenar para garantir ordem estável e determinística
    df_users.sort_values(by=['user_code', 'source'], inplace=True)

    # Gerar USER_ID sequencial após ordenação
    df_users['user_id'] = range(1, len(df_users) + 1)

    # Selecionar colunas finais para o CSV
    df_users_final = df_users[['user_id', 'user_code', 'source', 'name', 'age_group', 'gender', 'signup_date', 'subscription_status', 'country', 'district', 'city', 'postal_code', 'street_address', 'is_up_to_date']]

    output_path = os.path.join(output_dir, 'USERS.csv')
    df_users_final.to_csv(output_path, index=False)
    print(f"USERS.csv criado em {output_path}")

    return df_users_final

# --- DEVICES ---
def process_devices():
    # Ler os CSVs dos devices
    df_pg2_sessions = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'SESSIONS.csv'))
    
    df_csv_sessions = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_1_CSV', 'SESSIONS.csv'))
    df_csv_sessions.columns = df_csv_sessions.columns.str.lower() # Colocar as colunas em minúsculas

    def expand_devices(df_devices):
        df = df_devices.copy()

        # Normalizar os valores
        df['app_version'] = df['app_version'].apply(map_app_versions)

        return df

    df_pg2_expanded = expand_devices(df_pg2_sessions)
    df_csv_expanded = expand_devices(df_csv_sessions)

    # Concatenar e limpar duplicados
    df_devices = pd.concat([df_pg2_expanded, df_csv_expanded], ignore_index=True).drop_duplicates(subset=['platform', 'device_type', 'os_family', 'os_name', 'app_version']).reset_index(drop=True)

    # Ordenar para garantir ordem estável e determinística
    df_devices.sort_values(by=['platform', 'device_type', 'os_family', 'os_name', 'app_version'], inplace=True)

    # Gerar DEVICE_ID sequencial após ordenação
    df_devices['device_id'] = range(1, len(df_devices) + 1)

    # Selecionar colunas finais para o CSV
    df_devices_final = df_devices[['device_id', 'platform', 'device_type', 'os_family', 'os_name', 'app_version', 'is_up_to_date']]

    output_path = os.path.join(output_dir, 'DEVICES.csv')
    df_devices_final.to_csv(output_path, index=False)
    print(f"DEVICES.csv criado em {output_path}")

    return df_devices_final

# --- CONTENTS ---
def process_contents():
    # Ler os CSVs dos contents e das tabelas auxiliares
    df_pg2_contents = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'CONTENTS.csv'))
    df_pg2_contents['source'] = 'postgresql2'
    df_pg2_categories = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'CATEGORIES.csv'))
    df_pg2_types = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'TYPES.csv'))
    df_pg2_age_restrictions = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'AGE_RESTRICTIONS.csv'))
    df_pg2_directors = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'DIRECTORS.csv'))
    df_pg2_content_categories = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'CONTENT_CATEGORIES.csv'))

    df_mysql_contents = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'CONTENTS.csv'))
    df_mysql_contents.columns = df_mysql_contents.columns.str.lower()
    df_mysql_contents['source'] = 'postgresql1'
    df_mysql_genres = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'GENRES.csv'))
    df_mysql_genres.columns = df_mysql_genres.columns.str.lower()
    df_mysql_types = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'TYPES.csv'))
    df_mysql_types.columns = df_mysql_types.columns.str.lower()
    df_mysql_age_ratings = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'AGE_RATINGS.csv'))
    df_mysql_age_ratings.columns = df_mysql_age_ratings.columns.str.lower()
    df_mysql_directors = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'DIRECTORS.csv'))
    df_mysql_directors.columns = df_mysql_directors.columns.str.lower()
    df_mysql_content_genres = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL', 'CONTENT_GENRES.csv'))
    df_mysql_content_genres.columns = df_mysql_content_genres.columns.str.lower()

    def expand_contents(df_contents, df_cat_or_genres, df_content_cat_or_genres, df_types, df_age_ratings, df_directors, is_postgres):
        df = df_contents.copy()

        # Agregar géneros ou categorias numa única string separada por vírgulas
        if is_postgres:
            # PostgreSQL usa CONTENT_CATEGORIES, CATEGORIES e AGE_RESTRICTIONS
            df_cat_names = df_content_cat_or_genres.merge(df_cat_or_genres, left_on='category_id', right_on='category_id', how='left')
            df_agg = df_cat_names.groupby('content_code')['category_designation'].apply(lambda x: ';'.join(sorted(set(x)))).reset_index()
            df = df.merge(df_agg, on='content_code', how='left')
            df = df.merge(df_age_ratings[['age_restriction_id', 'age_restriction_designation']], on='age_restriction_id', how='left')
        else:
            # MySQL usa CONTENT_GENRES, GENRES e AGE_RATINGS
            df_genre_names = df_content_cat_or_genres.merge(df_cat_or_genres, left_on='genre_id', right_on='genre_id', how='left')
            df_agg = df_genre_names.groupby('content_code')['genre_designation'].apply(lambda x: ';'.join(sorted(set(x)))).reset_index()
            df = df.merge(df_agg, on='content_code', how='left')
            df = df.merge(df_age_ratings[['age_rating_id', 'age_rating_designation']], on='age_rating_id', how='left')

        # Fazer merge para trazer os valores descritivos para os IDs
        df = df.merge(df_types, left_on='type_id', right_on='type_id', how='left')
        df = df.merge(df_directors[['director_id', 'name']], on='director_id', how='left')

        # Renomear colunas para nomes padrão no modelo dimensional
        df.rename(columns={
            'category_designation': 'genres',
            'genre_designation': 'genres',
            'age_restriction_designation': 'age_rating',
            'age_rating_designation': 'age_rating',
            'name': 'director',
            'type_designation': 'type',
            'is_up_to_date_x': 'is_up_to_date'
        }, inplace=True)

        # Normalizar os valores
        df['genres'] = df['genres'].apply(normalize_genres)
        df['type'] = df['type'].apply(map_types)

        return df

    df_pg2_expanded = expand_contents(df_pg2_contents, df_pg2_categories, df_pg2_content_categories, df_pg2_types, df_pg2_age_restrictions, df_pg2_directors, is_postgres=True)
    df_mysql_expanded = expand_contents(df_mysql_contents, df_mysql_genres, df_mysql_content_genres, df_mysql_types, df_mysql_age_ratings, df_mysql_directors, is_postgres=False)

    # Concatenar e limpar duplicados
    df_contents = pd.concat([df_pg2_expanded.reset_index(drop=True), df_mysql_expanded.reset_index(drop=True)], ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Ordenar para garantir ordem estável e determinística
    df_contents.sort_values(by=['content_code', 'source'], inplace=True)

    # Gerar CONTENT_ID sequencial após ordenação
    df_contents['content_id'] = range(1, len(df_contents) + 1)

    # Selecionar colunas finais para o CSV
    df_contents_final = df_contents[['content_id', 'content_code', 'source', 'title', 'genres', 'release_date', 'type', 'duration', 'age_rating', 'director', 'is_up_to_date']]

    output_path = os.path.join(output_dir, 'CONTENTS.csv')
    df_contents_final.to_csv(output_path, index=False)
    print(f"CONTENTS.csv criado em {output_path}")

    return df_contents_final

# --- TIMES ---
def process_times(start_date='2023-01-01', end_date='2025-12-31'):
    # Gerar uma série temporal por minuto no intervalo desejado
    times = pd.date_range(start=start_date, end=end_date + ' 23:59', freq='min')

    df_times = pd.DataFrame()
    df_times['datetime'] = times

    # Preencher colunas
    df_times['time_id'] = np.arange(1, len(df_times) + 1)
    df_times['day'] = df_times['datetime'].dt.day
    df_times['week'] = df_times['datetime'].dt.isocalendar().week.astype(int)
    df_times['month'] = df_times['datetime'].dt.month
    df_times['year'] = df_times['datetime'].dt.year
    df_times['hour'] = df_times['datetime'].dt.hour
    df_times['minute'] = df_times['datetime'].dt.minute
    df_times['day_name'] = df_times['datetime'].dt.day_name()
    df_times['month_name'] = df_times['datetime'].dt.month_name()

    # Selecionar colunas finais
    df_times_final = df_times[['time_id', 'day', 'week', 'month', 'year', 'hour', 'minute', 'day_name', 'month_name']]

    # Guardar CSV
    output_path = os.path.join(output_dir, 'TIMES.csv')
    df_times_final.to_csv(output_path, index=False)
    print(f"TIMES.csv criado em {output_path}")

    return df_times_final

# --- SESSIONS ---
def process_sessions():
    # Ler os CSVs das sessions
    df_csv_sessions = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_1_CSV', 'SESSIONS.csv'))
    df_csv_sessions.columns = df_csv_sessions.columns.str.lower()
    df_csv_sessions['source'] = 'postgresql1'
    
    # Atualizar o campo is_up_to_date para 1 no ficheiro original (SESSIONS.csv)
    original_csv_path = os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_1_CSV', 'SESSIONS.csv')
    if os.path.exists(original_csv_path):
        df_original = pd.read_csv(original_csv_path)
        if 'IS_UP_TO_DATE' in df_original.columns:
            df_original['IS_UP_TO_DATE'] = 1
            df_original.to_csv(original_csv_path, index=False)
            print("Campo IS_UP_TO_DATE atualizado para 1 no ficheiro original SESSIONS.csv")
    
    df_pg2_sessions = pd.read_csv(os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_4_PostgreSQL2', 'SESSIONS.csv'))
    df_pg2_sessions['source'] = 'postgresql2'

    # Normalizar os nomes das colunas
    df_pg2_sessions.rename(columns={
        'time_': 'time'
    }, inplace=True)

    # Concatenar sessões
    df_sessions = pd.concat([df_pg2_sessions, df_csv_sessions], ignore_index=True)

    # Normalizar os valores
    df_sessions['app_version'] = df_sessions['app_version'].apply(map_app_versions)

    # Carregar dimensões    
    df_users = pd.read_csv(os.path.join(base_dir, '..', '3_3_DM', 'USERS.csv'))
    df_devices = pd.read_csv(os.path.join(base_dir, '..', '3_3_DM', 'DEVICES.csv'))
    df_contents = pd.read_csv(os.path.join(base_dir, '..', '3_3_DM', 'CONTENTS.csv'))
    df_times = pd.read_csv(os.path.join(base_dir, '..', '3_3_DM', 'TIMES.csv'))

    # Tratar do USER_ID
    df_sessions = df_sessions.merge(df_users[['user_id', 'user_code', 'source']], on=['user_code', 'source'], how='left')

    # Tratar do CONTENT_ID
    df_sessions = df_sessions.merge(df_contents[['content_id', 'content_code', 'source']], on=['content_code', 'source'], how='left')

    # Tratar do DEVICE_ID
    df_sessions = df_sessions.merge(df_devices[['device_id', 'platform', 'device_type', 'os_family', 'os_name', 'app_version']], on=['platform', 'device_type', 'os_family', 'os_name', 'app_version'], how='left')

    # Tratar do TIME_ID
    df_sessions['datetime'] = pd.to_datetime(df_sessions['time'], errors='coerce')
    df_sessions['day'] = df_sessions['datetime'].dt.day
    df_sessions['month'] = df_sessions['datetime'].dt.month
    df_sessions['year'] = df_sessions['datetime'].dt.year
    df_sessions['hour'] = df_sessions['datetime'].dt.hour
    df_sessions['minute'] = df_sessions['datetime'].dt.minute

    df_sessions = df_sessions.merge(df_times[['time_id', 'day', 'month', 'year', 'hour', 'minute']], on=['day', 'month', 'year', 'hour', 'minute'], how='left')

    # Calcular a percentagem assistida
    df_sessions = df_sessions.merge(df_contents[['content_id', 'duration']], on='content_id', how='left')
    
    df_sessions['watched_percent'] = (df_sessions['watched_duration'] / df_sessions['duration']) * 100

    # Limpar duplicados
    df_sessions = df_sessions.drop_duplicates().reset_index(drop=True)

    # Ordenar para garantir ordem estável e determinística
    df_sessions.sort_values(by=['session_code', 'source'], inplace=True)

    # Gerar SESSION_ID sequencial após ordenação
    df_sessions['session_id'] = range(1, len(df_sessions) + 1)

    # Selecionar colunas finais para o CSV
    df_sessions_final = df_sessions[['session_id', 'user_id', 'content_id', 'device_id', 'time_id', 'session_code', 'source', 'watched_duration', 'watched_percent', 'is_up_to_date']]

    output_path = os.path.join(output_dir, 'SESSIONS.csv')
    df_sessions_final.to_csv(output_path, index=False)
    print(f"SESSIONS.csv criado em {output_path}")

    return df_sessions_final

# --- Verificar se a tabela TIMES já existe ---
def should_generate_times(server, dbname, user, password, script_path):
    create_sqlserver_db(server, user, password, dbname, script_path) # pro caso da bd não existir
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={dbname};"
        f"UID={user};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM TIMES")
        result = cursor.fetchone()
        return result[0] == 0  # Só gera se estiver vazia
    except:
        return True
    finally:
        cursor.close()
        conn.close()

# --- Gerar os Dados para dar Upload ---
process_users()
process_devices()
process_contents()

if should_generate_times('localhost,1433', 'DM_DimensionalModel', 'sa', 'Password123!', script_path=sql_dimensional_model):
    process_times()
else:
    print("TIMES já tem dados — geração ignorada.")

process_sessions()

# --- Dar Upload dos Dados para o SQL Server ---
def import_dimensional_model(csv_dir, server, dbname, user, password, table_list):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={dbname};"
        f"UID={user};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str, autocommit=True)
    cur = conn.cursor()

    print(f"\nLigado ao SQL Server: {dbname}")

    scd_type_23_columns = {
        'USERS': ['name', 'age_group', 'gender', 'subscription_status', 'country', 'district', 'city', 'postal_code', 'street_address'],
        'CONTENTS': ['title', 'genres', 'type', 'age_rating'],
    }

    composite_keys = {
        'USERS': ['user_code', 'source'],
        'CONTENTS': ['content_code', 'source'],
        'DEVICES': ['platform', 'device_type', 'os_family', 'os_name', 'app_version'],
        'SESSIONS': ['session_code', 'source'],
    }

    # Importar dados CSV para cada tabela
    for table in table_list:
        file_path = os.path.abspath(os.path.join(csv_dir, f"{table}.csv"))
        imported_count, updated_count = 0, 0

        if table == "TIMES":
            if not should_generate_times(server, dbname, user, password, script_path=sql_dimensional_model):
                print(f"TIMES já contém dados — inserção ignorada.")
                continue
            else:
                # Caminho no container (montado via volume)
                print(f"A importar: {table}")
                container_path = f"/3_3_DM/{table}.csv"
                bulk_query = f"""
                BULK INSERT {table}
                FROM '{container_path}'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\\n',
                    TABLOCK
                );
                """
                try:
                    cur.execute(bulk_query)
                    conn.commit()
                    print(f"BULK INSERT concluído para {table}")
                except Exception as e:
                    print(f"Erro no BULK INSERT para {table}: {e}")
        elif table == "DEVICES":
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                columns = next(reader)
                if 'is_up_to_date' in columns:
                    is_up_to_date_index = columns.index('is_up_to_date')
                    columns.remove('is_up_to_date')

                # Remover DEVICE_ID do INSERT pois é gerado automaticamente
                insert_columns = [col for col in columns if col != 'device_id']

                for raw_values in reader:
                    if raw_values[is_up_to_date_index] != '0':
                        continue

                    values_dict = dict(zip(columns, [v for i, v in enumerate(raw_values) if i != is_up_to_date_index]))

                    key_cols = ['platform', 'device_type', 'os_family', 'os_name', 'app_version']
                    key_values = [values_dict[col] for col in key_cols]
                    where_clause = ' AND '.join([f"{col} = ?" for col in key_cols])

                    cur.execute(f"SELECT device_id FROM DEVICES WHERE {where_clause}", key_values)
                    row = cur.fetchone()

                    if not row:
                        insert_values = [values_dict[col] for col in insert_columns]
                        placeholders = ','.join('?' for _ in insert_columns)
                        insert_sql = f"INSERT INTO DEVICES ({','.join(insert_columns)}) VALUES ({placeholders})"
                        cur.execute(insert_sql, insert_values)
                        imported_count += 1
            print(f"{imported_count} Importados {updated_count} Atualizados para {table}")
        elif table == "SESSIONS":
            # Criar dicionário com os DEVICE_ID reais
            cur.execute("SELECT device_id, platform, device_type, os_family, os_name, app_version FROM DEVICES")
            device_rows = cur.fetchall()
            device_lookup = {
                (r[1], r[2], r[3], r[4], r[5]): r[0]
                for r in device_rows
            }

            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                columns = next(reader)
                if 'is_up_to_date' in columns:
                    is_up_to_date_index = columns.index('is_up_to_date')
                    columns.remove('is_up_to_date')

                device_id_index = columns.index('device_id')

                for raw_values in reader:
                    if raw_values[is_up_to_date_index] != '0':
                        continue

                    # Substituir o device_id temporário pelo real
                    # Primeiro, carregar CSV dos devices para obter os campos identificadores
                    device_csv_path = os.path.join(csv_dir, 'DEVICES.csv')
                    with open(device_csv_path, 'r', encoding='utf-8') as dev_f:
                        dev_reader = csv.reader(dev_f)
                        dev_columns = next(dev_reader)
                        dev_index = {name: idx for idx, name in enumerate(dev_columns)}

                        temp_device_id = raw_values[device_id_index]

                        for dev_row in dev_reader:
                            if dev_row[dev_index['device_id']] == temp_device_id:
                                key = (
                                    dev_row[dev_index['platform']],
                                    dev_row[dev_index['device_type']],
                                    dev_row[dev_index['os_family']],
                                    dev_row[dev_index['os_name']],
                                    dev_row[dev_index['app_version']],
                                )
                                real_device_id = device_lookup.get(key)
                                if real_device_id:
                                    raw_values[device_id_index] = str(real_device_id)
                                break

                    # Processamento normal (igual ao resto das tabelas)
                    values = [v for i, v in enumerate(raw_values) if i != is_up_to_date_index]
                    placeholders = ','.join('?' for _ in values)

                    values_dict = dict(zip(columns, values))
                    key_columns = composite_keys['SESSIONS']
                    key_values = [values_dict[col] for col in key_columns]
                    where_clause = ' AND '.join([f"{col} = ?" for col in key_columns])
                    cur.execute(f"SELECT COUNT(*) FROM SESSIONS WHERE {where_clause}", key_values)
                    exists = cur.fetchone()[0] > 0

                    if exists:
                        update_columns = [col for col in columns if col not in key_columns]
                        update_clause = ', '.join([f"{col} = ?" for col in update_columns])
                        update_values = [values[columns.index(col)] for col in update_columns] + key_values
                        update_sql = f"UPDATE SESSIONS SET {update_clause} WHERE {where_clause}"
                        cur.execute(update_sql, update_values)
                        updated_count += 1
                    else:
                        insert_sql = f"INSERT INTO SESSIONS ({','.join(columns)}) VALUES ({placeholders})"
                        cur.execute(insert_sql, values)
                        imported_count += 1

            print(f"{imported_count} Importados {updated_count} Atualizados para {table}")
        else: # USERS e CONTENTS
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                columns = next(reader)
                if 'is_up_to_date' in columns:
                    is_up_to_date_index = columns.index('is_up_to_date')
                    columns.remove('is_up_to_date')

                scd_cols = scd_type_23_columns.get(table, [])
                key_columns = composite_keys.get(table)

                for raw_values in reader:
                    if raw_values[is_up_to_date_index] != '0':
                        continue  # Ignorar registos que já estão atualizados

                    values = [v for i, v in enumerate(raw_values) if i != is_up_to_date_index]
                    values_dict = dict(zip(columns, values))

                    if not key_columns or not all(col in columns for col in key_columns):
                        print(f"Chave composta inválida ou incompleta para a tabela {table}")
                        continue

                    key_values = [values_dict[col] for col in key_columns]
                    where_clause = ' AND '.join([f"{col} = ?" for col in key_columns])
                    cur.execute(f"SELECT * FROM {table} WHERE {where_clause} AND ACTIVE = 1", key_values)
                    row = cur.fetchone()

                    pk_col = 'user_id' if table == 'USERS' else 'content_id'

                    if row: # Se o registo já existe
                        db_columns = [desc[0] for desc in cur.description]
                        db_row_dict = dict(zip(db_columns, row))

                        # Verificar alterações tipo 2.3 (que implicam novo registo)
                        has_scd23_change = any(values_dict[col] != str(db_row_dict.get(col, '')) for col in scd_cols)

                        if has_scd23_change:
                            # Inativar registo atual
                            cur.execute(
                                f"UPDATE {table} SET ACTIVE = 0, FINAL_DATE = ? WHERE {where_clause} AND ACTIVE = 1",
                                [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + key_values
                            )

                            # Preparar colunas e valores para novo registo (com atualizações tipo 1 e 2.3)
                            insert_columns = [col for col in columns if col != pk_col] + ['ACTIVE', 'INITIAL_DATE', 'FINAL_DATE']
                            insert_values = [values_dict[col] for col in columns if col != pk_col] + ['1', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None]
                            placeholders = ','.join('?' for _ in insert_values)

                            insert_sql = f"INSERT INTO {table} ({','.join(insert_columns)}) VALUES ({placeholders})"
                            cur.execute(insert_sql, insert_values)
                            imported_count += 1
                        else:
                            # Atualizações apenas de campos tipo 1 (no mesmo registo ativo)
                            update_cols = [col for col in columns if col not in scd_cols + key_columns and values_dict[col] != str(db_row_dict.get(col, ''))]

                            if update_cols:
                                update_clause = ', '.join([f"{col} = ?" for col in update_cols])
                                update_values = [values_dict[col] for col in update_cols] + key_values
                                update_sql = f"UPDATE {table} SET {update_clause} WHERE {where_clause} AND ACTIVE = 1"
                                cur.execute(update_sql, update_values)
                            updated_count += 1
                    else:
                        insert_columns = [col for col in columns if col != pk_col] + ['ACTIVE', 'INITIAL_DATE', 'FINAL_DATE']
                        insert_values = [values_dict[col] for col in columns if col != pk_col] + ['1', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None]
                        placeholders = ','.join('?' for _ in insert_values)
                        insert_sql = f"INSERT INTO {table} ({','.join(insert_columns)}) VALUES ({placeholders})"
                        cur.execute(insert_sql, insert_values)
                        imported_count += 1
                print(f"{imported_count} Importados {updated_count} Atualizados para {table}")

    cur.close()
    conn.close()
    print(f"Importação concluída para SQL Server: {dbname}")

# Executar importações
import_dimensional_model(csv_dir=csv_dimensional_model, server='localhost,1433', dbname='DM_DimensionalModel', user='sa', password='Password123!', table_list=ordered_tables_dimensional_model)