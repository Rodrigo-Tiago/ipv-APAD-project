import os
import csv
import mysql.connector
import psycopg2

# Caminho base
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, 'data_generation/csv_csv', 'SESSIONS.csv')

# Ler o CSV atual
with open(csv_path, 'r', newline='') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Adicionar um novo registo
new_row = {
    'SESSION_CODE': 'SESSION_00000101',
    'USER_CODE': 'USER_00000000001',
    'CONTENT_CODE': 'CONTENT_00000022',
    'TIME': '2024-05-20 14:30:00',
    'WATCHED_DURATION': '120',
    'PLATFORM': 'TV',
    'DEVICE_TYPE': 'Smart TV',
    'OS_FAMILY': 'tvOS',
    'OS_NAME': 'tvOS 14',
    'APP_VERSION': 'version 2.1.0',
    'IS_UP_TO_DATE': '0'
}
rows.append(new_row)
print(f"Novo registo adicionado")

# Atualizar registo existente
for row in rows:
    if row['SESSION_CODE'] == 'SESSION_00000001':
        row.update({
            'USER_CODE': 'USER_00000000001',
            'CONTENT_CODE': 'CONTENT_00000022',
            'TIME': '2024-05-21 10:00:00',
            'WATCHED_DURATION': '95',
            'PLATFORM': 'Computer',
            'DEVICE_TYPE': 'Laptop',
            'OS_FAMILY': 'Windows',
            'OS_NAME': 'Windows 11',
            'APP_VERSION': 'v2.1.0',
            'IS_UP_TO_DATE': '0'
        })
        print("Registo SESSION_00000001 atualizado.")
        break

# Reescrever o CSV
with open(csv_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# Scripts a executar
sql_dir = os.path.join(base_dir, 'sql_scripts')
scripts = {
    'mysql': os.path.join(sql_dir, 'Alter_MySQL_Data.sql'),
    'pg1': os.path.join(sql_dir, 'Alter_PostgreSQL1_Data.sql'),
    'pg2': os.path.join(sql_dir, 'Alter_PostgreSQL2_Data.sql')
}

# Função para executar script MySQL
def executar_sql_mysql(path_script):
    print(f"A executar script: {os.path.basename(path_script)}")
    conn = mysql.connector.connect(
        host='localhost',
        port=3307,
        user='user3',
        password='password3',
        database='db3'
    )
    cursor = conn.cursor()
    with open(path_script, 'r', encoding='utf-8') as f:
        sql = f.read()

    statements = sql.split(";\n")
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print(f"Erro ao executar: {stmt}\n→ {err}")

    conn.commit()
    cursor.close()
    conn.close()
    print("→ Script MySQL executado.\n")

# Função para executar script PostgreSQL
def executar_sql_postgres(path_script, dbname, user, port):
    print(f"A executar script: {os.path.basename(path_script)}")
    with open(path_script, 'r', encoding='utf-8') as f:
        sql = f.read()
    conn = psycopg2.connect(
        host='localhost',
        port=port,
        user=user,
        password=f'password{user[-1]}',  # password1 ou password2
        dbname=dbname
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        print(f"Erro ao executar script PostgreSQL ({dbname}): {e}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"→ Script PostgreSQL ({dbname}) executado.\n")

# Executar os scripts
executar_sql_mysql(scripts['mysql'])
executar_sql_postgres(scripts['pg1'], dbname='db1', user='user1', port=5434)
executar_sql_postgres(scripts['pg2'], dbname='db2', user='user2', port=5435)