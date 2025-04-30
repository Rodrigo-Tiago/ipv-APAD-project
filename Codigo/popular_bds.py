import os
import psycopg2
import mysql.connector

ordered_tables_pg1 = [
    'AGE_GROUPS',
    'GENDERS',
    'COUNTRIES',
    'SUBSCRIPTION_STATUS',
    'USERS',
]

ordered_tables_pg2 = [
    'AGE_RESTRICTIONS',
    'DIRECTORS',
    'TYPES',
    'CATEGORIES',
    'CONTENTS',
    'CONTENT_CATEGORIES',
    'AGE_GROUPS',
    'COUNTRIES',
    'GENDERS',
    'SUBSCRIPTION_STATUS',
    'USERS',
    'SESSIONS',
]

ordered_tables_mysql = [
    'AGE_RATINGS',
    'DIRECTORS',
    'TYPES',
    'GENRES',
    'CONTENTS',
    'CONTENT_GENRES',
]

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
            print(f"Erro ao executar: {stmt}\n→ {err}")
    conn.commit()
    cur.close()

def import_postgres(csv_dir, dbname, user, password, port, table_list, script_path):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host='localhost',
        port=port
    )
    cur = conn.cursor()
    print(f"\nLigado ao PostgreSQL: {dbname}")

    print(f"\nCriar tabelas em PostgreSQL: {dbname}")
    executar_script_sql(script_path, conn)

    for table in table_list:
        file = f"{table}.csv"
        path = os.path.join(csv_dir, file).replace('\\', '/')
        print(f"A importar: {table} ← {path}")
        with open(path, 'r', encoding='utf-8') as f:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Importação concluída para PostgreSQL: {dbname}")

def import_mysql(csv_dir, dbname, user, password, port, table_list, script_path):
    conn = mysql.connector.connect(
        database=dbname,
        user=user,
        password=password,
        host='localhost',
        port=port,
        allow_local_infile=True
    )
    cur = conn.cursor()
    print(f"\nLigado ao MySQL: {dbname}")

    print(f"\nCriar tabelas em MySQL: {dbname}")
    executar_script_sql(script_path, conn)

    for table in table_list:
        file = f"{table}.csv"
        path = os.path.abspath(os.path.join(csv_dir, file)).replace('\\', '/')
        print(f"A importar: {table} ← {path}")
        cur.execute(f"""
            LOAD DATA LOCAL INFILE '{path}'
            INTO TABLE {table}
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '\"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
        """)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Importação concluída para MySQL: {dbname}")

# Caminhos relativos ao repositório
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_pg1 = os.path.join(base_dir, 'geracao_dados/csv_postgresql1')
csv_pg2 = os.path.join(base_dir, 'geracao_dados/csv_postgresql2')
csv_mysql = os.path.join(base_dir, 'geracao_dados/csv_mysql')

sql_pg1 = os.path.join(base_dir, 'scripts_sql_tabelas/PostgreSQL1.sql')
sql_pg2 = os.path.join(base_dir, 'scripts_sql_tabelas/PostgreSQL2.sql')
sql_mysql = os.path.join(base_dir, 'scripts_sql_tabelas/MySQL.sql')

# Executar importações
import_postgres(csv_pg1, dbname='db1', user='user1', password='password1', port=5434, table_list=ordered_tables_pg1, script_path=sql_pg1)
import_postgres(csv_pg2, dbname='db2', user='user2', password='password2', port=5435, table_list=ordered_tables_pg2, script_path=sql_pg2)
import_mysql(csv_mysql, dbname='db3', user='user3', password='password3', port=3307, table_list=ordered_tables_mysql, script_path=sql_mysql)