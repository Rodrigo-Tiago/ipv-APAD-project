import os
import mysql.connector
import pyodbc

ordered_tables_dimensional_model = [
    'CONTENTS',
    'DEVICES',
    'TIMES',
    'USERS',
    'SESSIONS',
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

def create_sqlserver_db(server, user, password, dbname):
    conn_str_master = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE=master;"
        f"UID={user};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str_master, autocommit=True)
    cursor = conn.cursor()
    
    cursor.execute(f"""
    IF DB_ID(N'{dbname}') IS NULL
    BEGIN
        CREATE DATABASE {dbname};
    END
    """)
    cursor.close()
    conn.close()

def import_dimensional_model(csv_dir, server, dbname, user, password, table_list, script_path):
    print(f"\nA criar a BD do SQL Server, caso necessário...")
    create_sqlserver_db(server, user, password, dbname)

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

    print(f"\nCriar tabelas em SQL Server: {dbname}")
    executar_script_sql(script_path, conn)

    # Importar dados CSV para cada tabela
    for table in table_list:
        file_path = os.path.abspath(os.path.join(csv_dir, f"{table}.csv"))
        print(f"A importar: {table} ← {file_path}")

        if table == "TIMES":
            # Caminho no container (montado via volume)
            container_path = f"/csv_dimensional_model/{table}.csv"
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
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                columns = f.readline().strip().split(',')
                for line in f:
                    values = [v.strip().strip('"') for v in line.strip().split(',')]
                    placeholders = ','.join('?' for _ in values)
                    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                    try:
                        cur.execute(query, values)
                    except Exception as e:
                        print(f"Erro ao importar para {table}: {e}")

    cur.close()
    conn.close()
    print(f"Importação concluída para SQL Server: {dbname}")

# Caminhos relativos ao repositório
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_dimensional_model = os.path.join(base_dir, 'data_generation/csv_dimensional_model')
sql_dimensional_model = os.path.join(base_dir, 'sql_scripts/DimensionalModel.sql')

# Executar importações
import_dimensional_model(csv_dir=csv_dimensional_model, server='localhost,1433', dbname='DM_DimensionalModel', user='sa', password='Password123!', table_list=ordered_tables_dimensional_model, script_path=sql_dimensional_model)