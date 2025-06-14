import os
import csv
import mysql.connector
import psycopg2
import pandas as pd
import warnings

# Caminho base
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_1_CSV', 'SESSIONS.csv')
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

# Ler dados existentes
with open(csv_path, 'r', newline='') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Verificar se a coluna já existe
if 'IS_UP_TO_DATE' not in fieldnames:
    # Adicionar a coluna
    fieldnames.append('IS_UP_TO_DATE')
    for row in rows:
        row['IS_UP_TO_DATE'] = '0'

    # Guardar o CSV com a nova coluna
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("Coluna IS_UP_TO_DATE adicionada ao CSV.")

    # Caminhos dos scripts
    sql_dir = os.path.join(base_dir, 'scripts_sql')
    scripts = {
        'mysql': os.path.join(sql_dir, 'Alter_MySQL.sql'),
        'pg1': os.path.join(sql_dir, 'Alter_PostgreSQL1.sql'),
        'pg2': os.path.join(sql_dir, 'Alter_PostgreSQL2.sql')
    }

    # Função para executar SQL num ficheiro
    def executar_sql_mysql(path_script):

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

        # Separar comandos manualmente (cuidado com BEGIN...END blocks)
        # Usa delimitador de dois newlines para separar comandos "seguros"
        blocks = sql.split(";\n")
        for block in blocks:
            stmt = block.strip()
            if not stmt:
                continue

            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print(f"Erro ao executar: {stmt}\n {err}")

        conn.commit()
        cursor.close()
        conn.close()
        print("Script MySQL executado.")

    def executar_sql_postgres(script_path, dbname, user, port):
        with open(script_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        conn = psycopg2.connect(
            host='localhost',
            port=port,
            user=user,
            password=f'password{user[-1]}',  # 'password1' ou 'password2'
            dbname=dbname
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Script PostgreSQL ({dbname}) executado.")

    # Executar os scripts
    print("A executar script: Alter_MySQL.sql")
    executar_sql_mysql(scripts['mysql'])

    print("A executar script: Alter_PostgreSQL1.sql")
    executar_sql_postgres(scripts['pg1'], dbname='db1', user='user1', port=5434)

    print("A executar script: Alter_PostgreSQL2.sql")
    executar_sql_postgres(scripts['pg2'], dbname='db2', user='user2', port=5435)

else:
    print("A coluna IS_UP_TO_DATE já existe no CSV. Nenhuma alteração feita.")
    
# Exportar tabelas de MySQL
def exportar_tabelas_mysql():
    print("\n A exportar tabelas do MySQL...")
    conn = mysql.connector.connect(
        host='localhost',
        port=3307,
        user='user3',
        password='password3',
        database='db3'
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tabelas = [row[0] for row in cursor.fetchall()]
    output_dir = os.path.join(base_dir, '..', '3_1_Dados_Fonte', '3_1_2_MySQL')
    os.makedirs(output_dir, exist_ok=True)

    for tabela in tabelas:
        df = pd.read_sql(f"SELECT * FROM {tabela}", conn)
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).str.replace(r'\r\n|\r|\n', '', regex=True)
        df.to_csv(os.path.join(output_dir, f"{tabela.upper()}.csv"), index=False)
        print(f"Exportado: {tabela.upper()}.csv")

        try:
            cursor.execute("SET @DISABLE_IS_UP_TO_DATE_TRIGGER = 1")
            cursor.execute(f"UPDATE {tabela} SET IS_UP_TO_DATE = 1")
            cursor.execute("SET @DISABLE_IS_UP_TO_DATE_TRIGGER = 0")
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Erro ao atualizar IS_UP_TO_DATE em {tabela}: {err}")

    cursor.close()
    conn.close()

# Exportar tabelas de PostgreSQL
def exportar_tabelas_postgres(dbname, user, port, pasta_destino):
    print(f"\nA exportar tabelas do PostgreSQL: {dbname}...")
    conn = psycopg2.connect(
        host='localhost',
        port=port,
        user=user,
        password=f'password{user[-1]}',  # password1 ou password2
        dbname=dbname
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
    """)
    tabelas = [row[0] for row in cursor.fetchall()]
    output_dir = os.path.join(base_dir, '..', '3_1_Dados_Fonte', pasta_destino)
    os.makedirs(output_dir, exist_ok=True)

    for tabela in tabelas:
        df = pd.read_sql(f'SELECT * FROM "{tabela}"', conn)
        df.to_csv(os.path.join(output_dir, f"{tabela.upper()}.csv"), index=False)
        print(f"Exportado: {tabela.upper()}.csv")

        try:
            cursor.execute(f"ALTER TABLE {tabela} DISABLE TRIGGER trg_{tabela}_update")
            cursor.execute(f'UPDATE {tabela} SET is_up_to_date = 1')
            cursor.execute(f"ALTER TABLE {tabela} ENABLE TRIGGER trg_{tabela}_update")
            conn.commit()
        except Exception as err:
            print(f"Erro ao atualizar IS_UP_TO_DATE em {tabela}: {err}")

    cursor.close()
    conn.close()

# Executar exportações
exportar_tabelas_mysql()
exportar_tabelas_postgres(dbname='db1', user='user1', port=5434, pasta_destino='3_1_3_PostgreSQL1')
exportar_tabelas_postgres(dbname='db2', user='user2', port=5435, pasta_destino='3_1_4_PostgreSQL2')