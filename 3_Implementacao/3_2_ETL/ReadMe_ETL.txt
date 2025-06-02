ReadMe - Processo ETL

Este diretório contém os scripts e instruções necessárias para realizar o processo de ETL (Extract, Transform e Load) dos dados dos sistemas operacionais para o modelo dimensional.

➡ Etapas do Processo

1. Iniciar os containers dos sistemas operacionais

Executar o comando: docker compose up --build

Este comando inicializa os containers Docker com os serviços de base de dados MySQL, PostgreSQL1, PostgreSQL2 e SQL Server.


2. Criar as bases de dados e importar os dados

Executar a script: "create_os.py"

Esta script python executa as scripts SQL localizados em "2_Analise/scripts_criar_bd/" para gerar as tabelas de cada sistema operacional e importa os dados .csv previamente gerados na pasta 3_1_Dados_Fonte.


3. Extrair dados dos sistemas operacionais

Executar a script: "extract_os_data.py"

Esta script:

    Adiciona a coluna is_up_to_date às tabelas dos sistemas operacionais (caso ainda não exista). Esta coluna é criada com valor 0 por defeito, significando que o registo ainda não foi processado;

    Executa as scripts em "3_2_ETL/scripts_sql/Alter_*.sql" para adicionar triggers que colocam is_up_to_date = 0 sempre que um registo é atualizado;

    Guarda os dados extraídos em formato .csv, substituindo os anteriores nas pastas de origem (3_1_1_CSV, 3_1_2_MySQL, 3_1_3_PostgreSQL1, 3_1_4_PostgreSQL2).


4. Transformar e carregar dados para o modelo dimensional

Executar a script: "transform_load_os_data.py"

Esta script realiza a transformação dos dados extraídos e a carga dos dados agregados para o modelo dimensional (armazenado no SQL Server), com as seguintes particularidades:

    Caso ainda não tenha acontecido, executa a script em "3_2_ETL/scripts_sql/DimensionalModel.sql" para criar as tabelas do modelo dimensional

    São tratados os mapeamentos de valores distintos entre os sistemas operacionais, uniformizando os dados conforme os valores usados nos sistemas CSV, MySQL e PostgreSQL1;

    É realizado o tratamento de Slowly Changing Dimensions:

        SDC Tipo 0, os valores dos atributos associados à dimensão nunca são alterados;

        SCD tipo 1, apenas para o caso de ter sido cometido um erro, visto alterar por cima;

        SDC Tipo 2.1, cria-se um novo registo na dimensão;

        SCD Tipo 2.3,  cria-se um novo registo com uma nova chave primária, mas com a mesma chave natural para ligar as diferentes versões. Uma flag é incluída para indicar qual o registo mais recente, assim como uma data de validade (data de início e fim).

    Os registos provenientes de PostgreSQL2 recebem a marca source = postgresql2; os provenientes dos outros três sistemas são marcados como postgresql1.

➡ Saída do ETL

Os dados transformados e carregados são guardados na pasta 3_3_DM como ficheiros .csv