ReadMe - Geração de Dados

Este diretório contém os dados fonte utilizados no projeto, organizados por sistema operacional (CSV, MySQL, PostgreSQL1 e PostgreSQL2). 
Os dados são gerados a partir de scripts Python desenvolvidos especificamente para cada sistema.

➡ Scripts de Geração

    CSV.py – Gera os dados correspondentes ao ficheiro CSV

    MySQL.py – Gera os ficheiros .csv correspondentes às tabelas do sistema MySQL

    PostgreSQL1.py – Gera os ficheiros .csv correspondentes às tabelas do sistema PostgreSQL1

    PostgreSQL2.py – Gera os ficheiros .csv correspondentes às tabelas do sistema PostgreSQL2

➡ Estrutura de Pastas

    3_1_1_CSV - Dados do ficheiro CSV

    3_1_2_MySQL → Dados do sistema MySQL

    3_1_3_PostgreSQL1 → Dados do sistema PostgreSQL1

    3_1_4_PostgreSQL2 → Dados do sistema PostgreSQL2

➡ Estado Atual

Até ao momento, apenas foi realizada a geração dos dados em formato .csv. 
Os ficheiros correspondem a cada tabela de cada sistema operacional, mas ainda não foram importados para as respetivas bases de dados.
