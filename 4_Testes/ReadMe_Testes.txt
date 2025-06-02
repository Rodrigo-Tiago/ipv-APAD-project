ReadMe - Testes de Validação

Este diretório inclui os artefactos relacionados com os testes de validação do sistema. O objetivo destes testes é assegurar que o comportamento do processo de ETL está conforme o esperado quando ocorrem inserções e atualizações nos dados dos sistemas operacionais.

➡ Pasta "scripts_alterar_dados/"

Contém os scripts SQL com comandos de INSERT e UPDATE preparados para introduzir alterações nos dados dos sistemas operacionais MySQL, PostgreSQL1 e PostgreSQL2.

➡ alter_os_data.py

Script Python responsável por executar os comandos dos ficheiros .sql em cada um dos sistemas operacionais, aplicando as alterações de dados.
Após executar esta script, é necessário voltar a correr o processo de ETL ("extract_os_data.py" e "transform_load_os_data.py"), de forma a que as alterações efetuadas sejam refletidas no modelo dimensional.

➡ Testes_Validacao.pdf

Documento que detalha os testes realizados, com capturas de antes e depois das alterações. Validação dos dados carregados no modelo dimensional recorrendo ao SQL Server Management Studio.