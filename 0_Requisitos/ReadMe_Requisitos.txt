ReadMe - Requisitos

Este documento descreve os requisitos necessários para a correta execução do projeto StreamFlix.

➡ Ferramentas e Tecnologias

| Ferramenta / Linguagem                       | Versão Recomendada               | Observações                                                                |
|----------------------------------------------|----------------------------------|----------------------------------------------------------------------------|
| Python                                       | >= 3.10                          | Linguagem principal usada nos scripts de geração e ETL                     |
| Docker / Docker Compose                      | Docker >= 24.0, Compose >= 2.24  | Utilizado para levantar os containers das bases de dados operacionais      |
| Visual Studio 2022                           | Última versão estável            | Necessário para configurar o cubo OLAP                                     |
| Microsoft Analysis Services Projects 2022    | Extensão do Visual Studio        | Requisito para modelação e deployment do cubo OLAP                         |
| Power BI Desktop                             | Última versão                    | Ferramenta de visualização e análise OLAP                                  |
| PowerDesigner                                | Opcional                         | Utilizado apenas para modelação; scripts de criação já estão fornecidas    |
| MySQL Workbench / PgAdmin / SSMS             | Opcional                         | Para visualização manual das bases de dados, se necessário                 |
| SQL Server Analysis Services (SSAS)          | Última versão                    | Necessário para disponibilizar o cubo OLAP ao Power BI                     |

➡ Bibliotecas Python Utilizadas

Instalação recomendada com: pip install -r requirements.txt