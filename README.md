# PLSQL
Código de programa PL/SQL

Repositório com exemplos de códigos ORACLE PL/SQL autorais desenvolvidos para responder demandas reais.

Os arquivos estão organizados em pares de  mesmo nome e extensão diferentes:
- nome.pdf - contém o contexto que gerou a necessidade do código, bem como a documentação do programa.
- nome.sql - contém o código pl/sql.

Indice:

MATA_SESSAO - Procedure que tem por objetivo garantir a finalização de todas as sessões originadas por sistema multithreading (COBOL) e registrar esse evento em tabela própria (REGISTRO_MATA_SESSAO) para análise posterior.

WEBQUERY_GRANTS_REFRESH - Procedure executada diariamente para atualizar as permissões de acesso de usuários a sistemas de consultas WEB a banco de dados Oracle.

ORACLE_TO_POSTGRES - Bloco anônimo responsável por migrar registros de tabelas ORACLE para seus pares no POSTGRES.

FULL_ACCESS_REGISTER - Procedure que registra comandos SQL que fazem acesso FULL TABLE.
