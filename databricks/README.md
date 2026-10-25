# Notebooks e tabelas Delta

Exemplos da [Databricks Community com SQL reproduzível](community_lab/README.md):
hash canônico de chaves, diagnóstico de MERGE, contagens de mudanças e métricas
opcionais. Validação em Spark local; integração com Databricks ainda pendente.

## Requisitos da próxima entrega
Workspace de teste; declarar runtime, catálogo e schema; usar nomes isolados e documentar limpeza.

Primeiro cenário escolhido em 21/10/2026: [contrato de JOIN em Spark SQL](day_001_join_contract/README.md).
Notebook preparado somente com CTEs e dados sintéticos; não cria objetos persistentes.
Execução Databricks pendente de workspace autenticado e compute de teste.

Usar `day_NNN_assunto/` com README, código, dados sintéticos e testes.
Documentar no README do exemplo os comandos, versões, resultados esperados e limites de validação.
Separar testes locais ou simulados da validação real no serviço.

