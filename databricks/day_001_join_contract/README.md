# Primeiro cenário escolhido para Databricks

Validar no Spark SQL do workspace o mesmo problema de cardinalidade explicado no
[lab SQLite](../../sql/day_002_join_cardinality/README.md): três linhas antes e depois
do JOIN, mas 17.500 versus 25.000 centavos, uma chave duplicada e um pedido órfão.

Importe `notebook.py` como notebook Python e execute em compute de teste já existente.
As consultas usam apenas CTEs com dados fictícios: nenhum catálogo, schema ou tabela
é criado. Não execute em compute pago sem definir previamente o orçamento do teste.

**Estado em 21/10/2026: preparado, ainda não executado no Databricks.** O notebook registra versão
Spark, horário UTC, quatro asserções e resultados; salvar esse JSON e a URL de execução
é o critério de conclusão. Isso não valida Delta MERGE, Zerobus, Unity Catalog ou jobs.

[Semântica oficial de JOIN](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join).
