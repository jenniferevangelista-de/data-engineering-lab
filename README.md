# Data Engineering Lab

Laboratório público de engenharia de dados de **Jennifer Evangelista**.

Exemplos reproduzíveis de Python, SQL e PySpark para qualidade de dados, CDC,
cardinalidade de JOIN e freshness. Validação local e em CI com dados sintéticos.

## Laboratórios

| Entrega | O que faz, em linguagem simples | O que já foi conferido |
|---|---|---|
| [Vendas em Python](python/day_001_sales.py) e [a mesma ideia em SQL](sql/day_001_sales.sql) | Soma o valor das vendas de cada cliente. | 6 testes de Python e SQL passaram, incluindo dados vazios e valores inválidos. |
| [Organização de atualizações em PySpark](pyspark/day_001_cdc/README.md) | Recebe várias versões de um registro, escolhe a mais recente, elimina repetições exatas e recusa informações conflitantes. | 8 testes passaram usando Spark executado no computador e no GitHub. |
| [Quatro exemplos ligados à Databricks Community](databricks/community_lab/README.md) | Gera identificadores a partir de campos; encontra duplicações após padronizar os dados; conta inclusões, alterações e exclusões por hora; lê métricas sem confundir “não informado” com zero. | 10 testes passaram usando dados fictícios e Spark. A origem de cada discussão está indicada. |
| [Preservar valores JSON](databricks/community_lab/zerobus-json.md) | Mantém a diferença entre a string “5”, o número 5 e um valor ausente ao preparar campos JSON. | 8 testes de serialização Python local; integração Zerobus não executada. |
| [Cardinalidade de JOIN](sql/day_002_join_cardinality/README.md) | Mostra como um JOIN pode manter a contagem de linhas enquanto perde pedidos e duplica valores; verifica chaves, pedidos sem correspondência e totais em centavos. | 8 testes em SQLite com dados sintéticos; não valida Spark nem um modelo dimensional de produção. |
| [Contrato de freshness](airflow/day_001_freshness_contract/README.md) | Separa sucesso da execução e atualidade dos dados, verificando intervalo, timestamps, contagem e qualidade antes de liberar o resultado. | 11 testes de funções Python; nenhum scheduler Airflow ou serviço externo foi executado. |
| [Replay S3](aws/day_001_replay/README.md) | Reenvia o mesmo lote sem sobrescrever um conteúdo diferente. | 7 testes com Moto; execução AWS real pendente. |
| [Kafka: retomada](kafka/day_001_recovery/README.md) | Confirma entregas e retoma após interrupção sem duplicar efeitos no SQLite. | Broker Kafka 3.9.1 real local; quatro mensagens confirmadas e três eventos únicos. |
| [Carga por data de negócio](python/business_date_gate/README.md) | Impede que um backfill de outra data libere a carga diária; preserva as execuções que autorizaram a publicação e evita repetir a mesma gravação local. | 25 testes Python/SQLite, incluindo concorrência e rollback. Sem execução em Databricks. |

**São 83 testes: 6 de vendas + 8 de CDC + 10 de SQL Community + 8 de JSON + 8 de JOIN + 11 de freshness + 7 de replay S3/Moto + 25 de data de negócio.**
O experimento Kafka é uma integração separada dessa contagem.

[Ver as execuções e o resultado de cada versão no GitHub](https://github.com/jenniferevangelista-de/data-engineering-lab/actions/workflows/tests.yml?query=branch%3Amain)
· [Ver o escopo de validação e os comandos](docs/validation.md)
· [Ver o registro histórico dos testes da Community](databricks/community_lab/validation.json)


## Artigos no Medium e exemplos relacionados

| Artigo | Código ou guia relacionado | Escopo atual |
|---|---|---|
| [Your Delta MERGE Is Not a Mind Reader](https://medium.com/@jenniferevangelista.code/your-delta-merge-is-not-a-mind-reader-16e81b6c788b) | [Preparação determinística de CDC](pyspark/day_001_cdc/README.md) | Oito testes de preparação PySpark; o MERGE Delta é ilustrativo. |
| [Your S3 Pipeline Needs a Replay Plan, Not More Optimism](https://medium.com/@jenniferevangelista.code/your-s3-pipeline-needs-a-replay-plan-not-more-optimism-a62342b93e89) | [Preparação de CDC](pyspark/day_001_cdc/README.md) e [trilha AWS](aws/README.md) | O lote local testa ordenação e retries; não há integração S3 validada. |
| [Your Kafka Consumer Lag Is Not Your SLA](https://medium.com/@jenniferevangelista.code/your-kafka-consumer-lag-is-not-your-sla-offset-math-retention-and-the-missing-commit-trap-0bab431214bc) | [Métricas opcionais](databricks/community_lab/queries/optional_operation_metrics.sql) e [trilha Kafka](kafka/README.md) | A consulta demonstra NULL versus zero com mapas sintéticos; não coleta offsets Kafka. O artigo contém seu próprio exemplo Python. |
| [Unity Catalog Row Filters and Column Masks](https://medium.com/@jenniferevangelista.code/unity-catalog-row-filters-and-column-masks-a-fail-closed-security-design-you-can-test-d3e3b25e0ab0) | [Community lab](databricks/community_lab/README.md) | O lab contém SQL sintético em Spark local; políticas de Unity Catalog ainda não foram executadas em Databricks. |
| [Your SQL Join Passed the Row-Count Check. Revenue Was Still Wrong.](https://medium.com/@jenniferevangelista.code/your-sql-join-passed-the-row-count-check-revenue-was-still-wrong-84d6ff448745) | [Lab de cardinalidade de JOIN](sql/day_002_join_cardinality/README.md) | Oito testes SQLite com dados sintéticos; sem benchmark Spark/Databricks. |
| [Your Airflow DAG Is Green. Your Data Is From Yesterday.](https://medium.com/@jenniferevangelista.code/your-airflow-dag-is-green-your-data-is-from-yesterday-caefcac73db2) | [Contrato de freshness](airflow/day_001_freshness_contract/README.md) | Onze testes de funções Python; sem execução de scheduler Airflow. |
| [A business-date gate for daily loads](https://medium.com/@jenniferevangelista.code/a-business-date-gate-for-daily-loads-bc2d45c8f31f) | [Carga por data de negócio](python/business_date_gate/README.md) | 25 testes Python/SQLite, incluindo concorrência e rollback; sem integração Databricks. |

## O que ainda falta

| Parte | Situação atual | Próxima etapa |
|---|---|---|
| Databricks de verdade | Os trechos de código foram testados em Spark local e no GitHub. A integração com um ambiente Databricks ainda está pendente. | Executar os cenários em um ambiente de teste e registrar o resultado. |
| [AWS](aws/README.md) | Replay S3 executável com sete testes simulados Moto aprovados. | Executar em bucket de sandbox existente e registrar o resultado real. |
| [Kafka](kafka/README.md) | Envio, leitura e retomada após interrupção do consumidor validados em broker real local. | Ampliar para falha do broker e replicação somente em um ambiente apropriado; isso ainda não foi testado. |
| Novas soluções da Community | Quatro recortes SQL e um exemplo de serialização JSON estão publicados; outros assuntos precisam de validação adicional. | Selecionar o que for relevante, criar um exemplo próprio, testar e publicar sem duplicar o que já existe. |

## Executar

Os exemplos de vendas e serialização JSON requerem Python 3.12 ou superior:

```sh
python -m unittest discover -s tests -v
```

Os exemplos com Spark também precisam de Java 17 e PySpark 3.5.8. Os passos estão
no [guia de PySpark](pyspark/day_001_cdc/README.md) e no
[guia dos exemplos da Community](databricks/community_lab/README.md).

Consulte o [guia de validação](docs/validation.md) e as
[instruções para contribuir](CONTRIBUTING.md) para os detalhes técnicos.

## License

Original code and documentation are available under the [MIT License](LICENSE),
copyright 2026 Jennifer Evangelista. The `.gitignore` template from
[github/gitignore](https://github.com/github/gitignore) retains its
[CC0-1.0 terms](https://github.com/github/gitignore/blob/main/LICENSE).
External dependencies and linked third-party materials retain their own terms.

