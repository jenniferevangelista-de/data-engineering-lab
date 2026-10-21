# Databricks Community → exemplos reproduzíveis

Quatro recortes técnicos das contribuições de Jennifer Evangelista na Community,
com SQL executável e dados sintéticos. A validação usa **Apache Spark 3.5.3 local,
Python 3.12 e Java 17**, com timezone UTC e ANSI habilitado.

Os testes verificam os resultados desses recortes. Não estabelecem que o problema
de um participante foi resolvido no ambiente dele, nem que uma resposta foi aceita
no fórum. Nenhum workspace Databricks, tabela Delta ou cluster Kafka foi acessado
por esta suíte.

## Executar

A partir da raiz do repositório, em um ambiente virtual:

```sh
python -m pip install -r pyspark/day_001_cdc/requirements.txt
python -m unittest discover -s databricks/community_lab -p 'test_*.py' -v
```

No Windows, use Java 17 disponível no PATH. Em ambientes com resolução de hostname
restrita, defina `SPARK_LOCAL_IP=127.0.0.1`. A suíte cria apenas views temporárias
e encerra a sessão Spark. As consultas ficam em [queries/](queries/).

## O que foi verificado

| Recorte | Resultado esperado e limite | Origem |
|---|---|---|
| [Hash de chave](queries/business_key_hash.sql) | JSON com ordem fixa e NULL explícito distingue fronteiras e posições nulas; normalização é contrato de negócio. Não é implementação completa de Data Vault nem prova de ausência de colisões SHA-256. | [Data Vault](https://community.databricks.com/t5/data-engineering/datavault-implemention-on-databricks/m-p/173958#M56786) |
| [Colisões antes do MERGE](queries/normalized_merge_collisions.sql) | Detecta duplicatas após as expressões de normalização do exemplo e o filtro `col3='A'`. Execute o diagnóstico também na saída real de USING. Não executa Delta MERGE; NULLs no GROUP BY não provam correspondência por igualdade no ON. | [Discussão MERGE](https://community.databricks.com/t5/data-engineering/databricks-support-updating-multiple-target-rows-with-single/td-p/127573) |
| [Contagens horárias](queries/hourly_change_counts.sql) | Conta insert, update_postimage e delete em linhas sintéticas; update_preimage não duplica updates. Usa hora de commit UTC e expõe tipos desconhecidos. Não lê CDF real, não deduplica replay, não mede eventos de negócio. | [CDC por hora](https://community.databricks.com/t5/data-engineering/how-to-track-hourly-or-daily-of-upsert-delete-metrics-in-a-dlt/m-p/173769#M56774) |
| [Métricas opcionais](queries/optional_operation_metrics.sql) | Preserva o mapa original; chave ausente fica NULL e zero medido continua zero. Espera strings numéricas válidas. Não lê DESCRIBE HISTORY nem mede desempenho de clustering. | [Manutenção de tabelas](https://community.databricks.com/t5/data-engineering/production-table-maintenance/m-p/173766#M56771) |

São dez testes: fronteiras de componentes, posição de NULL, payload canônico,
normalização/filtro, chaves únicas, espaços não normalizados, preimage, horas e
operações desconhecidas, métricas ausentes e zero medido. A evidência da execução
fica em [validation.json](validation.json).

O exemplo existente de [preparação de CDC](../../pyspark/day_001_cdc/README.md)
complementa o diagnóstico de MERGE: oito testes de normalização, ordenação,
replay exato e rejeição de versões conflitantes. Sua execução também é local.

## Itens que ainda precisam de integração

| Discussão | Validação ainda necessária |
|---|---|
| [Auto Loader / type widening](https://community.databricks.com/t5/data-engineering/autoloader-type-widening/m-p/173758#M56767) | DBR compatível, evolução de schema, rescued data e reinício observados em workspace isolado. |
| [Kafka / checkpoints](https://community.databricks.com/t5/data-engineering/checkpoint-changes-not-working-on-my-databricks-job/m-p/173759#M56768) | Broker de teste, offsets persistidos, reexecução e recuperação com checkpoint. |
| [Schema de estado](https://community.databricks.com/t5/data-engineering/schema-evolution-with-structured-streaming-upstream-schema/m-p/173767#M56772) | Equivalência e cardinalidade do join, estado e compatibilidade real de checkpoint. |
| [Spark em cargas pequenas](https://community.databricks.com/t5/data-engineering/at-what-data-size-do-you-stop-reaching-for-spark/m-p/173768#M56773) | Benchmark representativo completo, cold/warm, memória, custo e correção equivalentes. |

Esses tópicos ainda não foram testados em um workspace Databricks.
Arquitetura Data Vault completa, PIT/backfill, otimização de MERGE e coleta
incremental de CDF também continuam fora do escopo dos quatro recortes locais.

## Fontes técnicas

- [Change data feed: metadados e tipos de mudança](https://docs.databricks.com/aws/en/tables/features/change-data-feed)
- [Histórico e operationMetrics](https://docs.databricks.com/aws/en/tables/history)

As fontes sustentam a interpretação dos campos; os testes são a evidência dos
resultados locais. Os exemplos são autoria própria e usam dados sintéticos.
Nenhum e-mail, dado de cliente, credencial ou conteúdo privado integra esta pasta.

## Artigos relacionados

- [Your Kafka Consumer Lag Is Not Your SLA](https://medium.com/@jenniferevangelista.code/your-kafka-consumer-lag-is-not-your-sla-offset-math-retention-and-the-missing-commit-trap-0bab431214bc): o artigo cita [métricas opcionais](queries/optional_operation_metrics.sql) para ilustrar a distinção entre valor ausente e zero medido. Esta consulta não coleta offsets.
- [Unity Catalog Row Filters and Column Masks](https://medium.com/@jenniferevangelista.code/unity-catalog-row-filters-and-column-masks-a-fail-closed-security-design-you-can-test-d3e3b25e0ab0): leitura sobre políticas de acesso. Os quatro recortes deste lab não implementam nem validam row filters ou column masks em Unity Catalog.

## Serialização JSON em Python

[Exemplo de preservação de tipos JSON](zerobus-json.md): oito verificações adicionais
com Python 3.12 e biblioteca padrão. Inclui a diferença entre string e número,
campos não VARIANT, JSON null e rejeição de NaN/Infinity. Sem chamada ao Zerobus.
O comando de testes SQL acima continua executando dez testes; os oito novos
ficam em `tests/test_variant_encoding.py`.
