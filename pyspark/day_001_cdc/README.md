# Dia 001 — contrato determinístico de CDC antes do MERGE

Dados sintéticos; Spark local real, sem credenciais AWS ou workspace Databricks.
Python 3.12, PySpark 3.5.8 e Java 17. Não pressupõe equivalência com DBR.

## Artigo relacionado

[Your Delta MERGE Is Not a Mind Reader — Jennifer Evangelista, Medium](https://medium.com/@jenniferevangelista.code/your-delta-merge-is-not-a-mind-reader-16e81b6c788b) explica precedência de versões e riscos de eventos duplicados ou conflitantes em Delta MERGE. Este laboratório demonstra somente a preparação determinística de um lote PySpark antes do MERGE; não executa nem valida o MERGE Delta em um workspace Databricks.

## Executar
```sh
python -m pip install -r pyspark/day_001_cdc/requirements.txt
python -m unittest discover -s pyspark/day_001_cdc -p 'test_*.py' -v
```
No Windows, defina JAVA_HOME para Java 17 e SPARK_LOCAL_IP=127.0.0.1 se
o hostname não resolver. Consulte a [revalidação com Spark 3.5.8](../../docs/validation-security-2026-10-24.json).
O registro em `docs/validacao-cdc.txt` preserva a execução histórica com Spark 3.5.3.

Entrada: key STRING, sequence BIGINT, value STRING. A sequência deve ser a
versão do produtor, comparável dentro de cada chave; não use hora de chegada.
Normalização assumida: upper(trim(key)); trim do Spark remove espaços nas bordas,
não é uma normalização Unicode completa. Confirme a regra com o domínio.

`a/1/old, A/2/new, A/2/new` produz `A/2/new`. Repetições exatas são descartadas.
Mesma chave/versão com payloads diferentes falha, inclusive NULL versus texto.
Chave nula/vazia e versão nula/negativa falham. NULL em value é permitido;
nenhuma semântica de exclusão é inferida. Colunas extras são descartadas.
DataFrame vazio mantém esquema e retorna zero linhas. Streaming é rejeitado.

## Uso no Databricks (adaptação não executada neste teste)
```python
from prepare_cdc import prepare_cdc
prepared = prepare_cdc(batch)  # batch finito, snapshot imutável
prepared.createOrReplaceTempView('prepared_updates')
```
```sql
MERGE INTO sandbox.customer_current AS t
USING prepared_updates AS s ON t.key = s.key
WHEN MATCHED AND s.sequence > t.sequence THEN
  UPDATE SET t.sequence = s.sequence, t.value = s.value
WHEN NOT MATCHED THEN
  INSERT (key, sequence, value) VALUES (s.key, s.sequence, s.value);
```
Crie previamente uma tabela Delta sandbox com chave normalizada e única e
sequence não nula. Empate entre lote e destino com payload diferente exige
quarentena/validação adicional; a função só detecta conflitos dentro do lote.
O predicado evita sobrescrita por versão atrasada, mas não implementa CDC geral,
SCD2, deletes, concorrência nem transação entre várias tabelas. Não existe
garantia de unicidade automática de chave em Delta baseada neste código.

## Testes e limites
Oito testes cobrem vencedor mais recente, normalização, retries, repartição,
idempotência da preparação, conflitos, NULL, vazio e esquema inválido.
Não há mock. Não foram executados MERGE Delta, S3, Auto Loader ou Lakeflow.
A função faz ações de validação e shuffles; não é benchmark nem solução de escala.
Use entrada estável: leituras distintas sobre fonte mutável podem discordar.
Para SCD2 preserve eventos intermediários; reduzir ao último evento perderia história.

Fontes: [MERGE](https://docs.databricks.com/aws/en/delta/merge),
[dropDuplicates](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html).

## Leitura complementar sobre recuperação

[Your S3 Pipeline Needs a Replay Plan, Not More Optimism](https://medium.com/@jenniferevangelista.code/your-s3-pipeline-needs-a-replay-plan-not-more-optimism-a62342b93e89) relaciona replay e idempotência ao desenho da ingestão. [Your Kafka Consumer Lag Is Not Your SLA](https://medium.com/@jenniferevangelista.code/your-kafka-consumer-lag-is-not-your-sla-offset-math-retention-and-the-missing-commit-trap-0bab431214bc) separa offsets de resultados downstream. Este laboratório continua limitado à preparação de lotes sintéticos em Spark local.
