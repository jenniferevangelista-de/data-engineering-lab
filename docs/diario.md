# Diário de validação
## 2026-10-18 — Dia 001
- Tema: total de vendas por cliente, com valores em centavos.
- Trilhas: Python e SQL (SQLite).
- Comando: `python -m unittest discover -s tests -v`.
- Resultado: consultar `docs/validacao-inicial.txt`, gerado pela execução local.
- PySpark, Databricks, Kafka e AWS: guias preparados; exemplos e integrações pendentes.

## 2026-10-18 — PySpark Dia 001 (Parte 1)
- Preparação determinística de lote CDC: normalização, retries, seleção por versão e conflitos.
- Python 3.12, Spark 3.5.3, Java 17; dados sintéticos; JVM local real.
- Comando: `python -m unittest discover -s pyspark/day_001_cdc -p 'test_*.py' -v`.
- Resultado: 8 testes passaram em 12.499 s; seis testes anteriores também passaram.
- Evidência: `docs/validacao-cdc.txt`. Avisos de sockets da biblioteca foram observados.
- Não executados: Delta MERGE, S3, Auto Loader, Lakeflow ou workspace Databricks.
- CI Spark adicionada; resultado remoto deve ser consultado separadamente.


## 2026-10-20 — JOIN e freshness
- JOIN: 8 testes em SQLite.
- Freshness: 11 testes de funções Python.
- Execução local: 33 testes de Python, SQLite e JSON.
- CI: inclui 18 testes Spark, totalizando 51 testes.
- Evidência da execução registrada: https://github.com/jenniferevangelista-de/data-engineering-lab/actions/runs/39413938821
- Os exemplos usam dados sintéticos. Integrações com scheduler Airflow e serviços de nuvem permanecem pendentes.

## 2026-10-21 — Kafka, replay S3 e notebook Databricks
- Testes locais: 14 base + 8 JOIN + 11 freshness + 7 replay S3/Moto = 40 aprovados. As 18 verificações Spark rodam na CI.
- AWS: exemplo boto3 com escrita condicional e integridade; sete testes simulados com Moto passaram. Ainda sem execução em AWS real.
- Kafka: Apache Kafka 3.9.1 real, Java 17, confluent-kafka 2.16.0, um nó KRaft local. Offsets produzidos 0/1/2/3; processados 0/1/1/2/3; checkpoint 1→4; três eventos únicos no SQLite e duas duplicações suprimidas. Consumidor interrompido após escrita no destino e antes do commit. Sem teste de falha ou replicação do broker.
- Databricks: contrato de JOIN escolhido como primeiro cenário; notebook com CTEs pronto, ainda não executado no serviço.

## 2026-10-24 — Atualização de segurança do PySpark
- Pin atualizado de PySpark 3.5.3 para 3.5.8 para corrigir o intervalo afetado pela CVE-2026-32773 no Spark History Server. Não foi demonstrado History Server público exposto neste lab.
- Ambiente isolado: Python 3.12, PySpark/Spark 3.5.8, Py4J 0.10.9.9 e Java 17 no Windows.
- Resultado: 83 testes aprovados — 58 Python/SQLite, 7 replay S3 com Moto, 8 CDC Spark e 10 SQL Community Spark.
- `pip check` passou; a consulta OSV dos 23 pacotes runtime resolvidos retornou zero advisories. Somente coordenadas públicas de pacotes e versões foram enviadas à consulta.
- Evidência atual: [validation-security-2026-10-24.json](validation-security-2026-10-24.json). Os registros de execução anteriores com Spark 3.5.3 foram preservados como históricos.
- Avisos locais de winutils/Hadoop, sockets e limpeza de processos Spark já encerrados foram registrados; as asserções e códigos de saída passaram. O tempo local não é benchmark.
- Sem chamadas reais a AWS, Databricks, Delta ou Kafka; nenhum publicador ou reenvio de competição foi executado.
