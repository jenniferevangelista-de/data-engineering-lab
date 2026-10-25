# Escopo e execução dos testes

Os exemplos usam dados sintéticos. Testes locais de Python/SQLite, testes de
Spark local, execuções do GitHub Actions e integrações com serviços externos
são evidências distintas. Um teste local não comprova uma integração em nuvem.

## Executar

Use Python 3.12. Para os exemplos Spark, instale as dependências fixadas no
README do exemplo e use Java 17. Execute os comandos na raiz do repositório:

```sh
python -m unittest discover -s tests -p 'test_*.py' -v
python -m unittest discover -s sql/day_002_join_cardinality -p 'test_*.py' -v
python -m unittest discover -s airflow/day_001_freshness_contract -p 'test_*.py' -v
python -m unittest discover -s python/business_date_gate -p 'test_*.py' -v
python -m unittest discover -s aws/day_001_replay -p 'test_*.py' -v
python -m unittest discover -s pyspark/day_001_cdc -p 'test_*.py' -v
python -m unittest discover -s databricks/community_lab -p 'test_*.py' -v
```

O replay S3 usa Moto. Consulte o README de cada laboratório para os requisitos,
o resultado esperado e os limites. Kafka é um experimento de integração local
separado das suítes acima.

## Conferir uma versão

Consulte a execução correspondente ao commit no
[workflow de testes](https://github.com/jenniferevangelista-de/data-engineering-lab/actions/workflows/tests.yml).
O resultado de um commit não certifica automaticamente versões posteriores.

Os resumos técnicos de [testes iniciais](validacao-inicial.txt) e
[CDC](validacao-cdc.txt) identificam seus ambientes e escopos históricos.
