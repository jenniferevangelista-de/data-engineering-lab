# Day 001: freshness contract after a green DAG

Task success and dataset freshness are different signals. This pure-Python lab
accepts a synthetic published snapshot description and checks run state,
required interval, event recency, row count, quality status and clock skew.

## Run

Python 3.12+, no Airflow service or third-party packages required:

```bash
python -m unittest discover -s airflow/day_001_freshness_contract -p 'test_*.py' -v
python airflow/day_001_freshness_contract/freshness_contract.py
```

Eleven tests cover success/failure and clock edge cases. Call `require_fresh()`
at the **publication boundary**, after reading the real output metadata, so a
contract violation raises an error instead of silently delivering stale data.

`max_age=90 minutes` is an **example-specific SLA**. Low-frequency streams and
snapshot-style datasets need another freshness definition. This contract is
illustrative; no Airflow DAG, cloud service, broker, or Databricks workspace was
run by these tests.

## Article

[Your Airflow DAG Is Green. Your Data Is From Yesterday.](https://medium.com/@jenniferevangelista.code/your-airflow-dag-is-green-your-data-is-from-yesterday-caefcac73db2) on Medium.

Official references: [DAG runs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html), [asset-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html).
