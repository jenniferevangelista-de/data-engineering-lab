# Databricks notebook source
# MAGIC %md
# MAGIC # Primeiro cenário Databricks: contrato de cardinalidade de JOIN
# MAGIC Preparado em 21/10/2026; **execução em Databricks pendente**.
# MAGIC Somente CTEs e dados sintéticos. Não cria tabelas persistentes nem exige catálogo/schema.
# MAGIC Execute em um compute de teste existente. Salve o JSON final e a URL da execução.

# COMMAND ----------
from datetime import datetime, timezone
import json

def validate_join_contract(spark):
    fixtures = """
    WITH orders AS (
      SELECT * FROM VALUES ('o1','c1',10000L),('o2','c2',5000L),('o3','c3',2500L)
      AS o(order_id,customer_id,amount_cents)
    ), profiles AS (
      SELECT * FROM VALUES ('c1','gold'),('c1','silver'),('c2','bronze')
      AS p(customer_id,tier)
    )
    """
    before = spark.sql(fixtures + "SELECT COUNT(*) AS n, SUM(amount_cents) AS cents FROM orders").first()
    after = spark.sql(fixtures + """SELECT COUNT(*) AS n, SUM(o.amount_cents) AS cents
                       FROM orders o INNER JOIN profiles p ON o.customer_id=p.customer_id""").first()
    duplicate = spark.sql(fixtures + """SELECT customer_id FROM profiles GROUP BY customer_id
                            HAVING COUNT(*)>1""").collect()
    orphan = spark.sql(fixtures + """SELECT o.order_id FROM orders o LEFT ANTI JOIN profiles p
                          ON o.customer_id=p.customer_id""").collect()
    assert (before.n, before.cents) == (3, 17500)
    assert (after.n, after.cents) == (3, 25000)
    assert [r.customer_id for r in duplicate] == ["c1"]
    assert [r.order_id for r in orphan] == ["o3"]
    return {"executed_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version, "source_rows": before.n,
            "joined_rows": after.n, "source_cents": before.cents,
            "joined_cents": after.cents, "duplicates": ["c1"], "orphans": ["o3"],
            "assertions": "PASS", "persistent_objects_created": False}

# COMMAND ----------
if "spark" in globals():
    print(json.dumps(validate_join_contract(spark), indent=2))
else:
    print("NOT EXECUTED: attach this notebook to a Databricks test compute.")
