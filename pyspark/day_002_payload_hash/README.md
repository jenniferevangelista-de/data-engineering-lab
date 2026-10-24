# Day 002 — comparing CDC payloads with a canonical hash

Synthetic data; real local Spark, no AWS credentials or Databricks workspace.
PySpark 3.5.8. No equivalence with DBR is assumed.

## Related article and idea

[Your Delta MERGE Is Not a Mind Reader](https://dev.to/jenniferevangelistade/your-delta-merge-is-not-a-mind-reader-2ppp)
([Medium version](https://medium.com/the-pipeline-ai-ml-and-cloud/your-delta-merge-is-not-a-mind-reader-16e81b6c788b))
rejects batches where the same key and sequence carry different payloads.
The suggestion to compare a hash of the value columns, instead of the raw columns, came from
[Karan Pulyala in the comments](https://dev.to/karanpulyala/comment/3ink7).
This lab tests that idea and measures what it solves and what it does not.
It complements [Day 001](../day_001_cdc/README.md); it does not replace it.

## Run
```sh
python -m pip install -r pyspark/day_002_payload_hash/requirements.txt
python -m unittest discover -s pyspark/day_002_payload_hash -p 'test_*.py' -v
```
On Windows, set JAVA_HOME and SPARK_LOCAL_IP=127.0.0.1 if the hostname does not resolve.

## Contract
`payload_hash(values, scales=None)` returns `sha2(to_json(struct(values...)), 256)`.
`conflict_report(batch, key, sequence, values, scales)` lists, for each key and
sequence with differing payloads, one row per distinct hash, with the raw values
next to the hash. An empty result means no conflict.
The key must already be normalized and validated (see Day 001).

## What the tests show
- The hash equals the SHA-256 of the JSON computed in Python, so the canonical form is known.
- An exact retry is not a conflict; a different payload is reported with values and hashes.
- A float with noise (`0.1 + 0.2` versus `0.3`) **produces a false conflict** when hashed raw.
  Rounding before the hash (`scales={"amount": 2}`) removes the false conflict, and a real
  change (`0.30` versus `0.31`) is still detected. The hash does not absorb noise by itself;
  the normalization before it does.
- Columns outside `values` do not influence the hash, so a new column does not become a
  conflict. With `distinct()` over the whole row, it would.
- NULL and the empty string stay different.
- **Column order changes the hash.** Keep the list fixed in code; do not rely on the
  DataFrame's column order.

## Limits
No mocks. The ten tests use a handful of rows and are not a benchmark. The hash is for
comparing and reporting; it does not replace the identity and version contract of Day 001.
SHA-256 collisions are negligible here, but the report keeps the raw values for whoever
investigates. No Delta MERGE, Databricks, S3 or streaming was run.
The behavior of `to_json` with complex types (nested structs, maps) is not covered.

Sources: [to_json](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html),
[sha2](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sha2.html).
