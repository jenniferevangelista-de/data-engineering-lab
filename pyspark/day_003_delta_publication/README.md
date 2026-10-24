# Day 003 — idempotent publication on local Delta

Synthetic data; real local Spark with open-source Delta Lake, no AWS credentials and no
Databricks workspace. PySpark 3.5.8 and delta-spark 3.3.2. No equivalence with DBR is assumed.

## Related article and idea

[Your pipeline ran green and shipped last week's data anyway](https://dev.to/jenniferevangelistade/your-pipeline-ran-green-and-shipped-last-weeks-data-anyway-1il3)
describes a business-date gate. Its lab, [business_date_gate](../../python/business_date_gate/README.md),
keeps the evidence in SQLite and states that it does not coordinate a Delta write with that
transaction. The suggestion to write the evidence into the commit metadata and keep a small
control table in Delta came from
[Karan Pulyala in the comments](https://dev.to/karanpulyala/comment/3ink9).
This lab builds that variant and tests it. It covers publication only, not the readiness
logic of the gate.

## Run
```sh
python -m pip install -r pyspark/day_003_delta_publication/requirements.txt
python -m unittest discover -s pyspark/day_003_delta_publication -p 'test_*.py' -v
```
The first run downloads the Delta jars from Maven Central, so it needs network access.
On Windows, set JAVA_HOME and SPARK_LOCAL_IP=127.0.0.1 if the hostname does not resolve.

## Contract
`publish(spark, control, target, consumer, business_date, total_cents, run_ids)` returns
`"published"` or `"retry"` and raises `PublicationConflict` on a contradiction.

1. Record the intent as `PENDING` in the control table (key: consumer + business date).
2. Write the target row; the commit's `userMetadata` carries the business date and run IDs.
3. Mark the control row `PUBLISHED`.

The control row freezes the amount and the dependency set. The same request again is a
no-op. A different amount or dependency set is rejected. A crash between the steps is
recovered by calling `publish` again with the same input.

## What the tests show
- The first publication writes the target row and the control row, and `DESCRIBE HISTORY`
  shows the evidence JSON on the target's MERGE commit. The session setting is cleared afterwards.
- A repeated publication creates no new commit in either table.
- A different amount or a different set of run IDs is rejected and nothing changes.
- A crash after the control record, or after the target write, is recovered without a second
  target write and with exactly one target row.
- Other consumers and dates are independent.
- Two publishers for the same key, each with its own session, leave one target row and one
  `PUBLISHED` control row. Three rounds per run, three runs: nine rounds, all passed.
- Invalid consumers, dates, amounts and run IDs are rejected before any write.

## Findings worth knowing
- `userMetadata` can be set with the write option or with the session setting
  `spark.databricks.delta.commitInfo.userMetadata`. A MERGE needs the setting. The setting is
  **session-scoped**: concurrent publishers sharing one session overwrite each other's
  evidence, so each publisher uses `spark.newSession()`.
- A MERGE cannot raise by itself. The rejection of a different amount comes from reading the
  frozen control row first, not from the MERGE.
- In a separate probe, two sessions ran the same insert-only MERGE at once, with and without a
  matched clause. In six of six rounds per variant, one committed, the other got
  `ConcurrentAppendException`, and one row remained. The default isolation level was used.
  This is a small sample on local Delta, not a proof, and Databricks may behave differently.
- Delta has no uniqueness constraint. The guard here is the control row plus conflict
  detection, and `publish` re-reads and decides again when a conflict happens.

## Limits
No mocks, but also no Databricks, no cloud storage and no real multi-node writers. The target
and control writes are separate commits, so the `PENDING` state and recovery exist precisely
because there is no cross-table atomicity. Table history is subject to log retention, so
`userMetadata` is a readable copy of the evidence, not a durable record; the control table is
the durable one. Local timing (about four minutes for the ten tests) is not a benchmark.
The gate's readiness logic, deletes, repairs and a repair policy are not implemented.

Sources: [MERGE](https://docs.databricks.com/aws/en/delta/merge),
[Delta concurrency control](https://docs.delta.io/latest/concurrency-control.html),
[DESCRIBE HISTORY](https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history).
