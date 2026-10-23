# Gate a daily load by business date

A backfill can run today while processing an older business date. This Python
and SQLite example requires the exact upstream jobs to succeed for an explicit
business date before publishing a synthetic daily total.

## Run

Python 3.12+, standard library only. From the repository root:

```sh
python -m unittest discover -s python/business_date_gate -p 'test_*.py' -v
python python/business_date_gate/gate.py
```

Expected demo output:

```text
Backfill completed today: False
Required business date ready: True
First publication: True
Same publication retried: False
```

## Contract

- A run processes exactly one canonical `YYYY-MM-DD` business date.
- Job IDs are explicit. Unrelated jobs and other dates cannot satisfy a gate.
- The producer assigns increasing attempt ordinals per job/date. A run ID is
  an identity, not a chronological sequence or a substitute for business date.
- The largest recorded attempt wins. A newer running, failed or canceled
  attempt blocks an earlier success. Late delivery of an older observation
  cannot overwrite a newer attempt.
- A `RUNNING` observation can become terminal once. Other identity fields and
  terminal results cannot change. Repairs need a new ordinal.
- Publication freezes the chosen successful run IDs. The same consumer/date
  and amount can be retried without a new write, even after a later upstream
  failure; this does not mean the original output was recomputed or revoked.
- A different amount or dependency set requires an explicit repair policy.

## What the tests exercise

The 25 tests include wrong-date backfills, runs spanning midnight, missing or
unrelated jobs, out-of-order observations, terminal transitions, invalid input,
replayed publications, evidence preservation and two concurrent publishers on
separate connections to the same temporary SQLite file. A trigger deliberately
fails the evidence insert to verify that the target insert rolls back too.

The target total and publication evidence are written inside one
`BEGIN IMMEDIATE` transaction in the same SQLite database. No network service
or external sink participates. This is not a distributed exactly-once claim.
The supplied total is synthetic; the gate does not compute or validate it.

## Integration boundaries

The producer must publish complete, authoritative attempt observations and
propagate business date according to the actual business calendar. A missing
new attempt can otherwise leave an earlier success looking current. This lab
does not call Databricks, resolve business calendars, validate upstream table
contents or coordinate a Delta/cloud write with its SQLite transaction.

For a real deployment, define how the run collector handles retries, repairs,
pagination, terminal-state mapping and late observations. Keep the gate and
publication evidence in a storage system appropriate for the deployment.

Official references:

- [Databricks run-list filters](https://docs.databricks.com/api/jobs/v2/list-runs).
- [Explicit job parameters](https://docs.databricks.com/aws/en/jobs/job-parameters).
- [Table-update triggers](https://docs.databricks.com/aws/en/jobs/trigger-table-update).
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html).
