"""Idempotent publication on local Delta: a control table plus a target table.

Order of operations (a crash between steps is recoverable by calling publish again):
  1. record the intent as PENDING in the control table (key: consumer + business date);
  2. write the target row, with the evidence in the commit's userMetadata;
  3. mark the control row PUBLISHED.
The control row freezes the amount and the evidence. Same input again is a no-op;
a different amount or dependency set is rejected. Not a distributed exactly-once claim.
"""
import json
from datetime import date
from typing import Callable, Dict, Optional, Sequence

from delta.exceptions import (ConcurrentAppendException, ConcurrentDeleteDeleteException,
                              ConcurrentDeleteReadException, ConcurrentTransactionException,
                              ConcurrentWriteException, MetadataChangedException)
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

USER_METADATA = "spark.databricks.delta.commitInfo.userMetadata"
CONTROL_SCHEMA = ("consumer string, business_date string, total_cents long, "
                  "evidence_json string, status string")
TARGET_SCHEMA = "consumer string, business_date string, total_cents long"
CONFLICTS = (ConcurrentAppendException, ConcurrentDeleteDeleteException,
             ConcurrentDeleteReadException, ConcurrentTransactionException,
             ConcurrentWriteException, MetadataChangedException)
MAX_ATTEMPTS = 5


class PublicationConflict(Exception):
    """The request contradicts what the control table already froze."""


def init_tables(spark: SparkSession, control: str, target: str) -> None:
    for path, schema in ((control, CONTROL_SCHEMA), (target, TARGET_SCHEMA)):
        if not DeltaTable.isDeltaTable(spark, path):
            spark.createDataFrame([], schema).write.format("delta").save(path)


def _validate(consumer: str, business_date: str, total_cents: int,
              run_ids: Sequence[str]) -> str:
    if not isinstance(consumer, str) or not consumer or consumer != consumer.strip():
        raise ValueError("consumer must be a nonempty, trimmed string")
    if not isinstance(business_date, str) or \
            date.fromisoformat(business_date).isoformat() != business_date:
        raise ValueError("business_date must be YYYY-MM-DD")
    if isinstance(total_cents, bool) or not isinstance(total_cents, int) or total_cents < 0:
        raise ValueError("total_cents must be a non-negative integer")
    if not run_ids or len(set(run_ids)) != len(run_ids) or \
            any(not isinstance(r, str) or not r or r != r.strip() for r in run_ids):
        raise ValueError("run_ids must be nonempty, trimmed and without repeats")
    return json.dumps({"business_date": business_date, "run_ids": sorted(run_ids)},
                      sort_keys=True, separators=(",", ":"))


def _key(consumer: str, business_date: str):
    return (F.col("consumer") == consumer) & (F.col("business_date") == business_date)


def _rows(spark: SparkSession, path: str, consumer: str, business_date: str) -> list:
    return spark.read.format("delta").load(path).where(_key(consumer, business_date)).collect()


def _one(rows: list, name: str):
    if len(rows) > 1:
        raise PublicationConflict(f"Duplicate {name} rows for one consumer and business date")
    return rows[0] if rows else None


def _publish_once(spark, control, target, consumer, business_date, total_cents, evidence,
                  hooks: Dict[str, Callable[[], None]]) -> str:
    row = _one(_rows(spark, control, consumer, business_date), "control")
    if row is not None:
        if row.total_cents != total_cents or row.evidence_json != evidence:
            raise PublicationConflict("A different amount or dependency set was already frozen")
        if row.status == "PUBLISHED":
            return "retry"
    else:
        record = spark.createDataFrame(
            [(consumer, business_date, total_cents, evidence, "PENDING")], CONTROL_SCHEMA)
        (DeltaTable.forPath(spark, control).alias("t")
         .merge(record.alias("s"), "t.consumer = s.consumer AND t.business_date = s.business_date")
         .whenNotMatchedInsertAll().execute())
        hooks.get("after_record", lambda: None)()

    existing = _one(_rows(spark, target, consumer, business_date), "target")
    if existing is None:
        source = spark.createDataFrame([(consumer, business_date, total_cents)], TARGET_SCHEMA)
        spark.conf.set(USER_METADATA, evidence)
        try:
            (DeltaTable.forPath(spark, target).alias("t")
             .merge(source.alias("s"),
                    "t.consumer = s.consumer AND t.business_date = s.business_date")
             .whenNotMatchedInsertAll().execute())
        finally:
            spark.conf.unset(USER_METADATA)
        existing = _one(_rows(spark, target, consumer, business_date), "target")
    if existing is None or existing.total_cents != total_cents:
        raise PublicationConflict("Target row disagrees with the frozen amount")
    hooks.get("after_target", lambda: None)()

    DeltaTable.forPath(spark, control).update(
        condition=_key(consumer, business_date) & (F.col("status") == "PENDING"),
        set={"status": F.lit("PUBLISHED")})
    return "published"


def publish(spark: SparkSession, control: str, target: str, consumer: str,
            business_date: str, total_cents: int, run_ids: Sequence[str],
            hooks: Optional[Dict[str, Callable[[], None]]] = None) -> str:
    """Return "published" or "retry"; raise PublicationConflict on a contradiction.

    `hooks` ("after_record", "after_target") exist only so tests can simulate a crash.
    The userMetadata setting is session-scoped: publishers running concurrently must
    use separate sessions (spark.newSession()), or they overwrite each other's evidence.
    """
    evidence = _validate(consumer, business_date, total_cents, run_ids)
    for _ in range(MAX_ATTEMPTS):
        try:
            return _publish_once(spark, control, target, consumer, business_date,
                                 total_cents, evidence, hooks or {})
        except CONFLICTS:
            continue  # another writer committed first: re-read the control row and decide again
    raise PublicationConflict("Could not commit after repeated concurrent conflicts")


def history(spark: SparkSession, path: str) -> DataFrame:
    return (spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
            .select("version", "operation", "userMetadata").orderBy("version"))
