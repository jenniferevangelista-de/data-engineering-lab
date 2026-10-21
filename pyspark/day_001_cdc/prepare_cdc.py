"""Validate and reduce a finite CDC batch before a Delta MERGE.

Contract: key STRING, sequence BIGINT, value STRING. Sequence is a source
version, strictly ordered per normalized business key. No writes or streaming.
"""
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import LongType, StringType


def prepare_cdc(batch: DataFrame) -> DataFrame:
    """Return key/sequence/value with one deterministic winner per key.

    Invalid keys/versions and conflicting versions fail the whole batch.
    Exact retries collapse; null payload is allowed and is NOT a delete.
    Upper/ASCII-space trim normalization is a business assumption, not universal.
    Validation launches Spark jobs; use a stable, immutable batch input.
    """
    if batch.isStreaming:
        raise ValueError("Use a finite immutable micro-batch, not a streaming DataFrame")
    if len(batch.columns) != len(set(batch.columns)):
        raise ValueError("Duplicate column names")
    expected = {"key": StringType, "sequence": LongType, "value": StringType}
    fields = {field.name: field.dataType for field in batch.schema.fields}
    if any(name not in fields or not isinstance(fields[name], kind)
           for name, kind in expected.items()):
        raise ValueError("Required schema: key STRING, sequence BIGINT, value STRING")
    clean = batch.select(
        F.upper(F.trim(F.col("key"))).alias("key"), "sequence", "value"
    )
    if clean.where(F.col("key").isNull() | (F.col("key") == "") |
                   F.col("sequence").isNull() | (F.col("sequence") < 0)).limit(1).count():
        raise ValueError("Null/blank key or null/negative sequence")
    unique = clean.dropDuplicates(["key", "sequence", "value"])
    if unique.groupBy("key", "sequence").count().where("count > 1").limit(1).count():
        raise ValueError("Conflicting payloads for the same key and sequence")
    order = Window.partitionBy("key").orderBy(F.col("sequence").desc())
    return (unique.withColumn("_rank", F.row_number().over(order))
            .where("_rank = 1").drop("_rank"))
