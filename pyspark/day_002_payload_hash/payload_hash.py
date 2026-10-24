"""Compare CDC payloads through a canonical hash of explicit value columns.

Contract: the caller names the value columns, in a fixed order, and may name a
rounding scale for float/decimal columns. Keys are assumed to be normalized and
validated already (see day_001_cdc). No writes, no streaming.
"""
from typing import Dict, Optional, Sequence

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType


def _check(frame: DataFrame, values: Sequence[str], scales: Dict[str, int]) -> None:
    if not values or len(set(values)) != len(values):
        raise ValueError("values must be a non-empty list without repeated names")
    types = {field.name: field.dataType for field in frame.schema.fields}
    missing = [name for name in list(values) + list(scales) if name not in types]
    if missing:
        raise ValueError("Unknown columns: " + ", ".join(sorted(set(missing))))
    if any(name not in values for name in scales):
        raise ValueError("A rounding scale needs a column listed in values")
    if any(not isinstance(types[name], (FloatType, DoubleType, DecimalType))
           for name in scales):
        raise ValueError("Rounding scales apply only to float, double or decimal columns")


def payload_hash(values: Sequence[str], scales: Optional[Dict[str, int]] = None) -> Column:
    """Return a sha256 hex Column over to_json(struct(values...)).

    Column order matters: the same columns in another order hash differently,
    so keep the list fixed in code. Columns outside `values` never influence it.
    Rounding is applied before hashing; it absorbs float noise, not real changes.
    """
    scales = scales or {}
    fields = [F.round(F.col(name), scales[name]).alias(name) if name in scales
              else F.col(name).alias(name) for name in values]
    return F.sha2(F.to_json(F.struct(*fields)), 256)


def conflict_report(batch: DataFrame, key: str = "key", sequence: str = "sequence",
                    values: Sequence[str] = ("value",),
                    scales: Optional[Dict[str, int]] = None) -> DataFrame:
    """List payloads that disagree for the same key and sequence.

    One row per distinct hash, with the raw value columns beside the hash so an
    investigator can see what differs. Empty result means no conflict.
    """
    if batch.isStreaming:
        raise ValueError("Use a finite immutable batch, not a streaming DataFrame")
    scales = scales or {}
    _check(batch, values, scales)
    hashed = (batch.select(key, sequence, *values,
                           payload_hash(values, scales).alias("payload_hash"))
              .dropDuplicates([key, sequence, "payload_hash"]))
    conflicting = (hashed.groupBy(key, sequence).count()
                   .where("count > 1").select(key, sequence))
    return (hashed.join(conflicting, [key, sequence], "inner")
            .orderBy(key, sequence, "payload_hash"))
