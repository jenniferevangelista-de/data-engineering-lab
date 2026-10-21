"""Encode native JSON values for explicitly designated VARIANT fields.

Pure Python serialization only: no HTTP client, credentials or service calls.
"""
import json
from collections.abc import Collection, Mapping
from typing import Any


def encode_variant_fields(
    record: Mapping[str, Any],
    variant_columns: Collection[str] = frozenset({"ValueJson"}),
) -> dict[str, Any]:
    """Return a new record; always JSON-encode designated fields, including strings.

    Input values must be native JSON-compatible Python values. Already serialized
    JSON is NOT detected automatically: decode it explicitly before calling.
    None becomes the JSON text 'null', not SQL NULL. Non-finite floats are rejected.
    Other columns are passed through unchanged; this is not full schema validation.
    """
    return {
        key: json.dumps(value, allow_nan=False) if key in variant_columns else value
        for key, value in record.items()
    }
