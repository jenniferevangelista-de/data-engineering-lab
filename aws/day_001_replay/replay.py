"""Immutable S3 batches: retries preserve a prior payload rather than overwrite it."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from botocore.exceptions import ClientError


def canonical_payload(events):
    selected = {}
    for event in events:
        if (not isinstance(event, dict) or set(event) != {"id", "version", "amount_cents"}
                or not isinstance(event["id"], str) or not event["id"]
                or type(event["version"]) is not int or event["version"] < 1
                or type(event["amount_cents"]) is not int):
            raise ValueError("Expected id, positive integer version and integer amount_cents")
        key = event["id"], event["version"]
        if key in selected and selected[key] != event:
            raise ValueError("Conflicting payload for the same id/version")
        selected[key] = event
    return json.dumps([selected[k] for k in sorted(selected)], sort_keys=True,
                      separators=(",", ":")).encode()


def write_batch(s3, bucket, prefix, batch_id, events):
    if not re.fullmatch(r"lab-replay/[a-zA-Z0-9_-]+/", prefix):
        raise ValueError("Use an isolated lab-replay/<run-id>/ prefix")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", batch_id):
        raise ValueError("Unsafe batch id")
    body = canonical_payload(events)
    digest = hashlib.sha256(body).hexdigest()
    key = prefix + batch_id + ".json"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*",
                      ContentType="application/json", Metadata={"sha256": digest})
        state = "CREATED"
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise
        state = "REPLAYED"
    actual = s3.get_object(Bucket=bucket, Key=key)
    stored = actual["Body"].read()
    if stored != body or actual.get("Metadata", {}).get("sha256") != digest:
        raise ValueError("Existing batch differs or failed integrity verification")
    return {"state": state, "key": key, "sha256": digest,
            "unique_events": len(json.loads(stored))}


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--events", type=Path, required=True)
    args = parser.parse_args()
    client = boto3.Session(profile_name=args.profile, region_name=args.region).client("s3")
    events = json.loads(args.events.read_text(encoding="utf-8"))
    prefix = f"lab-replay/{args.run_id}/"
    first = write_batch(client, args.bucket, prefix, "batch-001", events)
    second = write_batch(client, args.bucket, prefix, "batch-001", events)
    assert second["state"] == "REPLAYED"
    print(json.dumps({"scope": "REAL_AWS_S3", "first": first, "retry": second}, indent=2))


if __name__ == "__main__":
    main()
