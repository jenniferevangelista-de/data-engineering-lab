"""Portable freshness guard; no Airflow integration is exercised.

Designed as a pure function to call from an Airflow task after publishing data.
All timestamps must be timezone-aware UTC instants.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class Snapshot:
    dataset: str
    dag_state: str
    data_interval_end: datetime
    latest_event_at: datetime | None
    row_count: int
    quality_checks_passed: bool


class FreshnessContractError(ValueError):
    pass


def _utc_instant(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def assess_snapshot(snapshot: Snapshot, *, now: datetime,
                    required_interval_end: datetime,
                    max_age: timedelta,
                    future_skew: timedelta = timedelta(minutes=5)) -> tuple[str, ...]:
    """Return all breaches; a green DAG alone is insufficient.

    Contract assumes the dataset is expected to contain recent event timestamps.
    Not suitable without adaptation for naturally sparse/low-frequency streams.
    """
    now = _utc_instant(now, "now")
    required_end = _utc_instant(required_interval_end, "required_interval_end")
    actual_end = _utc_instant(snapshot.data_interval_end, "data_interval_end")
    if max_age <= timedelta(0) or future_skew < timedelta(0):
        raise ValueError("max_age must be positive; future_skew nonnegative")
    if snapshot.row_count < 0:
        raise ValueError("row_count must be >= 0")
    violations: list[str] = []
    if snapshot.dag_state != "success":
        violations.append("DAG not successful")
    if actual_end < required_end:
        violations.append("data interval behind required interval")
    if snapshot.row_count == 0:
        violations.append("empty dataset")
    if not snapshot.quality_checks_passed:
        violations.append("quality checks failed")
    if snapshot.latest_event_at is None:
        violations.append("missing latest event timestamp")
    else:
        event = _utc_instant(snapshot.latest_event_at, "latest_event_at")
        if event > now + future_skew:
            violations.append("event timestamp exceeds allowed clock skew")
        if now - event > max_age:
            violations.append("event timestamp too old")
    return tuple(violations)


def require_fresh(snapshot: Snapshot, **kwargs) -> None:
    violations = assess_snapshot(snapshot, **kwargs)
    if violations:
        raise FreshnessContractError(
            f"{snapshot.dataset}: " + "; ".join(violations)
        )


if __name__ == "__main__":
    t = datetime(2026, 10, 20, 8, tzinfo=timezone.utc)
    sample = Snapshot("orders_hourly", "success", t,
                      t - timedelta(hours=2), 100, True)
    print(assess_snapshot(sample, now=t, required_interval_end=t,
                          max_age=timedelta(minutes=75)))
