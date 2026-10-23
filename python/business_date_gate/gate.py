"""Business-date readiness and an atomic local SQLite publication example."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import sqlite3


@dataclass(frozen=True)
class Run:
    job_id: str
    business_date: str
    attempt: int
    run_id: str
    state: str
    started_at: datetime


@dataclass(frozen=True)
class Readiness:
    evidence: tuple[tuple[str, str], ...]
    missing: tuple[str, ...]
    unsuccessful: tuple[tuple[str, str], ...]

    @property
    def ready(self) -> bool:
        return not self.missing and not self.unsuccessful


def _text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be a nonempty, trimmed string")
    return value


def _date(value: str) -> str:
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("business_date must be YYYY-MM-DD")
    return value


class Gate:
    """One date per run; attempt ordinals come from a trusted producer.

    The greatest attempt wins, including RUNNING and FAILED attempts.
    Identity is immutable. RUNNING may become terminal once; terminal results
    cannot be rewritten. Replays of identical observations are no-ops.
    """

    def __init__(self, database: str = ":memory:") -> None:
        self.db = sqlite3.connect(database, isolation_level=None, timeout=10)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                job_id TEXT NOT NULL, business_date TEXT NOT NULL,
                attempt INTEGER NOT NULL, run_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL, started_at TEXT NOT NULL,
                PRIMARY KEY (job_id, business_date, attempt)
            );
            CREATE TABLE IF NOT EXISTS target_totals (
                consumer TEXT NOT NULL, business_date TEXT NOT NULL,
                total_cents INTEGER NOT NULL,
                PRIMARY KEY (consumer, business_date)
            );
            CREATE TABLE IF NOT EXISTS publications (
                consumer TEXT NOT NULL, business_date TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                PRIMARY KEY (consumer, business_date),
                FOREIGN KEY (consumer, business_date)
                    REFERENCES target_totals (consumer, business_date)
            );
        """)

    def close(self) -> None:
        self.db.close()

    def record(self, run: Run) -> bool:
        job = _text(run.job_id, "job_id")
        business_date = _date(run.business_date)
        run_id = _text(run.run_id, "run_id")
        if type(run.attempt) is not int or run.attempt < 1:
            raise ValueError("attempt must be a positive integer")
        if run.state not in {"SUCCESS", "FAILED", "RUNNING", "CANCELED"}:
            raise ValueError("unknown state")
        if not isinstance(run.started_at, datetime) or run.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        values = (job, business_date, run.attempt, run_id, run.state,
                  run.started_at.astimezone(timezone.utc).isoformat())
        try:
            self.db.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?)", values)
            return True
        except sqlite3.IntegrityError:
            existing = self.db.execute(
                "SELECT * FROM runs WHERE job_id=? AND business_date=? AND attempt=?",
                values[:3],
            ).fetchone()
            if existing == values:
                return False
            if (existing is not None and existing[:4] == values[:4]
                    and existing[5] == values[5] and existing[4] == "RUNNING"
                    and run.state != "RUNNING"):
                updated = self.db.execute("""
                    UPDATE runs SET state=?
                    WHERE job_id=? AND business_date=? AND attempt=? AND state='RUNNING'
                """, (run.state, job, business_date, run.attempt))
                if updated.rowcount == 1:
                    return True
                if self.db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone() == values:
                    return False
            raise ValueError("conflicting run identity or attempt") from None

    def check(self, business_date: str, required_jobs: tuple[str, ...]) -> Readiness:
        business_date = _date(business_date)
        if not isinstance(required_jobs, tuple) or not required_jobs:
            raise ValueError("required_jobs must be a nonempty tuple")
        for job in required_jobs:
            _text(job, "required job")
        if len(set(required_jobs)) != len(required_jobs):
            raise ValueError("required jobs must be unique")
        evidence, missing, unsuccessful = [], [], []
        for job in sorted(required_jobs):
            latest = self.db.execute("""
                SELECT run_id, state FROM runs
                WHERE job_id=? AND business_date=?
                ORDER BY attempt DESC LIMIT 1
            """, (job, business_date)).fetchone()
            if latest is None:
                missing.append(job)
            elif latest[1] != "SUCCESS":
                unsuccessful.append((job, latest[1]))
            else:
                evidence.append((job, latest[0]))
        return Readiness(tuple(evidence), tuple(missing), tuple(unsuccessful))

    def publish_total(self, consumer: str, business_date: str,
                      required_jobs: tuple[str, ...], total_cents: int) -> bool:
        """Publish one local total and its evidence in the same transaction.

        Replaying the same output is a no-op; changing an already published
        amount requires a separate, explicit repair policy.
        """
        consumer = _text(consumer, "consumer")
        business_date = _date(business_date)
        if type(total_cents) is not int or not -(2**63) <= total_cents < 2**63:
            raise ValueError("total_cents must be a signed 64-bit integer")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            # Validate the dependency contract even on a replay.
            readiness = self.check(business_date, required_jobs)
            existing = self.db.execute("""
                SELECT total_cents FROM target_totals
                WHERE consumer=? AND business_date=?
            """, (consumer, business_date)).fetchone()
            if existing is not None:
                original = self.db.execute("""
                    SELECT evidence_json FROM publications
                    WHERE consumer=? AND business_date=?
                """, (consumer, business_date)).fetchone()
                if original is None or tuple(job for job, _ in json.loads(original[0])) != tuple(sorted(required_jobs)):
                    raise ValueError("publication dependency contract changed or evidence missing")
                if existing[0] != total_cents:
                    raise ValueError("publication already exists with a different amount")
                self.db.execute("COMMIT")
                return False
            if not readiness.ready:
                raise ValueError(f"dependencies not ready: {readiness}")
            self.db.execute("INSERT INTO target_totals VALUES (?, ?, ?)",
                            (consumer, business_date, total_cents))
            self.db.execute("INSERT INTO publications VALUES (?, ?, ?)",
                            (consumer, business_date, json.dumps(readiness.evidence)))
            self.db.execute("COMMIT")
            return True
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise


def demo() -> None:
    gate = Gate()
    try:
        jobs = ("dim_customer", "dim_product")
        started = datetime(2026, 10, 23, 7, tzinfo=timezone.utc)
        for job in jobs:
            gate.record(Run(job, "2026-10-16", 1, job + "-backfill", "SUCCESS", started))
        print("Backfill completed today:", gate.check("2026-10-23", jobs).ready)
        for job in jobs:
            gate.record(Run(job, "2026-10-23", 1, job + "-current", "SUCCESS", started))
        print("Required business date ready:", gate.check("2026-10-23", jobs).ready)
        print("First publication:", gate.publish_total("daily_sales", "2026-10-23", jobs, 17500))
        print("Same publication retried:", gate.publish_total("daily_sales", "2026-10-23", jobs, 17500))
    finally:
        gate.close()


if __name__ == "__main__":
    demo()
