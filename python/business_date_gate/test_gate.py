from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from gate import Gate, Run

DAY = "2026-10-23"
JOBS = ("customer", "product")
NOW = datetime(2026, 10, 23, 7, tzinfo=timezone.utc)


class GateTests(unittest.TestCase):
    def setUp(self):
        self.gate = Gate()

    def tearDown(self):
        self.gate.close()

    def run_record(self, job="customer", day=DAY, attempt=1, state="SUCCESS", started=NOW):
        return Run(job, day, attempt, f"{job}-{day}-{attempt}", state, started)

    def ready(self):
        for job in JOBS:
            self.gate.record(self.run_record(job))

    def test_backfill_finishing_today_does_not_satisfy_current_date(self):
        for job in JOBS:
            self.gate.record(self.run_record(job, "2026-10-16"))
        result = self.gate.check(DAY, JOBS)
        self.assertFalse(result.ready)
        self.assertEqual(result.missing, JOBS)

    def test_current_date_run_started_yesterday_is_eligible(self):
        for job in JOBS:
            self.gate.record(self.run_record(job, started=NOW - timedelta(days=1)))
        self.assertTrue(self.gate.check(DAY, JOBS).ready)

    def test_wrong_job_cannot_replace_a_missing_dependency(self):
        self.gate.record(self.run_record())
        self.gate.record(self.run_record("unrelated"))
        self.assertEqual(self.gate.check(DAY, JOBS).missing, ("product",))

    def test_newer_failed_attempt_blocks_earlier_success(self):
        self.ready()
        self.gate.record(self.run_record(attempt=2, state="FAILED"))
        self.assertEqual(self.gate.check(DAY, JOBS).unsuccessful, (("customer", "FAILED"),))

    def test_running_attempt_blocks_earlier_success(self):
        self.ready()
        self.gate.record(self.run_record(attempt=2, state="RUNNING"))
        self.assertFalse(self.gate.check(DAY, JOBS).ready)

    def test_running_attempt_can_transition_to_success_once(self):
        self.gate.record(self.run_record(state="RUNNING"))
        self.gate.record(self.run_record("product"))
        self.assertFalse(self.gate.check(DAY, JOBS).ready)
        self.assertTrue(self.gate.record(self.run_record()))
        self.assertTrue(self.gate.check(DAY, JOBS).ready)
        self.assertFalse(self.gate.record(self.run_record()))

    def test_canceled_latest_attempt_blocks_publication(self):
        self.ready()
        self.gate.record(self.run_record(attempt=2, state="CANCELED"))
        with self.assertRaisesRegex(ValueError, "not ready"):
            self.gate.publish_total("sales", DAY, JOBS, 17500)

    def test_late_arriving_old_failure_does_not_replace_newer_success(self):
        self.gate.record(self.run_record(attempt=2))
        self.gate.record(self.run_record(state="FAILED"))
        self.gate.record(self.run_record("product"))
        self.assertTrue(self.gate.check(DAY, JOBS).ready)

    def test_identical_replayed_observation_is_noop(self):
        run = self.run_record()
        self.assertTrue(self.gate.record(run))
        self.assertFalse(self.gate.record(run))
        self.assertEqual(self.gate.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1)

    def test_conflicting_state_for_same_attempt_is_rejected(self):
        run = self.run_record()
        self.gate.record(run)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.gate.record(replace(run, state="FAILED"))

    def test_same_run_cannot_claim_two_dates(self):
        run = self.run_record()
        self.gate.record(run)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.gate.record(replace(run, business_date="2026-10-22"))

    def test_equivalent_timezones_are_normalized(self):
        run = self.run_record()
        self.gate.record(run)
        self.assertFalse(self.gate.record(replace(run, started_at=NOW.astimezone(timezone(timedelta(hours=-3))))))

    def test_naive_timestamp_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            self.gate.record(self.run_record(started=NOW.replace(tzinfo=None)))

    def test_invalid_dates_and_attempts_fail(self):
        for value in ("2026-02-30", "20261023", "2026-1-2", ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.gate.record(self.run_record(day=value))
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.gate.record(self.run_record(attempt=value))

    def test_empty_duplicate_and_malformed_dependencies_fail(self):
        for value in ((), ("customer", "customer"), (" customer",), ("",), "customer"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.gate.check(DAY, value)

    def test_unknown_state_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown state"):
            self.gate.record(self.run_record(state="COMPLETED"))

    def test_missing_dependencies_block_publication_without_writes(self):
        with self.assertRaisesRegex(ValueError, "not ready"):
            self.gate.publish_total("sales", DAY, JOBS, 17500)
        self.assertEqual(self.gate.db.execute("SELECT COUNT(*) FROM target_totals").fetchone()[0], 0)

    def test_publication_preserves_exact_selected_run_ids(self):
        self.ready()
        self.assertTrue(self.gate.publish_total("sales", DAY, JOBS, 17500))
        evidence = json.loads(self.gate.db.execute("SELECT evidence_json FROM publications").fetchone()[0])
        self.assertEqual(evidence, [[job, f"{job}-{DAY}-1"] for job in JOBS])

    def test_same_amount_retry_is_noop_even_after_later_failure(self):
        self.ready()
        self.gate.publish_total("sales", DAY, JOBS, 17500)
        original = self.gate.db.execute("SELECT evidence_json FROM publications").fetchone()[0]
        self.gate.record(self.run_record(attempt=2, state="FAILED"))
        self.assertFalse(self.gate.publish_total("sales", DAY, JOBS, 17500))
        self.assertEqual(self.gate.db.execute("SELECT COUNT(*) FROM target_totals").fetchone()[0], 1)
        self.assertEqual(self.gate.db.execute("SELECT evidence_json FROM publications").fetchone()[0], original)

    def test_changed_amount_requires_explicit_repair(self):
        self.ready()
        self.gate.publish_total("sales", DAY, JOBS, 17500)
        with self.assertRaisesRegex(ValueError, "different amount"):
            self.gate.publish_total("sales", DAY, JOBS, 18000)
        self.assertEqual(self.gate.db.execute("SELECT total_cents FROM target_totals").fetchone()[0], 17500)

    def test_changed_dependencies_cannot_reuse_existing_publication(self):
        self.ready()
        self.gate.publish_total("sales", DAY, JOBS, 17500)
        with self.assertRaisesRegex(ValueError, "dependency contract changed"):
            self.gate.publish_total("sales", DAY, ("unrelated",), 17500)

    def test_failure_after_target_insert_rolls_back_everything(self):
        self.ready()
        self.gate.db.execute("""CREATE TRIGGER reject_evidence BEFORE INSERT ON publications
            BEGIN SELECT RAISE(ABORT, 'simulated storage failure'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "simulated"):
            self.gate.publish_total("sales", DAY, JOBS, 17500)
        for table in ("target_totals", "publications"):
            self.assertEqual(self.gate.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.gate.db.execute("DROP TRIGGER reject_evidence")
        self.assertTrue(self.gate.publish_total("sales", DAY, JOBS, 17500))

    def test_consumers_are_independent(self):
        self.ready()
        for consumer in ("sales", "inventory"):
            self.assertTrue(self.gate.publish_total(consumer, DAY, JOBS, 100))
        self.assertEqual(self.gate.db.execute("SELECT COUNT(*) FROM target_totals").fetchone()[0], 2)

    def test_invalid_amounts_are_rejected(self):
        for value in (True, 1.5, "100", 2**63, -(2**63)-1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.gate.publish_total("sales", DAY, JOBS, value)

    def test_concurrent_publishers_create_one_durable_publication(self):
        with TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "gate.sqlite")
            setup = Gate(path)
            try:
                for job in JOBS:
                    setup.record(self.run_record(job))
            finally:
                setup.close()
            def publish(_):
                gate = Gate(path)
                try:
                    return gate.publish_total("sales", DAY, JOBS, 17500)
                finally:
                    gate.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                self.assertEqual(sorted(pool.map(publish, range(2))), [False, True])
            reopened = Gate(path)
            try:
                self.assertEqual(reopened.db.execute("SELECT COUNT(*) FROM publications").fetchone()[0], 1)
                self.assertEqual(reopened.db.execute("SELECT total_cents FROM target_totals").fetchone()[0], 17500)
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
