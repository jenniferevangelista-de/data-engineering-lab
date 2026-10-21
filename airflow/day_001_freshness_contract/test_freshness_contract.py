import unittest
from datetime import datetime, timedelta, timezone
from freshness_contract import Snapshot, assess_snapshot, require_fresh, FreshnessContractError

T = datetime(2026, 10, 20, 8, tzinfo=timezone.utc)
CONFIG = dict(now=T, required_interval_end=T,
              max_age=timedelta(minutes=90))


def good():
    return Snapshot("orders_hourly", "success", T,
                    T - timedelta(minutes=10), 4, True)


class FreshnessContractTests(unittest.TestCase):
    def test_good_data_passes(self):
        require_fresh(good(), **CONFIG)

    def test_green_dag_but_stale_data(self):
        s = Snapshot("orders_hourly", "success", T, T-timedelta(hours=3), 4, True)
        self.assertIn("event timestamp too old", assess_snapshot(s, **CONFIG))

    def test_prior_interval_rejected(self):
        s = Snapshot("orders_hourly", "success", T-timedelta(hours=1), T, 4, True)
        self.assertIn("data interval behind required interval",
                      assess_snapshot(s, **CONFIG))

    def test_empty_dataset_rejected(self):
        s = Snapshot("orders_hourly", "success", T, T, 0, True)
        self.assertIn("empty dataset", assess_snapshot(s, **CONFIG))

    def test_missing_event_timestamp_rejected(self):
        s = Snapshot("orders_hourly", "success", T, None, 4, True)
        self.assertIn("missing latest event timestamp", assess_snapshot(s, **CONFIG))

    def test_future_timestamp_above_skew_rejected(self):
        s = Snapshot("orders_hourly", "success", T, T+timedelta(minutes=6), 4, True)
        self.assertIn("event timestamp exceeds allowed clock skew",
                      assess_snapshot(s, **CONFIG))

    def test_future_timestamp_within_skew_accepted(self):
        s = Snapshot("orders_hourly", "success", T, T+timedelta(minutes=4), 4, True)
        require_fresh(s, **CONFIG)

    def test_naive_datetimes_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            assess_snapshot(good(), now=datetime(2026, 10, 20, 8),
                            required_interval_end=T, max_age=timedelta(minutes=90))

    def test_quality_breach_and_dag_failure_returned_together(self):
        s = Snapshot("orders_hourly", "failed", T, T, 4, False)
        self.assertEqual(assess_snapshot(s, **CONFIG),
                         ("DAG not successful", "quality checks failed"))
        with self.assertRaises(FreshnessContractError):
            require_fresh(s, **CONFIG)

    def test_invalid_parameters_fail_closed(self):
        with self.assertRaises(ValueError):
            assess_snapshot(good(), now=T, required_interval_end=T,
                            max_age=timedelta(0))
        with self.assertRaises(ValueError):
            assess_snapshot(Snapshot("x", "success", T, T, -1, True), **CONFIG)

    def test_timezone_offset_is_normalized(self):
        minus3 = timezone(timedelta(hours=-3))
        local = T.astimezone(minus3)
        s = Snapshot("orders_hourly", "success", local,
                     local - timedelta(minutes=10), 4, True)
        require_fresh(s, **CONFIG)


if __name__ == "__main__":
    unittest.main()
