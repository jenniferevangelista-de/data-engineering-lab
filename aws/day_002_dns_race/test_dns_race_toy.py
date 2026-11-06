import unittest
from dns_race_toy import (Endpoint, InconsistentState, check_newer, cleanup,
                          guarded_run, naive_run, planner, safe_cleanup,
                          write_if_newer, write_plan)


def fresh(n=10):
    ep = Endpoint()
    planner(ep, range(1, n + 1))
    return ep


class NaiveRace(unittest.TestCase):
    def test_stale_check_passed_at_start(self):
        self.assertTrue(check_newer(fresh(), 1))

    def test_race_leaves_empty_record(self):
        ep = naive_run()
        self.assertEqual(ep.active, 1)
        self.assertEqual(ep.record(), ())

    def test_active_plan_was_deleted(self):
        self.assertNotIn(1, naive_run().plans)

    def test_next_update_is_stuck(self):
        with self.assertRaisesRegex(InconsistentState, "active plan 1 was deleted"):
            write_plan(naive_run(), 11)

    def test_cleanup_keeps_recent_generations(self):
        ep = fresh()
        write_plan(ep, 10)
        cleanup(ep, newest=10)
        self.assertEqual(sorted(ep.plans), [8, 9, 10])


class Guarded(unittest.TestCase):
    def test_late_old_write_is_refused(self):
        _, late = guarded_run()
        self.assertFalse(late)

    def test_record_stays_on_newest_plan(self):
        ep, _ = guarded_run()
        self.assertEqual(ep.active, 10)
        self.assertEqual(ep.record(), ("10.0.10.1", "10.0.10.2"))

    def test_equal_generation_is_refused(self):
        ep = fresh()
        write_if_newer(ep, 10)
        self.assertFalse(write_if_newer(ep, 10))

    def test_newer_generation_is_accepted(self):
        ep = fresh(11)
        write_if_newer(ep, 10)
        self.assertTrue(write_if_newer(ep, 11))
        self.assertEqual(ep.active, 11)

    def test_safe_cleanup_never_deletes_live_plan(self):
        ep = fresh()
        ep.active = 1  # even if a stale plan got through
        safe_cleanup(ep, newest=10)
        self.assertIn(1, ep.plans)
        self.assertEqual(ep.record(), ("10.0.1.1", "10.0.1.2"))

    def test_safe_cleanup_still_removes_old_plans(self):
        ep, _ = guarded_run()
        self.assertEqual(sorted(ep.plans), [8, 9, 10])

    def test_empty_endpoint_has_empty_record(self):
        self.assertEqual(Endpoint().record(), ())


if __name__ == "__main__":
    unittest.main(verbosity=2)
