import unittest
from join_guard import make_connection, seed_broken_example, inspect_join, require_one_to_one


class JoinAuditTests(unittest.TestCase):
    def setUp(self):
        self.db = make_connection()

    def tearDown(self):
        self.db.close()

    def test_equal_row_counts_hide_bad_join(self):
        seed_broken_example(self.db)
        a = inspect_join(self.db)
        self.assertEqual((a.source_rows, a.joined_rows), (3, 3))
        self.assertEqual((a.source_amount_cents, a.joined_amount_cents),
                         (17500, 25000))
        self.assertFalse(a.valid)

    def test_duplicate_and_missing_keys_reported(self):
        seed_broken_example(self.db)
        a = inspect_join(self.db)
        self.assertEqual(a.duplicate_dimension_keys, ("c1",))
        self.assertEqual(a.unmatched_order_ids, ("o3",))

    def test_gate_rejects_problem_case(self):
        seed_broken_example(self.db)
        with self.assertRaisesRegex(ValueError, "Unsafe join"):
            require_one_to_one(self.db)

    def test_unique_complete_dimension_passes(self):
        self.db.executemany("INSERT INTO orders VALUES (?, ?, ?)",
                            [("o1", "c1", 100), ("o2", "c2", 0)])
        self.db.executemany("INSERT INTO customer_profiles VALUES (?, ?, ?)",
                            [("c1", 2, "gold"), ("c2", 1, "basic")])
        a = require_one_to_one(self.db)
        self.assertTrue(a.valid)
        self.assertEqual(a.source_amount_cents, 100)

    def test_only_orphan_is_detected(self):
        self.db.execute("INSERT INTO orders VALUES ('o1', 'missing', 100)")
        a = inspect_join(self.db)
        self.assertEqual(a.unmatched_order_ids, ("o1",))
        self.assertEqual(a.duplicate_dimension_keys, ())
        self.assertFalse(a.valid)

    def test_only_fanout_is_detected(self):
        self.db.execute("INSERT INTO orders VALUES ('o1', 'c1', 100)")
        self.db.executemany("INSERT INTO customer_profiles VALUES (?, ?, ?)",
                            [("c1", 1, "a"), ("c1", 2, "b")])
        a = inspect_join(self.db)
        self.assertEqual(a.joined_rows, 2)
        self.assertEqual(a.joined_amount_cents, 200)
        self.assertFalse(a.valid)

    def test_empty_inputs_are_well_defined(self):
        a = require_one_to_one(self.db)
        self.assertEqual(a.source_rows, 0)
        self.assertEqual(a.source_amount_cents, 0)
        self.assertTrue(a.valid)

    def test_refund_amounts_stay_in_integer_cents(self):
        self.db.execute("INSERT INTO orders VALUES ('refund1', 'c1', -125)")
        self.db.execute("INSERT INTO customer_profiles VALUES ('c1', 1, 'basic')")
        a = require_one_to_one(self.db)
        self.assertEqual(a.source_amount_cents, -125)
        self.assertEqual(a.joined_amount_cents, -125)


if __name__ == "__main__":
    unittest.main()
