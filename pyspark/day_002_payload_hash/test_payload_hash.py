"""Behavior tests run against a real local Spark JVM; no mocks."""
import hashlib
import unittest
from pyspark.sql import SparkSession
from payload_hash import conflict_report, payload_hash


class PayloadHashTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("jenny-payload-hash-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        print("RUNTIME Spark=" + cls.spark.version, flush=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, values, schema="key string, sequence long, value string"):
        return self.spark.createDataFrame(values, schema)

    def test_hash_matches_python_sha256_of_the_json(self):
        row = self.frame([("A", 1, "x")]).select(payload_hash(["value"]).alias("h")).first()
        self.assertEqual(row.h, hashlib.sha256(b'{"value":"x"}').hexdigest())

    def test_exact_retry_is_not_a_conflict(self):
        batch = self.frame([("A", 2, "new"), ("A", 2, "new")])
        self.assertEqual(conflict_report(batch).count(), 0)

    def test_different_payload_is_reported_with_values_and_hashes(self):
        batch = self.frame([("A", 2, "one"), ("A", 2, "two"), ("B", 1, "ok")])
        rows = conflict_report(batch).collect()
        self.assertEqual({(r.key, r.sequence, r.value) for r in rows},
                         {("A", 2, "one"), ("A", 2, "two")})
        self.assertEqual({r.payload_hash for r in rows},
                         {hashlib.sha256(('{"value":"%s"}' % v).encode()).hexdigest()
                          for v in ("one", "two")})

    def test_float_noise_conflicts_unless_rounded(self):
        schema = "key string, sequence long, amount double"
        batch = self.frame([("A", 1, 0.1 + 0.2), ("A", 1, 0.3)], schema)
        self.assertEqual(conflict_report(batch, values=["amount"]).count(), 2)
        self.assertEqual(
            conflict_report(batch, values=["amount"], scales={"amount": 2}).count(), 0)

    def test_real_change_survives_rounding(self):
        schema = "key string, sequence long, amount double"
        batch = self.frame([("A", 1, 0.30), ("A", 1, 0.31)], schema)
        self.assertEqual(
            conflict_report(batch, values=["amount"], scales={"amount": 2}).count(), 2)

    def test_columns_outside_values_are_ignored(self):
        schema = "key string, sequence long, value string, extra string"
        batch = self.frame([("A", 2, "new", None), ("A", 2, "new", "x")], schema)
        self.assertEqual(batch.distinct().count(), 2)
        self.assertEqual(conflict_report(batch).count(), 0)

    def test_null_and_empty_string_differ(self):
        batch = self.frame([("A", 2, None), ("A", 2, "")])
        self.assertEqual(conflict_report(batch).count(), 2)

    def test_column_order_changes_the_hash(self):
        batch = self.frame([("A", 1, "x", "y")],
                           "key string, sequence long, a string, b string")
        row = batch.select(payload_hash(["a", "b"]).alias("ab"),
                           payload_hash(["b", "a"]).alias("ba")).first()
        self.assertNotEqual(row.ab, row.ba)

    def test_invalid_arguments(self):
        batch = self.frame([("A", 1, "x")])
        with self.assertRaisesRegex(ValueError, "Unknown columns"):
            conflict_report(batch, values=["nope"])
        with self.assertRaisesRegex(ValueError, "non-empty"):
            conflict_report(batch, values=[])
        with self.assertRaisesRegex(ValueError, "float, double or decimal"):
            conflict_report(batch, values=["value"], scales={"value": 2})
        with self.assertRaisesRegex(ValueError, "listed in values"):
            conflict_report(batch, values=["value"], scales={"sequence": 0})

    def test_empty_batch(self):
        batch = self.frame([], "key string, sequence long, value string")
        self.assertEqual(conflict_report(batch).count(), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
