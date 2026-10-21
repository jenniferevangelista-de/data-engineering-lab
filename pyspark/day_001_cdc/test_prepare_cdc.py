"""Behavior tests run against a real local Spark JVM; no mocks."""
import unittest
from pyspark.sql import SparkSession
from prepare_cdc import prepare_cdc


class CdcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("jenny-cdc-contract-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        print("RUNTIME Spark=" + cls.spark.version, flush=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def data(self, values):
        return self.spark.sql("SELECT * FROM VALUES " + values +
                              " AS events(key, sequence, value)")

    def rows(self, frame):
        return [tuple(r) for r in frame.orderBy("key").collect()]

    def test_latest_normalized_and_retry(self):
        source = self.data("(' a ', 1L, 'old'), ('A', 2L, 'new'), "
                           "('A', 2L, 'new'), ('b', 7L, 'other')")
        self.assertEqual(self.rows(prepare_cdc(source)),
                         [('A', 2, 'new'), ('B', 7, 'other')])

    def test_repartition_and_idempotence(self):
        source = self.data("('a', 1L, 'old'), ('A', 2L, 'new'), ('b', 1L, 'x')")
        result = prepare_cdc(source)
        self.assertEqual(self.rows(result), self.rows(prepare_cdc(source.repartition(3))))
        self.assertEqual(self.rows(result), self.rows(prepare_cdc(result)))

    def test_conflicting_version_rejected(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            prepare_cdc(self.data("('a', 2L, 'one'), (' A ', 2L, 'two')"))

    def test_null_payload_conflict_detected(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            prepare_cdc(self.data("('a', 2L, CAST(NULL AS STRING)), ('a', 2L, 'x')"))

    def test_invalid_keys_and_versions(self):
        for values in ["(CAST(NULL AS STRING), 1L, 'x')", "('   ', 1L, 'x')",
                       "('a', CAST(NULL AS BIGINT), 'x')", "('a', -1L, 'x')"]:
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "Null/blank"):
                prepare_cdc(self.data(values))

    def test_empty(self):
        empty = self.spark.sql("SELECT CAST(NULL AS STRING) key, "
                               "CAST(NULL AS BIGINT) sequence, "
                               "CAST(NULL AS STRING) value WHERE false")
        self.assertEqual(self.rows(prepare_cdc(empty)), [])

    def test_null_payload_allowed(self):
        self.assertEqual(self.rows(prepare_cdc(self.data(
            "('a', 1L, CAST(NULL AS STRING)), ('a', 1L, CAST(NULL AS STRING))"))),
            [('A', 1, None)])

    def test_wrong_schema(self):
        with self.assertRaisesRegex(ValueError, "Required schema"):
            prepare_cdc(self.spark.sql("SELECT 'a' key, 1 sequence, 'x' value"))


if __name__ == '__main__':
    unittest.main(verbosity=2)
