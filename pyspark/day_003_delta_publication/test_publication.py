"""Behavior tests run against a real local Spark JVM with Delta; no mocks."""
import json
import os
import tempfile
import threading
import unittest

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from publication import PublicationConflict, history, init_tables, publish

DATE = "2026-10-24"
RUNS = ["run-b", "run-a"]


class Crash(Exception):
    pass


def crash():
    raise Crash()


class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (SparkSession.builder.master("local[2]")
                   .appName("jenny-delta-publication-test")
                   .config("spark.ui.enabled", "false")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        print("RUNTIME Spark=" + cls.spark.version, flush=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.control = os.path.join(self.tmp.name, "control")
        self.target = os.path.join(self.tmp.name, "target")
        init_tables(self.spark, self.control, self.target)

    def pub(self, total=1000, runs=RUNS, consumer="finance", date=DATE, spark=None, **kw):
        return publish(spark or self.spark, self.control, self.target, consumer, date,
                       total, runs, **kw)

    def rows(self, path):
        frame = self.spark.read.format("delta").load(path)
        return sorted(tuple(r) for r in frame.collect())

    def versions(self, path):
        return history(self.spark, path).count()

    def test_first_publication_writes_target_and_evidence(self):
        self.assertEqual(self.pub(), "published")
        self.assertEqual(self.rows(self.target), [("finance", DATE, 1000)])
        control = self.rows(self.control)
        self.assertEqual([(c, d, t, s) for c, d, t, _, s in control],
                         [("finance", DATE, 1000, "PUBLISHED")])
        merge = history(self.spark, self.target).where("operation = 'MERGE'").collect()
        self.assertEqual(len(merge), 1)
        self.assertEqual(json.loads(merge[0].userMetadata),
                         {"business_date": DATE, "run_ids": ["run-a", "run-b"]})
        self.assertIsNone(self.spark.conf.get("spark.databricks.delta.commitInfo.userMetadata",
                                              None))

    def test_same_publication_again_is_a_noop(self):
        self.pub()
        before = (self.versions(self.target), self.versions(self.control))
        self.assertEqual(self.pub(runs=["run-a", "run-b"]), "retry")
        self.assertEqual((self.versions(self.target), self.versions(self.control)), before)

    def test_different_amount_is_rejected_and_changes_nothing(self):
        self.pub()
        before = (self.rows(self.target), self.rows(self.control),
                  self.versions(self.target), self.versions(self.control))
        with self.assertRaisesRegex(PublicationConflict, "already frozen"):
            self.pub(total=2000)
        self.assertEqual((self.rows(self.target), self.rows(self.control),
                          self.versions(self.target), self.versions(self.control)), before)

    def test_different_dependency_set_is_rejected(self):
        self.pub()
        with self.assertRaisesRegex(PublicationConflict, "already frozen"):
            self.pub(runs=["run-a", "run-c"])

    def test_crash_after_record_is_recovered(self):
        with self.assertRaises(Crash):
            self.pub(hooks={"after_record": crash})
        self.assertEqual(self.rows(self.target), [])
        self.assertEqual([r[4] for r in self.rows(self.control)], ["PENDING"])
        self.assertEqual(self.pub(), "published")
        self.assertEqual(self.rows(self.target), [("finance", DATE, 1000)])
        self.assertEqual([r[4] for r in self.rows(self.control)], ["PUBLISHED"])

    def test_crash_after_target_write_is_recovered_without_a_second_write(self):
        with self.assertRaises(Crash):
            self.pub(hooks={"after_target": crash})
        written = self.versions(self.target)
        self.assertEqual([r[4] for r in self.rows(self.control)], ["PENDING"])
        self.assertEqual(self.pub(), "published")
        self.assertEqual(self.versions(self.target), written)
        self.assertEqual(self.rows(self.target), [("finance", DATE, 1000)])
        self.assertEqual([r[4] for r in self.rows(self.control)], ["PUBLISHED"])

    def test_recovery_still_rejects_a_different_amount(self):
        with self.assertRaises(Crash):
            self.pub(hooks={"after_record": crash})
        with self.assertRaisesRegex(PublicationConflict, "already frozen"):
            self.pub(total=999)

    def test_other_consumers_and_dates_are_independent(self):
        self.pub()
        self.pub(consumer="marketing")
        self.pub(date="2026-10-25", total=5)
        self.assertEqual(self.rows(self.target),
                         [("finance", DATE, 1000), ("finance", "2026-10-25", 5),
                          ("marketing", DATE, 1000)])

    def test_two_concurrent_publishers_leave_one_row(self):
        for attempt in range(3):
            consumer = f"c{attempt}"
            barrier = threading.Barrier(2)
            results, errors = [], []

            def worker():
                session = self.spark.newSession()  # userMetadata is session-scoped
                barrier.wait()
                try:
                    results.append(self.pub(consumer=consumer, spark=session))
                except Exception as error:  # report, do not hide, in the assertion below
                    errors.append(repr(error))

            threads = [threading.Thread(target=worker) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])
            self.assertTrue(set(results) <= {"published", "retry"})
            control = [r for r in self.rows(self.control) if r[0] == consumer]
            target = [r for r in self.rows(self.target) if r[0] == consumer]
            self.assertEqual([r[4] for r in control], ["PUBLISHED"])
            self.assertEqual(target, [(consumer, DATE, 1000)])

    def test_invalid_input(self):
        for kwargs in [{"consumer": " x"}, {"date": "2026-1-1"}, {"total": -1},
                       {"total": True}, {"runs": []}, {"runs": ["a", "a"]}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.pub(**kwargs)
        self.assertEqual(self.rows(self.control), [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
