"""Run the published SQL on a real local Spark JVM with synthetic inputs."""
from pathlib import Path
import hashlib
import unittest
from pyspark.sql import SparkSession

QUERIES = Path(__file__).with_name('queries')

class CommunitySqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]')
                     .appName('community-sql-contracts')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '2')
                     .config('spark.sql.session.timeZone', 'UTC')
                     .config('spark.sql.ansi.enabled', 'true').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        print('Spark runtime:', cls.spark.version, flush=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def query(self, name):
        return self.spark.sql((QUERIES/(name+'.sql')).read_text(encoding='utf-8'))

    def view(self, values, alias):
        self.spark.sql('SELECT * FROM VALUES '+values+' AS '+alias).createOrReplaceTempView(alias.split('(')[0])

    def hashes(self, values):
        self.view(values, 'source_keys(k1,k2)')
        return [r.business_key_hash for r in self.query('business_key_hash').collect()]

    def test_delimiter_boundaries_do_not_alias(self):
        hashes = self.hashes("('A||B','C'),('A','B||C')")
        self.assertNotEqual(hashes[0], hashes[1])

    def test_null_position_and_literal_null_are_distinct(self):
        hashes = self.hashes("(CAST(NULL AS STRING),'A'),('A',NULL),('null','A'),('','A')")
        self.assertEqual(len(set(hashes)),4)

    def test_canonical_payload_and_normalization(self):
        hashes = self.hashes("(' a ','b'),('A','B')")
        expected = hashlib.sha256(b'{"k1":"A","k2":"B"}').hexdigest()
        self.assertEqual(hashes,[expected,expected])

    def test_merge_collision_uses_normalized_key_and_filter(self):
        self.view("(' a ','b','c','A'),('A','B','C','A'),('A','B','C','B')", 'source_table(col4,col5,col6,col3)')
        rows=self.query('normalized_merge_collisions').collect()
        self.assertEqual([tuple(r) for r in rows],[('A','B','C',2)])

    def test_unique_source_has_no_collisions(self):
        self.view("('A','B','C','A'),('D','B','C','A')",'source_table(col4,col5,col6,col3)')
        self.assertEqual(self.query('normalized_merge_collisions').count(),0)

    def test_second_and_third_components_are_not_trimmed(self):
        self.view("('A',' B','C','A'),('A','B','C','A')",'source_table(col4,col5,col6,col3)')
        self.assertEqual(self.query('normalized_merge_collisions').count(),0)

    def test_change_counts_exclude_update_preimage(self):
        self.view("(TIMESTAMP '2026-10-18 10:01:00','insert'),(TIMESTAMP '2026-10-18 10:02:00','update_preimage'),(TIMESTAMP '2026-10-18 10:02:00','update_postimage'),(TIMESTAMP '2026-10-18 10:03:00','delete')",'changes(_commit_timestamp,_change_type)')
        row=self.query('hourly_change_counts').first()
        self.assertEqual((row.inserts,row.updates,row.deletes,row.unknown_types),(1,1,1,0))
        self.assertEqual(row.commit_hour.minute,0)

    def test_separate_hours_and_unknown_operations(self):
        self.view("(TIMESTAMP '2026-10-18 10:01:00','insert'),(TIMESTAMP '2026-10-18 11:02:00','unexpected'),(TIMESTAMP '2026-10-18 11:03:00',NULL)",'changes(_commit_timestamp,_change_type)')
        rows=self.query('hourly_change_counts').orderBy('commit_hour').collect()
        self.assertEqual(len(rows),2)
        self.assertEqual((rows[0].inserts,rows[1].unknown_types),(1,2))

    def test_missing_metric_is_unknown_and_map_is_preserved(self):
        self.spark.sql("SELECT map('executionTimeMs','100','numSourceRows','2') operationMetrics").createOrReplaceTempView('history')
        row=self.query('optional_operation_metrics').first()
        self.assertEqual((row.execution_time_ms,row.source_rows),(100,2))
        self.assertIsNone(row.scan_time_ms)
        self.assertEqual(row.operationMetrics,{'executionTimeMs':'100','numSourceRows':'2'})

    def test_measured_zero_and_unavailable_are_distinct(self):
        self.spark.sql("SELECT map('scanTimeMs','0') operationMetrics").createOrReplaceTempView('history')
        row=self.query('optional_operation_metrics').first()
        self.assertEqual(row.scan_time_ms,0)
        self.assertIsNone(row.execution_time_ms)

if __name__ == '__main__':
    unittest.main(verbosity=2)
