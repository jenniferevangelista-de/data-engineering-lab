import unittest

import boto3
from moto import mock_aws

from replay import canonical_payload, write_batch


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.addCleanup(self.mock.stop)
        self.s3 = boto3.client("s3", region_name="us-east-1",
                               aws_access_key_id="testing", aws_secret_access_key="testing")
        self.bucket = "jenny-replay-test"
        self.s3.create_bucket(Bucket=self.bucket)
        self.events = [{"id": "o1", "version": 1, "amount_cents": 10000}]

    def write(self, events):
        return write_batch(self.s3, self.bucket, "lab-replay/unit/", "batch-001", events)

    def test_retry_does_not_overwrite(self):
        first = self.write(self.events)
        retry = self.write(self.events)
        self.assertEqual((first["state"], retry["state"]), ("CREATED", "REPLAYED"))
        self.assertEqual(first["sha256"], retry["sha256"])
        self.assertEqual(len(self.s3.list_objects_v2(Bucket=self.bucket)["Contents"]), 1)

    def test_different_payload_same_batch_is_rejected(self):
        self.write(self.events)
        with self.assertRaisesRegex(ValueError, "differs"):
            self.write([{"id": "o1", "version": 1, "amount_cents": 999}])
        self.assertEqual(self.write(self.events)["state"], "REPLAYED")

    def test_exact_event_duplicates_are_deduplicated(self):
        self.assertEqual(self.write(self.events * 2)["unique_events"], 1)

    def test_conflicting_event_versions_fail_before_write(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.write(self.events + [{"id": "o1", "version": 1, "amount_cents": 1}])
        self.assertEqual(self.s3.list_objects_v2(Bucket=self.bucket)["KeyCount"], 0)

    def test_order_and_negative_cents_preserved(self):
        other = {"id": "refund", "version": 2, "amount_cents": -100}
        self.assertEqual(canonical_payload(self.events + [other]), canonical_payload([other] + self.events))

    def test_boolean_and_float_money_rejected(self):
        for amount in (True, 1.2):
            with self.assertRaises(ValueError):
                self.write([{"id": "o1", "version": 1, "amount_cents": amount}])

    def test_scope_and_integrity_fail_closed(self):
        with self.assertRaises(ValueError):
            write_batch(self.s3, self.bucket, "production/", "batch-001", self.events)
        self.write(self.events)
        self.s3.put_object(Bucket=self.bucket, Key="lab-replay/unit/batch-001.json", Body=b"tampered")
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.write(self.events)


if __name__ == "__main__":
    unittest.main()
