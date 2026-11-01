"""Idempotent DynamoDB put behavior for deterministic slot IDs."""
import os
import unittest
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
for key, value in {
    "DB_HOST_PARAMETER": "/test/host", "DB_PORT_PARAMETER": "/test/port",
    "DB_NAME_PARAMETER": "/test/name", "DB_USERNAME_PARAMETER": "/test/user",
    "DB_SECRET_ARN": "arn:aws:secretsmanager:ap-south-1:000000000000:secret:test",
    "SLOT_STATE_TABLE": "test-slots", "ASSETS_BUCKET": "test-assets"
}.items():
    os.environ.setdefault(key, value)

from src.lambdas.slot_time import put_if_absent


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression):
        key = Item["slot_id"]
        if key in self.items:
            error = RuntimeError("exists")
            error.response = {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}}
            raise error
        self.items[key] = Item


class SlotIdempotencyTests(unittest.TestCase):
    def test_repeated_materialisation_does_not_duplicate_slot(self):
        table = FakeTable()
        item = {"slot_id": "42#3#20261101T053000Z", "doctor_id": "42"}
        self.assertTrue(put_if_absent(table, item))
        self.assertFalse(put_if_absent(table, item))
        self.assertEqual(len(table.items), 1)


if __name__ == "__main__":
    unittest.main()
