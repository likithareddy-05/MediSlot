"""Idempotent DynamoDB put behavior for deterministic slot IDs."""
import unittest

from src.lambdas.shared.slot_time import put_if_absent


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression):
        key = Item["slot_id"]
        if key in self.items:
            error = RuntimeError("item already exists")
            error.response = {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "item already exists",
                }
            }
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
