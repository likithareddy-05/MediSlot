"""Sequential ids (1, 2, 3, ...) replace UUIDs for DynamoDB-backed records."""
import os
import re
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import aws_stubs  # noqa: F401  (installs boto3/botocore stand-ins when they are not installed)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "lambdas" / "auth"))
import os
for _k, _v in dict(AWS_DEFAULT_REGION="ap-south-1", JWT_SIGNING_SECRET_PARAMETER="j", REFRESH_TOKENS_TABLE="t", ACCESS_TOKEN_TTL_PARAMETER="a").items():
    os.environ.setdefault(_k, _v)
from refresh import ACCESS_TOKEN_SEQUENCE, REFRESH_TOKEN_SEQUENCE, SEQUENCE_PARTITION, next_id  # noqa: E402


class FakeCounterTable:
    """Mimics UpdateItem ... ADD current_value :one with ReturnValues=UPDATED_NEW."""

    def __init__(self):
        self.values = {}
        self.calls = []

    def update_item(self, **kwargs):
        self.calls.append(kwargs)
        key = (kwargs["Key"]["user_id"], kwargs["Key"]["token_id"])
        self.values[key] = self.values.get(key, 0) + kwargs["ExpressionAttributeValues"][":one"]
        return {"Attributes": {"current_value": Decimal(self.values[key])}}


class IdSequenceTest(unittest.TestCase):
    def test_numbers_start_at_one_and_increase_by_one(self):
        table = FakeCounterTable()
        self.assertEqual([next_id(table, REFRESH_TOKEN_SEQUENCE) for _ in range(3)], [1, 2, 3])

    def test_each_sequence_counts_on_its_own(self):
        table = FakeCounterTable()
        next_id(table, REFRESH_TOKEN_SEQUENCE)
        next_id(table, REFRESH_TOKEN_SEQUENCE)
        self.assertEqual(next_id(table, ACCESS_TOKEN_SEQUENCE), 1)

    def test_returns_a_plain_int_and_uses_the_reserved_partition(self):
        table = FakeCounterTable()
        value = next_id(table, REFRESH_TOKEN_SEQUENCE)
        self.assertIs(type(value), int)
        self.assertEqual(table.calls[0]["Key"]["user_id"], SEQUENCE_PARTITION)
        self.assertFalse(SEQUENCE_PARTITION.isdigit(), "must never collide with a numeric user id")

    def test_no_uuid_left_in_the_source(self):
        root = Path(__file__).resolve().parents[1]
        offenders = [str(p.relative_to(root)) for p in list((root / "src").rglob("*.py")) + [root / "dashboard" / "app.py"]
                     if re.search(r"import uuid|uuid\d?\(", p.read_text(encoding="utf-8"))]
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
