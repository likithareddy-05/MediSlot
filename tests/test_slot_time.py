
import sys
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import Mock

sys.path.insert(0, "src/lambdas/shared")

from slot_time import valid_local_time, put_if_absent


class SlotTimezoneTests(unittest.TestCase):

    def test_valid_local_time(self):
        zone = ZoneInfo("Asia/Kolkata")
        local_time = datetime(2026, 11, 2, 9, 0)

        result = valid_local_time(local_time, zone)

        self.assertIsNotNone(result)
        self.assertEqual(result.strftime("%H:%M"), "09:00")

    def test_dst_nonexistent_time_returns_none(self):
        zone = ZoneInfo("America/New_York")

        # On March 8, 2026, clocks jump from 01:59 to 03:00.
        nonexistent_time = datetime(2026, 3, 8, 2, 30)

        result = valid_local_time(nonexistent_time, zone)

        self.assertIsNone(result)

    def test_dst_overlap_chooses_fold_zero(self):
        zone = ZoneInfo("America/New_York")

        # 01:30 occurs twice when daylight saving time ends.
        overlapping_time = datetime(2026, 11, 1, 1, 30)

        result = valid_local_time(overlapping_time, zone)

        self.assertIsNotNone(result)
        self.assertEqual(result.fold, 0)

    def test_put_if_absent_success(self):
        table = Mock()

        result = put_if_absent(table, {"slot_id": "slot-001"})

        self.assertTrue(result)
        table.put_item.assert_called_once()

    def test_put_if_absent_duplicate(self):
        table = Mock()

        error = Exception("Duplicate item")
        error.response = {
            "Error": {
                "Code": "ConditionalCheckFailedException"
            }
        }

        table.put_item.side_effect = error

        result = put_if_absent(table, {"slot_id": "slot-001"})

        self.assertFalse(result)

    def test_put_if_absent_unexpected_error(self):
        table = Mock()
        table.put_item.side_effect = RuntimeError("DynamoDB unavailable")

        with self.assertRaises(RuntimeError):
            put_if_absent(table, {"slot_id": "slot-001"})


if __name__ == "__main__":
    unittest.main()
