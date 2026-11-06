"""Doctor dashboard week view: range schedule shape and limits."""
import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "doctor"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {"AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                   "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
                   "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "SLOT_TABLE_NAME": "slots",
                   "ASSETS_BUCKET": "bucket"}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import doctor_domain as dd  # noqa: E402


def fake_day(cursor, conn, doctor_id, day, patterns=None):
    return {"date": day.isoformat(), "timezone": "Asia/Kolkata", "is_blackout": day.weekday() == 6,
            "slots": [{"slot_id": f"{day}-1"}] if day.weekday() != 6 else [],
            "appointments": []}


class WeekScheduleTests(unittest.TestCase):
    def run_range(self, first, last):
        with mock.patch.object(dd, "_active_patterns", return_value=[]) as patterns, \
             mock.patch.object(dd, "_schedule_for_day", side_effect=fake_day):
            result = dd._schedule_for_range(None, None, 7, first, last)
        return result, patterns

    def test_week_returns_one_entry_per_day_and_summary(self):
        result, patterns = self.run_range(date(2026, 11, 2), date(2026, 11, 8))  # Mon..Sun
        self.assertEqual(len(result["days"]), 7)
        self.assertEqual(result["summary"], {"slots": 6, "appointments": 0, "blackout_days": 1})
        self.assertEqual((result["from"], result["to"]), ("2026-11-02", "2026-11-08"))
        patterns.assert_called_once()  # availability patterns are loaded once, not per day

    def test_reversed_range_rejected(self):
        with self.assertRaises(ValueError):
            self.run_range(date(2026, 11, 8), date(2026, 11, 2))

    def test_range_is_capped(self):
        with self.assertRaises(ValueError):
            self.run_range(date(2026, 11, 1), date(2026, 12, 31))

    def test_deactivation_messages(self):
        source = (ROOT / "src" / "lambdas" / "doctor" / "doctor_domain.py").read_text()
        self.assertIn("You cannot deactivate this consultation type until its slots are deleted.", source)
        self.assertNotIn("In use by", source)


if __name__ == "__main__":
    unittest.main()
