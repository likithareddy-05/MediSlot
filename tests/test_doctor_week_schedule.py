"""Doctor dashboard week view: range schedule shape and limits."""
import os
import sys
import unittest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
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


class FakeCursor:
    def __init__(self): self.sql = []
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, args=None): self.sql.append(sql)
    def fetchall(self): return []
    def fetchone(self): return None
    def commit(self): pass
    def close(self): pass


class PastDatesTests(unittest.TestCase):
    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()

    def test_past_day_is_hidden_without_touching_storage(self):
        cur = FakeCursor()
        with mock.patch.object(dd, "_query_day") as query:
            day = dd._schedule_for_day(cur, cur, 7, self.today - timedelta(days=1), patterns=[])
        self.assertEqual((day["status"], day["slots"], day["appointments"]), ("PAST", [], []))
        query.assert_not_called()
        self.assertEqual(cur.sql, [])

    def test_today_is_still_shown(self):
        cur = FakeCursor()
        with mock.patch.object(dd, "_query_day", return_value=[]), \
             mock.patch.object(dd, "_appointments_for_slots", return_value=[]):
            day = dd._schedule_for_day(cur, cur, 7, self.today, patterns=[])
        self.assertNotEqual(day["status"], "PAST")

    def test_materialize_never_starts_before_today(self):
        cur = FakeCursor()
        with mock.patch.object(dd, "db_connect", return_value=cur), mock.patch.object(dd, "ddb"), \
             mock.patch.object(dd, "_query_day", return_value=[]):
            result = dd.materialize_doctor(7, self.today - timedelta(days=5), 3)
        self.assertEqual(result["start_date"], self.today.isoformat())


if __name__ == "__main__":
    unittest.main()
