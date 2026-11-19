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
                   "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments",
                   "ASSETS_BUCKET": "bucket"}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import doctor_domain as dd  # noqa: E402


from datetime import time  # noqa: E402
from fakes_booking import FakeConn, FakeDynamo, FakeRds, appointments_table, slot_row  # noqa: E402
import slot_store  # noqa: E402

TZ = ZoneInfo("Asia/Kolkata")
TODAY = datetime.now(TZ).date()


class DoctorConn(FakeConn):
    """FakeRds connection that also answers the patterns and blackout lookups of the schedule view."""
    def __init__(self, state, blackouts=()):
        super().__init__(state)
        self.blackouts = list(blackouts)

    def execute(self, sql, args=None):
        n = " ".join(sql.split())
        if "FROM availability_patterns" in n:
            self._one, self._all = None, []
            return
        if n.startswith("SELECT slot_id, slot_date, status, booked_count"):
            self._one, self._all = None, []
            return
        if "FROM doctor_blackouts" in n:
            self._one, self._all = None, [{"blackout_date": d, "reason": "Leave"} for d in self.blackouts]
            return
        super().execute(sql, args)


def slot_on(day, hour=10, **kw):
    start = datetime.combine(day, time(hour, 0))
    return slot_row(f"7#4#{day:%Y%m%d}T{hour:02d}", doctor_id=7, start=start, **kw)


class WeekScheduleTests(unittest.TestCase):
    def run_range(self, first, last, slots=(), blackouts=()):
        rds = FakeRds(slots=slots)
        conn = DoctorConn(rds, blackouts)
        with mock.patch.object(dd, "ddb", FakeDynamo(appointments=appointments_table())):
            return dd._schedule_for_range(conn, conn, 7, first, last)

    def test_week_returns_one_entry_per_day_and_summary(self):
        days = [TODAY + timedelta(days=i) for i in range(7)]
        slots = [slot_on(d) for d in days if d != days[3]]            # six days have a slot
        result = self.run_range(days[0], days[-1], slots, blackouts=[days[3]])
        self.assertEqual(len(result["days"]), 7)   # all seven days arrive in one response
        self.assertEqual(result["summary"], {"slots": 6, "appointments": 0, "blackout_days": 1})
        self.assertEqual((result["from"], result["to"]), (days[0].isoformat(), days[-1].isoformat()))

    def test_range_view_uses_the_shared_slot_function(self):
        with mock.patch.object(slot_store, "get_slots", return_value=[]) as get_slots:
            self.run_range(TODAY, TODAY + timedelta(days=6))
        get_slots.assert_called_once()
        self.assertEqual(get_slots.call_args.args[-1], slot_store.VIEWER_DOCTOR)

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


class PastDatesTests(unittest.TestCase):
    def test_past_day_is_hidden_without_touching_storage(self):
        rds = FakeRds(slots=[slot_on(TODAY - timedelta(days=1))])
        conn = DoctorConn(rds)
        with mock.patch.object(slot_store, "get_slots") as get_slots:
            day = dd._schedule_for_day(conn, conn, 7, TODAY - timedelta(days=1))
        self.assertEqual((day["status"], day["slots"], day["appointments"]), ("PAST", [], []))
        get_slots.assert_not_called()

    def test_a_range_that_starts_in_the_past_is_only_looked_up_from_today(self):
        conn = DoctorConn(FakeRds())
        with mock.patch.object(slot_store, "get_slots", return_value=[]) as get_slots:
            dd._schedule_for_range(conn, conn, 7, TODAY - timedelta(days=3), TODAY + timedelta(days=3))
        self.assertEqual(get_slots.call_args.args[2], TODAY)

    def test_today_is_still_shown(self):
        rds = FakeRds(slots=[slot_on(TODAY, 20)])
        conn = DoctorConn(rds)
        with mock.patch.object(dd, "ddb", FakeDynamo(appointments=appointments_table())):
            day = dd._schedule_for_day(conn, conn, 7, TODAY)
        self.assertNotEqual(day["status"], "PAST")

    def test_materialize_never_starts_before_today(self):
        import slot_generation
        conn = DoctorConn(FakeRds())
        conn.close = lambda: None
        result = slot_generation.materialize_doctor_slots(conn, 7, TODAY - timedelta(days=5), 3)
        self.assertGreaterEqual(date.fromisoformat(str(result["start_date"])), date.today() - timedelta(days=1))


if __name__ == "__main__":
    unittest.main()
