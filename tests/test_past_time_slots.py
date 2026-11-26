"""Slots are never created for a start time that has already passed today."""
import sys
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "lambdas" / "shared"))

import slot_generation  # noqa: E402

TZ = "Asia/Kolkata"
# Fixed "now": Thu 26 Nov 2026, 13:30 IST (08:00 UTC).
NOW_UTC = datetime(2026, 11, 26, 8, 0, tzinfo=timezone.utc)
TODAY = NOW_UTC.astimezone(ZoneInfo(TZ)).date()


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW_UTC if tz is None else NOW_UTC.astimezone(tz)


def pattern(start, end, duration=15):
    return {"availability_pattern_id": 1, "consultation_type_id": 4, "weekday": TODAY.weekday(),
            "start_time": time.fromisoformat(start), "end_time": time.fromisoformat(end),
            "timezone_name": TZ, "effective_from": TODAY, "effective_to": None,
            "consultation_name": "Quick Consult", "duration_minutes": duration, "fee": 100, "slot_capacity": 1}


class Conn:
    """Minimal RDS fake for materialize_doctor_slots."""
    def __init__(self, patterns, stored=None):
        self.patterns, self.stored = patterns, dict(stored or {})
        self.inserted, self.deleted = [], []
        self._all, self.rowcount = [], 0

    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def commit(self): pass
    def fetchall(self): return self._all

    def execute(self, sql, args=None):
        n = " ".join(sql.split())
        self._all, self.rowcount = [], 0
        if "FROM availability_patterns" in n:
            self._all = self.patterns
        elif n.startswith("SELECT slot_id, slot_date"):
            self._all = list(self.stored.values())
        elif n.startswith("INSERT INTO slots"):
            self.inserted.append(args)
            self.rowcount = 1
        elif n.startswith("DELETE FROM slots"):
            if args[0] in self.stored and self.stored[args[0]]["booked_count"] == 0:
                self.deleted.append(args[0])
                del self.stored[args[0]]
                self.rowcount = 1


def run(conn):
    with mock.patch.object(slot_generation, "datetime", FrozenDatetime):
        return slot_generation.materialize_doctor_slots(conn, 7, TODAY, 1)


def stored_slot(local_hhmm, created_at, booked=0, status="AVAILABLE"):
    local = datetime.combine(TODAY, time.fromisoformat(local_hhmm), ZoneInfo(TZ))
    start = local.astimezone(timezone.utc).replace(tzinfo=None)
    sid = f"7#4#{start:%Y%m%dT%H%M%S}Z"
    return sid, {"slot_id": sid, "slot_date": TODAY, "status": status, "booked_count": booked,
                 "start_time_utc": start, "availability_pattern_id": 1, "created_at": created_at}


class PastTimeSlotTests(unittest.TestCase):
    def test_morning_window_created_in_the_afternoon_makes_no_slots(self):
        conn = Conn([pattern("02:00", "02:30")])
        result = run(conn)
        self.assertEqual((result["created"], result["past_times_skipped"]), (0, 2))
        self.assertEqual(conn.inserted, [])

    def test_window_spanning_now_only_creates_the_remaining_slots(self):
        conn = Conn([pattern("13:00", "14:30", 30)])  # 13:00 started before 13:30, 13:30 starts now
        result = run(conn)
        self.assertEqual(result["created"], 1)  # only 14:00
        self.assertEqual(conn.inserted[0][8], time(14, 0))

    def test_later_today_and_future_days_still_created(self):
        conn = Conn([pattern("18:00", "18:30")])
        self.assertEqual(run(conn)["created"], 2)

    def test_slot_created_after_its_start_is_cleaned_up(self):
        sid, row = stored_slot("02:00", created_at=NOW_UTC.replace(tzinfo=None) - timedelta(minutes=5))
        conn = Conn([pattern("02:00", "02:15")], {sid: row})
        result = run(conn)
        self.assertEqual(conn.deleted, [sid])
        self.assertEqual(result["removed"], 1)

    def test_genuine_past_slot_is_kept_for_history(self):
        # Created yesterday, so it was bookable before it started: leave it alone.
        sid, row = stored_slot("02:00", created_at=NOW_UTC.replace(tzinfo=None) - timedelta(days=1))
        conn = Conn([pattern("02:00", "02:15")], {sid: row})
        run(conn)
        self.assertEqual(conn.deleted, [])

    def test_edited_pattern_removes_old_past_slots_of_today(self):
        # Thursday pattern was 02:00-02:30 AM (slots made yesterday), now edited to 2:00-2:30 PM.
        yesterday = NOW_UTC.replace(tzinfo=None) - timedelta(days=1)
        old = dict([stored_slot("02:00", yesterday), stored_slot("02:15", yesterday)])
        conn = Conn([pattern("14:00", "14:30")], old)
        result = run(conn)
        self.assertEqual(sorted(conn.deleted), sorted(old))
        self.assertEqual((result["removed"], result["created"]), (2, 2))

    def test_edited_pattern_keeps_booked_past_slot(self):
        sid, row = stored_slot("02:00", NOW_UTC.replace(tzinfo=None) - timedelta(days=1), booked=1)
        conn = Conn([pattern("14:00", "14:30")], {sid: row})
        run(conn)
        self.assertEqual(conn.deleted, [])

    def test_booked_past_slot_is_never_deleted(self):
        sid, row = stored_slot("02:00", created_at=NOW_UTC.replace(tzinfo=None), booked=1)
        conn = Conn([pattern("02:00", "02:15")], {sid: row})
        run(conn)
        self.assertEqual(conn.deleted, [])


if __name__ == "__main__":
    unittest.main()
