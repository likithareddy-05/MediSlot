"""Overlap rule (leftover minutes stay free), delete protection and slot cancellation."""
import json
import os
import sys
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "auth", "doctor", "patient"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {
    "AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
    "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "SLOT_TABLE_NAME": "slots", "ASSETS_BUCKET": "bucket",
}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import doctor_domain as dd  # noqa: E402


class Cursor:
    """Scripted cursor: fetchone/fetchall pop queued results and every statement is recorded."""
    def __init__(self, ones=(), alls=()):
        self.ones, self.alls, self.sql = list(ones), list(alls), []
        self.rowcount, self.lastrowid, self.committed = 1, 0, False
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, args=None): self.sql.append((" ".join(sql.split()), args))
    def fetchone(self): return self.ones.pop(0) if self.ones else None
    def fetchall(self): return self.alls.pop(0) if self.alls else []
    def commit(self): self.committed = True
    def rollback(self): pass
    def close(self): pass


def t(text):
    return time.fromisoformat(text)


def existing_row(start, end, name="Quick Consult", minutes=15, pid=1):
    return {"availability_pattern_id": pid, "start_time": t(start), "end_time": t(end),
            "effective_from": datetime(2026, 1, 1).date(), "effective_to": None,
            "consultation_name": name, "duration_minutes": minutes}


def check(existing, start, end, duration, exclude_id=None):
    cur = Cursor(alls=[existing])
    dd._check_overlap(cur, 7, 0, t(start), t(end), duration, datetime(2026, 11, 5).date(), None, exclude_id)


class EffectiveEndTests(unittest.TestCase):
    def test_leftover_minutes_are_not_counted(self):
        self.assertEqual(dd._effective_end(t("09:00"), t("09:34"), 15), 9 * 60 + 30)

    def test_exact_fit_is_unchanged(self):
        self.assertEqual(dd._effective_end(t("12:00"), t("14:00"), 30), 14 * 60)

    def test_window_with_one_slot(self):
        self.assertEqual(dd._effective_end(t("09:00"), t("09:50"), 30), 9 * 60 + 30)


class OverlapTests(unittest.TestCase):
    # Existing: Quick Consult 09:00-09:34 -> slots 09:00 and 09:15, occupying 09:00-09:30.
    existing = [existing_row("09:00", "09:34")]

    def test_window_starting_where_the_last_slot_ends_is_allowed(self):
        check(self.existing, "09:30", "10:00", 15)            # the screenshot scenario

    def test_other_consultation_type_can_use_the_freed_minutes(self):
        check(self.existing, "09:30", "10:30", 30)

    def test_window_starting_inside_the_occupied_time_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "overlaps your existing Monday availability 09:00–09:30"):
            check(self.existing, "09:29", "10:00", 15)

    def test_window_covering_an_existing_one_is_rejected(self):
        with self.assertRaises(ValueError):
            check(self.existing, "08:00", "10:00", 60)

    def test_new_windows_own_leftover_minutes_are_also_free(self):
        # New 08:00-09:10 with 30-min slots only occupies 08:00-09:00, so it may sit before 09:00.
        check(self.existing, "08:00", "09:10", 30)

    def test_editing_a_window_ignores_itself(self):
        check(self.existing, "09:00", "09:34", 15, exclude_id=1)


class FakeTable:
    def __init__(self, item):
        self.item, self.updates = item, []
    def get_item(self, Key): return {"Item": self.item} if self.item else {}
    def update_item(self, **kw): self.updates.append(kw)


class FakeDdb:
    def __init__(self, item): self.table = FakeTable(item)
    def Table(self, name): return self.table


def slot_item(hours_from_now=24, doctor="7"):
    start = datetime.now(timezone.utc) + timedelta(hours=hours_from_now)
    end = start + timedelta(minutes=15)
    return {"slot_id": "7#1#X", "doctor_id": doctor, "status": "AVAILABLE", "booked_count": 1,
            "start_time_utc": start.isoformat().replace("+00:00", "Z"),
            "end_time_utc": end.isoformat().replace("+00:00", "Z")}


class HandlerTests(unittest.TestCase):
    def call(self, conn, method, path, body=None):
        dd.db_connect = lambda: conn
        dd._refresh_slots = lambda doctor_id: {"refreshed": True}
        event = {"httpMethod": method, "path": path, "resource": path, "body": json.dumps(body or {}),
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}}
        result = dd.lambda_handler(event, None)
        return result["statusCode"], json.loads(result["body"])

    # ---- consultation type cannot be deleted while it has availability
    def test_deactivate_is_blocked_while_availability_exists(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 2}])
        status, body = self.call(conn, "DELETE", "/doctors/me/consultation-types/1")
        self.assertEqual(status, 409)
        self.assertIn("cannot deactivate this consultation type until its slots are deleted", body["message"])
        self.assertFalse(any(s.startswith("UPDATE doctor_consultation_types") for s, _ in conn.sql))

    def test_deactivate_is_blocked_while_future_bookings_exist(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 0}, {"n": 3}])
        status, body = self.call(conn, "DELETE", "/doctors/me/consultation-types/1")
        self.assertEqual(status, 409)
        self.assertIn("upcoming booked", body["message"])

    def test_deactivate_works_once_nothing_depends_on_it(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 0}, {"n": 0}])
        status, _ = self.call(conn, "DELETE", "/doctors/me/consultation-types/1")
        self.assertEqual(status, 200)
        self.assertTrue(conn.sql[-1][0].startswith("UPDATE doctor_consultation_types SET is_active=0"))

    def test_deactivate_unknown_type_is_404(self):
        conn = Cursor(ones=[{"user_id": 7}, None])
        self.assertEqual(self.call(conn, "DELETE", "/doctors/me/consultation-types/9")[0], 404)

    def test_put_is_active_false_is_blocked_too(self):
        current = {"consultation_name": "Quick Consult", "duration_minutes": 15, "min_capacity": 1,
                   "max_capacity": 1, "slot_capacity": 1}
        conn = Cursor(ones=[{"user_id": 7}, current, {"n": 1}])
        status, _ = self.call(conn, "PUT", "/doctors/me/consultation-types/1", {"is_active": 0})
        self.assertEqual(status, 409)

    # ---- the screenshot scenario through the API
    def test_post_availability_0930_after_quick_consult_0900_0934(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"duration_minutes": 15}],
                      alls=[[{"timezone_name": "Asia/Kolkata"}], [existing_row("09:00", "09:34")]])
        status, body = self.call(conn, "POST", "/doctors/me/availability",
            {"weekday": 0, "start_time": "09:30", "end_time": "10:00", "timezone_name": "Asia/Kolkata",
             "consultation_type_id": 1})
        self.assertEqual(status, 201, body)
        self.assertTrue(any(sql.startswith("INSERT INTO availability_patterns") for sql, _ in conn.sql))

    def test_post_availability_inside_occupied_time_is_still_rejected(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"duration_minutes": 15}],
                      alls=[[{"timezone_name": "Asia/Kolkata"}], [existing_row("09:00", "09:34")]])
        status, body = self.call(conn, "POST", "/doctors/me/availability",
            {"weekday": 0, "start_time": "09:15", "end_time": "10:00", "timezone_name": "Asia/Kolkata",
             "consultation_type_id": 1})
        self.assertEqual(status, 400)
        self.assertIn("overlaps", body["message"])

    # ---- doctor cancels one slot
    def test_cancel_slot_closes_slot_and_cancels_bookings(self):
        dd.ddb = FakeDdb(slot_item())
        conn = Cursor(ones=[{"user_id": 7}], alls=[[{"appointment_id": 11}, {"appointment_id": 12}]])
        status, body = self.call(conn, "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"]), (200, 2))
        update = dd.ddb.table.updates[0]
        self.assertEqual(update["ExpressionAttributeValues"][":cancelled"], "CANCELLED")
        cancelled = [a for s, a in conn.sql if s.startswith("UPDATE appointments SET status='CANCELLED'")]
        self.assertEqual(cancelled, [(11,), (12,)])
        self.assertTrue(conn.committed)

    def test_cancel_slot_of_another_doctor_is_404(self):
        dd.ddb = FakeDdb(slot_item(doctor="99"))
        status, _ = self.call(Cursor(ones=[{"user_id": 7}]), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual(status, 404)
        self.assertEqual(dd.ddb.table.updates, [])

    def test_cancel_slot_in_the_past_is_rejected(self):
        dd.ddb = FakeDdb(slot_item(hours_from_now=-5))
        conn = Cursor(ones=[{"user_id": 7}])
        status, body = self.call(conn, "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual(status, 400)
        self.assertIn("already ended", body["message"])
        self.assertEqual(dd.ddb.table.updates, [])

    def test_cancel_slot_uses_the_api_gateway_path_parameter(self):
        dd.ddb = FakeDdb(slot_item())
        dd.db_connect = lambda: Cursor(ones=[{"user_id": 7}])
        event = {"httpMethod": "DELETE", "path": "/doctors/me/slots/ignored", "pathParameters": {"slotId": "7%231%23X"},
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}}
        self.assertEqual(dd.lambda_handler(event, None)["statusCode"], 200)


if __name__ == "__main__":
    unittest.main()
