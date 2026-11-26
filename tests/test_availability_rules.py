"""Overlap rule (leftover minutes stay free), delete protection and slot cancellation."""
import json
from decimal import Decimal
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
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments", "ASSETS_BUCKET": "bucket",
}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import doctor_domain as dd  # noqa: E402
from fakes_booking import FakeConn, FakeDynamo, FakeRds, appointments_table, slot_row  # noqa: E402


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


def appointment_item(aid, slot_id="7#1#X", patient=100, hours=24, status="CONFIRMED", ctype=1, fee="500.00"):
    start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=hours)
    return {"appointment_id": str(aid), "patient_id": str(patient), "doctor_id": "7", "slot_id": slot_id,
            "consultation_type_id": str(ctype), "consultation_name": "Quick Consult", "status": status,
            "start_time_utc": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time_utc": (start + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fee_amount": fee, "created_at": "2026-11-01T00:00:00Z", "updated_at": "2026-11-01T00:00:00Z"}


class DoctorConn(FakeConn):
    """FakeRds connection that also answers the doctor-profile lookup every doctor request starts with."""
    def execute(self, sql, args=None):
        if " ".join(sql.split()).startswith("SELECT user_id FROM doctors"):
            self._one = {"user_id": int(args[0])}
            return
        super().execute(sql, args)


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.rds = FakeRds(slots=[slot_row("7#1#X", doctor_id=7, ctype=1, capacity=2, booked=2, hours_ahead=24)])
        self.appts = appointments_table()
        self.events, self.metrics = [], []
        self._saved = {name: getattr(dd, name) for name in
                       ("ddb", "db_connect", "_refresh_slots", "_sleep", "publish_event", "emit_metric")}
        dd.ddb = FakeDynamo(appointments=self.appts)
        dd._sleep = lambda s: None
        dd.publish_event = lambda kind, detail: self.events.append((kind, detail))
        dd.emit_metric = lambda name, value=1, unit="Count", **dims: self.metrics.append((name, value, dims))
        self.addCleanup(self.restore)

    def restore(self):
        for name, value in self._saved.items():
            setattr(dd, name, value)

    def call(self, conn, method, path, body=None, extra=None):
        dd.db_connect = lambda: conn
        dd._refresh_slots = lambda doctor_id: {"refreshed": True}
        event = {"httpMethod": method, "path": path, "resource": path, "body": json.dumps(body or {}),
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}, **(extra or {})}
        result = dd.lambda_handler(event, None)
        return result["statusCode"], json.loads(result["body"])

    def doctor_conn(self):
        conn = DoctorConn(self.rds)
        return conn

    def add_appointment(self, aid, **kwargs):
        item = appointment_item(aid, **kwargs)
        self.appts.items[item["appointment_id"]] = item
        return item

    # ---- consultation type cannot be deleted while it has availability
    def test_deactivate_is_blocked_while_availability_exists(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 2}])
        status, body = self.call(conn, "DELETE", "/doctors/me/consultation-types/1")
        self.assertEqual(status, 409)
        self.assertIn("cannot deactivate this consultation type until its slots are deleted", body["message"])
        self.assertFalse(any(s.startswith("UPDATE doctor_consultation_types") for s, _ in conn.sql))

    def test_deactivate_is_blocked_while_future_bookings_exist(self):
        self.add_appointment(31)  # a CONFIRMED appointment of this doctor's type 1 tomorrow (DynamoDB)
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 0}])
        status, body = self.call(conn, "DELETE", "/doctors/me/consultation-types/1")
        self.assertEqual(status, 409)
        self.assertIn("upcoming booked", body["message"])

    def test_cancelled_or_other_type_bookings_do_not_block_deactivation(self):
        self.add_appointment(31, status="CANCELLED_BY_PATIENT")
        self.add_appointment(32, ctype=2)
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 0}])
        self.assertEqual(self.call(conn, "DELETE", "/doctors/me/consultation-types/1")[0], 200)

    def test_deactivate_works_once_nothing_depends_on_it(self):
        conn = Cursor(ones=[{"user_id": 7}, {"x": 1}, {"n": 0}])
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
    def test_cancel_slot_closes_slot_and_refunds_every_booking_in_full(self):
        self.add_appointment(11, patient=101)
        self.add_appointment(12, patient=102)
        self.appts.items["HOLD#7#1#X#101"] = {"appointment_id": "HOLD#7#1#X#101", "held_appointment_id": "11"}
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"], body["refunded_in_full"]), (200, 2, 2))
        self.assertEqual(body["pending_appointments"], 0)
        slot = self.rds.slots["7#1#X"]
        self.assertEqual(slot["status"], "CANCELLED")
        self.assertEqual(slot["version"], 1)  # version bump fails any in-flight booking write
        for aid in ("11", "12"):
            item = self.appts.items[aid]
            self.assertEqual(item["status"], "CANCELLED_BY_DOCTOR")
            ledger = self.rds.ledger[int(aid)]
            self.assertEqual(ledger["trigger_type"], "DOCTOR_CANCELLATION")
            self.assertEqual(ledger["refund_amount"], Decimal("500.00"))  # full fee
            self.assertEqual(ledger["refund_percent"], Decimal("100"))
        self.assertNotIn("HOLD#7#1#X#101", self.appts.items)
        self.assertEqual(len([e for e in self.events if e[0] == "AppointmentCancelled"]), 2)
        self.assertEqual(len([e for e in self.events if e[0] == "SlotSeatFreed"]), 0)  # closed slot: no seat to free
        self.assertIn(("CancellationsByTier", 1, {"Tier": "DOCTOR_CANCELLED"}), self.metrics)

    def test_cancelling_the_slot_twice_never_refunds_twice(self):
        self.add_appointment(11)
        self.add_appointment(12)
        self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"]), (200, 0))
        self.assertEqual(len(self.rds.ledger), 2)
        self.assertEqual(len([e for e in self.events if e[0] == "AppointmentCancelled"]), 2)

    def test_index_lag_is_covered_by_the_repeat_listing(self):
        # The second booking is not visible in the SlotIndex for the first 2 listings (eventual consistency).
        self.add_appointment(11)
        self.add_appointment(12)
        self.appts.hidden_from_indexes["12"] = 2
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"], body["pending_appointments"]), (200, 2, 0))
        self.assertEqual(self.appts.items["12"]["status"], "CANCELLED_BY_DOCTOR")

    def test_a_booking_the_index_never_shows_is_reported_as_pending_and_finished_on_repeat(self):
        self.add_appointment(11)
        self.add_appointment(12)
        self.appts.hidden_from_indexes["12"] = 50
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"], body["pending_appointments"]), (200, 1, 1))
        self.assertEqual(self.appts.items["12"]["status"], "CONFIRMED")
        self.appts.hidden_from_indexes.clear()
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["affected_appointments"], body["pending_appointments"]), (200, 1, 0))
        self.assertEqual(len(self.rds.ledger), 2)

    def test_a_patient_cancel_that_already_gave_the_seat_back_is_not_waited_for(self):
        self.rds.slots["7#1#X"]["booked_count"] = 1  # the other patient cancelled and released their seat
        self.add_appointment(11)
        self.add_appointment(12, status="CANCELLED_BY_PATIENT")
        self.appts.items["12"]["seat_released_at"] = "2026-11-01T00:00:00Z"
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual((status, body["pending_appointments"]), (200, 0))

    def test_a_ledger_failure_during_the_slot_cancel_is_repaired_by_repeating_it(self):
        self.add_appointment(11)
        self.add_appointment(12)
        self.rds.slots["7#1#X"]["booked_count"] = 2
        real = dd.record_refund
        calls = []

        def flaky(cursor, item, trigger, result, currency="INR"):
            calls.append(item["appointment_id"])
            if item["appointment_id"] == "12" and len(calls) < 3:
                raise RuntimeError("rds hiccup")
            return real(cursor, item, trigger, result, currency)

        dd.record_refund = flaky
        self.addCleanup(setattr, dd, "record_refund", real)
        self.assertEqual(self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")[0], 500)
        self.assertEqual(self.appts.items["12"]["status"], "CANCELLED_BY_DOCTOR")
        self.assertNotIn(12, self.rds.ledger)
        self.assertEqual(self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")[0], 200)
        self.assertEqual(sorted(self.rds.ledger), [11, 12])

    def test_no_show_is_recorded_in_the_ledger_with_no_refund(self):
        self.add_appointment(21, patient=9)
        status, _ = self.call(self.doctor_conn(), "POST", "/doctors/me/appointments/21/outcome", {"outcome": "NO_SHOW"})
        self.assertEqual(status, 200)
        self.assertEqual(self.appts.items["21"]["status"], "NO_SHOW")
        ledger = self.rds.ledger[21]
        self.assertEqual((ledger["trigger_type"], ledger["refund_amount"], ledger["tier"]),
                         ("NO_SHOW", Decimal("0.00"), "NO_SHOW"))

    def test_repeating_a_no_show_does_not_duplicate_the_ledger(self):
        self.add_appointment(21)
        for _ in range(2):
            self.assertEqual(self.call(self.doctor_conn(), "POST", "/doctors/me/appointments/21/outcome",
                                       {"outcome": "NO_SHOW"})[0], 200)
        self.assertEqual(len(self.rds.ledger), 1)

    def test_completed_appointment_writes_no_ledger_row(self):
        self.add_appointment(21)
        self.assertEqual(self.call(self.doctor_conn(), "POST", "/doctors/me/appointments/21/outcome",
                                   {"outcome": "COMPLETED"})[0], 200)
        self.assertEqual(self.appts.items["21"]["status"], "COMPLETED")
        self.assertEqual(self.rds.ledger, {})

    def test_outcome_only_applies_to_the_doctors_own_confirmed_appointments(self):
        self.add_appointment(21)
        self.appts.items["21"]["doctor_id"] = "99"
        self.assertEqual(self.call(self.doctor_conn(), "POST", "/doctors/me/appointments/21/outcome",
                                   {"outcome": "COMPLETED"})[0], 404)
        self.add_appointment(22, status="CANCELLED_BY_PATIENT")      # cancelled rows are final
        self.assertEqual(self.call(self.doctor_conn(), "POST", "/doctors/me/appointments/22/outcome",
                                   {"outcome": "COMPLETED"})[0], 400)
        self.assertEqual(self.appts.items["22"]["status"], "CANCELLED_BY_PATIENT")

    def test_cancel_slot_of_another_doctor_is_404(self):
        self.rds.slots["7#1#X"]["doctor_id"] = 99
        status, _ = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual(status, 404)
        self.assertEqual(self.rds.slots["7#1#X"]["status"], "AVAILABLE")

    def test_cancel_slot_in_the_past_is_rejected(self):
        self.rds.slots["7#1#X"] = slot_row("7#1#X", doctor_id=7, ctype=1, hours_ahead=-5)
        status, body = self.call(self.doctor_conn(), "DELETE", "/doctors/me/slots/7%231%23X")
        self.assertEqual(status, 400)
        self.assertIn("already ended", body["message"])
        self.assertEqual(self.rds.slots["7#1#X"]["status"], "AVAILABLE")

    def test_cancel_slot_uses_the_api_gateway_path_parameter(self):
        dd.db_connect = lambda: self.doctor_conn()
        event = {"httpMethod": "DELETE", "path": "/doctors/me/slots/ignored", "pathParameters": {"slotId": "7%231%23X"},
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}}
        self.assertEqual(dd.lambda_handler(event, None)["statusCode"], 200)


if __name__ == "__main__":
    unittest.main()
