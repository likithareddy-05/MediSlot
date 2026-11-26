"""Blackout dates, doctor status changes (cancel / complete / no-show) and the Cancelled list filter.

All tests run on in-memory fakes (tests/fakes_booking.py): nothing touches a real database or AWS.
"""
import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "auth", "doctor", "patient"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {
    "AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
    "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments",
    "ASSETS_BUCKET": "bucket",
}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import appointment_store as store  # noqa: E402
import booking_domain as bd  # noqa: E402
import doctor_domain as dd  # noqa: E402
from fakes_booking import FakeConn, FakeDynamo, FakeRds, appointments_table, slot_row  # noqa: E402


def appointment_item(aid, slot_id="7#1#X", patient=100, hours=24, status="CONFIRMED", fee="500.00"):
    start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=hours)
    return {"appointment_id": str(aid), "patient_id": str(patient), "doctor_id": "7", "slot_id": slot_id,
            "consultation_type_id": "1", "consultation_name": "Quick Consult", "status": status,
            "start_time_utc": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time_utc": (start + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fee_amount": fee, "doctor_name": "Asha Rao", "created_at": "2026-11-01T00:00:00Z",
            "updated_at": "2026-11-01T00:00:00Z"}


class DoctorConn(FakeConn):
    """Fake connection that also answers the lookups every doctor request makes."""
    blackout_rows = 0

    def execute(self, sql, args=None):
        n = " ".join(sql.split())
        if n.startswith("SELECT user_id FROM doctors"):
            self._one = {"user_id": int(args[0])}
            return
        if n.startswith("SELECT 1 FROM doctor_blackouts"):
            self._one = {"1": 1} if DoctorConn.blackout_rows else None
            return
        if n.startswith("INSERT INTO doctor_blackouts"):
            self.lastrowid = 55
            return
        super().execute(sql, args)


class Base(unittest.TestCase):
    def setUp(self):
        DoctorConn.blackout_rows = 0
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

    def call(self, rds, method, path, body=None, query=None):
        conn = DoctorConn(rds)
        dd.db_connect = lambda: conn
        dd._refresh_slots = lambda doctor_id: {"refreshed": True}
        event = {"httpMethod": method, "path": path, "resource": path, "body": json.dumps(body or {}),
                 "queryStringParameters": query,
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}}
        result = dd.lambda_handler(event, None)
        return result["statusCode"], json.loads(result["body"])

    def add(self, aid, **kw):
        item = appointment_item(aid, **kw)
        self.appts.items[item["appointment_id"]] = item
        return item


class BlackoutTests(Base):
    """Adding a blackout date deletes that day's slots; booked ones are cancelled and refunded in full."""

    def day_of(self, slot_id, hours=48, **kw):
        return slot_row(slot_id, doctor_id=7, ctype=1, hours_ahead=hours, **kw)

    def test_unbooked_slots_of_that_day_are_deleted_and_other_days_are_kept(self):
        target = self.day_of("7#1#A", 48)
        same_day = self.day_of("7#1#B", 48, start=target["start_time_utc"] + timedelta(hours=2))
        other_day = self.day_of("7#1#C", 96)
        rds = FakeRds(slots=[target, same_day, other_day])
        status, body = self.call(rds, "POST", "/doctors/me/blackouts",
                                 {"blackout_date": target["slot_date"].isoformat(), "reason": "Leave"})
        self.assertEqual(status, 201)
        self.assertEqual((body["slots_deleted"], body["slots_cancelled"], body["appointments_cancelled"]), (2, 0, 0))
        self.assertEqual(sorted(rds.slots), ["7#1#C"])

    def test_a_booked_slot_is_cancelled_and_every_patient_is_refunded_in_full(self):
        booked = self.day_of("7#1#A", 48, capacity=2, booked=2)
        empty = self.day_of("7#1#B", 48, start=booked["start_time_utc"] + timedelta(hours=3))
        rds = FakeRds(slots=[booked, empty])
        self.add(11, slot_id="7#1#A", patient=101)
        self.add(12, slot_id="7#1#A", patient=102)
        self.appts.items["HOLD#7#1#A#101"] = {"appointment_id": "HOLD#7#1#A#101", "held_appointment_id": "11"}
        status, body = self.call(rds, "POST", "/doctors/me/blackouts", {"blackout_date": booked["slot_date"].isoformat()})
        self.assertEqual(status, 201)
        self.assertEqual((body["slots_deleted"], body["slots_cancelled"], body["appointments_cancelled"],
                          body["pending_appointments"]), (1, 1, 2, 0))
        self.assertNotIn("7#1#B", rds.slots)                       # the empty slot is gone
        self.assertEqual(rds.slots["7#1#A"]["status"], "CANCELLED")  # kept as history for the refunds
        for aid in ("11", "12"):
            self.assertEqual(self.appts.items[aid]["status"], "CANCELLED_BY_DOCTOR")
            ledger = rds.ledger[int(aid)]
            self.assertEqual((ledger["trigger_type"], ledger["refund_amount"], ledger["refund_percent"]),
                             ("DOCTOR_CANCELLATION", Decimal("500.00"), Decimal("100")))
        self.assertNotIn("HOLD#7#1#A#101", self.appts.items)
        self.assertEqual(len([e for e in self.events if e[0] == "AppointmentCancelled"]), 2)

    def test_a_slot_that_is_already_over_stays_as_history(self):
        start = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None) - timedelta(hours=3)
        over = slot_row("7#1#OLD", doctor_id=7, ctype=1, capacity=1, booked=1, start=start)
        rds = FakeRds(slots=[over])
        self.add(31, slot_id="7#1#OLD", hours=-3, status="COMPLETED")
        status, body = self.call(rds, "POST", "/doctors/me/blackouts", {"blackout_date": over["slot_date"].isoformat()})
        self.assertEqual(status, 201)
        self.assertEqual((body["slots_cancelled"], body["appointments_cancelled"]), (0, 0))
        self.assertEqual(rds.slots["7#1#OLD"]["status"], "AVAILABLE")
        self.assertEqual(self.appts.items["31"]["status"], "COMPLETED")
        self.assertEqual(rds.ledger, {})

    def test_a_duplicate_blackout_changes_nothing(self):
        slot = self.day_of("7#1#A", 48)
        rds = FakeRds(slots=[slot])
        DoctorConn.blackout_rows = 1
        status, _ = self.call(rds, "POST", "/doctors/me/blackouts", {"blackout_date": slot["slot_date"].isoformat()})
        self.assertEqual(status, 409)
        self.assertIn("7#1#A", rds.slots)


class DoctorStatusChangeTests(Base):
    """The status dropdown: Completed, No-show and Cancelled (Confirmed is the starting point)."""

    def setUp(self):
        super().setUp()
        self.rds = FakeRds(slots=[slot_row("7#1#X", doctor_id=7, ctype=1, capacity=2, booked=2, hours_ahead=24)])

    def outcome(self, aid, value):
        return self.call(self.rds, "POST", f"/doctors/me/appointments/{aid}/outcome", {"outcome": value})

    def test_cancelling_one_appointment_refunds_in_full_and_frees_the_seat(self):
        self.add(11, patient=101)
        self.add(12, patient=102)
        self.appts.items["HOLD#7#1#X#101"] = {"appointment_id": "HOLD#7#1#X#101", "held_appointment_id": "11"}
        status, body = self.outcome(11, "CANCELLED")
        self.assertEqual((status, body["status"]), (200, "CANCELLED_BY_DOCTOR"))
        self.assertEqual(self.appts.items["11"]["status"], "CANCELLED_BY_DOCTOR")
        self.assertEqual(self.appts.items["12"]["status"], "CONFIRMED")      # the other patient is untouched
        ledger = self.rds.ledger[11]
        self.assertEqual((ledger["trigger_type"], ledger["refund_amount"]), ("DOCTOR_CANCELLATION", Decimal("500.00")))
        slot = self.rds.slots["7#1#X"]
        self.assertEqual((slot["booked_count"], slot["status"]), (1, "AVAILABLE"))  # the slot stays open
        self.assertNotIn("HOLD#7#1#X#101", self.appts.items)
        kinds = [e[0] for e in self.events]
        self.assertEqual((kinds.count("AppointmentCancelled"), kinds.count("SlotSeatFreed")), (1, 1))
        self.assertIn(("CancellationsByTier", 1, {"Tier": "DOCTOR_CANCELLED"}), self.metrics)

    def test_the_word_cancelled_by_doctor_is_accepted_too(self):
        self.add(11)
        self.assertEqual(self.outcome(11, "CANCELLED_BY_DOCTOR")[0], 200)

    def test_pressing_cancel_twice_never_refunds_or_frees_a_seat_twice(self):
        self.add(11)
        self.assertEqual(self.outcome(11, "CANCELLED")[0], 200)
        self.assertEqual(self.outcome(11, "CANCELLED")[0], 200)
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(self.rds.slots["7#1#X"]["booked_count"], 1)
        self.assertEqual([e[0] for e in self.events].count("AppointmentCancelled"), 1)

    def test_a_patient_cancelled_appointment_cannot_be_cancelled_by_the_doctor(self):
        self.add(11, status="CANCELLED_BY_PATIENT")
        status, body = self.outcome(11, "CANCELLED")
        self.assertEqual(status, 400)
        self.assertIn("cannot be cancelled", body["message"])

    def test_an_appointment_that_already_ended_cannot_be_cancelled(self):
        self.add(11, hours=-5)
        self.assertEqual(self.outcome(11, "CANCELLED")[0], 400)
        self.assertEqual(self.appts.items["11"]["status"], "CONFIRMED")

    def test_unknown_status_is_refused(self):
        self.add(11)
        status, body = self.outcome(11, "PENDING")
        self.assertEqual(status, 400)
        self.assertIn("CONFIRMED", body["message"])

    def test_every_status_can_be_corrected_among_confirmed_completed_no_show(self):
        self.add(11)
        self.assertEqual(self.outcome(11, "NO_SHOW")[0], 200)
        self.assertEqual(self.rds.ledger[11]["trigger_type"], "NO_SHOW")
        self.assertEqual(self.outcome(11, "COMPLETED")[0], 200)          # wrong click, corrected
        item = self.appts.items["11"]
        self.assertEqual(item["status"], "COMPLETED")
        self.assertNotIn("refund_tier", item)
        self.assertEqual(self.rds.ledger, {})                            # the 0% no-show row is gone
        self.assertEqual(self.outcome(11, "CONFIRMED")[0], 200)
        self.assertEqual(self.appts.items["11"]["status"], "CONFIRMED")
        self.assertNotIn("outcome", self.appts.items["11"])

    def test_a_no_show_can_still_be_cancelled_with_a_full_refund(self):
        self.add(11)
        self.outcome(11, "NO_SHOW")
        self.assertEqual(self.outcome(11, "CANCELLED")[0], 200)
        self.assertEqual(self.rds.ledger[11]["trigger_type"], "DOCTOR_CANCELLATION")
        self.assertEqual(self.rds.ledger[11]["refund_amount"], Decimal("500.00"))

    def test_a_cancelled_appointment_cannot_be_reopened(self):
        self.add(11, status="CANCELLED_BY_DOCTOR")
        status, body = self.outcome(11, "COMPLETED")
        self.assertEqual(status, 400)
        self.assertIn("final", body["message"])

    def test_completed_and_no_show_still_work(self):
        self.add(11)
        self.add(12)
        self.assertEqual(self.outcome(11, "COMPLETED")[0], 200)
        self.assertEqual(self.outcome(12, "NO_SHOW")[0], 200)
        self.assertEqual((self.appts.items["11"]["status"], self.appts.items["12"]["status"]), ("COMPLETED", "NO_SHOW"))

    def test_a_completed_appointment_can_be_changed_to_no_show(self):
        self.add(11, status="COMPLETED")
        self.assertEqual(self.outcome(11, "NO_SHOW")[0], 200)
        self.assertEqual(self.appts.items["11"]["status"], "NO_SHOW")

    def test_another_doctors_appointment_is_not_found(self):
        self.add(11)
        self.appts.items["11"]["doctor_id"] = "99"
        self.assertEqual(self.outcome(11, "CANCELLED")[0], 404)


class CancelledFilterTests(Base):
    """One Cancelled filter shows both kinds of cancellation, for patients and for doctors."""

    def fill(self):
        self.add(1, status="CONFIRMED")
        self.add(2, status="CANCELLED_BY_PATIENT")
        self.add(3, status="CANCELLED_BY_DOCTOR")
        self.add(4, status="COMPLETED")
        self.add(5, status="NO_SHOW")

    def test_patient_list_cancelled_returns_both_kinds(self):
        self.fill()
        result = bd._list_appointments(self.appts, 100, {"status": "CANCELLED", "page_size": "50"})
        self.assertEqual(sorted(a["status"] for a in result["appointments"]),
                         ["CANCELLED_BY_DOCTOR", "CANCELLED_BY_PATIENT"])
        self.assertEqual(result["pagination"]["total"], 2)

    def test_patient_list_exact_status_still_works(self):
        self.fill()
        result = bd._list_appointments(self.appts, 100, {"status": "CANCELLED_BY_PATIENT"})
        self.assertEqual([a["appointment_id"] for a in result["appointments"]], ["2"])

    def test_patient_list_rejects_an_unknown_status(self):
        with self.assertRaises(bd.ApiError):
            bd._list_appointments(self.appts, 100, {"status": "BOGUS"})

    def test_doctor_list_cancelled_returns_both_kinds(self):
        self.fill()
        rows = lambda cursor, items: [{"appointment_id": i["appointment_id"], "status": i["status"]} for i in items]
        today = datetime.now(timezone.utc).date()
        query = {"from": (today - timedelta(days=1)).isoformat(), "to": (today + timedelta(days=5)).isoformat(),
                 "status": "CANCELLED"}
        with mock.patch.object(dd, "_doctor_appointment_rows", rows):
            status, body = self.call(FakeRds(), "GET", "/doctors/me/appointments", query=query)
        self.assertEqual(status, 200)
        self.assertEqual(sorted(a["status"] for a in body["appointments"]),
                         ["CANCELLED_BY_DOCTOR", "CANCELLED_BY_PATIENT"])


class SlotStateTableRemovedTests(unittest.TestCase):
    def test_the_old_dynamodb_slot_table_is_gone_from_the_templates(self):
        text = "".join(p.read_text() for p in (ROOT / "infrastructure").glob("*.yaml"))
        self.assertNotIn("SlotStateTable", text)
        self.assertNotIn("slot-state", text)


if __name__ == "__main__":
    unittest.main()
