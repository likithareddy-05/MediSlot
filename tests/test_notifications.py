"""Milestone 6: notifications (EventBridge -> SQS -> Lambda -> SNS), reminders and the no-show sweeper.

* All eight notifications of the Frozen Rule Set reach SNS for the right recipients.
* Exactly once: the same event delivered twice (or in parallel), or the reminder dispatcher run
  twice, publishes each notification once. The DynamoDB dispatch log decides.
* A failed SNS publish is retried by SQS and then sent exactly once.
* The no-show sweeper marks appointments still CONFIRMED 30 minutes after their end, once.
"""
import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from fakes_workflows import Milestone6Harness, nt  # noqa: I001 (sets up paths and stubs)
from fakes_booking import slot_row
import appointment_store as store
import doctor_domain as dd

UTC = timezone.utc


def iso(value):
    return value.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def sqs(*events):
    return {"Records": [{"messageId": f"m{i}", "body": json.dumps(e)} for i, e in enumerate(events)]}


def bus_event(detail_type, **detail):
    return {"source": "medislot.booking", "detail-type": detail_type, "detail": detail}


def appointment(aid, start, patient=201, doctor=101, status="CONFIRMED", minutes=30):
    return {"appointment_id": str(aid), "patient_id": str(patient), "doctor_id": str(doctor), "slot_id": f"S{aid}",
            "consultation_type_id": "2", "consultation_name": "Standard Consult", "status": status,
            "start_time_utc": iso(start), "end_time_utc": iso(start + timedelta(minutes=minutes)),
            "fee_amount": "800.00", "doctor_name": "Asha Rao"}


class NotificationHarness(Milestone6Harness):
    def sent(self):
        return sorted((a["event_type"], a["recipient_role"], a["recipient_id"])
                      for a in map(self.sns.attributes, self.sns.messages))


class FrozenRuleSetEventTests(NotificationHarness):
    EVENTS = [
        bus_event("AppointmentBooked", appointment_id="1", patient_id=201, doctor_id=101),
        bus_event("AppointmentRescheduled", appointment_id="2", patient_id=201, doctor_id=101, reschedule_count=1),
        bus_event("AppointmentCancelled", appointment_id="3", patient_id=201, doctor_id=101, cancelled_by="PATIENT"),
        bus_event("AppointmentCancelled", appointment_id="4", patient_id=201, doctor_id=101, cancelled_by="DOCTOR"),
        bus_event("WaitlistOfferIssued", offer_id=7, patient_id=202, slot_id="S"),
        bus_event("WaitlistOfferExpired", offer_id=7, patient_id=202, slot_id="S"),
        bus_event("WaitlistJoined", entry_id=3, patient_id=203, slot_id="S", position=1),
        bus_event("SlotSeatFreed", slot_id="S"),  # not a notification
    ]

    def test_each_event_reaches_the_right_recipients(self):
        self.assertEqual(nt.event_handler(sqs(*self.EVENTS), None), {"batchItemFailures": []})
        self.assertEqual(self.sent(), sorted([
            ("APPOINTMENT_BOOKED", "PATIENT", "201"), ("APPOINTMENT_BOOKED", "DOCTOR", "101"),
            ("APPOINTMENT_RESCHEDULED", "PATIENT", "201"), ("APPOINTMENT_RESCHEDULED", "DOCTOR", "101"),
            ("APPOINTMENT_CANCELLED_BY_PATIENT", "PATIENT", "201"), ("APPOINTMENT_CANCELLED_BY_PATIENT", "DOCTOR", "101"),
            ("APPOINTMENT_CANCELLED_BY_DOCTOR", "PATIENT", "201"),
            ("WAITLIST_OFFER_ISSUED", "PATIENT", "202"), ("WAITLIST_OFFER_EXPIRED", "PATIENT", "202"),
            ("WAITLIST_JOINED", "PATIENT", "203")]))
        self.assertEqual(self.metric_total("NotificationsDispatched"), 10)

    def test_the_same_event_delivered_twice_is_sent_once(self):
        nt.event_handler(sqs(*self.EVENTS), None)
        nt.event_handler(sqs(*self.EVENTS), None)
        self.assertEqual(len(self.sns.messages), 10)
        self.assertEqual({i["state"] for i in self.dispatch_log.items.values()}, {"SENT"})

    def test_parallel_duplicate_deliveries_are_sent_once(self):
        event = self.EVENTS[0]
        threads = [threading.Thread(target=nt.event_handler, args=(sqs(event), None)) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(self.sns.messages), 2)

    def test_a_failed_publish_is_retried_by_sqs_and_then_sent_once(self):
        self.sns.fail_next = 1
        result = nt.event_handler(sqs(self.EVENTS[0]), None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "m0"}]})
        nt.event_handler(sqs(self.EVENTS[0]), None)  # SQS redelivers
        nt.event_handler(sqs(self.EVENTS[0]), None)
        self.assertEqual(self.sent(), [("APPOINTMENT_BOOKED", "DOCTOR", "101"), ("APPOINTMENT_BOOKED", "PATIENT", "201")])

    def test_messages_carry_no_personal_data(self):
        nt.event_handler(sqs(self.EVENTS[0]), None)
        body = self.sns.messages[0]["Message"]
        for word in ("@", "phone", "first_name", "last_name", "{"):
            self.assertNotIn(word, body)

    def test_dispatch_log_items_expire_by_ttl(self):
        nt.event_handler(sqs(self.EVENTS[0]), None)
        self.assertTrue(all(i["expires_at"] > i["claimed_at"] for i in self.dispatch_log.items.values()))


class EmailFormatTests(NotificationHarness):
    """The notifications read as professional e-mails, not JSON."""

    def setUp(self):
        super().setUp()
        self.appts.items["42"] = {**appointment(42, datetime(2026, 11, 30, 10, 30, tzinfo=UTC)),
                                  "doctor_name": "Karthik Rao", "consultation_name": "Standard Consult",
                                  "fee_amount": "1000.00"}

    def test_booked_email_for_the_patient(self):
        nt.event_handler(sqs(bus_event("AppointmentBooked", appointment_id="42", patient_id=201, doctor_id=101)), None)
        mail = [m for m in self.sns.of_type("APPOINTMENT_BOOKED") if m["recipient_role"] == "PATIENT"][0]
        self.assertEqual(mail["subject"], "MediSlot: Your appointment is confirmed")
        body = mail["body"]
        self.assertTrue(body.startswith("Dear Patient,"))
        for text in ("Appointment details", "Dr. Karthik Rao", "Standard Consult",
                     "Mon, 30 Nov 2026, 4:00 PM (Asia/Kolkata)", "\u20b91,000.00", "Kind regards,"):
            self.assertIn(text, body)
        with self.assertRaises(ValueError):
            json.loads(body)

    def test_doctor_email_names_the_patient_by_number(self):
        nt.event_handler(sqs(bus_event("AppointmentBooked", appointment_id="42", patient_id=201, doctor_id=101)), None)
        mail = [m for m in self.sns.of_type("APPOINTMENT_BOOKED") if m["recipient_role"] == "DOCTOR"][0]
        self.assertEqual(mail["subject"], "MediSlot: New appointment booked")
        self.assertIn("Dear Doctor,", mail["body"])
        self.assertIn("Patient #201", mail["body"])

    def test_offer_email_says_how_long_the_seat_is_held(self):
        nt.event_handler(sqs(bus_event("WaitlistOfferIssued", offer_id=9, patient_id=202, slot_id="S",
                                       doctor_name="Karthik Rao", consultation_name="Standard Consult", fee=1000,
                                       timezone="Asia/Kolkata", start_time_utc="2026-11-30T10:30:00Z",
                                       expires_at_utc="2026-11-28T14:15:00Z")), None)
        mail = self.sns.of_type("WAITLIST_OFFER_ISSUED")[0]
        self.assertEqual(mail["subject"], "MediSlot: A seat is being held for you - please confirm")
        self.assertIn("Seat held until", mail["body"])
        self.assertIn("Sat, 28 Nov 2026, 7:45 PM (Asia/Kolkata)", mail["body"])
        self.assertIn("choose Accept", mail["body"])

    def test_cancellation_email_shows_the_refund(self):
        nt.event_handler(sqs(bus_event("AppointmentCancelled", appointment_id="42", patient_id=201, doctor_id=101,
                                       cancelled_by="PATIENT", refund={"percent": 50.0, "amount": 500.0})), None)
        body = [m for m in self.sns.of_type("APPOINTMENT_CANCELLED_BY_PATIENT") if m["recipient_role"] == "PATIENT"][0]["body"]
        self.assertIn("\u20b9500.00 (50% of the fee)", body)

    def test_reschedule_email_shows_old_and_new_time(self):
        nt.event_handler(sqs(bus_event("AppointmentRescheduled", appointment_id="42", patient_id=201, doctor_id=101,
                                       reschedule_count=1, start_time_utc="2026-12-01T04:30:00Z",
                                       previous_start_time_utc="2026-11-30T10:30:00Z")), None)
        body = [m for m in self.sns.of_type("APPOINTMENT_RESCHEDULED") if m["recipient_role"] == "PATIENT"][0]["body"]
        self.assertIn("New date and time:", body)
        self.assertIn("Tue, 01 Dec 2026, 10:00 AM (Asia/Kolkata)", body)
        self.assertIn("Previous time:", body)
        self.assertIn("1 of 2", body)

    def test_every_notification_type_has_an_email(self):
        for event_type in nt.TEMPLATES:
            subject, body = nt.compose_email(event_type, "PATIENT", 5, {"appointment_id": "1"})
            self.assertTrue(subject.startswith("MediSlot: ") and subject.isascii() and len(subject) <= 100, subject)
            self.assertIn("Kind regards,", body)


class EndToEndBookingNotificationTests(NotificationHarness):
    slots = [slot_row("7#2#E2E", capacity=2, ctype=2)]

    def test_a_real_booking_produces_the_booked_notifications(self):
        status, _ = self.book(1, "7#2#E2E")
        self.assertEqual(status, 201)
        events = [{"detail-type": k, "detail": d} for k, d in self.events]
        nt.event_handler(sqs(*events), None)
        self.assertEqual(self.sent(), [("APPOINTMENT_BOOKED", "DOCTOR", "7"), ("APPOINTMENT_BOOKED", "PATIENT", "1")])


class ReminderTests(NotificationHarness):
    def setUp(self):
        super().setUp()
        self.now = datetime.now(UTC).replace(microsecond=0)
        for item in (appointment(1, self.now + timedelta(hours=23)),       # 24h reminder due
                     appointment(2, self.now + timedelta(hours=1, minutes=50)),  # 2h reminder due
                     appointment(3, self.now + timedelta(hours=30)),       # nothing yet
                     appointment(4, self.now + timedelta(hours=20), status="CANCELLED_BY_PATIENT")):
            self.appts.items[item["appointment_id"]] = item

    def test_due_reminders_are_sent_and_a_second_run_sends_nothing(self):
        first = nt.send_reminders(self.now)
        second = nt.send_reminders(self.now)
        self.assertEqual(first, {"reminders_sent": 2, "already_sent": 0})
        self.assertEqual(second, {"reminders_sent": 0, "already_sent": 2})
        self.assertEqual(self.sent(), [("REMINDER_24H", "PATIENT", "201"), ("REMINDER_2H", "PATIENT", "201")])

    def test_each_appointment_gets_both_reminders_once_over_time(self):
        for minutes in range(0, 30 * 60, 5):  # the dispatcher runs every 5 minutes for 30 hours
            nt.send_reminders(self.now + timedelta(minutes=minutes))
        kinds = sorted((a["appointment_id"], a["event_type"]) for a in map(self.sns.attributes, self.sns.messages))
        self.assertEqual(kinds, [("1", "REMINDER_24H"), ("1", "REMINDER_2H"), ("2", "REMINDER_2H"),
                                 ("3", "REMINDER_24H"), ("3", "REMINDER_2H")])

    def test_a_rescheduled_appointment_is_reminded_for_its_new_time(self):
        nt.send_reminders(self.now)
        self.appts.items["1"]["start_time_utc"] = iso(self.now + timedelta(hours=22))
        nt.send_reminders(self.now)
        self.assertEqual(len(self.sns.of_type("REMINDER_24H")), 2)


class NoShowSweeperTests(unittest.TestCase):
    def setUp(self):
        from fakes_booking import FakeDynamo, FakeRds, appointments_table
        self.rds = FakeRds()
        now = datetime.now(UTC).replace(microsecond=0)
        self.items = {"1": appointment(1, now - timedelta(hours=2)),                     # ended 90 min ago
                      "2": appointment(2, now - timedelta(minutes=50), minutes=30),     # ended 20 min ago
                      "3": appointment(3, now - timedelta(hours=3), status="COMPLETED"),
                      "4": appointment(4, now + timedelta(hours=3))}
        self.appts = appointments_table(list(self.items.values()))
        patches = [mock.patch.object(dd, "ddb", FakeDynamo(appointments=self.appts)),
                   mock.patch.object(dd, "db_connect", self.rds.connect),
                   mock.patch.object(dd, "emit_metric", lambda *a, **k: None)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_marks_only_appointments_30_minutes_past_their_end_and_is_safe_to_run_twice(self):
        first = dd.mark_no_shows()
        second = dd.mark_no_shows()
        self.assertEqual(first["marked_no_show"], 1)
        self.assertEqual(second["marked_no_show"], 0)
        statuses = {k: v["status"] for k, v in self.appts.items.items()}
        self.assertEqual(statuses, {"1": store.NO_SHOW, "2": "CONFIRMED", "3": "COMPLETED", "4": "CONFIRMED"})
        self.assertEqual([r["trigger_type"] for r in self.rds.ledger.values()], ["NO_SHOW"])


if __name__ == "__main__":
    unittest.main()
