"""Milestone 5 integration tests against the DEPLOYED API (run in CI after deployment).

    MEDISLOT_API_BASE_URL=https://<id>.execute-api.<region>.amazonaws.com/<stage> \
        python -m unittest discover -s tests/integration -v

Builds its own fixtures through the public API: one doctor with a Health Camp Session of
capacity 3, and 20 patients. Then:
  * Mandatory test 1: all 20 patients book the same slot at once -> exactly 3 succeed, the rest get 409.
  * Mandatory test 2: one patient replays the same Idempotency-Key -> exactly one appointment.
  * Cancellation: the refund tier, the ledger and the freed seat are checked on the real stack.
Everything is cleaned up afterwards (appointments cancelled, slot deleted, accounts removed).
Skipped when MEDISLOT_API_BASE_URL is not set.
"""
import os
import random
import threading
import time
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

BASE = os.environ.get("MEDISLOT_API_BASE_URL", "").rstrip("/")
try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

PASSWORD = "Integration#2026x"
PATIENTS = 20
CAPACITY = 3


def register(payload):
    return requests.post(f"{BASE}/auth/register", json=payload, timeout=30)


def login(email):
    r = requests.post(f"{BASE}/auth/login", json={"email": email, "password": PASSWORD}, timeout=30)
    body = r.json() if r.ok else {}
    return {"Authorization": f"Bearer {body.get('access_token', '')}"}, (body.get("user") or {}).get("user_id")


def call(headers, method, path, **kwargs):
    return requests.request(method, f"{BASE}/{path.lstrip('/')}", headers=headers, timeout=60, **kwargs)


def patient_payload(email, n):
    return {"role": "PATIENT", "email": email, "password": PASSWORD, "first_name": f"Itest{n}", "last_name": "Booker",
            "phone": "9876500000", "date_of_birth": "1990-01-15", "gender": "OTHER", "address": "1 Test Street",
            "city": "Hyderabad", "emergency_contact_name": "Test Contact", "emergency_contact_phone": "9876500001"}


def doctor_payload(email):
    return {"role": "DOCTOR", "email": email, "password": PASSWORD, "first_name": "Itest", "last_name": "Doctor",
            "phone": "9876500002", "date_of_birth": "1980-05-05", "gender": "OTHER", "address": "2 Test Street",
            "specialization_id": 1, "hospital_id": 1, "license_number": f"TS-{random.randint(10000, 99999)}",
            "years_of_experience": 10, "clinic_address": "2 Test Street, Hyderabad"}


@unittest.skipUnless(BASE and requests, "MEDISLOT_API_BASE_URL not set; deployed-API tests skipped")
class DeployedBookingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tag = f"{int(time.time())}-{uuid.uuid4().hex[:6]}"
        cls.tag = tag
        cls.doctor_email = f"itest-doc-{tag}@example.com"
        assert register(doctor_payload(cls.doctor_email)).status_code in (200, 201), "doctor registration failed"
        cls.doctor, cls.doctor_id = login(cls.doctor_email)
        cls.fee = 400
        r = call(cls.doctor, "POST", "doctors/me/consultation-types",
                 json={"consultation_name": "Health Camp Session", "fee": cls.fee, "slot_capacity": CAPACITY})
        assert r.status_code in (200, 201), r.text
        # an availability window on the weekday that falls 3+ days from now, so the slot is > 48 h away
        target = datetime.now(timezone.utc).date() + timedelta(days=4)
        r = call(cls.doctor, "POST", "doctors/me/availability",
                 json={"weekday": target.weekday(), "start_time": "10:00", "end_time": "11:00",
                       "timezone_name": "Asia/Kolkata", "consultation_type_id": 4})
        assert r.status_code in (200, 201), r.text
        cls.availability_id = (r.json() or {}).get("availability_pattern_id")
        assert call(cls.doctor, "POST", "doctors/me/materialize", json={}).status_code == 200

        cls.patients = []
        for n in range(PATIENTS):
            email = f"itest-pat-{tag}-{n}@example.com"
            assert register(patient_payload(email, n)).status_code in (200, 201)
            headers, user_id = login(email)
            cls.patients.append({"email": email, "headers": headers, "user_id": user_id})
        cls.slot = cls._find_slot(cls.patients[0]["headers"], target)
        cls.booked = []  # (patient index, appointment id)

    @classmethod
    def _find_slot(cls, headers, target):
        frm, to = target.isoformat(), (target + timedelta(days=1)).isoformat()
        r = call(headers, "GET", f"doctors/{cls.doctor_id}/slots", params={"from": frm, "to": to})
        assert r.status_code == 200, r.text
        slots = [s for s in r.json()["slots"] if s["capacity"] == CAPACITY]
        assert slots, "no capacity-3 slot was generated"
        return slots[0]

    @classmethod
    def tearDownClass(cls):
        for p in getattr(cls, "patients", []):
            r = call(p["headers"], "GET", "appointments", params={"upcoming": "true"})
            if r.ok:
                for a in r.json().get("appointments", []):
                    call(p["headers"], "POST", f"appointments/{a['appointment_id']}/cancel", json={})
            call(p["headers"], "DELETE", "patients/me")
        if getattr(cls, "slot", None):
            call(cls.doctor, "DELETE", f"doctors/me/slots/{requests.utils.quote(cls.slot['slot_id'], safe='')}")
        if getattr(cls, "availability_id", None):
            call(cls.doctor, "DELETE", f"doctors/me/availability/{cls.availability_id}")
        call(cls.doctor, "DELETE", "doctors/me/consultation-types/4")

    def book(self, patient, key=None):
        return requests.post(f"{BASE}/appointments", json={"slot_id": self.slot["slot_id"]}, timeout=60,
                             headers={**patient["headers"], "Idempotency-Key": key or f"it-{uuid.uuid4().hex}"})

    def test_1_concurrency_exactly_capacity_bookings_succeed(self):
        results, barrier = [None] * PATIENTS, threading.Barrier(PATIENTS)

        def worker(i):
            barrier.wait()
            results[i] = self.book(self.patients[i], f"it-book-{self.tag}-{i}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(PATIENTS)]
        [t.start() for t in threads]
        [t.join(90) for t in threads]
        statuses = [r.status_code for r in results]
        self.assertEqual(statuses.count(201), CAPACITY, statuses)
        self.assertTrue(all(s in (201, 409) for s in statuses), statuses)
        codes = {r.json().get("code") for r in results if r.status_code == 409}
        self.assertTrue(codes <= {"SLOT_FULL", "BOOKING_CONTENTION"}, codes)
        type(self).booked = [(i, r.json()["appointment"]["appointment_id"]) for i, r in enumerate(results)
                             if r.status_code == 201]
        # the public slot view agrees: no free seats
        r = call(self.patients[0]["headers"], "GET", f"doctors/{self.doctor_id}/slots",
                 params={"from": self.slot["slot_date"], "to": self.slot["slot_date"]})
        mine = [s for s in r.json()["slots"] if s["slot_id"] == self.slot["slot_id"]][0]
        self.assertEqual(mine["free_seats"], 0)

    def test_2_idempotent_replay_returns_the_original_appointment(self):
        winner_index, appointment_id = self.booked[0]
        patient = self.patients[winner_index]
        key = f"it-book-{self.tag}-{winner_index}"
        for _ in range(3):
            replay = self.book(patient, key)
            self.assertIn(replay.status_code, (200, 201), replay.text)
            self.assertEqual(replay.json()["appointment"]["appointment_id"], appointment_id)
        mine = [a for a in call(patient["headers"], "GET", "appointments").json()["appointments"]
                if a["slot_id"] == self.slot["slot_id"]]
        self.assertEqual([a["appointment_id"] for a in mine], [appointment_id])  # exactly one appointment
        # the same key with a different request is refused
        other = requests.post(f"{BASE}/appointments", json={"slot_id": "some-other-slot"}, timeout=60,
                              headers={**patient["headers"], "Idempotency-Key": key})
        self.assertEqual(other.status_code, 422)

    def test_3_cancel_refunds_by_tier_and_frees_the_seat(self):
        winner_index, appointment_id = self.booked[0]
        patient = self.patients[winner_index]
        detail = call(patient["headers"], "GET", f"appointments/{appointment_id}").json()["appointment"]
        self.assertEqual(detail["cancellation_preview"]["tier"], "GT_48H")
        r = call(patient["headers"], "POST", f"appointments/{appointment_id}/cancel", json={})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["appointment"]["refund"]["amount"], float(self.fee))
        again = call(patient["headers"], "POST", f"appointments/{appointment_id}/cancel", json={})
        self.assertEqual(again.status_code, 200)
        ledger = call(patient["headers"], "GET", "patients/me/refunds").json()
        self.assertEqual(len(ledger["refunds"]), 1)  # cancelling twice did not refund twice
        self.assertEqual(ledger["total_refunded"], float(self.fee))
        # the freed seat can be booked by a patient who was turned away
        loser = next(p for i, p in enumerate(self.patients) if i not in {b[0] for b in self.booked})
        self.assertEqual(self.book(loser).status_code, 201)

    def test_4_patients_cannot_touch_each_others_appointments(self):
        winner_index, appointment_id = self.booked[1]
        outsider = next(p for i, p in enumerate(self.patients) if i not in {b[0] for b in self.booked})
        self.assertEqual(call(outsider["headers"], "GET", f"appointments/{appointment_id}").status_code, 404)
        self.assertEqual(call(outsider["headers"], "POST", f"appointments/{appointment_id}/cancel", json={}).status_code, 404)

    def test_5_doctor_cancelling_the_slot_refunds_everyone_in_full(self):
        r = call(self.doctor, "DELETE", f"doctors/me/slots/{requests.utils.quote(self.slot['slot_id'], safe='')}")
        self.assertEqual(r.status_code, 200, r.text)
        affected = r.json()["affected_appointments"]
        self.assertGreaterEqual(affected, 2)
        for index, appointment_id in self.booked[1:]:
            a = call(self.patients[index]["headers"], "GET", f"appointments/{appointment_id}").json()["appointment"]
            self.assertEqual(a["status"], "CANCELLED_BY_DOCTOR")
            self.assertEqual(a["refund"]["percent"], 100.0)
        type(self).slot = None  # already removed


if __name__ == "__main__":
    unittest.main()
