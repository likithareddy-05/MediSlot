"""Integration tests against the DEPLOYED API (run in CI after deployment).

    MEDISLOT_API_BASE_URL=https://<id>.execute-api.<region>.amazonaws.com/<stage> \
        python -m unittest discover -s tests/integration -v

Creates two throw-away patients, proves they cannot see each other's data, exercises
doctor search / public profile / slot availability and removes both accounts at the end.
Skipped when MEDISLOT_API_BASE_URL is not set (so local unit runs stay green).
"""
import os
import time
import unittest
import uuid

BASE = os.environ.get("MEDISLOT_API_BASE_URL", "").rstrip("/")
try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

PASSWORD = "Integration#2026x"


def patient_payload(email, first="Itest", city="Hyderabad"):
    return {"role": "PATIENT", "email": email, "password": PASSWORD, "first_name": first, "last_name": "Patient",
            "phone": "9876500000", "date_of_birth": "1990-01-15", "gender": "OTHER", "address": "1 Test Street",
            "city": city, "emergency_contact_name": "Test Contact", "emergency_contact_phone": "9876500001"}


class Session:
    def __init__(self, email, **kwargs):
        self.email = email
        self.register = requests.post(f"{BASE}/auth/register", json=patient_payload(email, **kwargs), timeout=30)
        login = requests.post(f"{BASE}/auth/login", json={"email": email, "password": PASSWORD}, timeout=30)
        self.login_status = login.status_code
        body = login.json() if login.ok else {}
        self.headers = {"Authorization": f"Bearer {body.get('access_token', '')}"}
        self.refresh_token = body.get("refresh_token")
        self.user_id = (body.get("user") or {}).get("user_id")

    def call(self, method, path, **kwargs):
        return requests.request(method, f"{BASE}/{path.lstrip('/')}", headers=self.headers, timeout=30, **kwargs)


@unittest.skipUnless(BASE and requests, "MEDISLOT_API_BASE_URL not set; deployed-API tests skipped")
class DeployedPatientApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tag = f"{int(time.time())}-{uuid.uuid4().hex[:6]}"
        cls.alice = Session(f"itest-alice-{tag}@example.com", first="Alice", city="Hyderabad")
        cls.bob = Session(f"itest-bob-{tag}@example.com", first="Bob", city="Chennai")

    @classmethod
    def tearDownClass(cls):
        for who in (cls.alice, cls.bob):
            if who.login_status == 200:
                who.call("DELETE", "patients/me")

    def test_00_registration_and_login_worked(self):
        for who in (self.alice, self.bob):
            self.assertEqual(who.register.status_code, 201, who.register.text)
            self.assertEqual(who.login_status, 200)

    def test_requests_without_token_are_rejected(self):
        for path in ("patients/me", "doctors", "doctors/1", "doctors/1/slots"):
            response = requests.get(f"{BASE}/{path}", timeout=30)
            self.assertIn(response.status_code, (401, 403), path)

    def test_profile_read_and_update(self):
        profile = self.alice.call("GET", "patients/me")
        self.assertEqual(profile.status_code, 200, profile.text)
        self.assertEqual(profile.json()["profile"]["first_name"], "Alice")
        update = self.alice.call("PUT", "patients/me", json={"city": "Pune"})
        self.assertEqual(update.status_code, 200, update.text)
        self.assertEqual(self.alice.call("GET", "patients/me").json()["profile"]["city"], "Pune")
        invalid = self.alice.call("PUT", "patients/me", json={"emergency_contact_phone": "1"})
        self.assertEqual(invalid.status_code, 400)

    def test_patient_data_is_isolated(self):
        self.assertEqual(self.bob.call("GET", "patients/me").json()["profile"]["first_name"], "Bob")
        # Trying to act on someone else's record through query/body ids must have no effect.
        self.bob.call("PUT", "patients/me", json={"city": "Delhi", "user_id": self.alice.user_id,
                                                   "patient_id": self.alice.user_id})
        self.assertNotEqual(self.alice.call("GET", "patients/me").json()["profile"]["city"], "Delhi")
        spoof = self.bob.call("GET", "patients/me", params={"patient_id": self.alice.user_id})
        self.assertEqual(spoof.json()["profile"]["first_name"], "Bob")
        self.assertIn(self.bob.call("GET", f"patients/{self.alice.user_id}").status_code, (403, 404))

    def test_doctor_routes_are_forbidden_for_patients(self):
        for method, path in (("GET", "doctors/me"), ("GET", "doctors/me/schedule"), ("POST", "doctors/me/availability")):
            self.assertEqual(self.alice.call(method, path).status_code, 403, path)

    def test_doctor_search_pagination_and_public_profile(self):
        page = self.alice.call("GET", "doctors", params={"page": 1, "page_size": 2})
        self.assertEqual(page.status_code, 200, page.text)
        body = page.json()
        self.assertEqual(body["pagination"]["page"], 1)
        self.assertLessEqual(len(body["doctors"]), 2)
        self.assertEqual(self.alice.call("GET", "doctors", params={"page_size": 500}).status_code, 400)
        nothing = self.alice.call("GET", "doctors", params={"name": "zzzz-no-such-doctor"}).json()
        self.assertEqual(nothing["pagination"]["total"], 0)
        if not body["doctors"]:
            self.skipTest("no doctors registered in this environment")
        doctor_id = body["doctors"][0]["doctor_id"]
        profile = self.alice.call("GET", f"doctors/{doctor_id}")
        self.assertEqual(profile.status_code, 200)
        doctor = profile.json()["doctor"]
        self.assertIn("consultation_types", doctor)
        for private in ("email", "phone", "date_of_birth", "license_number"):
            self.assertNotIn(private, doctor)
        filtered = self.alice.call("GET", "doctors", params={"specialisation": doctor["specialization"]}).json()
        self.assertGreaterEqual(filtered["pagination"]["total"], 1)

    def test_slot_availability_range(self):
        doctors = self.alice.call("GET", "doctors", params={"page_size": 1}).json()["doctors"]
        if not doctors:
            self.skipTest("no doctors registered in this environment")
        doctor_id = doctors[0]["doctor_id"]
        slots = self.alice.call("GET", f"doctors/{doctor_id}/slots")
        self.assertEqual(slots.status_code, 200, slots.text)
        for slot in slots.json()["slots"]:
            self.assertEqual(slot["free_seats"], slot["capacity"] - slot["booked_count"])
            self.assertIn(slot["status"], ("AVAILABLE", "FULL"))
        too_long = self.alice.call("GET", f"doctors/{doctor_id}/slots", params={"from": "2026-01-01", "to": "2026-12-31"})
        self.assertEqual(too_long.status_code, 400)
        self.assertEqual(self.alice.call("GET", "doctors/999999999/slots").status_code, 404)


if __name__ == "__main__":
    unittest.main()
