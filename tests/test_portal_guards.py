"""Single Flask app: landing page, role guards, patient BFF allow-list, logout (no DB access, API client only)."""
import importlib.util
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
try:
    import flask  # noqa: F401
    import flask_session  # noqa: F401
    HAVE_FLASK = True
except ImportError:  # CI installs dashboard/requirements.txt; skip quietly when running without it
    HAVE_FLASK = False

if HAVE_FLASK:
    os.environ.update({"MEDISLOT_API_BASE_URL": "http://api.invalid/dev", "SESSION_COOKIE_SECURE": "false",
                       "SESSION_FILE_DIR": tempfile.mkdtemp(prefix="medislot-sessions-")})
    spec = importlib.util.spec_from_file_location("medislot_dashboard_app", ROOT / "dashboard" / "app.py")
    web = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(web)


class FakeResponse:
    def __init__(self, status=200, payload=None):
        self.status_code, self._payload = status, payload if payload is not None else {}
        self.content = b"{}"
        self.headers = {"Content-Type": "application/json"}
    def json(self): return self._payload
    @property
    def text(self): return "{}"


def login_payload(role):
    return {"access_token": "a", "refresh_token": "r", "user": {"user_id": 1, "email": "x@y.com", "role": role}}


@unittest.skipUnless(HAVE_FLASK, "Flask is not installed")
class PortalTests(unittest.TestCase):
    def setUp(self):
        self.client = web.app.test_client()

    def csrf(self):
        return self.client.get("/bff/session").get_json()["csrf_token"]

    def sign_in(self, role):
        token = self.csrf()
        path = "/bff/patient/login" if role == "PATIENT" else "/bff/login"
        with mock.patch.object(web, "_api_call", return_value=FakeResponse(200, login_payload(role))):
            result = self.client.post(path, json={"email": "x@y.com", "password": "pw"}, headers={"X-CSRF-Token": token})
        self.assertEqual(result.status_code, 200, result.get_data(as_text=True))
        return result.get_json()["csrf_token"]

    def test_landing_has_both_logins(self):
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn("Login as Doctor", html)
        self.assertIn("Login as Patient", html)

    def test_portals_are_public_entry_points(self):
        self.assertEqual(self.client.get("/doctor/dashboard").status_code, 200)
        self.assertEqual(self.client.get("/patient/dashboard").status_code, 200)

    def test_patient_session_is_redirected_away_from_doctor_portal(self):
        self.sign_in("PATIENT")
        for path in ("/doctor/dashboard", "/doctor", "/doctor/login"):
            result = self.client.get(path)
            self.assertEqual((result.status_code, result.headers["Location"]), (302, "/"), path)
        self.assertEqual(self.client.get("/patient/dashboard").status_code, 200)

    def test_doctor_session_is_redirected_away_from_patient_portal(self):
        self.sign_in("DOCTOR")
        result = self.client.get("/patient/dashboard")
        self.assertEqual((result.status_code, result.headers["Location"]), (302, "/"))
        self.assertEqual(self.client.get("/doctor/dashboard").status_code, 200)

    def test_wrong_role_cannot_sign_in_to_portal(self):
        token = self.csrf()
        with mock.patch.object(web, "_api_call", return_value=FakeResponse(200, login_payload("DOCTOR"))):
            result = self.client.post("/bff/patient/login", json={"email": "x@y.com", "password": "pw"},
                                      headers={"X-CSRF-Token": token})
        self.assertEqual(result.status_code, 403)
        self.assertIn("patient", result.get_json()["message"])
        self.assertEqual(self.client.get("/bff/session").get_json().get("authenticated"), False)

    def test_bff_apis_are_role_scoped(self):
        self.sign_in("PATIENT")
        self.assertEqual(self.client.get("/bff/api/profile").status_code, 403)
        self.client.post("/bff/logout", headers={"X-CSRF-Token": self.csrf()})
        self.sign_in("DOCTOR")
        self.assertEqual(self.client.get("/bff/patient/api/profile").status_code, 403)

    def test_patient_api_requires_sign_in(self):
        self.assertEqual(self.client.get("/bff/patient/api/profile").status_code, 401)

    def test_patient_routes_map_to_documented_api_paths(self):
        expected = {"/bff/patient/api/profile": "patients/me", "/bff/patient/api/doctors": "doctors",
                    "/bff/patient/api/doctors/5": "doctors/5", "/bff/patient/api/doctors/5/slots": "doctors/5/slots"}
        self.sign_in("PATIENT")
        for url, upstream in expected.items():
            with mock.patch.object(web, "_api_call", return_value=FakeResponse(200, {})) as call:
                self.assertEqual(self.client.get(url).status_code, 200, url)
            self.assertEqual(call.call_args.args[0], upstream)
        self.assertEqual(self.client.get("/bff/patient/api/doctors/me/schedule").status_code, 404)
        self.assertEqual(self.client.get("/bff/patient/api/auth/login").status_code, 404)

    def test_patient_writes_need_csrf_and_only_profile_is_writable(self):
        csrf = self.sign_in("PATIENT")
        self.assertEqual(self.client.put("/bff/patient/api/profile", json={"city": "X"}).status_code, 403)
        self.assertEqual(self.client.delete("/bff/patient/api/doctors/5", headers={"X-CSRF-Token": csrf}).status_code, 405)
        with mock.patch.object(web, "_api_call", return_value=FakeResponse(200, {})) as call:
            result = self.client.put("/bff/patient/api/profile", json={"city": "X"}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(call.call_args.args[:2], ("patients/me", "PUT"))

    def test_logout_calls_api_and_clears_session(self):
        csrf = self.sign_in("PATIENT")
        with mock.patch.object(web, "_api_call", return_value=FakeResponse(200, {})) as call:
            result = self.client.post("/bff/logout", headers={"X-CSRF-Token": csrf})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(call.call_args.args[:2], ("auth/logout", "POST"))
        self.assertEqual(self.client.get("/bff/patient/api/profile").status_code, 401)

    def test_registration_pages_link_to_registration(self):
        for page, marker in (("doctor_dashboard.html", "showRegister"), ("patient_dashboard.html", "showRegister")):
            self.assertIn(marker, (ROOT / "dashboard" / page).read_text())

    def test_app_is_a_pure_api_client(self):
        source = (ROOT / "dashboard" / "app.py").read_text()
        self.assertIsNone(re.search(r"^\s*(import|from)\s+(pymysql|boto3|mysql)", source, re.M))


if __name__ == "__main__":
    unittest.main()
