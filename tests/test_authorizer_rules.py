"""ROLE_ROUTE_RULES from the CloudFormation template decide who can call which route."""
import json
import os
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "lambdas" / "auth"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
TEMPLATE = (ROOT / "infrastructure" / "authentication.yaml").read_text()
RULES = re.search(r"ROLE_ROUTE_RULES: '(\{.*?\})'", TEMPLATE).group(1)
for key, value in {"AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                   "JWT_SIGNING_SECRET_PARAMETER": "/x", "ROLE_ROUTE_RULES": RULES}.items():
    os.environ[key] = value

import aws_stubs  # noqa: E402,F401
import authorizer  # noqa: E402


def allowed(role, method, path):
    arn = f"arn:aws:execute-api:ap-south-1:123456789012:abc123/dev/{method}/{path.lstrip('/')}"
    return authorizer.role_is_allowed({"role": role}, {"methodArn": arn})


class RoleRuleTests(unittest.TestCase):
    def test_template_rules_are_valid_json(self):
        self.assertGreaterEqual(len(json.loads(RULES)), 20)

    def test_patient_can_manage_own_profile_only_under_patients(self):
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            self.assertTrue(allowed("PATIENT", method, "patients/me"), method)
            self.assertFalse(allowed("DOCTOR", method, "patients/me"), method)

    def test_patient_can_search_and_view_doctors_and_slots(self):
        for path in ("doctors", "doctors/12", "doctors/12/slots"):
            self.assertTrue(allowed("PATIENT", "GET", path), path)
            self.assertTrue(allowed("DOCTOR", "GET", path), path)

    def test_patient_cannot_reach_doctor_only_routes(self):
        for method, path in (("GET", "doctors/me"), ("GET", "doctors/me/schedule"), ("PUT", "doctors/me"),
                             ("POST", "doctors/me/availability"), ("DELETE", "doctors/me/availability/3"),
                             ("POST", "doctors/me/consultation-types")):
            self.assertFalse(allowed("PATIENT", method, path), f"{method} {path}")

    def test_patient_cannot_write_to_doctor_directory(self):
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            self.assertFalse(allowed("PATIENT", method, "doctors/12"), method)

    def test_doctor_can_use_own_routes(self):
        self.assertTrue(allowed("DOCTOR", "GET", "doctors/me"))
        self.assertTrue(allowed("DOCTOR", "GET", "doctors/me/schedule"))
        self.assertTrue(allowed("DOCTOR", "POST", "doctors/me/availability"))

    def test_logout_is_allowed_for_both_and_unknown_routes_denied(self):
        for role in ("DOCTOR", "PATIENT"):
            self.assertTrue(allowed(role, "POST", "auth/logout"))
            self.assertFalse(allowed(role, "GET", "admin/anything"))


    def test_patient_can_reach_the_milestone_6_routes(self):
        routes = (("PATCH", "appointments/12"), ("DELETE", "appointments/12"),
                  ("POST", "slots/7%231%2320261201T033000Z/waitlist"), ("DELETE", "slots/7%231%2320261201T033000Z/waitlist"),
                  ("GET", "patients/me/waitlists"), ("POST", "waitlist-offers/9/accept"))
        for method, path in routes:
            self.assertTrue(allowed("PATIENT", method, path), f"{method} {path}")
            self.assertFalse(allowed("DOCTOR", method, path), f"{method} {path}")

    def test_doctor_slot_cancel_is_still_doctor_only(self):
        self.assertTrue(allowed("DOCTOR", "DELETE", "doctors/me/slots/7%231%23X"))
        self.assertFalse(allowed("PATIENT", "DELETE", "doctors/me/slots/7%231%23X"))

    def test_every_patient_api_route_in_the_template_is_allowed_for_patients(self):
        """Each route wired to the booking Lambda must pass the authorizer for a PATIENT."""
        methods = re.findall(r"ResourceId: !Ref (\w+)\n\s+HttpMethod: (\w+)\n(?:.*\n){0,14}?\s+- FunctionArn: !GetAtt BookingDomainFunction\.Arn",
                             TEMPLATE)
        self.assertGreaterEqual(len(methods), 10)
        samples = {"AppointmentsRootResource": "appointments", "AppointmentIdResource": "appointments/12",
                   "AppointmentCancelResource": "appointments/12/cancel", "PatientsMeRefundsResource": "patients/me/refunds",
                   "SlotWaitlistResource": "slots/7%231%23X/waitlist", "PatientsMeWaitlistsResource": "patients/me/waitlists",
                   "WaitlistOfferAcceptResource": "waitlist-offers/9/accept"}
        for resource, method in methods:
            self.assertIn(resource, samples, f"add a sample path for {resource}")
            self.assertTrue(allowed("PATIENT", method, samples[resource]), f"{method} {samples[resource]}")


if __name__ == "__main__":
    unittest.main()
