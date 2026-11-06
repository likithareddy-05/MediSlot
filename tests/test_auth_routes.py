"""Doctors and patients share one set of sign-in routes: /auth/register, /auth/login, /auth/refresh, /auth/logout."""
import os
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "auth", "doctor", "patient"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {
    "AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
    "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "JWT_SIGNING_SECRET_PARAMETER": "jwt",
    "REFRESH_TOKENS_TABLE": "rt", "ACCESS_TOKEN_TTL_PARAMETER": "a", "REFRESH_TOKEN_TTL_PARAMETER": "r",
    "BCRYPT_WORK_FACTOR_PARAMETER": "b",
}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401  (boto3 stand-ins when it is not installed)
import auth_handler  # noqa: E402

TEMPLATE = (ROOT / "infrastructure" / "authentication.yaml").read_text(encoding="utf-8")
BFF = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")


class LambdaRoutingTests(unittest.TestCase):
    def test_route_table_has_only_the_shared_routes(self):
        self.assertEqual(set(auth_handler.ROUTES), {
            ("POST", "/auth/register"), ("POST", "/auth/login"), ("POST", "/auth/refresh"),
            ("POST", "/auth/logout"), ("GET", "/lookups"),
        })

    def test_doctor_specific_routes_return_404(self):
        for resource in ("/auth/doctors/login", "/auth/doctors/register"):
            result = auth_handler.lambda_handler({"httpMethod": "POST", "resource": resource}, None)
            self.assertEqual(result["statusCode"], 404, resource)


class ApiGatewayTemplateTests(unittest.TestCase):
    def children_of(self, parent):
        blocks = re.findall(r"\n  (\w+):\n    Type: AWS::ApiGateway::Resource\n    Properties:\n(.*?)(?=\n\n|\Z)", TEMPLATE, re.S)
        found = set()
        for _, body in blocks:
            if re.search(r"ParentId: !Ref " + parent + r"\b", body):
                found.add(re.search(r"PathPart: (\S+)", body).group(1))
        return found

    def test_auth_has_only_the_four_shared_routes(self):
        self.assertEqual(self.children_of("AuthResource"), {"register", "login", "refresh", "logout"})

    def test_no_doctor_auth_resources_remain(self):
        self.assertNotIn("AuthDoctors", TEMPLATE)

    def test_doctor_api_is_still_there(self):
        self.assertIn("DoctorsMeGetMethod", TEMPLATE)
        self.assertIn("DoctorMaterializePostMethod", TEMPLATE)


class DashboardBackendTests(unittest.TestCase):
    def test_dashboard_uses_the_shared_routes(self):
        self.assertIn('_api_call("auth/login"', BFF)
        self.assertIn('_api_call("auth/register"', BFF)
        self.assertNotIn("auth/doctors", BFF)


if __name__ == "__main__":
    unittest.main()
