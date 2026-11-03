"""Hospitals and specializations are reference tables; doctors store their ids."""
import json
import os
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "lambdas"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {
    "AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
    "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "SLOT_TABLE_NAME": "slots", "ASSETS_BUCKET": "bucket",
    "JWT_SIGNING_SECRET_PARAMETER": "jwt", "REFRESH_TOKENS_TABLE": "rt", "ACCESS_TOKEN_TTL_PARAMETER": "a",
    "REFRESH_TOKEN_TTL_PARAMETER": "r", "BCRYPT_WORK_FACTOR_PARAMETER": "b",
}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401  (boto3 stand-ins when it is not installed)
import auth_handler  # noqa: E402
import doctor_domain as dd  # noqa: E402
import register  # noqa: E402

SCHEMA = (ROOT / "database" / "schema.sql").read_text(encoding="utf-8")


def table_sql(name):
    match = re.search(r"CREATE TABLE IF NOT EXISTS " + name + r" \((.*?)\n\) ENGINE=InnoDB", SCHEMA, re.S)
    assert match, f"table {name} not found in schema.sql"
    return match.group(1)


def seed_rows(table):
    block = re.search(r"INSERT IGNORE INTO " + table + r" .*?VALUES(.*?);\n", SCHEMA, re.S).group(1)
    return re.findall(r"^\s*\((\d+),\s*'((?:[^']|'')+)'", block, re.M)


class SchemaTests(unittest.TestCase):
    def test_removed_tables_are_gone(self):
        self.assertNotIn("refund_ledger", SCHEMA)
        self.assertNotIn("doctor_profile_assets", SCHEMA)

    def test_sample_data_with_unique_ids_and_names(self):
        for table, expected in (("hospitals", 14), ("specializations", 18)):
            rows = seed_rows(table)
            self.assertEqual(len(rows), expected, table)
            self.assertEqual(len({r[0] for r in rows}), expected, f"{table} ids must be unique")
            self.assertEqual(len({r[1] for r in rows}), expected, f"{table} names must be unique")

    def test_doctors_store_ids_and_photo_key(self):
        sql = table_sql("doctors")
        for column in ("specialization_id BIGINT UNSIGNED NOT NULL", "hospital_id BIGINT UNSIGNED NOT NULL", "photo_key VARCHAR(512) NULL"):
            self.assertIn(column, sql)
        self.assertNotRegex(sql, r"\n\s+specialization VARCHAR")
        self.assertRegex(sql, r"FOREIGN KEY \(specialization_id\)\s+REFERENCES specializations \(specialization_id\)")
        self.assertRegex(sql, r"FOREIGN KEY \(hospital_id\)\s+REFERENCES hospitals \(hospital_id\)")

    def test_lookup_tables_are_created_before_doctors(self):
        order = [SCHEMA.index("CREATE TABLE IF NOT EXISTS " + t + " (") for t in ("hospitals", "specializations", "doctors")]
        self.assertEqual(order, sorted(order))


class RegistrationIdTests(unittest.TestCase):
    base = {
        "role": "DOCTOR", "email": "doc@example.com", "password": "Passw0rd123", "first_name": "Nia", "last_name": "Rao",
        "phone": "9876500000", "date_of_birth": "1985-05-05", "gender": "FEMALE", "address": "12 Main Road",
        "license_number": "ts-54321", "years_of_experience": 7, "clinic_address": "Clinic Street 5",
        "specialization_id": 1, "hospital_id": 2,
    }

    def errors_for(self, **changes):
        body = {**self.base, **changes}
        for key in [k for k, v in body.items() if v is None]:
            del body[key]
        try:
            register.validate_registration(body)
        except register.ValidationError as exc:
            return exc.errors
        return {}

    def test_valid_ids(self):
        self.assertEqual(self.errors_for(), {})
        self.assertEqual(self.errors_for(specialization_id="3", hospital_id="5"), {})

    def test_ids_are_required_for_doctors(self):
        errors = self.errors_for(specialization_id=None, hospital_id=None)
        self.assertEqual(set(errors), {"specialization_id", "hospital_id"})

    def test_rejects_booleans_zero_negative_and_text(self):
        for bad in (True, 0, -3, "abc", 2.5, ""):
            errors = self.errors_for(hospital_id=bad)
            self.assertIn("hospital_id", errors, repr(bad))
            self.assertNotIn("specialization_id", errors)

    def test_patients_need_no_ids(self):
        body = {k: v for k, v in self.base.items() if k not in ("specialization_id", "hospital_id", "license_number", "years_of_experience", "clinic_address")}
        body["role"] = "PATIENT"
        self.assertEqual(register.validate_registration(body)["role"], "PATIENT")


class FakeCursor:
    def __init__(self, exists):
        self.exists, self.queries = exists, []
    def execute(self, sql, args=None): self.queries.append((" ".join(sql.split()), args))
    def fetchone(self): return {"1": 1} if self.exists else None


class LookupIdTests(unittest.TestCase):
    def test_active_id_is_accepted(self):
        cursor = FakeCursor(True)
        self.assertEqual(dd._lookup_id(cursor, "hospitals", "hospital_id", "6", "hospital"), 6)
        self.assertIn("is_active=1", cursor.queries[0][0])

    def test_unknown_or_hidden_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Select a valid hospital"):
            dd._lookup_id(FakeCursor(False), "hospitals", "hospital_id", 99, "hospital")

    def test_current_value_is_always_allowed_without_a_query(self):
        cursor = FakeCursor(False)
        self.assertEqual(dd._lookup_id(cursor, "hospitals", "hospital_id", 6, "hospital", current=6), 6)
        self.assertEqual(cursor.queries, [])

    def test_bad_values_are_rejected(self):
        for bad in (True, None, "x"):
            with self.assertRaises(ValueError):
                dd._lookup_id(FakeCursor(True), "hospitals", "hospital_id", bad, "hospital")


class LookupsEndpointTests(unittest.TestCase):
    class Conn:
        def __init__(self): self.results = [[{"hospital_id": 1, "hospital_name": "H", "city": "C", "state": "S"}],
                                            [{"specialization_id": 2, "specialization_name": "Cardiology"}]]
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, args=None): self.last = sql
        def fetchall(self): return self.results.pop(0)
        def close(self): pass

    def test_public_lookups_route_returns_both_lists(self):
        auth_handler.get_db_connection = lambda: self.Conn()
        result = auth_handler.lambda_handler({"httpMethod": "GET", "resource": "/lookups"}, None)
        body = json.loads(result["body"])
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(body["hospitals"][0]["hospital_id"], 1)
        self.assertEqual(body["specializations"][0]["specialization_name"], "Cardiology")

    def test_database_failure_returns_500(self):
        def boom(): raise RuntimeError("db down")
        auth_handler.get_db_connection = boom
        result = auth_handler.lambda_handler({"httpMethod": "GET", "resource": "/lookups"}, None)
        self.assertEqual(result["statusCode"], 500)


if __name__ == "__main__":
    unittest.main()
