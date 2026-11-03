"""Consultation types are split into a 4-row catalog plus a per-doctor table."""
import json
import os
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "lambdas"))
for key, value in {
    "AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
    "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
    "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "SLOT_TABLE_NAME": "slots", "ASSETS_BUCKET": "bucket",
}.items():
    os.environ.setdefault(key, value)

import doctor_domain as dd  # noqa: E402

SCHEMA = (ROOT / "database" / "schema.sql").read_text(encoding="utf-8")


def table_sql(name):
    match = re.search(r"CREATE TABLE IF NOT EXISTS " + name + r" \((.*?)\) ENGINE=InnoDB", SCHEMA, re.S)
    assert match, f"table {name} not found in schema.sql"
    return match.group(1)


class SchemaTests(unittest.TestCase):
    def test_catalog_holds_exactly_the_four_types_with_fixed_ids(self):
        seed = re.search(r"INSERT IGNORE INTO consultation_types .*?VALUES(.*?);", SCHEMA, re.S).group(1)
        rows = re.findall(r"\((\d+),\s*'([^']+)',\s*(\d+),\s*(\d+),\s*(\d+)\)", seed)
        self.assertEqual(
            [(int(i), n, int(d), int(lo), int(hi)) for i, n, d, lo, hi in rows],
            [(1, "Quick Consult", 15, 1, 1), (2, "Standard Consult", 30, 1, 1),
             (3, "Detailed Consult", 60, 1, 1), (4, "Health Camp Session", 60, 2, 10)])

    def test_catalog_has_no_per_doctor_columns(self):
        sql = table_sql("consultation_types")
        for column in ("doctor_id", "fee", "slot_capacity", "is_active"):
            self.assertNotIn(column, sql)

    def test_per_doctor_table_holds_doctor_columns(self):
        sql = table_sql("doctor_consultation_types")
        for column in ("doctor_id", "consultation_type_id", "fee", "slot_capacity", "is_active"):
            self.assertIn(column, sql)
        self.assertIn("PRIMARY KEY (doctor_id, consultation_type_id)", sql)

    def test_schedules_and_bookings_must_use_a_type_the_doctor_offers(self):
        for table in ("availability_patterns", "appointments"):
            sql = table_sql(table)
            self.assertRegex(sql, r"FOREIGN KEY \(doctor_id, consultation_type_id\)\s+REFERENCES doctor_consultation_types")

    def test_tables_are_created_in_dependency_order(self):
        order = [SCHEMA.index("CREATE TABLE IF NOT EXISTS " + t + " (") for t in
                 ("consultation_types", "doctor_consultation_types", "availability_patterns", "appointments")]
        self.assertEqual(order, sorted(order))


class CapacityTests(unittest.TestCase):
    fixed = {"min_capacity": 1, "max_capacity": 1}
    camp = {"min_capacity": 2, "max_capacity": 10}

    def test_fixed_capacity(self):
        dd._validate_capacity("Quick Consult", self.fixed, 1)
        with self.assertRaisesRegex(ValueError, "slot capacity must be 1"):
            dd._validate_capacity("Quick Consult", self.fixed, 2)

    def test_range_capacity(self):
        for ok in (2, 5, 10):
            dd._validate_capacity("Health Camp Session", self.camp, ok)
        for bad in (1, 11):
            with self.assertRaisesRegex(ValueError, "between 2 and 10"):
                dd._validate_capacity("Health Camp Session", self.camp, bad)


class FakeConn:
    """Scripted DB connection: returns queued rows and records every statement."""
    def __init__(self, rows):
        self.rows, self.sql, self.committed = list(rows), [], False
        self.rowcount, self.lastrowid = 1, 0
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, args=None): self.sql.append((" ".join(sql.split()), args))
    def fetchone(self): return self.rows.pop(0) if self.rows else None
    def fetchall(self): return []
    def commit(self): self.committed = True
    def rollback(self): pass
    def close(self): pass


class HandlerTests(unittest.TestCase):
    def call(self, conn, method, path, body):
        dd.db_connect = lambda: conn
        dd._refresh_slots = lambda doctor_id: {"refreshed": True}
        event = {"httpMethod": method, "path": path, "resource": path, "body": json.dumps(body),
                 "requestContext": {"authorizer": {"userId": "7", "role": "DOCTOR"}}}
        result = dd.lambda_handler(event, None)
        return result["statusCode"], json.loads(result["body"])

    quick = {"consultation_type_id": 1, "consultation_name": "Quick Consult", "duration_minutes": 15,
             "min_capacity": 1, "max_capacity": 1}

    def test_create_inserts_into_the_per_doctor_table_using_the_catalog_id(self):
        conn = FakeConn([{"doctor_id": 5}, self.quick, None])
        status, body = self.call(conn, "POST", "/doctors/me/consultation-types",
                                 {"consultation_name": "Quick Consult", "duration_minutes": 15, "fee": 500, "slot_capacity": 1})
        self.assertEqual((status, body["consultation_type_id"]), (201, 1))
        sql, args = conn.sql[-1]
        self.assertTrue(sql.startswith("INSERT INTO doctor_consultation_types"))
        self.assertEqual(args, (5, 1, 500.0, 1))
        self.assertTrue(conn.committed)

    def test_unknown_consultation_name_is_rejected(self):
        conn = FakeConn([{"doctor_id": 5}, None])
        status, body = self.call(conn, "POST", "/doctors/me/consultation-types",
                                 {"consultation_name": "Surgery", "fee": 1, "slot_capacity": 1})
        self.assertEqual(status, 400)
        self.assertIn("four approved", body["message"])

    def test_active_duplicate_is_rejected(self):
        conn = FakeConn([{"doctor_id": 5}, self.quick, {"is_active": 1}])
        status, body = self.call(conn, "POST", "/doctors/me/consultation-types",
                                 {"consultation_name": "Quick Consult", "fee": 500, "slot_capacity": 1})
        self.assertEqual(status, 400)
        self.assertIn("already exists", body["message"])

    def test_inactive_type_is_reactivated_not_duplicated(self):
        conn = FakeConn([{"doctor_id": 5}, self.quick, {"is_active": 0}])
        status, body = self.call(conn, "POST", "/doctors/me/consultation-types",
                                 {"consultation_name": "Quick Consult", "fee": 650, "slot_capacity": 1})
        self.assertEqual((status, body["consultation_type_id"]), (200, 1))
        self.assertTrue(conn.sql[-1][0].startswith("UPDATE doctor_consultation_types SET fee=%s,slot_capacity=%s,is_active=1"))

    def test_the_type_itself_cannot_be_changed_by_an_edit(self):
        current = {"consultation_name": "Quick Consult", "duration_minutes": 15, "min_capacity": 1, "max_capacity": 1, "slot_capacity": 1}
        conn = FakeConn([{"doctor_id": 5}, current])
        status, body = self.call(conn, "PUT", "/doctors/me/consultation-types/1", {"consultation_name": "Standard Consult"})
        self.assertEqual(status, 400)
        self.assertIn("cannot be changed", body["message"])

    def test_edit_only_updates_per_doctor_columns(self):
        current = {"consultation_name": "Quick Consult", "duration_minutes": 15, "min_capacity": 1, "max_capacity": 1, "slot_capacity": 1}
        conn = FakeConn([{"doctor_id": 5}, current])
        status, _ = self.call(conn, "PUT", "/doctors/me/consultation-types/1",
                              {"consultation_name": "Quick Consult", "duration_minutes": 15, "fee": 600, "slot_capacity": 1, "is_active": 1})
        self.assertEqual(status, 200)
        sql, args = conn.sql[-1]
        self.assertEqual(sql, "UPDATE doctor_consultation_types SET fee=%s,slot_capacity=%s,is_active=%s WHERE consultation_type_id=%s AND doctor_id=%s")
        self.assertEqual(args, [600.0, 1, 1, 1, 5])


if __name__ == "__main__":
    unittest.main()
