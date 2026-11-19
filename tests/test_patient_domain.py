"""Patient Lambda: profile CRUD, data isolation, search, public profile, slot availability, deletion."""
import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "patient"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {"AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                   "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
                   "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments",
                   "ASSETS_BUCKET": "bucket"}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import patient_domain as pd  # noqa: E402
import slot_store  # noqa: E402
from fakes_booking import FakeConn, FakeDynamo, FakeRds, appointments_table, slot_row  # noqa: E402


class Conn:
    """Scripted connection/cursor: fetchone/fetchall pop queued results, statements are recorded."""
    def __init__(self, ones=(), alls=()):
        self.ones, self.alls, self.sql = list(ones), list(alls), []
        self.committed = self.rolled_back = self.closed = False
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, args=None): self.sql.append((" ".join(sql.split()), args))
    def fetchone(self): return self.ones.pop(0) if self.ones else None
    def fetchall(self): return self.alls.pop(0) if self.alls else []
    def commit(self): self.committed = True
    def rollback(self): self.rolled_back = True
    def close(self): self.closed = True


def event(method, path, role="PATIENT", user_id="7", query=None, body=None):
    return {"httpMethod": method, "path": path, "queryStringParameters": query,
            "body": json.dumps(body) if body is not None else None,
            "requestContext": {"stage": "dev", "authorizer": {"userId": user_id, "role": role}}}


def call(conn, *args, **kwargs):
    with mock.patch.object(pd, "db_connect", return_value=conn):
        result = pd.lambda_handler(event(*args, **kwargs), mock.Mock(aws_request_id="r1"))
    return result["statusCode"], json.loads(result["body"])


PROFILE = {"user_id": 7, "patient_id": 7, "email": "a@b.com", "first_name": "Priya", "last_name": "Nair"}
VALID = {"first_name": "Priya", "last_name": "Nair", "phone": "9876501234", "date_of_birth": "1992-04-05",
         "gender": "FEMALE", "address": "12 Lake View Road", "city": "Hyderabad",
         "emergency_contact_name": "Arun Nair", "emergency_contact_phone": "9876501235"}


class AccessAndIsolationTests(unittest.TestCase):
    def test_missing_identity_is_rejected(self):
        status, _ = call(Conn(), "GET", "/patients/me", user_id=None)
        self.assertEqual(status, 403)

    def test_doctor_cannot_use_patient_profile_route(self):
        conn = Conn()
        status, _ = call(conn, "GET", "/patients/me", role="DOCTOR")
        self.assertEqual(status, 403)
        self.assertEqual(conn.sql, [])  # never reached the database

    def test_identity_comes_from_authorizer_only(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, PROFILE])
        status, _ = call(conn, "GET", "/patients/me", user_id="7", query={"patient_id": "99", "user_id": "99"})
        self.assertEqual(status, 200)
        for _, args in conn.sql:
            self.assertNotIn(99, list(args or ()))
            self.assertNotIn("99", list(args or ()))

    def test_update_ignores_ids_in_body(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, PROFILE, PROFILE])
        status, _ = call(conn, "PUT", "/patients/me", body={"city": "Chennai", "user_id": 99, "patient_id": 99})
        self.assertEqual(status, 200)
        update = [s for s in conn.sql if s[0].startswith("UPDATE patients")][0]
        self.assertEqual(update[1], ["Chennai", 7])
        self.assertTrue(update[0].endswith("WHERE user_id = %s"))
        self.assertTrue(conn.committed)

    def test_inactive_account_is_blocked(self):
        status, _ = call(Conn(ones=[{"user_id": 7, "account_status": "INACTIVE"}]), "GET", "/patients/me")
        self.assertEqual(status, 403)

    def test_only_get_is_allowed_on_directory(self):
        status, _ = call(Conn(), "DELETE", "/doctors/5")
        self.assertEqual(status, 405)


class ProfileCrudTests(unittest.TestCase):
    def test_create_requires_all_fields(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, None])
        status, body = call(conn, "POST", "/patients/me", body={"first_name": "Priya"})
        self.assertEqual(status, 400)
        for field in ("last_name", "phone", "city", "emergency_contact_name"):
            self.assertIn(field, body["errors"])

    def test_create_success(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, None, PROFILE])
        status, body = call(conn, "POST", "/patients/me", body=VALID)
        self.assertEqual(status, 201)
        insert = [s for s in conn.sql if s[0].startswith("INSERT INTO patients")][0]
        self.assertEqual(insert[1][0], 7)

    def test_create_twice_conflicts(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, PROFILE])
        status, _ = call(conn, "POST", "/patients/me", body=VALID)
        self.assertEqual(status, 409)

    def test_get_missing_profile_is_404(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, None])
        status, _ = call(conn, "GET", "/patients/me")
        self.assertEqual(status, 404)

    def test_validation_messages(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, PROFILE])
        status, body = call(conn, "PUT", "/patients/me", body={"emergency_contact_phone": "12", "gender": "x",
                                                                "date_of_birth": "2999-01-01"})
        self.assertEqual(status, 400)
        self.assertIn("8-15 digits", body["errors"]["emergency_contact_phone"])
        self.assertIn("gender", body["errors"])
        self.assertIn("date_of_birth", body["errors"])

    def test_empty_update_rejected(self):
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}, PROFILE])
        status, _ = call(conn, "PUT", "/patients/me", body={"user_id": 1})
        self.assertEqual(status, 400)


class DeleteTests(unittest.TestCase):
    def appointment(self, aid, status, hours):
        start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=hours)
        return {"appointment_id": str(aid), "patient_id": "7", "doctor_id": "3", "slot_id": f"3#1#{aid}",
                "status": status, "start_time_utc": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time_utc": (start + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")}

    def run_delete(self, *appointments):
        table = appointments_table()
        for item in appointments:
            table.items[item["appointment_id"]] = item
        conn = Conn(ones=[{"user_id": 7, "account_status": "ACTIVE"}])
        with mock.patch.object(pd, "ddb", FakeDynamo(appointments=table)), \
             mock.patch.object(pd, "_revoke_refresh_tokens", return_value=2) as revoke:
            status, body = call(conn, "DELETE", "/patients/me")
        return status, conn, revoke

    def test_blocked_with_upcoming_appointments(self):
        status, conn, revoke = self.run_delete(self.appointment(1, "CONFIRMED", 48))
        self.assertEqual(status, 409)
        self.assertFalse(conn.committed)
        revoke.assert_not_called()

    def test_hard_delete_without_history(self):
        status, conn, revoke = self.run_delete()
        self.assertEqual(status, 200)
        self.assertTrue(any(s[0].startswith("DELETE FROM patients") for s in conn.sql))
        revoke.assert_called_once_with(7)

    def test_anonymise_with_history(self):
        status, conn, _ = self.run_delete(self.appointment(1, "COMPLETED", -48),
                                          self.appointment(2, "CANCELLED_BY_PATIENT", 24))
        self.assertEqual(status, 200)
        self.assertFalse(any(s[0].startswith("DELETE FROM") for s in conn.sql))
        self.assertTrue(any("account_status = 'INACTIVE'" in s[0] for s in conn.sql))


class SearchTests(unittest.TestCase):
    def search(self, query, total=0, rows=()):
        conn = Conn(ones=[{"n": total}], alls=[list(rows)])
        with mock.patch.object(pd, "_photo_url", return_value=None):
            result = pd._search_doctors(conn, query)
        return result, conn.sql

    def test_defaults_and_pagination_math(self):
        result, sql = self.search({}, total=23)
        self.assertEqual(result["pagination"], {"page": 1, "page_size": 10, "total": 23, "total_pages": 3})
        self.assertEqual(sql[1][1][-2:], [10, 0])

    def test_page_offset_and_size_limit(self):
        _, sql = self.search({"page": "3", "page_size": "50"})
        self.assertEqual(sql[1][1][-2:], [50, 100])
        with self.assertRaises(ValueError):  # oversize pages are rejected (HTTP 400), never silently huge
            self.search({"page_size": "500"})

    def test_filters_use_bound_parameters_and_escape_wildcards(self):
        _, sql = self.search({"name": "Dr. Asha 100%", "hospital": "kims", "specialisation": "cardio", "city": "hyd_"})
        statement, args = sql[0]
        self.assertNotIn("kims", statement)
        self.assertIn("ESCAPE '!'", statement)
        self.assertIn("Asha%", args)
        self.assertIn("100!%%", args)
        self.assertIn("%kims%", args)
        self.assertIn("hyd!_%", args)
        self.assertNotIn("Dr.%", args)

    def test_only_active_doctors_and_stable_ordering(self):
        _, sql = self.search({})
        self.assertIn("account_status = 'ACTIVE'", sql[0][0])
        self.assertIn("ORDER BY d.last_name, d.first_name, d.user_id", sql[1][0])

    def test_invalid_page_is_rejected(self):
        with self.assertRaises(ValueError):
            self.search({"page": "0"})

    def test_rows_get_numeric_fee(self):
        from decimal import Decimal
        result, _ = self.search({}, total=1, rows=[{"doctor_id": 1, "starting_fee": Decimal("300.00"), "photo_key": None}])
        self.assertEqual(result["doctors"][0]["starting_fee"], 300.0)


class PublicProfileTests(unittest.TestCase):
    def test_unknown_doctor_is_404(self):
        status, _ = call(Conn(ones=[None]), "GET", "/doctors/5")
        self.assertEqual(status, 404)

    def test_profile_has_no_private_fields(self):
        row = {"doctor_id": 5, "first_name": "Asha", "last_name": "Rao", "photo_key": None,
               "clinic_address": "Clinic Road", "years_of_experience": 5}
        types = [{"consultation_type_id": 1, "consultation_name": "Standard", "duration_minutes": 30,
                  "fee": 500, "slot_capacity": 1}]
        conn = Conn(ones=[row], alls=[[dict(t) for t in types]])
        with mock.patch.object(pd, "_photo_url", return_value=None):
            status, body = call(conn, "GET", "/doctors/5")
        self.assertEqual(status, 200)
        doctor = body["doctor"]
        for private in ("email", "phone", "date_of_birth", "license_number", "address", "password_hash"):
            self.assertNotIn(private, doctor)
        self.assertEqual(doctor["consultation_types"][0]["fee"], 500.0)
        self.assertNotIn("email", conn.sql[0][0])
        self.assertNotIn("license", conn.sql[0][0])


class SlotAvailabilityTests(unittest.TestCase):
    def slots(self, items, query=None):
        rds = FakeRds(slots=items)
        return pd._doctor_slots(FakeConn(rds), 7, query or {})

    def row(self, slot_id="s", hours=30, **kw):
        kw.setdefault("capacity", 4)
        kw.setdefault("booked", 1)
        kw.setdefault("ctype", 1)
        return slot_row(slot_id, doctor_id=7, hours_ahead=hours, **kw)

    def test_free_seats_and_current_fee(self):
        slot = self.slots([self.row()])["slots"][0]
        self.assertEqual((slot["capacity"], slot["booked_count"], slot["free_seats"]), (4, 1, 3))
        self.assertEqual(slot["status"], "AVAILABLE")
        self.assertEqual(float(slot["fee"]), 500.0)

    def test_full_slot_listed_with_zero_free(self):
        slot = self.slots([self.row(booked=4)])["slots"][0]
        self.assertEqual((slot["free_seats"], slot["status"]), (0, "FULL"))

    def test_cancelled_removed_blackout_hidden(self):
        result = self.slots([self.row("a", status="CANCELLED"), self.row("b", status="REMOVED"),
                             self.row("c", status="BLACKED_OUT"), self.row("d")])
        self.assertEqual([s["slot_id"] for s in result["slots"]], ["d"])

    def test_past_slots_hidden(self):
        past = self.row("p", hours=-2)
        result = self.slots([past], {"from": datetime.now(timezone.utc).date().isoformat()})
        self.assertEqual(result["count"], 0)

    def test_consultation_type_filter(self):
        result = self.slots([self.row("a", ctype=1), self.row("b", ctype=2)], {"consultationTypeId": "2"})
        self.assertEqual([s["slot_id"] for s in result["slots"]], ["b"])

    def test_range_validation(self):
        today = datetime.now(timezone.utc).date()
        with self.assertRaises(ValueError):
            self.slots([], {"from": (today + timedelta(days=9)).isoformat(), "to": today.isoformat()})
        with self.assertRaises(ValueError):
            self.slots([], {"from": today.isoformat(), "to": (today + timedelta(days=90)).isoformat()})
        with self.assertRaises(ValueError):
            self.slots([], {"from": "not-a-date"})

    def test_default_range_is_seven_days(self):
        result = self.slots([])
        first = datetime.fromisoformat(result["from"]); last = datetime.fromisoformat(result["to"])
        self.assertEqual((last - first).days, 6)

    def test_dates_before_today_are_refused(self):
        today = datetime.now(timezone.utc).date()
        with self.assertRaisesRegex(ValueError, "Past dates"):
            self.slots([], {"from": (today - timedelta(days=2)).isoformat()})
        for ok in (today, today - timedelta(days=1)):   # yesterday UTC may still be today for the client
            self.slots([], {"from": ok.isoformat()})

    def test_patient_uses_the_same_slot_function_as_the_doctor(self):
        with mock.patch.object(slot_store, "get_slots", return_value=[]) as get_slots:
            self.slots([])
        self.assertEqual(get_slots.call_args.args[4], slot_store.VIEWER_PATIENT)


if __name__ == "__main__":
    unittest.main()
