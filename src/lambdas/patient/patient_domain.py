"""MediSlot patient-domain API (Milestone 4).

Every route is reached through API Gateway; the JWT authorizer injects the
caller's userId and role into the request context. The caller's identity is ALWAYS
taken from that context, never from the URL, query string or body. That is what
keeps one patient from reading or changing another patient's data.

Routes
  /patients/me                       GET POST PUT PATCH DELETE   PATIENT only
  /doctors                           GET   doctor search          signed-in PATIENT or DOCTOR
  /doctors/{doctorId}                GET   public doctor profile  signed-in PATIENT or DOCTOR
  /doctors/{doctorId}/slots          GET   slot availability      signed-in PATIENT or DOCTOR

(The doctor's own /doctors/me/... routes belong to the doctor-domain Lambda.)
Booking, cancellation, waitlists and refunds arrive in the next milestone.
"""
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from time import monotonic as _monotonic

import boto3
import pymysql
from boto3.dynamodb.conditions import Key
from botocore.config import Config

from common import db_connect, json_body, log_event, logger, response

ddb = boto3.resource("dynamodb")
s3 = boto3.client("s3", config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))

SLOT_TABLE_NAME = os.environ["SLOT_TABLE_NAME"]
ASSETS_BUCKET = os.environ["ASSETS_BUCKET"]
REFRESH_TOKENS_TABLE = os.environ.get("REFRESH_TOKENS_TABLE", "")

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 50
MAX_SLOT_RANGE_DAYS = 31
DEFAULT_SLOT_RANGE_DAYS = 7
BOOKABLE_SLOT_STATUSES = {"AVAILABLE", "FULL"}  # others: CANCELLED, REMOVED, BLACKED_OUT

GENDERS = {"FEMALE", "MALE", "OTHER", "PREFER_NOT_TO_SAY"}
EDITABLE_FIELDS = ("first_name", "last_name", "phone", "date_of_birth", "gender", "address",
                   "city", "emergency_contact_name", "emergency_contact_phone")
NAME_RE = re.compile(r"[^\W\d_]+(?:[ '’-][^\W\d_]+)*")
CITY_RE = re.compile(r"[^\W\d_]+(?:[ '’.-]+[^\W\d_]+)*\.?")
PHONE_RE = re.compile(r"\+?[1-9]\d{7,14}")
PHONE_MESSAGE = "must contain 8-15 digits and may start with a country-code +."


class FieldErrors(Exception):
    """One or more request fields are invalid; maps to HTTP 400 with per-field messages."""

    def __init__(self, errors):
        super().__init__("Some fields are invalid")
        self.errors = errors


# ---------------------------------------------------------------------------
# Profile validation
# ---------------------------------------------------------------------------

def _clean_profile(body, require_all):
    """Validate the editable patient fields present in body; return the cleaned values.

    With require_all every editable field must be present (profile creation).
    Unknown keys, including any user_id or patient_id the client sends, are ignored.
    """
    errors, out = {}, {}
    today = datetime.now(timezone.utc).date()

    for field in EDITABLE_FIELDS:
        if field not in body:
            if require_all:
                errors[field] = f"{field} is required."
            continue
        value = body[field]
        if not isinstance(value, str) or not value.strip():
            errors[field] = f"{field} must be a non-empty string."
            continue
        value = value.strip()

        if field in ("first_name", "last_name"):
            if not 1 <= len(value) <= 50 or not NAME_RE.fullmatch(value):
                errors[field] = f"{field} must be 1-50 letters (spaces, apostrophes and hyphens allowed)."
                continue
        elif field in ("phone", "emergency_contact_phone"):
            value = re.sub(r"[\s().-]", "", value)
            if not PHONE_RE.fullmatch(value):
                errors[field] = f"{field} {PHONE_MESSAGE}"
                continue
        elif field == "date_of_birth":
            try:
                parsed = date.fromisoformat(value)
            except ValueError:
                parsed = None
            if parsed is None or parsed.isoformat() != value:
                errors[field] = "date_of_birth must be a valid date in YYYY-MM-DD format."
                continue
            if parsed >= today or parsed.year < 1900:
                errors[field] = "date_of_birth must be a past date after 1899."
                continue
        elif field == "gender":
            value = value.upper()
            if value not in GENDERS:
                errors[field] = "gender must be one of " + ", ".join(sorted(GENDERS)) + "."
                continue
        elif field == "address":
            if not 5 <= len(value) <= 500:
                errors[field] = "address must be between 5 and 500 characters."
                continue
        elif field == "city":
            if not 2 <= len(value) <= 100 or not CITY_RE.fullmatch(value):
                errors[field] = "city must be 2-100 letters (spaces, dots, apostrophes and hyphens allowed)."
                continue
        elif field == "emergency_contact_name":
            if not 2 <= len(value) <= 100:
                errors[field] = "emergency_contact_name must be between 2 and 100 characters."
                continue
        out[field] = value

    if errors:
        raise FieldErrors(errors)
    return out


# ---------------------------------------------------------------------------
# Patient profile
# ---------------------------------------------------------------------------

PROFILE_SQL = """SELECT p.user_id, p.user_id AS patient_id, u.email, p.first_name, p.last_name, p.phone,
    p.date_of_birth, p.gender, p.address, p.city, p.emergency_contact_name,
    p.emergency_contact_phone, p.created_at, p.updated_at
    FROM patients p JOIN users u ON u.user_id = p.user_id
    WHERE p.user_id = %s AND u.role = 'PATIENT'"""


def _account(cursor, user_id):
    """The caller's account row (role PATIENT) or None. Looked up by the authorizer's user id only."""
    cursor.execute("SELECT user_id, account_status FROM users WHERE user_id = %s AND role = 'PATIENT'",
                   (user_id,))
    return cursor.fetchone()


def _profile(cursor, patient_id):
    cursor.execute(PROFILE_SQL, (patient_id,))
    return cursor.fetchone()


def _revoke_refresh_tokens(patient_id):
    """Best effort: delete the account's refresh tokens so it cannot mint new access tokens."""
    if not REFRESH_TOKENS_TABLE:
        return 0
    removed = 0
    try:
        table = ddb.Table(REFRESH_TOKENS_TABLE)
        kwargs = {"KeyConditionExpression": Key("user_id").eq(str(patient_id))}
        while True:
            page = table.query(**kwargs)
            for item in page.get("Items", []):
                table.delete_item(Key={"user_id": item["user_id"], "token_id": item["token_id"]})
                removed += 1
            if not page.get("LastEvaluatedKey"):
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    except Exception:
        logger.exception("Refresh token revocation failed for patient_id=%s", patient_id)
    return removed


def _delete_account(conn, cursor, patient_id):
    """Delete the caller's own account.

    - Upcoming booked appointments block deletion (cancel them first).
    - With no appointment history the profile and account rows are removed.
    - With history (the doctor's records and refunds refer to it) the personal data is
      scrubbed and the account is deactivated instead, so history stays consistent.
    """
    cursor.execute("""SELECT COUNT(*) AS n FROM appointments WHERE patient_id = %s
        AND status IN ('BOOKED','CONFIRMED') AND scheduled_end_utc > UTC_TIMESTAMP(6)""", (patient_id,))
    if int((cursor.fetchone() or {}).get("n") or 0):
        return response(409, {"message": "You have upcoming appointments. Cancel them before deleting your account."})
    cursor.execute("SELECT COUNT(*) AS n FROM appointments WHERE patient_id = %s", (patient_id,))
    has_history = int((cursor.fetchone() or {}).get("n") or 0) > 0
    if has_history:
        cursor.execute("""UPDATE patients SET first_name='Deleted', last_name='Patient', phone='0000000000',
            date_of_birth='1900-01-01', gender='PREFER_NOT_TO_SAY', address='Removed', city=NULL,
            emergency_contact_name=NULL, emergency_contact_phone=NULL WHERE user_id = %s""", (patient_id,))
        cursor.execute("""UPDATE users SET email = %s, password_hash = '!', account_status = 'INACTIVE'
            WHERE user_id = %s AND role = 'PATIENT'""", (f"deleted-{patient_id}@deleted.invalid", patient_id))
    else:
        cursor.execute("DELETE FROM patients WHERE user_id = %s", (patient_id,))
        cursor.execute("DELETE FROM users WHERE user_id = %s AND role = 'PATIENT'", (patient_id,))
    conn.commit()
    revoked = _revoke_refresh_tokens(patient_id)
    log_event("patient_account_deleted", patient_id=patient_id, anonymised=has_history, tokens_revoked=revoked)
    return response(200, {"message": "Your account has been deleted."})


def _handle_patient_me(conn, cursor, method, user_id, body):
    account = _account(cursor, user_id)
    if not account:
        return response(403, {"message": "A patient account is required"})
    if account["account_status"] != "ACTIVE":
        return response(403, {"message": "This account is not active"})
    patient_id = int(account["user_id"])

    if method == "GET":
        profile = _profile(cursor, patient_id)
        if not profile:
            return response(404, {"message": "Patient profile not found. Create it using POST /patients/me."})
        return response(200, {"profile": profile})

    if method == "POST":
        if _profile(cursor, patient_id):
            return response(409, {"message": "Patient profile already exists; use PUT to update it"})
        values = _clean_profile(body, require_all=True)
        columns = ["user_id"] + list(EDITABLE_FIELDS)
        cursor.execute(
            f"INSERT INTO patients ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})",
            [patient_id] + [values[f] for f in EDITABLE_FIELDS])
        conn.commit()
        log_event("patient_profile_created", patient_id=patient_id)
        return response(201, {"message": "Patient profile created", "profile": _profile(cursor, patient_id)})

    if method in ("PUT", "PATCH"):
        if not _profile(cursor, patient_id):
            return response(404, {"message": "Patient profile not found. Create it using POST /patients/me."})
        updates = _clean_profile(body, require_all=False)
        if not updates:
            return response(400, {"message": "Provide at least one editable profile field"})
        cursor.execute("UPDATE patients SET " + ",".join(f"{k} = %s" for k in updates) + " WHERE user_id = %s",
                       list(updates.values()) + [patient_id])
        conn.commit()
        log_event("patient_profile_updated", patient_id=patient_id, fields=sorted(updates))
        return response(200, {"message": "Patient profile updated", "profile": _profile(cursor, patient_id)})

    if method == "DELETE":
        return _delete_account(conn, cursor, patient_id)

    return response(405, {"message": "Method not allowed"})


# ---------------------------------------------------------------------------
# Doctor search, public profile and slot availability
# ---------------------------------------------------------------------------

def _like_escape(text):
    """Escape LIKE wildcards; queries use ESCAPE '!' so the pattern is portable and injection safe."""
    return text.replace("!", "!!").replace("%", "!%").replace("_", "!_")


def _text_filter(query, *names):
    for name in names:
        value = query.get(name)
        if value is not None and str(value).strip():
            value = str(value).strip()
            if len(value) > 100:
                raise ValueError(f"{names[0]} must be at most 100 characters")
            return value
    return None


def _whole_number(query, name, default, low, high):
    raw = query.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        number = int(str(raw).strip())
    except ValueError:
        raise ValueError(f"{name} must be a whole number")
    if not low <= number <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return number


def _name_tokens(text):
    tokens = text.split()
    if tokens and tokens[0].lower().rstrip(".") == "dr":
        tokens = tokens[1:]
    return tokens[:4]


def _photo_url(photo_key):
    if not photo_key:
        return None
    return s3.generate_presigned_url("get_object", Params={"Bucket": ASSETS_BUCKET, "Key": photo_key},
                                     ExpiresIn=300)


def _search_doctors(cursor, query):
    """Doctor search with name / hospital / specialisation / city filters and pagination.

    Index use (details and reasoning in docs/patient-domain.md):
      name           prefix LIKE on first_name (idx_doctors_first_name) or last_name (idx_doctors_name);
                     each word of the search must match the start of one of the two names
      specialisation specializations is a tiny reference table; matching ids then use idx_doctors_spec_hospital
      hospital       hospitals is a tiny reference table; matching ids then use idx_doctors_hospital
      city           prefix LIKE on hospitals.city (idx_hospitals_city), then idx_doctors_hospital
      ordering       last_name, first_name, user_id follows idx_doctors_name, so no filesort
    """
    page = _whole_number(query, "page", 1, 1, 100000)
    page_size = _whole_number(query, "page_size", _whole_number(query, "pageSize", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE), 1, MAX_PAGE_SIZE)

    where, args = ["u.account_status = 'ACTIVE'"], []

    name = _text_filter(query, "name")
    if name:
        for token in _name_tokens(name):
            pattern = _like_escape(token) + "%"
            where.append("(d.first_name LIKE %s ESCAPE '!' OR d.last_name LIKE %s ESCAPE '!')")
            args += [pattern, pattern]

    hospital = _text_filter(query, "hospital")
    if hospital:
        where.append("h.hospital_name LIKE %s ESCAPE '!'")
        args.append("%" + _like_escape(hospital) + "%")

    specialisation = _text_filter(query, "specialisation", "specialization")
    if specialisation:
        where.append("s.specialization_name LIKE %s ESCAPE '!'")
        args.append("%" + _like_escape(specialisation) + "%")

    city = _text_filter(query, "city")
    if city:
        where.append("h.city LIKE %s ESCAPE '!'")
        args.append(_like_escape(city) + "%")

    source = """FROM doctors d
        JOIN users u ON u.user_id = d.user_id
        JOIN hospitals h ON h.hospital_id = d.hospital_id
        JOIN specializations s ON s.specialization_id = d.specialization_id
        WHERE """ + " AND ".join(where)

    cursor.execute("SELECT COUNT(*) AS n " + source, args)
    total = int((cursor.fetchone() or {}).get("n") or 0)

    cursor.execute("""SELECT d.user_id AS doctor_id, d.first_name, d.last_name,
        s.specialization_id, s.specialization_name AS specialization,
        h.hospital_id, h.hospital_name, h.city, h.state, d.years_of_experience, d.photo_key,
        (SELECT MIN(t.fee) FROM doctor_consultation_types t
            WHERE t.doctor_id = d.user_id AND t.is_active = 1) AS starting_fee
        """ + source + """
        ORDER BY d.last_name, d.first_name, d.user_id
        LIMIT %s OFFSET %s""", args + [page_size, (page - 1) * page_size])
    doctors = []
    for row in cursor.fetchall():
        row["photo_url"] = _photo_url(row.pop("photo_key", None))
        if row.get("starting_fee") is not None:
            row["starting_fee"] = float(row["starting_fee"])
        doctors.append(row)

    return {"doctors": doctors,
            "pagination": {"page": page, "page_size": page_size, "total": total,
                           "total_pages": (total + page_size - 1) // page_size}}


def _active_doctor(cursor, doctor_id):
    cursor.execute("""SELECT d.user_id FROM doctors d JOIN users u ON u.user_id = d.user_id
        WHERE d.user_id = %s AND u.account_status = 'ACTIVE'""", (doctor_id,))
    return cursor.fetchone()


def _consultation_types(cursor, doctor_id):
    cursor.execute("""SELECT t.consultation_type_id, c.consultation_name, c.duration_minutes,
        t.fee, t.slot_capacity
        FROM doctor_consultation_types t
        JOIN consultation_types c ON c.consultation_type_id = t.consultation_type_id
        WHERE t.doctor_id = %s AND t.is_active = 1
        ORDER BY c.duration_minutes, c.consultation_type_id""", (doctor_id,))
    rows = cursor.fetchall()
    for row in rows:
        row["fee"] = float(row["fee"])
    return rows


def _doctor_profile(cursor, doctor_id):
    """Public doctor profile. Deliberately leaves out email, phone, date of birth,
    residential address and license number."""
    cursor.execute("""SELECT d.user_id AS doctor_id, d.first_name, d.last_name,
        s.specialization_id, s.specialization_name AS specialization,
        h.hospital_id, h.hospital_name, h.city, h.state,
        d.years_of_experience, d.clinic_address, d.photo_key
        FROM doctors d
        JOIN users u ON u.user_id = d.user_id
        JOIN hospitals h ON h.hospital_id = d.hospital_id
        JOIN specializations s ON s.specialization_id = d.specialization_id
        WHERE d.user_id = %s AND u.account_status = 'ACTIVE'""", (doctor_id,))
    row = cursor.fetchone()
    if not row:
        return None
    row["photo_url"] = _photo_url(row.pop("photo_key", None))
    row["consultation_types"] = _consultation_types(cursor, doctor_id)
    return row


def _parse_day(value, name):
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ValueError(f"{name} must be a date in YYYY-MM-DD format")


def _as_utc(text):
    parsed = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _query_slot_range(table, doctor_id, first_day, last_day):
    """Every slot record of one doctor for a local-date range (follows pagination)."""
    items = []
    kwargs = {"IndexName": "DoctorDateIndex",
              "KeyConditionExpression": Key("doctor_id").eq(str(doctor_id))
              & Key("slot_date").between(first_day.isoformat(), last_day.isoformat())}
    while True:
        page = table.query(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _doctor_slots(cursor, doctor_id, query):
    """Free seats per slot for a date range. Slot dates are the doctor's local calendar dates."""
    today = datetime.now(timezone.utc).date()
    first_day = _parse_day(query["from"], "from") if query.get("from") else today
    last_day = _parse_day(query["to"], "to") if query.get("to") else first_day + timedelta(days=DEFAULT_SLOT_RANGE_DAYS - 1)
    if last_day < first_day:
        raise ValueError("to must not be earlier than from")
    if (last_day - first_day).days + 1 > MAX_SLOT_RANGE_DAYS:
        raise ValueError(f"The date range can cover at most {MAX_SLOT_RANGE_DAYS} days")

    type_filter = None
    raw_type = query.get("consultationTypeId") or query.get("consultation_type_id")
    if raw_type not in (None, ""):
        try:
            type_filter = int(str(raw_type).strip())
        except ValueError:
            raise ValueError("consultationTypeId must be a whole number")

    # Current fee per consultation type comes from MySQL (the slot only holds the fee at creation time).
    fees = {int(t["consultation_type_id"]): t["fee"] for t in _consultation_types(cursor, doctor_id)}

    now = datetime.now(timezone.utc)
    slots = []
    for item in _query_slot_range(ddb.Table(SLOT_TABLE_NAME), doctor_id, first_day, last_day):
        if item.get("status") not in BOOKABLE_SLOT_STATUSES:
            continue
        type_id = int(item.get("consultation_type_id", 0))
        if type_filter is not None and type_id != type_filter:
            continue
        start_text = item.get("start_time_utc") or item.get("start_at_utc")
        end_text = item.get("end_time_utc") or item.get("end_at_utc")
        if not start_text or _as_utc(start_text) <= now:
            continue  # already started: not bookable
        capacity, booked = int(item.get("capacity", 0)), int(item.get("booked_count", 0))
        free = max(capacity - booked, 0)
        slots.append({
            "slot_id": item["slot_id"],
            "slot_date": item.get("slot_date"),
            "start_time_utc": start_text,
            "end_time_utc": end_text,
            "local_start": item.get("local_start"),
            "timezone": item.get("timezone") or item.get("timezone_name"),
            "consultation_type_id": type_id,
            "consultation_name": item.get("consultation_name"),
            "duration_minutes": int(item.get("duration_minutes", 0)),
            "fee": fees.get(type_id, float(item.get("fee", 0) or 0)),
            "capacity": capacity,
            "booked_count": booked,
            "free_seats": free,
            "status": "AVAILABLE" if free > 0 else "FULL",
        })
    slots.sort(key=lambda s: (s["start_time_utc"], s["consultation_type_id"]))
    return {"doctor_id": int(doctor_id), "from": first_day.isoformat(), "to": last_day.isoformat(),
            "consultation_type_id": type_filter, "count": len(slots), "slots": slots}


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def _clean_path(event):
    path = event.get("path") or event.get("resource") or ""
    path = "/" + path.strip("/")
    stage = (event.get("requestContext") or {}).get("stage")
    if stage and path.startswith("/" + stage + "/"):
        path = path[len(stage) + 1:]
    return path


def _handle_api(event, context):
    method = event.get("httpMethod", "GET").upper()
    if method == "OPTIONS":
        return response(204, {})
    auth = (event.get("requestContext") or {}).get("authorizer") or {}
    user_id, role = auth.get("userId"), auth.get("role")
    if not user_id or role not in ("PATIENT", "DOCTOR"):
        return response(403, {"message": "A signed-in account is required"})

    path = _clean_path(event)
    query = event.get("queryStringParameters") or {}
    conn = None
    try:
        if path == "/patients/me":
            if role != "PATIENT":
                return response(403, {"message": "A patient account is required"})
            body = json_body(event) if method in ("POST", "PUT", "PATCH") else {}
            conn = db_connect()
            with conn.cursor() as cur:
                return _handle_patient_me(conn, cur, method, user_id, body)

        if method != "GET":
            return response(405, {"message": "Method not allowed"})

        if path == "/doctors":
            conn = db_connect()
            with conn.cursor() as cur:
                return response(200, _search_doctors(cur, query))

        match = re.fullmatch(r"/doctors/(\d+)", path)
        if match:
            conn = db_connect()
            with conn.cursor() as cur:
                profile = _doctor_profile(cur, int(match.group(1)))
            if not profile:
                return response(404, {"message": "Doctor not found"})
            return response(200, {"doctor": profile})

        match = re.fullmatch(r"/doctors/(\d+)/slots", path)
        if match:
            doctor_id = int(match.group(1))
            conn = db_connect()
            with conn.cursor() as cur:
                if not _active_doctor(cur, doctor_id):
                    return response(404, {"message": "Doctor not found"})
                return response(200, _doctor_slots(cur, doctor_id, query))

        return response(404, {"message": "Endpoint not found", "path": path})
    except FieldErrors as exc:
        if conn:
            conn.rollback()
        return response(400, {"message": "Please correct the highlighted fields.", "errors": exc.errors})
    except (ValueError, TypeError) as exc:
        if conn:
            conn.rollback()
        log_event("patient_api_validation_failed", logging.WARNING,
                  request_id=getattr(context, "aws_request_id", None), method=method, path=path,
                  error_type=type(exc).__name__, message=str(exc)[:200])
        return response(400, {"message": str(exc)})
    except pymysql.err.IntegrityError as exc:
        if conn:
            conn.rollback()
        logger.info("Patient domain integrity validation: %s", str(exc))
        return response(409, {"message": "The requested record conflicts with existing data"})
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Patient domain request failed; request_id=%s", getattr(context, "aws_request_id", None))
        return response(500, {"message": "Patient domain request failed"})
    finally:
        if conn:
            conn.close()


def lambda_handler(event, context):
    started = _monotonic()
    result = _handle_api(event, context)
    status = result.get("statusCode") if isinstance(result, dict) else None
    level = logging.INFO if not status or status < 400 else logging.WARNING if status < 500 else logging.ERROR
    auth = (event.get("requestContext") or {}).get("authorizer") or {}
    log_event("patient_api_request", level, request_id=getattr(context, "aws_request_id", None),
              method=event.get("httpMethod"), resource=event.get("resource"), user_id=auth.get("userId"),
              status=status, duration_ms=int((_monotonic() - started) * 1000))
    return result
