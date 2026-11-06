"""MediSlot doctor-domain API and DST-safe slot materialiser.

All schedule wall-clock values are interpreted in the IANA timezone stored on
an availability pattern. UTC instants are persisted to DynamoDB. During a
fall-back overlap the first occurrence (fold=0) is selected; nonexistent
spring-forward wall times are skipped.
"""
import json
import logging
import os
import re
from time import monotonic as _monotonic
from urllib.parse import unquote
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import boto3
import pymysql
from botocore.config import Config
from boto3.dynamodb.conditions import Key
from slot_time import valid_local_time, put_if_absent

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _log_event(event_name, level=logging.INFO, **fields):
    """Emit one structured JSON log line (searchable with CloudWatch Logs Insights).

    Only ids, counts and field names belong here. Never pass emails, phone
    numbers, names, addresses, passwords or tokens.
    """
    logger.log(level, json.dumps({"event": event_name, **fields}, default=str))
ssm = boto3.client("ssm")
secrets = boto3.client("secretsmanager")
ddb = boto3.resource("dynamodb")
s3 = boto3.client("s3", config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))

DB_HOST_PARAMETER = os.environ["DB_HOST_PARAMETER"]
DB_PORT_PARAMETER = os.environ["DB_PORT_PARAMETER"]
DB_NAME_PARAMETER = os.environ["DB_NAME_PARAMETER"]
DB_USERNAME_PARAMETER = os.environ["DB_USERNAME_PARAMETER"]
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
# CloudFormation (authentication.yaml) defines SLOT_TABLE_NAME. Older builds read
# SLOT_STATE_TABLE, which crashed the Lambda at import time. Accept either name.
SLOT_TABLE_NAME = os.environ.get("SLOT_TABLE_NAME") or os.environ.get("SLOT_STATE_TABLE")
if not SLOT_TABLE_NAME:
    raise RuntimeError("Set SLOT_TABLE_NAME (or SLOT_STATE_TABLE) on the doctor-domain Lambda")
ASSETS_BUCKET = os.environ["ASSETS_BUCKET"]
_db_config = None


def _db_config_values():
    global _db_config
    if _db_config is None:
        names = [DB_HOST_PARAMETER, DB_PORT_PARAMETER, DB_NAME_PARAMETER, DB_USERNAME_PARAMETER]
        result = ssm.get_parameters(Names=names, WithDecryption=False)
        values = {p["Name"]: p["Value"] for p in result.get("Parameters", [])}
        missing = [n for n in names if n not in values]
        if missing:
            raise RuntimeError("Missing DB SSM parameters: " + ", ".join(missing))
        _db_config = {"host": values[DB_HOST_PARAMETER], "port": int(values[DB_PORT_PARAMETER]),
                     "name": values[DB_NAME_PARAMETER], "username": values[DB_USERNAME_PARAMETER]}
    return _db_config


def db_connect():
    cfg = _db_config_values()
    secret = secrets.get_secret_value(SecretId=DB_SECRET_ARN).get("SecretString")
    if not secret:
        raise RuntimeError("Database secret is empty")
    password = json.loads(secret)["password"]
    return pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
        password=password, database=cfg["name"], connect_timeout=5, read_timeout=15,
        write_timeout=15, cursorclass=pymysql.cursors.DictCursor, autocommit=False,
        charset="utf8mb4")


def response(code, body):
    return {"statusCode": code, "headers": {
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Authorization,Content-Type,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
            }, "body": json.dumps(body, default=str)}


def _json_body(event):
    raw = event.get("body")
    if not raw:
        return {}
    if event.get("isBase64Encoded"):
        import base64
        raw = base64.b64decode(raw).decode("utf-8")
    body = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")
    return body


def _doctor_id(cursor, user_id):
    """A doctor's id IS the account's user_id; this only confirms a doctor profile exists."""
    cursor.execute("SELECT user_id FROM doctors WHERE user_id=%s LIMIT 1", (user_id,))
    row = cursor.fetchone()
    return int(row["user_id"]) if row else None


INDIAN_MOBILE_PATTERN = re.compile(r"[6-9][0-9]{9}")
INDIAN_MOBILE_MESSAGE = ("Enter a valid 10-digit Indian mobile number starting with 6, 7, 8 or 9 "
                         "(digits only, no spaces, +91 or symbols).")


def _indian_mobile(value):
    """Return the phone if it matches ^[6-9][0-9]{9}$, otherwise raise ValueError."""
    if not isinstance(value, str) or not INDIAN_MOBILE_PATTERN.fullmatch(value.strip()):
        raise ValueError(INDIAN_MOBILE_MESSAGE)
    return value.strip()


def _catalog_type_by_name(cursor, name):
    """Row of the consultation_types catalog (the four approved types) or None."""
    cursor.execute("""SELECT consultation_type_id,consultation_name,duration_minutes,min_capacity,max_capacity
        FROM consultation_types WHERE consultation_name=%s""", (name,))
    return cursor.fetchone()


def _validate_capacity(name, catalog, capacity):
    """Capacity must be inside the catalog range for this consultation type."""
    low, high = int(catalog["min_capacity"]), int(catalog["max_capacity"])
    if low == high:
        if capacity != low:
            raise ValueError(f"{name} slot capacity must be {low}")
    elif not low <= capacity <= high:
        raise ValueError(f"{name} capacity must be between {low} and {high}")


def _check_window(cursor, doctor_id, consultation_id, start_value, end_value):
    """The availability window must fit at least one slot of the consultation duration.

    Returns the consultation duration in minutes (needed for the overlap check).
    """
    cursor.execute("""SELECT c.duration_minutes FROM doctor_consultation_types d
        JOIN consultation_types c ON c.consultation_type_id=d.consultation_type_id
        WHERE d.consultation_type_id=%s AND d.doctor_id=%s AND d.is_active=1""",
                   (consultation_id, doctor_id))
    row = cursor.fetchone()
    if not row:
        raise ValueError("The selected consultation type is inactive or does not belong to you. Activate it on the Consultations tab first.")
    minutes = (start_value.hour*60 + start_value.minute) - 0
    window = (end_value.hour*60 + end_value.minute) - minutes
    if window < int(row["duration_minutes"]):
        raise ValueError(f"The availability window ({window} min) is shorter than the consultation duration "
                         f"({int(row['duration_minutes'])} min). Slots are generated automatically from the duration.")
    return int(row["duration_minutes"])


DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


def _minutes(value):
    return value.hour * 60 + value.minute


def _effective_end(start_value, end_value, duration):
    """Minute at which the last whole slot of a window ends.

    Slots never run past the end time, so minutes at the tail that cannot hold
    another slot stay free. 09:00-09:34 with 15-minute slots gives slots at
    09:00 and 09:15 and occupies the doctor only until 09:30 (the other 4
    minutes remain available for another window).
    """
    start = _minutes(start_value)
    window = _minutes(end_value) - start
    return start + (window // int(duration)) * int(duration)


def _fmt_minutes(total):
    return f"{total // 60:02d}:{total % 60:02d}"


def _ranges_overlap(a_start, a_end, b_start, b_end):
    return a_start < b_end and b_start < a_end


def _dates_overlap(from_a, to_a, from_b, to_b):
    return (to_a is None or from_b <= to_a) and (to_b is None or from_a <= to_b)


def _check_overlap(cursor, doctor_id, weekday, start_value, end_value, duration, eff_from, eff_to, exclude_id=None):
    """One stretch of time on a weekday may belong to only one availability pattern.

    Overlapping windows for different consultation types would offer the same
    clock time as different slot grids (e.g. 15-min and 60-min), so they are rejected.
    Windows are compared by the time their slots actually occupy (see _effective_end),
    not by the raw end time typed in: leftover minutes too short for another slot
    do not block a following window.
    """
    new_start = _minutes(start_value)
    new_end = _effective_end(start_value, end_value, duration)
    cursor.execute("""SELECT a.availability_pattern_id,a.start_time,a.end_time,a.effective_from,a.effective_to,
        c.consultation_name,c.duration_minutes FROM availability_patterns a
        JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id
        WHERE a.doctor_id=%s AND a.is_active=1 AND a.weekday=%s""", (doctor_id, weekday))
    for row in cursor.fetchall():
        if exclude_id is not None and int(row["availability_pattern_id"]) == int(exclude_id):
            continue
        other_start, other_end = _as_time(row["start_time"]), _as_time(row["end_time"])
        other_from = _minutes(other_start)
        other_to = _effective_end(other_start, other_end, int(row["duration_minutes"]))
        if (_ranges_overlap(new_start, new_end, other_from, other_to)
                and _dates_overlap(eff_from, eff_to, row["effective_from"], row["effective_to"])):
            raise ValueError(
                f"This window overlaps your existing {DAY_NAMES[weekday]} availability "
                f"{_fmt_minutes(other_from)}–{_fmt_minutes(other_to)} ({row['consultation_name']}). "
                "The same time cannot be offered under two consultation types; "
                "choose a different time range or edit/remove the existing one.")


def _required(body, key):
    value = body.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{key} is required")
    return value.strip() if isinstance(value, str) else value


def _as_time(value):
    """Normalize MySQL TIME values (PyMySQL may return timedelta) to time."""
    if isinstance(value, time):
        return value
    if isinstance(value, timedelta):
        seconds = int(value.total_seconds()) % 86400
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)
    return time.fromisoformat(str(value))


def _profile(cursor, doctor_id):
    # `specialization` stays in the response as the readable name; the ids are what is stored.
    cursor.execute("""SELECT d.user_id,u.email,d.first_name,d.last_name,d.phone,
        d.date_of_birth,d.gender,d.address,d.specialization_id,s.specialization_name AS specialization,
        d.hospital_id,h.hospital_name,h.city AS hospital_city,d.license_number,
        d.years_of_experience,d.clinic_address,d.created_at,d.updated_at,
        d.photo_key FROM doctors d
        JOIN users u ON u.user_id=d.user_id
        JOIN specializations s ON s.specialization_id=d.specialization_id
        JOIN hospitals h ON h.hospital_id=d.hospital_id
        WHERE d.user_id=%s""", (doctor_id,))
    return cursor.fetchone()


def _lookup_id(cursor, table, id_column, value, label, current=None):
    """Validate an id chosen from the hospitals / specializations dropdown.

    The id must exist and be active. Keeping the doctor's current value is always
    allowed, so a profile can still be saved if that entry was later hidden.
    """
    try:
        if isinstance(value, bool):
            raise ValueError
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Select a valid {label}")
    if current is not None and number == int(current):
        return number
    cursor.execute(f"SELECT 1 FROM {table} WHERE {id_column}=%s AND is_active=1", (number,))
    if not cursor.fetchone():
        raise ValueError(f"Select a valid {label}")
    return number


APPOINTMENT_STATUSES = {"BOOKED","CANCELLED","COMPLETED","NO_SHOW"}
APPOINTMENT_SELECT = """SELECT a.appointment_id,a.slot_id,a.status,a.outcome,
    a.scheduled_start_utc,a.scheduled_end_utc,
    p.user_id AS patient_id,p.first_name,p.last_name,p.phone,c.consultation_name
    FROM appointments a JOIN patients p ON p.user_id=a.patient_id
    JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id"""


def _as_utc_naive(value, end_of_day):
    """Parse an ISO date or datetime into a naive UTC datetime for MySQL.

    A date-only value means midnight at the start of that day. When it is used as
    an exclusive upper bound (end_of_day=True) it moves to the next midnight so
    the whole named day is included.
    """
    text = str(value).strip()
    if len(text) == 10:
        day = date.fromisoformat(text)
        if end_of_day:
            day += timedelta(days=1)
        return datetime.combine(day, time.min)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _appointments_for_slots(cursor, doctor_id, slot_ids):
    if not slot_ids:
        return []
    marks = ",".join(["%s"] * len(slot_ids))
    cursor.execute(APPOINTMENT_SELECT + f" WHERE a.doctor_id=%s AND a.slot_id IN ({marks})"
                   " ORDER BY a.scheduled_start_utc", [doctor_id, *slot_ids])
    return cursor.fetchall()


def _cancel_slot(conn, cursor, doctor_id, slot_id):
    """Cancel one slot: close it in DynamoDB, then mark its booked appointments CANCELLED.

    Order matters. The slot is closed first so no new patient can book it while
    the appointments are being cancelled. Both steps are idempotent, so if the
    MySQL step fails the doctor can simply press Cancel again.
    There is no refund table: no refund record is created.
    """
    table = ddb.Table(SLOT_TABLE_NAME)
    item = table.get_item(Key={"slot_id":slot_id}).get("Item")
    if not item or str(item.get("doctor_id")) != str(doctor_id):
        _log_event("slot_cancel_not_found", logging.WARNING, doctor_id=doctor_id, slot_id=slot_id)
        return response(404,{"message":"Slot not found"})
    end_text = str(item.get("end_time_utc") or item.get("end_at_utc") or "")
    if end_text and datetime.fromisoformat(end_text.replace("Z", "+00:00")) <= datetime.now(timezone.utc):
        raise ValueError("This slot has already ended and can no longer be cancelled.")
    try:
        table.update_item(Key={"slot_id":slot_id},
            UpdateExpression="SET #s=:cancelled, updated_at=:now",
            ConditionExpression="#s <> :cancelled",
            ExpressionAttributeNames={"#s":"status"},
            ExpressionAttributeValues={":cancelled":"CANCELLED",":now":datetime.now(timezone.utc).isoformat()})
    except Exception as exc:
        # Already cancelled is fine; the cancel call is idempotent.
        code = getattr(exc,"response",{}).get("Error",{}).get("Code")
        if code != "ConditionalCheckFailedException":
            _log_event("slot_cancel_failed", logging.ERROR, doctor_id=doctor_id, slot_id=slot_id, error_code=code)
            raise
    cursor.execute("""SELECT appointment_id FROM appointments
        WHERE slot_id=%s AND doctor_id=%s AND status='BOOKED'""",(slot_id,doctor_id))
    affected = cursor.fetchall()
    for appt in affected:
        cursor.execute("UPDATE appointments SET status='CANCELLED' WHERE appointment_id=%s AND status='BOOKED'",
                       (appt["appointment_id"],))
    conn.commit()
    _log_event("slot_cancelled", doctor_id=doctor_id, slot_id=slot_id,
               affected_appointments=len(affected))
    return response(200,{"message":"Slot cancelled","slot_id":slot_id,
        "affected_appointments":len(affected)})


def _consultation_type_in_use(cursor, doctor_id, type_id):
    """Why a consultation type cannot be deactivated yet, or None when it is free.

    A type is a parent of the doctor's availability and bookings. It may only be
    removed once the availability built on it is removed and nothing upcoming is
    booked, so no slots or bookings are lost by a stray click.
    """
    cursor.execute("""SELECT COUNT(*) AS n FROM availability_patterns
        WHERE doctor_id=%s AND consultation_type_id=%s AND is_active=1""", (doctor_id, type_id))
    patterns = int((cursor.fetchone() or {}).get("n") or 0)
    if patterns:
        return "You cannot deactivate this consultation type until its slots are deleted."
    cursor.execute("""SELECT COUNT(*) AS n FROM appointments
        WHERE doctor_id=%s AND consultation_type_id=%s AND status='BOOKED'
        AND scheduled_end_utc > UTC_TIMESTAMP(6)""", (doctor_id, type_id))
    booked = int((cursor.fetchone() or {}).get("n") or 0)
    if booked:
        return ("You cannot deactivate this consultation type while it has upcoming booked appointments. "
                "Cancel those slots from My Schedule first.")
    return None


SCHEDULE_MESSAGES = {
    "OK": "",
    "BLACKOUT": "Doctor is unavailable on this date because it is marked as a blackout day.",
    "NO_AVAILABILITY": "No weekly availability is configured. Add your weekly availability to generate slots.",
    "NOT_MATERIALISED": "Slots for this date have not been generated yet. They are created automatically when availability is saved and by the daily refresh.",
    "NO_SLOTS": "No slots available for the selected date.",
    "PAST": "Past date. Slots are only created and shown from today onwards.",
}


def _active_patterns(cursor, doctor_id):
    cursor.execute("""SELECT weekday,timezone_name,effective_from,effective_to FROM availability_patterns
        WHERE doctor_id=%s AND is_active=1""", (doctor_id,))
    return cursor.fetchall()


def _schedule_for_day(cursor, conn, doctor_id, day, patterns=None):
    """Schedule payload with an explicit status so the UI can explain an empty day.

    status: OK | BLACKOUT | NO_AVAILABILITY | NOT_MATERIALISED | NO_SLOTS
    Slot dates are the doctor's local calendar dates (the zone stored on the pattern).
    `patterns` may be passed in so a multi-day view reads them only once.
    """
    if patterns is None:
        patterns = _active_patterns(cursor, doctor_id)
    tz_name = patterns[0]["timezone_name"] if patterns else "Asia/Kolkata"
    local_today = datetime.now(ZoneInfo(tz_name)).date()
    if day < local_today:
        # Slots exist only from today (doctor's local date) onwards: never list earlier days.
        return {"doctor_id": doctor_id, "date": day.isoformat(), "timezone": tz_name, "status": "PAST",
                "is_blackout": False, "blackout_reason": None, "message": SCHEDULE_MESSAGES["PAST"],
                "slots": [], "appointments": []}
    cursor.execute("SELECT blackout_id,reason FROM doctor_blackouts WHERE doctor_id=%s AND blackout_date=%s LIMIT 1",
                   (doctor_id, day))
    blackout = cursor.fetchone()
    all_slots = _query_day(ddb.Table(SLOT_TABLE_NAME), doctor_id, day)
    # Withdrawn/blacked-out slots are never shown as bookable.
    slots = [s for s in all_slots if s.get("status") not in ("REMOVED", "BLACKED_OUT")]
    if blackout:
        # Keep only slots that already hold patients so they are not lost from view.
        slots = [s for s in all_slots if int(s.get("booked_count", 0)) > 0]
        status = "BLACKOUT"
    elif slots:
        status = "OK"
    elif not patterns:
        status = "NO_AVAILABILITY"
    else:
        applicable = any(int(p["weekday"]) == day.weekday() and day >= p["effective_from"]
                         and (p["effective_to"] is None or day <= p["effective_to"]) for p in patterns)
        status = "NOT_MATERIALISED" if applicable and day >= local_today else "NO_SLOTS"
    appointments = _appointments_for_slots(cursor, doctor_id, [s["slot_id"] for s in slots])
    conn.commit()
    return {"doctor_id": doctor_id, "date": day.isoformat(), "timezone": tz_name, "status": status,
            "is_blackout": bool(blackout), "blackout_reason": (blackout or {}).get("reason"),
            "message": SCHEDULE_MESSAGES[status], "slots": slots, "appointments": appointments}


MAX_SCHEDULE_RANGE_DAYS = 14


def _schedule_for_range(cursor, conn, doctor_id, first_day, last_day):
    """Schedule for consecutive days (the week view): one entry per day, same shape as a single day."""
    if last_day < first_day:
        raise ValueError("to must not be earlier than from")
    count = (last_day - first_day).days + 1
    if count > MAX_SCHEDULE_RANGE_DAYS:
        raise ValueError(f"The schedule range can cover at most {MAX_SCHEDULE_RANGE_DAYS} days")
    patterns = _active_patterns(cursor, doctor_id)
    days = [_schedule_for_day(cursor, conn, doctor_id, first_day + timedelta(days=i), patterns)
            for i in range(count)]
    return {"doctor_id": doctor_id, "from": first_day.isoformat(), "to": last_day.isoformat(),
            "timezone": days[0]["timezone"], "days": days,
            "summary": {"slots": sum(len(d["slots"]) for d in days),
                        "appointments": sum(len(d["appointments"]) for d in days),
                        "blackout_days": sum(1 for d in days if d["is_blackout"])}}


def _handle_api(event, context):
    # Browser dashboard preflight. API Gateway maps OPTIONS without the JWT authorizer.
    if event.get("httpMethod", "GET").upper() == "OPTIONS":
        return response(204, {})
    auth = ((event.get("requestContext") or {}).get("authorizer") or {})
    user_id, role = auth.get("userId"), auth.get("role")
    if not user_id or role != "DOCTOR":
        return response(403, {"message": "A doctor account is required"})
    method = event.get("httpMethod", "GET").upper()
    # Explicit API Gateway resources provide event.resource (for example,
    # /doctors/me/availability). Keep the legacy proxy fallback for safe rollback.
    # NOTE: event["resource"] keeps placeholders such as /{consultationTypeId}, so the
    # numeric-id routes below would never match. Use the concrete request path instead.
    path = event.get("path") or event.get("resource") or (event.get("pathParameters") or {}).get("proxy", "")
    path = "/" + path.strip("/")
    stage = (event.get("requestContext") or {}).get("stage")
    if stage and path.startswith("/" + stage + "/"):
        path = path[len(stage) + 1:]
    if path.startswith("/doctors/"):
        path = path[len("/doctors"):]
    # Normalize the public /doctors/me contract to the existing domain handlers.
    if path == "/me":
        path = "/profile"
    elif path.startswith("/me/"):
        path = path[len("/me"):]
    query = event.get("queryStringParameters") or {}
    conn = None
    try:
        body = _json_body(event)
        conn = db_connect()
        with conn.cursor() as cur:
            doctor_id = _doctor_id(cur, user_id)
            # Normally doctor rows are created by the existing registration flow.
            # POST is also provided for accounts created before doctor profiles existed.
            if path == "/profile" and method == "POST":
                if doctor_id is not None:
                    return response(409, {"message": "Doctor profile already exists; use PUT to update it"})
                fields = ["first_name", "last_name", "phone", "date_of_birth", "gender", "address",
                          "specialization_id", "license_number", "years_of_experience", "clinic_address",
                          "hospital_id"]
                values = [_required(body, field) for field in fields]
                values[6] = _lookup_id(cur, "specializations", "specialization_id", values[6], "specialization")
                values[10] = _lookup_id(cur, "hospitals", "hospital_id", values[10], "hospital")
                values[2] = _indian_mobile(values[2])
                date.fromisoformat(str(values[3]))
                if values[4] not in {"FEMALE", "MALE", "OTHER", "PREFER_NOT_TO_SAY"}:
                    raise ValueError("Invalid gender")
                if not re.fullmatch(r"[A-Z]{2}-[0-9]{5}", str(values[7])):
                    raise ValueError("license_number must match AA-12345 format")
                if not 0 <= int(values[8]) <= 60:
                    raise ValueError("years_of_experience must be between 0 and 60")
                cur.execute("""INSERT INTO doctors (user_id,first_name,last_name,phone,date_of_birth,gender,
                    address,specialization_id,license_number,years_of_experience,clinic_address,hospital_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", [user_id] + values)
                doctor_id = int(user_id)  # the doctor id is the account's user_id
                conn.commit()
                _log_event("doctor_profile_created", doctor_id=doctor_id, user_id=user_id)
                return response(201, {"message": "Doctor profile created", "profile": _profile(cur, doctor_id)})
            if doctor_id is None:
                return response(404, {"message": "Doctor profile not found. Create it using POST /doctor/profile."})

            # Profile read/update. The registered identity/license fields are immutable here.
            if path == "/profile" and method == "GET":
                profile = _profile(cur, doctor_id)
                if profile and profile.get("photo_key"):
                    profile["photo_url"] = s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": ASSETS_BUCKET, "Key": profile["photo_key"]},
                        ExpiresIn=300,
                    )
                return response(200, {"profile": profile})
            if path == "/profile" and method in {"PUT", "PATCH"}:
                allowed = {"first_name", "last_name", "phone", "date_of_birth", "gender", "address",
                           "specialization_id", "hospital_id", "years_of_experience", "clinic_address"}
                updates = {k: body[k] for k in allowed if k in body}
                if not updates:
                    return response(400, {"message": "Provide at least one editable profile field"})
                if "phone" in updates:
                    updates["phone"] = _indian_mobile(updates["phone"])
                if "years_of_experience" in updates and not 0 <= int(updates["years_of_experience"]) <= 60:
                    raise ValueError("years_of_experience must be between 0 and 60")
                if "gender" in updates and updates["gender"] not in {"FEMALE", "MALE", "OTHER", "PREFER_NOT_TO_SAY"}:
                    raise ValueError("Invalid gender")
                if "date_of_birth" in updates:
                    date.fromisoformat(str(updates["date_of_birth"]))
                if "specialization_id" in updates or "hospital_id" in updates:
                    cur.execute("SELECT specialization_id,hospital_id FROM doctors WHERE user_id=%s", (doctor_id,))
                    current_ids = cur.fetchone()
                    if "specialization_id" in updates:
                        updates["specialization_id"] = _lookup_id(cur, "specializations", "specialization_id",
                            updates["specialization_id"], "specialization", current_ids["specialization_id"])
                    if "hospital_id" in updates:
                        updates["hospital_id"] = _lookup_id(cur, "hospitals", "hospital_id",
                            updates["hospital_id"], "hospital", current_ids["hospital_id"])
                sql = "UPDATE doctors SET " + ",".join(f"{k}=%s" for k in updates) + " WHERE user_id=%s"
                cur.execute(sql, list(updates.values()) + [doctor_id])
                conn.commit()
                _log_event("doctor_profile_updated", doctor_id=doctor_id, fields=sorted(updates))
                return response(200, {"message": "Doctor profile updated", "profile": _profile(cur, doctor_id)})

            if path == "/profile/photo-url" and method == "GET":
                cur.execute("SELECT photo_key FROM doctors WHERE user_id=%s",(doctor_id,))
                photo = cur.fetchone()
                if not photo or not photo["photo_key"]:
                    return response(404,{"message":"Profile photo not found"})
                url = s3.generate_presigned_url("get_object",Params={"Bucket":ASSETS_BUCKET,"Key":photo["photo_key"]},ExpiresIn=300)
                return response(200,{"photo_url":url,"photo_key":photo["photo_key"],"expires_in_seconds":300})

            if path in {"/photo", "/profile/photo-url"} and method == "POST":
                content_type = _required(body, "content_type")
                if content_type not in {"image/jpeg", "image/png", "image/webp"}:
                    raise ValueError("content_type must be image/jpeg, image/png, or image/webp")
                ext = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}[content_type]
                key = f"doctor-photos/{doctor_id}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{os.urandom(6).hex()}.{ext}"
                url = s3.generate_presigned_url("put_object", Params={"Bucket": ASSETS_BUCKET, "Key": key,
                    "ContentType": content_type, "ServerSideEncryption": "AES256"}, ExpiresIn=300)
                cur.execute("UPDATE doctors SET photo_key=%s WHERE user_id=%s", (key, doctor_id))
                conn.commit()
                _log_event("doctor_photo_upload_url_issued", doctor_id=doctor_id, photo_key=key, content_type=content_type)
                return response(200, {"upload_url": url, "photo_key": key, "method": "PUT",
                    "headers": {"Content-Type": content_type, "x-amz-server-side-encryption": "AES256"}, "expires_in_seconds": 300})

            if path == "/consultation-types" and method == "GET":
                cur.execute("""SELECT d.consultation_type_id,d.doctor_id,c.consultation_name,c.duration_minutes,
                    d.fee,d.slot_capacity,d.is_active,d.created_at,d.updated_at,
                    (SELECT COUNT(*) FROM availability_patterns a WHERE a.doctor_id=d.doctor_id
                        AND a.consultation_type_id=d.consultation_type_id AND a.is_active=1) AS active_availability_count
                    FROM doctor_consultation_types d
                    JOIN consultation_types c ON c.consultation_type_id=d.consultation_type_id
                    WHERE d.doctor_id=%s ORDER BY c.duration_minutes,c.consultation_type_id""", (doctor_id,))
                return response(200, {"consultation_types": cur.fetchall()})
            if path == "/consultation-types" and method == "POST":
                name = _required(body, "consultation_name")
                catalog = _catalog_type_by_name(cur, name)
                if not catalog:
                    raise ValueError("Choose one of the four approved consultation types")
                type_id = int(catalog["consultation_type_id"])
                fee, capacity = float(_required(body, "fee")), int(_required(body, "slot_capacity"))
                # Duration is defined by the catalog; a client may send it but it must match.
                required_duration = int(catalog["duration_minutes"])
                if body.get("duration_minutes") not in (None, "") and int(body["duration_minutes"]) != required_duration:
                    raise ValueError(f"{name} duration must be {required_duration} minutes")
                if fee < 0:
                    raise ValueError("fee must be zero or greater")
                _validate_capacity(name, catalog, capacity)
                cur.execute("SELECT is_active FROM doctor_consultation_types WHERE doctor_id=%s AND consultation_type_id=%s LIMIT 1", (doctor_id, type_id))
                existing_type = cur.fetchone()
                if existing_type and existing_type["is_active"]:
                    raise ValueError("This consultation type already exists in your account")
                if existing_type:
                    # Previously deactivated: bring it back with the submitted fee/capacity.
                    cur.execute("UPDATE doctor_consultation_types SET fee=%s,slot_capacity=%s,is_active=1 WHERE doctor_id=%s AND consultation_type_id=%s",
                                (fee, capacity, doctor_id, type_id))
                    conn.commit()
                    _log_event("consultation_type_reactivated", doctor_id=doctor_id,
                               consultation_type_id=type_id, consultation_name=name)
                    return response(200, {"message": "Consultation type reactivated", "consultation_type_id": type_id,
                                          "slots": _refresh_slots(doctor_id)})
                cur.execute("INSERT INTO doctor_consultation_types (doctor_id,consultation_type_id,fee,slot_capacity,is_active) VALUES (%s,%s,%s,%s,1)", (doctor_id, type_id, fee, capacity))
                conn.commit()
                _log_event("consultation_type_created", doctor_id=doctor_id,
                           consultation_type_id=type_id, consultation_name=name)
                return response(201, {"message": "Consultation type created", "consultation_type_id": type_id})

            match = re.fullmatch(r"/consultation-types/(\d+)", path)
            if match and method in {"PUT", "PATCH"}:
                fields = {k: body[k] for k in ("consultation_name", "duration_minutes", "fee", "slot_capacity", "is_active") if k in body}
                if not fields: raise ValueError("Provide consultation fields to update")
                type_id = int(match.group(1))
                cur.execute("""SELECT c.consultation_name,c.duration_minutes,c.min_capacity,c.max_capacity,d.slot_capacity
                    FROM doctor_consultation_types d JOIN consultation_types c ON c.consultation_type_id=d.consultation_type_id
                    WHERE d.consultation_type_id=%s AND d.doctor_id=%s""", (type_id, doctor_id))
                current = cur.fetchone()
                if not current: return response(404, {"message": "Consultation type not found"})
                name = current["consultation_name"]
                # Name and duration come from the catalog; they may be sent back unchanged but not altered.
                if "consultation_name" in fields and fields["consultation_name"] != name:
                    raise ValueError("The consultation type cannot be changed. Add the other type and deactivate this one.")
                if "duration_minutes" in fields and int(fields["duration_minutes"]) != int(current["duration_minutes"]):
                    raise ValueError(f"{name} duration must be {int(current['duration_minutes'])} minutes")
                fields = {k: fields[k] for k in ("fee", "slot_capacity", "is_active") if k in fields}
                if "slot_capacity" in fields: fields["slot_capacity"] = int(fields["slot_capacity"])
                if "fee" in fields: fields["fee"] = float(fields["fee"])
                if "is_active" in fields: fields["is_active"] = 1 if fields["is_active"] in (1, True, "1", "true", "True") else 0
                if fields.get("is_active") == 0:
                    blocked = _consultation_type_in_use(cur, doctor_id, type_id)
                    if blocked:
                        return response(409, {"message": blocked})
                _validate_capacity(name, current, fields.get("slot_capacity", int(current["slot_capacity"])))
                if "fee" in fields and fields["fee"] < 0: raise ValueError("fee cannot be negative")
                if fields:
                    cur.execute("UPDATE doctor_consultation_types SET " + ",".join(f"{k}=%s" for k in fields) + " WHERE consultation_type_id=%s AND doctor_id=%s", list(fields.values())+[type_id,doctor_id])
                conn.commit()
                _log_event("consultation_type_updated", doctor_id=doctor_id,
                           consultation_type_id=int(match.group(1)), fields=sorted(fields))
                return response(200, {"message": "Consultation type updated", "slots": _refresh_slots(doctor_id)})
            if match and method == "DELETE":
                cur.execute("SELECT 1 FROM doctor_consultation_types WHERE consultation_type_id=%s AND doctor_id=%s", (int(match.group(1)),doctor_id))
                if not cur.fetchone(): return response(404, {"message": "Consultation type not found"})
                blocked = _consultation_type_in_use(cur, doctor_id, int(match.group(1)))
                if blocked:
                    return response(409, {"message": blocked})
                cur.execute("UPDATE doctor_consultation_types SET is_active=0 WHERE consultation_type_id=%s AND doctor_id=%s", (int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404, {"message": "Consultation type not found"})
                conn.commit()
                _log_event("consultation_type_deactivated", doctor_id=doctor_id,
                           consultation_type_id=int(match.group(1)))
                return response(200, {"message": "Consultation type deactivated", "slots": _refresh_slots(doctor_id)})

            if path == "/availability" and method == "PUT":
                # Replace the doctor's active weekly patterns in one transaction.
                # Existing rows are deactivated (not deleted) to preserve history.
                patterns = body.get("patterns")
                timezone_name = body.get("timezone_name") or body.get("home_timezone")
                if not isinstance(patterns, list):
                    raise ValueError("patterns must be an array")
                if not timezone_name:
                    if patterns and patterns[0].get("timezone_name"):
                        timezone_name = patterns[0]["timezone_name"]
                    else:
                        raise ValueError("timezone_name is required")
                ZoneInfo(str(timezone_name))
                effective_from = date.fromisoformat(str(body.get("effective_from") or
                    datetime.now(ZoneInfo(str(timezone_name))).date().isoformat()))
                # Validate every item before changing existing patterns.
                validated = []
                for item in patterns:
                    if not isinstance(item, dict):
                        raise ValueError("Each availability pattern must be an object")
                    weekday = int(item.get("weekday", item.get("day_of_week", -1)))
                    if not 0 <= weekday <= 6:
                        raise ValueError("weekday must be 0-6 (Monday=0)")
                    start_value = time.fromisoformat(str(_required(item, "start_time")))
                    end_value = time.fromisoformat(str(_required(item, "end_time")))
                    if start_value >= end_value:
                        raise ValueError("start_time must be before end_time")
                    consultation_id = int(_required(item, "consultation_type_id"))
                    duration = _check_window(cur, doctor_id, consultation_id, start_value, end_value)
                    new_end = _effective_end(start_value, end_value, duration)
                    for _, other_day, other_start, _other_end, other_effective_end in validated:
                        if other_day == weekday and _ranges_overlap(_minutes(start_value), new_end,
                                                                    _minutes(other_start), other_effective_end):
                            raise ValueError(f"Overlapping {DAY_NAMES[weekday]} availability windows are not allowed")
                    # The window is stored exactly as entered; new_end is only used for the overlap check.
                    validated.append((consultation_id, weekday, start_value, end_value, new_end))

                cur.execute("UPDATE availability_patterns SET is_active=0 WHERE doctor_id=%s AND is_active=1", (doctor_id,))
                for consultation_id, weekday, start_value, end_value, _effective in validated:
                    cur.execute("""INSERT INTO availability_patterns
                        (doctor_id,consultation_type_id,weekday,start_time,end_time,timezone_name,effective_from,effective_to,is_active)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,1)""",
                        (doctor_id, consultation_id, weekday, start_value, end_value, str(timezone_name), effective_from))
                conn.commit()
                _log_event("availability_replaced", doctor_id=doctor_id, patterns_saved=len(validated),
                           timezone_name=str(timezone_name))
                slots = _refresh_slots(doctor_id)
                return response(200, {"message": "Weekly availability replaced", "patterns_saved": len(validated), "timezone_name": str(timezone_name), "slots": slots})

            if path == "/availability" and method == "GET":
                cur.execute("SELECT * FROM availability_patterns WHERE doctor_id=%s AND is_active=1 ORDER BY weekday,start_time", (doctor_id,))
                rows = cur.fetchall()
                cur.execute("SELECT consultation_type_id,duration_minutes FROM consultation_types")
                durations = {int(r["consultation_type_id"]): int(r["duration_minutes"]) for r in cur.fetchall()}
                for row in rows:
                    start_t, end_t = _as_time(row["start_time"]), _as_time(row["end_time"])
                    duration = durations.get(int(row["consultation_type_id"]))
                    # last_slot_end: where the final whole slot ends (differs from end_time when minutes are left over)
                    row["last_slot_end"] = _fmt_minutes(_effective_end(start_t, end_t, duration)) if duration else None
                    row["start_time"] = start_t.strftime("%H:%M:%S")
                    row["end_time"] = end_t.strftime("%H:%M:%S")
                timezone_name = rows[0]["timezone_name"] if rows else "Asia/Kolkata"
                return response(200, {"availability_patterns": rows, "timezone_name": timezone_name})
            if path == "/availability" and method == "POST":
                weekday = int(_required(body,"weekday")); start = time.fromisoformat(_required(body,"start_time")); end = time.fromisoformat(_required(body,"end_time")); tz = _required(body,"timezone_name")
                ZoneInfo(tz)
                cur.execute("SELECT DISTINCT timezone_name FROM availability_patterns WHERE doctor_id=%s AND is_active=1",(doctor_id,))
                existing_zones = {row["timezone_name"] for row in cur.fetchall()}
                if existing_zones and tz not in existing_zones:
                    raise ValueError("All active availability patterns for one doctor must use the same timezone")
                if not 0 <= weekday <= 6 or start >= end: raise ValueError("weekday must be 0-6 and start_time must be before end_time")
                effective_from = date.fromisoformat(str(body.get("effective_from") or datetime.now(ZoneInfo(tz)).date().isoformat()))
                effective_to = date.fromisoformat(str(body["effective_to"])) if body.get("effective_to") else None
                if effective_to and effective_to < effective_from: raise ValueError("effective_to cannot precede effective_from")
                consultation_id = int(_required(body,"consultation_type_id"))
                cur.execute("SELECT 1 FROM doctor_consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(consultation_id,doctor_id))
                if not cur.fetchone(): raise ValueError("Invalid active consultation_type_id")
                duration = _check_window(cur, doctor_id, consultation_id, start, end)
                _check_overlap(cur, doctor_id, weekday, start, end, duration, effective_from, effective_to)
                cur.execute("INSERT INTO availability_patterns (doctor_id,consultation_type_id,weekday,start_time,end_time,timezone_name,effective_from,effective_to,is_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1)",(doctor_id,consultation_id,weekday,start,end,tz,effective_from,effective_to))
                conn.commit(); new_id = cur.lastrowid
                _log_event("availability_created", doctor_id=doctor_id, availability_pattern_id=new_id,
                           weekday=weekday, timezone_name=tz)
                return response(201,{"message":"Availability pattern created","availability_id":new_id,"slots":_refresh_slots(doctor_id)})
            match = re.fullmatch(r"/availability/(\d+)", path)
            if match and method in {"PUT", "PATCH"}:
                fields = {k: body[k] for k in ("weekday", "start_time", "end_time", "timezone_name", "effective_from", "effective_to", "consultation_type_id") if k in body}
                if not fields: raise ValueError("Provide availability fields to update")
                if "weekday" in fields:
                    fields["weekday"] = int(fields["weekday"])
                    if not 0 <= fields["weekday"] <= 6: raise ValueError("weekday must be 0-6")
                if "start_time" in fields: fields["start_time"] = time.fromisoformat(str(fields["start_time"]))
                if "end_time" in fields: fields["end_time"] = time.fromisoformat(str(fields["end_time"]))
                if "timezone_name" in fields: ZoneInfo(fields["timezone_name"])
                if "effective_from" in fields: fields["effective_from"] = date.fromisoformat(str(fields["effective_from"]))
                if "effective_to" in fields and fields["effective_to"] is not None: fields["effective_to"] = date.fromisoformat(str(fields["effective_to"]))
                if "consultation_type_id" in fields:
                    consultation_id = int(fields["consultation_type_id"])
                    cur.execute("SELECT 1 FROM doctor_consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(consultation_id,doctor_id))
                    if not cur.fetchone(): raise ValueError("Invalid active consultation_type_id")
                    fields["consultation_type_id"] = consultation_id
                cur.execute("SELECT consultation_type_id,weekday,start_time,end_time,effective_from,effective_to,timezone_name FROM availability_patterns WHERE availability_pattern_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                existing = cur.fetchone()
                if not existing: return response(404,{"message":"Availability pattern not found"})
                existing_weekday = int(existing["weekday"])
                start_value = _as_time(fields.get("start_time",existing["start_time"]))
                end_value = _as_time(fields.get("end_time",existing["end_time"]))
                if start_value >= end_value: raise ValueError("start_time must be before end_time")
                duration = _check_window(cur, doctor_id, int(fields.get("consultation_type_id", existing["consultation_type_id"])), start_value, end_value)
                _check_overlap(cur, doctor_id, int(fields.get("weekday", existing_weekday)), start_value, end_value, duration,
                               fields.get("effective_from", existing["effective_from"]),
                               fields.get("effective_to", existing["effective_to"]), exclude_id=int(match.group(1)))
                from_value = fields.get("effective_from",existing["effective_from"])
                to_value = fields.get("effective_to",existing["effective_to"])
                if to_value is not None and to_value < from_value: raise ValueError("effective_to cannot precede effective_from")
                if "timezone_name" in fields:
                    cur.execute("SELECT DISTINCT timezone_name FROM availability_patterns WHERE doctor_id=%s AND is_active=1 AND availability_pattern_id<>%s",(doctor_id,int(match.group(1))))
                    zones = {row["timezone_name"] for row in cur.fetchall()}
                    if zones and fields["timezone_name"] not in zones: raise ValueError("All active availability patterns for one doctor must use the same timezone")
                cur.execute("UPDATE availability_patterns SET " + ",".join(f"{k}=%s" for k in fields) + " WHERE availability_pattern_id=%s AND doctor_id=%s",list(fields.values())+[int(match.group(1)),doctor_id])
                conn.commit()
                _log_event("availability_updated", doctor_id=doctor_id,
                           availability_pattern_id=int(match.group(1)), fields=sorted(fields))
                return response(200,{"message":"Availability pattern updated","slots":_refresh_slots(doctor_id)})
            if match and method == "DELETE":
                cur.execute("UPDATE availability_patterns SET is_active=0 WHERE availability_pattern_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404,{"message":"Availability pattern not found"})
                conn.commit()
                _log_event("availability_deactivated", doctor_id=doctor_id,
                           availability_pattern_id=int(match.group(1)))
                return response(200,{"message":"Availability pattern deactivated","slots":_refresh_slots(doctor_id)})

            if path == "/blackouts" and method == "GET":
                cur.execute("SELECT * FROM doctor_blackouts WHERE doctor_id=%s ORDER BY blackout_date",(doctor_id,))
                return response(200,{"blackouts":cur.fetchall()})
            if path == "/blackouts" and method == "POST":
                blackout_date = date.fromisoformat(str(_required(body,"blackout_date")))
                reason = body.get("reason")
                cur.execute("INSERT INTO doctor_blackouts (doctor_id,blackout_date,reason) VALUES (%s,%s,%s)",(doctor_id,blackout_date,reason))
                blackout_id = cur.lastrowid
                conn.commit()
                # Slots may already have been materialised for this date. Mark
                # them unavailable immediately so a newly-added blackout takes effect.
                slot_table = ddb.Table(SLOT_TABLE_NAME)
                existing = slot_table.query(IndexName="DoctorDateIndex",
                    KeyConditionExpression=Key("doctor_id").eq(str(doctor_id)) & Key("slot_date").eq(blackout_date.isoformat()))
                for slot in existing.get("Items", []):
                    if slot.get("status") == "CANCELLED":
                        continue  # a cancelled slot stays cancelled; removing the blackout must not reopen it
                    slot_table.update_item(Key={"slot_id":slot["slot_id"]},
                        UpdateExpression="SET #status = :status",
                        ExpressionAttributeNames={"#status":"status"},
                        ExpressionAttributeValues={":status":"BLACKED_OUT"})
                marked_items = existing.get("Items", [])
                booked_slots = sum(1 for s in marked_items if int(s.get("booked_count", 0)) > 0)
                # Booked appointments are NOT cancelled by a blackout: warn so it is visible.
                _log_event("blackout_created", logging.WARNING if booked_slots else logging.INFO,
                           doctor_id=doctor_id, blackout_id=blackout_id,
                           blackout_date=blackout_date.isoformat(), slots_marked=len(marked_items),
                           slots_with_bookings=booked_slots)
                return response(201,{"message":"Blackout date created","blackout_id":blackout_id,
                    "existing_slots_marked":len(existing.get("Items", []))})
            match = re.fullmatch(r"/blackouts/(\d+)",path)
            if match and method == "DELETE":
                cur.execute("DELETE FROM doctor_blackouts WHERE blackout_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404,{"message":"Blackout date not found"})
                conn.commit()
                _log_event("blackout_removed", doctor_id=doctor_id, blackout_id=int(match.group(1)))
                return response(200,{"message":"Blackout date removed","slots":_refresh_slots(doctor_id)})

            if path == "/schedule" and method == "GET":
                if query.get("from"):
                    # Week (or other short range) view: ?from=YYYY-MM-DD[&to=YYYY-MM-DD], default 7 days.
                    first_day = date.fromisoformat(str(query["from"]))
                    last_day = (date.fromisoformat(str(query["to"])) if query.get("to")
                                else first_day + timedelta(days=6))
                    return response(200, _schedule_for_range(cur, conn, doctor_id, first_day, last_day))
                return response(200, _schedule_for_day(cur, conn, doctor_id, date.fromisoformat(str(_required(query,"date")))))

            if path == "/appointments" and method == "GET":
                # from/to are UTC bounds. A date-only "to" value includes the whole day.
                start = _as_utc_naive(_required(query,"from"), end_of_day=False)
                end = _as_utc_naive(_required(query,"to"), end_of_day=True)
                sql = APPOINTMENT_SELECT + """ WHERE a.doctor_id=%s AND a.scheduled_start_utc >= %s
                    AND a.scheduled_start_utc < %s"""
                args = [doctor_id, start, end]
                status = query.get("status")
                if status:
                    status = status.upper()
                    if status not in APPOINTMENT_STATUSES:
                        raise ValueError("status must be one of " + ", ".join(sorted(APPOINTMENT_STATUSES)))
                    sql += " AND a.status=%s"
                    args.append(status)
                cur.execute(sql + " ORDER BY a.scheduled_start_utc", args)
                rows = cur.fetchall()
                conn.commit()
                return response(200,{"appointments":rows})

            match = re.fullmatch(r"/appointments/(\d+)/outcome",path)
            if match and method == "POST":
                outcome = str(body.get("outcome","")).upper()
                if outcome not in {"COMPLETED","NO_SHOW"}:
                    raise ValueError("outcome must be COMPLETED or NO_SHOW")
                cur.execute("""UPDATE appointments SET status=%s, outcome=%s
                    WHERE appointment_id=%s AND doctor_id=%s AND status='BOOKED'""",
                    (outcome,outcome,int(match.group(1)),doctor_id))
                if not cur.rowcount:
                    _log_event("appointment_outcome_rejected", logging.WARNING, doctor_id=doctor_id,
                               appointment_id=int(match.group(1)), outcome=outcome)
                    return response(404,{"message":"Booked appointment not found"})
                conn.commit()
                _log_event("appointment_outcome_recorded", doctor_id=doctor_id,
                           appointment_id=int(match.group(1)), outcome=outcome)
                return response(200,{"message":f"Appointment marked {outcome}"})

            match = re.fullmatch(r"/slots/(.+)",path)
            if match and method == "DELETE":
                # Slot ids contain '#', so clients send them URL-encoded.
                raw_slot = (event.get("pathParameters") or {}).get("slotId") or match.group(1)
                return _cancel_slot(conn, cur, doctor_id, unquote(raw_slot))

            if path == "/materialize" and method == "POST":
                start = date.fromisoformat(str(body["start_date"])) if body.get("start_date") else None
                return response(200,materialize_doctor(doctor_id,start,30))

        return response(404,{"message":"Doctor endpoint not found","path":path})
    except (ValueError, TypeError, json.JSONDecodeError, ZoneInfoNotFoundError) as exc:
        if conn: conn.rollback()
        _log_event("doctor_api_validation_failed", logging.WARNING,
                   request_id=getattr(context, "aws_request_id", None), method=method, path=path,
                   error_type=type(exc).__name__, message=str(exc)[:200])
        return response(400,{"message":str(exc)})
    except pymysql.err.IntegrityError as exc:
        if conn: conn.rollback()
        logger.info("Doctor domain integrity validation: %s",str(exc))
        return response(409,{"message":"The requested record conflicts with existing data"})
    except Exception:
        if conn: conn.rollback()
        logger.exception("Doctor domain request failed; request_id=%s",getattr(context,"aws_request_id",None))
        return response(500,{"message":"Doctor domain request failed"})
    finally:
        if conn: conn.close()


def _query_day(table, doctor_id, day):
    """All slot records for one doctor/date (handles pagination)."""
    items, kwargs = [], dict(IndexName="DoctorDateIndex",
        KeyConditionExpression=Key("doctor_id").eq(str(doctor_id)) & Key("slot_date").eq(day.isoformat()))
    while True:
        page = table.query(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _set_status(table, slot_id, status, expected):
    try:
        table.update_item(Key={"slot_id": slot_id},
            UpdateExpression="SET #s = :s",
            ConditionExpression="#s = :expected AND booked_count = :zero",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": status, ":expected": expected, ":zero": 0})
        return True
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def _delete_slot(table, slot_id, expected_statuses):
    """Hard-delete an unbooked slot; refuses if it was booked in the meantime."""
    try:
        table.delete_item(Key={"slot_id": slot_id},
            ConditionExpression="booked_count = :zero AND #s IN (" + ",".join(f":e{i}" for i in range(len(expected_statuses))) + ")",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":zero": 0, **{f":e{i}": v for i, v in enumerate(expected_statuses)}})
        return True
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def materialize_doctor(doctor_id, start_day=None, days=30):
    """Create/refresh this doctor's slots for the rolling window starting at start_day.

    - Idempotent: slot ids are deterministic (doctor#consultation#UTC start).
    - Existing slots are never overwritten; booking counters are untouched.
    - Unbooked future slots that no longer match the active patterns (edited or
      removed patterns) are deleted; slots with bookings are kept.
    """
    conn = db_connect()
    table = ddb.Table(SLOT_TABLE_NAME)
    created = skipped = removed = restored = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT a.availability_pattern_id,a.consultation_type_id,a.weekday,
                a.start_time,a.end_time,a.timezone_name,a.effective_from,a.effective_to,
                c.consultation_name,c.duration_minutes,d.fee,d.slot_capacity
                FROM availability_patterns a
                JOIN doctor_consultation_types d
                ON d.doctor_id=a.doctor_id AND d.consultation_type_id=a.consultation_type_id
                JOIN consultation_types c ON c.consultation_type_id=d.consultation_type_id
                WHERE a.doctor_id=%s AND a.is_active=1 AND d.is_active=1""",(doctor_id,))
            patterns = cur.fetchall()
            local_today = (datetime.now(ZoneInfo(patterns[0]["timezone_name"])).date()
                           if patterns else date.today())
            # Never create slots for dates before today (doctor's local date).
            if start_day is None or start_day < local_today:
                start_day = local_today
            cur.execute("SELECT blackout_date FROM doctor_blackouts WHERE doctor_id=%s AND blackout_date >= %s AND blackout_date < %s",(doctor_id,start_day,start_day+timedelta(days=days)))
            blackouts = {r["blackout_date"] for r in cur.fetchall()}
            conn.commit()
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        for offset in range(days):
            day = start_day + timedelta(days=offset)
            existing = {s["slot_id"]: s for s in _query_day(table, doctor_id, day)}
            desired = set()
            if day not in blackouts:
                for p in patterns:
                    if day.weekday() != int(p["weekday"]): continue
                    if day < p["effective_from"] or (p["effective_to"] and day > p["effective_to"]): continue
                    zone = ZoneInfo(p["timezone_name"])
                    start_t = _as_time(p["start_time"])
                    end_t = _as_time(p["end_time"])
                    duration = int(p["duration_minutes"])
                    local_end = datetime.combine(day,end_t)
                    cursor = datetime.combine(day,start_t)
                    while cursor + timedelta(minutes=duration) <= local_end:
                        aware_start = valid_local_time(cursor,zone)
                        aware_end = valid_local_time(cursor+timedelta(minutes=duration),zone)
                        if aware_start is None or aware_end is None:
                            skipped += 1
                            cursor += timedelta(minutes=duration)
                            continue
                        start_utc = aware_start.astimezone(timezone.utc)
                        # Elapsed time, not wall-clock subtraction (DST safe).
                        end_utc = start_utc + timedelta(minutes=duration)
                        slot_id = f"{doctor_id}#{p['consultation_type_id']}#{start_utc.strftime('%Y%m%dT%H%M%SZ')}"
                        desired.add(slot_id)
                        current = existing.get(slot_id)
                        if current is not None:
                            if current.get("status") in ("REMOVED", "BLACKED_OUT") and int(current.get("booked_count", 0)) == 0:
                                if _set_status(table, slot_id, "AVAILABLE", current["status"]):
                                    restored += 1
                            # Same slot still wanted after an edit: point it at the current pattern.
                            if (current.get("status") == "AVAILABLE"
                                    and int(current.get("availability_pattern_id", -1)) != int(p["availability_pattern_id"])):
                                table.update_item(Key={"slot_id": slot_id},
                                    UpdateExpression="SET availability_pattern_id = :pid",
                                    ExpressionAttributeValues={":pid": int(p["availability_pattern_id"])})
                            cursor += timedelta(minutes=duration)
                            continue
                        start_text = start_utc.isoformat().replace("+00:00", "Z")
                        end_text = end_utc.isoformat().replace("+00:00", "Z")
                        item = {
                            "slot_id": slot_id,
                            "doctor_id": str(doctor_id),
                            "slot_date": day.isoformat(),
                            "consultation_type_id": int(p["consultation_type_id"]),
                            "consultation_name": p["consultation_name"],
                            "start_time_utc": start_text,
                            "end_time_utc": end_text,
                            "timezone": p["timezone_name"],
                            "local_date": day.isoformat(),
                            "local_start": cursor.isoformat(),
                            "duration_minutes": duration,
                            "fee": str(p["fee"]),
                            "capacity": int(p["slot_capacity"]),
                            "booked_count": 0,
                            "status": "AVAILABLE",
                            "availability_pattern_id": int(p["availability_pattern_id"]),
                            "start_at_utc": start_text,
                            "end_at_utc": end_text,
                            "timezone_name": p["timezone_name"],
                        }
                        if put_if_absent(table, item):
                            created += 1
                        cursor += timedelta(minutes=duration)
            # Withdraw unbooked future slots that no active pattern produces any more
            # (edited/removed patterns, or legacy duplicate ids). Blackouts are handled
            # by the blackout endpoint (status BLACKED_OUT).
            if day not in blackouts:
                for sid, slot in existing.items():
                    if sid in desired or slot.get("status") not in ("AVAILABLE", "REMOVED"): continue
                    if int(slot.get("booked_count", 0)) != 0: continue
                    if str(slot.get("start_time_utc") or slot.get("start_at_utc") or "") <= now_utc: continue
                    # Old pattern's unbooked future slots are deleted outright; slots that
                    # hold patients (booked_count > 0) are never touched.
                    if _delete_slot(table, sid, ("AVAILABLE", "REMOVED")):
                        removed += 1
        # skipped > 0 means local wall times were dropped (DST gap): surface it as a warning.
        _log_event("materialize_doctor_completed", logging.WARNING if skipped else logging.INFO,
                   doctor_id=doctor_id, start_date=start_day.isoformat(), days=days,
                   patterns=len(patterns), blackout_days=len(blackouts), created=created,
                   restored=restored, removed=removed, dst_wall_times_skipped=skipped)
        return {"doctor_id":doctor_id,"start_date":start_day.isoformat(),"days":days,
                "created":created,"restored":restored,"removed":removed,
                "dst_wall_times_skipped":skipped,"idempotent":True}
    finally:
        conn.close()


def _refresh_slots(doctor_id):
    """Best-effort slot refresh after an availability/blackout change.

    The pattern change is already committed; a failure here must not undo it.
    The daily scheduler (and POST /doctors/me/materialize) will catch up.
    """
    try:
        return materialize_doctor(doctor_id, None, 30)
    except Exception:
        logger.exception("Slot refresh failed for doctor_id=%s", doctor_id)
        return {"error": "Slot refresh failed; it will be retried by the daily job"}


def materializer_handler(event, context):
    """EventBridge Scheduler entry point: materialize next rolling 30 days."""
    conn = db_connect()
    summaries = []
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT d.user_id AS doctor_id, COALESCE((SELECT a.timezone_name
                FROM availability_patterns a WHERE a.doctor_id=d.user_id AND a.is_active=1
                ORDER BY a.availability_pattern_id LIMIT 1), 'UTC') AS timezone_name
                FROM doctors d""")
            doctors = cur.fetchall()
    finally:
        conn.close()
    _log_event("slot_materialisation_started", doctors=len(doctors))
    for doctor in doctors:
        try:
            local_today = datetime.now(ZoneInfo(doctor["timezone_name"])).date()
            summaries.append(materialize_doctor(doctor["doctor_id"],local_today,30))
        except Exception:
            logger.exception("Slot materialisation failed for doctor_id=%s", doctor["doctor_id"])
            raise
    _log_event("slot_materialisation_completed", doctors_processed=len(summaries),
               created=sum(s["created"] for s in summaries), restored=sum(s["restored"] for s in summaries),
               removed=sum(s["removed"] for s in summaries),
               dst_wall_times_skipped=sum(s["dst_wall_times_skipped"] for s in summaries))
    return {"doctors_processed":len(summaries),"results":summaries}


def lambda_handler(event, context):
    # Scheduled invocation is handled by the dedicated handler; API requests use
    # the authenticated doctor identity injected by the existing JWT authorizer.
    started = _monotonic()
    request_id = getattr(context, "aws_request_id", None)
    try:
        result = _handle_api(event, context)
    except Exception:
        logger.exception("Doctor API unhandled error; request_id=%s", request_id)
        raise
    status = result.get("statusCode") if isinstance(result, dict) else None
    duration_ms = int((_monotonic() - started) * 1000)
    level = logging.INFO if not status or status < 400 else logging.WARNING if status < 500 else logging.ERROR
    if level == logging.INFO and duration_ms > 20000:
        level = logging.WARNING  # API Gateway gives up at 29 s
    auth = ((event.get("requestContext") or {}).get("authorizer") or {})
    _log_event("doctor_api_request", level, request_id=request_id,
               method=event.get("httpMethod"), resource=event.get("resource"),
               user_id=auth.get("userId"), status=status, duration_ms=duration_ms)
    return result