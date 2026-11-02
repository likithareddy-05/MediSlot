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
    cursor.execute("SELECT doctor_id FROM doctors WHERE user_id=%s LIMIT 1", (user_id,))
    row = cursor.fetchone()
    return row["doctor_id"] if row else None


INDIAN_MOBILE_PATTERN = re.compile(r"[6-9][0-9]{9}")
INDIAN_MOBILE_MESSAGE = ("Enter a valid 10-digit Indian mobile number starting with 6, 7, 8 or 9 "
                         "(digits only, no spaces, +91 or symbols).")


def _indian_mobile(value):
    """Return the phone if it matches ^[6-9][0-9]{9}$, otherwise raise ValueError."""
    if not isinstance(value, str) or not INDIAN_MOBILE_PATTERN.fullmatch(value.strip()):
        raise ValueError(INDIAN_MOBILE_MESSAGE)
    return value.strip()


def _check_window(cursor, doctor_id, consultation_id, start_value, end_value):
    """The availability window must fit at least one slot of the consultation duration."""
    cursor.execute("SELECT duration_minutes FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",
                   (consultation_id, doctor_id))
    row = cursor.fetchone()
    if not row:
        raise ValueError("The selected consultation type is inactive or does not belong to you. Activate it on the Consultations tab first.")
    minutes = (start_value.hour*60 + start_value.minute) - 0
    window = (end_value.hour*60 + end_value.minute) - minutes
    if window < int(row["duration_minutes"]):
        raise ValueError(f"The availability window ({window} min) is shorter than the consultation duration "
                         f"({int(row['duration_minutes'])} min). Slots are generated automatically from the duration.")


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
    cursor.execute("""SELECT d.doctor_id,d.user_id,u.email,d.first_name,d.last_name,d.phone,
        d.date_of_birth,d.gender,d.address,d.specialization,d.license_number,
        d.years_of_experience,d.clinic_address,d.created_at,d.updated_at,
        a.photo_key FROM doctors d
        JOIN users u ON u.user_id=d.user_id
        LEFT JOIN doctor_profile_assets a ON a.doctor_id=d.doctor_id
        WHERE d.doctor_id=%s""", (doctor_id,))
    return cursor.fetchone()


APPOINTMENT_STATUSES = {"BOOKED","CANCELLED","COMPLETED","NO_SHOW"}
APPOINTMENT_SELECT = """SELECT a.appointment_id,a.slot_id,a.status,a.outcome,
    a.scheduled_start_utc,a.scheduled_end_utc,
    p.patient_id,p.first_name,p.last_name,p.phone,c.consultation_name
    FROM appointments a JOIN patients p ON p.patient_id=a.patient_id
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
    """Cancel a slot and record a full-fee PENDING refund for each booked patient.

    There is no payment gateway in this repository yet, so refund rows stay
    PENDING until a payment processor is connected.
    """
    table = ddb.Table(SLOT_TABLE_NAME)
    item = table.get_item(Key={"slot_id":slot_id}).get("Item")
    if not item or str(item.get("doctor_id")) != str(doctor_id):
        return response(404,{"message":"Slot not found"})
    cursor.execute("""SELECT a.appointment_id,a.patient_id,c.fee
        FROM appointments a JOIN consultation_types c
        ON c.consultation_type_id=a.consultation_type_id
        WHERE a.slot_id=%s AND a.doctor_id=%s AND a.status='BOOKED'""",(slot_id,doctor_id))
    affected = cursor.fetchall()
    for appt in affected:
        cursor.execute("UPDATE appointments SET status='CANCELLED' WHERE appointment_id=%s AND status='BOOKED'",
                       (appt["appointment_id"],))
        cursor.execute("""INSERT INTO refund_ledger
            (appointment_id,patient_id,doctor_id,refund_amount,refund_status)
            VALUES (%s,%s,%s,%s,'PENDING')
            ON DUPLICATE KEY UPDATE appointment_id=VALUES(appointment_id)""",
            (appt["appointment_id"],appt["patient_id"],doctor_id,appt["fee"]))
    conn.commit()
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
            raise
    return response(200,{"message":"Slot cancelled","slot_id":slot_id,
        "affected_appointments":len(affected),"refunds_created":len(affected),
        "refund_status":"PENDING_PAYMENT_PROCESSOR"})


SCHEDULE_MESSAGES = {
    "OK": "",
    "BLACKOUT": "Doctor is unavailable on this date because it is marked as a blackout day.",
    "NO_AVAILABILITY": "No weekly availability is configured. Add your weekly availability to generate slots.",
    "NOT_MATERIALISED": "Slots for this date have not been generated yet. They are created automatically when availability is saved and by the daily refresh.",
    "NO_SLOTS": "No slots available for the selected date.",
}


def _schedule_for_day(cursor, conn, doctor_id, day):
    """Schedule payload with an explicit status so the UI can explain an empty day.

    status: OK | BLACKOUT | NO_AVAILABILITY | NOT_MATERIALISED | NO_SLOTS
    Slot dates are the doctor's local calendar dates (the zone stored on the pattern).
    """
    cursor.execute("""SELECT weekday,timezone_name,effective_from,effective_to FROM availability_patterns
        WHERE doctor_id=%s AND is_active=1""", (doctor_id,))
    patterns = cursor.fetchall()
    tz_name = patterns[0]["timezone_name"] if patterns else "Asia/Kolkata"
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
        local_today = datetime.now(ZoneInfo(tz_name)).date()
        status = "NOT_MATERIALISED" if applicable and day >= local_today else "NO_SLOTS"
    appointments = _appointments_for_slots(cursor, doctor_id, [s["slot_id"] for s in slots])
    conn.commit()
    return {"doctor_id": doctor_id, "date": day.isoformat(), "timezone": tz_name, "status": status,
            "is_blackout": bool(blackout), "blackout_reason": (blackout or {}).get("reason"),
            "message": SCHEDULE_MESSAGES[status], "slots": slots, "appointments": appointments}


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
                          "specialization", "license_number", "years_of_experience", "clinic_address"]
                values = [_required(body, field) for field in fields]
                values[2] = _indian_mobile(values[2])
                date.fromisoformat(str(values[3]))
                if values[4] not in {"FEMALE", "MALE", "OTHER", "PREFER_NOT_TO_SAY"}:
                    raise ValueError("Invalid gender")
                if not re.fullmatch(r"[A-Z]{2}-[0-9]{5}", str(values[7])):
                    raise ValueError("license_number must match AA-12345 format")
                if not 0 <= int(values[8]) <= 60:
                    raise ValueError("years_of_experience must be between 0 and 60")
                cur.execute("""INSERT INTO doctors (user_id,first_name,last_name,phone,date_of_birth,gender,
                    address,specialization,license_number,years_of_experience,clinic_address)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", [user_id] + values)
                doctor_id = cur.lastrowid
                conn.commit()
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
                           "specialization", "years_of_experience", "clinic_address"}
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
                sql = "UPDATE doctors SET " + ",".join(f"{k}=%s" for k in updates) + " WHERE doctor_id=%s"
                cur.execute(sql, list(updates.values()) + [doctor_id])
                conn.commit()
                return response(200, {"message": "Doctor profile updated", "profile": _profile(cur, doctor_id)})

            if path == "/profile/photo-url" and method == "GET":
                cur.execute("SELECT photo_key FROM doctor_profile_assets WHERE doctor_id=%s",(doctor_id,))
                photo = cur.fetchone()
                if not photo:
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
                cur.execute("""INSERT INTO doctor_profile_assets (doctor_id,photo_key) VALUES (%s,%s)
                    ON DUPLICATE KEY UPDATE photo_key=VALUES(photo_key),updated_at=CURRENT_TIMESTAMP(6)""", (doctor_id, key))
                conn.commit()
                return response(200, {"upload_url": url, "photo_key": key, "method": "PUT",
                    "headers": {"Content-Type": content_type, "x-amz-server-side-encryption": "AES256"}, "expires_in_seconds": 300})

            if path == "/consultation-types" and method == "GET":
                cur.execute("SELECT consultation_type_id,doctor_id,consultation_name,duration_minutes,fee,slot_capacity,is_active,created_at,updated_at FROM consultation_types WHERE doctor_id=%s ORDER BY duration_minutes", (doctor_id,))
                return response(200, {"consultation_types": cur.fetchall()})
            if path == "/consultation-types" and method == "POST":
                name = _required(body, "consultation_name")
                consultation_rules = {
                    "Quick Consult": (15, 1),
                    "Standard Consult": (30, 1),
                    "Detailed Consult": (60, 1),
                    "Health Camp Session": (60, None),
                }
                if name not in consultation_rules:
                    raise ValueError("Choose one of the four approved consultation types")
                duration, fee, capacity = int(_required(body, "duration_minutes")), float(_required(body, "fee")), int(_required(body, "slot_capacity"))
                required_duration, fixed_capacity = consultation_rules[name]
                if duration != required_duration:
                    raise ValueError(f"{name} duration must be {required_duration} minutes")
                if fee < 0:
                    raise ValueError("fee must be zero or greater")
                if fixed_capacity is not None and capacity != fixed_capacity:
                    raise ValueError(f"{name} slot capacity must be 1")
                if fixed_capacity is None and not 2 <= capacity <= 10:
                    raise ValueError("Health Camp Session capacity must be between 2 and 10")
                cur.execute("SELECT consultation_type_id,is_active FROM consultation_types WHERE doctor_id=%s AND consultation_name=%s LIMIT 1", (doctor_id,name))
                existing_type = cur.fetchone()
                if existing_type and existing_type["is_active"]:
                    raise ValueError("This consultation type already exists in your account")
                if existing_type:
                    # Previously deactivated: bring it back with the submitted fee/capacity.
                    cur.execute("UPDATE consultation_types SET duration_minutes=%s,fee=%s,slot_capacity=%s,is_active=1 WHERE consultation_type_id=%s AND doctor_id=%s",
                                (duration, fee, capacity, existing_type["consultation_type_id"], doctor_id))
                    conn.commit()
                    return response(200, {"message": "Consultation type reactivated", "consultation_type_id": existing_type["consultation_type_id"],
                                          "slots": _refresh_slots(doctor_id)})
                cur.execute("INSERT INTO consultation_types (doctor_id,consultation_name,duration_minutes,fee,slot_capacity,is_active) VALUES (%s,%s,%s,%s,%s,1)", (doctor_id,name,duration,fee,capacity))
                conn.commit()
                return response(201, {"message": "Consultation type created", "consultation_type_id": cur.lastrowid})

            match = re.fullmatch(r"/consultation-types/(\d+)", path)
            if match and method in {"PUT", "PATCH"}:
                fields = {k: body[k] for k in ("consultation_name", "duration_minutes", "fee", "slot_capacity", "is_active") if k in body}
                if not fields: raise ValueError("Provide consultation fields to update")
                cur.execute("SELECT consultation_name,duration_minutes,slot_capacity FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s", (int(match.group(1)),doctor_id))
                current = cur.fetchone()
                if not current: return response(404, {"message": "Consultation type not found"})
                for k in ("duration_minutes", "slot_capacity"):
                    if k in fields: fields[k] = int(fields[k])
                if "fee" in fields: fields["fee"] = float(fields["fee"])
                if "is_active" in fields: fields["is_active"] = 1 if fields["is_active"] in (1, True, "1", "true", "True") else 0
                name = fields.get("consultation_name", current["consultation_name"])
                rules = {"Quick Consult": (15, 1), "Standard Consult": (30, 1), "Detailed Consult": (60, 1), "Health Camp Session": (60, None)}
                if name not in rules: raise ValueError("Choose one of the four approved consultation types")
                duration = fields.get("duration_minutes", int(current["duration_minutes"]))
                capacity = fields.get("slot_capacity", int(current["slot_capacity"]))
                required_duration, fixed_capacity = rules[name]
                if duration != required_duration: raise ValueError(f"{name} duration must be {required_duration} minutes")
                if fixed_capacity is not None and capacity != fixed_capacity: raise ValueError(f"{name} slot capacity must be 1")
                if fixed_capacity is None and not 2 <= capacity <= 10: raise ValueError("Health Camp Session capacity must be between 2 and 10")
                if "fee" in fields and fields["fee"] < 0: raise ValueError("fee cannot be negative")
                if "consultation_name" in fields:
                    cur.execute("SELECT 1 FROM consultation_types WHERE doctor_id=%s AND consultation_name=%s AND consultation_type_id<>%s LIMIT 1", (doctor_id,name,int(match.group(1))))
                    if cur.fetchone(): raise ValueError("This consultation type already exists in your account")
                cur.execute("UPDATE consultation_types SET " + ",".join(f"{k}=%s" for k in fields) + " WHERE consultation_type_id=%s AND doctor_id=%s", list(fields.values())+[int(match.group(1)),doctor_id])
                if cur.rowcount == 0: return response(404, {"message": "Consultation type not found"})
                conn.commit(); return response(200, {"message": "Consultation type updated", "slots": _refresh_slots(doctor_id)})
            if match and method == "DELETE":
                cur.execute("UPDATE consultation_types SET is_active=0 WHERE consultation_type_id=%s AND doctor_id=%s", (int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404, {"message": "Consultation type not found"})
                conn.commit(); return response(200, {"message": "Consultation type deactivated", "slots": _refresh_slots(doctor_id)})

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
                    _check_window(cur, doctor_id, consultation_id, start_value, end_value)
                    validated.append((consultation_id, weekday, start_value, end_value))

                cur.execute("UPDATE availability_patterns SET is_active=0 WHERE doctor_id=%s AND is_active=1", (doctor_id,))
                for consultation_id, weekday, start_value, end_value in validated:
                    cur.execute("""INSERT INTO availability_patterns
                        (doctor_id,consultation_type_id,weekday,start_time,end_time,timezone_name,effective_from,effective_to,is_active)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,1)""",
                        (doctor_id, consultation_id, weekday, start_value, end_value, str(timezone_name), effective_from))
                conn.commit()
                slots = _refresh_slots(doctor_id)
                return response(200, {"message": "Weekly availability replaced", "patterns_saved": len(validated), "timezone_name": str(timezone_name), "slots": slots})

            if path == "/availability" and method == "GET":
                cur.execute("SELECT * FROM availability_patterns WHERE doctor_id=%s AND is_active=1 ORDER BY weekday,start_time", (doctor_id,))
                rows = cur.fetchall()
                for row in rows:
                    row["start_time"] = _as_time(row["start_time"]).strftime("%H:%M:%S")
                    row["end_time"] = _as_time(row["end_time"]).strftime("%H:%M:%S")
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
                cur.execute("SELECT 1 FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(consultation_id,doctor_id))
                if not cur.fetchone(): raise ValueError("Invalid active consultation_type_id")
                _check_window(cur, doctor_id, consultation_id, start, end)
                cur.execute("INSERT INTO availability_patterns (doctor_id,consultation_type_id,weekday,start_time,end_time,timezone_name,effective_from,effective_to,is_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1)",(doctor_id,consultation_id,weekday,start,end,tz,effective_from,effective_to))
                conn.commit(); new_id = cur.lastrowid; return response(201,{"message":"Availability pattern created","availability_id":new_id,"slots":_refresh_slots(doctor_id)})
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
                    cur.execute("SELECT 1 FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(consultation_id,doctor_id))
                    if not cur.fetchone(): raise ValueError("Invalid active consultation_type_id")
                    fields["consultation_type_id"] = consultation_id
                cur.execute("SELECT consultation_type_id,start_time,end_time,effective_from,effective_to,timezone_name FROM availability_patterns WHERE availability_pattern_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                existing = cur.fetchone()
                if not existing: return response(404,{"message":"Availability pattern not found"})
                start_value = _as_time(fields.get("start_time",existing["start_time"]))
                end_value = _as_time(fields.get("end_time",existing["end_time"]))
                if start_value >= end_value: raise ValueError("start_time must be before end_time")
                _check_window(cur, doctor_id, int(fields.get("consultation_type_id", existing["consultation_type_id"])), start_value, end_value)
                from_value = fields.get("effective_from",existing["effective_from"])
                to_value = fields.get("effective_to",existing["effective_to"])
                if to_value is not None and to_value < from_value: raise ValueError("effective_to cannot precede effective_from")
                if "timezone_name" in fields:
                    cur.execute("SELECT DISTINCT timezone_name FROM availability_patterns WHERE doctor_id=%s AND is_active=1 AND availability_pattern_id<>%s",(doctor_id,int(match.group(1))))
                    zones = {row["timezone_name"] for row in cur.fetchall()}
                    if zones and fields["timezone_name"] not in zones: raise ValueError("All active availability patterns for one doctor must use the same timezone")
                cur.execute("UPDATE availability_patterns SET " + ",".join(f"{k}=%s" for k in fields) + " WHERE availability_pattern_id=%s AND doctor_id=%s",list(fields.values())+[int(match.group(1)),doctor_id])
                conn.commit(); return response(200,{"message":"Availability pattern updated","slots":_refresh_slots(doctor_id)})
            if match and method == "DELETE":
                cur.execute("UPDATE availability_patterns SET is_active=0 WHERE availability_pattern_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404,{"message":"Availability pattern not found"})
                conn.commit(); return response(200,{"message":"Availability pattern deactivated","slots":_refresh_slots(doctor_id)})

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
                    slot_table.update_item(Key={"slot_id":slot["slot_id"]},
                        UpdateExpression="SET #status = :status",
                        ExpressionAttributeNames={"#status":"status"},
                        ExpressionAttributeValues={":status":"BLACKED_OUT"})
                return response(201,{"message":"Blackout date created","blackout_id":blackout_id,
                    "existing_slots_marked":len(existing.get("Items", []))})
            match = re.fullmatch(r"/blackouts/(\d+)",path)
            if match and method == "DELETE":
                cur.execute("DELETE FROM doctor_blackouts WHERE blackout_id=%s AND doctor_id=%s",(int(match.group(1)),doctor_id))
                if not cur.rowcount: return response(404,{"message":"Blackout date not found"})
                conn.commit(); return response(200,{"message":"Blackout date removed","slots":_refresh_slots(doctor_id)})

            if path == "/schedule" and method == "GET":
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
                    return response(404,{"message":"Booked appointment not found"})
                conn.commit()
                return response(200,{"message":f"Appointment marked {outcome}"})

            match = re.fullmatch(r"/slots/(.+)",path)
            if match and method == "DELETE":
                # Slot ids contain '#', so clients send them URL-encoded.
                slot_id = unquote(match.group(1))
                return _cancel_slot(conn, cur, doctor_id, slot_id)

            if path == "/materialize" and method == "POST":
                start = date.fromisoformat(str(body["start_date"])) if body.get("start_date") else None
                return response(200,materialize_doctor(doctor_id,start,30))

        return response(404,{"message":"Doctor endpoint not found","path":path})
    except (ValueError, TypeError, json.JSONDecodeError, ZoneInfoNotFoundError) as exc:
        if conn: conn.rollback()
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


def materialize_doctor(doctor_id, start_day=None, days=30):
    """Create/refresh this doctor's slots for the rolling window starting at start_day.

    - Idempotent: slot ids are deterministic (doctor#consultation#UTC start).
    - Existing slots are never overwritten; booking counters are untouched.
    - Unbooked future slots that no longer match the active patterns are marked
      REMOVED; they are restored automatically if the pattern returns.
    """
    conn = db_connect()
    table = ddb.Table(SLOT_TABLE_NAME)
    created = skipped = removed = restored = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT a.availability_pattern_id,a.consultation_type_id,a.weekday,
                a.start_time,a.end_time,a.timezone_name,a.effective_from,a.effective_to,
                c.consultation_name,c.duration_minutes,c.fee,c.slot_capacity
                FROM availability_patterns a JOIN consultation_types c
                ON c.consultation_type_id=a.consultation_type_id AND c.doctor_id=a.doctor_id
                WHERE a.doctor_id=%s AND a.is_active=1 AND c.is_active=1""",(doctor_id,))
            patterns = cur.fetchall()
            if start_day is None:
                start_day = (datetime.now(ZoneInfo(patterns[0]["timezone_name"])).date()
                             if patterns else date.today())
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
                    if sid in desired or slot.get("status") != "AVAILABLE": continue
                    if int(slot.get("booked_count", 0)) != 0: continue
                    if str(slot.get("start_time_utc") or slot.get("start_at_utc") or "") <= now_utc: continue
                    if _set_status(table, sid, "REMOVED", "AVAILABLE"):
                        removed += 1
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
            cur.execute("""SELECT d.doctor_id, COALESCE((SELECT a.timezone_name
                FROM availability_patterns a WHERE a.doctor_id=d.doctor_id AND a.is_active=1
                ORDER BY a.availability_pattern_id LIMIT 1), 'UTC') AS timezone_name
                FROM doctors d""")
            doctors = cur.fetchall()
    finally:
        conn.close()
    for doctor in doctors:
        local_today = datetime.now(ZoneInfo(doctor["timezone_name"])).date()
        summaries.append(materialize_doctor(doctor["doctor_id"],local_today,30))
    logger.info("Slot materialisation completed: %s",json.dumps(summaries,default=str))
    return {"doctors_processed":len(summaries),"results":summaries}


def lambda_handler(event, context):
    # Scheduled invocation is handled by the dedicated handler; API requests use
    # the authenticated doctor identity injected by the existing JWT authorizer.
    return _handle_api(event,context)