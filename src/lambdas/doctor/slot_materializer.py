"""Daily rolling 30-day doctor slot materialiser (scheduled Lambda).

Slots live in RDS (the `slots` table). The generation logic is shared with the
doctor-domain Lambda in shared/slot_generation.py, so there is one implementation.

Safe to re-run:
- Deterministic slot IDs and INSERT ... ON DUPLICATE KEY (an existing slot is never touched)
- Booking counters of existing slots are never changed
- Nothing is created for dates before today (the doctor's local date)
"""

import os
import json
import logging
import time as _time

from datetime import date, datetime
from zoneinfo import ZoneInfo

import boto3
import pymysql

from slot_generation import materialize_doctor_slots

log = logging.getLogger()
log.setLevel(logging.INFO)


def _log_event(event_name, level=logging.INFO, **fields):
    """One structured JSON log line. Ids and counts only, never personal data."""
    log.log(level, json.dumps({"event": event_name, **fields}, default=str))


ssm = boto3.client("ssm")
sm = boto3.client("secretsmanager")

PARAMS = [
    os.environ["DB_HOST_PARAMETER"],
    os.environ["DB_PORT_PARAMETER"],
    os.environ["DB_NAME_PARAMETER"],
    os.environ["DB_USERNAME_PARAMETER"],
]

SECRET = os.environ["DB_SECRET_ARN"]

_cfg = None


# ------------------------------------------------------------
# Database connection
# ------------------------------------------------------------

def connection():
    global _cfg

    if _cfg is None:

        response = ssm.get_parameters(
            Names=PARAMS,
            WithDecryption=True
        )

        values = {
            item["Name"]: item["Value"]
            for item in response["Parameters"]
        }

        password_response = sm.get_secret_value(
            SecretId=SECRET
        )

        password = json.loads(
            password_response["SecretString"]
        )["password"]

        _cfg = {
            "host": values[PARAMS[0]],
            "port": int(values[PARAMS[1]]),
            "database": values[PARAMS[2]],
            "user": values[PARAMS[3]],
            "password": password
        }

    return pymysql.connect(
        **_cfg,
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


# ------------------------------------------------------------
# Lambda handler
# ------------------------------------------------------------

def handler(event, context):
    event = event or {}
    requested_start = date.fromisoformat(event["start_date"]) if event.get("start_date") else None
    days = int(event.get("days", 30))
    if not 1 <= days <= 60:
        raise ValueError("days must be between 1 and 60")

    started = _time.monotonic()
    request_id = getattr(context, "aws_request_id", None)
    _log_event("slot_materializer_started", request_id=request_id, days=days,
               requested_start=requested_start.isoformat() if requested_start else None,
               trigger="schedule" if event.get("source") == "aws.events" else "manual")

    conn = connection()
    results = []
    doctor_id = None
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT d.user_id AS doctor_id, COALESCE((SELECT a.timezone_name
                FROM availability_patterns a WHERE a.doctor_id=d.user_id AND a.is_active=1
                ORDER BY a.availability_pattern_id LIMIT 1), 'UTC') AS timezone_name FROM doctors d""")
            doctors = cur.fetchall()
        for doctor in doctors:
            doctor_id = doctor["doctor_id"]
            # The window starts at the doctor's local today unless a later start was asked for.
            local_today = datetime.now(ZoneInfo(doctor["timezone_name"])).date()
            start = max(requested_start, local_today) if requested_start else local_today
            results.append(materialize_doctor_slots(conn, doctor_id, start, days))

        created = sum(r["created"] for r in results)
        _log_event("slot_materializer_completed", request_id=request_id, doctors_processed=len(results),
                   created=created, removed=sum(r["removed"] for r in results),
                   restored=sum(r["restored"] for r in results), days=days,
                   duration_ms=int((_time.monotonic() - started) * 1000))
        return {"statusCode": 200, "body": json.dumps({
            "created": created, "doctors_processed": len(results), "days": days,
            "window_start_by_timezone": sorted({r["start_date"] for r in results})})}
    except Exception:
        log.exception(json.dumps({"event": "slot_materializer_failed", "request_id": request_id,
                                  "doctor_id": doctor_id, "doctors_done": len(results)}, default=str))
        raise
    finally:
        conn.close()
