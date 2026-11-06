
"""Daily rolling 30-day doctor slot materialiser.

Safe to re-run:
- Deterministic slot IDs
- Conditional DynamoDB PutItem
- Existing slots are not overwritten
"""

import os
import json
import logging
import time as _time

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
import pymysql

from slot_time import build_slots, put_if_absent


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

log = logging.getLogger()
log.setLevel(logging.INFO)


def _log_event(event_name, level=logging.INFO, **fields):
    """One structured JSON log line. Ids and counts only, never personal data."""
    log.log(level, json.dumps({"event": event_name, **fields}, default=str))


# ------------------------------------------------------------
# AWS clients
# ------------------------------------------------------------

ssm = boto3.client("ssm")
sm = boto3.client("secretsmanager")
ddb = boto3.resource("dynamodb")


# ------------------------------------------------------------
# Environment configuration
# ------------------------------------------------------------

TABLE = os.environ["SLOT_TABLE_NAME"]

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

    requested_start = (
        date.fromisoformat(event["start_date"])
        if event.get("start_date")
        else None
    )

    days = int(event.get("days", 30))

    if not 1 <= days <= 60:
        raise ValueError("days must be between 1 and 60")

    started = _time.monotonic()
    request_id = getattr(context, "aws_request_id", None)
    _log_event("slot_materializer_started", request_id=request_id, days=days,
               requested_start=requested_start.isoformat() if requested_start else None,
               trigger="schedule" if event.get("source") == "aws.events" else "manual")

    conn = connection()
    table = ddb.Table(TABLE)

    created = 0
    skipped = 0

    window_starts = set()

    try:

        with conn.cursor() as cur:

            # ------------------------------------------------
            # Fetch active doctor availability patterns.
            #
            # Uses the existing schema:
            # availability_patterns.timezone_name
            # availability_patterns.weekday
            # availability_patterns.effective_from
            # availability_patterns.effective_to
            #
            # No doctor_settings table is required.
            # ------------------------------------------------

            cur.execute("""
                SELECT
                    d.user_id AS doctor_id,
                    ap.availability_pattern_id,
                    ap.timezone_name AS home_timezone,
                    ap.weekday AS day_of_week,
                    ap.effective_from,
                    ap.effective_to,

                    TIME_FORMAT(
                        ap.start_time,
                        '%H:%i:%s'
                    ) AS start_time,

                    TIME_FORMAT(
                        ap.end_time,
                        '%H:%i:%s'
                    ) AS end_time,

                    ct.consultation_type_id,
                    ct.consultation_name,
                    dct.fee,
                    ct.duration_minutes,
                    dct.slot_capacity

                FROM doctors d

                JOIN availability_patterns ap
                    ON ap.doctor_id = d.user_id
                    AND ap.is_active = 1

                JOIN doctor_consultation_types dct
                    ON dct.doctor_id = ap.doctor_id
                    AND dct.consultation_type_id =
                        ap.consultation_type_id
                    AND dct.is_active = 1

                JOIN consultation_types ct
                    ON ct.consultation_type_id =
                       dct.consultation_type_id

            """)

            patterns = cur.fetchall()

            # ------------------------------------------------
            # Fetch doctor blackout dates.
            # ------------------------------------------------

            cur.execute("""
                SELECT
                    doctor_id,
                    blackout_date
                FROM doctor_blackouts
            """)

            blackout_rows = cur.fetchall()

            blackouts = {
                (
                    int(row["doctor_id"]),
                    row["blackout_date"].isoformat()
                )
                for row in blackout_rows
            }

            _log_event("slot_materializer_loaded", request_id=request_id,
                       patterns=len(patterns), blackout_days=len(blackouts),
                       doctors=len({int(p["doctor_id"]) for p in patterns}))
            if not patterns:
                _log_event("slot_materializer_no_active_patterns", logging.WARNING, request_id=request_id)

        # ----------------------------------------------------
        # Materialize slots for each doctor availability pattern.
        # ----------------------------------------------------

        for pattern in patterns:

            doctor_id = int(pattern["doctor_id"])

            doctor_timezone = pattern["home_timezone"]

            zone = ZoneInfo(doctor_timezone)

            # Use doctor's local date when no start date
            # is explicitly provided.

            start_day = (
                requested_start
                if requested_start
                else datetime.now(timezone.utc)
                    .astimezone(zone)
                    .date()
            )

            window_starts.add(start_day.isoformat())

            for offset in range(days):

                day = start_day + timedelta(days=offset)

                # ------------------------------------------------
                # Weekday mapping in your schema:
                # Monday = 0
                # Tuesday = 1
                # ...
                # Sunday = 6
                #
                # Python weekday() uses the same mapping.
                # ------------------------------------------------

                if int(pattern["day_of_week"]) != day.weekday():
                    continue

                # Respect availability effective dates.

                if day < pattern["effective_from"]:
                    continue

                effective_to = pattern["effective_to"]

                if effective_to is not None and day > effective_to:
                    continue

                # Skip blackout dates.

                if (
                    doctor_id,
                    day.isoformat()
                ) in blackouts:
                    continue

                duration = int(pattern["duration_minutes"])

                # ------------------------------------------------
                # Generate consultation slots.
                # build_slots() handles local-to-UTC conversion.
                # ------------------------------------------------

                generated_slots = build_slots(
                    day,
                    str(pattern["start_time"]),
                    str(pattern["end_time"]),
                    duration,
                    doctor_timezone
                )

                for (
                    local_start,
                    local_end,
                    utc_start,
                    utc_end
                ) in generated_slots:

                    # Same ID format as doctor_domain.materialize_doctor so the
                    # scheduled job and the on-save refresh never create duplicates.
                    slot_id = (
                        f"{doctor_id}#{pattern['consultation_type_id']}#"
                        f"{utc_start.strftime('%Y%m%dT%H%M%SZ')}"
                    )

                    start_text = utc_start.isoformat().replace("+00:00", "Z")
                    end_text = utc_end.isoformat().replace("+00:00", "Z")

                    item = {
                        "slot_id": slot_id,
                        "doctor_id": str(doctor_id),
                        "slot_date": day.isoformat(),
                        "consultation_type_id": int(pattern["consultation_type_id"]),
                        "consultation_name": pattern["consultation_name"],
                        "start_time_utc": start_text,
                        "end_time_utc": end_text,
                        "timezone": doctor_timezone,
                        "local_date": day.isoformat(),
                        "local_start": local_start.replace(tzinfo=None).isoformat(),
                        "duration_minutes": duration,
                        "fee": str(pattern["fee"]),
                        "capacity": int(pattern["slot_capacity"]),
                        "booked_count": 0,
                        "status": "AVAILABLE",
                        "availability_pattern_id": int(pattern["availability_pattern_id"]),
                        "start_at_utc": start_text,
                        "end_at_utc": end_text,
                        "timezone_name": doctor_timezone,
                    }

                    # Conditional insert: existing slots (and their booking
                    # state) are never touched.
                    if put_if_absent(table, item):
                        created += 1
                    else:
                        skipped += 1

        # ----------------------------------------------------
        # Successful response.
        # ----------------------------------------------------

        _log_event("slot_materializer_completed", request_id=request_id, created=created,
                   already_present=skipped, days=days,
                   duration_ms=int((_time.monotonic() - started) * 1000))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "created": created,
                "already_present": skipped,
                "window_start_by_timezone": sorted(
                    window_starts
                ),
                "days": days
            })
        }

    except Exception:
        failing = locals().get("pattern") or {}
        log.exception(json.dumps({
            "event": "slot_materializer_failed",
            "request_id": request_id,
            "doctor_id": failing.get("doctor_id"),
            "availability_pattern_id": failing.get("availability_pattern_id"),
            "slots_created_before_failure": created,
        }, default=str))
        raise

    finally:
        conn.close()
