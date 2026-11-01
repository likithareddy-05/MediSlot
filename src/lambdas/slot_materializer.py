
"""Daily rolling 30-day doctor slot materialiser.

Safe to re-run:
- Deterministic slot IDs
- Conditional DynamoDB PutItem
- Existing slots are not overwritten
"""

import os
import json
import logging
import hashlib

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
                    d.doctor_id,
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
                    ct.duration_minutes,
                    ct.slot_capacity

                FROM doctors d

                JOIN availability_patterns ap
                    ON ap.doctor_id = d.doctor_id
                    AND ap.is_active = 1

                JOIN consultation_types ct
                    ON ct.consultation_type_id =
                       ap.consultation_type_id
                    AND ct.is_active = 1

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

                    # ------------------------------------------------
                    # Deterministic slot ID.
                    # Same doctor, consultation, date, local start
                    # and timezone always produce the same ID.
                    # ------------------------------------------------

                    material = (
                        f"{doctor_id}|"
                        f"{pattern['consultation_type_id']}|"
                        f"{day.isoformat()}|"
                        f"{local_start.time().isoformat()}|"
                        f"{doctor_timezone}"
                    )

                    slot_id = hashlib.sha256(
                        material.encode("utf-8")
                    ).hexdigest()

                    # ------------------------------------------------
                    # DynamoDB slot record.
                    # ------------------------------------------------

                    item = {
                        "slot_id": slot_id,

                        "doctor_id": str(doctor_id),

                        "consultation_type_id": int(
                            pattern["consultation_type_id"]
                        ),

                        "local_date": day.isoformat(),

                        "local_start": (
                            local_start.time().isoformat()
                        ),

                        "start_time_utc": utc_start.isoformat(),

                        "end_time_utc": utc_end.isoformat(),

                        "timezone": doctor_timezone,

                        "capacity": int(
                            pattern["slot_capacity"]
                        ),

                        "booked_count": 0,

                        "status": "AVAILABLE",

                        "created_at": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }

                    # ------------------------------------------------
                    # Conditional insertion:
                    # Existing slots are not overwritten.
                    # ------------------------------------------------

                    if put_if_absent(table, item):
                        created += 1
                    else:
                        skipped += 1

        # ----------------------------------------------------
        # Successful response.
        # ----------------------------------------------------

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

    finally:
        conn.close()
