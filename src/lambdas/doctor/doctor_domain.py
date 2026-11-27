"""MediSlot doctor-domain API and DST-safe slot materialiser.

All schedule wall-clock values are interpreted in the IANA timezone stored on
an availability pattern. UTC instants are persisted to DynamoDB. During a
fall-back overlap the first occurrence (fold=0) is selected; nonexistent
spring-forward wall times are skipped.

Milestone 5 layout: slots are in RDS (shared/slot_store.py, shared/slot_generation.py) and
appointments are in DynamoDB (shared/appointment_store.py).
"""
import json
import logging
import os
import re
import time as time_module
from time import monotonic as _monotonic
from urllib.parse import unquote
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import boto3
import pymysql
from botocore.config import Config
import appointment_store as store
import slot_store
import waitlist_store
from slot_generation import materialize_doctor_slots
from refunds import delete_ledger_row, full_refund, no_show_refund, record_refund, refund_fields
from events import emit_metric, publish_event

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
APPOINTMENTS_TABLE_NAME = os.environ["APPOINTMENTS_TABLE_NAME"]
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
    except (TypeError, ValueError) as exc:
        _log_event("lookup_id_invalid", logging.WARNING, lookup=table,
                   error_type=type(exc).__name__)
        raise ValueError(f"Select a valid {label}")
    if current is not None and number == int(current):
        return number
    cursor.execute(f"SELECT 1 FROM {table} WHERE {id_column}=%s AND is_active=1", (number,))
    if not cursor.fetchone():
        raise ValueError(f"Select a valid {label}")
    return number


APPOINTMENT_STATUSES = set(store.STATUS_FILTERS)
FAR_FUTURE_ISO = "9999-12-31T23:59:59Z"


def _appointments_table():
    return ddb.Table(APPOINTMENTS_TABLE_NAME)


def _sleep(seconds):
    time_module.sleep(seconds)


def _patient_names(cursor, patient_ids):
    """Patient names and phones from RDS (the appointment itself only stores ids)."""
    ids = sorted({int(i) for i in patient_ids})
    if not ids:
        return {}
    marks = ",".join(["%s"] * len(ids))
    cursor.execute(f"SELECT user_id, first_name, last_name, phone FROM patients WHERE user_id IN ({marks})", ids)
    return {int(r["user_id"]): r for r in cursor.fetchall()}


def _money_text(value):
    """A DynamoDB money string as a number for the dashboard (None stays None)."""
    return None if value is None else float(value)


def _doctor_appointment_rows(cursor, items):
    """DynamoDB appointment items as the rows the doctor dashboard shows (patient name from RDS)."""
    names = _patient_names(cursor, [i["patient_id"] for i in items])
    rows = []
    for item in items:
        patient = names.get(int(item["patient_id"]), {})
        rows.append({
            "appointment_id": item["appointment_id"], "slot_id": item["slot_id"], "status": item["status"],
            "outcome": item.get("outcome"), "scheduled_start_utc": item["start_time_utc"],
            "scheduled_end_utc": item["end_time_utc"], "patient_id": int(item["patient_id"]),
            "first_name": patient.get("first_name"), "last_name": patient.get("last_name"),
            "phone": patient.get("phone"), "consultation_name": item.get("consultation_name"),
            "fee": _money_text(item.get("fee_amount")), "cancelled_at": item.get("cancelled_at"),
            "cancellation_reason": item.get("cancellation_reason"),
            "refund": None if item.get("refund_tier") is None else {
                "tier": item["refund_tier"], "percent": float(item["refund_percent"]),
                "amount": _money_text(item.get("refund_amount")),
                "deduction": _money_text(item.get("deduction_amount"))}})
    return rows


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


def _cancel_confirmed_by_doctor(table, cursor, conn, doctor_id, item):
    """Move one CONFIRMED appointment to CANCELLED_BY_DOCTOR with a 100% refund. True when it changed."""
    refund = full_refund(item["fee_amount"])
    now_text = store.now_iso()
    updated = store.transition(table, item["appointment_id"], store.CONFIRMED, store.CANCELLED_BY_DOCTOR, {
        "cancelled_at": now_text, "seat_released_at": now_text,
        "cancellation_reason": "Slot cancelled by the doctor", **refund_fields(refund)})
    if updated is None:
        return False
    try:
        store.release_hold(table, item["slot_id"], int(item["patient_id"]))
    except Exception as exc:
        # A leftover hold is cleaned up by the next booking attempt; the cancellation stands.
        _log_event("hold_release_failed", logging.ERROR, slot_id=item["slot_id"], error_type=type(exc).__name__)
    publish_event("AppointmentCancelled", {
        "appointment_id": item["appointment_id"], "patient_id": int(item["patient_id"]),
        "doctor_id": int(doctor_id), "slot_id": item["slot_id"],
        "consultation_type_id": int(item["consultation_type_id"]),
        "start_time_utc": item["start_time_utc"], "end_time_utc": item["end_time_utc"],
        "cancelled_by": "DOCTOR",
        "refund": {"tier": refund.tier, "percent": 100, "amount": float(refund.refund_amount)}})
    return True


CANCEL_SETTLE_ATTEMPTS = 5
CANCEL_SETTLE_PAUSE_SECONDS = 0.4


def _cancel_slot(conn, cursor, doctor_id, slot_id):
    """Cancel one slot: close it in RDS, then cancel and fully refund every confirmed appointment.

    Order matters. The slot is closed first (status CANCELLED, version + 1), so no new patient
    can claim a seat and any booking that read the old row loses its conditional update.
    Each CONFIRMED appointment (DynamoDB) then becomes CANCELLED_BY_DOCTOR with a 100% refund
    and a ledger row (RDS, one row per appointment, so nobody is refunded twice).

    DynamoDB index reads are eventually consistent, so a booking that was saved a moment ago
    may not show up in the first listing. The slot's booked_count is the exact number of
    appointments to expect: the listing is repeated a few times until all of them are
    accounted for. If some are still missing the answer says how many are pending and the
    doctor can press Cancel again. Every step is idempotent. No SlotSeatFreed event is sent:
    the slot is closed, so there is no seat for a waitlist to be promoted into.
    """
    slot = slot_store.get_slot(cursor, slot_id)
    conn.commit()
    if not slot or int(slot["doctor_id"]) != int(doctor_id):
        _log_event("slot_cancel_not_found", logging.WARNING, doctor_id=doctor_id, slot_id=slot_id)
        return response(404,{"message":"Slot not found"})
    if datetime.fromisoformat(slot["end_time_utc"].replace("Z", "+00:00")) <= datetime.now(timezone.utc):
        raise ValueError("This slot has already ended and can no longer be cancelled.")

    newly_cancelled, expected, pending = _cancel_slot_appointments(conn, cursor, doctor_id, slot_id)
    _log_event("slot_cancelled", logging.WARNING if pending else logging.INFO, doctor_id=doctor_id,
               slot_id=slot_id, affected_appointments=newly_cancelled, expected_appointments=expected,
               pending_appointments=pending)
    return response(200,{"message":"Slot cancelled" if not pending else
        "Slot cancelled. Some bookings are still being refunded; press Cancel again in a moment.",
        "slot_id":slot_id, "affected_appointments":newly_cancelled, "refunded_in_full":newly_cancelled,
        "pending_appointments":pending})


def _cancel_slot_appointments(conn, cursor, doctor_id, slot_id):
    """Close a slot and refund everyone in it. Returns (newly_cancelled, expected, pending).

    Shared by "Cancel slot" and by adding a blackout date. See _cancel_slot for the ordering rules.
    """
    slot_store.close_slot(cursor, slot_id)
    # Milestone 6: nobody can be promoted into a closed slot. End its waitlist and live offers.
    waitlist_store.close_waitlist(cursor, slot_id, datetime.now(timezone.utc).replace(tzinfo=None))
    conn.commit()
    expected = slot_store.get_slot(cursor, slot_id)["booked_count"]
    conn.commit()

    table = _appointments_table()
    newly_cancelled = 0
    accounted = 0
    for attempt in range(CANCEL_SETTLE_ATTEMPTS):
        items = store.list_for_slot(table, slot_id)
        for item in items:
            if item["status"] == store.CONFIRMED and _cancel_confirmed_by_doctor(table, cursor, conn, doctor_id, item):
                newly_cancelled += 1
                item = {**item, "status": store.CANCELLED_BY_DOCTOR}
                emit_metric("CancellationsByTier", 1, Tier="DOCTOR_CANCELLED")
            if item["status"] == store.CANCELLED_BY_DOCTOR:
                # Make sure the ledger row exists (also repairs a cancel that stopped half way).
                fresh = store.get_appointment(table, item["appointment_id"])
                record_refund(cursor, fresh, "DOCTOR_CANCELLATION", full_refund(fresh["fee_amount"]))
        conn.commit()
        accounted = sum(1 for i in items if i["status"] in (store.CANCELLED_BY_DOCTOR, store.COMPLETED,
                                                           store.NO_SHOW)
                        or (i["status"] == store.CANCELLED_BY_PATIENT and not i.get("seat_released_at")))
        if accounted >= expected:
            break
        _sleep(CANCEL_SETTLE_PAUSE_SECONDS)
    return newly_cancelled, expected, max(expected - accounted, 0)


NO_SHOW_TRIGGER = "NO_SHOW"
# Attributes a correction removes again when an appointment goes back to CONFIRMED / COMPLETED.
_OUTCOME_FIELDS = ("outcome", "refund_tier", "refund_percent", "refund_amount", "deduction_amount",
                   "hours_before_start")


def _cancel_one_appointment(conn, cursor, doctor_id, item):
    """The doctor cancels ONE appointment (the slot stays open for other patients).

    CONFIRMED, COMPLETED or NO_SHOW -> CANCELLED_BY_DOCTOR with a 100% refund, then: hold released,
    ledger row written (RDS; a no-show's 0% row is replaced), seat given back (RDS), events sent.
    Every step after the status change can be repeated, so pressing the button again finishes a cancel
    that stopped half way. Returns the updated appointment item.
    """
    table = _appointments_table()
    appointment_id = item["appointment_id"]
    if item["status"] in (store.CONFIRMED, store.COMPLETED, store.NO_SHOW):
        if item["status"] == store.CONFIRMED and item["end_time_utc"] <= store.now_iso():
            raise ValueError("This appointment has already ended. Mark it Completed or No-show instead.")
        refund = full_refund(item["fee_amount"])
        updated = store.transition(table, appointment_id, item["status"], store.CANCELLED_BY_DOCTOR, {
            "cancelled_at": store.now_iso(), "cancellation_reason": "Cancelled by the doctor",
            **refund_fields(refund)}, remove=("outcome",))
        if updated is None:
            raise ValueError("This appointment changed while you were working. Refresh and try again.")
        item = updated
        publish = True
    elif item["status"] == store.CANCELLED_BY_DOCTOR:
        publish = False
    else:
        raise ValueError(f"This appointment is {item['status']} and cannot be cancelled.")

    try:
        store.release_hold(table, item["slot_id"], int(item["patient_id"]))
    except Exception as exc:
        _log_event("hold_release_failed", logging.ERROR, slot_id=item["slot_id"], error_type=type(exc).__name__)
    delete_ledger_row(cursor, appointment_id, NO_SHOW_TRIGGER)  # a no-show's 0% row must not block the refund
    record_refund(cursor, item, "DOCTOR_CANCELLATION", full_refund(item["fee_amount"]))
    conn.commit()

    slot_after = None
    if not item.get("seat_released_at"):
        if slot_store.release_seat(cursor, item["slot_id"]):
            slot_after = slot_store.get_slot(cursor, item["slot_id"])
            store.set_fields(table, appointment_id, {"seat_released_at": store.now_iso()})
        conn.commit()
    if publish:
        emit_metric("CancellationsByTier", 1, Tier="DOCTOR_CANCELLED")
        publish_event("AppointmentCancelled", {
            "appointment_id": appointment_id, "patient_id": int(item["patient_id"]),
            "doctor_id": int(doctor_id), "slot_id": item["slot_id"],
            "consultation_type_id": int(item["consultation_type_id"]),
            "start_time_utc": item["start_time_utc"], "end_time_utc": item["end_time_utc"],
            "cancelled_by": "DOCTOR",
            "refund": {"tier": "DOCTOR_CANCELLED", "percent": 100, "amount": float(item["fee_amount"])}})
    if slot_after is not None and slot_after["status"] == "AVAILABLE":
        publish_event("SlotSeatFreed", {
            "slot_id": item["slot_id"], "doctor_id": int(doctor_id), "appointment_id": appointment_id,
            "consultation_type_id": int(item["consultation_type_id"]), "start_time_utc": item["start_time_utc"],
            "capacity": slot_after["capacity"], "booked_count": slot_after["booked_count"],
            "free_seats": slot_after["free_seats"]})
    _log_event("appointment_cancelled_by_doctor", doctor_id=doctor_id, appointment_id=appointment_id,
               seat_released=slot_after is not None)
    return store.get_appointment(table, appointment_id) or item


def _set_doctor_status(conn, cursor, doctor_id, item, target):
    """The doctor writes the status of one appointment (the dropdown). Returns the updated item.

    CONFIRMED, COMPLETED and NO_SHOW can be changed into each other (a wrong click is correctable),
    and into CANCELLED. A no-show carries a 0% ledger row, which is removed again when the status
    leaves NO_SHOW. A cancelled appointment is final: its refund was recorded and its seat was given back.
    """
    table = _appointments_table()
    appointment_id, current = item["appointment_id"], item["status"]
    if target == store.CANCELLED_BY_DOCTOR:
        return _cancel_one_appointment(conn, cursor, doctor_id, item)
    if current in store.CANCELLED_STATUSES:
        raise ValueError("A cancelled appointment is final: its refund has already been recorded.")
    if current == target:
        if target == store.NO_SHOW:  # repeated call: finish a ledger write that failed before
            record_refund(cursor, item, NO_SHOW_TRIGGER, no_show_refund(item["fee_amount"]))
            conn.commit()
        return item
    if target == store.NO_SHOW:
        refund = no_show_refund(item["fee_amount"])
        fields, remove = {"outcome": target, **refund_fields(refund)}, ()
    elif target == store.COMPLETED:
        fields, remove = {"outcome": target}, _OUTCOME_FIELDS[1:]
    else:  # back to CONFIRMED
        fields, remove = {}, _OUTCOME_FIELDS
    updated = store.transition(table, appointment_id, current, target, fields, remove)
    if updated is None:
        raise ValueError("This appointment changed while you were working. Refresh and try again.")
    if current == store.NO_SHOW:
        delete_ledger_row(cursor, appointment_id, NO_SHOW_TRIGGER)
        conn.commit()
    if target == store.NO_SHOW:
        record_refund(cursor, updated, NO_SHOW_TRIGGER, refund)  # a no-show is never refunded: audit row only
        conn.commit()
        emit_metric("CancellationsByTier", 1, Tier="NO_SHOW")
    _log_event("appointment_status_set", doctor_id=doctor_id, appointment_id=appointment_id,
               previous=current, status=target)
    return updated


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
    now_text = store.now_iso()
    upcoming = store.list_for_doctor(_appointments_table(), doctor_id, now_text[:10] + "T00:00:00Z", FAR_FUTURE_ISO)
    booked = sum(1 for i in upcoming if i.get("status") == store.CONFIRMED
                 and str(i.get("consultation_type_id")) == str(type_id) and i["end_time_utc"] > now_text)
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
    "PAST": "Past date. No appointments were held on this date.",
}


def _active_patterns(cursor, doctor_id):
    cursor.execute("""SELECT weekday,timezone_name,effective_from,effective_to FROM availability_patterns
        WHERE doctor_id=%s AND is_active=1""", (doctor_id,))
    return cursor.fetchall()


def _build_day(day, doctor_id, tz_name, local_today, patterns, blackout, day_slots, appointments):
    """One day of the schedule payload with an explicit status so the UI can explain an empty day."""
    if day < local_today:
        # Past days are history: only slots that held patients are listed (with their appointments),
        # so the doctor can review what was completed, missed or cancelled.
        past_slots = [s for s in day_slots if int(s.get("booked_count", 0)) > 0]
        past_ids = {s["slot_id"] for s in past_slots}
        return {"doctor_id": doctor_id, "date": day.isoformat(), "timezone": tz_name, "status": "PAST",
                "is_blackout": False, "blackout_reason": None,
                "message": "" if past_slots else SCHEDULE_MESSAGES["PAST"],
                "slots": past_slots, "appointments": [a for a in appointments if a["slot_id"] in past_ids]}
    # Withdrawn/blacked-out slots are never shown as bookable.
    slots = [s for s in day_slots if s.get("status") not in ("REMOVED", "BLACKED_OUT")]
    if blackout:
        # Keep only slots that already hold patients so they are not lost from view.
        slots = [s for s in day_slots if int(s.get("booked_count", 0)) > 0]
        status = "BLACKOUT"
    elif slots:
        status = "OK"
    elif not patterns:
        status = "NO_AVAILABILITY"
    else:
        applicable = any(int(p["weekday"]) == day.weekday() and day >= p["effective_from"]
                         and (p["effective_to"] is None or day <= p["effective_to"]) for p in patterns)
        status = "NOT_MATERIALISED" if applicable and day >= local_today else "NO_SLOTS"
    slot_ids = {s["slot_id"] for s in slots}
    return {"doctor_id": doctor_id, "date": day.isoformat(), "timezone": tz_name, "status": status,
            "is_blackout": bool(blackout), "blackout_reason": (blackout or {}).get("reason"),
            "message": SCHEDULE_MESSAGES[status], "slots": slots,
            "appointments": [a for a in appointments if a["slot_id"] in slot_ids]}


MAX_SCHEDULE_RANGE_DAYS = 14


def _schedule_for_range(cursor, conn, doctor_id, first_day, last_day):
    """Schedule for consecutive days (the week view): one entry per day, same shape as a single day.

    Slots come from slot_store.get_slots, the same function the patient's slot browser uses.
    """
    if last_day < first_day:
        raise ValueError("to must not be earlier than from")
    count = (last_day - first_day).days + 1
    if count > MAX_SCHEDULE_RANGE_DAYS:
        raise ValueError(f"The schedule range can cover at most {MAX_SCHEDULE_RANGE_DAYS} days")
    patterns = _active_patterns(cursor, doctor_id)
    tz_name = patterns[0]["timezone_name"] if patterns else "Asia/Kolkata"
    local_today = datetime.now(ZoneInfo(tz_name)).date()
    shown_first = first_day  # past days are looked up too: they show what was completed
    slots_by_day, blackouts, appointments = {}, {}, []
    if shown_first <= last_day:
        for slot in slot_store.get_slots(cursor, doctor_id, shown_first, last_day, slot_store.VIEWER_DOCTOR):
            slots_by_day.setdefault(slot["slot_date"], []).append(slot)
        cursor.execute("SELECT blackout_date,reason FROM doctor_blackouts WHERE doctor_id=%s "
                       "AND blackout_date BETWEEN %s AND %s", (doctor_id, shown_first, last_day))
        blackouts = {r["blackout_date"]: r for r in cursor.fetchall()}
        booked_ids = {s["slot_id"] for day_slots in slots_by_day.values() for s in day_slots}
        if booked_ids:
            window_start = (datetime.combine(shown_first, time.min) - timedelta(days=1))
            window_end = (datetime.combine(last_day, time.min) + timedelta(days=2))
            items = store.list_for_doctor(_appointments_table(), doctor_id,
                                          slot_store.iso_utc(window_start), slot_store.iso_utc(window_end))
            appointments = _doctor_appointment_rows(cursor, [i for i in items if i["slot_id"] in booked_ids])
    conn.commit()
    days = [_build_day(first_day + timedelta(days=i), doctor_id, tz_name, local_today, patterns,
                       blackouts.get(first_day + timedelta(days=i)),
                       slots_by_day.get((first_day + timedelta(days=i)).isoformat(), []), appointments)
            for i in range(count)]
    return {"doctor_id": doctor_id, "from": first_day.isoformat(), "to": last_day.isoformat(),
            "timezone": tz_name, "days": days,
            "summary": {"slots": sum(len(d["slots"]) for d in days),
                        "appointments": sum(len(d["appointments"]) for d in days),
                        "blackout_days": sum(1 for d in days if d["is_blackout"])}}


def _schedule_for_day(cursor, conn, doctor_id, day, patterns=None):
    """A single day: the one-day case of the same range function."""
    return _schedule_for_range(cursor, conn, doctor_id, day, day)["days"][0]


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
                key = f"doctor-photos/{doctor_id}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.{ext}"
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
                # Check first: a rejected INSERT would still use up an AUTO_INCREMENT id.
                cur.execute("SELECT 1 FROM doctor_blackouts WHERE doctor_id=%s AND blackout_date=%s",(doctor_id,blackout_date))
                if cur.fetchone():
                    _log_event("blackout_duplicate", logging.INFO, doctor_id=doctor_id,
                               blackout_date=blackout_date.isoformat())
                    return response(409,{"message":"The requested record conflicts with existing data"})
                cur.execute("INSERT INTO doctor_blackouts (doctor_id,blackout_date,reason) VALUES (%s,%s,%s)",(doctor_id,blackout_date,reason))
                blackout_id = cur.lastrowid
                conn.commit()
                # The day is now closed. Slots nobody booked are deleted; slots that hold patients are
                # cancelled and every patient is refunded in full (same rules as "Cancel slot").
                deleted = slot_store.delete_empty_slots_on_day(cur, doctor_id, blackout_date)
                conn.commit()
                booked = slot_store.booked_slots_on_day(cur, doctor_id, blackout_date)
                conn.commit()
                now_text = store.now_iso()
                slots_cancelled = appointments_cancelled = pending = 0
                for slot_id, end_utc in booked:
                    if slot_store.iso_utc(end_utc) <= now_text:
                        continue  # already over: it stays as history
                    done, _expected, waiting = _cancel_slot_appointments(conn, cur, doctor_id, slot_id)
                    slots_cancelled += 1
                    appointments_cancelled += done
                    pending += waiting
                _log_event("blackout_created", logging.WARNING if slots_cancelled else logging.INFO,
                           doctor_id=doctor_id, blackout_id=blackout_id,
                           blackout_date=blackout_date.isoformat(), slots_deleted=deleted,
                           slots_cancelled=slots_cancelled, appointments_cancelled=appointments_cancelled,
                           pending_appointments=pending)
                return response(201,{"message":"Blackout date created","blackout_id":blackout_id,
                    "slots_deleted":deleted,"slots_cancelled":slots_cancelled,
                    "appointments_cancelled":appointments_cancelled,"pending_appointments":pending})
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
                status = (query.get("status") or "").upper()
                if status and status not in APPOINTMENT_STATUSES:
                    raise ValueError("status must be one of " + ", ".join(sorted(APPOINTMENT_STATUSES)))
                start_text, end_text = slot_store.iso_utc(start), slot_store.iso_utc(end)
                items = [i for i in store.list_for_doctor(_appointments_table(), doctor_id, start_text, end_text)
                         if i["start_time_utc"] < end_text and store.status_matches(i["status"], status)]
                rows = _doctor_appointment_rows(cur, items)
                conn.commit()
                return response(200,{"appointments":rows})

            match = re.fullmatch(r"/appointments/(\d+)/outcome",path)
            if match and method == "POST":
                outcome = str(body.get("outcome") or body.get("status") or "").upper()
                if outcome == "CANCELLED":
                    outcome = store.CANCELLED_BY_DOCTOR
                if outcome not in {store.CONFIRMED, store.COMPLETED, store.NO_SHOW, store.CANCELLED_BY_DOCTOR}:
                    raise ValueError("status must be CONFIRMED, COMPLETED, NO_SHOW or CANCELLED")
                appointment_id = match.group(1)
                table = _appointments_table()
                item = store.get_appointment(table, appointment_id)
                if not item or str(item.get("doctor_id")) != str(doctor_id):
                    _log_event("appointment_outcome_rejected", logging.WARNING, doctor_id=doctor_id,
                               appointment_id=appointment_id, outcome=outcome)
                    return response(404,{"message":"Appointment not found"})
                updated = _set_doctor_status(conn, cur, doctor_id, item, outcome)
                return response(200,{"message":("Appointment cancelled and refunded in full"
                                                if outcome == store.CANCELLED_BY_DOCTOR
                                                else f"Appointment marked {outcome}"),
                                     "appointment_id":appointment_id,"status":updated["status"]})

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
        _log_event("doctor_api_integrity_conflict", logging.WARNING,
                   request_id=getattr(context, "aws_request_id", None), method=method, path=path,
                   mysql_error_code=exc.args[0] if exc.args else None)
        return response(409,{"message":"The requested record conflicts with existing data"})
    except Exception:
        if conn: conn.rollback()
        logger.exception(json.dumps({"event": "doctor_api_failed",
                                     "request_id": getattr(context, "aws_request_id", None),
                                     "method": method, "path": path}))
        return response(500,{"message":"Doctor domain request failed"})
    finally:
        if conn: conn.close()


def materialize_doctor(doctor_id, start_day=None, days=30):
    """Create/refresh this doctor's slots (RDS) for the rolling window starting at start_day.

    The work is done by shared/slot_generation.py, the same code the scheduled
    slot-materializer Lambda runs.
    """
    conn = db_connect()
    try:
        return materialize_doctor_slots(conn, doctor_id, start_day, days)
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


NO_SHOW_GRACE_MINUTES = 30
NO_SHOW_LOOKBACK_DAYS = 7


def mark_no_shows(now=None):
    """Mark every appointment still CONFIRMED 30 minutes after its slot end as NO_SHOW.

    Uses the doctor's own NO_SHOW rule (_set_doctor_status): conditional status change plus the
    0% ledger row. Running it twice changes nothing the second time.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = (now - timedelta(minutes=NO_SHOW_GRACE_MINUTES)).replace(microsecond=0)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    first_iso = (cutoff - timedelta(days=NO_SHOW_LOOKBACK_DAYS)).isoformat().replace("+00:00", "Z")
    table = _appointments_table()
    candidates = [i for i in store.list_by_status(table, store.CONFIRMED, first_iso, cutoff_iso)
                  if str(i.get("end_time_utc", "")) <= cutoff_iso]
    marked = 0
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            for item in candidates:
                try:
                    _set_doctor_status(conn, cur, int(item["doctor_id"]), item, store.NO_SHOW)
                    marked += 1
                except ValueError as exc:  # changed meanwhile (the doctor marked it, a cancel, ...)
                    _log_event("no_show_skipped", logging.WARNING, appointment_id=item["appointment_id"],
                               reason=str(exc)[:120])
    finally:
        conn.close()
    if marked:
        emit_metric("NoShowsMarked", marked)
    return {"candidates": len(candidates), "marked_no_show": marked}


def no_show_sweeper_handler(event, context):
    """EventBridge Scheduler entry point (every 15 minutes)."""
    summary = mark_no_shows()
    _log_event("no_show_sweep_completed", **summary)
    return summary


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