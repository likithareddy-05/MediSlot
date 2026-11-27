"""MediSlot booking engine (Milestone 5): book, list, cancel, refunds.

Where the data lives (as the mentor suggested)
  * SLOTS are in RDS MySQL (`slots` table, see shared/slot_store.py): capacity, booked_count,
    version, status.
  * APPOINTMENTS are in DynamoDB (see shared/appointment_store.py).
  * The refund LEDGER is in RDS (`refund_ledger`), the idempotency records are in DynamoDB.

Routes (PATIENT only; the caller is ALWAYS the authorizer's userId, never an id from the
URL, query string or body, so a patient can only ever see or change their own data)

  POST /appointments                    book one seat in a slot (Idempotency-Key header required)
  GET  /appointments                    own appointments (?status=&upcoming=true&page=&page_size=)
  GET  /appointments/{id}               one own appointment, with a refund preview when cancellable
  POST /appointments/{id}/cancel        cancel; refund tier comes from the SSM refund policy
  DELETE /appointments/{id}             the same cancel (Milestone 6 route name); returns the refund
  PATCH /appointments/{id}              reschedule to {"target_slot_id": ...} (Step Functions saga)
  GET  /patients/me/refunds             own refund ledger entries and their total
  POST/DELETE /slots/{slotId}/waitlist  join / leave a waitlist      (see waitlist_domain.py)
  GET  /patients/me/waitlists           active waitlists and pending hold offers
  POST /waitlist-offers/{id}/accept     accept a hold offer

How a booking stays correct under contention (optimistic concurrency control)
  * A seat is claimed with ONE conditional UPDATE on the slot row:
        SET booked_count = booked_count + 1, version = version + 1
        WHERE status = 'AVAILABLE' AND booked_count < capacity AND version = <version we read>
    Two requests holding the same version cannot both win and booked_count can never pass
    capacity: exactly `capacity` bookings succeed, however many arrive at once.
  * A request that loses the race re-reads the slot. If it is now full the answer is
    409 SLOT_FULL. Otherwise it retries after a jittered exponential backoff, at most
    `max-conflict-retries` times (SSM), then answers 409 BOOKING_CONTENTION.
  * The number of lost races is published as the BookingConflictRetries metric.
  * A patient's own bookings are serialised with a row lock on the patient (RDS), and a HOLD
    item in the appointments table (DynamoDB conditional put) allows one active appointment
    per patient per slot.

Order of the steps of a booking
  idempotency key -> lock patient -> read and validate slot -> hold -> doctor limit -> fee
  -> claim seat (OCC) -> save appointment -> re-check the slot -> event.
Any failure after the seat was claimed gives the seat and the hold back.

Cancellation order: the appointment is moved CONFIRMED -> CANCELLED_BY_PATIENT with one
conditional write that also stores the refund result. After that, in this order: delete the
hold, write the ledger row (idempotent), give the seat back, publish events. DynamoDB and
RDS cannot share a transaction, so every step after the status change is idempotent and the
cancel call can simply be repeated to finish a cancellation that was interrupted.
"""
import hashlib
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from urllib.parse import unquote
from decimal import Decimal
from time import monotonic as _monotonic

import boto3

import appointment_store as store
import reschedule_rules
import slot_store
import waitlist_domain
from common import (ApiError, db_connect, error_response, get_config_value, is_transient_db_error,
                    json_body, log_event, logger, response)
from events import emit_metric, publish_event
from refunds import (DEFAULT_POLICY, PolicyError, calculate_refund, full_refund, money, parse_policy,
                     record_refund, refund_fields, result_from_item)

ddb = boto3.resource("dynamodb")

APPOINTMENTS_TABLE_NAME = os.environ["APPOINTMENTS_TABLE_NAME"]
IDEMPOTENCY_TABLE_NAME = os.environ["IDEMPOTENCY_TABLE_NAME"]

DEFAULT_MAX_UPCOMING_PER_DOCTOR = 3
DEFAULT_MAX_CONFLICT_RETRIES = 6
BACKOFF_BASE_SECONDS = 0.02
BACKOFF_CAP_SECONDS = 0.4
IDEMPOTENCY_TTL_SECONDS = 24 * 3600
IN_PROGRESS_STALE_SECONDS = 120
DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 50

KEY_RE = re.compile(r"[A-Za-z0-9._:-]{8,128}")
ID_RE = r"\d+"
CONDITIONAL_FAILED = "ConditionalCheckFailedException"


def _error_response(exc):
    return error_response(exc)


# ---------------------------------------------------------------------------
# Small helpers (module level so tests can replace them)
# ---------------------------------------------------------------------------

def _utcnow():
    return datetime.now(timezone.utc)


def _sleep(seconds):
    time.sleep(seconds)


def _appointments_table():
    return ddb.Table(APPOINTMENTS_TABLE_NAME)


def _idempotency_table():
    return ddb.Table(IDEMPOTENCY_TABLE_NAME)


def _error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


def _backoff_seconds(retry_number):
    """Full-jitter exponential backoff: random between 0 and min(cap, base * 2^n)."""
    return random.uniform(0, min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** retry_number)))


def _int_setting(env_name, default):
    raw = get_config_value(os.environ.get(env_name), None)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except ValueError:
        logger.warning(json.dumps({"event": "setting_not_a_number", "setting": env_name}))
        return default
    return value if value > 0 else default


def _max_upcoming_per_doctor():
    return _int_setting("MAX_UPCOMING_PARAMETER", DEFAULT_MAX_UPCOMING_PER_DOCTOR)


def _max_conflict_retries():
    return _int_setting("MAX_RETRIES_PARAMETER", DEFAULT_MAX_CONFLICT_RETRIES)


def _refund_policy():
    raw = get_config_value(os.environ.get("REFUND_POLICY_PARAMETER"), None)
    if raw is None:
        return parse_policy(DEFAULT_POLICY)
    try:
        return parse_policy(raw)
    except PolicyError as exc:
        # A broken policy in Parameter Store must not make cancellations fail or pay out wrongly.
        logger.error(json.dumps({"event": "refund_policy_invalid_using_default", "error": str(exc)[:200]}))
        return parse_policy(DEFAULT_POLICY)


def _parse_instant(text):
    return datetime.fromisoformat(str(text).replace("Z", "+00:00"))


def _naive_utc(value):
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value


def _iso(value):
    """Naive-UTC datetime (as stored in MySQL) or aware datetime -> 2026-11-12T03:30:00Z."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return slot_store.iso_utc(slot_store.to_utc_naive(value))


def _amount(value):
    return float(money(value)) if value is not None else 0.0


# ---------------------------------------------------------------------------
# Idempotency store (DynamoDB)
# ---------------------------------------------------------------------------

def _idempotency_key(event):
    headers = {str(k).lower(): v for k, v in (event.get("headers") or {}).items()}
    key = headers.get("idempotency-key")
    if not key:
        raise ApiError(400, "IDEMPOTENCY_KEY_REQUIRED", "The Idempotency-Key header is required.")
    key = key.strip()
    if not KEY_RE.fullmatch(key):
        raise ApiError(400, "IDEMPOTENCY_KEY_INVALID",
                       "Idempotency-Key must be 8-128 characters: letters, digits, . _ : -")
    return key


def _idem_begin(store_key, request_hash):
    """Claim the key. Returns None for a first request, or the stored item to replay."""
    table = _idempotency_table()
    for _ in range(3):
        now = int(time.time())
        try:
            table.put_item(
                Item={"idempotency_key": store_key, "request_hash": request_hash, "state": "IN_PROGRESS",
                      "started_at": now, "expires_at": now + IDEMPOTENCY_TTL_SECONDS},
                ConditionExpression="attribute_not_exists(idempotency_key)")
            return None
        except Exception as exc:
            if _error_code(exc) != CONDITIONAL_FAILED:
                logger.error(json.dumps({"event": "idempotency_claim_failed", "error_type": type(exc).__name__,
                                         "error_code": _error_code(exc)}))
                raise
            logger.info(json.dumps({"event": "idempotency_key_exists"}))
        existing = table.get_item(Key={"idempotency_key": store_key}, ConsistentRead=True).get("Item")
        if not existing:
            continue  # deleted between our two calls: try to claim again
        if existing.get("request_hash") != request_hash:
            raise ApiError(422, "IDEMPOTENCY_KEY_REUSED",
                           "This Idempotency-Key was already used with a different request.")
        if existing.get("state") == "COMPLETED":
            return existing
        if now - int(existing.get("started_at") or 0) > IN_PROGRESS_STALE_SECONDS:
            try:  # the first request died without finishing: take the key over
                table.update_item(
                    Key={"idempotency_key": store_key},
                    UpdateExpression="SET started_at = :now",
                    ConditionExpression="#st = :ip AND started_at = :old",
                    ExpressionAttributeNames={"#st": "state"},
                    ExpressionAttributeValues={":now": now, ":ip": "IN_PROGRESS", ":old": existing["started_at"]})
                logger.warning(json.dumps({"event": "idempotency_key_taken_over"}))
                return None
            except Exception as exc:
                if _error_code(exc) != CONDITIONAL_FAILED:
                    logger.error(json.dumps({"event": "idempotency_takeover_failed",
                                             "error_type": type(exc).__name__}))
                    raise
                logger.info(json.dumps({"event": "idempotency_takeover_lost"}))
        raise ApiError(409, "REQUEST_IN_PROGRESS",
                       "A request with this Idempotency-Key is still being processed. Retry shortly.")
    raise ApiError(409, "REQUEST_IN_PROGRESS", "Could not claim the Idempotency-Key. Retry shortly.")


def _idem_complete(store_key, status_code, body):
    _idempotency_table().update_item(
        Key={"idempotency_key": store_key},
        UpdateExpression="SET #st = :done, response_status = :rs, response_body = :rb",
        ExpressionAttributeNames={"#st": "state"},
        ExpressionAttributeValues={":done": "COMPLETED", ":rs": status_code,
                                   ":rb": json.dumps(body, default=str)})


def _idem_abort(store_key):
    """The request did not produce an appointment: free the key so the client can try again."""
    try:
        _idempotency_table().delete_item(Key={"idempotency_key": store_key})
    except Exception as exc:
        logger.error(json.dumps({"event": "idempotency_release_failed", "error_type": type(exc).__name__,
                                 "error": str(exc)[:200]}))


# ---------------------------------------------------------------------------
# Appointment views
# ---------------------------------------------------------------------------

def _view(item, preview=None):
    refund = None
    if item.get("refund_tier") is not None:
        refund = {"tier": item["refund_tier"], "percent": float(Decimal(str(item["refund_percent"]))),
                  "amount": _amount(item.get("refund_amount")), "deduction": _amount(item.get("deduction_amount")),
                  "currency": item.get("currency") or "INR"}
    out = {
        "appointment_id": item["appointment_id"],
        "status": item["status"],
        "slot_id": item["slot_id"],
        "doctor_id": int(item["doctor_id"]),
        "doctor_name": item.get("doctor_name") or "",
        "consultation_type_id": int(item["consultation_type_id"]),
        "consultation_name": item.get("consultation_name") or "",
        "start_time_utc": item["start_time_utc"],
        "end_time_utc": item["end_time_utc"],
        "fee": _amount(item.get("fee_amount")),
        "booked_at": item.get("created_at"),
        "cancelled_at": item.get("cancelled_at"),
        "cancellation_reason": item.get("cancellation_reason"),
        "refund": refund,
        "reschedule_count": int(item.get("reschedule_count") or 0),
        "previous_slot_id": item.get("previous_slot_id"),
        "booked_via": item.get("booked_via") or "DIRECT",
    }
    if preview is not None:
        out["cancellation_preview"] = preview
    return out


def _own_appointment(table, appointment_id, patient_id):
    """The appointment, only when it belongs to this patient (otherwise it looks like it does not exist)."""
    item = store.get_appointment(table, appointment_id)
    if not item or str(item.get("patient_id")) != str(patient_id):
        raise ApiError(404, "APPOINTMENT_NOT_FOUND", "Appointment not found.")
    return item


def _cancellation_preview(item):
    """What cancelling right now would refund (shown to the patient before they confirm)."""
    now = _utcnow()
    start = _parse_instant(item["start_time_utc"])
    if item["status"] != store.CONFIRMED or start <= now:
        return None
    result = calculate_refund(item["fee_amount"], _naive_utc(start), _naive_utc(now), _refund_policy())
    return {"tier": result.tier, "refund_percent": float(result.percent), "refund_amount": _amount(result.refund_amount),
            "deduction": _amount(result.deduction_amount), "hours_before_start": float(result.hours_before_start)}


def _page_args(query):
    try:
        page = int(query.get("page") or 1)
        size = int(query.get("page_size") or query.get("pageSize") or DEFAULT_PAGE_SIZE)
    except ValueError as exc:
        logger.warning(json.dumps({"event": "page_arguments_invalid", "error": str(exc)[:100]}))
        raise ApiError(400, "VALIDATION", "page and page_size must be whole numbers.")
    if page < 1 or not 1 <= size <= MAX_PAGE_SIZE:
        raise ApiError(400, "VALIDATION", f"page must be 1 or more and page_size between 1 and {MAX_PAGE_SIZE}.")
    return page, size


def _list_appointments(table, patient_id, query):
    page, size = _page_args(query)
    status = (query.get("status") or "").upper()
    if status and status not in store.STATUS_FILTERS:
        raise ApiError(400, "VALIDATION", "status must be one of " + ", ".join(sorted(store.STATUS_FILTERS)))
    upcoming = str(query.get("upcoming") or "").lower() in ("1", "true", "yes")
    now_text = store.now_iso()
    items = store.list_for_patient(table, patient_id, newest_first=not upcoming)
    if status:
        items = [i for i in items if store.status_matches(i.get("status"), status)]
    if upcoming:
        items = [i for i in items if i.get("status") == store.CONFIRMED and i["start_time_utc"] > now_text]
    total = len(items)
    chosen = items[(page - 1) * size: page * size]
    return {"appointments": [_view(i) for i in chosen],
            "pagination": {"page": page, "page_size": size, "total": total,
                           "total_pages": max(1, -(-total // size))}}


def _list_refunds(cursor, patient_id, query):
    page, size = _page_args(query)
    cursor.execute("SELECT COUNT(*) AS n, COALESCE(SUM(refund_amount), 0) AS total FROM refund_ledger "
                   "WHERE patient_id = %s", (patient_id,))
    summary = cursor.fetchone() or {}
    total = int(summary.get("n") or 0)
    cursor.execute("""SELECT r.refund_id, r.appointment_id, r.trigger_type, r.tier, r.refund_percent,
        r.fee_amount, r.refund_amount, r.deduction_amount, r.hours_before_start, r.currency, r.created_at,
        r.slot_id, r.scheduled_start_utc, d.first_name AS doctor_first_name, d.last_name AS doctor_last_name
        FROM refund_ledger r JOIN doctors d ON d.user_id = r.doctor_id
        WHERE r.patient_id = %s ORDER BY r.created_at DESC, r.refund_id DESC LIMIT %s OFFSET %s""",
                   (patient_id, size, (page - 1) * size))
    entries = [{
        "refund_id": int(r["refund_id"]), "appointment_id": r["appointment_id"],
        "trigger": r["trigger_type"], "tier": r["tier"], "percent": float(r["refund_percent"]),
        "fee": _amount(r["fee_amount"]), "refund_amount": _amount(r["refund_amount"]),
        "deduction": _amount(r["deduction_amount"]), "currency": r["currency"],
        "doctor_name": f"{r['doctor_first_name']} {r['doctor_last_name']}",
        "appointment_start_utc": _iso(r["scheduled_start_utc"]), "recorded_at": _iso(r["created_at"]),
    } for r in cursor.fetchall()]
    return {"refunds": entries, "total_refunded": _amount(summary.get("total")),
            "note": "Refunds are recorded in a ledger only. No payment is made or taken.",
            "pagination": {"page": page, "page_size": size, "total": total,
                           "total_pages": max(1, -(-total // size))}}


def _event_detail(item):
    return {"appointment_id": item["appointment_id"], "patient_id": int(item["patient_id"]),
            "doctor_id": int(item["doctor_id"]), "slot_id": item["slot_id"],
            "consultation_type_id": int(item["consultation_type_id"]),
            "start_time_utc": item["start_time_utc"], "end_time_utc": item["end_time_utc"],
            "fee": _amount(item.get("fee_amount"))}


# ---------------------------------------------------------------------------
# Slot access with the RDS connection (OCC)
# ---------------------------------------------------------------------------

def _read_slot(slot_conn, slot_id):
    """Read the slot on a fresh snapshot (ends the read transaction so the next read sees new data)."""
    with slot_conn.cursor() as cur:
        slot = slot_store.get_slot(cur, slot_id)
    slot_conn.commit()
    return slot


def _check_bookable(slot, now):
    """Raise the right ApiError unless the slot can take a booking right now."""
    if not slot:
        raise ApiError(404, "SLOT_NOT_FOUND", "This slot does not exist.")
    if slot["status"] != "AVAILABLE":
        raise ApiError(409, "SLOT_UNAVAILABLE", "This slot is no longer available.")
    if _parse_instant(slot["start_time_utc"]) <= now:
        raise ApiError(400, "SLOT_IN_PAST", "This slot has already started and can no longer be booked.")
    if slot["free_seats"] <= 0:  # seats held for a waitlist offer count as taken
        raise ApiError(409, "SLOT_FULL", "This slot is full.")


def _claim_seat(slot_conn, slot, stats):
    """Take one seat with the version-checked UPDATE; retry with backoff on a lost race."""
    slot_id = slot["slot_id"]
    max_retries = _max_conflict_retries()
    while True:
        with slot_conn.cursor() as cur:
            won = slot_store.claim_seat(cur, slot_id, slot["version"])
        slot_conn.commit()
        if won:
            return
        # Lost the race (somebody else changed the slot). Re-read: "now full" becomes SLOT_FULL,
        # otherwise back off and try again with the new version.
        stats["retries"] += 1
        slot = _read_slot(slot_conn, slot_id)
        _check_bookable(slot, _utcnow())
        if stats["retries"] > max_retries:
            raise ApiError(409, "BOOKING_CONTENTION",
                           "This slot is very busy right now. Please try again in a moment.")
        _sleep(_backoff_seconds(stats["retries"]))


def _release_seat(slot_conn, slot_id):
    """Give one seat back. Returns the slot after the release, or None when it could not be released."""
    try:
        with slot_conn.cursor() as cur:
            released = slot_store.release_seat(cur, slot_id)
        slot_conn.commit()
        return _read_slot(slot_conn, slot_id) if released else None
    except Exception as exc:
        logger.error(json.dumps({"event": "seat_release_failed", "slot_id": slot_id,
                                 "error_type": type(exc).__name__, "error": str(exc)[:200]}))
        return None


def _write_ledger(item, trigger_type, result, currency="INR"):
    """Insert the refund ledger row (RDS). Repeating it is harmless."""
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            record_refund(cur, item, trigger_type, result, currency)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Booking
# ---------------------------------------------------------------------------

def _book_once(patient_id, slot_id, idempotency_key):
    """Validate, hold, claim a seat, persist the appointment. Returns (view, replayed)."""
    table = _appointments_table()
    patient_conn, slot_conn = db_connect(), db_connect()
    stats = {"retries": 0}
    claimed = held = False
    try:
        # Lock this patient's row: their own concurrent bookings queue up here, so the
        # "at most 3 upcoming with one doctor" count below cannot be raced by themselves.
        with patient_conn.cursor() as cur:
            cur.execute("""SELECT p.user_id FROM patients p JOIN users u ON u.user_id = p.user_id
                WHERE p.user_id = %s AND u.role = 'PATIENT' AND u.account_status = 'ACTIVE' FOR UPDATE OF p""",
                        (patient_id,))
            if not cur.fetchone():
                raise ApiError(409, "PROFILE_REQUIRED", "Create your patient profile before booking.")

        slot = _read_slot(slot_conn, slot_id)
        _check_bookable(slot, _utcnow())
        doctor_id, ctype_id = slot["doctor_id"], slot["consultation_type_id"]

        appointment_id = store.new_appointment_id(table)  # only after the cheap checks, so rejected requests burn no numbers
        won, holder = store.acquire_hold(table, slot_id, patient_id, appointment_id, idempotency_key)
        if not won:
            # Same key seen before (e.g. the idempotency record expired): hand back that appointment.
            if holder and holder.get("idempotency_key") == idempotency_key and holder.get("status") == store.CONFIRMED:
                return _view(holder), True
            if holder and holder.get("status") == "IN_PROGRESS":
                raise ApiError(409, "REQUEST_IN_PROGRESS", "A booking for this slot is being processed.")
            raise ApiError(409, "ALREADY_BOOKED", "You already have an appointment in this slot.")
        held = True

        limit = _max_upcoming_per_doctor()
        if store.count_upcoming_with_doctor(table, patient_id, doctor_id) >= limit:
            raise ApiError(409, "DOCTOR_LIMIT_REACHED",
                           f"You can hold at most {limit} upcoming appointments with the same doctor.")

        with slot_conn.cursor() as cur:
            cur.execute("""SELECT dct.fee, doc.first_name, doc.last_name FROM doctor_consultation_types dct
                JOIN users u ON u.user_id = dct.doctor_id JOIN doctors doc ON doc.user_id = dct.doctor_id
                WHERE dct.doctor_id = %s AND dct.consultation_type_id = %s AND dct.is_active = 1
                AND u.account_status = 'ACTIVE'""", (doctor_id, ctype_id))
            fee_row = cur.fetchone()
        slot_conn.commit()
        if not fee_row:
            raise ApiError(409, "SLOT_UNAVAILABLE", "This doctor is not offering this consultation any more.")

        _claim_seat(slot_conn, slot, stats)
        claimed = True

        now_text = store.now_iso()
        item = {
            "appointment_id": appointment_id, "patient_id": str(patient_id), "doctor_id": str(doctor_id),
            "slot_id": slot_id, "consultation_type_id": str(ctype_id),
            "consultation_name": slot["consultation_name"],
            "doctor_name": f"{fee_row['first_name']} {fee_row['last_name']}",
            "status": store.CONFIRMED, "start_time_utc": slot["start_time_utc"],
            "end_time_utc": slot["end_time_utc"], "fee_amount": str(money(fee_row["fee"])),
            "idempotency_key": idempotency_key, "created_at": now_text, "updated_at": now_text,
        }
        store.put_appointment(table, item)
        claimed = held = False  # the appointment exists now; later failures do not give the seat back

        _close_race_with_doctor_cancel(table, slot_conn, item)
        publish_event("AppointmentBooked", _event_detail(item))
        log_event("appointment_booked", patient_id=patient_id, appointment_id=appointment_id, slot_id=slot_id,
                  conflict_retries=stats["retries"])
        return _view(item), False
    except BaseException as exc:
        logger.warning(json.dumps({"event": "booking_not_completed", "slot_id": slot_id, "patient_id": patient_id,
                                   "error_type": type(exc).__name__, "claimed": claimed}))
        if claimed:
            # The seat was taken but no appointment was saved: hand it back.
            if _release_seat(slot_conn, slot_id) is None:
                logger.error(json.dumps({"event": "seat_release_failed_after_booking_error", "slot_id": slot_id}))
                emit_metric("SeatReleaseFailures", 1)
        if held:
            _release_hold_quietly(table, slot_id, patient_id)
        for conn in (patient_conn, slot_conn):
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.warning(json.dumps({"event": "rollback_failed", "error": str(rollback_error)[:200]}))
        raise
    finally:
        emit_metric("BookingConflictRetries", stats["retries"])
        patient_conn.close()
        slot_conn.close()


def _release_hold_quietly(table, slot_id, patient_id):
    try:
        store.release_hold(table, slot_id, patient_id)
    except Exception as exc:
        logger.error(json.dumps({"event": "hold_release_failed", "slot_id": slot_id, "patient_id": patient_id,
                                 "error_type": type(exc).__name__}))


def _close_race_with_doctor_cancel(table, slot_conn, item):
    """A doctor may cancel the slot after we claimed the seat but before the appointment was saved.

    The doctor's cancellation only refunds appointments it can see, so such a late
    appointment would be stranded on a closed slot. Re-check the slot after saving and,
    if it was closed meanwhile, cancel this appointment as doctor-cancelled with a full refund.
    """
    slot = _read_slot(slot_conn, item["slot_id"])
    if slot and slot["status"] == "AVAILABLE":
        return
    result = full_refund(item["fee_amount"])
    updated = store.transition(table, item["appointment_id"], store.CONFIRMED, store.CANCELLED_BY_DOCTOR, {
        "cancelled_at": store.now_iso(), "seat_released_at": store.now_iso(),
        "cancellation_reason": "Slot cancelled by the doctor", **refund_fields(result)})
    if updated is not None:
        _release_hold_quietly(table, item["slot_id"], int(item["patient_id"]))
        _write_ledger(updated, "DOCTOR_CANCELLATION", result)
        publish_event("AppointmentCancelled", {**_event_detail(updated), "cancelled_by": "DOCTOR", "refund": {
            "tier": result.tier, "percent": 100, "amount": _amount(result.refund_amount)}})
        emit_metric("CancellationsByTier", 1, Tier=result.tier)
    raise ApiError(409, "SLOT_CANCELLED", "The doctor cancelled this slot just now. You have not been charged.")


def book(event, patient_id):
    key = _idempotency_key(event)
    body = json_body(event)
    slot_id = body.get("slot_id")
    if not isinstance(slot_id, str) or not slot_id.strip() or len(slot_id) > 128:
        raise ApiError(400, "VALIDATION", "slot_id is required.")
    slot_id = slot_id.strip()
    request_hash = hashlib.sha256(json.dumps({"slot_id": slot_id}, sort_keys=True).encode()).hexdigest()
    store_key = f"{patient_id}#{key}"

    replay = _idem_begin(store_key, request_hash)
    if replay:
        log_event("booking_replayed", patient_id=patient_id)
        result = response(int(replay["response_status"]), json.loads(replay["response_body"]))
        result["headers"]["Idempotent-Replayed"] = "true"
        return result
    try:
        appointment, replayed = _book_once(patient_id, slot_id, key)
    except BaseException as exc:
        logger.warning(json.dumps({"event": "booking_failed_releasing_idempotency_key",
                                   "error_type": type(exc).__name__}))
        _idem_abort(store_key)
        raise
    body = {"message": "Appointment confirmed", "appointment": appointment}
    status_code = 200 if replayed else 201
    _idem_complete(store_key, status_code, body)
    return response(status_code, body)


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------

def finish_cancellation(table, item, trigger_type, publish):
    """Everything after the status change of a cancelled appointment. Every step is repeatable.

    hold -> ledger -> seat -> events. Returns the slot after the seat was given back (or None).
    """
    result = result_from_item(item)
    _release_hold_quietly(table, item["slot_id"], int(item["patient_id"]))
    _write_ledger(item, trigger_type, result, item.get("currency") or "INR")
    slot_after = None
    if not item.get("seat_released_at"):
        slot_conn = db_connect()
        try:
            slot_after = _release_seat(slot_conn, item["slot_id"])
        finally:
            slot_conn.close()
        if slot_after is None:
            logger.error(json.dumps({"event": "seat_release_incomplete", "slot_id": item["slot_id"],
                                     "appointment_id": item["appointment_id"]}))
            emit_metric("SeatReleaseFailures", 1)
        else:
            store.set_fields(table, item["appointment_id"], {"seat_released_at": store.now_iso()})
    if publish:
        publish_cancellation_events(item, result, slot_after, "PATIENT")
    elif slot_after is not None:
        _publish_seat_freed(item, slot_after)  # a repeated cancel just gave the missing seat back
    return slot_after


def _publish_seat_freed(item, slot_after):
    """Tell the world a seat is free again (never for a closed slot: nobody can take that seat)."""
    if slot_after is not None and slot_after["status"] == "AVAILABLE":
        publish_event("SlotSeatFreed", {
            "slot_id": item["slot_id"], "doctor_id": int(item["doctor_id"]),
            "appointment_id": item["appointment_id"], "consultation_type_id": int(item["consultation_type_id"]),
            "start_time_utc": item["start_time_utc"], "capacity": slot_after["capacity"],
            "booked_count": slot_after["booked_count"], "free_seats": slot_after["free_seats"]})


def publish_cancellation_events(item, result, slot_after, cancelled_by):
    publish_event("AppointmentCancelled", {**_event_detail(item), "cancelled_by": cancelled_by, "refund": {
        "tier": result.tier, "percent": float(result.percent), "amount": _amount(result.refund_amount),
        "currency": item.get("currency") or "INR"}})
    _publish_seat_freed(item, slot_after)
    emit_metric("CancellationsByTier", 1, Tier=result.tier)


def cancel(patient_id, appointment_id, body):
    reason = body.get("reason")
    if reason is not None and (not isinstance(reason, str) or len(reason) > 255):
        raise ApiError(400, "VALIDATION", "reason must be text of at most 255 characters.")
    reason = reason.strip() if reason else None
    table = _appointments_table()
    item = _own_appointment(table, appointment_id, patient_id)

    if item["status"] == store.CANCELLED_BY_PATIENT:
        # Repeating a cancellation is harmless. Finish any step that failed earlier.
        finish_cancellation(table, item, "PATIENT_CANCELLATION", publish=False)
        item = store.get_appointment(table, appointment_id)
        return response(200, {"message": "Appointment already cancelled", "appointment": _view(item)})
    if item["status"] != store.CONFIRMED:
        raise ApiError(409, "NOT_CANCELLABLE", f"This appointment is {item['status']} and cannot be cancelled.")
    if reschedule_rules.lock_is_fresh(item, _utcnow()):
        raise ApiError(409, "RESCHEDULE_IN_PROGRESS", "This appointment is being rescheduled. Try again in a moment.")

    now = _utcnow()
    start = _parse_instant(item["start_time_utc"])
    if start <= now:
        raise ApiError(409, "ALREADY_STARTED", "This appointment has already started and cannot be cancelled.")

    policy = _refund_policy()
    result = calculate_refund(item["fee_amount"], _naive_utc(start), _naive_utc(now), policy)
    fields = {"cancelled_at": store.now_iso(), **refund_fields(result, policy["currency"])}
    if reason:
        fields["cancellation_reason"] = reason
    updated = store.transition(table, appointment_id, store.CONFIRMED, store.CANCELLED_BY_PATIENT, fields)
    if updated is None:
        # Somebody else changed it first (a repeated click, or the doctor).
        current = store.get_appointment(table, appointment_id)
        if current and current["status"] == store.CANCELLED_BY_PATIENT:
            finish_cancellation(table, current, "PATIENT_CANCELLATION", publish=False)
            return response(200, {"message": "Appointment already cancelled", "appointment": _view(current)})
        raise ApiError(409, "NOT_CANCELLABLE", "This appointment can no longer be cancelled.")

    slot_after = finish_cancellation(table, updated, "PATIENT_CANCELLATION", publish=True)
    log_event("appointment_cancelled", patient_id=patient_id, appointment_id=appointment_id,
              tier=result.tier, refund_percent=float(result.percent), seat_released=slot_after is not None)
    updated = store.get_appointment(table, appointment_id) or updated
    return response(200, {"message": "Appointment cancelled", "appointment": _view(updated)})


# ---------------------------------------------------------------------------
# Reschedule (Milestone 6): start the Step Functions saga and wait briefly for it
# ---------------------------------------------------------------------------

_sfn_client = None
RESCHEDULE_WAIT_SECONDS = 20


def set_sfn_client(client):
    """Tests inject a fake Step Functions client here."""
    global _sfn_client
    _sfn_client = client


def _sfn():
    global _sfn_client
    if _sfn_client is None:
        _sfn_client = boto3.client("stepfunctions")
    return _sfn_client


def _saga_failure(error, cause):
    """Turn a failed execution into the API answer. The original appointment is always intact."""
    detail = {}
    try:
        message = json.loads(cause or "{}").get("errorMessage") or "{}"
        detail = json.loads(message) if message.startswith("{") else {"message": message}
    except (TypeError, ValueError) as exc:
        logger.warning(json.dumps({"event": "saga_cause_unreadable", "error_type": type(exc).__name__}))
    if error == "RescheduleCompensated":
        emit_metric("ReschedulesFailed", 1, Reason="COMPENSATED")
        return ApiError(409, "RESCHEDULE_COMPENSATED", "The reschedule could not be completed and was undone. "
                        "Your original appointment is unchanged.")
    if detail.get("code"):
        return ApiError(int(detail.get("status") or 409), detail["code"], detail.get("message") or "Reschedule refused.")
    emit_metric("ReschedulesFailed", 1, Reason=error or "UNKNOWN")
    return ApiError(502, "RESCHEDULE_FAILED", "The reschedule could not be completed. Your original appointment is unchanged.")


def reschedule(patient_id, appointment_id, body):
    target = body.get("target_slot_id") or body.get("slot_id")
    if not isinstance(target, str) or not target.strip() or len(target) > 128:
        raise ApiError(400, "VALIDATION", "target_slot_id is required.")
    target = target.strip()
    table = _appointments_table()
    item = _own_appointment(table, appointment_id, patient_id)
    slot_conn = db_connect()
    try:
        target_slot = _read_slot(slot_conn, target)
    finally:
        slot_conn.close()
    problem = reschedule_rules.check_rules(item, target_slot, _utcnow())
    if problem:
        raise ApiError(*problem)

    name = f"appt-{appointment_id}-{int(time.time() * 1000)}"
    started = _sfn().start_execution(
        stateMachineArn=os.environ["RESCHEDULE_STATE_MACHINE_ARN"], name=name,
        input=json.dumps({"appointment_id": str(appointment_id), "patient_id": int(patient_id),
                          "target_slot_id": target, "requested_at": store.now_iso()}))
    execution_arn = started["executionArn"]
    log_event("reschedule_started", patient_id=patient_id, appointment_id=appointment_id, execution=name)
    deadline = _monotonic() + RESCHEDULE_WAIT_SECONDS
    pause = 0.2
    while True:
        result = _sfn().describe_execution(executionArn=execution_arn)
        status = result.get("status")
        if status == "SUCCEEDED":
            updated = store.get_appointment(table, appointment_id)
            return response(200, {"message": "Appointment rescheduled", "appointment": _view(updated),
                                  "execution": name})
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            raise _saga_failure(result.get("error"), result.get("cause"))
        if _monotonic() >= deadline:
            return response(202, {"message": "The reschedule is still being processed. Refresh in a moment.",
                                  "execution": name, "code": "RESCHEDULE_PENDING"})
        _sleep(pause)
        pause = min(pause * 1.5, 1.0)


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
    method = (event.get("httpMethod") or "GET").upper()
    if method == "OPTIONS":
        return response(204, {})
    auth = (event.get("requestContext") or {}).get("authorizer") or {}
    user_id, role = auth.get("userId"), auth.get("role")
    if not user_id or role != "PATIENT":
        return response(403, {"message": "A patient account is required", "code": "FORBIDDEN"})
    patient_id = int(user_id)
    path = _clean_path(event)
    query = event.get("queryStringParameters") or {}
    try:
        if path == "/appointments" and method == "POST":
            return book(event, patient_id)

        if path == "/appointments" and method == "GET":
            return response(200, _list_appointments(_appointments_table(), patient_id, query))

        match = re.fullmatch(f"/appointments/({ID_RE})", path)
        if match and method == "GET":
            item = _own_appointment(_appointments_table(), match.group(1), patient_id)
            return response(200, {"appointment": _view(item, _cancellation_preview(item))})

        match = re.fullmatch(f"/appointments/({ID_RE})/cancel", path)
        if match and method == "POST":
            return cancel(patient_id, match.group(1), json_body(event))

        match = re.fullmatch(f"/appointments/({ID_RE})", path)
        if match and method == "DELETE":
            return cancel(patient_id, match.group(1), json_body(event))
        if match and method == "PATCH":
            return reschedule(patient_id, match.group(1), json_body(event))

        match = re.fullmatch(r"/slots/([^/]{1,128})/waitlist", path)
        if match and method in ("POST", "DELETE"):
            slot_id = unquote(match.group(1))
            action = waitlist_domain.join if method == "POST" else waitlist_domain.leave
            return response(*action(patient_id, slot_id))

        if path == "/patients/me/waitlists" and method == "GET":
            return response(*waitlist_domain.list_mine(patient_id))

        match = re.fullmatch(f"/waitlist-offers/({ID_RE})/accept", path)
        if match and method == "POST":
            status, body, _item = waitlist_domain.accept(patient_id, int(match.group(1)))
            if body.get("appointment_id"):
                body["appointment"] = _view(store.get_appointment(_appointments_table(), body["appointment_id"]))
            return response(status, body)

        if path == "/patients/me/refunds" and method == "GET":
            conn = db_connect()
            try:
                with conn.cursor() as cur:
                    return response(200, _list_refunds(cur, patient_id, query))
            finally:
                conn.close()

        if re.fullmatch(f"/appointments(/{ID_RE}(/cancel)?)?|/patients/me/(refunds|waitlists)"
                        f"|/slots/[^/]+/waitlist|/waitlist-offers/{ID_RE}/accept", path):
            return response(405, {"message": "Method not allowed", "code": "METHOD_NOT_ALLOWED"})
        return response(404, {"message": "Endpoint not found", "path": path})
    except ApiError as exc:
        log_event("booking_rejected", logging.WARNING, request_id=getattr(context, "aws_request_id", None),
                  code=exc.code, status=exc.status, method=method, path=path)
        if exc.code in ("SLOT_FULL", "BOOKING_CONTENTION", "DOCTOR_LIMIT_REACHED", "ALREADY_BOOKED"):
            emit_metric("BookingsRejected", 1, Reason=exc.code)
        return _error_response(exc)
    except (ValueError, TypeError) as exc:
        log_event("booking_validation_failed", logging.WARNING, request_id=getattr(context, "aws_request_id", None),
                  method=method, path=path, error_type=type(exc).__name__, message=str(exc)[:200])
        return response(400, {"message": str(exc), "code": "VALIDATION"})
    except Exception as exc:
        logger.exception("Booking request failed; request_id=%s", getattr(context, "aws_request_id", None))
        if is_transient_db_error(exc):
            # Overload, not a bug: nothing was booked (seat and hold were released), the client may retry
            # with the same Idempotency-Key.
            result = response(503, {"message": "The service is busy. Please retry in a moment.",
                                    "code": "TEMPORARILY_UNAVAILABLE"})
            result["headers"]["Retry-After"] = "1"
            return result
        return response(500, {"message": "Booking request failed", "code": "INTERNAL_ERROR"})


def lambda_handler(event, context):
    started = _monotonic()
    result = _handle_api(event, context)
    status = result.get("statusCode") if isinstance(result, dict) else None
    level = logging.INFO if not status or status < 400 else logging.WARNING if status < 500 else logging.ERROR
    auth = (event.get("requestContext") or {}).get("authorizer") or {}
    log_event("booking_api_request", level, request_id=getattr(context, "aws_request_id", None),
              method=event.get("httpMethod"), resource=event.get("resource"), user_id=auth.get("userId"),
              status=status, duration_ms=int((_monotonic() - started) * 1000))
    return result
