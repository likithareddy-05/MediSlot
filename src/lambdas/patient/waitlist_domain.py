"""MediSlot waitlist and hold offers (Milestone 6).

Three entry points share this module (one ZIP, three Lambda handlers):

  * API (called by booking_domain's router, PATIENT only; the caller is always the authorizer's
    userId, never an id taken from the request):
        POST   /slots/{slotId}/waitlist          join the line of a full slot
        DELETE /slots/{slotId}/waitlist          leave it (a live offer is declined and passed on)
        GET    /patients/me/waitlists            active waitlists with their pending offers
        POST   /waitlist-offers/{offerId}/accept turn a hold offer into a CONFIRMED appointment
  * seat_freed_handler: SQS consumer of SlotSeatFreed (EventBridge -> SQS -> Lambda). Issues a
    hold offer to the first patient in line for every held seat that has none.
  * expiry_sweeper_handler: runs every minute (EventBridge Scheduler). Expires offers whose hold
    time is over and passes the seat to the next patient in line. It also re-checks every slot
    that still holds seats, so a lost SlotSeatFreed event cannot leave a seat stuck.

Rules (Frozen Rule Set): the line is ordered by join time; a hold reserves the seat exclusively
for the hold duration (SSM, default 15 minutes) and the seat is not bookable by anyone else
meanwhile; a patient may be on at most 5 waitlists (SSM); one freed seat is never offered to two
patients at once (see shared/waitlist_store.py for why that holds).

Every handler is safe to run twice: each change is conditional on the current status, under the
slot row lock.
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3

import appointment_store as store
import slot_store
import waitlist_store as wl
from common import ApiError, db_connect, int_setting, log_event, logger
from events import emit_metric, publish_event
from refunds import money

ddb = boto3.resource("dynamodb")

DEFAULT_HOLD_MINUTES = 15
DEFAULT_MAX_ACTIVE_WAITLISTS = 5
DEFAULT_MAX_UPCOMING_PER_DOCTOR = 3


def _utcnow():
    """Naive UTC now (the form MySQL stores). Tests move the clock by replacing this."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _appointments_table():
    return ddb.Table(os.environ["APPOINTMENTS_TABLE_NAME"])


def hold_minutes():
    return int_setting("WAITLIST_HOLD_MINUTES_PARAMETER", DEFAULT_HOLD_MINUTES)


def max_active_waitlists():
    return int_setting("WAITLIST_MAX_ACTIVE_PARAMETER", DEFAULT_MAX_ACTIVE_WAITLISTS)


def _iso(value):
    return slot_store.iso_utc(value) if value is not None else None


# ---------------------------------------------------------------------------- events

def publish_offer_events(issued, expired=()):
    """Events for offers created / expired by a transaction that has committed."""
    for offer in expired:
        publish_event("WaitlistOfferExpired", {
            "offer_id": int(offer["offer_id"]), "slot_id": offer["slot_id"], "patient_id": int(offer["patient_id"]),
            "expired_at_utc": _iso(offer.get("resolved_at"))})
        emit_metric("OffersExpired", 1)
    for offer in issued:
        publish_event("WaitlistOfferIssued", {
            "offer_id": int(offer["offer_id"]), "slot_id": offer["slot_id"], "patient_id": offer["patient_id"],
            "doctor_id": offer["doctor_id"], "consultation_type_id": offer["consultation_type_id"],
            "start_time_utc": _iso(offer["start_time_utc"]), "expires_at_utc": _iso(offer["expires_at"])})
        emit_metric("OffersIssued", 1)


def _lock_patient(cursor, patient_id):
    """Serialise one patient's own waitlist and booking actions (same lock the booking engine takes)."""
    cursor.execute("""SELECT p.user_id FROM patients p JOIN users u ON u.user_id = p.user_id
        WHERE p.user_id = %s AND u.role = 'PATIENT' AND u.account_status = 'ACTIVE' FOR UPDATE OF p""", (patient_id,))
    if not cursor.fetchone():
        raise ApiError(409, "PROFILE_REQUIRED", "Create your patient profile first.")


def _has_appointment_in_slot(table, slot_id, patient_id):
    hold = table.get_item(Key={"appointment_id": store.hold_key(slot_id, patient_id)}, ConsistentRead=True).get("Item")
    if not hold:
        return False
    holder = store.get_appointment(table, hold.get("held_appointment_id"))
    return bool(holder and holder.get("status") == store.CONFIRMED)


def _position(cursor, slot_id, entry_id):
    line = [int(r["entry_id"]) for r in wl.waiting_line(cursor, slot_id)]
    return line.index(int(entry_id)) + 1 if int(entry_id) in line else None


# ---------------------------------------------------------------------------- API

def join(patient_id, slot_id):
    now = _utcnow()
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            _lock_patient(cur, patient_id)
            slot = wl.lock_slot(cur, slot_id)
            if not slot:
                raise ApiError(404, "SLOT_NOT_FOUND", "This slot does not exist.")
            if slot["status"] != "AVAILABLE":
                raise ApiError(409, "SLOT_UNAVAILABLE", "This slot is no longer available.")
            if slot["start_time_utc"] <= now:
                raise ApiError(400, "SLOT_IN_PAST", "This slot has already started.")
            existing = wl.active_entry(cur, slot_id, patient_id)
            if existing:
                position = _position(cur, slot_id, existing["entry_id"])
                conn.commit()
                return 200, {"message": "You are already on this waitlist", "entry_id": int(existing["entry_id"]),
                             "status": existing["status"], "position": position}
            if int(slot["booked_count"]) + int(slot["held_count"] or 0) < int(slot["capacity"]):
                raise ApiError(409, "SLOT_NOT_FULL", "This slot still has free seats. Book it directly.")
            if _has_appointment_in_slot(_appointments_table(), slot_id, patient_id):
                raise ApiError(409, "ALREADY_BOOKED", "You already have an appointment in this slot.")
            limit = max_active_waitlists()
            if wl.count_active_for_patient(cur, patient_id) >= limit:
                raise ApiError(409, "WAITLIST_LIMIT_REACHED", f"You can be on at most {limit} waitlists at once.")
            entry_id = wl.add_entry(cur, slot_id, patient_id)
            position = _position(cur, slot_id, entry_id)
        conn.commit()
    except BaseException:
        conn.rollback()
        logger.warning(json.dumps({"event": "waitlist_join_not_completed", "slot_id": slot_id, "patient_id": patient_id}))
        raise
    finally:
        conn.close()
    emit_metric("WaitlistJoins", 1)
    log_event("waitlist_joined", patient_id=patient_id, slot_id=slot_id, entry_id=entry_id, position=position)
    return 201, {"message": "You are on the waitlist", "entry_id": int(entry_id), "status": wl.WAITING,
                 "position": position}


def leave(patient_id, slot_id):
    now = _utcnow()
    conn = db_connect()
    issued = []
    try:
        with conn.cursor() as cur:
            _lock_patient(cur, patient_id)
            wl.lock_slot(cur, slot_id)
            entry = wl.active_entry(cur, slot_id, patient_id)
            if not entry:
                raise ApiError(404, "NOT_ON_WAITLIST", "You are not on the waitlist of this slot.")
            declined = None
            if entry["status"] == wl.OFFERED:
                declined = wl.live_offer_for_entry(cur, entry["entry_id"])
                if declined:
                    wl.resolve_offer(cur, declined["offer_id"], wl.OFFER_DECLINED, now)
                wl.set_entry_status(cur, entry["entry_id"], wl.LEFT, wl.OFFERED)
                issued, _ = wl.issue_offers(cur, slot_id, now, hold_minutes())  # the seat goes to the next in line
            else:
                wl.set_entry_status(cur, entry["entry_id"], wl.LEFT, wl.WAITING)
        conn.commit()
    except BaseException:
        conn.rollback()
        logger.warning(json.dumps({"event": "waitlist_leave_not_completed", "slot_id": slot_id, "patient_id": patient_id}))
        raise
    finally:
        conn.close()
    publish_offer_events(issued)
    log_event("waitlist_left", patient_id=patient_id, slot_id=slot_id, declined_offer=bool(declined),
              offers_passed_on=len(issued))
    return 200, {"message": "You have left the waitlist" + (" and declined your offer" if declined else ""),
                 "slot_id": slot_id}


def list_mine(patient_id):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            rows = wl.list_for_patient(cur, patient_id)
            waitlists = []
            for r in rows:
                offer = None
                if r.get("offer_id"):
                    offer = {"offer_id": int(r["offer_id"]), "expires_at_utc": _iso(r["expires_at"])}
                waitlists.append({
                    "entry_id": int(r["entry_id"]), "slot_id": r["slot_id"], "status": r["status"],
                    "joined_at_utc": _iso(r["joined_at"]),
                    "position": _position(cur, r["slot_id"], r["entry_id"]) if r["status"] == wl.WAITING else None,
                    "doctor_id": int(r["doctor_id"]), "doctor_name": f"{r['first_name']} {r['last_name']}",
                    "consultation_type_id": int(r["consultation_type_id"]),
                    "consultation_name": r["consultation_name"], "fee": float(money(r["fee"])),
                    "start_time_utc": _iso(r["start_time_utc"]), "end_time_utc": _iso(r["end_time_utc"]),
                    "timezone": r.get("timezone_name"), "offer": offer})
        conn.commit()
    finally:
        conn.close()
    return 200, {"waitlists": waitlists, "limit": max_active_waitlists(), "hold_minutes": hold_minutes()}


def _fee_and_doctor(cursor, doctor_id, ctype_id):
    cursor.execute("""SELECT dct.fee, doc.first_name, doc.last_name FROM doctor_consultation_types dct
        JOIN users u ON u.user_id = dct.doctor_id JOIN doctors doc ON doc.user_id = dct.doctor_id
        WHERE dct.doctor_id = %s AND dct.consultation_type_id = %s AND dct.is_active = 1
        AND u.account_status = 'ACTIVE'""", (doctor_id, ctype_id))
    return cursor.fetchone()


def _appointment_item(appointment_id, patient_id, slot, fee_row, offer_id):
    now_text = store.now_iso()
    return {
        "appointment_id": str(appointment_id), "patient_id": str(patient_id), "doctor_id": str(slot["doctor_id"]),
        "slot_id": slot["slot_id"], "consultation_type_id": str(slot["consultation_type_id"]),
        "consultation_name": slot["consultation_name"],
        "doctor_name": f"{fee_row['first_name']} {fee_row['last_name']}",
        "status": store.CONFIRMED, "start_time_utc": slot["start_time_utc"], "end_time_utc": slot["end_time_utc"],
        "fee_amount": str(money(fee_row["fee"])), "idempotency_key": f"offer-{offer_id}",
        "booked_via": "WAITLIST_OFFER", "offer_id": str(offer_id), "created_at": now_text, "updated_at": now_text,
    }


def _upcoming_limit():
    return int_setting("MAX_UPCOMING_PARAMETER", DEFAULT_MAX_UPCOMING_PER_DOCTOR)


def accept(patient_id, offer_id):
    """Turn a live hold offer into a CONFIRMED appointment. Returns (status, body, appointment item).

    Order: patient lock -> slot lock -> checks -> hold item (DynamoDB, one appointment per patient
    per slot) -> held seat becomes a booked seat + offer ACCEPTED + entry PROMOTED (one MySQL
    commit) -> appointment saved. If the appointment save fails after the commit, accepting again
    recreates it with the same id (the offer remembers it), so the seat is never lost.
    """
    table = _appointments_table()
    now = _utcnow()
    conn = db_connect()
    held = committed = False
    slot_view = fee_row = None
    appointment_id = None
    try:
        with conn.cursor() as cur:
            _lock_patient(cur, patient_id)
            offer = wl.get_offer(cur, offer_id)
            if not offer or int(offer["patient_id"]) != int(patient_id):
                raise ApiError(404, "OFFER_NOT_FOUND", "Offer not found.")
            wl.lock_slot(cur, offer["slot_id"])
            offer = wl.get_offer(cur, offer_id)  # re-read under the slot lock
            slot_view = slot_store.get_slot(cur, offer["slot_id"])
            if offer["status"] == wl.OFFER_ACCEPTED and offer.get("appointment_id"):
                appointment_id = str(offer["appointment_id"])
                existing = store.get_appointment(table, appointment_id)
                if existing:
                    conn.commit()
                    return 200, {"message": "Offer already accepted", "appointment_id": appointment_id}, existing
                fee_row = _fee_and_doctor(cur, slot_view["doctor_id"], slot_view["consultation_type_id"])
                committed = True  # the seat is already ours: only the appointment item is missing
            else:
                if offer["status"] != wl.OFFER_LIVE:
                    raise ApiError(409, "OFFER_NOT_LIVE", f"This offer is {offer['status'].lower()} and can no longer be accepted.")
                if offer["expires_at"] <= now:
                    raise ApiError(409, "OFFER_EXPIRED", "This offer has expired.")
                if not slot_view or slot_view["status"] != "AVAILABLE" \
                        or slot_store.to_utc_naive(datetime.fromisoformat(slot_view["start_time_utc"].replace("Z", "+00:00"))) <= now:
                    raise ApiError(409, "SLOT_UNAVAILABLE", "This slot is no longer available.")
                fee_row = _fee_and_doctor(cur, slot_view["doctor_id"], slot_view["consultation_type_id"])
                if not fee_row:
                    raise ApiError(409, "SLOT_UNAVAILABLE", "This doctor is not offering this consultation any more.")
                limit = _upcoming_limit()
                if store.count_upcoming_with_doctor(table, patient_id, slot_view["doctor_id"]) >= limit:
                    raise ApiError(409, "DOCTOR_LIMIT_REACHED",
                                   f"You can hold at most {limit} upcoming appointments with the same doctor.")
                appointment_id = store.new_appointment_id(table)
                won, _holder = store.acquire_hold(table, offer["slot_id"], patient_id, appointment_id, f"offer-{offer_id}")
                if not won:
                    raise ApiError(409, "ALREADY_BOOKED", "You already have an appointment in this slot.")
                held = True
                if not wl.take_held_seat(cur, offer["slot_id"]):
                    raise ApiError(409, "SLOT_UNAVAILABLE", "This slot is no longer available.")
                wl.resolve_offer(cur, offer_id, wl.OFFER_ACCEPTED, now, int(appointment_id))
                wl.set_entry_status(cur, offer["entry_id"], wl.PROMOTED, wl.OFFERED)
        conn.commit()
        committed = True
    except BaseException as exc:
        logger.warning(json.dumps({"event": "offer_accept_not_completed", "offer_id": offer_id,
                                   "patient_id": patient_id, "error_type": type(exc).__name__}))
        if not committed:
            conn.rollback()
            if held:
                try:
                    store.release_hold(table, slot_view["slot_id"], patient_id)
                except Exception as release_error:
                    logger.error(json.dumps({"event": "hold_release_failed", "error": str(release_error)[:200]}))
        raise
    finally:
        conn.close()

    item = _appointment_item(appointment_id, patient_id, slot_view, fee_row, offer_id)
    try:
        store.put_appointment(table, item)
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            logger.error(json.dumps({"event": "offer_appointment_save_failed", "offer_id": offer_id,
                                     "appointment_id": appointment_id, "error_type": type(exc).__name__}))
            raise
        logger.info(json.dumps({"event": "offer_appointment_already_saved", "appointment_id": appointment_id}))
    publish_event("AppointmentBooked", {
        "appointment_id": item["appointment_id"], "patient_id": int(patient_id), "doctor_id": int(item["doctor_id"]),
        "slot_id": item["slot_id"], "consultation_type_id": int(item["consultation_type_id"]),
        "start_time_utc": item["start_time_utc"], "end_time_utc": item["end_time_utc"],
        "fee": float(money(item["fee_amount"])), "booked_via": "WAITLIST_OFFER", "offer_id": int(offer_id)})
    emit_metric("WaitlistPromotions", 1)
    log_event("waitlist_offer_accepted", patient_id=patient_id, offer_id=offer_id, appointment_id=appointment_id)
    return 201, {"message": "Offer accepted. Your appointment is confirmed.", "appointment_id": item["appointment_id"]}, item


# ---------------------------------------------------------------------------- workers

def promote_slot(slot_id, now=None):
    """Issue offers for every unassigned held seat of one slot. Returns the offers issued."""
    now = now or _utcnow()
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            issued, returned = wl.issue_offers(cur, slot_id, now, hold_minutes())
        conn.commit()
    except BaseException:
        conn.rollback()
        logger.warning(json.dumps({"event": "waitlist_promotion_not_completed", "slot_id": slot_id}))
        raise
    finally:
        conn.close()
    publish_offer_events(issued)
    if issued or returned:
        log_event("waitlist_promoted", slot_id=slot_id, offers_issued=len(issued), seats_returned=returned)
    return issued


def seat_freed_handler(event, context):
    """SQS batch of SlotSeatFreed events (EventBridge -> SQS). Failed messages are retried, then go to the DLQ."""
    failures = []
    for record in event.get("Records", []):
        try:
            detail = json.loads(record["body"]).get("detail") or {}
            slot_id = detail["slot_id"]
            promote_slot(slot_id)
        except Exception as exc:
            logger.error(json.dumps({"event": "seat_freed_processing_failed", "message_id": record.get("messageId"),
                                     "error_type": type(exc).__name__, "error": str(exc)[:200]}))
            failures.append({"itemIdentifier": record.get("messageId")})
    return {"batchItemFailures": failures}


def expire_offer(offer_id, slot_id, now):
    """Expire one due offer and pass its seat on. Returns (expired offer or None, offers issued)."""
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            wl.lock_slot(cur, slot_id)
            offer = wl.get_offer(cur, offer_id)
            if not offer or offer["status"] != wl.OFFER_LIVE or offer["expires_at"] > now:
                conn.commit()
                return None, []  # accepted, declined or already expired by another run
            wl.resolve_offer(cur, offer_id, wl.OFFER_EXPIRED, now)
            wl.set_entry_status(cur, offer["entry_id"], wl.EXPIRED, wl.OFFERED)
            issued, _ = wl.issue_offers(cur, slot_id, now, hold_minutes())
        conn.commit()
    except BaseException:
        conn.rollback()
        logger.warning(json.dumps({"event": "offer_expiry_not_completed", "offer_id": offer_id}))
        raise
    finally:
        conn.close()
    expired = {**offer, "resolved_at": now}
    publish_offer_events(issued, [expired])
    return expired, issued


def sweep(now=None):
    """One run of the expiry sweeper. Safe to run twice or concurrently."""
    now = now or _utcnow()
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            due = wl.due_offers(cur, now)
            recheck = set(wl.slots_with_held_seats(cur)) | set(wl.started_slots_with_waitlist(cur, now))
        conn.commit()
    finally:
        conn.close()
    expired = issued = 0
    for row in due:
        gone, new_offers = expire_offer(row["offer_id"], row["slot_id"], now)
        expired += 1 if gone else 0
        issued += len(new_offers)
    for slot_id in sorted(recheck):
        issued += len(promote_slot(slot_id, now))
    return {"offers_expired": expired, "offers_issued": issued, "slots_checked": len(recheck)}


def expiry_sweeper_handler(event, context):
    summary = sweep()
    log_event("offer_expiry_sweep_completed", logging.INFO, **summary)
    return summary
