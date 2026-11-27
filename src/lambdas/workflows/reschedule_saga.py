"""Reschedule saga (Milestone 6): the task Lambda behind the Step Functions state machine.

infrastructure/statemachines/reschedule-saga.asl.json calls `handler` once per step with
{"action": <step>, "saga": <state>, "execution": <execution name>} and gets the updated state back.

    ValidateRequest -> ClaimTargetSeat -> SwitchAppointment -> ReleaseOriginalSeat -> Finalize
                                               |                     |
                                               |                     +-- fails --> CompensateRevertAppointment
                                               +-- fails ----------------------------> CompensateReleaseTarget
                                                                                        -> MarkCompensated -> Fail

From the patient's side the operation is atomic: they always hold exactly one appointment.
  * The target seat is claimed first (optimistic concurrency, the booking engine's rule). A full
    target ends the saga before anything changed: the original stays CONFIRMED.
  * The appointment then moves to the target slot in ONE conditional DynamoDB write. Its id and
    status never change, so there is never "neither" and never "both".
  * The original seat is released last. If that release fails after the successful claim, the
    compensation moves the appointment back and gives the target seat back.

Rules (Frozen Rule Set): at most 2 reschedules per appointment, none within 6 hours of the start
(both SSM-configurable), no refund or fee change. They live in shared/reschedule_rules.py, which
the PATCH endpoint also uses to refuse an invalid request before an execution starts.

Forcing a failure (mentor checkpoint): set /medislot/<env>/reschedule/force-release-failure to
"true" and the ReleaseOriginalSeat step fails, which runs the compensation branch.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

import appointment_store as store
import slot_store
from common import db_connect, get_config_value, log_event, logger
from events import emit_metric, publish_event
from reschedule_rules import LOCK_STALE_SECONDS, check_rules

ddb = boto3.resource("dynamodb")

CLAIM_ATTEMPTS = 5


class RescheduleRejected(Exception):
    """A rule said no before anything changed. The message is JSON: {status, code, message}."""

    def __init__(self, status, code, message):
        super().__init__(json.dumps({"status": status, "code": code, "message": message}))
        self.status, self.code, self.text = status, code, message


class TargetSlotFull(RescheduleRejected):
    def __init__(self):
        super().__init__(409, "SLOT_FULL", "The target slot is full. Your original appointment is unchanged.")


class SwitchFailed(Exception):
    """The appointment changed while the saga ran (e.g. it was cancelled)."""


class ReleaseFailed(Exception):
    """The original seat could not be released."""


class ForcedReleaseFailure(ReleaseFailed):
    """Fault injection for the checkpoint: the release fails on purpose."""


class CompensationIncomplete(Exception):
    """A compensating action could not finish; Step Functions retries it."""


def _utcnow():
    return datetime.now(timezone.utc)


def _table():
    return ddb.Table(os.environ["APPOINTMENTS_TABLE_NAME"])


def _force_release_failure(state):
    if state.get("force_release_failure"):
        return True
    raw = get_config_value(os.environ.get("RESCHEDULE_FORCE_FAILURE_PARAMETER"), "false", ttl=5)
    return str(raw).strip().lower() in ("true", "1", "yes")


def _read_slot(slot_id):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            slot = slot_store.get_slot(cur, slot_id)
        conn.commit()
        return slot
    finally:
        conn.close()


def _release_seat(slot_id):
    """Give one seat back. Returns the slot afterwards, or None when nothing was released."""
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            released = slot_store.release_seat(cur, slot_id)
            slot = slot_store.get_slot(cur, slot_id) if released else None
        conn.commit()
        return slot
    finally:
        conn.close()


def _release_hold(slot_id, patient_id):
    try:
        store.release_hold(_table(), slot_id, patient_id)
    except Exception as exc:  # a leftover hold is cleaned up by the next booking attempt
        logger.error(json.dumps({"event": "hold_release_failed", "slot_id": slot_id, "error_type": type(exc).__name__}))


def _unlock(state, execution):
    """Remove this execution's lock from the appointment (no-op when it is not ours)."""
    try:
        _table().update_item(
            Key={"appointment_id": state["appointment_id"]},
            UpdateExpression="SET updated_at = :now REMOVE reschedule_lock, reschedule_lock_at",
            ConditionExpression="reschedule_lock = :exec",
            ExpressionAttributeValues={":exec": execution, ":now": store.now_iso()})
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            logger.error(json.dumps({"event": "reschedule_unlock_failed", "appointment_id": state["appointment_id"],
                                     "error_type": type(exc).__name__}))
            raise
        logger.info(json.dumps({"event": "reschedule_lock_not_ours", "appointment_id": state["appointment_id"]}))


# ---------------------------------------------------------------- steps

def validate(state, execution):
    table = _table()
    now = _utcnow()
    item = store.get_appointment(table, state["appointment_id"])
    if not item or str(item.get("patient_id")) != str(state["patient_id"]):
        raise RescheduleRejected(404, "APPOINTMENT_NOT_FOUND", "Appointment not found.")
    target = _read_slot(state["target_slot_id"])
    problem = check_rules(item, target, now, execution)
    if problem:
        raise RescheduleRejected(*problem)
    try:
        table.update_item(
            Key={"appointment_id": item["appointment_id"]},
            UpdateExpression="SET reschedule_lock = :exec, reschedule_lock_at = :now",
            ConditionExpression="#st = :confirmed AND slot_id = :orig AND (attribute_not_exists(reschedule_lock) "
                                "OR reschedule_lock = :exec OR reschedule_lock_at < :stale)",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":exec": execution, ":now": store.now_iso(), ":confirmed": store.CONFIRMED,
                                       ":orig": item["slot_id"],
                                       ":stale": (now - timedelta(seconds=LOCK_STALE_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")})
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            logger.error(json.dumps({"event": "reschedule_lock_failed", "error_type": type(exc).__name__}))
            raise
        logger.warning(json.dumps({"event": "reschedule_lock_refused", "appointment_id": item["appointment_id"]}))
        raise RescheduleRejected(409, "RESCHEDULE_IN_PROGRESS", "This appointment changed or is already being rescheduled.")
    return {**state, "locked": True, "doctor_id": int(item["doctor_id"]),
            "original_slot_id": item["slot_id"], "original_start_utc": item["start_time_utc"],
            "original_end_utc": item["end_time_utc"], "original_reschedule_count": int(item.get("reschedule_count") or 0),
            "target_start_utc": target["start_time_utc"], "target_end_utc": target["end_time_utc"]}


def claim_target(state, execution):
    """Hold item for (target slot, patient), then one seat of the target slot (OCC, bounded retries)."""
    table = _table()
    patient_id = int(state["patient_id"])
    won, holder = store.acquire_hold(table, state["target_slot_id"], patient_id, state["appointment_id"],
                                     f"reschedule-{execution}")
    if not won and not (holder and str(holder.get("appointment_id")) == str(state["appointment_id"])):
        raise RescheduleRejected(409, "ALREADY_BOOKED", "You already have an appointment in the target slot.")
    conn = db_connect()
    try:
        for _ in range(CLAIM_ATTEMPTS):
            with conn.cursor() as cur:
                slot = slot_store.get_slot(cur, state["target_slot_id"])
                if not slot or slot["status"] != "AVAILABLE" or slot["free_seats"] <= 0:
                    conn.commit()
                    _release_hold(state["target_slot_id"], patient_id)
                    raise TargetSlotFull()
                if slot_store.claim_seat(cur, slot["slot_id"], slot["version"]):
                    conn.commit()
                    return {**state, "target_claimed": True}
            conn.commit()
    finally:
        conn.close()
    _release_hold(state["target_slot_id"], patient_id)
    raise RescheduleRejected(409, "BOOKING_CONTENTION", "The target slot is very busy. Please try again.")


def switch(state, execution):
    """Move the appointment to the target slot in one conditional write (the commit point)."""
    table = _table()
    try:
        table.update_item(
            Key={"appointment_id": state["appointment_id"]},
            UpdateExpression="SET slot_id = :t, start_time_utc = :ts, end_time_utc = :te, "
                             "reschedule_count = if_not_exists(reschedule_count, :zero) + :one, "
                             "previous_slot_id = :o, rescheduled_at = :now, updated_at = :now",
            ConditionExpression="#st = :confirmed AND slot_id = :o AND reschedule_lock = :exec",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":t": state["target_slot_id"], ":ts": state["target_start_utc"],
                                       ":te": state["target_end_utc"], ":o": state["original_slot_id"],
                                       ":zero": 0, ":one": 1, ":now": store.now_iso(), ":confirmed": store.CONFIRMED,
                                       ":exec": execution})
    except Exception as exc:
        current = store.get_appointment(table, state["appointment_id"]) or {}
        if current.get("slot_id") == state["target_slot_id"] and current.get("reschedule_lock") == execution:
            logger.info(json.dumps({"event": "reschedule_switch_already_done", "appointment_id": state["appointment_id"]}))
            return {**state, "switched": True}
        logger.error(json.dumps({"event": "reschedule_switch_failed", "appointment_id": state["appointment_id"],
                                 "error_type": type(exc).__name__}))
        raise SwitchFailed("The appointment changed while it was being rescheduled.")
    return {**state, "switched": True}


def release_original(state, execution):
    if _force_release_failure(state):
        logger.warning(json.dumps({"event": "reschedule_release_forced_failure", "appointment_id": state["appointment_id"]}))
        raise ForcedReleaseFailure("Forced failure while releasing the original seat (fault injection).")
    slot_after = _release_seat(state["original_slot_id"])
    if slot_after is None:
        raise ReleaseFailed("The original seat could not be released.")
    _release_hold(state["original_slot_id"], int(state["patient_id"]))
    return {**state, "original_released": True, "original_free_seats": slot_after["free_seats"],
            "original_slot_status": slot_after["status"], "original_held_count": slot_after.get("held_count", 0)}


def compensate_revert(state, execution):
    """Move the appointment back to the original slot (only if this execution moved it)."""
    table = _table()
    try:
        table.update_item(
            Key={"appointment_id": state["appointment_id"]},
            UpdateExpression="SET slot_id = :o, start_time_utc = :os, end_time_utc = :oe, reschedule_count = :count, "
                             "updated_at = :now REMOVE previous_slot_id, rescheduled_at",
            ConditionExpression="slot_id = :t AND reschedule_lock = :exec",
            ExpressionAttributeValues={":o": state["original_slot_id"], ":os": state["original_start_utc"],
                                       ":oe": state["original_end_utc"], ":count": state["original_reschedule_count"],
                                       ":t": state["target_slot_id"], ":exec": execution, ":now": store.now_iso()})
    except Exception as exc:
        current = store.get_appointment(table, state["appointment_id"]) or {}
        if current.get("slot_id") != state["target_slot_id"]:
            logger.info(json.dumps({"event": "reschedule_revert_not_needed", "appointment_id": state["appointment_id"]}))
            return {**state, "reverted": True}
        logger.error(json.dumps({"event": "reschedule_revert_failed", "error_type": type(exc).__name__}))
        raise CompensationIncomplete("Could not move the appointment back to its original slot.")
    return {**state, "reverted": True}


def compensate_release_target(state, execution):
    """Give the claimed target seat back and drop the target hold."""
    if state.get("target_claimed") and not state.get("target_released"):
        if _release_seat(state["target_slot_id"]) is None:
            raise CompensationIncomplete("Could not release the target seat.")
    _release_hold(state["target_slot_id"], int(state["patient_id"]))
    return {**state, "target_released": True}


def mark_compensated(state, execution):
    _unlock(state, execution)
    emit_metric("ReschedulesCompensated", 1)
    log_event("reschedule_compensated", logging.WARNING, appointment_id=state["appointment_id"],
              original_slot_id=state.get("original_slot_id"), target_slot_id=state.get("target_slot_id"),
              execution=execution)
    return state


def finalize(state, execution):
    _unlock(state, execution)
    count = state["original_reschedule_count"] + 1
    publish_event("AppointmentRescheduled", {
        "appointment_id": state["appointment_id"], "patient_id": int(state["patient_id"]),
        "doctor_id": int(state["doctor_id"]), "from_slot_id": state["original_slot_id"],
        "to_slot_id": state["target_slot_id"], "previous_start_time_utc": state["original_start_utc"],
        "start_time_utc": state["target_start_utc"], "end_time_utc": state["target_end_utc"],
        "reschedule_count": count})
    if state.get("original_slot_status") == "AVAILABLE":
        publish_event("SlotSeatFreed", {"slot_id": state["original_slot_id"], "doctor_id": int(state["doctor_id"]),
                                        "appointment_id": state["appointment_id"], "reason": "RESCHEDULED",
                                        "free_seats": state.get("original_free_seats"),
                                        "held_count": state.get("original_held_count")})
    emit_metric("ReschedulesCompleted", 1)
    log_event("appointment_rescheduled", appointment_id=state["appointment_id"], reschedule_count=count,
              execution=execution)
    return {**state, "result": "RESCHEDULED", "reschedule_count": count}


def unlock(state, execution):
    """Used on the rejection path: nothing changed, only the lock (if any) is removed."""
    if state.get("locked") or state.get("appointment_id"):
        _unlock(state, execution)
    return state


ACTIONS = {"validate": validate, "claim_target": claim_target, "switch": switch,
           "release_original": release_original, "compensate_revert": compensate_revert,
           "compensate_release_target": compensate_release_target, "mark_compensated": mark_compensated,
           "finalize": finalize, "unlock": unlock}


def handler(event, context):
    action = event["action"]
    state = event.get("saga") or {}
    execution = event.get("execution") or state.get("execution") or "manual"
    log_event("reschedule_step", action=action, appointment_id=state.get("appointment_id"), execution=execution)
    return ACTIONS[action](state, execution)
