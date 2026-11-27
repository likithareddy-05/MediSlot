"""Reschedule rules (Milestone 6), shared by the PATCH /appointments/{id} endpoint and the saga.

Frozen Rule Set
  * An appointment may be rescheduled at most 2 times (SSM /medislot/<env>/reschedule/max-reschedules).
  * No reschedule within 6 hours of the appointment start (SSM .../reschedule/cutoff-hours).
  * The target is a slot of the same doctor and consultation type, so there is no fee change and
    no refund implication.
  * While a saga runs, the appointment carries a lock (reschedule_lock); a second reschedule or a
    cancellation is refused until it ends. A lock older than 5 minutes is considered abandoned.
"""
import json
from datetime import datetime, timedelta

import appointment_store as store
from common import int_setting, logger

DEFAULT_MAX_RESCHEDULES = 2
DEFAULT_CUTOFF_HOURS = 6
LOCK_STALE_SECONDS = 300


def _instant(text):
    return datetime.fromisoformat(str(text).replace("Z", "+00:00"))


def max_reschedules():
    return int_setting("RESCHEDULE_MAX_PARAMETER", DEFAULT_MAX_RESCHEDULES)


def cutoff_hours():
    return int_setting("RESCHEDULE_CUTOFF_HOURS_PARAMETER", DEFAULT_CUTOFF_HOURS)


def lock_is_fresh(item, now, execution=None):
    lock = item.get("reschedule_lock")
    if not lock or lock == execution:
        return False
    try:
        return (now - _instant(item.get("reschedule_lock_at"))).total_seconds() < LOCK_STALE_SECONDS
    except (TypeError, ValueError) as exc:
        logger.warning(json.dumps({"event": "reschedule_lock_unreadable", "error_type": type(exc).__name__}))
        return False


def check_rules(item, target, now, execution=None):
    """Why this reschedule is not allowed, as (status, code, message), or None when it is allowed."""
    if item.get("status") != store.CONFIRMED:
        return 409, "NOT_RESCHEDULABLE", f"This appointment is {item.get('status')} and cannot be rescheduled."
    if lock_is_fresh(item, now, execution):
        return 409, "RESCHEDULE_IN_PROGRESS", "A reschedule of this appointment is already in progress."
    limit = max_reschedules()
    if int(item.get("reschedule_count") or 0) >= limit:
        return 409, "RESCHEDULE_LIMIT_REACHED", f"An appointment can be rescheduled at most {limit} times."
    hours = cutoff_hours()
    if _instant(item["start_time_utc"]) - now < timedelta(hours=hours):
        return 409, "RESCHEDULE_TOO_LATE", f"Appointments cannot be rescheduled within {hours} hours of the start."
    if not target:
        return 404, "SLOT_NOT_FOUND", "The target slot does not exist."
    if target["slot_id"] == item["slot_id"]:
        return 400, "SAME_SLOT", "The appointment is already in this slot."
    if int(target["doctor_id"]) != int(item["doctor_id"]) \
            or int(target["consultation_type_id"]) != int(item["consultation_type_id"]):
        return 400, "DIFFERENT_DOCTOR", "Pick a slot of the same doctor and consultation type."
    if target["status"] != "AVAILABLE":
        return 409, "SLOT_UNAVAILABLE", "The target slot is no longer available."
    if _instant(target["start_time_utc"]) <= now:
        return 400, "SLOT_IN_PAST", "The target slot has already started."
    if int(target["free_seats"]) <= 0:
        return 409, "SLOT_FULL", "The target slot is full."
    return None
