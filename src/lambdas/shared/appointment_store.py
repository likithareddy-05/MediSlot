"""Appointments in DynamoDB: the one place that reads and writes the appointments table.

Milestone 5 keeps appointments in DynamoDB and slots in RDS (see slot_store.py).

Table layout (see infrastructure/data.yaml):

* Partition key `appointment_id`: a sequential number stored as text ("1", "2", ...) taken from an
  atomic counter item, the same approach the project uses for token ids. No UUIDs.
* Index PatientIndex  (patient_id, start_time_utc): a patient's appointments.
* Index DoctorIndex   (doctor_id,  start_time_utc): a doctor's appointments.
* Index SlotIndex     (slot_id):                    every appointment of one slot.

Ids are stored as strings. Money is stored as a string such as "500.00" so no float rounding
can creep in. Times are ISO-8601 UTC text ending in Z, which sorts correctly.

A second kind of item lives in the same table: the HOLD item. Its key is
`HOLD#<slot_id>#<patient_id>` and it is written with attribute_not_exists, so DynamoDB itself
refuses a second active appointment for the same patient in the same slot. A hold item has no
patient_id / doctor_id / slot_id attributes, so it never appears in the indexes. The hold is
deleted when the appointment is cancelled, which lets the patient book that slot again.

Index queries are eventually consistent (a write may take a moment to appear). Reads by
`appointment_id` use strongly consistent reads.
"""
import json
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()

CONFIRMED = "CONFIRMED"
COMPLETED = "COMPLETED"
NO_SHOW = "NO_SHOW"
CANCELLED_BY_PATIENT = "CANCELLED_BY_PATIENT"
CANCELLED_BY_DOCTOR = "CANCELLED_BY_DOCTOR"
STATUSES = (CONFIRMED, COMPLETED, NO_SHOW, CANCELLED_BY_PATIENT, CANCELLED_BY_DOCTOR)
CANCELLED_STATUSES = (CANCELLED_BY_PATIENT, CANCELLED_BY_DOCTOR)

HOLD_PREFIX = "HOLD#"
STALE_HOLD_SECONDS = 120


def _log_event(event_name, level=logging.INFO, **fields):
    logger.log(level, json.dumps({"event": event_name, **fields}, default=str))


def _error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


SEQUENCE_KEY = "SEQ#appointment"


def new_appointment_id(table):
    """Next appointment number (1, 2, 3, ...) from an atomic counter item in the same table.

    The counter item has a non-numeric key and no index attributes, so it never shows up as an
    appointment and never appears in the indexes. A number may be skipped when a booking fails
    after it was issued; numbers are never reused.
    """
    result = table.update_item(Key={"appointment_id": SEQUENCE_KEY},
                               UpdateExpression="ADD current_value :one",
                               ExpressionAttributeValues={":one": 1}, ReturnValues="UPDATED_NEW")
    return str(int(result["Attributes"]["current_value"]))


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def hold_key(slot_id, patient_id):
    return f"{HOLD_PREFIX}{slot_id}#{patient_id}"


def is_hold(item):
    return str(item.get("appointment_id", "")).startswith(HOLD_PREFIX)


# ---------------------------------------------------------------- holds (one seat per patient per slot)

def acquire_hold(table, slot_id, patient_id, appointment_id, idempotency_key):
    """Reserve "this patient has an appointment in this slot". Returns (True, None) or (False, holder).

    `holder` is the existing appointment that already holds the slot for this patient, or None
    when the hold is held by something that no longer exists (it is cleaned up and retried once).
    """
    key = hold_key(slot_id, patient_id)
    item = {"appointment_id": key, "held_appointment_id": appointment_id,
            "idempotency_key": idempotency_key, "created_at": now_iso()}
    for attempt in range(2):
        try:
            table.put_item(Item=item, ConditionExpression="attribute_not_exists(appointment_id)")
            return True, None
        except Exception as exc:
            if _error_code(exc) != "ConditionalCheckFailedException":
                _log_event("hold_acquire_failed", logging.ERROR, slot_id=slot_id, patient_id=patient_id,
                     error_code=_error_code(exc), error_type=type(exc).__name__)
                raise
            existing = table.get_item(Key={"appointment_id": key}, ConsistentRead=True).get("Item")
            if existing is None:
                continue  # released in the meantime: try again
            holder = get_appointment(table, existing["held_appointment_id"])
            stale = holder is None and _age_seconds(existing.get("created_at")) > STALE_HOLD_SECONDS
            released = holder is not None and holder.get("status") in CANCELLED_STATUSES
            if stale or released:
                # A hold left behind by a crash or an unfinished cancel: remove it and take it over.
                _log_event("hold_stale_removed", logging.WARNING, slot_id=slot_id, patient_id=patient_id,
                     stale_missing_appointment=stale, stale_cancelled_appointment=released)
                release_hold(table, slot_id, patient_id)
                continue
            return False, holder or {"appointment_id": existing["held_appointment_id"],
                                     "idempotency_key": existing.get("idempotency_key"), "status": "IN_PROGRESS"}
    return False, None


def release_hold(table, slot_id, patient_id):
    table.delete_item(Key={"appointment_id": hold_key(slot_id, patient_id)})


def _age_seconds(created_at):
    try:
        then = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        _log_event("hold_timestamp_unreadable", logging.WARNING, error_type=type(exc).__name__)
        return float("inf")
    return (datetime.now(timezone.utc) - then).total_seconds()


# ---------------------------------------------------------------- single appointment

def put_appointment(table, item):
    """Create the appointment; refuses to overwrite an existing id."""
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(appointment_id)")


def get_appointment(table, appointment_id):
    if not str(appointment_id or "").isdigit():
        return None  # holds and the counter item are not appointments
    return table.get_item(Key={"appointment_id": str(appointment_id)}, ConsistentRead=True).get("Item")


def transition(table, appointment_id, expected_status, new_status, fields=None):
    """Move an appointment from one status to another in one conditional write.

    Returns the updated item, or None when the appointment was no longer in `expected_status`
    (somebody else changed it first). `fields` are extra attributes set in the same write.
    """
    values = {":expected": expected_status, ":new": new_status, ":now": now_iso()}
    names = {"#st": "status"}
    sets = ["#st = :new", "updated_at = :now"]
    for index, (name, value) in enumerate((fields or {}).items()):
        names[f"#f{index}"] = name
        values[f":f{index}"] = value
        sets.append(f"#f{index} = :f{index}")
    try:
        result = table.update_item(Key={"appointment_id": appointment_id},
            UpdateExpression="SET " + ", ".join(sets),
            ConditionExpression="#st = :expected",
            ExpressionAttributeNames=names, ExpressionAttributeValues=values, ReturnValues="ALL_NEW")
        return result.get("Attributes")
    except Exception as exc:
        if _error_code(exc) == "ConditionalCheckFailedException":
            _log_event("appointment_transition_skipped", appointment_id=appointment_id,
                 expected_status=expected_status, new_status=new_status)
            return None
        _log_event("appointment_transition_failed", logging.ERROR, appointment_id=appointment_id,
             new_status=new_status, error_code=_error_code(exc), error_type=type(exc).__name__)
        raise


def set_fields(table, appointment_id, fields):
    """Set extra attributes on an appointment that already exists."""
    names, values, sets = {}, {":now": now_iso()}, ["updated_at = :now"]
    for index, (name, value) in enumerate(fields.items()):
        names[f"#f{index}"] = name
        values[f":f{index}"] = value
        sets.append(f"#f{index} = :f{index}")
    table.update_item(Key={"appointment_id": appointment_id}, UpdateExpression="SET " + ", ".join(sets),
                      ConditionExpression="attribute_exists(appointment_id)",
                      ExpressionAttributeNames=names, ExpressionAttributeValues=values)


# ---------------------------------------------------------------- queries

def _query_all(table, **kwargs):
    items = []
    while True:
        page = table.query(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def list_for_patient(table, patient_id, newest_first=False):
    """Every appointment of one patient, ordered by start time."""
    return _query_all(table, IndexName="PatientIndex", KeyConditionExpression="patient_id = :p",
                      ExpressionAttributeValues={":p": str(patient_id)}, ScanIndexForward=not newest_first)


def list_for_doctor(table, doctor_id, first_iso, last_iso):
    """A doctor's appointments that start in [first_iso, last_iso)."""
    return _query_all(table, IndexName="DoctorIndex",
                      KeyConditionExpression="doctor_id = :d AND start_time_utc BETWEEN :a AND :b",
                      ExpressionAttributeValues={":d": str(doctor_id), ":a": first_iso, ":b": last_iso})


def list_for_slot(table, slot_id):
    return _query_all(table, IndexName="SlotIndex", KeyConditionExpression="slot_id = :s",
                      ExpressionAttributeValues={":s": slot_id})


def count_upcoming_with_doctor(table, patient_id, doctor_id, now_text=None):
    """How many CONFIRMED appointments in the future this patient has with one doctor."""
    items = _query_all(table, IndexName="PatientIndex",
                       KeyConditionExpression="patient_id = :p AND start_time_utc > :now",
                       FilterExpression="doctor_id = :d AND #st = :confirmed",
                       ExpressionAttributeNames={"#st": "status"},
                       ExpressionAttributeValues={":p": str(patient_id), ":d": str(doctor_id),
                                                  ":confirmed": CONFIRMED, ":now": now_text or now_iso()})
    return len(items)


def has_upcoming_or_history(table, patient_id, now_text=None):
    """(has_upcoming_confirmed, has_any_history) for account deletion."""
    items = list_for_patient(table, patient_id)
    now_text = now_text or now_iso()
    upcoming = any(i.get("status") == CONFIRMED and str(i.get("end_time_utc", "")) > now_text for i in items)
    return upcoming, bool(items)
