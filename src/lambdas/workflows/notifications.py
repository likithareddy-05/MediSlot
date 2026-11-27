"""Notifications (Milestone 6): EventBridge -> SQS -> this Lambda -> SNS, plus the reminder dispatcher.

Two handlers share this module:
  * event_handler: SQS batch consumer. Each message is an EventBridge event; it becomes one SNS
    message per recipient. Failed messages are reported back (ReportBatchItemFailures), retried
    by SQS and finally moved to the consumer's dead-letter queue.
  * reminder_handler: runs every 5 minutes (EventBridge Scheduler) and sends the 24-hour and the
    2-hour reminder of every CONFIRMED appointment.

The eight notifications of the Frozen Rule Set
    AppointmentBooked                      patient and doctor
    AppointmentRescheduled                 patient and doctor
    AppointmentCancelled (by the patient)  patient and doctor
    AppointmentCancelled (by the doctor)   patient
    WaitlistOfferIssued                    patient
    WaitlistOfferExpired                   patient
    Reminder 24 hours before               patient
    Reminder 2 hours before                patient

Exactly once
  Every notification has a dispatch id built from the business event, e.g.
  "BOOKED#42#PATIENT#201" or "REMINDER_24H#42#2026-12-01T03:30:00Z#PATIENT#201". Before
  publishing, the dispatcher claims that id in the DynamoDB dispatch log with a conditional put
  (attribute_not_exists). A second run, a redelivered SQS message or a duplicate event finds the
  id already SENT and does nothing. A claim whose publish failed is deleted again so a retry can
  send it; a claim left by a crashed run is taken over after a few minutes. Log items expire via
  DynamoDB TTL after 30 days.

Messages carry ids, times and the event type only, never names, e-mail addresses or phone
numbers. Real e-mail / SMS delivery is out of scope: subscribe to the SNS topic to see them.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

import appointment_store as store
from events import emit_metric

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb = boto3.resource("dynamodb")
_sns = None

LOG_TTL_SECONDS = 30 * 24 * 3600
STALE_CLAIM_SECONDS = 300
REMINDERS = (("REMINDER_24H", timedelta(hours=24), timedelta(hours=2)),
             ("REMINDER_2H", timedelta(hours=2), timedelta(0)))


class DispatchInProgress(Exception):
    """Another run is sending this notification right now; the message is retried later."""


def set_sns_client(client):
    """Tests inject a fake SNS client here."""
    global _sns
    _sns = client


def _sns_client():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


def _log_event(event_name, level=logging.INFO, **fields):
    logger.log(level, json.dumps({"event": event_name, **fields}, default=str))


def _now():
    return datetime.now(timezone.utc)


def _iso(value):
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _dispatch_table():
    return ddb.Table(os.environ["DISPATCH_LOG_TABLE_NAME"])


def _appointments_table():
    return ddb.Table(os.environ["APPOINTMENTS_TABLE_NAME"])


def _error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


# ---------------------------------------------------------------- what to send

def notifications_for(detail_type, detail):
    """The notifications one business event produces: [(dispatch_id, event_type, recipient_role, recipient_id)]."""
    appt = detail.get("appointment_id")
    patient, doctor = detail.get("patient_id"), detail.get("doctor_id")
    both = (("PATIENT", patient), ("DOCTOR", doctor))
    if detail_type == "AppointmentBooked":
        return [(f"BOOKED#{appt}#{role}#{who}", "APPOINTMENT_BOOKED", role, who) for role, who in both]
    if detail_type == "AppointmentRescheduled":
        n = detail.get("reschedule_count")
        return [(f"RESCHEDULED#{appt}#{n}#{role}#{who}", "APPOINTMENT_RESCHEDULED", role, who) for role, who in both]
    if detail_type == "AppointmentCancelled":
        if detail.get("cancelled_by") == "DOCTOR":
            return [(f"CANCELLED#{appt}#PATIENT#{patient}", "APPOINTMENT_CANCELLED_BY_DOCTOR", "PATIENT", patient)]
        return [(f"CANCELLED#{appt}#{role}#{who}", "APPOINTMENT_CANCELLED_BY_PATIENT", role, who) for role, who in both]
    if detail_type == "WaitlistOfferIssued":
        return [(f"OFFER_ISSUED#{detail['offer_id']}#PATIENT#{patient}", "WAITLIST_OFFER_ISSUED", "PATIENT", patient)]
    if detail_type == "WaitlistOfferExpired":
        return [(f"OFFER_EXPIRED#{detail['offer_id']}#PATIENT#{patient}", "WAITLIST_OFFER_EXPIRED", "PATIENT", patient)]
    return []


SUBJECTS = {
    "APPOINTMENT_BOOKED": "Appointment booked",
    "APPOINTMENT_RESCHEDULED": "Appointment rescheduled",
    "APPOINTMENT_CANCELLED_BY_PATIENT": "Appointment cancelled by the patient",
    "APPOINTMENT_CANCELLED_BY_DOCTOR": "Appointment cancelled by the doctor",
    "WAITLIST_OFFER_ISSUED": "A seat is being held for you",
    "WAITLIST_OFFER_EXPIRED": "Your held seat has expired",
    "REMINDER_24H": "Reminder: appointment in 24 hours",
    "REMINDER_2H": "Reminder: appointment in 2 hours",
}


# ---------------------------------------------------------------- exactly-once dispatch

def dispatch(dispatch_id, event_type, recipient_role, recipient_id, detail):
    """Publish one notification at most once. Returns True when it was sent by this call."""
    table = _dispatch_table()
    now = int(time.time())
    try:
        table.put_item(
            Item={"dispatch_id": dispatch_id, "state": "PENDING", "claimed_at": now, "event_type": event_type,
                  "expires_at": now + LOG_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(dispatch_id) OR (#s = :pending AND claimed_at < :stale)",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":pending": "PENDING", ":stale": now - STALE_CLAIM_SECONDS})
    except Exception as exc:
        if _error_code(exc) != "ConditionalCheckFailedException":
            _log_event("dispatch_claim_failed", logging.ERROR, dispatch_id=dispatch_id, error_type=type(exc).__name__)
            raise
        existing = table.get_item(Key={"dispatch_id": dispatch_id}, ConsistentRead=True).get("Item") or {}
        if existing.get("state") == "SENT":
            _log_event("notification_duplicate_skipped", dispatch_id=dispatch_id)
            return False
        _log_event("notification_dispatch_in_progress", logging.WARNING, dispatch_id=dispatch_id)
        raise DispatchInProgress(dispatch_id)

    message = {"event_type": event_type, "subject": SUBJECTS.get(event_type, event_type),
               "recipient_role": recipient_role, "recipient_id": str(recipient_id), "dispatch_id": dispatch_id,
               "details": detail}
    try:
        result = _sns_client().publish(
            TopicArn=os.environ["NOTIFICATION_TOPIC_ARN"],
            Subject=SUBJECTS.get(event_type, "MediSlot notification")[:100],
            Message=json.dumps(message, default=str),
            MessageAttributes={"event_type": {"DataType": "String", "StringValue": event_type},
                               "recipient_role": {"DataType": "String", "StringValue": recipient_role}})
    except Exception as exc:
        _log_event("notification_publish_failed", logging.ERROR, dispatch_id=dispatch_id, error_type=type(exc).__name__)
        try:  # free the claim so the retry can send it
            table.delete_item(Key={"dispatch_id": dispatch_id}, ConditionExpression="#s = :pending",
                              ExpressionAttributeNames={"#s": "state"}, ExpressionAttributeValues={":pending": "PENDING"})
        except Exception as cleanup_error:
            _log_event("dispatch_claim_cleanup_failed", logging.ERROR, dispatch_id=dispatch_id,
                       error_type=type(cleanup_error).__name__)
        raise
    table.update_item(Key={"dispatch_id": dispatch_id},
                      UpdateExpression="SET #s = :sent, sent_at = :at, message_id = :mid",
                      ExpressionAttributeNames={"#s": "state"},
                      ExpressionAttributeValues={":sent": "SENT", ":at": _iso(_now()),
                                                 ":mid": str(result.get("MessageId", ""))})
    emit_metric("NotificationsDispatched", 1, EventType=event_type)
    _log_event("notification_dispatched", dispatch_id=dispatch_id, event_type=event_type, recipient_role=recipient_role)
    return True


def _detail_for_message(detail):
    keep = ("appointment_id", "slot_id", "offer_id", "start_time_utc", "end_time_utc", "previous_start_time_utc",
            "expires_at_utc", "reschedule_count", "cancelled_by", "refund")
    return {k: detail[k] for k in keep if k in detail}


def handle_event(event):
    """One EventBridge event -> its notifications. Returns how many were sent now."""
    detail_type, detail = event.get("detail-type"), event.get("detail") or {}
    sent = 0
    for dispatch_id, event_type, role, recipient in notifications_for(detail_type, detail):
        if recipient is None:
            _log_event("notification_without_recipient", logging.WARNING, dispatch_id=dispatch_id)
            continue
        sent += 1 if dispatch(dispatch_id, event_type, role, recipient, _detail_for_message(detail)) else 0
    return sent


def event_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        try:
            handle_event(json.loads(record["body"]))
        except Exception as exc:
            _log_event("notification_message_failed", logging.ERROR, message_id=record.get("messageId"),
                       error_type=type(exc).__name__, error=str(exc)[:200])
            failures.append({"itemIdentifier": record.get("messageId")})
    return {"batchItemFailures": failures}


# ---------------------------------------------------------------- reminders

def _confirmed_starting_between(first, last):
    return store.list_by_status(_appointments_table(), "CONFIRMED", _iso(first), _iso(last))


def send_reminders(now=None):
    """Send every reminder that is due. Safe to run any number of times."""
    now = now or _now()
    sent = skipped = 0
    for kind, window, floor in REMINDERS:
        # Due when the appointment starts within `window` (and more than `floor`) from now.
        for item in _confirmed_starting_between(now + floor + timedelta(seconds=1), now + window):
            dispatch_id = f"{kind}#{item['appointment_id']}#{item['start_time_utc']}#PATIENT#{item['patient_id']}"
            detail = {"appointment_id": item["appointment_id"], "slot_id": item["slot_id"],
                      "start_time_utc": item["start_time_utc"], "end_time_utc": item["end_time_utc"]}
            try:
                if dispatch(dispatch_id, kind, "PATIENT", item["patient_id"], detail):
                    sent += 1
                else:
                    skipped += 1
            except DispatchInProgress:
                _log_event("reminder_left_for_next_run", dispatch_id=dispatch_id)
    return {"reminders_sent": sent, "already_sent": skipped}


def reminder_handler(event, context):
    summary = send_reminders()
    _log_event("reminder_dispatch_completed", **summary)
    return summary
