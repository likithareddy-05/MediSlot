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
    (plus WaitlistJoined                   patient, a confirmation that they are in line)
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

Message format: a plain-text e-mail written for a person (subject line, greeting, what happened,
the appointment details, what to do next, sign-off). Details missing from the event (doctor,
consultation, fee) are read from the appointment record. The patient's own name, e-mail address
and phone number are never included. Times are shown in the slot's time zone (DISPLAY_TIMEZONE,
default Asia/Kolkata, when the event has none). Real e-mail / SMS delivery to each patient is out
of scope: subscribe an address to the SNS topic to read them.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

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
    if detail_type == "WaitlistJoined":
        return [(f"WAITLIST_JOINED#{detail['entry_id']}#PATIENT#{patient}", "WAITLIST_JOINED", "PATIENT", patient)]
    if detail_type == "WaitlistOfferIssued":
        return [(f"OFFER_ISSUED#{detail['offer_id']}#PATIENT#{patient}", "WAITLIST_OFFER_ISSUED", "PATIENT", patient)]
    if detail_type == "WaitlistOfferExpired":
        return [(f"OFFER_EXPIRED#{detail['offer_id']}#PATIENT#{patient}", "WAITLIST_OFFER_EXPIRED", "PATIENT", patient)]
    return []


# ---------------------------------------------------------------- the e-mail text

# (subject for the patient, subject for the doctor, opening for the patient, opening for the doctor)
TEMPLATES = {
    "APPOINTMENT_BOOKED": (
        "Your appointment is confirmed", "New appointment booked",
        "Your appointment has been booked and confirmed. The details are below.",
        "A patient has booked an appointment with you. The details are below."),
    "APPOINTMENT_RESCHEDULED": (
        "Your appointment has been rescheduled", "An appointment has been rescheduled",
        "Your appointment has been moved to a new time. There is no change to the fee.",
        "A patient has moved their appointment with you to a new time."),
    "APPOINTMENT_CANCELLED_BY_PATIENT": (
        "Your appointment has been cancelled", "An appointment has been cancelled",
        "Your appointment has been cancelled as you requested.",
        "A patient has cancelled their appointment with you. The time is free again."),
    "APPOINTMENT_CANCELLED_BY_DOCTOR": (
        "Your appointment has been cancelled by the doctor", None,
        "We are sorry: your doctor is unable to see you at the time below, so the appointment has been "
        "cancelled. You will receive a full refund.", None),
    "WAITLIST_JOINED": (
        "You are on the waitlist", None,
        "You have joined the waitlist for the appointment slot below.", None),
    "WAITLIST_OFFER_ISSUED": (
        "A seat is being held for you - please confirm", None,
        "Good news: a seat has become free in the slot you were waiting for, and it is being held for you alone.",
        None),
    "WAITLIST_OFFER_EXPIRED": (
        "Your held seat has expired", None,
        "The seat we were holding for you was not confirmed in time, so it has been offered to the next "
        "patient in line.", None),
    "REMINDER_24H": (
        "Reminder: your appointment is tomorrow", None,
        "This is a friendly reminder that your appointment starts in about 24 hours.", None),
    "REMINDER_2H": (
        "Reminder: your appointment starts in 2 hours", None,
        "This is a friendly reminder that your appointment starts in about 2 hours.", None),
}

NEXT_STEPS = {
    "APPOINTMENT_BOOKED": ("Please arrive 10 minutes early. You can reschedule (up to twice, and not within 6 hours "
                           "of the start) or cancel from My Appointments in the patient portal."),
    "APPOINTMENT_RESCHEDULED": "You can see the updated appointment under My Appointments in the patient portal.",
    "APPOINTMENT_CANCELLED_BY_PATIENT": "Any refund shown above is returned to your original payment method.",
    "APPOINTMENT_CANCELLED_BY_DOCTOR": "You can book another time with the same doctor in the patient portal.",
    "WAITLIST_JOINED": ("If a seat becomes free, it will be held for you and we will write to you straight away. "
                        "You can leave the waitlist at any time from Waitlists in the patient portal."),
    "WAITLIST_OFFER_ISSUED": ("To confirm, open Waitlists in the patient portal and choose Accept before the hold expires. "
                              "If you do not accept in time, the seat passes to the next patient in line."),
    "WAITLIST_OFFER_EXPIRED": "You can join another waitlist or book a different time in the patient portal.",
    "REMINDER_24H": "If you can no longer attend, please cancel from My Appointments so the seat can go to someone else.",
    "REMINDER_2H": "Please plan to arrive 10 minutes early.",
}
DOCTOR_NEXT_STEP = "Your up-to-date schedule is in the doctor portal under My Schedule and Appointments."
DISPLAY_TIMEZONE = os.environ.get("DISPLAY_TIMEZONE", "Asia/Kolkata")


def _when(value, tz_name=None):
    """'Mon, 30 Nov 2026, 4:00 PM (Asia/Kolkata)' from an ISO UTC time, or None."""
    if not value:
        return None
    try:
        instant = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        zone = ZoneInfo(tz_name or DISPLAY_TIMEZONE)
    except (ValueError, KeyError) as exc:
        _log_event("notification_time_unreadable", logging.WARNING, value=str(value)[:40], error_type=type(exc).__name__)
        return str(value)
    local = instant.astimezone(zone)
    return f"{local:%a, %d %b %Y}, {local.strftime('%I:%M %p').lstrip('0')} ({zone.key})"


def _money(value):
    if value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        _log_event("notification_amount_unreadable", logging.WARNING, error_type=type(exc).__name__)
        return str(value)
    return f"₹{amount:,.2f}"


def _doctor(name):
    return f"Dr. {name}" if name and not str(name).startswith("Dr") else name


def compose_email(event_type, recipient_role, recipient_id, detail, appointment=None):
    """(subject, body) of one notification e-mail. `appointment` is the stored appointment, if any."""
    appointment = appointment or {}
    patient_subject, doctor_subject, patient_intro, doctor_intro = TEMPLATES[event_type]
    to_doctor = recipient_role == "DOCTOR"
    subject = f"MediSlot: {doctor_subject if to_doctor else patient_subject}"
    tz_name = detail.get("timezone")

    def pick(*keys):
        for key in keys:
            for source in (detail, appointment):
                if source.get(key) not in (None, ""):
                    return source[key]
        return None

    rows = [("Appointment", f"#{detail['appointment_id']}" if detail.get("appointment_id") else None),
            ("Doctor", None if to_doctor else _doctor(pick("doctor_name"))),
            ("Patient", f"Patient #{pick('patient_id')}" if to_doctor else None),
            ("Consultation", pick("consultation_name")),
            ("New date and time" if event_type == "APPOINTMENT_RESCHEDULED" else "Date and time",
             _when(pick("start_time_utc"), tz_name)),
            ("Previous time", _when(detail.get("previous_start_time_utc"), tz_name)
             if event_type == "APPOINTMENT_RESCHEDULED" else None),
            ("Fee", _money(pick("fee", "fee_amount")))]
    refund = detail.get("refund") or {}
    if event_type.startswith("APPOINTMENT_CANCELLED") and not to_doctor:
        percent = refund.get("percent", appointment.get("refund_percent"))
        amount = refund.get("amount", appointment.get("refund_amount"))
        if amount is not None:
            rows.append(("Refund", f"{_money(amount)} ({float(percent):g}% of the fee)" if percent is not None
                         else _money(amount)))
    if event_type == "WAITLIST_JOINED" and detail.get("position"):
        rows.append(("Your place in line", f"Number {detail['position']}"))
    if event_type == "WAITLIST_OFFER_ISSUED":
        rows.append(("Seat held until", _when(detail.get("expires_at_utc"), tz_name)))
    if event_type == "APPOINTMENT_RESCHEDULED" and detail.get("reschedule_count"):
        rows.append(("Reschedules used", f"{detail['reschedule_count']} of 2"))
    rows = [(label, value) for label, value in rows if value]
    width = max(len(label) for label, _ in rows) + 2 if rows else 0

    intro = doctor_intro if to_doctor else patient_intro
    if event_type == "APPOINTMENT_BOOKED" and not to_doctor and pick("booked_via") == "WAITLIST_OFFER":
        intro = "You accepted the seat that was held for you, and your appointment is now confirmed."
    lines = ["Dear Doctor," if to_doctor else "Dear Patient,", "", intro, ""]
    if rows:
        lines += ["Appointment details", "-" * 19] + [f"  {label + ':':<{width}}{value}" for label, value in rows] + [""]
    lines += [DOCTOR_NEXT_STEP if to_doctor else NEXT_STEPS[event_type], "",
              "Kind regards,", "MediSlot Appointments Team", "",
              "-" * 60,
              "This is an automated message from MediSlot. Please do not reply to it.",
              f"Sent to: {'Doctor' if to_doctor else 'Patient'} #{recipient_id}"]
    return subject[:100], "\n".join(lines)


def _appointment_for(detail):
    """The stored appointment of an event (for the doctor, consultation and fee), or None."""
    appointment_id = detail.get("appointment_id")
    if not appointment_id:
        return None
    try:
        return store.get_appointment(_appointments_table(), appointment_id)
    except Exception as exc:  # the e-mail is still sent, with the details the event carries
        _log_event("notification_appointment_lookup_failed", logging.WARNING, appointment_id=str(appointment_id),
                   error_type=type(exc).__name__)
        return None


# ---------------------------------------------------------------- exactly-once dispatch

def dispatch(dispatch_id, event_type, recipient_role, recipient_id, detail, appointment=None):
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

    subject, body = compose_email(event_type, recipient_role, recipient_id, detail, appointment)
    attributes = {"event_type": event_type, "recipient_role": recipient_role, "recipient_id": str(recipient_id),
                  "dispatch_id": dispatch_id}
    if detail.get("appointment_id"):
        attributes["appointment_id"] = str(detail["appointment_id"])
    try:
        result = _sns_client().publish(
            TopicArn=os.environ["NOTIFICATION_TOPIC_ARN"], Subject=subject, Message=body,
            MessageAttributes={k: {"DataType": "String", "StringValue": v} for k, v in attributes.items()})
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


def handle_event(event):
    """One EventBridge event -> its notifications. Returns how many were sent now."""
    detail_type, detail = event.get("detail-type"), event.get("detail") or {}
    sent = 0
    planned = notifications_for(detail_type, detail)
    appointment = _appointment_for(detail) if planned else None
    for dispatch_id, event_type, role, recipient in planned:
        if recipient is None:
            _log_event("notification_without_recipient", logging.WARNING, dispatch_id=dispatch_id)
            continue
        sent += 1 if dispatch(dispatch_id, event_type, role, recipient, detail, appointment) else 0
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
                if dispatch(dispatch_id, kind, "PATIENT", item["patient_id"], detail, item):
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
