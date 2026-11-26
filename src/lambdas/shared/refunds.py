"""Refund tier calculation and the refund ledger.

Pure calculation (no AWS, no database) so every boundary can be unit tested. The
policy comes from SSM Parameter Store as JSON; DEFAULT_POLICY mirrors the product rules:

    more than 48 h before start       100 %
    24 h up to and including 48 h      75 %
    6 h up to 24 h                     50 %
    less than 6 h                       0 %
    cancelled by the doctor           100 % regardless of timing
    patient marked NO_SHOW              0 % regardless of timing

Boundary rule: a tier matches when the time left is at least its min_hours, or strictly
more than min_hours when the tier has "inclusive": false. So exactly 48 h before the
start is in the 75 % tier ("more than 48 hours" is strictly greater), exactly 24 h is
in the 75 % tier, exactly 6 h is in the 50 % tier, and 5 h 59 m 59 s is in the 0 % tier.
Refunds are only recorded in a ledger; no money moves.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger()

DEFAULT_POLICY = {
    "currency": "INR",
    "tiers": [
        {"name": "GT_48H", "min_hours": 48, "inclusive": False, "percent": 100},
        {"name": "24H_TO_48H", "min_hours": 24, "inclusive": True, "percent": 75},
        {"name": "6H_TO_24H", "min_hours": 6, "inclusive": True, "percent": 50},
        {"name": "LT_6H", "min_hours": 0, "inclusive": True, "percent": 0},
    ],
}

DOCTOR_CANCEL_TIER = "DOCTOR_CANCELLED"
NO_SHOW_TIER = "NO_SHOW"
TWO_PLACES = Decimal("0.01")


class PolicyError(ValueError):
    """The refund policy in configuration is not usable."""


@dataclass(frozen=True)
class RefundResult:
    tier: str
    percent: Decimal
    fee_amount: Decimal
    refund_amount: Decimal
    deduction_amount: Decimal
    hours_before_start: Decimal


def money(value):
    return Decimal(str(value)).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


def parse_policy(text):
    """Parse and validate the JSON policy. Raises PolicyError when it is unusable."""
    try:
        policy = json.loads(text) if isinstance(text, str) else dict(text)
    except (TypeError, ValueError) as exc:
        logger.warning(json.dumps({"event": "refund_policy_unreadable", "error": str(exc)[:200]}))
        raise PolicyError(f"refund policy is not valid JSON: {exc}") from exc
    tiers = policy.get("tiers")
    if not isinstance(tiers, list) or not tiers:
        raise PolicyError("refund policy needs at least one tier")
    cleaned, previous = [], None
    for tier in tiers:
        try:
            name = str(tier["name"]).strip()
            min_hours = Decimal(str(tier["min_hours"]))
            percent = Decimal(str(tier["percent"]))
        except (KeyError, ArithmeticError, TypeError) as exc:
            logger.warning(json.dumps({"event": "refund_policy_tier_invalid", "error_type": type(exc).__name__}))
            raise PolicyError(f"invalid refund tier {tier!r}") from exc
        if not name or min_hours < 0 or not Decimal(0) <= percent <= Decimal(100):
            raise PolicyError(f"invalid refund tier {tier!r}")
        if previous is not None and min_hours >= previous:
            raise PolicyError("refund tiers must be listed from the longest to the shortest notice")
        previous = min_hours
        cleaned.append({"name": name, "min_hours": min_hours, "percent": percent,
                        "inclusive": bool(tier.get("inclusive", True))})
    if cleaned[-1]["min_hours"] != 0 or not cleaned[-1]["inclusive"]:
        raise PolicyError("the last refund tier must start at 0 hours (inclusive) so every cancellation matches")
    return {"currency": str(policy.get("currency", "INR")), "tiers": cleaned}


def calculate_refund(fee_amount, appointment_start, cancelled_at, policy):
    """Refund for a patient cancellation. Datetimes must both be UTC (naive or aware alike).

    The tier is chosen on the exact time left (to the microsecond); the amount is
    fee * percent / 100, rounded half-up to two decimals, and the deduction is the rest.
    """
    if getattr(appointment_start, "tzinfo", None) is not None:
        appointment_start = appointment_start.replace(tzinfo=None)
    if getattr(cancelled_at, "tzinfo", None) is not None:
        cancelled_at = cancelled_at.replace(tzinfo=None)
    fee = money(fee_amount)
    time_left = appointment_start - cancelled_at
    for tier in policy["tiers"]:
        limit = timedelta(hours=float(tier["min_hours"]))
        matched = time_left >= limit if tier["inclusive"] else time_left > limit
        if matched:
            refund = money(fee * tier["percent"] / Decimal(100))
            return RefundResult(tier["name"], tier["percent"], fee, refund, fee - refund,
                                Decimal(str(round(time_left.total_seconds() / 3600, 2))))
    # Only reachable when the appointment already started (negative time left).
    return RefundResult(policy["tiers"][-1]["name"], policy["tiers"][-1]["percent"], fee,
                        money(fee * policy["tiers"][-1]["percent"] / Decimal(100)),
                        fee - money(fee * policy["tiers"][-1]["percent"] / Decimal(100)),
                        Decimal(str(round(time_left.total_seconds() / 3600, 2))))


def full_refund(fee_amount, tier=DOCTOR_CANCEL_TIER, hours_before_start=None):
    fee = money(fee_amount)
    return RefundResult(tier, Decimal(100), fee, fee, Decimal("0.00"),
                        Decimal(str(hours_before_start)) if hours_before_start is not None else Decimal("0.00"))


def no_show_refund(fee_amount):
    fee = money(fee_amount)
    return RefundResult(NO_SHOW_TIER, Decimal(0), fee, Decimal("0.00"), fee, Decimal("0.00"))


LEDGER_INSERT = """INSERT INTO refund_ledger
    (appointment_id, patient_id, doctor_id, slot_id, scheduled_start_utc, trigger_type, tier, refund_percent,
     fee_amount, refund_amount, deduction_amount, hours_before_start, currency)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE refund_id = refund_id"""


def record_refund(cursor, appointment, trigger_type, result, currency="INR"):
    """Write the ledger row for one appointment (a DynamoDB item). Replaying it is harmless.

    The ledger has one row per appointment (unique key), so cancelling twice, or finishing a
    cancellation that failed half-way, never refunds twice. The slot and start time are
    copied so the patient's refund list needs no appointment lookup.
    """
    start = datetime.fromisoformat(str(appointment["start_time_utc"]).replace("Z", "+00:00"))
    start = start.astimezone(timezone.utc).replace(tzinfo=None)
    cursor.execute(LEDGER_INSERT, (
        int(appointment["appointment_id"]), int(appointment["patient_id"]), int(appointment["doctor_id"]),
        appointment["slot_id"], start, trigger_type, result.tier, result.percent,
        result.fee_amount, result.refund_amount, result.deduction_amount,
        result.hours_before_start, currency))


def delete_ledger_row(cursor, appointment_id, trigger_type):
    """Remove a ledger row of one trigger type (a corrected no-show). Other trigger types are never touched."""
    cursor.execute("DELETE FROM refund_ledger WHERE appointment_id = %s AND trigger_type = %s",
                   (int(appointment_id), trigger_type))
    return cursor.rowcount


def refund_fields(result, currency="INR"):
    """The refund outcome as strings, ready to be stored on the DynamoDB appointment item."""
    return {"refund_tier": result.tier, "refund_percent": str(result.percent),
            "fee_amount": str(result.fee_amount), "refund_amount": str(result.refund_amount),
            "deduction_amount": str(result.deduction_amount),
            "hours_before_start": str(result.hours_before_start), "currency": currency}


def result_from_item(item):
    """Rebuild the RefundResult stored on an appointment item (used to finish an interrupted cancel)."""
    return RefundResult(
        tier=item["refund_tier"], percent=Decimal(str(item["refund_percent"])),
        fee_amount=Decimal(str(item["fee_amount"])), refund_amount=Decimal(str(item["refund_amount"])),
        deduction_amount=Decimal(str(item["deduction_amount"])),
        hours_before_start=Decimal(str(item["hours_before_start"])))
