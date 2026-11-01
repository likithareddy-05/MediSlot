"""Timezone-safe slot calculations shared by the materialiser and tests."""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def valid_local_time(local_naive, zone):
    """Resolve a wall time to a real instant; skip gaps and choose fold=0 overlaps."""
    candidate = local_naive.replace(tzinfo=zone, fold=0)
    utc = candidate.astimezone(timezone.utc)
    if utc.astimezone(zone).replace(tzinfo=None) != local_naive:
        return None
    return candidate


def build_slots(day, start_time, end_time, duration_minutes, timezone_name):
    """Yield local/UTC slot boundaries aligned to the pattern's local start.

    Slot duration is elapsed time. End instants are calculated in UTC so a
    consultation crossing a DST transition remains exactly duration_minutes.
    Nonexistent local start times are skipped; ambiguous starts use fold=0.
    """
    if not isinstance(day, date):
        raise TypeError("day must be a datetime.date")
    if isinstance(start_time, str):
        start_time = time.fromisoformat(start_time)
    if isinstance(end_time, str):
        end_time = time.fromisoformat(end_time)
    if not isinstance(start_time, time) or not isinstance(end_time, time):
        raise TypeError("start_time and end_time must be time values or ISO time strings")
    if end_time <= start_time:
        raise ValueError("end_time must be after start_time")
    if not isinstance(duration_minutes, int) or duration_minutes <= 0:
        raise ValueError("duration_minutes must be a positive integer")

    zone = ZoneInfo(timezone_name)
    cursor = datetime.combine(day, start_time)
    finish = datetime.combine(day, end_time)
    finish_local = valid_local_time(finish, zone)
    if finish_local is None:
        # A nonexistent pattern boundary cannot define a safe booking window.
        return
    finish_utc = finish_local.astimezone(timezone.utc)
    step = timedelta(minutes=duration_minutes)

    while cursor < finish:
        local_start = valid_local_time(cursor, zone)
        if local_start is not None:
            utc_start = local_start.astimezone(timezone.utc)
            utc_end = utc_start + step
            if utc_end <= finish_utc:
                local_end = utc_end.astimezone(zone)
                yield local_start, local_end, utc_start, utc_end
        cursor += step


def put_if_absent(table, item):
    """Conditionally insert a deterministic slot; return False if it exists."""
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(slot_id)")
        return True
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") == "ConditionalCheckFailedException":
            return False
        raise
