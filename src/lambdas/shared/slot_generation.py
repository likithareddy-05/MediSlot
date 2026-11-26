"""Slot generation: turns availability patterns into rows of the RDS `slots` table.

Used by BOTH the doctor-domain Lambda (after an availability change) and the scheduled
slot-materializer Lambda, so there is exactly one implementation.

Rules
  * Idempotent: slot ids are deterministic (doctor#consultation#UTC start).
  * Existing slots are never overwritten; booking counters are untouched.
  * Nothing is created for dates before today (the doctor's local date), and on
    today nothing is created for a start time that has already passed.
  * An unbooked slot that was created after its own start time (it could never be
    booked) is deleted on the next run.
  * Unbooked slots from today on (even ones whose time already passed) that no
    active pattern produces any more are deleted;
    slots that hold patients are kept.
  * Wall-clock times are interpreted in the pattern's IANA zone. Nonexistent
    spring-forward times are skipped, ambiguous fall-back times use the first occurrence.
"""
import json
import logging
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import slot_store
from slot_time import valid_local_time

logger = logging.getLogger()


def _as_time(value):
    """Normalize MySQL TIME values (PyMySQL may return timedelta) to time."""
    if isinstance(value, time):
        return value
    if isinstance(value, timedelta):
        seconds = int(value.total_seconds()) % 86400
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)
    return time.fromisoformat(str(value))


def _created_after_start(row):
    """True when a stored slot row was created at or after its own start (both naive UTC)."""
    created, start = row.get("created_at"), row.get("start_time_utc")
    if not hasattr(created, "year") or not hasattr(start, "hour"):
        return False  # missing or unexpected value from the driver: keep the slot
    return created >= start


PATTERN_SQL = """SELECT a.availability_pattern_id,a.consultation_type_id,a.weekday,
    a.start_time,a.end_time,a.timezone_name,a.effective_from,a.effective_to,
    c.consultation_name,c.duration_minutes,d.fee,d.slot_capacity
    FROM availability_patterns a
    JOIN doctor_consultation_types d
      ON d.doctor_id=a.doctor_id AND d.consultation_type_id=a.consultation_type_id
    JOIN consultation_types c ON c.consultation_type_id=d.consultation_type_id
    WHERE a.doctor_id=%s AND a.is_active=1 AND d.is_active=1"""


def materialize_doctor_slots(conn, doctor_id, start_day=None, days=30):
    """Create or refresh one doctor's slots for `days` days from start_day. Returns a summary dict."""
    created = skipped = removed = restored = past_skipped = 0
    with conn.cursor() as cur:
        cur.execute(PATTERN_SQL, (doctor_id,))
        patterns = cur.fetchall()
        local_today = (datetime.now(ZoneInfo(patterns[0]["timezone_name"])).date()
                       if patterns else date.today())
        # Never create slots for dates before today (doctor's local date).
        if start_day is None or start_day < local_today:
            start_day = local_today
        last_day = start_day + timedelta(days=days - 1)
        cur.execute("SELECT blackout_date FROM doctor_blackouts WHERE doctor_id=%s AND blackout_date BETWEEN %s AND %s",
                    (doctor_id, start_day, last_day))
        blackouts = {r["blackout_date"] for r in cur.fetchall()}
        stored = slot_store.existing_slots(cur, doctor_id, start_day, last_day)
    conn.commit()

    by_day = {}
    for sid, row in stored.items():
        day = row["slot_date"] if isinstance(row["slot_date"], date) else date.fromisoformat(str(row["slot_date"]))
        by_day.setdefault(day, {})[sid] = row
    now_utc = datetime.now(timezone.utc)

    for offset in range(days):
        day = start_day + timedelta(days=offset)
        existing = by_day.get(day, {})
        desired = set()
        with conn.cursor() as cur:
            if day not in blackouts:
                for p in patterns:
                    if day.weekday() != int(p["weekday"]):
                        continue
                    if day < p["effective_from"] or (p["effective_to"] and day > p["effective_to"]):
                        continue
                    zone = ZoneInfo(p["timezone_name"])
                    duration = int(p["duration_minutes"])
                    local_end = datetime.combine(day, _as_time(p["end_time"]))
                    cursor_time = datetime.combine(day, _as_time(p["start_time"]))
                    while cursor_time + timedelta(minutes=duration) <= local_end:
                        aware_start = valid_local_time(cursor_time, zone)
                        aware_end = valid_local_time(cursor_time + timedelta(minutes=duration), zone)
                        if aware_start is None or aware_end is None:
                            skipped += 1
                            cursor_time += timedelta(minutes=duration)
                            continue
                        start_utc = aware_start.astimezone(timezone.utc)
                        end_utc = start_utc + timedelta(minutes=duration)  # elapsed time, DST safe
                        slot_id = f"{doctor_id}#{p['consultation_type_id']}#{start_utc.strftime('%Y%m%dT%H%M%SZ')}"
                        desired.add(slot_id)
                        current = existing.get(slot_id)
                        if start_utc <= now_utc:
                            # Start time already passed: never create or restore it. A slot that was
                            # created after it started could never be booked, so remove it.
                            if (current is not None and current["status"] == "AVAILABLE"
                                    and int(current["booked_count"]) == 0
                                    and _created_after_start(current)
                                    and slot_store.delete_if_unbooked(cur, slot_id, ("AVAILABLE",))):
                                removed += 1
                            past_skipped += 1
                            cursor_time += timedelta(minutes=duration)
                            continue
                        if current is not None:
                            if current["status"] in ("REMOVED", "BLACKED_OUT") and int(current["booked_count"]) == 0:
                                if slot_store.set_status_if_unbooked(cur, slot_id, "AVAILABLE", current["status"]):
                                    restored += 1
                            # Same slot still wanted after an edit: point it at the current pattern.
                            if (current["status"] == "AVAILABLE"
                                    and current.get("availability_pattern_id") != int(p["availability_pattern_id"])):
                                slot_store.repoint_pattern(cur, slot_id, int(p["availability_pattern_id"]))
                        elif slot_store.insert_slot_if_absent(cur, {
                                "slot_id": slot_id, "doctor_id": doctor_id,
                                "consultation_type_id": int(p["consultation_type_id"]),
                                "availability_pattern_id": int(p["availability_pattern_id"]),
                                "slot_date": day, "start_utc": start_utc, "end_utc": end_utc,
                                "timezone_name": p["timezone_name"], "local_start": cursor_time.time(),
                                "duration_minutes": duration, "fee": p["fee"], "capacity": int(p["slot_capacity"])}):
                            created += 1
                        cursor_time += timedelta(minutes=duration)
                # Withdraw unbooked slots (from today on, including ones earlier today whose time
                # has passed) that no active pattern produces any more, e.g. after a pattern edit.
                # Slots holding patients are always kept.
                # Blackouts are handled by the blackout endpoint (status BLACKED_OUT).
                for sid, row in existing.items():
                    if sid in desired or row["status"] not in ("AVAILABLE", "REMOVED"):
                        continue
                    if int(row["booked_count"]) != 0:
                        continue
                    if slot_store.delete_if_unbooked(cur, sid, ("AVAILABLE", "REMOVED")):
                        removed += 1
        conn.commit()

    logger.log(logging.WARNING if skipped else logging.INFO, json.dumps({
        "event": "materialize_doctor_completed", "doctor_id": doctor_id, "start_date": start_day.isoformat(),
        "days": days, "patterns": len(patterns), "blackout_days": len(blackouts), "created": created,
        "restored": restored, "removed": removed, "dst_wall_times_skipped": skipped,
        "past_times_skipped": past_skipped}))
    return {"doctor_id": doctor_id, "start_date": start_day.isoformat(), "days": days,
            "created": created, "restored": restored, "removed": removed,
            "dst_wall_times_skipped": skipped, "past_times_skipped": past_skipped, "idempotent": True}
