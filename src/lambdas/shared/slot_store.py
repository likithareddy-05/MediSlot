"""Slots in RDS: the one place that reads and writes the `slots` table.

Milestone 5 keeps slots in MySQL and appointments in DynamoDB. Everything that needs a
slot (the doctor's week schedule, the patient's slot browser, the booking engine and the
slot materialiser) goes through this module, so there is exactly one query for viewing
slots and one set of statements for changing them.

`get_slots` is the single function used to VIEW slots. The doctor and the patient views
differ only in the `viewer` argument, which decides which slots are included.

The functions take a cursor and never commit: the caller owns the transaction. They do not
catch exceptions either; the Lambda that calls them logs and decides what to do.
"""
from datetime import datetime, timezone

VIEWER_DOCTOR = "DOCTOR"
VIEWER_PATIENT = "PATIENT"

SLOT_SELECT = """SELECT s.slot_id, s.doctor_id, s.slot_date, s.consultation_type_id, c.consultation_name,
    s.start_time_utc, s.end_time_utc, s.timezone_name, s.local_start, s.duration_minutes,
    s.fee AS slot_fee, d.fee AS current_fee, s.capacity, s.booked_count, s.version, s.status,
    s.availability_pattern_id
    FROM slots s
    JOIN consultation_types c ON c.consultation_type_id = s.consultation_type_id
    JOIN doctor_consultation_types d
      ON d.doctor_id = s.doctor_id AND d.consultation_type_id = s.consultation_type_id"""


def iso_utc(value):
    """A MySQL naive-UTC datetime as an ISO text ending in Z (None stays None)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if value.endswith("Z") else value.replace(" ", "T") + "Z"
    return value.replace(microsecond=0).isoformat() + "Z"


def to_utc_naive(value):
    """An aware or naive datetime as naive UTC, the form MySQL stores."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _present(row):
    """One slots row as the dict both dashboards receive."""
    capacity, booked = int(row["capacity"]), int(row["booked_count"])
    fee = row.get("current_fee") if row.get("current_fee") is not None else row.get("slot_fee")
    return {
        "slot_id": row["slot_id"],
        "doctor_id": int(row["doctor_id"]),
        "slot_date": row["slot_date"].isoformat() if hasattr(row["slot_date"], "isoformat") else str(row["slot_date"]),
        "consultation_type_id": int(row["consultation_type_id"]),
        "consultation_name": row["consultation_name"],
        "start_time_utc": iso_utc(row["start_time_utc"]),
        "end_time_utc": iso_utc(row["end_time_utc"]),
        "timezone": row["timezone_name"],
        "local_start": row["local_start"].isoformat() if hasattr(row["local_start"], "isoformat") else str(row["local_start"]),
        "duration_minutes": int(row["duration_minutes"]),
        "fee": float(fee),
        "capacity": capacity,
        "booked_count": booked,
        "free_seats": max(capacity - booked, 0),
        "version": int(row["version"]),
        "status": row["status"],
        "availability_pattern_id": row.get("availability_pattern_id"),
    }


def get_slots(cursor, doctor_id, first_day, last_day, viewer, consultation_type_id=None, now=None):
    """The ONE function that lists a doctor's slots for a range of local dates.

    first_day and last_day are inclusive dates (the doctor's local calendar days). `viewer`
    decides what is included:

    * PATIENT: only slots a patient could book right now, meaning status AVAILABLE and not
      yet started. Full slots are included (marked full) so the patient sees they exist.
    * DOCTOR: the doctor's own view: available and cancelled slots, plus any slot that
      holds patients even if a blackout or edit withdrew it. Past slots stay visible.

    Returns a list of dicts ordered by start time. Nothing is committed.
    """
    if viewer not in (VIEWER_DOCTOR, VIEWER_PATIENT):
        raise ValueError("viewer must be DOCTOR or PATIENT")
    sql = SLOT_SELECT + " WHERE s.doctor_id = %s AND s.slot_date BETWEEN %s AND %s"
    args = [doctor_id, first_day, last_day]
    if consultation_type_id is not None:
        sql += " AND s.consultation_type_id = %s"
        args.append(consultation_type_id)
    if viewer == VIEWER_PATIENT:
        sql += " AND s.status = 'AVAILABLE' AND s.start_time_utc > %s"
        args.append(to_utc_naive(now or datetime.now(timezone.utc)))
    else:
        sql += " AND (s.status IN ('AVAILABLE','CANCELLED') OR s.booked_count > 0)"
    sql += " ORDER BY s.start_time_utc, s.consultation_type_id"
    cursor.execute(sql, args)
    return [_present(row) for row in cursor.fetchall()]


def get_slot(cursor, slot_id):
    """One slot by id (any status), or None. Used by the booking engine and slot cancel."""
    cursor.execute(SLOT_SELECT + " WHERE s.slot_id = %s", (slot_id,))
    row = cursor.fetchone()
    return _present(row) if row else None


def existing_slots(cursor, doctor_id, first_day, last_day):
    """Raw state of every stored slot in a date range, keyed by slot_id (materialiser)."""
    cursor.execute("""SELECT slot_id, slot_date, status, booked_count, start_time_utc, availability_pattern_id,
        created_at FROM slots WHERE doctor_id = %s AND slot_date BETWEEN %s AND %s""", (doctor_id, first_day, last_day))
    return {row["slot_id"]: row for row in cursor.fetchall()}


INSERT_SLOT = """INSERT INTO slots (slot_id, doctor_id, consultation_type_id, availability_pattern_id, slot_date,
    start_time_utc, end_time_utc, timezone_name, local_start, duration_minutes, fee, capacity, booked_count,
    version, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,'AVAILABLE')
    ON DUPLICATE KEY UPDATE slot_id = slot_id"""


def insert_slot_if_absent(cursor, slot):
    """Create a slot unless its deterministic id already exists. True when a row was created.

    An existing slot (and its booking counter) is never touched.
    """
    cursor.execute(INSERT_SLOT, (
        slot["slot_id"], slot["doctor_id"], slot["consultation_type_id"], slot["availability_pattern_id"],
        slot["slot_date"], to_utc_naive(slot["start_utc"]), to_utc_naive(slot["end_utc"]), slot["timezone_name"],
        slot["local_start"], slot["duration_minutes"], slot["fee"], slot["capacity"]))
    return cursor.rowcount == 1


def set_status_if_unbooked(cursor, slot_id, new_status, expected_status):
    """Change status only while the slot is still empty and still in `expected_status`."""
    cursor.execute("""UPDATE slots SET status = %s, version = version + 1
        WHERE slot_id = %s AND status = %s AND booked_count = 0""", (new_status, slot_id, expected_status))
    return cursor.rowcount == 1


def delete_if_unbooked(cursor, slot_id, statuses):
    """Delete an empty slot. Refuses when a patient booked it in the meantime."""
    marks = ",".join(["%s"] * len(statuses))
    cursor.execute(f"DELETE FROM slots WHERE slot_id = %s AND booked_count = 0 AND status IN ({marks})",
                   (slot_id, *statuses))
    return cursor.rowcount == 1


def repoint_pattern(cursor, slot_id, pattern_id):
    cursor.execute("UPDATE slots SET availability_pattern_id = %s WHERE slot_id = %s AND status = 'AVAILABLE'",
                   (pattern_id, slot_id))
    return cursor.rowcount == 1


def close_slot(cursor, slot_id):
    """The doctor cancels a slot. Bumping the version makes any booking that read the old row fail."""
    cursor.execute("""UPDATE slots SET status = 'CANCELLED', version = version + 1
        WHERE slot_id = %s AND status <> 'CANCELLED'""", (slot_id,))
    return cursor.rowcount == 1


def delete_empty_slots_on_day(cursor, doctor_id, day):
    """Delete every slot of a doctor on one local date that nobody has booked. Returns how many.

    Used when a blackout date is added. The DELETE is conditional on booked_count = 0, so a slot
    a patient claimed a moment ago is never removed (it is cancelled with refunds instead).
    """
    cursor.execute("DELETE FROM slots WHERE doctor_id = %s AND slot_date = %s AND booked_count = 0",
                   (doctor_id, day))
    return cursor.rowcount


def booked_slots_on_day(cursor, doctor_id, day):
    """Slots on one local date that still hold patients and are not cancelled yet: [(slot_id, end_utc)]."""
    cursor.execute("""SELECT slot_id, end_time_utc FROM slots
        WHERE doctor_id = %s AND slot_date = %s AND booked_count > 0 AND status <> 'CANCELLED'
        ORDER BY start_time_utc""", (doctor_id, day))
    return [(row["slot_id"], row["end_time_utc"]) for row in cursor.fetchall()]


def claim_seat(cursor, slot_id, expected_version):
    """Optimistic concurrency control: take one seat if nobody changed the slot since it was read.

    One conditional UPDATE. True means the seat is ours; False means the slot changed
    (somebody else booked, the doctor cancelled it, ...) or is full, and the caller must
    re-read and decide.
    """
    cursor.execute("""UPDATE slots SET booked_count = booked_count + 1, version = version + 1
        WHERE slot_id = %s AND status = 'AVAILABLE' AND booked_count < capacity AND version = %s""",
                   (slot_id, expected_version))
    return cursor.rowcount == 1


def release_seat(cursor, slot_id):
    """Give one seat back. The decrement is atomic in SQL, so no version check is needed."""
    cursor.execute("""UPDATE slots SET booked_count = booked_count - 1, version = version + 1
        WHERE slot_id = %s AND booked_count > 0""", (slot_id,))
    return cursor.rowcount == 1
