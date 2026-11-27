"""Waitlist entries and hold offers in RDS: the one place that reads and writes them.

Milestone 6. The waitlist lives next to the seat counter (the `slots` table) on purpose: a
held seat and its offer change in ONE MySQL transaction, so they can never disagree.

How a freed seat reaches the waitlist
  1. A seat is given back (slot_store.release_seat). If anybody is WAITING for the slot, the
     same UPDATE moves the seat to slots.held_count, so normal booking cannot take it.
  2. SlotSeatFreed -> EventBridge -> SQS -> waitlist worker calls `issue_offers`, which locks
     the slot row and turns every unassigned held seat into one OFFERED offer for the first
     patient in line. With nobody left in line the seat is returned to normal booking.
  3. The patient accepts (`accept_offer`: held seat -> booked seat, in one statement), or the
     offer expires and the expiry sweeper calls `expire_offer`, which passes the seat on.

Exclusivity
  Every function that changes an offer first locks the slot row (SELECT ... FOR UPDATE).
  Under that lock `issue_offers` only creates an offer while
      live offers of the slot  <  held seats of the slot
  so one freed seat is never offered to two patients at the same time.

The functions take a cursor and never commit: the caller owns the transaction. Times are
naive UTC datetimes (the form MySQL stores) and are always passed in by the caller, so the
sweeper and the tests decide what "now" is.
"""
from datetime import timedelta

WAITING, OFFERED, PROMOTED, EXPIRED, LEFT, CLOSED = "WAITING", "OFFERED", "PROMOTED", "EXPIRED", "LEFT", "CLOSED"
ACTIVE_ENTRY_STATUSES = (WAITING, OFFERED)
OFFER_LIVE, OFFER_ACCEPTED, OFFER_EXPIRED = "OFFERED", "ACCEPTED", "EXPIRED"
OFFER_DECLINED, OFFER_CANCELLED, OFFER_FAILED = "DECLINED", "CANCELLED", "FAILED"


# ---------------------------------------------------------------- the slot row (the lock)

def lock_slot(cursor, slot_id):
    """Lock one slot row for the rest of the transaction and return it (None when it is gone)."""
    cursor.execute("""SELECT slot_id, doctor_id, consultation_type_id, start_time_utc, end_time_utc, capacity,
        booked_count, held_count, status FROM slots WHERE slot_id = %s FOR UPDATE""", (slot_id,))
    return cursor.fetchone()


def slots_with_held_seats(cursor):
    """Ids of slots that hold seats for the waitlist (the sweeper re-checks each of them)."""
    cursor.execute("SELECT slot_id FROM slots WHERE held_count > 0")
    return [row["slot_id"] for row in cursor.fetchall()]


# ---------------------------------------------------------------- entries

def active_entry(cursor, slot_id, patient_id):
    cursor.execute("""SELECT entry_id, status, joined_at FROM waitlist_entries
        WHERE slot_id = %s AND patient_id = %s AND status IN ('WAITING','OFFERED')""", (slot_id, patient_id))
    return cursor.fetchone()


def count_active_for_patient(cursor, patient_id):
    cursor.execute("""SELECT COUNT(*) AS n FROM waitlist_entries
        WHERE patient_id = %s AND status IN ('WAITING','OFFERED')""", (patient_id,))
    return int((cursor.fetchone() or {}).get("n") or 0)


def add_entry(cursor, slot_id, patient_id):
    """Put a patient at the end of the line. Returns the new entry id."""
    cursor.execute("INSERT INTO waitlist_entries (slot_id, patient_id, status) VALUES (%s, %s, 'WAITING')",
                   (slot_id, patient_id))
    return cursor.lastrowid


def waiting_line(cursor, slot_id):
    """Entry ids of everybody still WAITING for a slot, first in line first."""
    cursor.execute("""SELECT entry_id, patient_id FROM waitlist_entries
        WHERE slot_id = %s AND status = 'WAITING' ORDER BY joined_at, entry_id""", (slot_id,))
    return cursor.fetchall()


def set_entry_status(cursor, entry_id, new_status, expected_status):
    """Move one entry between statuses; False when it was no longer in `expected_status`."""
    cursor.execute("UPDATE waitlist_entries SET status = %s WHERE entry_id = %s AND status = %s",
                   (new_status, entry_id, expected_status))
    return cursor.rowcount == 1


def list_for_patient(cursor, patient_id):
    """A patient's active waitlists with the slot, the doctor and any live offer."""
    cursor.execute("""SELECT w.entry_id, w.slot_id, w.status, w.joined_at, o.offer_id, o.expires_at,
        s.doctor_id, s.consultation_type_id, c.consultation_name, s.start_time_utc, s.end_time_utc,
        s.timezone_name, s.fee, d.first_name, d.last_name
        FROM waitlist_entries w
        JOIN slots s ON s.slot_id = w.slot_id
        JOIN consultation_types c ON c.consultation_type_id = s.consultation_type_id
        JOIN doctors d ON d.user_id = s.doctor_id
        LEFT JOIN waitlist_offers o ON o.entry_id = w.entry_id AND o.status = 'OFFERED'
        WHERE w.patient_id = %s AND w.status IN ('WAITING','OFFERED')
        ORDER BY s.start_time_utc, w.entry_id""", (patient_id,))
    return cursor.fetchall()


# ---------------------------------------------------------------- offers

def live_offers(cursor, slot_id):
    cursor.execute("""SELECT offer_id, entry_id, patient_id, expires_at FROM waitlist_offers
        WHERE slot_id = %s AND status = 'OFFERED' ORDER BY offer_id""", (slot_id,))
    return cursor.fetchall()


def get_offer(cursor, offer_id):
    cursor.execute("""SELECT offer_id, slot_id, entry_id, patient_id, status, offered_at, expires_at,
        appointment_id FROM waitlist_offers WHERE offer_id = %s""", (offer_id,))
    return cursor.fetchone()


def live_offer_for_entry(cursor, entry_id):
    cursor.execute("""SELECT offer_id, slot_id, entry_id, patient_id, status, offered_at, expires_at,
        appointment_id FROM waitlist_offers WHERE entry_id = %s AND status = 'OFFERED'""", (entry_id,))
    return cursor.fetchone()


def due_offers(cursor, now, limit=200):
    """Live offers whose hold time is over (oldest first)."""
    cursor.execute("""SELECT offer_id, slot_id FROM waitlist_offers
        WHERE status = 'OFFERED' AND expires_at <= %s ORDER BY expires_at, offer_id LIMIT %s""", (now, limit))
    return cursor.fetchall()


def resolve_offer(cursor, offer_id, new_status, now, appointment_id=None):
    """End a live offer (ACCEPTED / EXPIRED / DECLINED / CANCELLED / FAILED). False when it was not live."""
    cursor.execute("""UPDATE waitlist_offers SET status = %s, resolved_at = %s, appointment_id = %s
        WHERE offer_id = %s AND status = 'OFFERED'""", (new_status, now, appointment_id, offer_id))
    return cursor.rowcount == 1


def _return_held_seats(cursor, slot_id, count):
    """Give `count` held seats back to normal booking."""
    cursor.execute("""UPDATE slots SET held_count = held_count - %s, version = version + 1
        WHERE slot_id = %s AND held_count >= %s""", (count, slot_id, count))
    return cursor.rowcount == 1


def take_held_seat(cursor, slot_id):
    """Accepting an offer: one held seat becomes a booked seat (one atomic statement)."""
    cursor.execute("""UPDATE slots SET held_count = held_count - 1, booked_count = booked_count + 1,
        version = version + 1 WHERE slot_id = %s AND held_count > 0 AND status = 'AVAILABLE'""", (slot_id,))
    return cursor.rowcount == 1


def issue_offers(cursor, slot_id, now, hold_minutes):
    """Give every held seat of a locked slot that has no live offer to the next patient in line.

    The caller must already hold the slot lock (`lock_slot`) in this transaction. Returns
    (issued, returned): the offers created as dicts, and how many held seats went back to
    normal booking because nobody was waiting any more (or the slot can no longer be used).
    """
    slot = lock_slot(cursor, slot_id)
    if slot is None:
        return [], 0
    held = int(slot["held_count"] or 0)
    if slot["status"] != "AVAILABLE" or slot["start_time_utc"] <= now:
        close_waitlist(cursor, slot_id, now)
        return [], held
    unassigned = held - len(live_offers(cursor, slot_id))
    issued = []
    line = list(waiting_line(cursor, slot_id)) if unassigned > 0 else []
    while unassigned > 0 and line:
        entry = line.pop(0)
        if not set_entry_status(cursor, entry["entry_id"], OFFERED, WAITING):
            continue
        expires = min(now + timedelta(minutes=hold_minutes), slot["start_time_utc"])
        cursor.execute("""INSERT INTO waitlist_offers (slot_id, entry_id, patient_id, status, offered_at, expires_at)
            VALUES (%s, %s, %s, 'OFFERED', %s, %s)""", (slot_id, entry["entry_id"], entry["patient_id"], now, expires))
        issued.append({"offer_id": cursor.lastrowid, "slot_id": slot_id, "entry_id": entry["entry_id"],
                       "patient_id": int(entry["patient_id"]), "doctor_id": int(slot["doctor_id"]),
                       "consultation_type_id": int(slot["consultation_type_id"]),
                       "start_time_utc": slot["start_time_utc"], "expires_at": expires})
        unassigned -= 1
    returned = 0
    if unassigned > 0 and _return_held_seats(cursor, slot_id, unassigned):
        returned = unassigned
    return issued, returned


def close_waitlist(cursor, slot_id, now):
    """The slot was cancelled or has started: end its waitlist and give the held seats back."""
    cursor.execute("""UPDATE waitlist_offers SET status = 'CANCELLED', resolved_at = %s
        WHERE slot_id = %s AND status = 'OFFERED'""", (now, slot_id))
    cursor.execute("""UPDATE waitlist_entries SET status = 'CLOSED'
        WHERE slot_id = %s AND status IN ('WAITING','OFFERED')""", (slot_id,))
    cursor.execute("UPDATE slots SET held_count = 0, version = version + 1 WHERE slot_id = %s AND held_count > 0",
                   (slot_id,))


def started_slots_with_waitlist(cursor, now):
    """Slots that have started while patients are still in their line (the sweeper closes them)."""
    cursor.execute("""SELECT DISTINCT w.slot_id FROM waitlist_entries w JOIN slots s ON s.slot_id = w.slot_id
        WHERE w.status IN ('WAITING','OFFERED') AND s.start_time_utc <= %s""", (now,))
    return [row["slot_id"] for row in cursor.fetchall()]
