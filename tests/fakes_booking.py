"""In-memory stand-ins for DynamoDB and MySQL used by the booking engine tests.

FakeTable is a thread-safe DynamoDB table that really evaluates the condition and update
expressions the Lambdas send (attribute_exists / attribute_not_exists, =, <, >, AND, OR,
parentheses, SET with if_not_exists, ADD) and answers index queries (GSIs) the way DynamoDB
does: only items that have the index key attributes appear, ordered by the range key. One lock
makes every call atomic, the guarantee DynamoDB gives for a single-item write.

FakeRds is a small MySQL stand-in with the `slots` table, patients, doctor fees, the refund
ledger and (Milestone 6) the waitlist tables. It answers exactly the statements the Lambdas run.
The seat claim runs the same rule as the real UPDATE (status AVAILABLE, booked_count + held_count
< capacity, version equal) under one lock, which is the atomicity a single MySQL UPDATE statement
has. Row locks (SELECT ... FOR UPDATE on a patient or a slot) are held by the connection until it
commits, rolls back or closes, like InnoDB row locks.

Waitlist invariant: after every change the fake checks that no slot has more live (OFFERED)
offers than held seats, and records the highest number of live offers each slot ever had.
"""
import copy
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pymysql
from botocore.exceptions import ClientError


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


# --------------------------------------------------------------------------- DynamoDB
_TOKEN = re.compile(r"\s*(attribute_not_exists|attribute_exists|if_not_exists|AND|OR|\(|\)|,|<=|>=|<>|=|<|>|\+|-|[#:]?[A-Za-z_][A-Za-z0-9_]*)")


def _tokens(text):
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise ValueError(f"cannot parse {text[pos:]!r}")
        out.append(m.group(1))
        pos = m.end()
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return out


class _Eval:
    def __init__(self, item, names, values):
        self.item, self.names, self.values = item, names or {}, values or {}

    def name(self, tok):
        return self.names.get(tok, tok)

    def operand(self, tok):
        if tok.startswith(":"):
            return self.values[tok]
        return self.item.get(self.name(tok))

    # condition grammar: or_expr := and_expr (OR and_expr)* ; and_expr := term (AND term)*
    def condition(self, text):
        self.t, self.i = _tokens(text), 0
        result = self._or()
        assert self.i == len(self.t), f"trailing tokens in {text!r}"
        return result

    def _peek(self):
        return self.t[self.i] if self.i < len(self.t) else None

    def _next(self):
        tok = self.t[self.i]
        self.i += 1
        return tok

    def _or(self):
        value = self._and()
        while self._peek() == "OR":
            self._next()
            value = self._and() or value
        return value

    def _and(self):
        value = self._term()
        while self._peek() == "AND":
            self._next()
            value = self._term() and value
        return value

    def _term(self):
        tok = self._next()
        if tok == "(":
            value = self._or()
            assert self._next() == ")"
            return value
        if tok == "attribute_not_exists":
            assert self._next() == "("
            attr = self.name(self._next())
            assert self._next() == ")"
            return attr not in self.item
        if tok == "attribute_exists":
            assert self._next() == "("
            attr = self.name(self._next())
            assert self._next() == ")"
            return attr in self.item
        left = self.operand(tok)
        op = self._next()
        right = self.operand(self._next())
        if op == "=":
            return left == right
        if left is None or right is None:
            return False
        return {"<": left < right, ">": left > right, "<=": left <= right, ">=": left >= right,
                "<>": left != right}[op]

    # update grammar: SET a = expr, b = expr ; expr := operand ((+|-) operand)?
    def apply_update(self, text):
        add = re.match(r"^\s*ADD\s+(\S+)\s+(:\w+)\s*$", text)
        if add:
            attr = self.name(add.group(1))
            self.item[attr] = self.item.get(attr, 0) + self.values[add.group(2)]
            return
        removal = re.search(r"\sREMOVE\s+(.*)$", text, flags=re.I)
        if removal:
            text = text[:removal.start()]
            for token in removal.group(1).split(","):
                self.item.pop(self.name(token.strip()), None)
        body = re.sub(r"^\s*SET\s+", "", text, flags=re.I)
        parts, depth, cur = [], 0, ""
        for ch in body:
            depth += (ch == "(") - (ch == ")")
            if ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts.append(cur)
        updates = {}
        for part in parts:
            target, expr = part.split("=", 1)
            self.t, self.i = _tokens(expr), 0
            value = self._value()
            if self._peek() in ("+", "-"):
                op = self._next()
                other = self._value()
                value = value + other if op == "+" else value - other
            updates[self.name(target.strip())] = value
        self.item.update(updates)

    def _value(self):
        tok = self._next()
        if tok == "if_not_exists":
            assert self._next() == "("
            attr = self.name(self._next())
            assert self._next() == ","
            default = self.operand(self._next())
            assert self._next() == ")"
            return self.item.get(attr, default)
        return self.operand(tok)


class FakeTable:
    def __init__(self, key_name, items=None, read_latency=0.0, indexes=None):
        self.key_name, self.items, self.lock = key_name, {}, threading.Lock()
        self.indexes = indexes or {}      # name -> (hash attr, range attr or None)
        self.hidden_from_indexes = {}     # key -> number of index queries that still do not show it (lag)
        self.read_latency = read_latency  # simulated network delay: widens the read-then-write window
        self.calls = {"update_item": 0, "conflicts": 0}
        for item in items or []:
            self.items[item[key_name]] = copy.deepcopy(item)

    def get_item(self, Key, ConsistentRead=False):
        with self.lock:
            item = self.items.get(Key[self.key_name])
            found = {"Item": copy.deepcopy(item)} if item is not None else {}
        if self.read_latency:
            time.sleep(self.read_latency)  # the caller now holds a possibly stale copy
        return found

    def put_item(self, Item, ConditionExpression=None, **kw):
        with self.lock:
            existing = self.items.get(Item[self.key_name])
            if ConditionExpression and not _Eval(existing or {}, kw.get("ExpressionAttributeNames"),
                                                  kw.get("ExpressionAttributeValues")).condition(ConditionExpression):
                raise client_error("ConditionalCheckFailedException")
            self.items[Item[self.key_name]] = copy.deepcopy(Item)

    def update_item(self, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, ReturnValues=None):
        with self.lock:
            self.calls["update_item"] += 1
            item = self.items.get(Key[self.key_name])
            if item is None:
                if ConditionExpression is None and not UpdateExpression.strip().startswith("ADD"):
                    raise client_error("ValidationException")
                item = {}
            ev = _Eval(item, ExpressionAttributeNames, ExpressionAttributeValues)
            if ConditionExpression and not ev.condition(ConditionExpression):
                self.calls["conflicts"] += 1
                raise client_error("ConditionalCheckFailedException")
            ev.apply_update(UpdateExpression)
            item.setdefault(self.key_name, Key[self.key_name])
            self.items[Key[self.key_name]] = item
            return {"Attributes": copy.deepcopy(item)}

    def query(self, IndexName, KeyConditionExpression, ExpressionAttributeValues=None, ExpressionAttributeNames=None,
              FilterExpression=None, ScanIndexForward=True, ExclusiveStartKey=None, **kw):
        hash_attr, range_attr = self.indexes[IndexName]
        names, values = ExpressionAttributeNames or {}, ExpressionAttributeValues or {}
        with self.lock:
            lagging = {k for k, n in self.hidden_from_indexes.items() if n > 0}
            for k in lagging:
                self.hidden_from_indexes[k] -= 1
            rows = [copy.deepcopy(i) for k, i in self.items.items()
                    if hash_attr in i and (range_attr is None or range_attr in i) and k not in lagging]
        condition = KeyConditionExpression
        rows = [r for r in rows if _Eval(r, names, values).condition(
            re.sub(r"(\S+) BETWEEN (:\w+) AND (:\w+)", r"\1 >= \2 AND \1 <= \3", condition))]
        if FilterExpression:
            rows = [r for r in rows if _Eval(r, names, values).condition(FilterExpression)]
        if range_attr:
            rows.sort(key=lambda r: (r[range_attr], r[self.key_name]), reverse=not ScanIndexForward)
        return {"Items": rows}

    def delete_item(self, Key, **kw):
        with self.lock:
            self.items.pop(Key[self.key_name], None)




class FakeDynamo:
    def __init__(self, **tables):
        self.tables = tables

    def Table(self, name):
        return self.tables[name]


def appointments_table(items=None):
    """An appointments table with the four indexes of infrastructure/data.yaml."""
    return FakeTable("appointment_id", items, indexes={
        "PatientIndex": ("patient_id", "start_time_utc"),
        "DoctorIndex": ("doctor_id", "start_time_utc"),
        "SlotIndex": ("slot_id", None),
        "StatusStartIndex": ("status", "start_time_utc")})


# --------------------------------------------------------------------------- MySQL (slots, ledger)

def slot_row(slot_id="7#4#20261201T033000Z", doctor_id=7, capacity=3, booked=0, hours_ahead=72, version=0,
             status="AVAILABLE", ctype=4, fee=Decimal("500.00"), start=None, held=0):
    """One row of the RDS `slots` table in the shape slot_store.SLOT_SELECT returns."""
    start = start or (datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None) + timedelta(hours=hours_ahead))
    return {"slot_id": slot_id, "doctor_id": doctor_id, "slot_date": start.date(), "consultation_type_id": ctype,
            "consultation_name": "General Consultation", "start_time_utc": start,
            "end_time_utc": start + timedelta(hours=1), "timezone_name": "Asia/Kolkata",
            "local_start": start.time(), "duration_minutes": 60, "slot_fee": fee, "current_fee": fee,
            "capacity": capacity, "booked_count": booked, "held_count": held, "version": version, "status": status,
            "availability_pattern_id": 1}


class FakeRds:
    """Shared state of the fake MySQL database; every FakeConn works on it."""

    def __init__(self, patients=(), slots=(), fee=Decimal("500.00"), read_latency=0.0):
        self.lock = threading.Lock()
        self.patient_locks = {int(p): threading.Lock() for p in patients}
        self.slots = {r["slot_id"]: copy.deepcopy(r) for r in slots}
        self.fees = {}
        self.default_fee = fee
        self.ledger = {}
        self.read_latency = read_latency
        self.fail_release = 0     # the next N seat releases raise
        self.statements = []
        for row in self.slots.values():
            row.setdefault("held_count", 0)
        # Milestone 6: waitlist tables and InnoDB-like row locks on slots
        self.slot_locks = {}
        self.entries, self.offers = {}, {}
        self.next_entry = self.next_offer = 1
        self.join_clock = 0
        self.max_live_offers = {}       # slot_id -> highest number of live offers ever seen
        self.invariant_violations = []

    def slot_lock(self, slot_id):
        with self.lock:
            return self.slot_locks.setdefault(slot_id, threading.Lock())

    def check_invariant(self):
        """Called under self.lock after every waitlist change."""
        for slot_id, row in self.slots.items():
            live = sum(1 for o in self.offers.values() if o["slot_id"] == slot_id and o["status"] == "OFFERED")
            self.max_live_offers[slot_id] = max(self.max_live_offers.get(slot_id, 0), live)
            if live > row.get("held_count", 0) or row["booked_count"] + row.get("held_count", 0) > row["capacity"]:
                self.invariant_violations.append((slot_id, live, row.get("held_count"), row["booked_count"]))

    def connect(self):
        return FakeConn(self)

    def fee_for(self, doctor_id, ctype):
        return self.fees.get((doctor_id, ctype), self.default_fee)


class FakeConn:
    def __init__(self, state):
        self.state, self.held = state, []
        self.locked_slots = set()
        self._one, self._all, self.rowcount, self.lastrowid = None, [], 0, None

    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def fetchone(self): return self._one
    def fetchall(self): return self._all

    def _release_locks(self):
        while self.held:
            self.held.pop().release()
        self.locked_slots.clear()

    def commit(self): self._release_locks()
    def rollback(self): self._release_locks()
    def close(self): self._release_locks()


    # ------------------------------------------------------------------ Milestone 6: waitlist
    def _lock_slot_row(self, slot_id):
        if slot_id not in self.locked_slots:
            lock = self.state.slot_lock(slot_id)
            lock.acquire()
            self.held.append(lock)
            self.locked_slots.add(slot_id)

    def _waitlist_sql(self, n, a):
        """Statements of shared/waitlist_store.py. Returns True when the statement was handled."""
        st = self.state
        if n.startswith("SELECT slot_id, doctor_id, consultation_type_id, start_time_utc, end_time_utc, capacity, "
                        "booked_count, held_count, status FROM slots WHERE slot_id = %s FOR UPDATE"):
            self._lock_slot_row(a[0])
            with st.lock:
                row = st.slots.get(a[0])
                self._one = {k: row[k] for k in ("slot_id", "doctor_id", "consultation_type_id", "start_time_utc",
                             "end_time_utc", "capacity", "booked_count", "held_count", "status")} if row else None
            return True
        if n == "SELECT slot_id FROM slots WHERE held_count > 0":
            with st.lock:
                self._all = [{"slot_id": sid} for sid, r in st.slots.items() if r["held_count"] > 0]
            return True
        if n.startswith("SELECT entry_id, status, joined_at FROM waitlist_entries WHERE slot_id = %s AND patient_id = %s"):
            with st.lock:
                rows = [dict(e) for e in st.entries.values() if e["slot_id"] == a[0] and e["patient_id"] == a[1]
                        and e["status"] in ("WAITING", "OFFERED")]
            self._one = rows[0] if rows else None
            return True
        if n.startswith("SELECT COUNT(*) AS n FROM waitlist_entries WHERE patient_id = %s"):
            with st.lock:
                self._one = {"n": sum(1 for e in st.entries.values() if e["patient_id"] == a[0]
                                      and e["status"] in ("WAITING", "OFFERED"))}
            return True
        if n.startswith("INSERT INTO waitlist_entries"):
            with st.lock:
                if any(e["slot_id"] == a[0] and e["patient_id"] == a[1] and e["status"] in ("WAITING", "OFFERED")
                       for e in st.entries.values()):
                    raise pymysql.err.IntegrityError(1062, "Duplicate entry for key uq_waitlist_active")
                entry_id, st.next_entry = st.next_entry, st.next_entry + 1
                st.join_clock += 1
                st.entries[entry_id] = {"entry_id": entry_id, "slot_id": a[0], "patient_id": a[1], "status": "WAITING",
                                        "joined_at": datetime(2026, 1, 1) + timedelta(microseconds=st.join_clock)}
            self.lastrowid, self.rowcount = entry_id, 1
            return True
        if n.startswith("SELECT entry_id, patient_id FROM waitlist_entries WHERE slot_id = %s AND status = 'WAITING'"):
            with st.lock:
                rows = sorted((dict(e) for e in st.entries.values() if e["slot_id"] == a[0] and e["status"] == "WAITING"),
                              key=lambda e: (e["joined_at"], e["entry_id"]))
            self._all = [{"entry_id": e["entry_id"], "patient_id": e["patient_id"]} for e in rows]
            return True
        if n.startswith("UPDATE waitlist_entries SET status = %s WHERE entry_id = %s AND status = %s"):
            with st.lock:
                entry = st.entries.get(a[1])
                if entry and entry["status"] == a[2]:
                    entry["status"] = a[0]
                    self.rowcount = 1
            return True
        if n.startswith("SELECT w.entry_id, w.slot_id, w.status, w.joined_at, o.offer_id"):
            with st.lock:
                rows = []
                for e in st.entries.values():
                    if e["patient_id"] != a[0] or e["status"] not in ("WAITING", "OFFERED"):
                        continue
                    slot = st.slots[e["slot_id"]]
                    live = [o for o in st.offers.values() if o["entry_id"] == e["entry_id"] and o["status"] == "OFFERED"]
                    rows.append({**e, "offer_id": live[0]["offer_id"] if live else None,
                                 "expires_at": live[0]["expires_at"] if live else None,
                                 "doctor_id": slot["doctor_id"], "consultation_type_id": slot["consultation_type_id"],
                                 "consultation_name": slot["consultation_name"], "start_time_utc": slot["start_time_utc"],
                                 "end_time_utc": slot["end_time_utc"], "timezone_name": slot["timezone_name"],
                                 "fee": slot["slot_fee"], "first_name": "Asha", "last_name": "Rao"})
            self._all = sorted(rows, key=lambda r: (r["start_time_utc"], r["entry_id"]))
            return True
        if n.startswith("SELECT offer_id, entry_id, patient_id, expires_at FROM waitlist_offers WHERE slot_id = %s"):
            with st.lock:
                self._all = [dict(o) for o in sorted(st.offers.values(), key=lambda o: o["offer_id"])
                             if o["slot_id"] == a[0] and o["status"] == "OFFERED"]
            return True
        if n.startswith("SELECT offer_id, slot_id, entry_id, patient_id, status, offered_at, expires_at, appointment_id "
                        "FROM waitlist_offers WHERE offer_id = %s"):
            with st.lock:
                offer = st.offers.get(a[0])
                self._one = dict(offer) if offer else None
            return True
        if n.startswith("SELECT offer_id, slot_id, entry_id, patient_id, status, offered_at, expires_at, appointment_id "
                        "FROM waitlist_offers WHERE entry_id = %s AND status = 'OFFERED'"):
            with st.lock:
                rows = [dict(o) for o in st.offers.values() if o["entry_id"] == a[0] and o["status"] == "OFFERED"]
            self._one = rows[0] if rows else None
            return True
        if n.startswith("SELECT offer_id, slot_id FROM waitlist_offers WHERE status = 'OFFERED' AND expires_at <= %s"):
            with st.lock:
                rows = sorted((o for o in st.offers.values() if o["status"] == "OFFERED" and o["expires_at"] <= a[0]),
                              key=lambda o: (o["expires_at"], o["offer_id"]))
            self._all = [{"offer_id": o["offer_id"], "slot_id": o["slot_id"]} for o in rows[:a[1]]]
            return True
        if n.startswith("UPDATE waitlist_offers SET status = %s, resolved_at = %s, appointment_id = %s WHERE offer_id = %s"):
            with st.lock:
                offer = st.offers.get(a[3])
                if offer and offer["status"] == "OFFERED":
                    offer.update(status=a[0], resolved_at=a[1], appointment_id=a[2])
                    self.rowcount = 1
                st.check_invariant()
            return True
        if n.startswith("INSERT INTO waitlist_offers"):
            with st.lock:
                if any(o["entry_id"] == a[1] for o in st.offers.values()):
                    raise pymysql.err.IntegrityError(1062, "Duplicate entry for key uq_offer_entry")
                offer_id, st.next_offer = st.next_offer, st.next_offer + 1
                st.offers[offer_id] = {"offer_id": offer_id, "slot_id": a[0], "entry_id": a[1], "patient_id": a[2],
                                       "status": "OFFERED", "offered_at": a[3], "expires_at": a[4],
                                       "resolved_at": None, "appointment_id": None}
                st.check_invariant()
            self.lastrowid, self.rowcount = offer_id, 1
            return True
        if n.startswith("UPDATE slots SET held_count = held_count - %s, version = version + 1 WHERE slot_id = %s"):
            with st.lock:
                row = st.slots.get(a[1])
                if row and row["held_count"] >= a[2]:
                    row["held_count"] -= a[0]
                    row["version"] += 1
                    self.rowcount = 1
                st.check_invariant()
            return True
        if n.startswith("UPDATE slots SET held_count = held_count - 1, booked_count = booked_count + 1"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["held_count"] > 0 and row["status"] == "AVAILABLE":
                    row["held_count"] -= 1
                    row["booked_count"] += 1
                    row["version"] += 1
                    self.rowcount = 1
            return True
        if n.startswith("UPDATE waitlist_offers SET status = 'CANCELLED', resolved_at = %s WHERE slot_id = %s"):
            with st.lock:
                for o in st.offers.values():
                    if o["slot_id"] == a[1] and o["status"] == "OFFERED":
                        o.update(status="CANCELLED", resolved_at=a[0])
            return True
        if n.startswith("UPDATE waitlist_entries SET status = 'CLOSED' WHERE slot_id = %s"):
            with st.lock:
                for e in st.entries.values():
                    if e["slot_id"] == a[0] and e["status"] in ("WAITING", "OFFERED"):
                        e["status"] = "CLOSED"
            return True
        if n.startswith("UPDATE slots SET held_count = 0, version = version + 1 WHERE slot_id = %s"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["held_count"] > 0:
                    row["held_count"], row["version"] = 0, row["version"] + 1
                    self.rowcount = 1
                st.check_invariant()
            return True
        if n.startswith("SELECT DISTINCT w.slot_id FROM waitlist_entries w JOIN slots s"):
            with st.lock:
                self._all = [{"slot_id": sid} for sid in sorted({e["slot_id"] for e in st.entries.values()
                             if e["status"] in ("WAITING", "OFFERED") and st.slots[e["slot_id"]]["start_time_utc"] <= a[0]})]
            return True
        return False

    def execute(self, sql, args=None):
        n = " ".join(sql.split())
        a = tuple(args or ())
        st = self.state
        self._one, self._all, self.rowcount = None, [], 0
        st.statements.append(n)
        if "FOR UPDATE OF p" in n:
            lock = st.patient_locks.get(int(a[0]))
            if lock is not None:
                lock.acquire()
                self.held.append(lock)
                self._one = {"user_id": int(a[0])}
            return
        if n.startswith("SELECT s.slot_id") and n.endswith("WHERE s.slot_id = %s"):
            with st.lock:
                row = copy.deepcopy(st.slots.get(a[0]))
            if st.read_latency:
                time.sleep(st.read_latency)  # the caller now holds a possibly stale copy
            self._one = row
            return
        if n.startswith("SELECT s.slot_id") and "s.doctor_id = %s AND s.slot_date BETWEEN %s AND %s" in n:
            doctor, first, last, rest = a[0], a[1], a[2], list(a[3:])
            ctype = rest.pop(0) if "AND s.consultation_type_id = %s" in n else None
            now = rest.pop(0) if "s.status = 'AVAILABLE' AND s.start_time_utc >" in n else None
            with st.lock:
                rows = [copy.deepcopy(r) for r in st.slots.values()
                        if r["doctor_id"] == doctor and first <= r["slot_date"] <= last
                        and (ctype is None or r["consultation_type_id"] == ctype)]
            if now is not None:
                rows = [r for r in rows if r["status"] == "AVAILABLE" and r["start_time_utc"] > now]
            else:
                rows = [r for r in rows if r["status"] in ("AVAILABLE", "CANCELLED") or r["booked_count"] > 0]
            self._all = sorted(rows, key=lambda r: (r["start_time_utc"], r["consultation_type_id"]))
            return
        if self._waitlist_sql(n, a):
            return
        if n.startswith("UPDATE slots SET booked_count = booked_count + 1"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["status"] == "AVAILABLE" and row["booked_count"] + row["held_count"] < row["capacity"] \
                        and row["version"] == a[1]:
                    row["booked_count"] += 1
                    row["version"] += 1
                    self.rowcount = 1
            return
        if n.startswith("UPDATE slots s SET s.booked_count = s.booked_count - 1"):
            with st.lock:
                if st.fail_release > 0:
                    st.fail_release -= 1
                    raise RuntimeError("seat release unavailable")
                row = st.slots.get(a[0])
                if row and row["booked_count"] > 0:
                    row["booked_count"] -= 1
                    waiting = any(e["slot_id"] == a[0] and e["status"] == "WAITING" for e in st.entries.values())
                    if row["status"] == "AVAILABLE" and waiting:
                        row["held_count"] += 1  # the seat goes to the waitlist, not to normal booking
                    row["version"] += 1
                    self.rowcount = 1
                st.check_invariant()
            return
        if n.startswith("UPDATE slots SET status = 'CANCELLED'"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["status"] != "CANCELLED":
                    row["status"], row["version"] = "CANCELLED", row["version"] + 1
                    self.rowcount = 1
            return
        if n.startswith("DELETE FROM slots WHERE doctor_id = %s AND slot_date = %s AND booked_count = 0"):
            with st.lock:
                doomed = [sid for sid, r in st.slots.items() if r["doctor_id"] == a[0]
                          and str(r["slot_date"]) == str(a[1]) and r["booked_count"] == 0]
                for sid in doomed:
                    del st.slots[sid]
                    for table in (st.entries, st.offers):  # ON DELETE CASCADE
                        for key in [k for k, v in table.items() if v["slot_id"] == sid]:
                            del table[key]
            self.rowcount = len(doomed)
            return
        if n.startswith("SELECT slot_id, end_time_utc FROM slots WHERE doctor_id = %s AND slot_date = %s"):
            with st.lock:
                rows = sorted((r for r in st.slots.values() if r["doctor_id"] == a[0]
                               and str(r["slot_date"]) == str(a[1]) and r["booked_count"] > 0
                               and r["status"] != "CANCELLED"), key=lambda r: r["start_time_utc"])
            self._all = [{"slot_id": r["slot_id"], "end_time_utc": r["end_time_utc"]} for r in rows]
            return
        if n.startswith("SELECT dct.fee, doc.first_name"):
            doctor, ctype = a
            fee = st.fee_for(doctor, ctype)
            self._one = None if fee is None else {"fee": fee, "first_name": "Asha", "last_name": "Rao"}
            return
        if n.startswith("INSERT INTO refund_ledger"):
            (appointment_id, patient_id, doctor_id, slot_id, start, trigger, tier, percent,
             fee, refund, deduction, hours, currency) = a
            with st.lock:
                if appointment_id in st.ledger:
                    self.rowcount = 0  # ON DUPLICATE KEY UPDATE refund_id = refund_id: nothing changes
                else:
                    st.ledger[appointment_id] = {
                        "refund_id": len(st.ledger) + 1, "appointment_id": appointment_id, "patient_id": patient_id,
                        "doctor_id": doctor_id, "slot_id": slot_id, "scheduled_start_utc": start,
                        "trigger_type": trigger, "tier": tier, "refund_percent": percent, "fee_amount": fee,
                        "refund_amount": refund, "deduction_amount": deduction, "hours_before_start": hours,
                        "currency": currency, "created_at": datetime.now(timezone.utc).replace(tzinfo=None)}
                    self.rowcount = 1
            return
        if n.startswith("DELETE FROM refund_ledger WHERE appointment_id = %s AND trigger_type = %s"):
            with st.lock:
                row = st.ledger.get(a[0])
                if row and row["trigger_type"] == a[1]:
                    del st.ledger[a[0]]
                    self.rowcount = 1
            return
        if n.startswith("SELECT COUNT(*) AS n, COALESCE(SUM(refund_amount), 0) AS total FROM refund_ledger"):
            with st.lock:
                rows = [r for r in st.ledger.values() if r["patient_id"] == a[0]]
            self._one = {"n": len(rows), "total": sum((r["refund_amount"] for r in rows), Decimal("0"))}
            return
        if "FROM refund_ledger r JOIN doctors d" in n:
            patient, size, offset = a
            with st.lock:
                rows = sorted((copy.deepcopy(r) for r in st.ledger.values() if r["patient_id"] == patient),
                              key=lambda r: (r["created_at"], r["refund_id"]), reverse=True)
            for r in rows:
                r["doctor_first_name"], r["doctor_last_name"] = "Asha", "Rao"
            self._all = rows[offset:offset + size]
            return
        raise AssertionError("unexpected SQL in test double: " + n[:120])
