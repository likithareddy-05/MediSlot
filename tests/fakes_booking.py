"""In-memory stand-ins for DynamoDB and MySQL used by the booking engine tests.

FakeTable is a thread-safe DynamoDB table that really evaluates the condition and update
expressions the Lambdas send (attribute_exists / attribute_not_exists, =, <, >, AND, OR,
parentheses, SET with if_not_exists, ADD) and answers index queries (GSIs) the way DynamoDB
does: only items that have the index key attributes appear, ordered by the range key. One lock
makes every call atomic, the guarantee DynamoDB gives for a single-item write.

FakeRds is a small MySQL stand-in with the `slots` table, patients, doctor fees and the refund
ledger. It answers exactly the statements the Lambdas run. The seat claim runs the same rule as
the real UPDATE (status AVAILABLE, booked_count < capacity, version equal) under one lock, which
is the atomicity a single MySQL UPDATE statement has. A patient row lock (SELECT ... FOR UPDATE)
is held by the connection until it commits, rolls back or closes.
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
    """An appointments table with the three indexes of infrastructure/data.yaml."""
    return FakeTable("appointment_id", items, indexes={
        "PatientIndex": ("patient_id", "start_time_utc"),
        "DoctorIndex": ("doctor_id", "start_time_utc"),
        "SlotIndex": ("slot_id", None)})


# --------------------------------------------------------------------------- MySQL (slots, ledger)

def slot_row(slot_id="7#4#20261201T033000Z", doctor_id=7, capacity=3, booked=0, hours_ahead=72, version=0,
             status="AVAILABLE", ctype=4, fee=Decimal("500.00"), start=None):
    """One row of the RDS `slots` table in the shape slot_store.SLOT_SELECT returns."""
    start = start or (datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None) + timedelta(hours=hours_ahead))
    return {"slot_id": slot_id, "doctor_id": doctor_id, "slot_date": start.date(), "consultation_type_id": ctype,
            "consultation_name": "General Consultation", "start_time_utc": start,
            "end_time_utc": start + timedelta(hours=1), "timezone_name": "Asia/Kolkata",
            "local_start": start.time(), "duration_minutes": 60, "slot_fee": fee, "current_fee": fee,
            "capacity": capacity, "booked_count": booked, "version": version, "status": status,
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

    def connect(self):
        return FakeConn(self)

    def fee_for(self, doctor_id, ctype):
        return self.fees.get((doctor_id, ctype), self.default_fee)


class FakeConn:
    def __init__(self, state):
        self.state, self.held = state, []
        self._one, self._all, self.rowcount = None, [], 0

    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def fetchone(self): return self._one
    def fetchall(self): return self._all

    def _release_locks(self):
        while self.held:
            self.held.pop().release()

    def commit(self): self._release_locks()
    def rollback(self): self._release_locks()
    def close(self): self._release_locks()

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
        if n.startswith("UPDATE slots SET booked_count = booked_count + 1"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["status"] == "AVAILABLE" and row["booked_count"] < row["capacity"] \
                        and row["version"] == a[1]:
                    row["booked_count"] += 1
                    row["version"] += 1
                    self.rowcount = 1
            return
        if n.startswith("UPDATE slots SET booked_count = booked_count - 1"):
            with st.lock:
                if st.fail_release > 0:
                    st.fail_release -= 1
                    raise RuntimeError("seat release unavailable")
                row = st.slots.get(a[0])
                if row and row["booked_count"] > 0:
                    row["booked_count"] -= 1
                    row["version"] += 1
                    self.rowcount = 1
            return
        if n.startswith("UPDATE slots SET status = 'CANCELLED'"):
            with st.lock:
                row = st.slots.get(a[0])
                if row and row["status"] != "CANCELLED":
                    row["status"], row["version"] = "CANCELLED", row["version"] + 1
                    self.rowcount = 1
            return
        if n.startswith("SELECT COUNT(*) AS n, COALESCE(SUM(booked_count > 0)"):
            with st.lock:
                rows = [r for r in st.slots.values() if r["doctor_id"] == a[0] and str(r["slot_date"]) == str(a[1])
                        and r["status"] not in ("CANCELLED", "BLACKED_OUT")]
            self._one = {"n": len(rows), "booked": sum(1 for r in rows if r["booked_count"] > 0)}
            return
        if n.startswith("UPDATE slots SET status = 'BLACKED_OUT'"):
            with st.lock:
                for r in st.slots.values():
                    if r["doctor_id"] == a[0] and str(r["slot_date"]) == str(a[1]) \
                            and r["status"] not in ("CANCELLED", "BLACKED_OUT"):
                        r["status"], r["version"] = "BLACKED_OUT", r["version"] + 1
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
