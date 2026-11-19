"""Milestone 5: booking engine, concurrency, idempotency, cancellation and refunds.

Architecture under test: slots in RDS (MySQL), appointments in DynamoDB.

Mandatory test 1 (concurrency): 24 patients book one capacity-3 slot at the same moment;
exactly 3 succeed, the slot ends at 3 booked and 3 CONFIRMED appointments exist.
Mandatory test 2 (idempotency): the same Idempotency-Key sent repeatedly (even in parallel)
creates exactly one appointment and returns the original one every time.

The stand-ins in fakes_booking.py run the real conditional-write rules (DynamoDB conditions,
the MySQL compare-and-set on the slot's version), so these tests exercise the actual booking
code, not a mock of it.
"""
import hashlib
import json
import os
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "patient"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {"AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                   "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
                   "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments",
                   "ASSETS_BUCKET": "bucket", "IDEMPOTENCY_TABLE_NAME": "idem"}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import booking_domain as bd  # noqa: E402
from fakes_booking import FakeDynamo, FakeRds, FakeTable, appointments_table, slot_row  # noqa: E402

SLOT_ID = "7#4#20261201T033000Z"


def make_event(method, path, patient_id, body=None, key=None, query=None, role="PATIENT"):
    headers = {"Idempotency-Key": key} if key else {}
    return {"httpMethod": method, "path": path, "headers": headers, "queryStringParameters": query,
            "body": json.dumps(body) if body is not None else None,
            "requestContext": {"stage": "dev", "authorizer": {"userId": str(patient_id), "role": role}}}


def request_hash(slot_id):
    return hashlib.sha256(json.dumps({"slot_id": slot_id}, sort_keys=True).encode()).hexdigest()


class Harness(unittest.TestCase):
    patients = range(1, 31)

    def setUp(self):
        self.rds = FakeRds(patients=self.patients, slots=[slot_row(SLOT_ID, capacity=3)], read_latency=0.002)
        self.appts = appointments_table()
        self.idem = FakeTable("idempotency_key")
        self.events, self.metrics = [], []
        self.lock = threading.Lock()
        patches = [
            mock.patch.object(bd, "ddb", FakeDynamo(appointments=self.appts, idem=self.idem)),
            mock.patch.object(bd, "db_connect", self.rds.connect),
            mock.patch.object(bd, "_sleep", lambda s: None),
            mock.patch.object(bd, "publish_event", self.capture_event),
            mock.patch.object(bd, "emit_metric", self.capture_metric),
            mock.patch.object(bd, "get_config_value", lambda name, default=None, ttl=60: default),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def capture_event(self, detail_type, detail):
        with self.lock:
            self.events.append((detail_type, detail))
        return True

    def capture_metric(self, name, value=1, unit="Count", **dims):
        with self.lock:
            self.metrics.append((name, value, dims))

    def call(self, *args, **kwargs):
        result = bd.lambda_handler(make_event(*args, **kwargs), mock.Mock(aws_request_id="req"))
        return result["statusCode"], json.loads(result["body"]), result

    def book(self, patient, slot_id=SLOT_ID, key=None):
        return self.call("POST", "/appointments", patient, {"slot_id": slot_id},
                         key=key or "key-{}-{}".format(patient, slot_id.replace("#", "_")))

    def events_of(self, kind):
        return [d for k, d in self.events if k == kind]

    def metric_total(self, name):
        return sum(v for n, v, _ in self.metrics if n == name)

    # ---- state inspection
    def appointments(self, status=None):
        """Real appointment items (not holds, not the counter item)."""
        found = [i for k, i in self.appts.items.items() if k.isdigit()]
        return [i for i in found if status is None or i["status"] == status]

    def holds(self):
        return [k for k in self.appts.items if k.startswith("HOLD#")]

    def slot(self, slot_id=SLOT_ID):
        return self.rds.slots[slot_id]


# ---------------------------------------------------------------- Mandatory test 1
class ConcurrencyTest(Harness):
    def run_race(self, patients):
        results, barrier = {}, threading.Barrier(len(patients))

        def worker(pid):
            barrier.wait()  # release every thread at the same instant
            results[pid] = self.book(pid)[:2]

        threads = [threading.Thread(target=worker, args=(p,)) for p in patients]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertFalse(any(t.is_alive() for t in threads), "a booking request hung")
        return results

    def test_24_simultaneous_bookings_of_a_capacity_3_slot_confirm_exactly_3(self):
        results = self.run_race(list(range(1, 25)))
        winners = [p for p, (status, _) in results.items() if status == 201]
        losers = [(status, body["code"]) for p, (status, body) in results.items() if status != 201]
        self.assertEqual(len(winners), 3, results)
        self.assertEqual(len(losers), 21)
        # every loser got a clean 409, never a 5xx or a stray exception
        self.assertTrue(all(status == 409 and code in ("SLOT_FULL", "BOOKING_CONTENTION") for status, code in losers),
                        losers)
        self.assertEqual(self.slot()["booked_count"], 3)
        self.assertEqual(len(self.appointments("CONFIRMED")), 3)
        self.assertEqual({int(a["patient_id"]) for a in self.appointments("CONFIRMED")}, set(winners))
        self.assertEqual(len(self.events_of("AppointmentBooked")), 3)
        self.assertEqual(len(self.holds()), 3, "losers must give their holds back")

    def test_contention_is_visible_in_the_conflict_retry_metric(self):
        self.run_race(list(range(1, 25)))
        self.assertGreater(self.metric_total("BookingConflictRetries"), 0,
                           "the test never produced a real lost race")

    def test_repeated_races_never_oversell(self):
        for round_number in range(5):
            self.setUp()
            results = self.run_race(list(range(1, 31)))
            self.assertEqual(sum(1 for s, _ in results.values() if s == 201), 3, f"round {round_number}")
            self.assertEqual(self.slot()["booked_count"], 3)

    def test_single_seat_slot_with_many_contenders(self):
        self.slot()["capacity"] = 1
        results = self.run_race(list(range(1, 21)))
        self.assertEqual(sum(1 for s, _ in results.values() if s == 201), 1)
        self.assertEqual(self.slot()["booked_count"], 1)

    def test_retries_are_bounded(self):
        # A slot that always loses the compare-and-set must end in 409 BOOKING_CONTENTION, not loop forever.
        attempts = []

        def always_lost(cursor, slot_id, version):
            attempts.append(1)
            return False

        with mock.patch.object(bd.slot_store, "claim_seat", always_lost):
            status, body, _ = self.book(1)
        self.assertEqual((status, body["code"]), (409, "BOOKING_CONTENTION"))
        self.assertEqual(len(attempts), bd.DEFAULT_MAX_CONFLICT_RETRIES + 1)
        self.assertEqual(self.appointments(), [])
        self.assertEqual(self.holds(), [], "the hold must be given back")
        self.assertEqual(self.slot()["booked_count"], 0)

    def test_retry_limit_comes_from_configuration(self):
        attempts = []

        def always_lost(cursor, slot_id, version):
            attempts.append(1)
            return False

        with mock.patch.object(bd.slot_store, "claim_seat", always_lost), \
                mock.patch.object(bd, "get_config_value", lambda name, default=None, ttl=60: "2" if name else default), \
                mock.patch.dict(os.environ, {"MAX_RETRIES_PARAMETER": "/medislot/dev/booking/max-conflict-retries"}):
            self.book(1)
        self.assertEqual(len(attempts), 3)

    def test_backoff_grows_and_is_capped(self):
        waits = [max(bd._backoff_seconds(n) for _ in range(200)) for n in (1, 3, 10)]
        self.assertLessEqual(waits[0], bd.BACKOFF_BASE_SECONDS * 2)
        self.assertLessEqual(waits[2], bd.BACKOFF_CAP_SECONDS)

    def test_full_slot_returns_409_slot_full(self):
        for p in (1, 2, 3):
            self.assertEqual(self.book(p)[0], 201)
        status, body, _ = self.book(4)
        self.assertEqual((status, body["code"]), (409, "SLOT_FULL"))
        self.assertEqual(self.slot()["booked_count"], 3)

    def test_every_claim_bumps_the_version(self):
        self.book(1)
        self.book(2)
        self.assertEqual(self.slot()["version"], 2)


# ---------------------------------------------------------------- Mandatory test 2
class IdempotencyTest(Harness):
    def test_same_key_replayed_creates_exactly_one_appointment(self):
        first = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="idem-key-0001")
        self.assertEqual(first[0], 201)
        replays = [self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="idem-key-0001") for _ in range(4)]
        for status, body, result in replays:
            self.assertEqual(status, 201)
            self.assertEqual(body["appointment"]["appointment_id"], first[1]["appointment"]["appointment_id"])
            self.assertEqual(result["headers"].get("Idempotent-Replayed"), "true")
        self.assertEqual(len(self.appointments()), 1)
        self.assertEqual(self.slot()["booked_count"], 1)
        self.assertEqual(len(self.events_of("AppointmentBooked")), 1)

    def test_parallel_replays_of_one_key_create_one_appointment(self):
        results, barrier = [], threading.Barrier(10)

        def worker():
            barrier.wait()
            results.append(self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="idem-key-0002")[:2])

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(len(self.appointments()), 1)
        self.assertEqual(self.slot()["booked_count"], 1)
        ids = {body["appointment"]["appointment_id"] for status, body in results if status in (200, 201)}
        self.assertEqual(len(ids), 1)
        # everyone else was told to retry; nobody got a 5xx
        self.assertTrue(all(s in (200, 201) or (s == 409 and b["code"] in ("REQUEST_IN_PROGRESS", "ALREADY_BOOKED"))
                            for s, b in results), results)

    def test_same_key_with_a_different_request_is_422(self):
        self.rds.slots["other"] = slot_row("other", capacity=3)
        self.assertEqual(self.book(5, SLOT_ID, key="idem-key-0003")[0], 201)
        status, body, _ = self.book(5, "other", key="idem-key-0003")
        self.assertEqual((status, body["code"]), (422, "IDEMPOTENCY_KEY_REUSED"))

    def test_key_still_being_processed_returns_409(self):
        self.idem.items["5#idem-key-0004"] = {"idempotency_key": "5#idem-key-0004", "state": "IN_PROGRESS",
                                              "request_hash": request_hash(SLOT_ID), "started_at": int(time.time())}
        status, body, _ = self.book(5, key="idem-key-0004")
        self.assertEqual((status, body["code"]), (409, "REQUEST_IN_PROGRESS"))
        self.assertEqual(self.appointments(), [])

    def test_abandoned_in_progress_key_is_taken_over(self):
        self.idem.items["5#idem-key-0005"] = {"idempotency_key": "5#idem-key-0005", "state": "IN_PROGRESS",
                                              "request_hash": request_hash(SLOT_ID),
                                              "started_at": int(time.time()) - 600}
        self.assertEqual(self.book(5, key="idem-key-0005")[0], 201)

    def test_a_failed_booking_frees_its_key_so_the_client_can_retry(self):
        self.slot()["booked_count"] = 3  # full
        self.assertEqual(self.book(5, key="idem-key-0006")[1]["code"], "SLOT_FULL")
        self.assertNotIn("5#idem-key-0006", self.idem.items)
        self.slot()["booked_count"] = 2  # a seat opens up
        self.assertEqual(self.book(5, key="idem-key-0006")[0], 201)

    def test_key_is_scoped_to_the_patient(self):
        self.assertEqual(self.book(5, key="shared-key-01")[0], 201)
        self.assertEqual(self.book(6, key="shared-key-01")[0], 201)
        self.assertEqual(len(self.appointments()), 2)

    def test_expired_idempotency_record_still_returns_the_original_appointment(self):
        first = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="idem-key-0007")
        self.idem.items.clear()  # DynamoDB TTL removed the record
        again = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="idem-key-0007")
        self.assertEqual(again[0], 200)
        self.assertEqual(again[1]["appointment"]["appointment_id"], first[1]["appointment"]["appointment_id"])
        self.assertEqual(len(self.appointments()), 1)
        self.assertEqual(self.slot()["booked_count"], 1)

    def test_missing_or_malformed_key_is_400(self):
        status, body, _ = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID})
        self.assertEqual((status, body["code"]), (400, "IDEMPOTENCY_KEY_REQUIRED"))
        status, body, _ = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="short")
        self.assertEqual((status, body["code"]), (400, "IDEMPOTENCY_KEY_INVALID"))


# ---------------------------------------------------------------- validation rules
class BookingValidationTests(Harness):
    def test_success_response_event_and_stored_appointment(self):
        status, body, _ = self.book(5)
        self.assertEqual(status, 201)
        appt = body["appointment"]
        self.assertEqual((appt["status"], appt["fee"], appt["slot_id"]), ("CONFIRMED", 500.0, SLOT_ID))
        self.assertEqual(appt["appointment_id"], "1")  # sequential ids, no UUIDs
        stored = self.appts.items["1"]
        self.assertEqual((stored["patient_id"], stored["doctor_id"], stored["fee_amount"]), ("5", "7", "500.00"))
        event = self.events_of("AppointmentBooked")[0]
        self.assertEqual((event["patient_id"], event["slot_id"], event["appointment_id"]),
                         (5, SLOT_ID, appt["appointment_id"]))

    def test_appointment_numbers_count_up(self):
        ids = [self.book(p)[1]["appointment"]["appointment_id"] for p in (1, 2, 3)]
        self.assertEqual(ids, ["1", "2", "3"])

    def test_unknown_slot_is_404(self):
        status, body, _ = self.book(5, "does-not-exist")
        self.assertEqual((status, body["code"]), (404, "SLOT_NOT_FOUND"))

    def test_missing_slot_id_is_400(self):
        status, _, _ = self.call("POST", "/appointments", 5, {}, key="key-no-slot-1")
        self.assertEqual(status, 400)

    def test_past_slot_is_400(self):
        self.rds.slots["past"] = slot_row("past", hours_ahead=-2)
        status, body, _ = self.book(5, "past")
        self.assertEqual((status, body["code"]), (400, "SLOT_IN_PAST"))

    def test_cancelled_removed_or_blacked_out_slots_are_unavailable(self):
        for state in ("CANCELLED", "REMOVED", "BLACKED_OUT"):
            self.rds.slots["x"] = slot_row("x", status=state)
            status, body, _ = self.book(5, "x", key=f"key-{state}-0001")
            self.assertEqual((status, body["code"]), (409, "SLOT_UNAVAILABLE"), state)

    def test_one_appointment_per_patient_per_slot(self):
        self.assertEqual(self.book(5, key="first-key-001")[0], 201)
        status, body, _ = self.book(5, key="second-key-02")
        self.assertEqual((status, body["code"]), (409, "ALREADY_BOOKED"))
        self.assertEqual(self.slot()["booked_count"], 1)

    def test_at_most_three_upcoming_appointments_with_one_doctor(self):
        for i in range(4):
            self.rds.slots[f"s{i}"] = slot_row(f"s{i}", capacity=1, hours_ahead=72 + i)
        for i in range(3):
            self.assertEqual(self.book(5, f"s{i}")[0], 201)
        status, body, _ = self.book(5, "s3")
        self.assertEqual((status, body["code"]), (409, "DOCTOR_LIMIT_REACHED"))
        self.assertEqual(self.slot("s3")["booked_count"], 0)  # no seat was taken
        self.assertNotIn("HOLD#s3#5", self.appts.items)       # and no hold is left behind
        first = self.appointments("CONFIRMED")[0]["appointment_id"]
        self.assertEqual(self.call("POST", f"/appointments/{first}/cancel", 5, {})[0], 200)
        self.assertEqual(self.book(5, "s3", key="retry-after-cancel")[0], 201)

    def test_the_doctor_limit_is_per_doctor(self):
        for i in range(3):
            self.rds.slots[f"d7-{i}"] = slot_row(f"d7-{i}", capacity=1, hours_ahead=72 + i)
            self.assertEqual(self.book(5, f"d7-{i}")[0], 201)
        self.rds.slots["d8"] = slot_row("d8", doctor_id=8, capacity=1)
        self.assertEqual(self.book(5, "d8")[0], 201)

    def test_profile_is_required(self):
        status, body, _ = self.book(99)
        self.assertEqual((status, body["code"]), (409, "PROFILE_REQUIRED"))
        self.assertEqual(self.slot()["booked_count"], 0)

    def test_doctor_not_offering_the_consultation_any_more(self):
        self.rds.fees[(7, 4)] = None
        status, body, _ = self.book(5)
        self.assertEqual((status, body["code"]), (409, "SLOT_UNAVAILABLE"))
        self.assertEqual((self.slot()["booked_count"], self.holds()), (0, []))

    def test_only_patients_may_use_the_endpoints(self):
        for role in ("DOCTOR", None):
            status, _, _ = self.call("POST", "/appointments", 5, {"slot_id": SLOT_ID}, key="role-key-0001", role=role)
            self.assertEqual(status, 403)
        status, _, _ = self.call("GET", "/appointments", 5, role="DOCTOR")
        self.assertEqual(status, 403)

    def test_seat_and_hold_are_returned_when_saving_the_appointment_fails(self):
        with mock.patch.object(bd.store, "put_appointment", side_effect=RuntimeError("dynamodb went away")):
            status, _, _ = self.book(5)
        self.assertEqual(status, 500)
        self.assertEqual(self.slot()["booked_count"], 0, "the claimed seat must be handed back")
        self.assertEqual(self.holds(), [], "the hold must be handed back")
        self.assertEqual(self.idem.items, {})  # the key is free, so the client may retry
        self.assertEqual(self.book(5)[0], 201)

    def test_doctor_cancelling_during_the_booking_refunds_the_patient(self):
        # The doctor cancels between our seat claim and our re-check after the appointment was saved.
        real = bd._close_race_with_doctor_cancel

        def cancel_then_check(*a, **kw):
            self.slot()["status"] = "CANCELLED"
            return real(*a, **kw)

        with mock.patch.object(bd, "_close_race_with_doctor_cancel", cancel_then_check):
            status, body, _ = self.book(5)
        self.assertEqual((status, body["code"]), (409, "SLOT_CANCELLED"))
        appt = self.appointments()[0]
        self.assertEqual(appt["status"], "CANCELLED_BY_DOCTOR")
        ledger = self.rds.ledger[int(appt["appointment_id"])]
        self.assertEqual((ledger["refund_amount"], ledger["trigger_type"]), (Decimal("500.00"), "DOCTOR_CANCELLATION"))
        self.assertEqual(self.holds(), [])


# ---------------------------------------------------------------- listing and scoping
class ListingTests(Harness):
    def test_patients_only_see_their_own_appointments(self):
        self.slot()["capacity"] = 5
        a = self.book(1)[1]["appointment"]["appointment_id"]
        b = self.book(2)[1]["appointment"]["appointment_id"]
        status, body, _ = self.call("GET", "/appointments", 1)
        self.assertEqual([x["appointment_id"] for x in body["appointments"]], [a])
        self.assertEqual(self.call("GET", f"/appointments/{a}", 1)[0], 200)
        self.assertEqual(self.call("GET", f"/appointments/{b}", 1)[0], 404)  # someone else's
        self.assertEqual(self.call("POST", f"/appointments/{b}/cancel", 1, {})[0], 404)
        self.assertEqual(self.appts.items[b]["status"], "CONFIRMED")

    def test_holds_and_the_counter_are_never_listed_or_readable(self):
        self.book(1)
        self.assertEqual(self.call("GET", "/appointments", 1)[1]["pagination"]["total"], 1)
        self.assertEqual(self.call("GET", f"/appointments/{bd.store.SEQUENCE_KEY}", 1)[0], 404)

    def test_detail_includes_a_refund_preview(self):
        a = self.book(1)[1]["appointment"]["appointment_id"]
        body = self.call("GET", f"/appointments/{a}", 1)[1]["appointment"]
        self.assertEqual(body["cancellation_preview"]["tier"], "GT_48H")  # slot is 72 h away
        self.assertEqual(body["cancellation_preview"]["refund_amount"], 500.0)

    def test_filters_paging_and_validation(self):
        self.book(1)
        self.assertEqual(self.call("GET", "/appointments", 1, query={"upcoming": "true"})[1]["pagination"]["total"], 1)
        self.assertEqual(self.call("GET", "/appointments", 1, query={"status": "COMPLETED"})[1]["pagination"]["total"], 0)
        self.assertEqual(self.call("GET", "/appointments", 1, query={"status": "BOGUS"})[0], 400)
        self.assertEqual(self.call("GET", "/appointments", 1, query={"page_size": "999"})[0], 400)
        self.assertEqual(self.call("GET", "/appointments", 1, query={"page": "x"})[0], 400)

    def test_lists_are_ordered_by_start_time(self):
        for i, hours in enumerate((100, 80, 90)):
            self.rds.slots[f"o{i}"] = slot_row(f"o{i}", capacity=1, hours_ahead=hours)
            self.book(1, f"o{i}")
        upcoming = self.call("GET", "/appointments", 1, query={"upcoming": "true"})[1]["appointments"]
        starts = [a["start_time_utc"] for a in upcoming]
        self.assertEqual(starts, sorted(starts))


# ---------------------------------------------------------------- cancellation and refunds
class CancellationTests(Harness):
    def book_at(self, hours_ahead, patient=5, slot_id="c1"):
        self.rds.slots[slot_id] = slot_row(slot_id, capacity=2, hours_ahead=hours_ahead)
        status, body, _ = self.book(patient, slot_id, key=f"book-{slot_id}-{patient}")
        self.assertEqual(status, 201, body)
        return body["appointment"]["appointment_id"]

    def cancel(self, appointment_id, patient=5):
        return self.call("POST", f"/appointments/{appointment_id}/cancel", patient, {})

    def test_cancelling_far_ahead_refunds_everything_and_frees_the_seat(self):
        aid = self.book_at(72)
        status, body, _ = self.cancel(aid)
        self.assertEqual(status, 200)
        refund = body["appointment"]["refund"]
        self.assertEqual((refund["tier"], refund["percent"], refund["amount"]), ("GT_48H", 100.0, 500.0))
        self.assertEqual(body["appointment"]["status"], "CANCELLED_BY_PATIENT")
        self.assertEqual(self.slot("c1")["booked_count"], 0)
        self.assertIn("seat_released_at", self.appts.items[aid])
        self.assertEqual(self.rds.ledger[int(aid)]["trigger_type"], "PATIENT_CANCELLATION")
        self.assertNotIn("HOLD#c1#5", self.appts.items)

    def test_each_tier_through_the_api(self):
        for hours, tier, amount in ((60, "GT_48H", 500.0), (30, "24H_TO_48H", 375.0),
                                    (10, "6H_TO_24H", 250.0), (2, "LT_6H", 0.0)):
            with self.subTest(hours=hours):
                self.setUp()
                aid = self.book_at(hours)
                refund = self.cancel(aid)[1]["appointment"]["refund"]
                self.assertEqual((refund["tier"], refund["amount"]), (tier, amount))

    def test_events_and_metrics_on_cancellation(self):
        aid = self.book_at(30)
        self.cancel(aid)
        cancelled = self.events_of("AppointmentCancelled")[0]
        self.assertEqual((cancelled["appointment_id"], cancelled["cancelled_by"], cancelled["refund"]["tier"]),
                         (aid, "PATIENT", "24H_TO_48H"))
        freed = self.events_of("SlotSeatFreed")[0]
        self.assertEqual((freed["slot_id"], freed["booked_count"], freed["free_seats"]), ("c1", 0, 2))
        self.assertIn(("CancellationsByTier", 1, {"Tier": "24H_TO_48H"}), self.metrics)

    def test_cancelling_twice_never_refunds_twice(self):
        aid = self.book_at(72)
        self.cancel(aid)
        status, body, _ = self.cancel(aid)
        self.assertEqual(status, 200)
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(self.slot("c1")["booked_count"], 0)  # not decremented a second time
        self.assertEqual(len(self.events_of("AppointmentCancelled")), 1)

    def test_failed_seat_release_is_finished_by_repeating_the_cancel(self):
        aid = self.book_at(72)
        self.rds.fail_release = 1
        status = self.cancel(aid)[0]
        self.assertEqual(status, 200)
        self.assertEqual(self.appts.items[aid]["status"], "CANCELLED_BY_PATIENT")  # refund already safe
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(self.slot("c1")["booked_count"], 1)  # seat still held
        self.assertEqual(self.metric_total("SeatReleaseFailures"), 1)
        self.assertEqual(self.cancel(aid)[0], 200)
        self.assertEqual(self.slot("c1")["booked_count"], 0)
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(len(self.events_of("SlotSeatFreed")), 1)

    def test_cancelling_after_the_ledger_write_failed_completes_it_on_repeat(self):
        aid = self.book_at(72)
        with mock.patch.object(bd, "_write_ledger", side_effect=RuntimeError("rds down")):
            self.assertEqual(self.cancel(aid)[0], 500)
        self.assertEqual(self.appts.items[aid]["status"], "CANCELLED_BY_PATIENT")
        self.assertEqual(len(self.rds.ledger), 0)
        self.assertEqual(self.cancel(aid)[0], 200)
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(self.slot("c1")["booked_count"], 0)

    def test_started_appointment_cannot_be_cancelled(self):
        aid = self.book_at(72)
        self.appts.items[aid]["start_time_utc"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        status, body, _ = self.cancel(aid)
        self.assertEqual((status, body["code"]), (409, "ALREADY_STARTED"))
        self.assertEqual(len(self.rds.ledger), 0)

    def test_completed_appointment_cannot_be_cancelled(self):
        aid = self.book_at(72)
        self.appts.items[aid]["status"] = "COMPLETED"
        status, body, _ = self.cancel(aid)
        self.assertEqual((status, body["code"]), (409, "NOT_CANCELLABLE"))

    def test_rebooking_after_cancelling_is_allowed(self):
        aid = self.book_at(72)
        self.cancel(aid)
        self.assertEqual(self.book(5, "c1", key="rebook-key-001")[0], 201)

    def test_freed_seat_can_be_booked_by_someone_else(self):
        self.rds.slots["one"] = slot_row("one", capacity=1)
        a = self.book(1, "one")[1]["appointment"]["appointment_id"]
        self.assertEqual(self.book(2, "one")[1]["code"], "SLOT_FULL")
        self.cancel(a, patient=1)
        self.assertEqual(self.book(2, "one", key="after-free-0001")[0], 201)

    def test_several_cancel_clicks_at_once_refund_once(self):
        aid = self.book_at(72)
        results, barrier = [], threading.Barrier(6)

        def worker():
            barrier.wait()
            results.append(self.cancel(aid)[0])

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertTrue(all(s == 200 for s in results), results)
        self.assertEqual(len(self.rds.ledger), 1)
        self.assertEqual(self.slot("c1")["booked_count"], 0)

    def test_refund_policy_comes_from_configuration(self):
        policy = json.dumps({"currency": "INR", "tiers": [
            {"name": "ANYTIME", "min_hours": 0, "inclusive": True, "percent": 20}]})
        aid = self.book_at(72)
        with mock.patch.object(bd, "get_config_value", lambda name, default=None, ttl=60: policy if name and "refund" in name else default), \
                mock.patch.dict(os.environ, {"REFUND_POLICY_PARAMETER": "/medislot/dev/booking/refund-policy"}):
            refund = self.cancel(aid)[1]["appointment"]["refund"]
        self.assertEqual((refund["tier"], refund["amount"]), ("ANYTIME", 100.0))

    def test_a_broken_policy_falls_back_to_the_default_tiers(self):
        aid = self.book_at(72)
        with mock.patch.object(bd, "get_config_value", lambda name, default=None, ttl=60: "{not json" if name else default), \
                mock.patch.dict(os.environ, {"REFUND_POLICY_PARAMETER": "/medislot/dev/booking/refund-policy"}):
            refund = self.cancel(aid)[1]["appointment"]["refund"]
        self.assertEqual(refund["tier"], "GT_48H")

    def test_refund_ledger_endpoint_is_scoped_to_the_caller(self):
        a = self.book_at(72, patient=1, slot_id="m1")
        b = self.book_at(10, patient=2, slot_id="m2")
        self.cancel(a, patient=1)
        self.cancel(b, patient=2)
        body = self.call("GET", "/patients/me/refunds", 1)[1]
        self.assertEqual([str(r["appointment_id"]) for r in body["refunds"]], [a])
        self.assertEqual(body["total_refunded"], 500.0)
        self.assertIn("ledger", body["note"])
        self.assertEqual(self.call("GET", "/patients/me/refunds", 2)[1]["total_refunded"], 250.0)

    def test_unknown_routes_and_methods(self):
        self.assertEqual(self.call("GET", "/appointments/nope", 1)[0], 404)
        self.assertEqual(self.call("DELETE", "/appointments", 1)[0], 405)


if __name__ == "__main__":
    unittest.main()
