"""Milestone 6: waitlist, hold offers, exclusivity, acceptance and the expiry sweeper.

Mandatory Test 4: one freed seat, three waitlisted patients. At every moment at most ONE live
offer exists for the seat; while it is held nobody else can book it; when the offer expires the
seat passes to the next patient in line; acceptance turns it into a CONFIRMED appointment.

The fake database checks the invariant "live offers <= held seats" after every single write and
records the highest number of live offers the slot ever had (FakeRds.max_live_offers).
"""
import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from fakes_workflows import Milestone6Harness, wd  # noqa: I001 (sets up paths and stubs)
from fakes_booking import slot_row
import waitlist_store as wl

NOW = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
SLOT = "7#1#FULL"
OTHER_SLOTS = [f"7#1#S{i}" for i in range(6)]


def sqs_batch(*details):
    return {"Records": [{"messageId": f"m{i}", "body": json.dumps({"detail-type": "SlotSeatFreed", "detail": d})}
                        for i, d in enumerate(details)]}


class WaitlistHarness(Milestone6Harness):
    slots = [slot_row(SLOT, capacity=1, ctype=1, start=NOW + timedelta(hours=48))] + \
            [slot_row(s, capacity=1, ctype=1, booked=1, start=NOW + timedelta(hours=50 + i)) for i, s in enumerate(OTHER_SLOTS)]

    def setUp(self):
        super().setUp()
        status, body = self.book(1, SLOT)
        self.assertEqual(status, 201, body)
        self.booked_id = body["appointment"]["appointment_id"]
        self.clock = NOW
        p = mock.patch.object(wd, "_utcnow", lambda: self.clock)
        p.start()
        self.addCleanup(p.stop)

    def join(self, patient, slot=SLOT):
        return self.call("POST", f"/slots/{slot.replace('#', '%23')}/waitlist", patient)

    def leave(self, patient, slot=SLOT):
        return self.call("DELETE", f"/slots/{slot.replace('#', '%23')}/waitlist", patient)

    def accept(self, patient, offer_id):
        return self.call("POST", f"/waitlist-offers/{offer_id}/accept", patient)

    def live_offers(self, slot=SLOT):
        return [o for o in self.rds.offers.values() if o["slot_id"] == slot and o["status"] == "OFFERED"]

    def free_the_seat(self):
        """Patient 1 cancels; the SlotSeatFreed event is delivered to the waitlist worker."""
        status, body = self.call("DELETE", f"/appointments/{self.booked_id}", 1)
        self.assertEqual(status, 200, body)
        freed = self.events_of("SlotSeatFreed")
        self.assertEqual(len(freed), 1)
        return freed[0]


class MandatoryTest4ExclusivityTests(WaitlistHarness):
    """Mandatory Test 4: one freed seat, three waitlisted patients, only one live offer at any moment."""

    def test_one_freed_seat_three_waiting_patients_one_live_offer_at_a_time(self):
        for patient in (2, 3, 4):
            self.assertEqual(self.join(patient)[0], 201)

        freed = self.free_the_seat()
        # The released seat went straight to the waitlist: it is held, not free.
        self.assertEqual((self.slot(SLOT)["booked_count"], self.slot(SLOT)["held_count"]), (0, 1))

        # SQS may deliver the same event more than once: still exactly one offer.
        self.assertEqual(wd.seat_freed_handler(sqs_batch(freed, freed), None), {"batchItemFailures": []})
        offers = self.live_offers()
        self.assertEqual([o["patient_id"] for o in offers], [2], "first in line gets the only offer")

        # While the seat is held it is invisible to normal booking.
        status, body = self.book(5, SLOT)
        self.assertEqual((status, body["code"]), (409, "SLOT_FULL"))

        # The offer expires; the sweeper (run twice) passes the seat to patient 3, never to two people.
        self.clock = NOW + timedelta(minutes=16)
        wd.sweep()
        wd.sweep()
        self.assertEqual([o["patient_id"] for o in self.live_offers()], [3])
        self.assertEqual(self.accept(2, offers[0]["offer_id"])[1]["code"], "OFFER_NOT_LIVE")

        status, body = self.accept(3, self.live_offers()[0]["offer_id"])
        self.assertEqual(status, 201, body)
        self.assertEqual(body["appointment"]["status"], "CONFIRMED")
        self.assertEqual(self.live_offers(), [])
        self.assertEqual((self.slot(SLOT)["booked_count"], self.slot(SLOT)["held_count"]), (1, 0))
        self.assertEqual([i["patient_id"] for i in self.appointments(status="CONFIRMED")], ["3"])
        self.assertEqual(sorted((e["patient_id"], e["status"]) for e in self.rds.entries.values()),
                         [(2, "EXPIRED"), (3, "PROMOTED"), (4, "WAITING")])

        self.assertEqual(self.rds.max_live_offers[SLOT], 1, "never two live offers for the one freed seat")
        self.assertEqual(self.rds.invariant_violations, [])
        self.assertEqual((self.metric_total("OffersExpired"), self.metric_total("WaitlistPromotions")), (1, 1))
        self.assertEqual([e["patient_id"] for e in self.events_of("WaitlistOfferIssued")], [2, 3])
        self.assertEqual([e["patient_id"] for e in self.events_of("WaitlistOfferExpired")], [2])

    def test_concurrent_deliveries_and_sweeps_never_create_a_second_live_offer(self):
        for patient in (2, 3, 4):
            self.join(patient)
        freed = self.free_the_seat()
        self.clock = NOW + timedelta(minutes=1)
        workers = [threading.Thread(target=wd.seat_freed_handler, args=(sqs_batch(freed), None)) for _ in range(6)]
        workers += [threading.Thread(target=wd.sweep) for _ in range(6)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        self.assertEqual(len(self.live_offers()), 1)

        self.clock = NOW + timedelta(minutes=17)  # expire and pass on, again from many threads at once
        workers = [threading.Thread(target=wd.sweep) for _ in range(8)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        self.assertEqual([o["patient_id"] for o in self.live_offers()], [3])
        self.assertEqual(self.rds.max_live_offers[SLOT], 1)
        self.assertEqual(self.rds.invariant_violations, [])
        self.assertEqual(self.metric_total("OffersExpired"), 1)

    def test_parallel_accepts_of_the_one_offer_create_one_appointment(self):
        self.join(2)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        offer_id = self.live_offers()[0]["offer_id"]
        results = []
        threads = [threading.Thread(target=lambda: results.append(self.accept(2, offer_id)[0])) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(results), [200, 200, 200, 200, 201])
        self.assertEqual(len(self.appointments(patient=2)), 1)
        self.assertEqual((self.slot(SLOT)["booked_count"], self.slot(SLOT)["held_count"]), (1, 0))


class WaitlistRuleTests(WaitlistHarness):
    def test_only_a_full_slot_has_a_waitlist(self):
        self.call("DELETE", f"/appointments/{self.booked_id}", 1)
        self.assertEqual(self.join(2)[1]["code"], "SLOT_NOT_FULL")

    def test_joining_twice_is_harmless_and_keeps_the_place_in_line(self):
        self.assertEqual(self.join(2)[0], 201)
        self.assertEqual(self.join(3)[1]["position"], 2)
        status, body = self.join(2)
        self.assertEqual((status, body["position"]), (200, 1))

    def test_a_patient_with_an_appointment_in_the_slot_cannot_join(self):
        self.assertEqual(self.join(1)[1]["code"], "ALREADY_BOOKED")

    def test_at_most_five_waitlists(self):
        for slot in OTHER_SLOTS[:5]:
            self.assertEqual(self.join(2, slot)[0], 201)
        status, body = self.join(2, OTHER_SLOTS[5])
        self.assertEqual((status, body["code"]), (409, "WAITLIST_LIMIT_REACHED"))
        self.leave(2, OTHER_SLOTS[0])
        self.assertEqual(self.join(2, OTHER_SLOTS[5])[0], 201, "leaving frees a place")

    def test_waitlist_cap_comes_from_ssm(self):
        self.settings["maxwl"] = "2"
        self.join(2, OTHER_SLOTS[0])
        self.join(2, OTHER_SLOTS[1])
        self.assertEqual(self.join(2, OTHER_SLOTS[2])[1]["code"], "WAITLIST_LIMIT_REACHED")

    def test_hold_duration_comes_from_ssm(self):
        self.settings["hold"] = "5"
        self.join(2)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        self.assertEqual(self.live_offers()[0]["expires_at"], NOW + timedelta(minutes=5))

    def test_listing_shows_position_and_pending_offer(self):
        self.join(2)
        self.join(3)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        mine = self.call("GET", "/patients/me/waitlists", 2)[1]["waitlists"]
        self.assertEqual((mine[0]["status"], mine[0]["offer"]["offer_id"] > 0), ("OFFERED", True))
        theirs = self.call("GET", "/patients/me/waitlists", 3)[1]["waitlists"]
        self.assertEqual((theirs[0]["status"], theirs[0]["position"], theirs[0]["offer"]), ("WAITING", 1, None))

    def test_leaving_with_a_live_offer_passes_it_to_the_next_patient(self):
        self.join(2)
        self.join(3)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        status, body = self.leave(2)
        self.assertEqual(status, 200, body)
        self.assertEqual([o["patient_id"] for o in self.live_offers()], [3])
        self.assertEqual(self.rds.max_live_offers[SLOT], 1)

    def test_with_nobody_left_in_line_the_seat_returns_to_normal_booking(self):
        self.join(2)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        self.clock = NOW + timedelta(minutes=16)
        wd.sweep()
        self.assertEqual((self.slot(SLOT)["held_count"], self.live_offers()), (0, []))
        self.assertEqual(self.book(5, SLOT)[0], 201)

    def test_nobody_waiting_means_the_freed_seat_is_simply_free(self):
        self.free_the_seat()
        self.assertEqual(self.slot(SLOT)["held_count"], 0)
        self.assertEqual(self.book(5, SLOT)[0], 201)

    def test_the_sweeper_repairs_a_lost_seat_freed_event(self):
        self.join(2)
        self.free_the_seat()  # the event is never delivered to the worker
        self.assertEqual(self.live_offers(), [])
        wd.sweep()
        self.assertEqual([o["patient_id"] for o in self.live_offers()], [2])

    def test_an_expired_offer_cannot_be_accepted_even_before_the_sweeper_runs(self):
        self.join(2)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        self.clock = NOW + timedelta(minutes=15, seconds=1)
        self.assertEqual(self.accept(2, self.live_offers()[0]["offer_id"])[1]["code"], "OFFER_EXPIRED")

    def test_only_the_offered_patient_can_accept(self):
        self.join(2)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        self.assertEqual(self.accept(3, self.live_offers()[0]["offer_id"])[0], 404)

    def test_a_cancelled_slot_closes_its_waitlist_and_offers(self):
        self.join(2)
        self.join(3)
        wd.seat_freed_handler(sqs_batch(self.free_the_seat()), None)
        conn = self.rds.connect()
        with conn.cursor() as cur:
            wl.close_waitlist(cur, SLOT, NOW)
        conn.commit()
        self.assertEqual(self.live_offers(), [])
        self.assertEqual({e["status"] for e in self.rds.entries.values()}, {"CLOSED"})
        self.assertEqual(self.slot(SLOT)["held_count"], 0)


if __name__ == "__main__":
    unittest.main()
