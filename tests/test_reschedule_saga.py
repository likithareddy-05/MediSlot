"""Milestone 6: reschedule saga (Step Functions) with compensation.

Mandatory Test 3: a failure is forced while the original seat is being released, after the target
seat was claimed and the appointment was moved. The compensation branch must run, and afterwards
the patient holds EXACTLY ONE appointment (the original, still CONFIRMED), the target seat is given
back and no hold or lock is left behind.

The saga runs through infrastructure/statemachines/reschedule-saga.asl.json (see AslRunner), and
the PATCH /appointments/{id} endpoint is called the way API Gateway calls it.
"""
import json
import unittest
from datetime import datetime, timedelta, timezone

from fakes_workflows import DEFINITION, AslRunner, Milestone6Harness, saga  # noqa: I001 (sets up paths and stubs)
from fakes_booking import slot_row

NOW = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
ORIGINAL = "7#2#ORIGINAL"
TARGET = "7#2#TARGET"
TARGET_2 = "7#2#TARGET2"
TARGET_3 = "7#2#TARGET3"
OTHER_DOCTOR = "8#2#OTHER"
SOON = "7#2#SOON"
PATIENT = 1


def two_type_slot(slot_id, hours, doctor=7, capacity=1):
    return slot_row(slot_id, doctor_id=doctor, capacity=capacity, ctype=2, start=NOW + timedelta(hours=hours))


class RescheduleHarness(Milestone6Harness):
    slots = [two_type_slot(ORIGINAL, 72), two_type_slot(TARGET, 96), two_type_slot(TARGET_2, 120),
             two_type_slot(TARGET_3, 144), two_type_slot(OTHER_DOCTOR, 100, doctor=8), two_type_slot(SOON, 5)]

    def setUp(self):
        super().setUp()
        status, body = self.book(PATIENT, ORIGINAL)
        self.assertEqual(status, 201, body)
        self.appointment_id = body["appointment"]["appointment_id"]

    def reschedule(self, target, patient=PATIENT, appointment_id=None):
        return self.call("PATCH", f"/appointments/{appointment_id or self.appointment_id}", patient,
                         {"target_slot_id": target})

    def assert_exactly_one_appointment_on(self, slot_id):
        mine = self.appointments(PATIENT)
        self.assertEqual(len(mine), 1, "the patient must hold exactly one appointment")
        self.assertEqual((mine[0]["status"], mine[0]["slot_id"]), ("CONFIRMED", slot_id))
        self.assertNotIn("reschedule_lock", mine[0])
        self.assertEqual([h for h in self.holds() if h.endswith(f"#{PATIENT}")], [f"HOLD#{slot_id}#{PATIENT}"])
        return mine[0]


class MandatoryTest3CompensationTests(RescheduleHarness):
    """Mandatory Test 3: forced failure during release -> the patient holds exactly one appointment."""

    def test_forced_release_failure_runs_compensation_and_keeps_exactly_one_appointment(self):
        self.settings["force"] = "true"  # /medislot/<env>/reschedule/force-release-failure

        status, body = self.reschedule(TARGET)

        self.assertEqual((status, body["code"]), (409, "RESCHEDULE_COMPENSATED"))
        run = self.sfn.last()
        self.assertEqual((run["status"], run["error"]), ("FAILED", "RescheduleCompensated"))
        # the execution history shows the claim, the switch, the failed release and both compensations
        self.assertEqual(run["history"], ["ValidateRequest", "ClaimTargetSeat", "SwitchAppointment",
                                          "ReleaseOriginalSeat", "CompensateRevertAppointment",
                                          "CompensateReleaseTarget", "MarkCompensated", "RescheduleCompensated"])
        appointment = self.assert_exactly_one_appointment_on(ORIGINAL)
        self.assertEqual(appointment["appointment_id"], self.appointment_id)
        self.assertEqual(int(appointment.get("reschedule_count", 0)), 0)
        self.assertEqual((self.slot(ORIGINAL)["booked_count"], self.slot(TARGET)["booked_count"]), (1, 0))
        self.assertEqual(self.metric_total("ReschedulesCompensated"), 1)
        self.assertEqual(self.events_of("AppointmentRescheduled"), [])
        self.assertEqual(self.events_of("SlotSeatFreed"), [])

    def test_a_real_database_failure_during_the_release_is_compensated_too(self):
        self.rds.fail_release = 1  # the UPDATE that gives the original seat back raises

        status, body = self.reschedule(TARGET)

        self.assertEqual((status, body["code"]), (409, "RESCHEDULE_COMPENSATED"))
        self.assertIn("CompensateReleaseTarget", self.sfn.last()["history"])
        self.assert_exactly_one_appointment_on(ORIGINAL)
        self.assertEqual((self.slot(ORIGINAL)["booked_count"], self.slot(TARGET)["booked_count"]), (1, 0))

    def test_after_a_compensated_attempt_the_same_reschedule_succeeds(self):
        self.settings["force"] = "true"
        self.assertEqual(self.reschedule(TARGET)[0], 409)
        self.settings["force"] = "false"

        status, body = self.reschedule(TARGET)

        self.assertEqual(status, 200, body)
        moved = self.assert_exactly_one_appointment_on(TARGET)
        self.assertEqual(int(moved["reschedule_count"]), 1, "a compensated attempt does not use up a reschedule")


class RescheduleSuccessTests(RescheduleHarness):
    def test_success_moves_the_appointment_and_both_seats(self):
        status, body = self.reschedule(TARGET)

        self.assertEqual(status, 200, body)
        self.assertEqual(body["appointment"]["slot_id"], TARGET)
        self.assertEqual(self.sfn.last()["history"], ["ValidateRequest", "ClaimTargetSeat", "SwitchAppointment",
                                                      "ReleaseOriginalSeat", "Finalize", "RescheduleSucceeded"])
        moved = self.assert_exactly_one_appointment_on(TARGET)
        self.assertEqual(moved["appointment_id"], self.appointment_id)
        self.assertEqual((moved["previous_slot_id"], int(moved["reschedule_count"])), (ORIGINAL, 1))
        self.assertEqual((self.slot(ORIGINAL)["booked_count"], self.slot(TARGET)["booked_count"]), (0, 1))
        published = self.events_of("AppointmentRescheduled")
        self.assertEqual(len(published), 1)
        self.assertEqual((published[0]["from_slot_id"], published[0]["to_slot_id"], published[0]["reschedule_count"]),
                         (ORIGINAL, TARGET, 1))
        self.assertEqual([e["slot_id"] for e in self.events_of("SlotSeatFreed")], [ORIGINAL])

    def test_reschedule_carries_no_fee_or_refund(self):
        fee_before = self.appointments(PATIENT)[0]["fee_amount"]
        self.reschedule(TARGET)
        after = self.appointments(PATIENT)[0]
        self.assertEqual(after["fee_amount"], fee_before)
        self.assertNotIn("refund_amount", after)
        self.assertEqual(self.rds.ledger, {})


class RescheduleRuleTests(RescheduleHarness):
    def test_at_most_two_reschedules(self):
        self.assertEqual(self.reschedule(TARGET)[0], 200)
        self.assertEqual(self.reschedule(TARGET_2)[0], 200)

        status, body = self.reschedule(TARGET_3)

        self.assertEqual((status, body["code"]), (409, "RESCHEDULE_LIMIT_REACHED"))
        self.assert_exactly_one_appointment_on(TARGET_2)

    def test_limit_is_configurable(self):
        self.settings["maxres"] = "1"
        self.assertEqual(self.reschedule(TARGET)[0], 200)
        self.assertEqual(self.reschedule(TARGET_2)[1]["code"], "RESCHEDULE_LIMIT_REACHED")

    def test_not_within_six_hours_of_the_start(self):
        status, body = self.book(2, SOON)
        self.assertEqual(status, 201)
        status, body = self.reschedule(TARGET, patient=2, appointment_id=body["appointment"]["appointment_id"])
        self.assertEqual((status, body["code"]), (409, "RESCHEDULE_TOO_LATE"))

    def test_target_must_be_the_same_doctor_and_consultation(self):
        self.assertEqual(self.reschedule(OTHER_DOCTOR)[1]["code"], "DIFFERENT_DOCTOR")

    def test_full_target_is_refused_and_nothing_changes(self):
        self.assertEqual(self.book(2, TARGET)[0], 201)
        status, body = self.reschedule(TARGET)
        self.assertEqual((status, body["code"]), (409, "SLOT_FULL"))
        self.assert_exactly_one_appointment_on(ORIGINAL)

    def test_target_filling_up_after_the_api_check_ends_the_saga_with_nothing_changed(self):
        # The API saw a free seat; somebody takes it before ClaimTargetSeat runs.
        runner = AslRunner(DEFINITION, lambda payload: saga.handler(payload, None))
        self.rds.slots[TARGET]["booked_count"] = 1
        result = runner.run({"appointment_id": self.appointment_id, "patient_id": PATIENT,
                             "target_slot_id": TARGET}, "race")
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["history"][-2:], ["ReleaseLockAfterRejection", "RescheduleRejected"])
        self.assertIn(result["error"], ("RescheduleRejected", "TargetSlotFull"))
        self.assertEqual(json.loads(json.loads(result["cause"])["errorMessage"])["code"], "SLOT_FULL")
        self.assert_exactly_one_appointment_on(ORIGINAL)

    def test_another_patient_cannot_reschedule_my_appointment(self):
        self.assertEqual(self.reschedule(TARGET, patient=2)[0], 404)
        self.assert_exactly_one_appointment_on(ORIGINAL)

    def test_cancel_is_refused_while_a_reschedule_holds_the_lock(self):
        self.appts.items[self.appointment_id].update(reschedule_lock="appt-x", reschedule_lock_at=
                                                     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        status, body = self.call("DELETE", f"/appointments/{self.appointment_id}", PATIENT)
        self.assertEqual((status, body["code"]), (409, "RESCHEDULE_IN_PROGRESS"))

    def test_delete_route_cancels_with_a_refund(self):
        status, body = self.call("DELETE", f"/appointments/{self.appointment_id}", PATIENT)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["appointment"]["status"], "CANCELLED_BY_PATIENT")
        self.assertEqual(body["appointment"]["refund"]["percent"], 100.0)


class StateMachineDefinitionTests(unittest.TestCase):
    def test_every_transition_points_at_a_state(self):
        states = DEFINITION["States"]
        for name, spec in states.items():
            targets = [spec.get("Next")] + [c["Next"] for c in spec.get("Catch", [])]
            for target in filter(None, targets):
                self.assertIn(target, states, f"{name} -> {target}")

    def test_release_failure_is_routed_to_the_compensation_branch(self):
        release = DEFINITION["States"]["ReleaseOriginalSeat"]
        self.assertEqual(release["Catch"][0]["Next"], "CompensateRevertAppointment")
        self.assertEqual(DEFINITION["States"]["CompensateRevertAppointment"]["Next"], "CompensateReleaseTarget")
        self.assertEqual(DEFINITION["States"]["SwitchAppointment"]["Catch"][0]["Next"], "CompensateReleaseTarget")

    def test_non_idempotent_steps_only_retry_errors_raised_before_the_lambda_ran(self):
        for name in ("ClaimTargetSeat", "ReleaseOriginalSeat", "CompensateReleaseTarget"):
            for rule in DEFINITION["States"][name]["Retry"]:
                self.assertNotIn("States.ALL", rule["ErrorEquals"], name)


if __name__ == "__main__":
    unittest.main()
