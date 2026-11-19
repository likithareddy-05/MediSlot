"""Refund tier boundaries, rounding, policy validation (Milestone 5 unit tests).

Rules (defaults, all configurable in SSM): more than 48 h before the start -> 100%,
24-48 h -> 75%, 6-24 h -> 50%, under 6 h -> 0%. Exactly 48 h falls in the 24-48 h tier;
exactly 24 h and exactly 6 h stay in the tier that begins at that boundary's lower side.
"""
import copy
import sys
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "lambdas" / "shared"))

import refunds  # noqa: E402

START = datetime(2026, 12, 1, 10, 0, 0)
POLICY = refunds.parse_policy(refunds.DEFAULT_POLICY)
FEE = Decimal("500.00")


def refund_at(**time_before):
    """Refund when cancelling `time_before` (a timedelta's kwargs) ahead of START."""
    return refunds.calculate_refund(FEE, START, START - timedelta(**time_before), POLICY)


class TierBoundaryTests(unittest.TestCase):
    CASES = [
        # (label, time before start, expected tier, expected percent)
        ("a week before", dict(days=7), "GT_48H", 100),
        ("48 h and 1 second", dict(hours=48, seconds=1), "GT_48H", 100),
        ("48 h and 1 microsecond", dict(hours=48, microseconds=1), "GT_48H", 100),
        ("exactly 48 h", dict(hours=48), "24H_TO_48H", 75),
        ("47 h 59 m 59 s", dict(hours=47, minutes=59, seconds=59), "24H_TO_48H", 75),
        ("36 h", dict(hours=36), "24H_TO_48H", 75),
        ("exactly 24 h", dict(hours=24), "24H_TO_48H", 75),
        ("23 h 59 m 59 s", dict(hours=23, minutes=59, seconds=59), "6H_TO_24H", 50),
        ("12 h", dict(hours=12), "6H_TO_24H", 50),
        ("exactly 6 h", dict(hours=6), "6H_TO_24H", 50),
        ("5 h 59 m 59 s", dict(hours=5, minutes=59, seconds=59), "LT_6H", 0),
        ("1 h", dict(hours=1), "LT_6H", 0),
        ("1 second", dict(seconds=1), "LT_6H", 0),
    ]

    def test_every_boundary(self):
        for label, delta, tier, percent in self.CASES:
            with self.subTest(label):
                result = refund_at(**delta)
                self.assertEqual((result.tier, int(result.percent)), (tier, percent))
                self.assertEqual(result.refund_amount, (FEE * percent / 100).quantize(Decimal("0.01")))
                self.assertEqual(result.refund_amount + result.deduction_amount, FEE)

    def test_zero_percent_tier_refunds_nothing(self):
        result = refund_at(hours=2)
        self.assertEqual((result.refund_amount, result.deduction_amount), (Decimal("0.00"), FEE))

    def test_timezone_aware_inputs_give_the_same_answer(self):
        from datetime import timezone
        aware_start = START.replace(tzinfo=timezone.utc)
        result = refunds.calculate_refund(FEE, aware_start, aware_start - timedelta(hours=48), POLICY)
        self.assertEqual(result.tier, "24H_TO_48H")


class AmountTests(unittest.TestCase):
    def test_rounds_half_up_to_two_places(self):
        # 33.33 * 75% = 24.9975 -> 25.00 ; 0.01 * 50% = 0.005 -> 0.01 (half up, not banker's)
        self.assertEqual(refunds.calculate_refund(Decimal("33.33"), START, START - timedelta(hours=30), POLICY)
                         .refund_amount, Decimal("25.00"))
        self.assertEqual(refunds.calculate_refund(Decimal("0.01"), START, START - timedelta(hours=10), POLICY)
                         .refund_amount, Decimal("0.01"))

    def test_free_consultation_refunds_zero(self):
        result = refunds.calculate_refund(Decimal("0"), START, START - timedelta(days=5), POLICY)
        self.assertEqual((result.refund_amount, result.deduction_amount), (Decimal("0.00"), Decimal("0.00")))

    def test_refund_never_exceeds_the_fee(self):
        for hours in (200, 48, 30, 10, 1):
            result = refund_at(hours=hours)
            self.assertLessEqual(result.refund_amount, FEE)


class DoctorCancelAndNoShowTests(unittest.TestCase):
    def test_doctor_cancellation_is_always_a_full_refund(self):
        result = refunds.full_refund(FEE)
        self.assertEqual((result.tier, int(result.percent), result.refund_amount, result.deduction_amount),
                         ("DOCTOR_CANCELLED", 100, FEE, Decimal("0.00")))

    def test_no_show_is_never_refunded(self):
        result = refunds.no_show_refund(FEE)
        self.assertEqual((result.tier, int(result.percent), result.refund_amount, result.deduction_amount),
                         ("NO_SHOW", 0, Decimal("0.00"), FEE))


class ConfigurablePolicyTests(unittest.TestCase):
    def policy(self, **changes):
        data = copy.deepcopy(refunds.DEFAULT_POLICY)
        data.update(changes)
        return data

    def test_boundaries_and_percentages_come_from_the_policy(self):
        custom = self.policy(tiers=[
            {"name": "EARLY", "min_hours": 72, "inclusive": True, "percent": 90},
            {"name": "MID", "min_hours": 12, "inclusive": True, "percent": 40},
            {"name": "LATE", "min_hours": 0, "inclusive": True, "percent": 10}])
        parsed = refunds.parse_policy(custom)
        at = lambda h: refunds.calculate_refund(FEE, START, START - timedelta(hours=h), parsed)
        self.assertEqual((at(72).tier, int(at(72).percent)), ("EARLY", 90))
        self.assertEqual((at(71.99).tier, int(at(71.99).percent)), ("MID", 40))
        self.assertEqual((at(1).tier, at(1).refund_amount), ("LATE", Decimal("50.00")))

    def test_json_text_is_accepted(self):
        import json
        self.assertEqual(refunds.parse_policy(json.dumps(refunds.DEFAULT_POLICY))["tiers"][0]["name"], "GT_48H")

    def test_invalid_policies_are_rejected(self):
        bad = [
            "not json",
            self.policy(tiers=[]),
            self.policy(tiers=[{"name": "A", "min_hours": 24, "inclusive": True, "percent": 100}]),  # no zero tier
            self.policy(tiers=[{"name": "A", "min_hours": 0, "inclusive": True, "percent": 150}]),
            self.policy(tiers=[{"name": "A", "min_hours": 6, "inclusive": True, "percent": 50},
                               {"name": "B", "min_hours": 24, "inclusive": True, "percent": 75},
                               {"name": "C", "min_hours": 0, "inclusive": True, "percent": 0}]),  # wrong order
        ]
        for policy in bad:
            with self.subTest(policy=str(policy)[:60]):
                with self.assertRaises(refunds.PolicyError):
                    refunds.parse_policy(policy)


if __name__ == "__main__":
    unittest.main()
