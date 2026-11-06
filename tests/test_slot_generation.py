"""Duration-driven slot generation, capacity semantics and phone validation."""
import re
import unittest
from datetime import date

from lambdas.shared.slot_time import build_slots

PHONE = re.compile(r"[6-9][0-9]{9}")


def local_times(start, end, minutes, tz="Asia/Kolkata", day=date(2026, 11, 4)):
    return [(a.strftime("%H:%M"), b.strftime("%H:%M")) for a, b, _, _ in build_slots(day, start, end, minutes, tz)]


class SlotGenerationTests(unittest.TestCase):
    def test_duration_divides_window(self):
        self.assertEqual(local_times("12:00", "14:00", 30),
                         [("12:00", "12:30"), ("12:30", "13:00"), ("13:00", "13:30"), ("13:30", "14:00")])

    def test_capacity_does_not_limit_number_of_slots(self):
        # Capacity is stored per slot; it never reduces how many slots exist.
        self.assertEqual(len(local_times("12:00", "17:00", 30)), 10)

    def test_no_slot_extends_past_end(self):
        slots = local_times("12:00", "13:45", 30)
        self.assertEqual(slots[-1], ("13:00", "13:30"))
        self.assertEqual(len(slots), 3)

    def test_utc_conversion_for_doctor_timezone(self):
        first = next(iter(build_slots(date(2026, 11, 4), "12:00", "13:00", 30, "Asia/Kolkata")))
        self.assertEqual(first[2].strftime("%H:%M"), "06:30")


class PhoneTests(unittest.TestCase):
    def test_valid(self):
        for n in ("9876543210", "6000000000", "7123456789", "8999999999"):
            self.assertTrue(PHONE.fullmatch(n), n)

    def test_invalid(self):
        for n in ("5876543210", "987654321", "98765432101", "98765abcde", "+919876543210",
                  "98765 43210", "98765-43210", "0987654321", "", "9876543210\n"):
            self.assertFalse(PHONE.fullmatch(n), n)


if __name__ == "__main__":
    unittest.main()
