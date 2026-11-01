"""DST policy checks for the MediSlot local-time resolver and slot builder."""
import unittest
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.lambdas.slot_time import build_slots, valid_local_time


class DstMaterializerTests(unittest.TestCase):
    def test_spring_forward_nonexistent_wall_time_is_skipped(self):
        zone = ZoneInfo("America/New_York")
        self.assertIsNone(valid_local_time(datetime(2026, 3, 8, 2, 30), zone))

    def test_spring_forward_valid_times_have_correct_utc_offsets(self):
        zone = ZoneInfo("America/New_York")
        before = valid_local_time(datetime(2026, 3, 8, 1, 30), zone)
        after = valid_local_time(datetime(2026, 3, 8, 3, 0), zone)
        self.assertEqual(before.utcoffset(), timedelta(hours=-5))
        self.assertEqual(after.utcoffset(), timedelta(hours=-4))

    def test_fall_back_ambiguous_time_chooses_first_occurrence(self):
        zone = ZoneInfo("America/New_York")
        chosen = valid_local_time(datetime(2026, 11, 1, 1, 30), zone)
        self.assertEqual(chosen.fold, 0)
        self.assertEqual(chosen.utcoffset(), timedelta(hours=-4))

    def test_slot_crossing_fall_back_keeps_elapsed_duration(self):
        slots = list(build_slots(date(2026, 11, 1), "01:30", "02:00", 30, "America/New_York"))
        self.assertEqual(len(slots), 1)
        local_start, local_end, utc_start, utc_end = slots[0]
        self.assertEqual(local_start.strftime("%H:%M"), "01:30")
        self.assertEqual(local_start.fold, 0)
        self.assertEqual(local_end.strftime("%H:%M"), "01:00")
        self.assertEqual(local_end.fold, 1)
        self.assertEqual(utc_end - utc_start, timedelta(minutes=30))

    def test_spring_forward_skips_nonexistent_starts_and_preserves_duration(self):
        slots = list(build_slots(date(2026, 3, 8), "01:00", "04:00", 30, "America/New_York"))
        labels = [start.strftime("%H:%M") for start, _, _, _ in slots]
        self.assertEqual(labels, ["01:00", "01:30", "03:00", "03:30"])
        for _, _, utc_start, utc_end in slots:
            self.assertEqual(utc_end - utc_start, timedelta(minutes=30))


if __name__ == "__main__":
    unittest.main()
