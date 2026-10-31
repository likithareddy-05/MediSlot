"""DST policy checks for the MediSlot local-time resolver.

Run from repository root:
  python -m unittest discover -s tests -v
"""
import os
import unittest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
for key, value in {
    "DB_HOST_PARAMETER": "/test/host", "DB_PORT_PARAMETER": "/test/port",
    "DB_NAME_PARAMETER": "/test/name", "DB_USERNAME_PARAMETER": "/test/user",
    "DB_SECRET_ARN": "arn:aws:secretsmanager:ap-south-1:000000000000:secret:test",
    "SLOT_STATE_TABLE": "test-slots", "ASSETS_BUCKET": "test-assets"
}.items():
    os.environ.setdefault(key, value)

from src.lambdas.slot_time import valid_local_time


class DstMaterializerTests(unittest.TestCase):
    def test_spring_forward_nonexistent_wall_time_is_skipped(self):
        zone = ZoneInfo("America/New_York")
        # 02:30 does not exist on 8 March 2026 in New York.
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
        start_utc = chosen.astimezone(timezone.utc)
        end_utc = start_utc + timedelta(minutes=30)
        self.assertEqual(end_utc - start_utc, timedelta(minutes=30))
        # The 30-minute consultation crosses the fall-back boundary and remains
        # exactly 30 elapsed minutes even though local clock labels move backward.
        self.assertEqual(end_utc.astimezone(zone).strftime("%H:%M"), "01:00")


if __name__ == "__main__":
    unittest.main()
