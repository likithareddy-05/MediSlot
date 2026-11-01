import sys, unittest
from datetime import date
sys.path.insert(0,"src/lambdas")
from slot_time import build_slots

class SlotTimezoneTests(unittest.TestCase):
    def test_local_hours_remain_constant_across_dst(self):
        before=list(build_slots(date(2026,3,1),"09:00","17:00",60,"America/New_York"))
        after=list(build_slots(date(2026,3,8),"09:00","17:00",60,"America/New_York"))
        self.assertEqual(before[0][0].strftime("%H:%M"),"09:00")
        self.assertEqual(after[0][0].strftime("%H:%M"),"09:00")
        self.assertEqual(before[-1][1].strftime("%H:%M"),"17:00")
        self.assertEqual(after[-1][1].strftime("%H:%M"),"17:00")
        self.assertEqual(before[0][2].strftime("%H:%M"),"14:00")
        self.assertEqual(after[0][2].strftime("%H:%M"),"13:00")
        self.assertEqual(len(before),8)
        self.assertEqual(len(after),8)

    def test_slots_align_to_pattern_start(self):
        slots=list(build_slots(date(2026,11,2),"09:10","10:40",30,"Asia/Kolkata"))
        self.assertEqual([x[0].strftime("%H:%M") for x in slots],["09:10","09:40","10:10"])

if __name__=="__main__": unittest.main()
