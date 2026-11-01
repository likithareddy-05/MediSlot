"""Timezone-safe slot calculations shared by the materialiser and tests."""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

def _valid_local(day, wall_time, zone):
    naive=datetime.combine(day,wall_time)
    aware=naive.replace(tzinfo=zone,fold=0)
    # Nonexistent local wall times do not round-trip through UTC.
    if aware.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) != naive:
        return None
    return aware

def build_slots(day, start_time, end_time, duration_minutes, timezone_name):
    """Yield (local_start, local_end, utc_start, utc_end) aligned from pattern start."""
    zone=ZoneInfo(timezone_name)
    if isinstance(start_time,str): start_time=time.fromisoformat(start_time)
    if isinstance(end_time,str): end_time=time.fromisoformat(end_time)
    if end_time<=start_time: raise ValueError("end_time must be after start_time")
    cursor=datetime.combine(day,start_time)
    finish=datetime.combine(day,end_time)
    while cursor+timedelta(minutes=duration_minutes)<=finish:
        local_start=_valid_local(day,cursor.time(),zone)
        end_naive=cursor+timedelta(minutes=duration_minutes)
        local_end=_valid_local(day,end_naive.time(),zone)
        if local_start and local_end:
            yield (local_start,local_end,local_start.astimezone(timezone.utc),local_end.astimezone(timezone.utc))
        cursor+=timedelta(minutes=duration_minutes)
