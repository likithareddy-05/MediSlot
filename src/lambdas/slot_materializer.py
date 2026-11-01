"""Daily rolling 30-day doctor slot materialiser. Safe to re-run: deterministic slot IDs + conditional PutItem."""
import os, json, logging, hashlib
from datetime import date, datetime, time, timedelta, timezone
from slot_time import build_slots, put_if_absent
import boto3, pymysql

log=logging.getLogger(); log.setLevel(logging.INFO)
ssm=boto3.client("ssm"); sm=boto3.client("secretsmanager"); ddb=boto3.resource("dynamodb")
TABLE=os.environ["SLOT_TABLE_NAME"]
PARAMS=[os.environ["DB_HOST_PARAMETER"],os.environ["DB_PORT_PARAMETER"],os.environ["DB_NAME_PARAMETER"],os.environ["DB_USERNAME_PARAMETER"]]
SECRET=os.environ["DB_SECRET_ARN"]
_cfg=None

def connection():
    global _cfg
    if _cfg is None:
        r=ssm.get_parameters(Names=PARAMS)["Parameters"]; v={x["Name"]:x["Value"] for x in r}
        password=json.loads(sm.get_secret_value(SecretId=SECRET)["SecretString"])["password"]
        _cfg={"host":v[PARAMS[0]],"port":int(v[PARAMS[1]]),"database":v[PARAMS[2]],"user":v[PARAMS[3]],"password":password}
    return pymysql.connect(**_cfg,connect_timeout=5,cursorclass=pymysql.cursors.DictCursor,autocommit=True)

def handler(event,context):
    requested_start=date.fromisoformat(event["start_date"]) if event.get("start_date") else None
    days=int(event.get("days",30))
    if not 1<=days<=60: raise ValueError("days must be 1..60")
    conn=connection(); table=ddb.Table(TABLE)
    created=0; skipped=0
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT d.doctor_id,ds.home_timezone,ap.day_of_week,TIME_FORMAT(ap.start_time,'%H:%i:%s') AS start_time,
                       TIME_FORMAT(ap.end_time,'%H:%i:%s') AS end_time,
                       ct.consultation_type_id,ct.duration_minutes,ct.slot_capacity
                       FROM doctors d JOIN doctor_settings ds ON ds.doctor_id=d.doctor_id
                       JOIN availability_patterns ap ON ap.doctor_id=d.doctor_id AND ap.is_active=1
                       JOIN consultation_types ct ON ct.consultation_type_id=ap.consultation_type_id AND ct.is_active=1
                       WHERE ap.is_active=1""")
            patterns=cur.fetchall()
            cur.execute("SELECT doctor_id,blackout_date FROM doctor_blackouts")
            blackouts={(int(x["doctor_id"]),x["blackout_date"].isoformat()) for x in cur.fetchall()}
        window_starts=[]
        for p in patterns:
            zone=__import__("zoneinfo").ZoneInfo(p["home_timezone"])
            start_day=requested_start or datetime.now(timezone.utc).astimezone(zone).date()
            window_starts.append(start_day.isoformat())
            did=int(p["doctor_id"])
            for offset in range(days):
                day=start_day+timedelta(days=offset)
                if int(p["day_of_week"])!=day.isoweekday() or (did,day.isoformat()) in blackouts: continue
                duration=int(p["duration_minutes"])
                for local_start,local_end,utc_start,utc_end in build_slots(
                    day,str(p["start_time"]),str(p["end_time"]),duration,p["home_timezone"]
                ):
                    material=f"{did}|{p['consultation_type_id']}|{day.isoformat()}|{local_start.time().isoformat()}|{p['home_timezone']}"
                    slot_id=hashlib.sha256(material.encode()).hexdigest()
                    item={"slot_id":slot_id,"doctor_id":str(did),"consultation_type_id":int(p["consultation_type_id"]),
                          "local_date":day.isoformat(),"local_start":local_start.time().isoformat(),
                          "start_time_utc":utc_start.isoformat(),"end_time_utc":utc_end.isoformat(),
                          "timezone":p["home_timezone"],"capacity":int(p["slot_capacity"]),
                          "booked_count":0,"status":"AVAILABLE","created_at":datetime.now(timezone.utc).isoformat()}
                    if put_if_absent(table, item):
                        created += 1
                    else:
                        skipped += 1
        return {"statusCode":200,"body":json.dumps({"created":created,"already_present":skipped,"window_start_by_timezone":sorted(set(window_starts)),"days":days})}
    finally: conn.close()
