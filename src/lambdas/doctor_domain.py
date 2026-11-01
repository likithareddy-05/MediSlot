"""MediSlot Milestone 3 doctor-domain API Lambda.

All API requests are authenticated by the existing API Gateway JWT authorizer.
RDS is the source of truth for doctor configuration; DynamoDB stores materialised slots.
"""
import base64, json, logging, os, re, uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import boto3
import pymysql

log = logging.getLogger()
log.setLevel(logging.INFO)
ssm = boto3.client("ssm")
secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
ddb = boto3.resource("dynamodb")

DB_HOST_PARAMETER = os.environ["DB_HOST_PARAMETER"]
DB_PORT_PARAMETER = os.environ["DB_PORT_PARAMETER"]
DB_NAME_PARAMETER = os.environ["DB_NAME_PARAMETER"]
DB_USERNAME_PARAMETER = os.environ["DB_USERNAME_PARAMETER"]
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
SLOT_TABLE_NAME = os.environ["SLOT_TABLE_NAME"]
ASSETS_BUCKET = os.environ["ASSETS_BUCKET"]
MATERIALIZER_FUNCTION_NAME = os.environ.get("MATERIALIZER_FUNCTION_NAME")
_db = None

def db_config():
    global _db
    if _db is None:
        names=[DB_HOST_PARAMETER,DB_PORT_PARAMETER,DB_NAME_PARAMETER,DB_USERNAME_PARAMETER]
        vals={x["Name"]:x["Value"] for x in ssm.get_parameters(Names=names)["Parameters"]}
        if any(n not in vals for n in names): raise RuntimeError("Database SSM configuration missing")
        pwd=json.loads(secrets.get_secret_value(SecretId=DB_SECRET_ARN)["SecretString"])["password"]
        _db={ "host":vals[DB_HOST_PARAMETER], "port":int(vals[DB_PORT_PARAMETER]),
              "database":vals[DB_NAME_PARAMETER], "user":vals[DB_USERNAME_PARAMETER], "password":pwd }
    return _db

def connect():
    c=db_config()
    return pymysql.connect(**c, connect_timeout=5, read_timeout=15, write_timeout=15,
                           cursorclass=pymysql.cursors.DictCursor, autocommit=False, charset="utf8mb4")

def reply(code, body):
    return {"statusCode":code,"headers":{"Content-Type":"application/json","Cache-Control":"no-store"},
            "body":json.dumps(body,default=json_default)}

def json_default(v):
    if isinstance(v,(datetime,date,time)): return v.isoformat()
    if isinstance(v,Decimal): return float(v)
    return str(v)

def body_of(event):
    raw=event.get("body")
    if not raw: return {}
    if event.get("isBase64Encoded"): raw=base64.b64decode(raw).decode()
    return json.loads(raw)

def doctor_identity(event):
    a=((event.get("requestContext") or {}).get("authorizer") or {})
    if a.get("role") != "DOCTOR" or not a.get("userId"): return None
    return str(a["userId"])

def doctor_id_for(conn,user_id):
    with conn.cursor() as cur:
        cur.execute("SELECT doctor_id FROM doctors WHERE user_id=%s",(user_id,))
        r=cur.fetchone()
    return r["doctor_id"] if r else None

def scan_doctor_slots(table, did, local_date):
    items=[]
    kwargs={"FilterExpression": Attr("local_date").eq(local_date) & Attr("doctor_id").eq(str(did))}
    while True:
        page=table.scan(**kwargs)
        items.extend(page.get("Items",[]))
        if "LastEvaluatedKey" not in page: break
        kwargs["ExclusiveStartKey"]=page["LastEvaluatedKey"]
    return items

def refresh_future_slots(did, timezone_name="Asia/Kolkata"):
    table=ddb.Table(SLOT_TABLE_NAME)
    today=datetime.now(timezone.utc).astimezone(ZoneInfo(timezone_name)).date().isoformat()
    kwargs={"FilterExpression": Attr("doctor_id").eq(str(did)) & Attr("status").eq("AVAILABLE") & Attr("local_date").gte(today)}
    while True:
        page=table.scan(**kwargs)
        for item in page.get("Items",[]):
            if int(item.get("booked_count",0))==0:
                table.delete_item(Key={"slot_id":item["slot_id"]})
        if "LastEvaluatedKey" not in page: break
        kwargs["ExclusiveStartKey"]=page["LastEvaluatedKey"]
    if MATERIALIZER_FUNCTION_NAME:
        try:
            lambda_client.invoke(FunctionName=MATERIALIZER_FUNCTION_NAME,InvocationType="Event",Payload=b"{}")
        except Exception:
            log.exception("Could not invoke slot materialiser; daily schedule remains active")

def ensure_settings(conn,did):
    with conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO doctor_settings (doctor_id,home_timezone) VALUES (%s,'Asia/Kolkata')",(did,))

def fetch_profile(conn,did):
    with conn.cursor() as cur:
        cur.execute("""SELECT d.doctor_id,d.user_id,d.first_name,d.last_name,u.email,d.phone,
          d.date_of_birth,d.gender,d.address,d.specialization,d.license_number,d.years_of_experience,
          d.clinic_address,ds.home_timezone,ds.photo_key,d.created_at,d.updated_at
          FROM doctors d JOIN users u ON u.user_id=d.user_id
          LEFT JOIN doctor_settings ds ON ds.doctor_id=d.doctor_id WHERE d.doctor_id=%s""",(did,))
        return cur.fetchone()

def handle(event, context):
    user_id=doctor_identity(event)
    if not user_id: return reply(403,{"message":"Doctor role required"})
    method=event.get("httpMethod","GET").upper()
    path=(event.get("path") or "").rstrip("/") or "/"
    qs=event.get("queryStringParameters") or {}
    data=body_of(event)
    conn=None
    try:
        conn=connect()
        did=doctor_id_for(conn,user_id)
        if not did: return reply(404,{"message":"Doctor profile not found"})
        ensure_settings(conn,did)
        # GET /doctors/me and PUT /doctors/me
        if path.endswith("/doctors/me") and method=="GET":
            conn.commit(); return reply(200,{"profile":fetch_profile(conn,did)})
        if path.endswith("/doctors/me") and method=="PUT":
            allowed=["first_name","last_name","phone","date_of_birth","gender","address","specialization","years_of_experience","clinic_address"]
            updates={k:data[k] for k in allowed if k in data}
            if not updates: return reply(400,{"message":"No editable profile fields supplied"})
            if "years_of_experience" in updates and not 0 <= int(updates["years_of_experience"]) <= 60:
                return reply(400,{"message":"years_of_experience must be between 0 and 60"})
            if "date_of_birth" in updates: date.fromisoformat(str(updates["date_of_birth"]))
            sql=", ".join(f"{k}=%s" for k in updates)
            with conn.cursor() as cur: cur.execute(f"UPDATE doctors SET {sql} WHERE doctor_id=%s",(*updates.values(),did))
            conn.commit(); return reply(200,{"profile":fetch_profile(conn,did)})
        # Photo URL; object key is constrained to this doctor's prefix.
        if path.endswith("/doctors/me/photo") and method=="POST":
            filename=os.path.basename(data.get("filename","profile.jpg"))
            ext=os.path.splitext(filename)[1].lower()
            if ext not in {".jpg",".jpeg",".png",".webp"}: return reply(400,{"message":"Only JPG, PNG or WEBP images are supported"})
            key=f"doctor-photos/{did}/{uuid.uuid4().hex}{ext}"
            url=s3.generate_presigned_url("put_object",Params={"Bucket":ASSETS_BUCKET,"Key":key,"ContentType":data.get("content_type","image/jpeg")},ExpiresIn=300)
            with conn.cursor() as cur: cur.execute("UPDATE doctor_settings SET photo_key=%s WHERE doctor_id=%s",(key,did))
            conn.commit()
            return reply(200,{"upload_url":url,"photo_key":key,"expires_in":300,"method":"PUT"})
        # Consultation type CRUD.
        if "/consultation-types" in path:
            if method=="GET":
                with conn.cursor() as cur: cur.execute("SELECT * FROM consultation_types WHERE doctor_id=%s ORDER BY consultation_type_id",(did,)); rows=cur.fetchall()
                conn.commit(); return reply(200,{"consultation_types":rows})
            if method=="POST":
                name=data.get("consultation_name"); duration=int(data.get("duration_minutes",0)); fee=Decimal(str(data.get("fee",0))); cap=int(data.get("slot_capacity",1))
                if not name or not 5<=duration<=480 or fee<0 or not 1<=cap<=100: return reply(400,{"message":"Invalid consultation name, duration, fee or capacity"})
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO consultation_types (doctor_id,consultation_name,duration_minutes,fee,slot_capacity) VALUES (%s,%s,%s,%s,%s)",(did,name,duration,fee,cap)); cid=cur.lastrowid
                conn.commit(); return reply(201,{"consultation_type_id":cid})
            m=re.search(r"/consultation-types/(\d+)$",path)
            if m:
                cid=int(m.group(1))
                with conn.cursor() as cur:
                    cur.execute("SELECT consultation_type_id FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s",(cid,did))
                    if not cur.fetchone(): return reply(404,{"message":"Consultation type not found"})
                    if method=="PUT":
                        allowed=["consultation_name","duration_minutes","fee","slot_capacity","is_active"]
                        updates={k:data[k] for k in allowed if k in data}
                        if not updates: return reply(400,{"message":"No fields supplied"})
                        sql=", ".join(f"{k}=%s" for k in updates)
                        cur.execute(f"UPDATE consultation_types SET {sql} WHERE consultation_type_id=%s AND doctor_id=%s",(*updates.values(),cid,did))
                    elif method=="DELETE":
                        cur.execute("UPDATE consultation_types SET is_active=0 WHERE consultation_type_id=%s AND doctor_id=%s",(cid,did))
                    else: return reply(405,{"message":"Method not allowed"})
                conn.commit(); return reply(200,{"message":"Consultation type updated" if method=="PUT" else "Consultation type deactivated"})
        # Replace all weekly patterns in one transaction.
        if path.endswith("/availability") and method=="PUT":
            tz=data.get("home_timezone","Asia/Kolkata")
            try: ZoneInfo(tz)
            except ZoneInfoNotFoundError: return reply(400,{"message":"home_timezone must be a valid IANA timezone"})
            patterns=data.get("patterns",[])
            with conn.cursor() as cur:
                cur.execute("UPDATE doctor_settings SET home_timezone=%s WHERE doctor_id=%s",(tz,did))
                cur.execute("DELETE FROM availability_patterns WHERE doctor_id=%s",(did,))
                for p in patterns:
                    day=int(p["day_of_week"]); start=time.fromisoformat(p["start_time"]); end=time.fromisoformat(p["end_time"]); cid=int(p["consultation_type_id"])
                    if day not in range(1,8) or end<=start: raise ValueError("Invalid weekly availability pattern")
                    cur.execute("SELECT consultation_type_id FROM consultation_types WHERE doctor_id=%s AND consultation_type_id=%s AND is_active=1",(did,cid))
                    if not cur.fetchone(): raise ValueError("Consultation type does not belong to this doctor or is inactive")
                    cur.execute("""INSERT INTO availability_patterns (doctor_id,day_of_week,start_time,end_time,consultation_type_id)
                                   VALUES (%s,%s,%s,%s,%s)""",(did,day,start,end,cid))
            conn.commit()
            refresh_future_slots(did)
            return reply(200,{"message":"Availability saved; future slots are being refreshed","pattern_count":len(patterns),"home_timezone":tz})
        if path.endswith("/availability") and method=="GET":
            with conn.cursor() as cur:
                cur.execute("SELECT home_timezone FROM doctor_settings WHERE doctor_id=%s",(did,)); tz=cur.fetchone()["home_timezone"]
                cur.execute("""SELECT availability_pattern_id,day_of_week,TIME_FORMAT(start_time,'%H:%i') AS start_time,
                               TIME_FORMAT(end_time,'%H:%i') AS end_time,consultation_type_id
                               FROM availability_patterns WHERE doctor_id=%s AND is_active=1 ORDER BY day_of_week,start_time""",(did,)); rows=cur.fetchall()
            conn.commit(); return reply(200,{"home_timezone":tz,"patterns":rows})
        # Blackout dates
        if "/blackouts" in path:
            if method=="POST":
                d=date.fromisoformat(data["blackout_date"])
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO doctor_blackouts (doctor_id,blackout_date,reason) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE reason=VALUES(reason)",(did,d,data.get("reason")))
                    # Suppress slots already materialised for this local date.
                    slot_table=ddb.Table(SLOT_TABLE_NAME)
                    slots=scan_doctor_slots(slot_table,did,d.isoformat())
                    for slot in slots:
                        cur.execute("""SELECT a.appointment_id,a.patient_id,c.fee FROM appointments a
                            JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id
                            WHERE a.slot_id=%s AND a.doctor_id=%s AND a.status='BOOKED'""",(slot["slot_id"],did))
                        for appt in cur.fetchall():
                            cur.execute("UPDATE appointments SET status='CANCELLED' WHERE appointment_id=%s",(appt["appointment_id"],))
                            cur.execute("""INSERT INTO refund_ledger (appointment_id,patient_id,doctor_id,refund_amount,refund_status)
                                VALUES (%s,%s,%s,%s,'PENDING') ON DUPLICATE KEY UPDATE appointment_id=VALUES(appointment_id)""",
                                (appt["appointment_id"],appt["patient_id"],did,appt["fee"]))
                conn.commit()
                for slot in slots:
                    slot_table.update_item(Key={"slot_id":slot["slot_id"]},
                        UpdateExpression="SET #s=:cancelled, updated_at=:now",
                        ExpressionAttributeNames={"#s":"status"},
                        ExpressionAttributeValues={":cancelled":"CANCELLED",":now":datetime.now(timezone.utc).isoformat()})
                return reply(201,{"message":"Blackout date saved; existing slots suppressed","suppressed_slots":len(slots)})
            if method=="GET":
                with conn.cursor() as cur: cur.execute("SELECT blackout_id,blackout_date,reason FROM doctor_blackouts WHERE doctor_id=%s AND blackout_date >= CURDATE() ORDER BY blackout_date",(did,)); rows=cur.fetchall()
                conn.commit(); return reply(200,{"blackouts":rows})
            m=re.search(r"/blackouts/(\d+)$",path)
            if m and method=="DELETE":
                with conn.cursor() as cur:
                    cur.execute("SELECT blackout_date FROM doctor_blackouts WHERE blackout_id=%s AND doctor_id=%s",(int(m.group(1)),did))
                    blackout=cur.fetchone()
                    if not blackout: return reply(404,{"message":"Blackout not found"})
                    blackout_day=blackout["blackout_date"].isoformat()
                    cur.execute("DELETE FROM doctor_blackouts WHERE blackout_id=%s AND doctor_id=%s",(int(m.group(1)),did))
                    cur.execute("SELECT home_timezone FROM doctor_settings WHERE doctor_id=%s",(did,))
                    tz_row=cur.fetchone()
                conn.commit()
                # Remove cancelled slot markers for this date so the deterministic
                # IDs can be materialised again after the blackout is lifted.
                table=ddb.Table(SLOT_TABLE_NAME)
                for slot in scan_doctor_slots(table,did,blackout_day):
                    if slot.get("status")=="CANCELLED":
                        table.delete_item(Key={"slot_id":slot["slot_id"]})
                refresh_future_slots(did,tz_row["home_timezone"] if tz_row else "Asia/Kolkata")
                return reply(200,{"message":"Blackout removed; slot materialisation refreshed"})
        # Doctor day schedule. Query local date, convert its bounds using doctor's zone.
        if path.endswith("/schedule") and method=="GET":
            day=date.fromisoformat(qs.get("date",date.today().isoformat()))
            with conn.cursor() as cur:
                cur.execute("SELECT home_timezone FROM doctor_settings WHERE doctor_id=%s",(did,)); tz=ZoneInfo(cur.fetchone()["home_timezone"])
                start=datetime.combine(day,time.min,tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)
                end=datetime.combine(day+timedelta(days=1),time.min,tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)
                cur.execute("""SELECT a.appointment_id,a.slot_id,a.status,a.outcome,a.scheduled_start_utc,a.scheduled_end_utc,
                  p.patient_id,p.first_name,p.last_name,p.phone,p.gender,p.date_of_birth,c.consultation_name
                  FROM appointments a JOIN patients p ON p.patient_id=a.patient_id
                  JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id
                  WHERE a.doctor_id=%s AND a.scheduled_start_utc >= %s AND a.scheduled_start_utc < %s
                  ORDER BY a.scheduled_start_utc""",(did,start,end)); appts=cur.fetchall()
            conn.commit()
            slots=scan_doctor_slots(ddb.Table(SLOT_TABLE_NAME),did,day.isoformat())
            return reply(200,{"date":day.isoformat(),"timezone":str(tz),"slots":slots,"appointments":appts})
        if path.endswith("/appointments") and method=="GET":
            start=qs.get("from"); end=qs.get("to"); status=qs.get("status")
            if not start or not end: return reply(400,{"message":"from and to dates are required"})
            if len(start)==10: start=datetime.combine(date.fromisoformat(start),time.min)
            if len(end)==10: end=datetime.combine(date.fromisoformat(end)+timedelta(days=1),time.min)
            sql="""SELECT a.appointment_id,a.slot_id,a.status,a.outcome,a.scheduled_start_utc,a.scheduled_end_utc,
                   p.first_name,p.last_name,p.phone,c.consultation_name
                   FROM appointments a JOIN patients p ON p.patient_id=a.patient_id
                   JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id
                   WHERE a.doctor_id=%s AND a.scheduled_start_utc >= %s AND a.scheduled_start_utc < %s"""
            args=[did,start,end]
            if status: sql+=" AND a.status=%s"; args.append(status.upper())
            with conn.cursor() as cur: cur.execute(sql+" ORDER BY a.scheduled_start_utc",args); rows=cur.fetchall()
            conn.commit(); return reply(200,{"appointments":rows})
        m=re.search(r"/appointments/(\d+)/outcome$",path)
        if m and method=="POST":
            outcome=data.get("outcome","").upper()
            if outcome not in {"COMPLETED","NO_SHOW"}: return reply(400,{"message":"outcome must be COMPLETED or NO_SHOW"})
            with conn.cursor() as cur:
                cur.execute("UPDATE appointments SET status=%s,outcome=%s WHERE appointment_id=%s AND doctor_id=%s AND status='BOOKED'",(outcome,outcome,int(m.group(1)),did))
                if cur.rowcount==0: return reply(404,{"message":"Bookable appointment not found"})
            conn.commit(); return reply(200,{"message":f"Appointment marked {outcome}"})
        m=re.search(r"/slots/([^/]+)$",path)
        if m and method=="DELETE":
            slot_id=m.group(1)
            table=ddb.Table(SLOT_TABLE_NAME)
            item=table.get_item(Key={"slot_id":slot_id}).get("Item")
            if not item or str(item.get("doctor_id"))!=str(did): return reply(404,{"message":"Slot not found"})
            # Persist full-fee refund instructions in the ledger for every booked patient.
            # The current repository has no payment gateway/settlement integration, so
            # ledger entries are PENDING until a payment processor is connected.
            with conn.cursor() as cur:
                cur.execute("""SELECT a.appointment_id,a.patient_id,c.fee
                    FROM appointments a JOIN consultation_types c ON c.consultation_type_id=a.consultation_type_id
                    WHERE a.slot_id=%s AND a.doctor_id=%s AND a.status='BOOKED'""",(slot_id,did))
                affected=cur.fetchall()
                for appt in affected:
                    cur.execute("UPDATE appointments SET status='CANCELLED' WHERE appointment_id=%s AND status='BOOKED'",(appt["appointment_id"],))
                    cur.execute("""INSERT INTO refund_ledger (appointment_id,patient_id,doctor_id,refund_amount,refund_status)
                        VALUES (%s,%s,%s,%s,'PENDING')
                        ON DUPLICATE KEY UPDATE appointment_id=VALUES(appointment_id)""",
                        (appt["appointment_id"],appt["patient_id"],did,appt["fee"]))
            conn.commit()
            try:
                table.update_item(Key={"slot_id":slot_id},UpdateExpression="SET #s=:cancelled, updated_at=:now",
                  ConditionExpression="#s <> :cancelled",ExpressionAttributeNames={"#s":"status"},
                  ExpressionAttributeValues={":cancelled":"CANCELLED",":now":datetime.now(timezone.utc).isoformat()})
            except Exception as update_error:
                if "ConditionalCheckFailedException" not in str(update_error): raise
            return reply(200,{"message":"Slot cancelled","slot_id":slot_id,"affected_appointments":len(affected),
                              "refunds_created":len(affected),"refund_status":"PENDING_PAYMENT_PROCESSOR"})
        conn.commit()
        return reply(404,{"message":"Doctor endpoint not found","path":path,"method":method})
    except (ValueError,KeyError,TypeError) as e:
        if conn: conn.rollback()
        return reply(400,{"message":"Invalid request","detail":str(e)})
    except Exception:
        if conn: conn.rollback()
        log.exception("doctor_domain_error")
        return reply(500,{"message":"Doctor service failed"})
    finally:
        if conn: conn.close()

def lambda_handler(event, context):
    return handle(event,context)
