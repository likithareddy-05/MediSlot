"""Helpers shared by the MediSlot domain Lambdas (patient domain today).

Packaged next to patient_domain.py in the patient-domain ZIP. Environment
variables are read when a function runs, not at import time, so the module can be
imported in tests without any AWS configuration.
"""
import json
import logging
import os

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ssm = boto3.client("ssm")
_secrets = boto3.client("secretsmanager")
_db_config = None


def log_event(event_name, level=logging.INFO, **fields):
    """Emit one structured JSON log line (searchable with CloudWatch Logs Insights).

    Only ids, counts and field names belong here. Never pass emails, phone
    numbers, names, addresses, passwords or tokens.
    """
    logger.log(level, json.dumps({"event": event_name, **fields}, default=str))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Authorization,Content-Type,X-Amz-Date,X-Api-Key,X-Amz-Security-Token,Idempotency-Key",
            "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }


def json_body(event):
    """Parse the JSON request body into a dict ({} when there is no body)."""
    raw = event.get("body")
    if not raw:
        return {}
    if event.get("isBase64Encoded"):
        import base64
        raw = base64.b64decode(raw).decode("utf-8")
    body = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")
    return body


def _db_settings():
    global _db_config
    if _db_config is None:
        names = [
            os.environ["DB_HOST_PARAMETER"],
            os.environ["DB_PORT_PARAMETER"],
            os.environ["DB_NAME_PARAMETER"],
            os.environ["DB_USERNAME_PARAMETER"],
        ]
        result = _ssm.get_parameters(Names=names, WithDecryption=False)
        values = {p["Name"]: p["Value"] for p in result.get("Parameters", [])}
        missing = [n for n in names if n not in values]
        if missing:
            raise RuntimeError("Missing DB SSM parameters: " + ", ".join(missing))
        _db_config = {
            "host": values[names[0]],
            "port": int(values[names[1]]),
            "name": values[names[2]],
            "username": values[names[3]],
        }
    return _db_config


def db_connect():
    """Open a MySQL connection (transactions are explicit: autocommit is off)."""
    cfg = _db_settings()
    secret = _secrets.get_secret_value(SecretId=os.environ["DB_SECRET_ARN"]).get("SecretString")
    if not secret:
        raise RuntimeError("Database secret is empty")
    password = json.loads(secret)["password"]
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["username"], password=password,
        database=cfg["name"], connect_timeout=5, read_timeout=15, write_timeout=15,
        cursorclass=pymysql.cursors.DictCursor, autocommit=False, charset="utf8mb4",
    )
