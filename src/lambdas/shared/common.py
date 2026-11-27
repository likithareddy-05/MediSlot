"""Helpers shared by the MediSlot domain Lambdas (patient domain today).

Packaged next to patient_domain.py in the patient-domain ZIP. Environment
variables are read when a function runs, not at import time, so the module can be
imported in tests without any AWS configuration.
"""
import json
import random
import logging
import os
import time

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ssm = boto3.client("ssm")
_secrets = boto3.client("secretsmanager")
_db_config = None


_config_cache = {}
CONFIG_TTL_SECONDS = 60


def get_config_value(name, default=None, ttl=CONFIG_TTL_SECONDS):
    """Read one SSM parameter, cached for about a minute per Lambda container.

    Changing the parameter therefore takes effect without a redeploy. If SSM cannot be
    reached the last good value is kept; with none yet the caller's default is used.
    """
    if not name:
        return default
    cached = _config_cache.get(name)
    now = time.monotonic()
    if cached and now - cached[0] < ttl:
        return cached[1]
    try:
        value = _ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception as exc:
        logger.warning(json.dumps({"event": "config_read_failed", "parameter": name, "error": str(exc)[:200]}))
        return cached[1] if cached else default
    _config_cache[name] = (now, value)
    return value


def int_setting(env_name, default):
    """A positive whole number from the SSM parameter named by an environment variable.

    Falls back to `default` when the variable is unset, SSM cannot be read, or the value is
    not a positive whole number (a typo in Parameter Store must not break the API).
    """
    raw = get_config_value(os.environ.get(env_name), None)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except ValueError:
        logger.warning(json.dumps({"event": "setting_not_a_number", "setting": env_name}))
        return default
    return value if value > 0 else default


class ApiError(Exception):
    """A business rule said no. Carries the HTTP status and a machine-readable code."""

    def __init__(self, status, code, message, **extra):
        super().__init__(message)
        self.status, self.code, self.message, self.extra = status, code, message, extra


def error_response(exc):
    return response(exc.status, {"message": exc.message, "code": exc.code, **exc.extra})


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


_db_password = {"value": None, "expires": 0.0}
TRANSIENT_DB_ERRORS = {1040, 1205, 1213, 2003, 2006, 2013}  # too many connections, lock timeout, deadlock, lost/refused


def is_transient_db_error(exc):
    """True for temporary database trouble (overload, deadlock, dropped connection) that a retry may fix."""
    code = exc.args[0] if getattr(exc, "args", None) and isinstance(exc.args[0], int) else None
    return isinstance(exc, pymysql.err.OperationalError) and code in TRANSIENT_DB_ERRORS


def _database_password():
    """The DB password from Secrets Manager, cached for 5 minutes per container (one call, not one per connection)."""
    now = time.monotonic()
    if _db_password["value"] is None or now >= _db_password["expires"]:
        secret = _secrets.get_secret_value(SecretId=os.environ["DB_SECRET_ARN"]).get("SecretString")
        if not secret:
            raise RuntimeError("Database secret is empty")
        _db_password["value"] = json.loads(secret)["password"]
        _db_password["expires"] = now + 300
    return _db_password["value"]


def db_connect():
    """Open a MySQL connection (transactions are explicit: autocommit is off).

    A burst of simultaneous requests can briefly exhaust the database's connection limit, so
    temporary connect errors are retried a few times with a short random delay.
    """
    cfg = _db_settings()
    last_error = None
    for attempt in range(1, 5):
        try:
            return pymysql.connect(
                host=cfg["host"], port=cfg["port"], user=cfg["username"], password=_database_password(),
                database=cfg["name"], connect_timeout=5, read_timeout=15, write_timeout=15,
                cursorclass=pymysql.cursors.DictCursor, autocommit=False, charset="utf8mb4",
            )
        except pymysql.err.OperationalError as exc:
            last_error = exc
            logger.warning(json.dumps({"event": "db_connect_retry", "attempt": attempt,
                                                    "error": str(exc)[:200]}))
            if attempt == 4:
                break
            time.sleep(random.uniform(0.05, 0.25) * attempt)
    raise last_error
