import json
import logging
import os

import boto3
import pymysql
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_manager = boto3.client("secretsmanager")

DB_HOST_PARAMETER = os.environ["DB_HOST_PARAMETER"]
DB_PORT_PARAMETER = os.environ["DB_PORT_PARAMETER"]
DB_NAME_PARAMETER = os.environ["DB_NAME_PARAMETER"]
DB_USERNAME_PARAMETER = os.environ["DB_USERNAME_PARAMETER"]
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]

# Cache non-secret database settings for the lifetime of this Lambda environment.
_db_config = None


def get_db_config():
    """Read database host, port, name, and username from SSM Parameter Store."""
    global _db_config
    if _db_config is None:
        ssm_client = boto3.client("ssm")
        names = [
            DB_HOST_PARAMETER,
            DB_PORT_PARAMETER,
            DB_NAME_PARAMETER,
            DB_USERNAME_PARAMETER,
        ]
        response = ssm_client.get_parameters(Names=names, WithDecryption=False)
        values = {item["Name"]: item["Value"] for item in response.get("Parameters", [])}
        missing = [name for name in names if name not in values]
        if missing:
            raise RuntimeError("Missing database configuration SSM parameter(s): " + ", ".join(missing))
        try:
            port = int(values[DB_PORT_PARAMETER])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Database port SSM parameter must be an integer") from exc
        if not 1 <= port <= 65535:
            raise RuntimeError("Database port SSM parameter is outside the valid range")
        _db_config = {
            "host": values[DB_HOST_PARAMETER],
            "port": port,
            "name": values[DB_NAME_PARAMETER],
            "username": values[DB_USERNAME_PARAMETER],
        }
    return _db_config


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache"
        },
        "body": json.dumps(body, default=str)
    }


def get_db_credentials():
    result = secrets_manager.get_secret_value(SecretId=DB_SECRET_ARN)
    secret_string = result.get("SecretString")
    if not secret_string:
        raise RuntimeError("Database secret has no SecretString")

    credentials = json.loads(secret_string)
    password = credentials.get("password")
    if not isinstance(password, str) or not password:
        raise RuntimeError("Database secret must contain a password")
    return password


def get_db_connection():
    db_config = get_db_config()
    password = get_db_credentials()
    return pymysql.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["username"],
        password=password,
        database=db_config["name"],
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        charset="utf8mb4"
    )


def lambda_handler(event, context):
    """Return the caller's own users-table account record only."""
    connection = None
    try:
        request_context = event.get("requestContext") or {}
        authorizer = request_context.get("authorizer") or {}
        user_id = authorizer.get("userId")
        role = authorizer.get("role")

        if user_id is None or role not in {"PATIENT", "DOCTOR"}:
            return response(401, {"message": "Unauthorized"})

        # Keep the URL and JWT role aligned even if route rules are changed later.
        path = event.get("path", "")
        expected_role = {
            "/patient/me": "PATIENT",
            "/doctor/me": "DOCTOR"
        }.get(path)
        if expected_role is None:
            return response(404, {"message": "Endpoint not found"})
        if role != expected_role:
            return response(403, {"message": "Forbidden for this role"})

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, email, role, account_status, created_at, updated_at
                FROM users
                WHERE user_id = %s AND role = %s
                LIMIT 1
                """,
                (user_id, expected_role)
            )
            account = cursor.fetchone()

        if not account:
            return response(404, {"message": "Account not found"})

        # Password hashes and other authentication secrets are never returned.
        return response(200, {"account": account})

    except (ClientError, pymysql.MySQLError):
        logger.exception(json.dumps({
            "event": "account_profile_service_error",
            "request_id": getattr(context, "aws_request_id", None)
        }))
        return response(500, {"message": "Unable to retrieve account details"})
    except Exception:
        logger.exception(json.dumps({
            "event": "account_profile_failed",
            "request_id": getattr(context, "aws_request_id", None)
        }))
        return response(500, {"message": "Unable to retrieve account details"})
    finally:
        if connection:
            connection.close()
