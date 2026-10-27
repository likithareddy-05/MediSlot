import json
import os
import re
import logging

import boto3
import bcrypt
import pymysql

from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_manager = boto3.client("secretsmanager")
ssm = boto3.client("ssm")

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
BCRYPT_WORK_FACTOR_PARAMETER = os.environ["BCRYPT_WORK_FACTOR_PARAMETER"]


def get_bcrypt_work_factor():
    """Read and validate the bcrypt work factor from SSM."""
    result = boto3.client("ssm").get_parameter(
        Name=BCRYPT_WORK_FACTOR_PARAMETER, WithDecryption=False
    )
    raw_value = result["Parameter"]["Value"]
    try:
        rounds = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("SSM bcrypt work factor must be an integer") from exc
    if not 12 <= rounds <= 15:
        raise RuntimeError("SSM bcrypt work factor must be between 12 and 15")
    return rounds


def response(status_code, body):
    """Return a consistent API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache"
        },
        "body": json.dumps(body)
    }


def get_db_credentials():
    """Retrieve RDS credentials from Secrets Manager."""
    result = secrets_manager.get_secret_value(
        SecretId=DB_SECRET_ARN
    )

    secret_string = result.get("SecretString")

    if not secret_string:
        raise RuntimeError("Database secret has no SecretString")

    credentials = json.loads(secret_string)

    password = credentials.get("password")
    if not isinstance(password, str) or not password:
        raise RuntimeError("Database secret must contain a password")
    return password


def get_db_connection():
    """Connect to the RDS MySQL database."""
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
        autocommit=False,
        charset="utf8mb4"
    )


def lambda_handler(event, context):
    """API Gateway Lambda handler for user registration."""
    connection = None

    try:
        body = event.get("body")

        if isinstance(body, str):
            body = json.loads(body)
        elif body is None:
            body = {}

        if not isinstance(body, dict):
            return response(400, {
                "message": "Invalid request body"
            })

        email = body.get("email")
        password = body.get("password")
        role = body.get("role")

        if not all(
            isinstance(value, str)
            for value in (email, password, role)
        ):
            return response(400, {
                "message": "Email, password and role are required"
            })

        email = email.strip().lower()
        role = role.strip().upper()

        # Validate email format and length.
        if (
            len(email) > 254
            or not re.fullmatch(
                r"[^@\s]+@[^@\s]+\.[^@\s]+",
                email
            )
        ):
            return response(400, {
                "message": "Invalid email address"
            })

        # Only the supported application roles may register.
        if role not in {"DOCTOR", "PATIENT"}:
            return response(400, {
                "message": "Role must be DOCTOR or PATIENT"
            })

        # Enforce the registration password policy.
        if (
            len(password) < 10
            or not re.search(r"[A-Za-z]", password)
            or not re.search(r"\d", password)
        ):
            return response(400, {
                "message": (
                    "Password must be at least 10 characters "
                    "and contain a letter and a digit"
                )
            })

        # bcrypt supports a maximum of 72 password bytes.
        if len(password.encode("utf-8")) > 72:
            return response(400, {
                "message": "Password exceeds the supported length"
            })

        # Read the configured bcrypt work factor and hash the password.
        bcrypt_work_factor = get_bcrypt_work_factor()
        password_hash = bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt(rounds=bcrypt_work_factor)
        ).decode("utf-8")

        connection = get_db_connection()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO users (email, password_hash, role)
                VALUES (%s, %s, %s)
                """,
                (email, password_hash, role)
            )

        connection.commit()

        logger.info(json.dumps({
            "event": "user_registered",
            "request_id": getattr(
                context, "aws_request_id", None
            ),
            "role": role
        }))

        return response(201, {
            "message": "Registration successful"
        })

    except json.JSONDecodeError:
        return response(400, {
            "message": "Request body must be valid JSON"
        })

    except pymysql.err.IntegrityError as exc:
        if connection:
            connection.rollback()

        if exc.args and exc.args[0] == 1062:
            return response(409, {
                "message": "An account with this email already exists"
            })

        logger.exception(json.dumps({
            "event": "registration_database_integrity_error",
            "request_id": getattr(
                context, "aws_request_id", None
            )
        }))

        return response(500, {
            "message": "Registration failed"
        })

    except (ClientError, pymysql.MySQLError):
        if connection:
            connection.rollback()

        logger.exception(json.dumps({
            "event": "registration_service_error",
            "request_id": getattr(
                context, "aws_request_id", None
            )
        }))

        return response(500, {
            "message": "Registration failed"
        })

    except Exception:
        if connection:
            connection.rollback()

        logger.exception(json.dumps({
            "event": "registration_failed",
            "request_id": getattr(
                context, "aws_request_id", None
            )
        }))

        return response(500, {
            "message": "Registration failed"
        })

    finally:
        if connection:
            connection.close()
