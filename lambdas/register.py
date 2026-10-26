
import json
import os
import re
import logging

import boto3
import bcrypt
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")

DB_HOST_PARAMETER = os.environ["DB_HOST_PARAMETER"]
DB_PORT_PARAMETER = os.environ.get("DB_PORT_PARAMETER", "3306")
DB_NAME_PARAMETER = os.environ["DB_NAME_PARAMETER"]
DB_USERNAME_PARAMETER = os.environ["DB_USERNAME_PARAMETER"]
DB_PASSWORD_PARAMETER = os.environ["DB_PASSWORD_PARAMETER"]


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store"
        },
        "body": json.dumps(body)
    }


def get_parameter(name, decrypt=False):
    result = ssm.get_parameter(
        Name=name,
        WithDecryption=decrypt
    )
    return result["Parameter"]["Value"]


def get_db_connection():
    return pymysql.connect(
        host=get_parameter(DB_HOST_PARAMETER),
        port=int(get_parameter(DB_PORT_PARAMETER)),
        user=get_parameter(DB_USERNAME_PARAMETER),
        password=get_parameter(DB_PASSWORD_PARAMETER, decrypt=True),
        database=get_parameter(DB_NAME_PARAMETER),
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )


def lambda_handler(event, context):
    connection = None

    try:
        body = event.get("body")

        if isinstance(body, str):
            body = json.loads(body)
        elif body is None:
            body = {}

        if not isinstance(body, dict):
            return response(400, {"message": "Invalid request body"})

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

        if (
            len(email) > 254
            or not re.fullmatch(
                r"[^@\s]+@[^@\s]+\.[^@\s]+", email
            )
        ):
            return response(400, {
                "message": "Invalid email address"
            })

        if role not in {"DOCTOR", "PATIENT"}:
            return response(400, {
                "message": "Role must be DOCTOR or PATIENT"
            })

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

        if len(password.encode("utf-8")) > 72:
            return response(400, {
                "message": "Password exceeds the supported length"
            })

        password_hash = bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt(rounds=12)
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
            "request_id": getattr(context, "aws_request_id", None),
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

        logger.exception("Database integrity error")
        return response(500, {
            "message": "Registration failed"
        })

    except Exception:
        if connection:
            connection.rollback()

        logger.exception(json.dumps({
            "event": "registration_failed",
            "request_id": getattr(context, "aws_request_id", None)
        }))

        return response(500, {
            "message": "Registration failed"
        })

    finally:
        if connection:
            connection.close()
