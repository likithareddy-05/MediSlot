import json
import logging
import os
import re
from datetime import date

import bcrypt
import boto3
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
BCRYPT_WORK_FACTOR_PARAMETER = os.environ["BCRYPT_WORK_FACTOR_PARAMETER"]

# Cache non-secret database settings for the lifetime of this Lambda environment.
_db_config = None

# Required license format: two uppercase letters, hyphen, five digits.
LICENSE_NUMBER_PATTERN = re.compile(r"^[A-Z]{2}-[0-9]{5}$")


class ValidationError(Exception):
    """Raised when one or more registration fields fail validation."""

    def __init__(self, errors):
        self.errors = errors
        super().__init__("Registration request validation failed")


def get_db_config():
    """Read database host, port, name, and username from SSM Parameter Store."""
    global _db_config

    if _db_config is None:
        names = [
            DB_HOST_PARAMETER,
            DB_PORT_PARAMETER,
            DB_NAME_PARAMETER,
            DB_USERNAME_PARAMETER,
        ]

        result = ssm.get_parameters(
            Names=names,
            WithDecryption=False,
        )

        values = {
            item["Name"]: item["Value"]
            for item in result.get("Parameters", [])
        }

        missing = [name for name in names if name not in values]

        if missing:
            raise RuntimeError(
                "Missing database configuration SSM parameter(s): "
                + ", ".join(missing)
            )

        try:
            port = int(values[DB_PORT_PARAMETER])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Database port SSM parameter must be an integer"
            ) from exc

        if not 1 <= port <= 65535:
            raise RuntimeError(
                "Database port SSM parameter is outside the valid range"
            )

        _db_config = {
            "host": values[DB_HOST_PARAMETER],
            "port": port,
            "name": values[DB_NAME_PARAMETER],
            "username": values[DB_USERNAME_PARAMETER],
        }

    return _db_config


def get_bcrypt_work_factor():
    """Read and validate the bcrypt work factor from SSM."""
    result = ssm.get_parameter(
        Name=BCRYPT_WORK_FACTOR_PARAMETER,
        WithDecryption=False,
    )

    raw_value = result["Parameter"]["Value"]

    try:
        rounds = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "SSM bcrypt work factor must be an integer"
        ) from exc

    if not 12 <= rounds <= 15:
        raise RuntimeError(
            "SSM bcrypt work factor must be between 12 and 15"
        )

    return rounds


def response(status_code, body):
    """Return a consistent API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
        },
        "body": json.dumps(body),
    }


def get_db_credentials():
    """Retrieve the RDS password from Secrets Manager."""
    result = secrets_manager.get_secret_value(
        SecretId=DB_SECRET_ARN
    )

    secret_string = result.get("SecretString")

    if not secret_string:
        raise RuntimeError(
            "Database secret has no SecretString"
        )

    credentials = json.loads(secret_string)
    password = credentials.get("password")

    if not isinstance(password, str) or not password:
        raise RuntimeError(
            "Database secret must contain a password"
        )

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
        charset="utf8mb4",
    )


def _required_string(body, field, errors):
    """Return a trimmed required string, recording a field-specific error."""
    value = body.get(field)

    if not isinstance(value, str) or not value.strip():
        errors[field] = (
            f"{field} is required and must be a non-empty string."
        )
        return None

    return value.strip()


def _validate_name(value, field, errors):
    """Validate a first or last name."""
    if value is None:
        return None

    # Permit Unicode letters, spaces, apostrophes, and hyphens.
    if not 1 <= len(value) <= 50:
        errors[field] = (
            f"{field} must be between 1 and 50 characters."
        )
    elif not re.fullmatch(
        r"[^\W\d_]+(?:[ '\u2019-][^\W\d_]+)*",
        value,
        flags=re.UNICODE,
    ):
        errors[field] = (
            f"{field} may contain letters, spaces, apostrophes, "
            "and hyphens only."
        )

    return value


def _validate_phone(value, errors):
    """Validate and normalize a phone number."""
    if value is None:
        return None

    # Allow formatting characters in the submitted phone number.
    normalized = re.sub(r"[\s().-]", "", value)

    # Optional + followed by 8–15 digits; first digit cannot be zero.
    if not re.fullmatch(r"\+?[1-9]\d{7,14}", normalized):
        errors["phone"] = (
            "phone must contain 8–15 digits and may start "
            "with a country-code +."
        )
        return value

    return normalized


def _validate_date_of_birth(value, errors):
    """Validate YYYY-MM-DD format and reject future dates."""
    if value is None:
        return None

    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        errors["date_of_birth"] = (
            "date_of_birth must be a valid date in YYYY-MM-DD format."
        )
        return value

    if parsed.isoformat() != value:
        errors["date_of_birth"] = (
            "date_of_birth must use YYYY-MM-DD format."
        )
    elif parsed > date.today():
        errors["date_of_birth"] = (
            "date_of_birth cannot be in the future."
        )

    return value


def _validate_gender(value, errors):
    """Validate and normalize gender."""
    if value is None:
        return None

    normalized = value.upper()

    allowed = {
        "FEMALE",
        "MALE",
        "OTHER",
        "PREFER_NOT_TO_SAY",
    }

    if normalized not in allowed:
        errors["gender"] = (
            "gender must be FEMALE, MALE, OTHER, "
            "or PREFER_NOT_TO_SAY."
        )
        return value

    return normalized


def _validate_address(value, field, errors):
    """Validate a residential or clinic address."""
    if value is None:
        return None

    if not 5 <= len(value) <= 500:
        errors[field] = (
            f"{field} must be between 5 and 500 characters."
        )

    return value


def validate_registration(body):
    """Validate common and role-specific fields; return normalized values."""
    errors = {}

    common_fields = (
        "email",
        "password",
        "role",
        "first_name",
        "last_name",
    )

    values = {
        field: _required_string(body, field, errors)
        for field in common_fields
    }

    # Normalize and validate role first.
    role = values["role"]

    if role is not None:
        role = role.upper()
        values["role"] = role

        if role not in {"DOCTOR", "PATIENT"}:
            errors["role"] = "role must be DOCTOR or PATIENT."

    # Email validation.
    email = values["email"]

    if email is not None:
        email = email.lower()
        values["email"] = email

        if len(email) > 254 or not re.fullmatch(
            r"[^@\s]+@[^@\s]+\.[^@\s]+",
            email,
        ):
            errors["email"] = (
                "email must be a valid email address "
                "(maximum 254 characters)."
            )

    # Password validation.
    password = values["password"]

    if password is not None:
        if len(password) < 10:
            errors["password"] = (
                "password must be at least 10 characters long."
            )
        elif (
            not re.search(r"[A-Za-z]", password)
            or not re.search(r"\d", password)
        ):
            errors["password"] = (
                "password must contain at least one letter "
                "and one digit."
            )
        elif len(password.encode("utf-8")) > 72:
            errors["password"] = (
                "password must not exceed 72 UTF-8 bytes "
                "(bcrypt limit)."
            )

    _validate_name(
        values["first_name"],
        "first_name",
        errors,
    )

    _validate_name(
        values["last_name"],
        "last_name",
        errors,
    )

    # Fields required for each role.
    role_fields = {
        "PATIENT": (
            "phone",
            "date_of_birth",
            "gender",
            "address",
        ),
        "DOCTOR": (
            "phone",
            "date_of_birth",
            "gender",
            "address",
            "specialization",
            "license_number",
            "years_of_experience",
            "clinic_address",
        ),
    }

    if role in role_fields:
        for field in role_fields[role]:
            # Experience must be a JSON integer, not a string.
            if field == "years_of_experience":
                continue

            values[field] = _required_string(
                body,
                field,
                errors,
            )

        values["phone"] = _validate_phone(
            values.get("phone"),
            errors,
        )

        values["date_of_birth"] = _validate_date_of_birth(
            values.get("date_of_birth"),
            errors,
        )

        values["gender"] = _validate_gender(
            values.get("gender"),
            errors,
        )

        values["address"] = _validate_address(
            values.get("address"),
            "address",
            errors,
        )

        if role == "DOCTOR":
            # Validate specialization.
            specialization = values.get("specialization")

            if (
                specialization is not None
                and not 2 <= len(specialization) <= 100
            ):
                errors["specialization"] = (
                    "specialization must be between 2 and 100 characters."
                )

            # License must be exactly AA-12345.
            # Normalize lowercase input to uppercase first.
            license_number = values.get("license_number")

            if license_number is not None:
                license_number = license_number.upper()
                values["license_number"] = license_number

                if not LICENSE_NUMBER_PATTERN.fullmatch(
                    license_number
                ):
                    errors["license_number"] = (
                        "license_number must contain exactly two "
                        "letters, a hyphen, and five digits "
                        "(example: TS-12345)."
                    )

            # Validate experience as an integer from 0 through 60.
            years = body.get("years_of_experience")

            # bool is a subclass of int in Python, so reject it explicitly.
            if isinstance(years, bool) or not isinstance(years, int):
                errors["years_of_experience"] = (
                    "years_of_experience is required and must be "
                    "a whole number from 0 to 60."
                )
                values["years_of_experience"] = None

            elif not 0 <= years <= 60:
                errors["years_of_experience"] = (
                    "years_of_experience must be between 0 and 60."
                )
                values["years_of_experience"] = years

            else:
                values["years_of_experience"] = years

            values["clinic_address"] = _validate_address(
                values.get("clinic_address"),
                "clinic_address",
                errors,
            )

    if errors:
        raise ValidationError(errors)

    return values


def lambda_handler(event, context):
    """Register a user and the corresponding patient or doctor profile."""
    connection = None

    try:
        body = event.get("body")

        if isinstance(body, str):
            body = json.loads(body)
        elif body is None:
            body = {}

        if not isinstance(body, dict):
            return response(
                400,
                {
                    "message": (
                        "Invalid request body. Expected a JSON object."
                    )
                },
            )

        try:
            data = validate_registration(body)

        except ValidationError as exc:
            return response(
                400,
                {
                    "message": (
                        "Validation failed. Correct the fields "
                        "listed in errors."
                    ),
                    "errors": exc.errors,
                },
            )

        # Hash the password using the configured bcrypt work factor.
        bcrypt_work_factor = get_bcrypt_work_factor()

        password_hash = bcrypt.hashpw(
            data["password"].encode("utf-8"),
            bcrypt.gensalt(rounds=bcrypt_work_factor),
        ).decode("utf-8")

        connection = get_db_connection()

        with connection.cursor() as cursor:
            # Insert common account details.
            cursor.execute(
                """
                INSERT INTO users (email, password_hash, role)
                VALUES (%s, %s, %s)
                """,
                (
                    data["email"],
                    password_hash,
                    data["role"],
                ),
            )

            user_id = cursor.lastrowid

            if data["role"] == "PATIENT":
                # Insert patient-specific profile details.
                cursor.execute(
                    """
                    INSERT INTO patients
                        (
                            user_id,
                            first_name,
                            last_name,
                            phone,
                            date_of_birth,
                            gender,
                            address
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        data["first_name"],
                        data["last_name"],
                        data["phone"],
                        data["date_of_birth"],
                        data["gender"],
                        data["address"],
                    ),
                )

            else:
                # Insert doctor-specific profile details.
                cursor.execute(
                    """
                    INSERT INTO doctors
                        (
                            user_id,
                            first_name,
                            last_name,
                            phone,
                            date_of_birth,
                            gender,
                            address,
                            specialization,
                            license_number,
                            years_of_experience,
                            clinic_address
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        data["first_name"],
                        data["last_name"],
                        data["phone"],
                        data["date_of_birth"],
                        data["gender"],
                        data["address"],
                        data["specialization"],
                        data["license_number"],
                        data["years_of_experience"],
                        data["clinic_address"],
                    ),
                )

        # Commit both inserts together.
        connection.commit()

        logger.info(
            json.dumps(
                {
                    "event": "user_registered",
                    "request_id": getattr(
                        context,
                        "aws_request_id",
                        None,
                    ),
                    "role": data["role"],
                }
            )
        )

        return response(
            201,
            {
                "message": "Registration successful",
                "role": data["role"],
                "user_id": user_id,
            },
        )

    except json.JSONDecodeError:
        return response(
            400,
            {
                "message": "Request body must be valid JSON."
            },
        )

    except pymysql.err.IntegrityError as exc:
        if connection:
            connection.rollback()

        # Error 1062 indicates a duplicate unique value.
        if exc.args and exc.args[0] == 1062:
            return response(
                409,
                {
                    "message": (
                        "A unique value already exists. Check the email "
                        "and, for doctors, license_number."
                    )
                },
            )

        logger.exception(
            json.dumps(
                {
                    "event": "registration_database_integrity_error",
                    "request_id": getattr(
                        context,
                        "aws_request_id",
                        None,
                    ),
                }
            )
        )

        return response(
            500,
            {
                "message": (
                    "Registration failed due to a database constraint."
                )
            },
        )

    except (ClientError, pymysql.MySQLError):
        if connection:
            connection.rollback()

        logger.exception(
            json.dumps(
                {
                    "event": "registration_service_error",
                    "request_id": getattr(
                        context,
                        "aws_request_id",
                        None,
                    ),
                }
            )
        )

        return response(
            500,
            {
                "message": (
                    "Registration failed. Please try again later."
                )
            },
        )

    except Exception:
        if connection:
            connection.rollback()

        logger.exception(
            json.dumps(
                {
                    "event": "registration_failed",
                    "request_id": getattr(
                        context,
                        "aws_request_id",
                        None,
                    ),
                }
            )
        )

        return response(
            500,
            {
                "message": (
                    "Registration failed. Please try again later."
                )
            },
        )

    finally:
        if connection:
            connection.close()