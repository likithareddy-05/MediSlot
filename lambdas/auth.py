
import os
import json
import time
import uuid
import hashlib
import secrets
import logging

import boto3
import bcrypt
import jwt
import pymysql

from botocore.exceptions import ClientError

# --------------------------------------------------
# Logging
# --------------------------------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------
# AWS clients
# --------------------------------------------------

ssm = boto3.client("ssm")
secretsmanager = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")

# --------------------------------------------------
# Database configuration
# --------------------------------------------------

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ["DB_NAME"]
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]

# --------------------------------------------------
# Authentication configuration
# --------------------------------------------------

JWT_SIGNING_SECRET_PARAMETER = os.environ[
    "JWT_SIGNING_SECRET_PARAMETER"
]

JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")

ACCESS_TOKEN_TTL = 15 * 60
REFRESH_TOKEN_TTL = 7 * 24 * 60 * 60

REFRESH_TOKENS_TABLE = os.environ["REFRESH_TOKENS_TABLE"]

refresh_tokens_table = dynamodb.Table(REFRESH_TOKENS_TABLE)

# Cache secrets in the warm Lambda execution environment.
_db_credentials = None
_jwt_signing_secret = None


# --------------------------------------------------
# HTTP response helper
# --------------------------------------------------

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache"
        },
        "body": json.dumps(body)
    }


# --------------------------------------------------
# Get RDS credentials from Secrets Manager
# --------------------------------------------------

def get_db_credentials():
    global _db_credentials

    if _db_credentials is None:
        result = secretsmanager.get_secret_value(
            SecretId=DB_SECRET_ARN
        )

        secret_string = result.get("SecretString")

        if not secret_string:
            raise RuntimeError(
                "RDS credentials secret has no SecretString"
            )

        credentials = json.loads(secret_string)

        username = credentials.get("username")
        password = credentials.get("password")

        if not username or password is None:
            raise RuntimeError(
                "RDS credentials secret is missing username or password"
            )

        _db_credentials = {
            "username": username,
            "password": password
        }

    return _db_credentials


# --------------------------------------------------
# Get JWT signing secret from SSM Parameter Store
# --------------------------------------------------

def get_jwt_signing_secret():
    global _jwt_signing_secret

    if _jwt_signing_secret is None:
        result = ssm.get_parameter(
            Name=JWT_SIGNING_SECRET_PARAMETER,
            WithDecryption=True
        )

        secret = result["Parameter"]["Value"]

        if not secret:
            raise RuntimeError("JWT signing secret is empty")

        _jwt_signing_secret = secret

    return _jwt_signing_secret


# --------------------------------------------------
# Connect to RDS MySQL
# --------------------------------------------------

def get_db_connection():
    credentials = get_db_credentials()

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=credentials["username"],
        password=credentials["password"],
        database=DB_NAME,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        charset="utf8mb4"
    )


# --------------------------------------------------
# Login handler
# --------------------------------------------------

def lambda_handler(event, context):
    connection = None

    try:
        # Parse the incoming request body.
        body = event.get("body")

        if isinstance(body, str):
            body = json.loads(body)
        elif body is None:
            body = {}

        if not isinstance(body, dict):
            return response(
                400,
                {"message": "Invalid request body"}
            )

        email = body.get("email")
        password = body.get("password")

        if (
            not isinstance(email, str)
            or not isinstance(password, str)
        ):
            return response(
                400,
                {"message": "Email and password are required"}
            )

        email = email.strip().lower()

        if (
            not email
            or len(email) > 254
            or not password
        ):
            return response(
                400,
                {"message": "Invalid email or password"}
            )

        # bcrypt supports a maximum of 72 password bytes.
        if len(password.encode("utf-8")) > 72:
            return response(
                401,
                {"message": "Invalid email or password"}
            )

        # Look up the user in RDS.
        connection = get_db_connection()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    email,
                    password_hash,
                    role,
                    account_status
                FROM users
                WHERE email = %s
                LIMIT 1
                """,
                (email,)
            )

            user = cursor.fetchone()

        connection.close()
        connection = None

        if not user:
            return response(
                401,
                {"message": "Invalid email or password"}
            )

        if user["account_status"] != "ACTIVE":
            return response(
                403,
                {"message": "Account is not active"}
            )

        # Verify the submitted password against the bcrypt hash.
        stored_hash = user["password_hash"].encode("utf-8")

        if not bcrypt.checkpw(
            password.encode("utf-8"),
            stored_hash
        ):
            return response(
                401,
                {"message": "Invalid email or password"}
            )

        # Generate the access JWT.
        signing_secret = get_jwt_signing_secret()

        now = int(time.time())
        access_token_id = str(uuid.uuid4())

        access_payload = {
            "sub": str(user["user_id"]),
            "role": user["role"],
            "iat": now,
            "exp": now + ACCESS_TOKEN_TTL,
            "jti": access_token_id,
            "iss": JWT_ISSUER
        }

        access_token = jwt.encode(
            access_payload,
            signing_secret,
            algorithm="HS256"
        )

        # Generate a cryptographically secure opaque refresh token.
        refresh_token = secrets.token_urlsafe(48)

        # Store only its SHA-256 hash in DynamoDB.
        refresh_token_hash = hashlib.sha256(
            refresh_token.encode("utf-8")
        ).hexdigest()

        refresh_token_id = str(uuid.uuid4())
        refresh_expires_at = now + REFRESH_TOKEN_TTL

        # Store the user's role along with the refresh token.
        # refresh.py needs this role when issuing a new access JWT.
        refresh_tokens_table.put_item(
            Item={
                "user_id": str(user["user_id"]),
                "token_id": refresh_token_id,
                "token_hash": refresh_token_hash,
                "role": user["role"],
                "expires_at": refresh_expires_at,
                "created_at": now
            },
            ConditionExpression=(
                "attribute_not_exists(user_id) "
                "AND attribute_not_exists(token_id)"
            )
        )

        logger.info(json.dumps({
            "event": "login_success",
            "request_id": getattr(
                context,
                "aws_request_id",
                None
            ),
            "role": user["role"]
        }))

        return response(
            200,
            {
                "message": "Login successful",
                "token_type": "Bearer",
                "access_token": access_token,
                "expires_in": ACCESS_TOKEN_TTL,
                "refresh_token": refresh_token,
                "refresh_expires_in": REFRESH_TOKEN_TTL,
                "user": {
                    "user_id": str(user["user_id"]),
                    "email": user["email"],
                    "role": user["role"]
                }
            }
        )

    except json.JSONDecodeError:
        return response(
            400,
            {"message": "Request body must be valid JSON"}
        )

    except ClientError:
        logger.exception(json.dumps({
            "event": "login_aws_service_error",
            "request_id": getattr(
                context,
                "aws_request_id",
                None
            )
        }))

        return response(
            500,
            {"message": "Unable to complete login"}
        )

    except Exception:
        logger.exception(json.dumps({
            "event": "login_failed",
            "request_id": getattr(
                context,
                "aws_request_id",
                None
            )
        }))

        return response(
            500,
            {"message": "Unable to complete login"}
        )

    finally:
        if connection:
            connection.close()
