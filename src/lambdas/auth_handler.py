"""Login handler for MediSlot authentication."""
import hashlib
import json
import logging
import os
import secrets
import time
import uuid

import bcrypt
import boto3
import jwt
import pymysql
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
secrets_manager = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")

DB_HOST_PARAMETER = os.environ["DB_HOST_PARAMETER"]
DB_PORT_PARAMETER = os.environ["DB_PORT_PARAMETER"]
DB_NAME_PARAMETER = os.environ["DB_NAME_PARAMETER"]
DB_USERNAME_PARAMETER = os.environ["DB_USERNAME_PARAMETER"]
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
JWT_SIGNING_SECRET_PARAMETER = os.environ["JWT_SIGNING_SECRET_PARAMETER"]
JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")
REFRESH_TOKENS_TABLE = os.environ["REFRESH_TOKENS_TABLE"]
ACCESS_TOKEN_TTL_PARAMETER = os.environ["ACCESS_TOKEN_TTL_PARAMETER"]
REFRESH_TOKEN_TTL_PARAMETER = os.environ["REFRESH_TOKEN_TTL_PARAMETER"]

refresh_tokens_table = dynamodb.Table(REFRESH_TOKENS_TABLE)
_db_config = None
_jwt_secret = None


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
        },
        "body": json.dumps(body, default=str),
    }


def get_integer_parameter(name, minimum, maximum):
    result = ssm.get_parameter(Name=name, WithDecryption=False)
    raw = result["Parameter"]["Value"]
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"SSM parameter {name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"SSM parameter {name} is outside its allowed range")
    return value


def get_db_config():
    global _db_config
    if _db_config is None:
        names = [DB_HOST_PARAMETER, DB_PORT_PARAMETER, DB_NAME_PARAMETER, DB_USERNAME_PARAMETER]
        result = ssm.get_parameters(Names=names, WithDecryption=False)
        values = {item["Name"]: item["Value"] for item in result.get("Parameters", [])}
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


def get_db_password():
    result = secrets_manager.get_secret_value(SecretId=DB_SECRET_ARN)
    secret_string = result.get("SecretString")
    if not secret_string:
        raise RuntimeError("Database secret has no SecretString")
    secret = json.loads(secret_string)
    password = secret.get("password")
    if not isinstance(password, str) or not password:
        raise RuntimeError("Database secret must contain a password")
    return password


def get_db_connection():
    config = get_db_config()
    return pymysql.connect(
        host=config["host"], port=config["port"], user=config["username"],
        password=get_db_password(), database=config["name"], connect_timeout=5,
        read_timeout=10, write_timeout=10, cursorclass=pymysql.cursors.DictCursor,
        autocommit=True, charset="utf8mb4",
    )


def get_jwt_secret():
    global _jwt_secret
    if _jwt_secret is None:
        result = ssm.get_parameter(Name=JWT_SIGNING_SECRET_PARAMETER, WithDecryption=True)
        _jwt_secret = result["Parameter"]["Value"]
        if not _jwt_secret:
            raise RuntimeError("JWT signing secret is empty")
    return _jwt_secret


def hash_refresh_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_access_token(user_id, role, now, ttl):
    claims = {
        "sub": str(user_id),
        "role": role,
        "iat": now,
        "exp": now + ttl,
        "jti": str(uuid.uuid4()),
        "iss": JWT_ISSUER,
    }
    return jwt.encode(claims, get_jwt_secret(), algorithm="HS256")


def login_handler(event, context):
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
        if not isinstance(email, str) or not email.strip():
            return response(400, {"message": "email is required"})
        if not isinstance(password, str) or not password:
            return response(400, {"message": "password is required"})
        email = email.strip().lower()
        if len(email) > 254 or len(password.encode("utf-8")) > 72:
            return response(400, {"message": "Invalid email or password"})

        access_ttl = get_integer_parameter(ACCESS_TOKEN_TTL_PARAMETER, 60, 86400)
        refresh_ttl = get_integer_parameter(REFRESH_TOKEN_TTL_PARAMETER, 300, 2592000)

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, email, password_hash, role, account_status "
                "FROM users WHERE email = %s LIMIT 1",
                (email,),
            )
            user = cursor.fetchone()

        # Use the same response for unknown emails and incorrect passwords.
        if not user:
            return response(401, {"message": "Invalid email or password"})
        stored_hash = user.get("password_hash")
        if not isinstance(stored_hash, str) or not stored_hash:
            logger.error("User record has no valid password hash")
            return response(500, {"message": "Unable to sign in"})
        try:
            password_matches = bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
        except (ValueError, TypeError):
            password_matches = False
        if not password_matches:
            return response(401, {"message": "Invalid email or password"})
        if user.get("account_status") != "ACTIVE":
            return response(403, {"message": "This account is not active"})

        role = user.get("role")
        if role not in {"PATIENT", "DOCTOR"}:
            logger.error("User record contains an unsupported role")
            return response(403, {"message": "This account cannot sign in"})

        now = int(time.time())
        user_id = str(user["user_id"])
        access_token = create_access_token(user_id, role, now, access_ttl)

        # Opaque refresh token is created at login and remains unchanged
        # during refresh; only its SHA-256 hash is persisted.
        refresh_token = secrets.token_urlsafe(48)
        token_id = str(uuid.uuid4())
        expires_at = now + refresh_ttl
        refresh_tokens_table.put_item(
            Item={
                "user_id": user_id,
                "token_id": token_id,
                "token_hash": hash_refresh_token(refresh_token),
                "role": role,
                "expires_at": expires_at,
                "created_at": now,
            },
            ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(token_id)",
        )

        logger.info(json.dumps({
            "event": "login_success",
            "request_id": getattr(context, "aws_request_id", None),
            "role": role,
        }))
        return response(200, {
            "message": "Login successful",
            "user": {"user_id": user_id, "email": user["email"], "role": role},
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": access_ttl,
            "refresh_token": refresh_token,
            "refresh_expires_in": refresh_ttl,
        })
    except json.JSONDecodeError:
        return response(400, {"message": "Request body must be valid JSON"})
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        logger.exception(json.dumps({
            "event": "login_aws_service_error",
            "request_id": getattr(context, "aws_request_id", None),
            "error_code": code,
        }))
        return response(500, {"message": "Unable to sign in"})
    except pymysql.MySQLError:
        logger.exception(json.dumps({
            "event": "login_database_error",
            "request_id": getattr(context, "aws_request_id", None),
        }))
        return response(500, {"message": "Unable to sign in"})
    except Exception:
        logger.exception(json.dumps({
            "event": "login_failed",
            "request_id": getattr(context, "aws_request_id", None),
        }))
        return response(500, {"message": "Unable to sign in"})
    finally:
        if connection:
            connection.close()

# One deployed authentication Lambda dispatches these API Gateway routes.
from register import lambda_handler as register_handler
from refresh import lambda_handler as refresh_handler
from logout import lambda_handler as logout_handler

ROUTES = {
    ("POST", "/auth/register"): register_handler,
    ("POST", "/auth/login"): login_handler,
    ("POST", "/auth/refresh"): refresh_handler,
    ("POST", "/auth/logout"): logout_handler,
}

def lambda_handler(event, context):
    """Dispatch an API Gateway REST proxy event to the matching auth action."""
    method = (event.get("httpMethod") or "").upper()
    resource = event.get("resource") or event.get("path") or ""
    resource = "/" + resource.lstrip("/")
    handler = ROUTES.get((method, resource))
    if handler is None:
        return {
            "statusCode": 404,
            "headers": {
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
            },
            "body": json.dumps({"message": "Route not found"}),
        }
    return handler(event, context)
