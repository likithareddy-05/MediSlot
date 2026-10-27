import os
import json
import time
import uuid
import hashlib
import secrets
import logging

import boto3
import jwt

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# --------------------------------------------------
# Logging and AWS clients
# --------------------------------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
dynamodb = boto3.resource("dynamodb")

# --------------------------------------------------
# Environment configuration
# --------------------------------------------------

JWT_SIGNING_SECRET_PARAMETER = os.environ[
    "JWT_SIGNING_SECRET_PARAMETER"
]

JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")

REFRESH_TOKENS_TABLE = os.environ["REFRESH_TOKENS_TABLE"]

ACCESS_TOKEN_TTL_PARAMETER = os.environ["ACCESS_TOKEN_TTL_PARAMETER"]
REFRESH_TOKEN_TTL_PARAMETER = os.environ["REFRESH_TOKEN_TTL_PARAMETER"]
ACCESS_TOKEN_TTL = None
REFRESH_TOKEN_TTL = None

REFRESH_TOKEN_INDEX = "RefreshTokenHashIndex"

refresh_tokens_table = dynamodb.Table(REFRESH_TOKENS_TABLE)
dynamodb_client = boto3.client("dynamodb")

_jwt_signing_secret = None


# --------------------------------------------------
# HTTP response helper
# --------------------------------------------------

def get_integer_parameter(name, minimum, maximum):
    """Read and validate a numeric runtime setting from SSM Parameter Store."""
    result = ssm.get_parameter(Name=name, WithDecryption=False)
    raw_value = result["Parameter"]["Value"]
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"SSM parameter {name} must contain an integer") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"SSM parameter {name} is outside its allowed range")
    return value


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
# Read JWT signing secret from SSM
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
# Create a new access JWT
# --------------------------------------------------

def create_access_token(user_id, role, now):
    if role not in ("PATIENT", "DOCTOR"):
        raise ValueError("Invalid user role")

    signing_secret = get_jwt_signing_secret()

    payload = {
        "sub": str(user_id),
        "role": role,
        "iat": now,
        "exp": now + ACCESS_TOKEN_TTL,
        "jti": str(uuid.uuid4()),
        "iss": JWT_ISSUER
    }

    return jwt.encode(
        payload,
        signing_secret,
        algorithm="HS256"
    )


# --------------------------------------------------
# Hash opaque refresh token
# --------------------------------------------------

def hash_refresh_token(token):
    return hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()


# --------------------------------------------------
# Find refresh token by its hash using the GSI
# --------------------------------------------------

def find_refresh_token(token_hash):
    items = []
    last_evaluated_key = None

    while True:
        query_args = {
            "IndexName": REFRESH_TOKEN_INDEX,
            "KeyConditionExpression": Key(
                "token_hash"
            ).eq(token_hash)
        }

        if last_evaluated_key:
            query_args["ExclusiveStartKey"] = (
                last_evaluated_key
            )

        result = refresh_tokens_table.query(**query_args)
        items.extend(result.get("Items", []))

        # A token hash must identify exactly one record.
        if len(items) > 1:
            return None

        last_evaluated_key = result.get(
            "LastEvaluatedKey"
        )

        if not last_evaluated_key:
            break

    if len(items) != 1:
        return None

    return items[0]


# --------------------------------------------------
# Refresh handler
# --------------------------------------------------

def lambda_handler(event, context):
    global ACCESS_TOKEN_TTL, REFRESH_TOKEN_TTL
    try:
        # Load current token lifetimes from SSM for this invocation.
        ACCESS_TOKEN_TTL = get_integer_parameter(
            ACCESS_TOKEN_TTL_PARAMETER, minimum=60, maximum=86400
        )
        REFRESH_TOKEN_TTL = get_integer_parameter(
            REFRESH_TOKEN_TTL_PARAMETER, minimum=300, maximum=2592000
        )

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

        refresh_token = body.get("refresh_token")

        if (
            not isinstance(refresh_token, str)
            or not refresh_token
            or len(refresh_token) > 512
        ):
            return response(
                400,
                {"message": "Refresh token is required"}
            )

        now = int(time.time())
        old_hash = hash_refresh_token(refresh_token)

        # Find the stored record using its hash.
        stored_token = find_refresh_token(old_hash)

        if not stored_token:
            return response(
                401,
                {"message": "Invalid or expired refresh token"}
            )

        user_id = stored_token.get("user_id")
        old_token_id = stored_token.get("token_id")
        role = stored_token.get("role")

        if (
            not isinstance(user_id, str)
            or not user_id
            or not isinstance(old_token_id, str)
            or not old_token_id
            or role not in ("PATIENT", "DOCTOR")
            or stored_token.get("token_hash") != old_hash
        ):
            logger.error(json.dumps({
                "event": "refresh_token_record_invalid",
                "request_id": getattr(
                    context, "aws_request_id", None
                )
            }))

            return response(
                401,
                {"message": "Invalid or expired refresh token"}
            )

        try:
            expires_at = int(
                stored_token.get("expires_at", 0)
            )
        except (TypeError, ValueError):
            return response(
                401,
                {"message": "Invalid or expired refresh token"}
            )

        if expires_at <= now:
            return response(
                401,
                {"message": "Invalid or expired refresh token"}
            )

        # Create the replacement refresh token.
        new_refresh_token = secrets.token_urlsafe(48)
        new_refresh_hash = hash_refresh_token(
            new_refresh_token
        )
        new_token_id = str(uuid.uuid4())
        new_refresh_expiry = now + REFRESH_TOKEN_TTL

        new_access_token = create_access_token(
            user_id,
            role,
            now
        )

        # Atomically invalidate the old refresh token and save
        # the replacement. The condition prevents the old token
        # from being successfully rotated more than once.
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "TableName": REFRESH_TOKENS_TABLE,
                        "Key": {
                            "user_id": {"S": user_id},
                            "token_id": {"S": old_token_id}
                        },
                        "ConditionExpression": (
                            "token_hash = :old_hash "
                            "AND expires_at > :now"
                        ),
                        "ExpressionAttributeValues": {
                            ":old_hash": {"S": old_hash},
                            ":now": {"N": str(now)}
                        }
                    }
                },
                {
                    "Put": {
                        "TableName": REFRESH_TOKENS_TABLE,
                        "Item": {
                            "user_id": {"S": user_id},
                            "token_id": {"S": new_token_id},
                            "token_hash": {
                                "S": new_refresh_hash
                            },
                            "expires_at": {
                                "N": str(new_refresh_expiry)
                            },
                            "created_at": {"N": str(now)},
                            "role": {"S": role}
                        },
                        "ConditionExpression": (
                            "attribute_not_exists(user_id) "
                            "AND attribute_not_exists(token_id)"
                        )
                    }
                }
            ]
        )

        logger.info(json.dumps({
            "event": "refresh_success",
            "request_id": getattr(
                context, "aws_request_id", None
            )
        }))

        return response(
            200,
            {
                "message": "Token refreshed successfully",
                "token_type": "Bearer",
                "access_token": new_access_token,
                "expires_in": ACCESS_TOKEN_TTL,
                "refresh_token": new_refresh_token,
                "refresh_expires_in": REFRESH_TOKEN_TTL
            }
        )

    except json.JSONDecodeError:
        return response(
            400,
            {"message": "Request body must be valid JSON"}
        )

    except ClientError as error:
        error_code = error.response.get(
            "Error", {}
        ).get("Code", "")

        if error_code in (
            "ConditionalCheckFailedException",
            "TransactionCanceledException"
        ):
            logger.info(json.dumps({
                "event": "refresh_transaction_rejected",
                "request_id": getattr(
                    context, "aws_request_id", None
                ),
                "error_code": error_code
            }))

            return response(
                401,
                {"message": "Invalid or expired refresh token"}
            )

        logger.exception(json.dumps({
            "event": "refresh_aws_service_error",
            "request_id": getattr(
                context, "aws_request_id", None
            ),
            "error_code": error_code
        }))

        return response(
            500,
            {"message": "Unable to refresh session"}
        )

    except Exception:
        logger.exception(json.dumps({
            "event": "refresh_failed",
            "request_id": getattr(
                context, "aws_request_id", None
            )
        }))

        return response(
            500,
            {"message": "Unable to refresh session"}
        )