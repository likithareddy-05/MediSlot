import os
import json
import time
import hashlib
import logging

import boto3
import jwt

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
dynamodb = boto3.resource("dynamodb")

JWT_SIGNING_SECRET_PARAMETER = os.environ["JWT_SIGNING_SECRET_PARAMETER"]
JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")

REFRESH_TOKENS_TABLE = os.environ["REFRESH_TOKENS_TABLE"]
REVOKED_TOKENS_TABLE = os.environ["REVOKED_TOKENS_TABLE"]

refresh_tokens_table = dynamodb.Table(REFRESH_TOKENS_TABLE)
revoked_tokens_table = dynamodb.Table(REVOKED_TOKENS_TABLE)

_jwt_secret = None


def response(status_code, message):
    """Return a consistent API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(message)
    }


def get_jwt_secret():
    """Retrieve and cache the JWT signing secret from SSM."""
    global _jwt_secret

    if _jwt_secret is None:
        result = ssm.get_parameter(
            Name=JWT_SIGNING_SECRET_PARAMETER,
            WithDecryption=True
        )
        _jwt_secret = result["Parameter"]["Value"]

        if not _jwt_secret:
            raise ValueError("JWT signing secret is empty")

    return _jwt_secret


def get_bearer_token(event):
    """Extract the access JWT from the Authorization header."""
    headers = event.get("headers") or {}

    if not isinstance(headers, dict):
        return None

    authorization = next(
        (
            value
            for key, value in headers.items()
            if isinstance(key, str)
            and key.lower() == "authorization"
        ),
        None
    )

    if not isinstance(authorization, str):
        return None

    parts = authorization.strip().split()

    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1]


def decode_access_token(token):
    """Verify the JWT signature, issuer, expiry and required claims."""
    try:
        claims = jwt.decode(
            token,
            get_jwt_secret(),
            algorithms=["HS256"],
            issuer=JWT_ISSUER,
            options={
                "require": [
                    "sub",
                    "role",
                    "jti",
                    "iat",
                    "exp",
                    "iss"
                ]
            }
        )

        user_id = claims.get("sub")
        role = claims.get("role")
        token_id = claims.get("jti")
        issued_at = claims.get("iat")
        expires_at = claims.get("exp")

        if not isinstance(user_id, str) or not user_id.strip():
            return None

        if role not in ("PATIENT", "DOCTOR"):
            return None

        if not isinstance(token_id, str) or not token_id.strip():
            return None

        if (
            isinstance(issued_at, bool)
            or not isinstance(issued_at, (int, float))
        ):
            return None

        if (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
        ):
            return None

        if expires_at <= time.time():
            return None

        if issued_at > time.time() + 60:
            return None

        return claims

    except jwt.InvalidTokenError:
        return None


def hash_refresh_token(refresh_token):
    """Hash the opaque refresh token; never store the raw token."""
    return hashlib.sha256(
        refresh_token.encode("utf-8")
    ).hexdigest()


def revoke_refresh_token(refresh_token, user_id):
    """
    Find the refresh-token record through its GSI and delete the
    matching record from the base table.

    The conditional delete prevents deleting a record if its hash
    has changed since it was read.
    """
    token_hash = hash_refresh_token(refresh_token)
    items = []
    last_evaluated_key = None

    while True:
        query_args = {
            "IndexName": "RefreshTokenHashIndex",
            "KeyConditionExpression": Key("token_hash").eq(token_hash)
        }

        if last_evaluated_key:
            query_args["ExclusiveStartKey"] = last_evaluated_key

        result = refresh_tokens_table.query(**query_args)
        items.extend(result.get("Items", []))

        last_evaluated_key = result.get("LastEvaluatedKey")

        if not last_evaluated_key:
            break

    # Do not revoke a token if the hash unexpectedly identifies
    # multiple records.
    if len(items) > 1:
        logger.error("Multiple refresh-token records share the same hash")
        raise RuntimeError("Ambiguous refresh-token records")

    if not items:
        # The token may already have been rotated or revoked.
        return

    item = items[0]

    # A valid access token must not be able to revoke another
    # user's refresh token.
    if item.get("user_id") != user_id:
        return

    if (
        not item.get("token_id")
        or item.get("token_hash") != token_hash
    ):
        return

    try:
        refresh_tokens_table.delete_item(
            Key={
                "user_id": item["user_id"],
                "token_id": item["token_id"]
            },
            ConditionExpression=(
                "token_hash = :expected_hash "
                "AND expires_at > :now"
            ),
            ExpressionAttributeValues={
                ":expected_hash": token_hash,
                ":now": int(time.time())
            }
        )

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            # The record expired, was already removed, or changed
            # after the query. Do not delete a different record.
            logger.info("Refresh token was already expired or revoked")
            return

        raise


def lambda_handler(event, context):
    """API Gateway Lambda handler for logout."""
    try:
        access_token = get_bearer_token(event)

        if not access_token:
            return response(
                401,
                {"message": "Missing or invalid Authorization header"}
            )

        claims = decode_access_token(access_token)

        if not claims:
            return response(
                401,
                {"message": "Invalid or expired access token"}
            )

        body_text = event.get("body")

        if not body_text:
            return response(
                400,
                {"message": "Request body is required"}
            )

        try:
            body = (
                json.loads(body_text)
                if isinstance(body_text, str)
                else body_text
            )
        except (json.JSONDecodeError, TypeError):
            return response(
                400,
                {"message": "Request body must be valid JSON"}
            )

        if not isinstance(body, dict):
            return response(
                400,
                {"message": "Request body must be a JSON object"}
            )

        refresh_token = body.get("refresh_token")

        if (
            not isinstance(refresh_token, str)
            or not refresh_token.strip()
            or len(refresh_token) > 4096
        ):
            return response(
                400,
                {"message": "A valid refresh_token is required"}
            )

        user_id = claims["sub"]
        token_id = claims["jti"]
        token_expiry = int(claims["exp"])

        # Revoke the current access token. The Lambda authorizer
        # must check RevokedTokensTable for every protected request.
        #
        # A conditional put makes repeated logout attempts for the
        # same jti safe without overwriting an existing record.
        try:
            revoked_tokens_table.put_item(
                Item={
                    "jti": token_id,
                    "expires_at": token_expiry
                },
                ConditionExpression="attribute_not_exists(jti)"
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")

            if error_code != "ConditionalCheckFailedException":
                raise

            # This access token has already been revoked.

        # Revoke the supplied refresh token belonging to this user.
        revoke_refresh_token(refresh_token, user_id)

        logger.info("Logout completed for user_id=%s", user_id)

        return response(
            200,
            {"message": "Logout successful"}
        )

    except ClientError:
        logger.exception("AWS error while processing logout")
        return response(
            500,
            {"message": "Unable to complete logout. Please try again."}
        )

    except Exception:
        logger.exception("Unexpected error while processing logout")
        return response(
            500,
            {"message": "An unexpected error occurred"}
        )
