
import os
import json
import time
import fnmatch
import logging

import boto3
import jwt

from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
dynamodb = boto3.resource("dynamodb")

JWT_SIGNING_SECRET_PARAMETER = os.environ["JWT_SIGNING_SECRET_PARAMETER"]
JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")
REVOKED_TOKENS_TABLE = os.environ["REVOKED_TOKENS_TABLE"]

# Optional JSON configuration for role-based route restrictions.
# Example:
# {
#   "GET /doctor/*": "DOCTOR",
#   "POST /doctor/*": "DOCTOR",
#   "GET /patient/*": "PATIENT",
#   "POST /patient/*": "PATIENT"
# }
ROLE_ROUTE_RULES = json.loads(
    os.environ.get("ROLE_ROUTE_RULES", "{}")
)

revoked_tokens_table = dynamodb.Table(REVOKED_TOKENS_TABLE)

_jwt_secret = None


def get_jwt_secret():
    """Retrieve and cache the JWT signing secret from SSM."""
    global _jwt_secret

    if _jwt_secret is None:
        result = ssm.get_parameter(
            Name=JWT_SIGNING_SECRET_PARAMETER,
            WithDecryption=True
        )
        _jwt_secret = result["Parameter"]["Value"]

    return _jwt_secret


def extract_token(event):
    """Support API Gateway REST TOKEN and REQUEST authorizers."""
    token = event.get("authorizationToken")

    if not token:
        headers = event.get("headers") or {}

        authorization = next(
            (
                value
                for key, value in headers.items()
                if key.lower() == "authorization"
            ),
            None
        )

        token = authorization

    if not isinstance(token, str):
        return None

    parts = token.strip().split()

    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]

    # TOKEN authorizers may pass the raw token, depending on configuration.
    if len(parts) == 1:
        return parts[0]

    return None


def decode_access_token(token):
    """Verify JWT signature, issuer, expiry and required claims."""
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

    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise jwt.InvalidTokenError("Invalid subject")

    if claims.get("role") not in ("PATIENT", "DOCTOR"):
        raise jwt.InvalidTokenError("Invalid role")

    if not isinstance(claims.get("jti"), str) or not claims["jti"]:
        raise jwt.InvalidTokenError("Invalid token ID")

    if isinstance(claims.get("exp"), bool):
        raise jwt.InvalidTokenError("Invalid expiry")

    if not isinstance(claims.get("exp"), (int, float)):
        raise jwt.InvalidTokenError("Invalid expiry")

    return claims


def is_token_revoked(jti):
    """Check whether the access token ID exists in the denylist."""
    result = revoked_tokens_table.get_item(
        Key={"jti": jti},
        ConsistentRead=True
    )

    item = result.get("Item")

    if not item:
        return False

    expires_at = item.get("expires_at")

    # Treat malformed revocation records as revoked for safety.
    if isinstance(expires_at, bool) or not isinstance(
        expires_at, (int, float)
    ):
        return True

    # Ignore expired denylist records; JWT expiry is checked separately.
    return expires_at > int(time.time())


def get_method_and_path(event):
    """Extract the HTTP method and resource path from the method ARN."""
    method_arn = event.get("methodArn")

    if isinstance(method_arn, str):
        parts = method_arn.split("/", 2)

        if len(parts) == 3:
            method = parts[1].upper()
            path = "/" + parts[2].split("?", 1)[0]
            return method, path

    # Support HTTP API / REQUEST authorizer event shapes too.
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}

    method = http_context.get("method") or event.get("httpMethod")
    path = event.get("rawPath") or event.get("path")

    if method and path:
        return method.upper(), "/" + path.lstrip("/")

    return None, None


def role_is_allowed(claims, event):
    """
    Apply configured role rules to the requested method and path.
    Routes without a matching rule allow either authenticated role.
    """
    if not ROLE_ROUTE_RULES:
        return True

    method, path = get_method_and_path(event)

    if not method or not path:
        # Fail closed if role rules are configured but route cannot be read.
        return False

    request_route = f"{method} {path}"

    for route_pattern, required_role in ROLE_ROUTE_RULES.items():
        if fnmatch.fnmatchcase(request_route, route_pattern):
            return claims["role"] == required_role

    # No matching role restriction: authenticated users of either role.
    return True


def generate_policy(principal_id, effect, resource, claims=None):
    """Create a policy response for API Gateway REST Lambda authorizers."""
    response = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource
                }
            ]
        }
    }

    if claims:
        response["context"] = {
            "userId": str(claims["sub"]),
            "role": str(claims["role"]),
            "jti": str(claims["jti"])
        }

    return response


def lambda_handler(event, context):
    """API Gateway Lambda authorizer entry point."""
    token = extract_token(event)
    method_arn = event.get("methodArn")

    if not token:
        logger.warning("Authorization token is missing or malformed")
        raise Exception("Unauthorized")

    if not method_arn:
        logger.error("methodArn is missing; configure a REST API authorizer")
        raise Exception("Unauthorized")

    try:
        claims = decode_access_token(token)

        if is_token_revoked(claims["jti"]):
            logger.info("Rejected revoked access token")
            raise Exception("Unauthorized")

        if not role_is_allowed(claims, event):
            logger.info(
                "Role denied for user_id=%s role=%s",
                claims["sub"],
                claims["role"]
            )

            return generate_policy(
                claims["sub"],
                "Deny",
                method_arn,
                claims
            )

        return generate_policy(
            claims["sub"],
            "Allow",
            method_arn,
            claims
        )

    except jwt.InvalidTokenError:
        logger.info("Rejected invalid or expired access token")
        raise Exception("Unauthorized")

    except ClientError:
        # Fail closed if SSM or DynamoDB is unavailable or access is denied.
        logger.exception("AWS error during authorization")
        raise Exception("Unauthorized")
