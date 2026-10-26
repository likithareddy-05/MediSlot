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

JWT_SIGNING_SECRET_PARAMETER = os.environ[
    "JWT_SIGNING_SECRET_PARAMETER"
]
JWT_ISSUER = os.environ.get("JWT_ISSUER", "medislot")
REVOKED_TOKENS_TABLE = os.environ["REVOKED_TOKENS_TABLE"]

# Values can be a single role or a list of allowed roles.
# Example:
# {
#   "GET /doctor/*": "DOCTOR",
#   "POST /doctor/*": "DOCTOR",
#   "GET /patient/*": "PATIENT",
#   "POST /patient/*": "PATIENT",
#   "POST /auth/logout": ["DOCTOR", "PATIENT"]
# }
ROLE_ROUTE_RULES = json.loads(
    os.environ.get("ROLE_ROUTE_RULES", "{}")
)

VALID_ROLES = {"DOCTOR", "PATIENT"}

if not isinstance(ROLE_ROUTE_RULES, dict):
    raise ValueError("ROLE_ROUTE_RULES must be a JSON object")

for route_pattern, required_roles in ROLE_ROUTE_RULES.items():
    if not isinstance(route_pattern, str) or not route_pattern.strip():
        raise ValueError("Route patterns must be non-empty strings")

    if isinstance(required_roles, str):
        required_roles = [required_roles]

    if (
        not isinstance(required_roles, list)
        or not required_roles
        or any(
            not isinstance(role, str) or role not in VALID_ROLES
            for role in required_roles
        )
    ):
        raise ValueError(
            "Each route must map to DOCTOR, PATIENT, "
            "or a non-empty list of those roles"
        )

    ROLE_ROUTE_RULES[route_pattern] = required_roles

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

        secret = result["Parameter"]["Value"]

        if not secret:
            raise RuntimeError("JWT signing secret is empty")

        _jwt_secret = secret

    return _jwt_secret


def extract_token(event):
    """Extract the bearer token from a TOKEN or REQUEST event."""
    token = event.get("authorizationToken")

    if not token:
        headers = event.get("headers") or {}

        if isinstance(headers, dict):
            token = next(
                (
                    value
                    for key, value in headers.items()
                    if isinstance(key, str)
                    and key.lower() == "authorization"
                ),
                None
            )

    if not isinstance(token, str):
        return None

    parts = token.strip().split()

    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]

    # REST TOKEN authorizers may be configured to receive a raw token.
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
            "require": ["sub", "role", "jti", "iat", "exp", "iss"]
        }
    )

    user_id = claims.get("sub")
    role = claims.get("role")
    token_id = claims.get("jti")
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    now = time.time()

    if not isinstance(user_id, str) or not user_id.strip():
        raise jwt.InvalidTokenError("Invalid subject")

    if role not in VALID_ROLES:
        raise jwt.InvalidTokenError("Invalid role")

    if not isinstance(token_id, str) or not token_id.strip():
        raise jwt.InvalidTokenError("Invalid token ID")

    if (
        isinstance(issued_at, bool)
        or not isinstance(issued_at, (int, float))
    ):
        raise jwt.InvalidTokenError("Invalid issued-at time")

    if (
        isinstance(expires_at, bool)
        or not isinstance(expires_at, (int, float))
    ):
        raise jwt.InvalidTokenError("Invalid expiry")

    if issued_at > now + 60:
        raise jwt.InvalidTokenError("Issued-at time is in the future")

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

    # Fail closed for malformed revocation records.
    if (
        isinstance(expires_at, bool)
        or not isinstance(expires_at, (int, float))
    ):
        return True

    return expires_at > int(time.time())


def get_method_and_path(event):
    """Extract HTTP method and resource path from the authorizer event."""
    method_arn = event.get("methodArn")

    if isinstance(method_arn, str):
        arn_parts = method_arn.split("/", 3)

        if len(arn_parts) >= 3:
            method = arn_parts[2].upper()
            path = (
                "/" + arn_parts[3]
                if len(arn_parts) == 4
                else "/"
            )

            if method and method != "$DEFAULT":
                return method, path.split("?", 1)[0]

    # Fallback for REQUEST authorizer event shapes.
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}

    method = http_context.get("method") or event.get("httpMethod")
    path = (
        event.get("rawPath")
        or event.get("path")
        or event.get("resource")
    )

    if isinstance(method, str) and isinstance(path, str):
        path = "/" + path.lstrip("/").split("?", 1)[0]
        return method.upper(), path

    return None, None


def role_is_allowed(claims, event):
    """
    Enforce configured route-to-role rules.

    When rules are configured, unmatched routes are denied.
    A route may explicitly permit both PATIENT and DOCTOR.
    """
    if not ROLE_ROUTE_RULES:
        # No role restrictions configured.
        return True

    method, path = get_method_and_path(event)

    if not method or not path:
        return False

    request_route = f"{method} {path}"
    matched_rules = [
        (pattern, roles)
        for pattern, roles in ROLE_ROUTE_RULES.items()
        if fnmatch.fnmatchcase(request_route, pattern)
    ]

    if not matched_rules:
        return False

    # Prefer the most specific matching pattern. If multiple patterns
    # have equal specificity, require every matching rule to permit
    # the role, avoiding an overlapping broad rule granting access.
    max_specificity = max(
        len(pattern.replace("*", "").replace("?", ""))
        for pattern, _ in matched_rules
    )

    most_specific_rules = [
        roles
        for pattern, roles in matched_rules
        if len(pattern.replace("*", "").replace("?", ""))
        == max_specificity
    ]

    return all(
        claims["role"] in roles
        for roles in most_specific_rules
    )


def generate_policy(principal_id, effect, resource, claims=None):
    """Create an API Gateway REST Lambda authorizer policy."""
    policy = {
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
        policy["context"] = {
            "userId": str(claims["sub"]),
            "role": str(claims["role"]),
            "jti": str(claims["jti"])
        }

    return policy


def lambda_handler(event, context):
    """API Gateway TOKEN authorizer entry point."""
    request_id = getattr(context, "aws_request_id", None)

    token = extract_token(event)
    method_arn = event.get("methodArn")

    if not token:
        logger.warning(json.dumps({
            "event": "authorization_token_missing_or_malformed",
            "request_id": request_id
        }))
        raise Exception("Unauthorized")

    if not isinstance(method_arn, str) or not method_arn:
        logger.error(json.dumps({
            "event": "authorizer_method_arn_missing",
            "request_id": request_id
        }))
        raise Exception("Unauthorized")

    try:
        claims = decode_access_token(token)

        if is_token_revoked(claims["jti"]):
            logger.info(json.dumps({
                "event": "revoked_access_token_rejected",
                "request_id": request_id,
                "user_id": claims["sub"]
            }))
            raise Exception("Unauthorized")

        if not role_is_allowed(claims, event):
            logger.info(json.dumps({
                "event": "role_or_route_access_denied",
                "request_id": request_id,
                "user_id": claims["sub"],
                "role": claims["role"]
            }))

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
        logger.info(json.dumps({
            "event": "invalid_or_expired_access_token_rejected",
            "request_id": request_id
        }))
        raise Exception("Unauthorized")

    except ClientError:
        # Fail closed if SSM or DynamoDB is unavailable or access is denied.
        logger.exception(json.dumps({
            "event": "aws_error_during_authorization",
            "request_id": request_id
        }))
        raise Exception("Unauthorized")
