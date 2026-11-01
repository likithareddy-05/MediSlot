"""Pure timezone helpers shared by the slot materialiser and DST tests."""
from datetime import datetime, timezone


def valid_local_time(local_naive, zone):
    """Resolve a wall time to a real instant; skip gaps and choose fold=0 overlaps."""
    candidate = local_naive.replace(tzinfo=zone, fold=0)
    utc = candidate.astimezone(timezone.utc)
    if utc.astimezone(zone).replace(tzinfo=None) != local_naive:
        return None
    return candidate


def put_if_absent(table, item):
    """Conditional DynamoDB put; return False for an existing deterministic key."""
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(slot_id)")
        return True
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") == "ConditionalCheckFailedException":
            return False
        raise
