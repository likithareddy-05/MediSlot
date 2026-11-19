"""Business events (EventBridge) and custom metrics (CloudWatch Embedded Metric Format).

Shared by the booking Lambda and the doctor-domain Lambda. Nothing here talks to a
database. Publishing is best effort: a failed event never fails or rolls back a booking
or a cancellation, it is logged and counted in the EventPublishFailures metric.

Metrics are written as EMF log lines, so no CloudWatch API permission or VPC endpoint
is needed: CloudWatch Logs turns the line into metrics automatically.
"""
import json
import logging
import os
import time

logger = logging.getLogger()

EVENT_SOURCE = "medislot.booking"
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "MediSlot")

_client = None


def set_client(client):
    """Tests inject a fake EventBridge client here."""
    global _client
    _client = client


def _events_client():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("events")
    return _client


def emit_metric(name, value=1, unit="Count", **dimensions):
    """Write one CloudWatch metric data point as an EMF log line.

    Environment is always added as a dimension. Extra keyword arguments become
    further dimensions, e.g. emit_metric("CancellationsByTier", 1, Tier="GT_48H").
    """
    dims = {"Environment": os.environ.get("ENVIRONMENT_NAME", "unknown")}
    dims.update({k: str(v) for k, v in dimensions.items()})
    record = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": METRICS_NAMESPACE,
                "Dimensions": [sorted(dims)],
                "Metrics": [{"Name": name, "Unit": unit}],
            }],
        },
        name: value,
        **dims,
    }
    print(json.dumps(record, default=str))


def publish_event(detail_type, detail):
    """Put one event on the MediSlot bus. Returns True when EventBridge accepted it."""
    bus = os.environ.get("EVENT_BUS_NAME")
    if not bus:
        logger.warning(json.dumps({"event": "event_bus_not_configured", "detail_type": detail_type}))
        return False
    try:
        result = _events_client().put_events(Entries=[{
            "Source": EVENT_SOURCE,
            "DetailType": detail_type,
            "Detail": json.dumps(detail, default=str),
            "EventBusName": bus,
        }])
        if int(result.get("FailedEntryCount", 0)):
            raise RuntimeError(json.dumps(result.get("Entries", []), default=str)[:300])
        return True
    except Exception as exc:  # never let an event problem break a booking
        logger.error(json.dumps({"event": "event_publish_failed", "detail_type": detail_type,
                                 "error": str(exc)[:300]}))
        emit_metric("EventPublishFailures", 1, DetailType=detail_type)
        return False
