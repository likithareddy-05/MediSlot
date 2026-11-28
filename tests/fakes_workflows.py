"""Milestone 6 test helpers: a Step Functions interpreter, a fake SNS and the shared test base.

AslRunner executes infrastructure/statemachines/reschedule-saga.asl.json itself (Task, Retry,
Catch with ResultPath, Fail with ErrorPath/CausePath, Succeed), calling the real saga Lambda
handler for every Task state. So the tests check the deployed definition, including that each
compensation state is wired to the right failure, not a hand-written copy of it.

Milestone6Harness wires every Lambda module to the in-memory fakes of fakes_booking.py.
"""
import copy
import json
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("shared", "patient", "workflows", "doctor"):
    sys.path.insert(0, str(ROOT / "src" / "lambdas" / _sub))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for key, value in {"AWS_DEFAULT_REGION": "ap-south-1", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                   "DB_HOST_PARAMETER": "h", "DB_PORT_PARAMETER": "p", "DB_NAME_PARAMETER": "n",
                   "DB_USERNAME_PARAMETER": "u", "DB_SECRET_ARN": "arn", "APPOINTMENTS_TABLE_NAME": "appointments",
                   "ASSETS_BUCKET": "bucket", "IDEMPOTENCY_TABLE_NAME": "idem", "DISPATCH_LOG_TABLE_NAME": "dispatch",
                   "NOTIFICATION_TOPIC_ARN": "arn:aws:sns:ap-south-1:1:medislot-dev-notifications",
                   "RESCHEDULE_STATE_MACHINE_ARN": "arn:aws:states:ap-south-1:1:stateMachine:medislot-dev-reschedule-saga",
                   "RESCHEDULE_FORCE_FAILURE_PARAMETER": "/medislot/dev/reschedule/force-release-failure"}.items():
    os.environ.setdefault(key, value)

import aws_stubs  # noqa: E402,F401
import booking_domain as bd  # noqa: E402
import common  # noqa: E402
import notifications as nt  # noqa: E402
import reschedule_saga as saga  # noqa: E402
import waitlist_domain as wd  # noqa: E402
from fakes_booking import FakeDynamo, FakeRds, FakeTable, appointments_table  # noqa: E402

DEFINITION = json.loads((ROOT / "infrastructure" / "statemachines" / "reschedule-saga.asl.json").read_text())
LAMBDA_ERRORS = ("Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException",
                 "Lambda.TooManyRequestsException")


def _path_get(document, path):
    if path == "$":
        return document
    value = document
    for part in path[2:].split("."):
        value = value[part]
    return value


class AslRunner:
    """A small interpreter for the subset of Amazon States Language the saga uses."""

    def __init__(self, definition, task):
        self.definition, self.task = definition, task

    def _params(self, template, state, context):
        out = {}
        for key, value in template.items():
            if key.endswith(".$"):
                out[key[:-2]] = context[value[3:]] if value.startswith("$$.") else copy.deepcopy(_path_get(state, value))
            elif isinstance(value, dict):
                out[key] = self._params(value, state, context)
            else:
                out[key] = value
        return out

    @staticmethod
    def _matches(rule, error):
        return error in rule["ErrorEquals"] or ("States.ALL" in rule["ErrorEquals"] and not error.startswith("States.Runtime"))

    def run(self, execution_input, name):
        context = {"Execution.Name": name}
        state, current, history = copy.deepcopy(execution_input), self.definition["StartAt"], []
        while True:
            spec = self.definition["States"][current]
            history.append(current)
            if spec["Type"] == "Succeed":
                return {"status": "SUCCEEDED", "output": state, "history": history}
            if spec["Type"] == "Fail":
                error = _path_get(state, spec["ErrorPath"]) if "ErrorPath" in spec else spec.get("Error")
                cause = _path_get(state, spec["CausePath"]) if "CausePath" in spec else spec.get("Cause")
                return {"status": "FAILED", "error": error, "cause": cause, "output": state, "history": history}
            assert spec["Type"] == "Task", spec["Type"]
            payload = self._params(spec["Parameters"], state, context)["Payload"]
            attempts = {}
            while True:
                try:
                    result = {"Payload": self.task(copy.deepcopy(payload))}
                    error = None
                except Exception as exc:  # the Lambda error name is the exception class name
                    error = type(exc).__name__
                    cause = json.dumps({"errorMessage": str(exc), "errorType": error})
                if error is None:
                    break
                rule = next((r for r in spec.get("Retry", []) if self._matches(r, error)), None)
                if rule and attempts.get(id(rule), 0) < rule["MaxAttempts"]:
                    attempts[id(rule)] = attempts.get(id(rule), 0) + 1
                    history.append(current + ":retry")
                    continue
                catch = next((c for c in spec.get("Catch", []) if self._matches(c, error)), None)
                if catch is None:
                    return {"status": "FAILED", "error": error, "cause": cause, "output": state, "history": history}
                state = {**state, catch["ResultPath"][2:]: {"Error": error, "Cause": cause}}
                break
            if error is not None:
                current = catch["Next"]
                continue
            state = _path_get(result, spec["OutputPath"]) if "OutputPath" in spec else result
            if spec.get("End"):
                return {"status": "SUCCEEDED", "output": state, "history": history}
            current = spec["Next"]


class FakeStepFunctions:
    """start_execution runs the saga synchronously through AslRunner; describe_execution reports it."""

    def __init__(self):
        self.runner = AslRunner(DEFINITION, lambda payload: saga.handler(payload, None))
        self.executions = {}

    def start_execution(self, stateMachineArn, name, input):
        result = self.runner.run(json.loads(input), name)
        arn = stateMachineArn.replace(":stateMachine:", ":execution:") + ":" + name
        self.executions[arn] = result
        return {"executionArn": arn}

    def describe_execution(self, executionArn):
        r = self.executions[executionArn]
        return {"status": r["status"], "output": json.dumps(r.get("output")), "error": r.get("error"),
                "cause": r.get("cause")}

    def last(self):
        return list(self.executions.values())[-1]


class FakeSns:
    def __init__(self):
        self.messages, self.fail_next = [], 0
        self.lock = threading.Lock()

    def publish(self, **kwargs):
        with self.lock:
            if self.fail_next:
                self.fail_next -= 1
                raise RuntimeError("sns unavailable")
            self.messages.append(kwargs)
            return {"MessageId": f"m-{len(self.messages)}"}

    @staticmethod
    def attributes(message):
        return {k: v["StringValue"] for k, v in message["MessageAttributes"].items()}

    def of_type(self, event_type):
        """Published messages of one notification type: their attributes plus subject and body."""
        return [{**self.attributes(m), "subject": m["Subject"], "body": m["Message"]} for m in self.messages
                if m["MessageAttributes"]["event_type"]["StringValue"] == event_type]


def make_event(method, path, user_id, body=None, key=None, role="PATIENT"):
    headers = {"Idempotency-Key": key} if key else {}
    return {"httpMethod": method, "path": path, "headers": headers, "queryStringParameters": None,
            "body": json.dumps(body) if body is not None else None,
            "requestContext": {"stage": "dev", "authorizer": {"userId": str(user_id), "role": role}}}


class Milestone6Harness(unittest.TestCase):
    """Every Lambda module of Milestones 5 and 6 wired to one set of in-memory fakes."""

    patients = range(1, 11)
    slots = ()
    config = {}

    def setUp(self):
        self.rds = FakeRds(patients=self.patients, slots=self.slots)
        self.appts = appointments_table()
        self.idem = FakeTable("idempotency_key")
        self.dispatch_log = FakeTable("dispatch_id")
        self.dynamo = FakeDynamo(appointments=self.appts, idem=self.idem, dispatch=self.dispatch_log)
        self.sfn, self.sns = FakeStepFunctions(), FakeSns()
        self.events, self.metrics = [], []
        self.capture_lock = threading.Lock()
        self.settings = dict(self.config)

        def setting(name, default=None, ttl=60):
            return self.settings.get(name, default) if name else default

        os.environ.update({"WAITLIST_HOLD_MINUTES_PARAMETER": "hold", "WAITLIST_MAX_ACTIVE_PARAMETER": "maxwl",
                           "RESCHEDULE_MAX_PARAMETER": "maxres", "RESCHEDULE_CUTOFF_HOURS_PARAMETER": "cutoff",
                           "RESCHEDULE_FORCE_FAILURE_PARAMETER": "force"})
        patches = [mock.patch.object(common, "get_config_value", setting),
                   mock.patch.object(bd, "get_config_value", setting),
                   mock.patch.object(saga, "get_config_value", setting),
                   mock.patch.object(bd, "_sleep", lambda s: None),
                   mock.patch.object(nt, "ddb", self.dynamo)]
        for module in (bd, wd, saga):
            patches += [mock.patch.object(module, "ddb", self.dynamo),
                        mock.patch.object(module, "db_connect", self.rds.connect),
                        mock.patch.object(module, "publish_event", self.capture_event)]
        for module in (bd, wd, saga, nt):
            patches.append(mock.patch.object(module, "emit_metric", self.capture_metric))
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        bd.set_sfn_client(self.sfn)
        nt.set_sns_client(self.sns)
        self.addCleanup(bd.set_sfn_client, None)
        self.addCleanup(nt.set_sns_client, None)

    # ---- capture
    def capture_event(self, detail_type, detail):
        with self.capture_lock:
            self.events.append((detail_type, json.loads(json.dumps(detail, default=str))))
        return True

    def capture_metric(self, name, value=1, unit="Count", **dims):
        with self.capture_lock:
            self.metrics.append((name, value, dims))

    def events_of(self, kind):
        return [d for k, d in self.events if k == kind]

    def metric_total(self, name):
        return sum(v for n, v, _ in self.metrics if n == name)

    # ---- API calls
    def call(self, method, path, user_id, body=None, key=None):
        result = bd.lambda_handler(make_event(method, path, user_id, body, key), mock.Mock(aws_request_id="req"))
        return result["statusCode"], json.loads(result["body"])

    def book(self, patient, slot_id):
        return self.call("POST", "/appointments", patient, {"slot_id": slot_id},
                         key=f"key-{patient}-{slot_id.replace('#', '_')}")

    # ---- state
    def appointments(self, patient=None, status=None):
        found = [i for k, i in self.appts.items.items() if k.isdigit()]
        return [i for i in found if (patient is None or i["patient_id"] == str(patient))
                and (status is None or i["status"] == status)]

    def holds(self):
        return sorted(k for k in self.appts.items if k.startswith("HOLD#"))

    def slot(self, slot_id):
        return self.rds.slots[slot_id]
