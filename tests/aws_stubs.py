"""Stand-ins for boto3 / botocore so the Lambda modules can be imported in CI.

boto3 is built into the AWS Lambda runtime, so it is deliberately not listed in
src/lambdas/requirements.txt (that file also builds the Lambda zips). When it is
not installed, the tests use these stubs; when it is installed they do nothing.
"""
import sys
import types
from unittest import mock


def install_if_missing():
    try:
        import boto3  # noqa: F401
        import botocore  # noqa: F401
        return
    except ImportError:
        pass

    class Key:
        def __init__(self, *args, **kwargs): pass
        def eq(self, *args): return self
        def __and__(self, other): return self

    class ClientError(Exception):
        def __init__(self, error_response=None, operation_name=""):
            super().__init__(str(error_response))
            self.response = error_response or {}

    def module(name, **attrs):
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        sys.modules[name] = mod
        return mod

    boto3 = module("boto3", client=mock.MagicMock(), resource=mock.MagicMock())
    boto3.dynamodb = module("boto3.dynamodb")
    boto3.dynamodb.conditions = module("boto3.dynamodb.conditions", Key=Key)
    botocore = module("botocore")
    botocore.config = module("botocore.config", Config=mock.MagicMock)
    botocore.exceptions = module("botocore.exceptions", ClientError=ClientError)


install_if_missing()
