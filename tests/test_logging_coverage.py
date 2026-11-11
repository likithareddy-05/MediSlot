"""Every try/except in the Lambdas and the dashboard must write a CloudWatch log line.

A handler that swallows an error silently is invisible in CloudWatch. This test
parses the source and fails, naming file and line, for any `except` block that has
no logging call.
"""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}
LOG_FUNCTIONS = {"_log_event", "log_event"}
LOG_OBJECTS = {"logger", "log", "LOGGER"}


def _is_log_call(node):
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in LOG_FUNCTIONS
    if isinstance(func, ast.Attribute) and func.attr in LOG_METHODS:
        target = func.value
        if isinstance(target, ast.Name):
            return target.id in LOG_OBJECTS
        # app.logger.warning(...) in Flask
        return isinstance(target, ast.Attribute) and target.attr == "logger"
    return False


def _unlogged_handlers(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                if not any(_is_log_call(n) for n in ast.walk(handler)):
                    found.append(handler.lineno)
    return found


class LoggingCoverageTest(unittest.TestCase):
    def test_every_except_block_logs(self):
        files = sorted((ROOT / "src" / "lambdas").glob("*/*.py")) + [ROOT / "dashboard" / "app.py"]
        self.assertGreater(len(files), 10)
        problems = []
        for path in files:
            for line in _unlogged_handlers(path):
                problems.append(f"{path.relative_to(ROOT)}:{line}")
        self.assertEqual(problems, [], "except blocks without a log line: " + ", ".join(problems))


if __name__ == "__main__":
    unittest.main()
