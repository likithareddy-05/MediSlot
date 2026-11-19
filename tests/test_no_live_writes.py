"""Guard: tests must never create data in the real (deployed) database or API.

Every test runs against in-memory fakes. This fails the build if someone adds a test that
talks to the deployed API, or a CI step that runs one.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
THIS_FILE = Path(__file__).name


class NoLiveWritesTest(unittest.TestCase):
    def test_there_is_no_deployed_api_test_folder(self):
        self.assertFalse((ROOT / "tests" / "integration").exists(),
                         "tests/integration was removed on purpose: such tests create real users and appointments")

    def test_no_test_calls_a_live_service(self):
        pattern = re.compile(r"requests\.(get|post|put|delete|patch)\(|pymysql\.connect\(|boto3\.client\(")
        offenders = []
        for path in (ROOT / "tests").rglob("*.py"):
            if path.name == THIS_FILE:
                continue
            if pattern.search(path.read_text(encoding="utf-8")):
                offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [], "tests must use the in-memory fakes, not live services: " + ", ".join(offenders))

    def test_the_pipeline_runs_no_deployed_api_tests(self):
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
        self.assertNotIn("tests/integration", workflow.replace("never touch the real database", ""))


if __name__ == "__main__":
    unittest.main()
