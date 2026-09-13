#!/usr/bin/env python3
"""Exercise the shell check's process contract without PostgreSQL or Cargo builds."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(os.environ.get(
    "APALIS_ISOLATION_CHECK_SCRIPT",
    str(Path(__file__).with_name("check-kerberos-isolation.sh")),
)).resolve()

CHILD = r'''#!PYTHON
import json, os, pathlib, sys
d = pathlib.Path(os.environ["CHECK_FIXTURE"])
settings = json.loads((d / "settings.json").read_text())
tool = pathlib.Path(sys.argv[0]).name
with (d / "calls.jsonl").open("a") as log:
    log.write(json.dumps({"tool": tool, "args": sys.argv[1:], "cwd": os.getcwd(), "database": os.getenv("DATABASE_URL"),
        "gss": os.getenv("PGGSSENCMODE"), "required": os.getenv("APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE")}) + "\n")
if tool == "psql":
    if settings.get("control_trace", True):
        pathlib.Path(os.environ["KRB5_TRACE"]).touch()
    sys.exit(settings.get("control_exit", 0))
listing = "--list" in sys.argv
prefix = "list" if listing else "run"
if settings.get(prefix + "_trace", False):
    pathlib.Path(os.environ["KRB5_TRACE"]).touch()
print(settings[prefix + "_output"], end="")
sys.exit(settings.get(prefix + "_exit", 0))
'''.replace("#!PYTHON", "#!" + sys.executable)


def block(target, names, listing=False):
    header = f"     Running {target} (/fixture/{Path(target).stem})\n"
    if listing:
        return header + "".join(f"{name}: test\n" for name in names) + f"\n{len(names)} tests, 0 benchmarks\n"
    return (header + f"\nrunning {len(names)} tests\n"
            + "".join(f"test {name} ... ok\n" for name in names)
            + f"\ntest result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n")


class WholeSuiteCheck(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="apalis-isolation-contract-")
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        for name in ("cargo", "psql"):
            executable = self.folder / name
            executable.write_text(CHILD)
            executable.chmod(0o700)
        self.targets = [("unittests src/lib.rs", ["shared::case", "unit::case"]),
                        ("tests/example.rs", ["shared::case"]), ("tests/empty.rs", [])]
        self.settings = {
            "list_output": "".join(block(t, n, True) for t, n in self.targets),
            "run_output": "".join(block(t, n) for t, n in self.targets),
        }
        self.env = {"PATH": str(self.folder) + os.pathsep + os.defpath,
                    "CHECK_FIXTURE": str(self.folder), "TMPDIR": str(self.folder),
                    "DATABASE_URL": "postgres://unused.invalid/example",
                    "PGGSSENCMODE": "disable",
                    "APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE": "0"}

    def run_check(self, *args):
        (self.folder / "settings.json").write_text(json.dumps(self.settings))
        return subprocess.run(["/bin/bash", str(SCRIPT), *args], env=self.env,
                              cwd=self.folder, capture_output=True, text=True, timeout=10)

    def calls(self):
        path = self.folder / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def assert_rejected(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("checked 3 tests", result.stdout)

    def test_complete_suite_requires_database_and_preserves_duplicate_names_across_targets(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("checked 3 tests", result.stdout)
        calls = self.calls()
        self.assertEqual([c["tool"] for c in calls], ["psql", "cargo", "cargo"])
        self.assertEqual(calls[0]["gss"], "prefer")
        scope = ["test", "--color", "never", "--locked", "--all-features", "--tests", "--"]
        self.assertEqual(calls[1]["args"], scope + ["--list", "--format", "pretty"])
        self.assertEqual(calls[2]["args"], scope + ["--test-threads=1", "--format", "pretty"])
        for call in calls[1:]:
            self.assertIsNone(call["gss"])
            self.assertEqual(call["required"], "1")
            self.assertEqual(call["database"], self.env["DATABASE_URL"] + "?gssencmode=disable")
            self.assertEqual(call["cwd"], str(SCRIPT.parent.parent))
        self.assertIn("--list", calls[1]["args"])
        self.assertNotIn("--list", calls[2]["args"])
        self.assertEqual(list(self.folder.glob("tmp.*")), [])

    def test_existing_uri_parameters_are_preserved(self):
        self.env["DATABASE_URL"] += "?sslmode=require&application_name=contract"
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls()[-1]["database"], self.env["DATABASE_URL"] + "&gssencmode=disable")

    def test_expected_panic_tests_keep_their_discovered_identity(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test unit::case ... ok", "test unit::case - should panic ... ok")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_success_line_split_by_uncaptured_output_is_still_counted(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test unit::case ... ok\n", "test unit::case ... background diagnostic\nok\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("checked 3 tests", result.stdout)

    def test_multiline_diagnostics_that_resemble_report_lines_are_still_counted(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test unit::case ... ok\n",
            "test unit::case ... background diagnostic\nrunning cleanup hook\ntest harness note\nok\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("checked 3 tests", result.stdout)

    def test_a_second_test_line_while_a_verdict_is_pending_is_rejected(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test shared::case ... ok\ntest unit::case ... ok\n",
            "test shared::case ... background diagnostic\ntest unit::case ... ok\nok\n")
        self.assert_rejected(self.run_check())

    def test_a_split_success_line_without_its_verdict_is_rejected(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test unit::case ... ok\n", "test unit::case ... background diagnostic\n")
        self.assert_rejected(self.run_check())

    def test_a_success_line_split_by_status_like_output_is_still_counted(self):
        for noise in ["ignored by the component", "FAILED to open a trace", "FAILED"]:
            with self.subTest(noise=noise):
                self.settings["run_output"] = "".join(block(t, n) for t, n in self.targets).replace(
                    "test unit::case ... ok\n", f"test unit::case ... {noise}\nok\n")
                result = self.run_check()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("checked 3 tests", result.stdout)

    def test_a_failed_test_is_rejected_even_when_later_output_ends_with_ok(self):
        self.settings["run_output"] = self.settings["run_output"].replace(
            "test unit::case ... ok\n", "test unit::case ... FAILED\nlooks ok\n").replace(
            "test result: ok. 2 passed; 0 failed", "test result: FAILED. 1 passed; 1 failed")
        self.assert_rejected(self.run_check())

    def test_discovery_with_an_incorrect_summary_is_rejected(self):
        self.settings["list_output"] = self.settings["list_output"].replace("2 tests,", "3 tests,")
        self.assert_rejected(self.run_check())

    def test_execution_with_an_incorrect_declared_count_is_rejected(self):
        self.settings["run_output"] = self.settings["run_output"].replace("running 2 tests", "running 3 tests")
        self.assert_rejected(self.run_check())

    def test_caller_arguments_are_rejected_before_any_child_runs(self):
        for args in [("--no-run",), ("--", "--list"), ("--", "missing_filter"),
                     ("--lib",), ("--test", "example"), ("--", "--skip", "shared"),
                     ("--", "--ignored"), ("--", "--test-threads=1")]:
            with self.subTest(args=args):
                self.assert_rejected(self.run_check(*args))
                self.assertEqual(self.calls(), [])

    def test_invalid_database_addresses_are_rejected_before_any_child_runs(self):
        for url in [None, "", "host=localhost", "postgres://host/db?gssencmode=prefer"]:
            with self.subTest(url=url):
                self.env.pop("DATABASE_URL", None)
                if url is not None:
                    self.env["DATABASE_URL"] = url
                self.assert_rejected(self.run_check())
                self.assertEqual(self.calls(), [])

    def test_control_error_prevents_cargo_execution(self):
        self.settings["control_exit"] = 7
        self.assert_rejected(self.run_check())
        self.assertEqual([c["tool"] for c in self.calls()], ["psql"])

    def test_missing_control_trace_prevents_cargo_execution(self):
        self.settings["control_trace"] = False
        self.assert_rejected(self.run_check())
        self.assertEqual([c["tool"] for c in self.calls()], ["psql"])

    def test_discovery_error_prevents_execution(self):
        self.settings["list_exit"] = 101
        self.assert_rejected(self.run_check())
        self.assertEqual(len(self.calls()), 2)

    def test_empty_discovery_is_rejected(self):
        self.settings["list_output"] = block("tests/empty.rs", [], True)
        self.assert_rejected(self.run_check())

    def test_execution_error_is_rejected_even_with_complete_success_lines(self):
        self.settings["run_exit"] = 101
        self.assert_rejected(self.run_check())

    def test_empty_execution_is_rejected(self):
        self.settings["run_output"] = ""
        self.assert_rejected(self.run_check())

    def test_missing_target_is_rejected(self):
        self.settings["run_output"] = block(*self.targets[0])
        self.assert_rejected(self.run_check())

    def test_wrong_test_name_is_rejected_even_with_equal_totals(self):
        self.settings["run_output"] = self.settings["run_output"].replace("unit::case", "unit::other")
        self.assert_rejected(self.run_check())

    def test_wrong_target_is_rejected_even_with_equal_test_names(self):
        self.settings["run_output"] = self.settings["run_output"].replace("tests/example.rs", "tests/other.rs")
        self.assert_rejected(self.run_check())

    def test_duplicate_execution_is_rejected(self):
        self.settings["run_output"] += block(*self.targets[0])
        self.assert_rejected(self.run_check())

    def test_unfinished_harness_is_rejected_after_all_success_lines(self):
        self.settings["run_output"] = self.settings["run_output"].rsplit("test result:", 1)[0]
        self.assert_rejected(self.run_check())

    def test_ignored_or_filtered_tests_are_rejected(self):
        for field in ["failed", "ignored", "measured", "filtered out"]:
            with self.subTest(field=field):
                original = "".join(block(t, n) for t, n in self.targets)
                self.settings["run_output"] = original.replace("0 " + field, "1 " + field, 1)
                self.assert_rejected(self.run_check())

    def test_success_lines_without_harness_identity_are_rejected(self):
        self.settings["run_output"] = "test shared::case ... ok\n"
        self.assert_rejected(self.run_check())

    def test_empty_trace_file_from_discovery_or_execution_is_rejected(self):
        for phase in ["list", "run"]:
            with self.subTest(phase=phase):
                self.settings["list_trace"] = phase == "list"
                self.settings["run_trace"] = phase == "run"
                self.assert_rejected(self.run_check())


if __name__ == "__main__":
    unittest.main()
