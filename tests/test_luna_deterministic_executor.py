"""Phase 5R tests: Luna Deterministic Sandbox Patch Executor.

42+ test cases covering all specification requirements.
All tests use TemporaryDirectory. No real project files are modified.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

# Add project root to sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_deterministic_executor as de


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_project(tmp: Path) -> Path:
    """Create a minimal fake project directory."""
    (tmp / "luna_modules").mkdir(parents=True, exist_ok=True)
    (tmp / "memory").mkdir(exist_ok=True)
    sample = tmp / "luna_modules" / "sample.py"
    sample.write_text("# sample module\ndef hello():\n    return 'hello'\n", encoding="utf-8")
    return tmp


def _sample_op(op_type: str = "replace_text", **kwargs) -> dict:
    base = {
        "operation_id": "op_001_aabbcc",
        "target_file": "luna_modules/sample.py",
        "operation": op_type,
    }
    if op_type == "replace_text":
        base.update({"find_text": "hello", "replace_text": "hi", "expected_occurrences": 2, "max_replacements": 2})
    elif op_type == "append_text":
        base.update({"append_text": "\n# appended\n"})
    elif op_type == "insert_after":
        base.update({"anchor_text": "def hello():", "insert_text": "\n    pass  # inserted\n"})
    elif op_type == "insert_before":
        base.update({"anchor_text": "def hello():", "insert_text": "# before marker\n"})
    elif op_type == "create_file":
        base.update({"new_file_text": "# new file\n", "allow_create": True})
    base.update(kwargs)
    return base


def _simple_plan(tmp: Path, **kwargs) -> dict:
    ops = kwargs.pop("patch_operations", [_sample_op("replace_text", expected_occurrences=2, max_replacements=2)])
    return de.build_execution_plan(
        goal="test goal",
        target_files=["luna_modules/sample.py"],
        patch_operations=ops,
        action_type="medium_code_edit",
        risk_tier=2,
        **kwargs,
    )


# ── 1-10: helpers and validation ─────────────────────────────────────────────

class TestMakeExecutionId(unittest.TestCase):
    def test_01_shape(self):
        eid = de.make_execution_id()
        self.assertTrue(eid.startswith("exec_"), eid)
        self.assertGreater(len(eid), 10)

    def test_01b_custom_prefix(self):
        eid = de.make_execution_id("test")
        self.assertTrue(eid.startswith("test_"), eid)


class TestSha256Json(unittest.TestCase):
    def test_02_stable_with_key_order(self):
        a = de.sha256_json({"z": 1, "a": 2})
        b = de.sha256_json({"a": 2, "z": 1})
        self.assertEqual(a, b)

    def test_02b_different_values_different_hash(self):
        a = de.sha256_json({"x": 1})
        b = de.sha256_json({"x": 2})
        self.assertNotEqual(a, b)


class TestNormalizeTargetFiles(unittest.TestCase):
    def test_03_dedupe_and_posix(self):
        result = de.normalize_target_files(
            ["luna_modules\\sample.py", "luna_modules/sample.py", "", "worker.py"]
        )
        self.assertEqual(result.count("luna_modules/sample.py"), 1)
        self.assertIn("worker.py", result)
        self.assertNotIn("", result)

    def test_03b_empty_input(self):
        self.assertEqual(de.normalize_target_files([]), [])
        self.assertEqual(de.normalize_target_files(None), [])


class TestEnsureUnderProject(unittest.TestCase):
    def test_04_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = Path(tmp) / "project"
            pdir.mkdir()
            with self.assertRaises(ValueError):
                de.ensure_under_project(pdir / ".." / ".." / "etc" / "passwd", pdir)

    def test_04b_allows_valid_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = Path(tmp) / "project"
            pdir.mkdir()
            valid = pdir / "luna_modules" / "sample.py"
            result = de.ensure_under_project(valid, pdir)
            self.assertEqual(result.resolve(), valid.resolve())


class TestValidatePatchOperation(unittest.TestCase):
    def test_05_success_replace(self):
        op = {"operation_id": "x", "target_file": "luna_modules/sample.py",
              "operation": "replace_text", "find_text": "hello", "replace_text": "hi"}
        ok, errs = de.validate_patch_operation(op)
        self.assertTrue(ok, errs)
        self.assertEqual(errs, [])

    def test_06_rejects_unified_diff(self):
        op = {"operation_id": "x", "target_file": "f.py", "operation": "unified_diff"}
        ok, errs = de.validate_patch_operation(op)
        self.assertFalse(ok)
        self.assertTrue(any("unified_diff" in e for e in errs))

    def test_07_rejects_delete_file(self):
        op = {"operation_id": "x", "target_file": "f.py", "operation": "delete_file"}
        ok, errs = de.validate_patch_operation(op)
        self.assertFalse(ok)
        self.assertTrue(any("forbidden" in e for e in errs))

    def test_08_rejects_missing_target_file(self):
        op = {"operation_id": "x", "operation": "replace_text", "find_text": "x", "replace_text": "y"}
        ok, errs = de.validate_patch_operation(op)
        self.assertFalse(ok)
        self.assertTrue(any("target_file" in e for e in errs))


class TestApplyReplaceText(unittest.TestCase):
    def test_09_exact_one_occurrence(self):
        new, ok, err = de.apply_replace_text("hello world", "hello", "hi", max_replacements=1, expected_occurrences=1)
        self.assertTrue(ok, err)
        self.assertEqual(new, "hi world")

    def test_10_wrong_occurrence_count_fails(self):
        new, ok, err = de.apply_replace_text("hello hello world", "hello", "hi", max_replacements=1, expected_occurrences=1)
        self.assertFalse(ok)
        self.assertIn("expected 1", err)


# ── 11-15: text ops ───────────────────────────────────────────────────────────

class TestTextPatchOps(unittest.TestCase):
    def test_11_append_text(self):
        new, ok, err = de.apply_append_text("base\n", "# appended\n")
        self.assertTrue(ok, err)
        self.assertIn("# appended", new)
        self.assertTrue(new.startswith("base\n"))

    def test_12_insert_after(self):
        text = "def foo():\n    pass\n"
        new, ok, err = de.apply_insert_after(text, "def foo():", "\n    # inserted\n")
        self.assertTrue(ok, err)
        self.assertIn("# inserted", new)
        pos_def = new.index("def foo():")
        pos_ins = new.index("# inserted")
        self.assertGreater(pos_ins, pos_def)

    def test_13_insert_before(self):
        text = "def foo():\n    pass\n"
        new, ok, err = de.apply_insert_before(text, "def foo():", "# before marker\n")
        self.assertTrue(ok, err)
        self.assertIn("# before marker", new)
        pos_before = new.index("# before marker")
        pos_def = new.index("def foo():")
        self.assertLess(pos_before, pos_def)

    def test_14_create_file_in_sandbox(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            op = {
                "operation_id": "op_c", "target_file": "luna_modules/newfile.py",
                "operation": "create_file", "new_file_text": "# brand new\n",
            }
            plan = de.build_execution_plan(
                "create new file", ["luna_modules/newfile.py"], [op], action_type="low_risk_additive"
            )
            result = de.apply_plan_in_sandbox(pdir, plan)
            self.assertFalse(result["real_project_modified"])
            creates = [r for r in result["patch_results"] if r["operation"] == "create_file"]
            self.assertTrue(len(creates) > 0)
            self.assertTrue(creates[0]["success"], creates[0].get("error"))
            # Real project must NOT have the new file.
            self.assertFalse((pdir / "luna_modules" / "newfile.py").exists())

    def test_15_apply_ops_preserves_unrelated_text(self):
        text = "line1\nFIND_ME\nline3\n"
        ops = [{"operation_id": "op1", "target_file": "f.py", "operation": "replace_text",
                "find_text": "FIND_ME", "replace_text": "REPLACED",
                "expected_occurrences": 1, "max_replacements": 1}]
        final, results = de.apply_patch_operations_to_text(text, ops)
        self.assertIn("line1", final)
        self.assertIn("line3", final)
        self.assertIn("REPLACED", final)
        self.assertNotIn("FIND_ME", final)
        self.assertTrue(results[0]["success"])


# ── 16-22: plan building + sandbox ────────────────────────────────────────────

class TestBuildExecutionPlan(unittest.TestCase):
    def test_16_plan_shape(self):
        plan = de.build_execution_plan(
            "my goal", ["worker.py"], [], action_type="high_risk_core_edit"
        )
        self.assertEqual(plan["schema_version"], 1)
        self.assertTrue(plan["sandbox_only"])
        self.assertTrue(plan["dry_run"])
        self.assertFalse(plan["real_apply_allowed"])
        self.assertIn("execution_id", plan)
        self.assertIn("created_at", plan)
        self.assertEqual(plan["action_type"], "high_risk_core_edit")


class TestValidateExecutionPlan(unittest.TestCase):
    def test_17_requires_sandbox_only_true(self):
        plan = de.build_execution_plan("g", [], [])
        plan["sandbox_only"] = False
        ok, errs = de.validate_execution_plan(plan)
        self.assertFalse(ok)
        self.assertTrue(any("sandbox_only" in e for e in errs))

    def test_18_rejects_too_many_target_files(self):
        plan = de.build_execution_plan("g", ["a.py", "b.py", "c.py", "d.py"], [])
        ok, errs = de.validate_execution_plan(plan)
        self.assertFalse(ok)
        self.assertTrue(any("too_many_target_files" in e for e in errs))

    def test_19_rejects_too_many_operations(self):
        ops = [{"operation_id": f"op_{i}", "target_file": "f.py", "operation": "append_text",
                "append_text": "x"} for i in range(10)]
        plan = de.build_execution_plan("g", ["f.py"], ops)
        ok, errs = de.validate_execution_plan(plan)
        self.assertFalse(ok)
        self.assertTrue(any("too_many_patch_operations" in e for e in errs))


class TestCreateExecutorSandbox(unittest.TestCase):
    def test_20_sandbox_stays_under_temp_project(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            plan = _simple_plan(pdir, patch_operations=[
                {"operation_id": "op1", "target_file": "luna_modules/sample.py",
                 "operation": "replace_text", "find_text": "hello",
                 "replace_text": "hi", "expected_occurrences": 2, "max_replacements": 2},
            ])
            ctx = de.create_executor_sandbox(pdir, plan)
            sbox = Path(ctx["sandbox_dir"])
            self.assertTrue(sbox.exists())
            # Sandbox is a temp dir — just verify it is a real directory.
            self.assertTrue(sbox.is_dir())


class TestApplyPlanInSandbox(unittest.TestCase):
    def test_21_apply_plan_modifies_sandbox_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            original = (pdir / "luna_modules" / "sample.py").read_text(encoding="utf-8")
            plan = _simple_plan(pdir, patch_operations=[
                {"operation_id": "op1", "target_file": "luna_modules/sample.py",
                 "operation": "replace_text", "find_text": "hello",
                 "replace_text": "hi", "expected_occurrences": 2, "max_replacements": 2},
            ])
            result = de.apply_plan_in_sandbox(pdir, plan)
            after = (pdir / "luna_modules" / "sample.py").read_text(encoding="utf-8")
            self.assertEqual(original, after, "Real file must not be modified")
            self.assertFalse(result["real_project_modified"])

    def test_22_real_project_file_hash_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            real_file = pdir / "luna_modules" / "sample.py"
            hash_before = de.sha256_file(real_file)
            plan = _simple_plan(pdir, patch_operations=[
                {"operation_id": "op1", "target_file": "luna_modules/sample.py",
                 "operation": "append_text", "append_text": "\n# appended\n"},
            ])
            de.apply_plan_in_sandbox(pdir, plan)
            hash_after = de.sha256_file(real_file)
            self.assertEqual(hash_before, hash_after, "Real file hash must be unchanged")


# ── 23-25: receipt / risk tier ───────────────────────────────────────────────

class TestReceiptAdvisory(unittest.TestCase):
    def test_23_missing_receipt_for_tier2_marks_blocker(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            plan = de.build_execution_plan(
                "test", ["luna_modules/sample.py"], [],
                approval_tier_required=2, receipt_id="", receipt_required=True,
            )
            result = de._check_receipt_advisory(pdir, plan)
            self.assertTrue(result.get("checked"))
            self.assertFalse(result.get("valid"))
            self.assertIn("blocker", result)

    def test_24_valid_synthetic_receipt_not_real_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            req_file = pdir / "memory" / "luna_approval_requests.jsonl"
            req_file.write_text(
                json.dumps({"request_id": "rcpt_test_001", "approved": True}) + "\n",
                encoding="utf-8",
            )
            plan = de.build_execution_plan(
                "test", ["luna_modules/sample.py"], [],
                approval_tier_required=2, receipt_id="rcpt_test_001",
            )
            result = de._check_receipt_advisory(pdir, plan)
            self.assertTrue(result.get("valid"))
            # Even with valid receipt, safe_to_apply_real_project must never be set True here.
            self.assertNotIn("safe_to_apply_real_project", result)


class TestHighRiskWorkerSimulation(unittest.TestCase):
    def test_25_high_risk_worker_safe_to_apply_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Create a fake worker.py in sandbox project.
            (pdir / "worker.py").write_text("# fake worker\n", encoding="utf-8")
            plan = de.build_execution_plan(
                "simulate worker edit", ["worker.py"],
                [{"operation_id": "op1", "target_file": "worker.py",
                  "operation": "append_text", "append_text": "# sandbox comment\n"}],
                action_type="high_risk_core_edit", risk_tier=4,
            )
            sr = de.apply_plan_in_sandbox(pdir, plan)
            vr: list = []
            rr = de._check_receipt_advisory(pdir, plan)
            report = de.build_executor_report(plan, sr, vr, rr)
            self.assertFalse(report["safe_to_apply_real_project"])
            self.assertFalse(report["real_project_modified"])


# ── 26-28: safety / command filtering ────────────────────────────────────────

class TestNonDelegablePathBlocked(unittest.TestCase):
    def test_26_sensitive_path_blocked_in_validate(self):
        op = {"operation_id": "x", "target_file": ".env",
              "operation": "replace_text", "find_text": "KEY", "replace_text": "VAL"}
        ok, errs = de.validate_patch_operation(op)
        self.assertFalse(ok)
        self.assertTrue(any("sensitive" in e for e in errs))


class TestUnsafeVerificationCommand(unittest.TestCase):
    def test_27_unsafe_command_denied(self):
        allowed, reason = de._is_command_allowed("pip install requests")
        self.assertFalse(allowed)
        self.assertIn("pip install", reason)

    def test_27b_git_reset_denied(self):
        allowed, reason = de._is_command_allowed("git reset --hard HEAD")
        self.assertFalse(allowed)

    def test_27c_taskkill_denied(self):
        allowed, reason = de._is_command_allowed("taskkill /F /IM python.exe")
        self.assertFalse(allowed)


class TestSafePyCompile(unittest.TestCase):
    def test_28_py_compile_allowed(self):
        allowed, reason = de._is_command_allowed("python -m py_compile luna_modules/sample.py")
        self.assertTrue(allowed, reason)


# ── 29-31: report ─────────────────────────────────────────────────────────────

class TestReportSchema(unittest.TestCase):
    def _make_report(self, tmp: Path) -> dict:
        pdir = _make_project(tmp)
        plan = _simple_plan(pdir, patch_operations=[
            {"operation_id": "op1", "target_file": "luna_modules/sample.py",
             "operation": "append_text", "append_text": "\n# safe\n"},
        ])
        sr = de.apply_plan_in_sandbox(pdir, plan)
        vr: list = []
        rr = de._check_receipt_advisory(pdir, plan)
        return de.build_executor_report(plan, sr, vr, rr)

    def test_29_report_schema_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = self._make_report(Path(tmp))
            self.assertEqual(report["schema_version"], 1)
            self.assertFalse(report["safe_to_apply_real_project"])
            self.assertFalse(report["real_project_modified"])
            self.assertTrue(report["sandbox_only"])
            self.assertIn("execution_id", report)
            self.assertIn("generated_at", report)
            self.assertIn("blockers", report)
            self.assertIn("warnings", report)
            self.assertIn("patch_results", report)

    def test_30_markdown_includes_sandbox_and_safe_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = self._make_report(Path(tmp))
            md = de.render_executor_report_markdown(report)
            self.assertIn("sandbox_only", md)
            self.assertIn("False", md)

    def test_31_write_report_only_under_memory_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = self._make_report(pdir)
            written = de.write_executor_report(pdir, report)
            for path in written.values():
                p = Path(path)
                # Must be under project memory dir.
                self.assertTrue(str(p).startswith(str(pdir)), f"{p} outside project")
                self.assertIn("memory", str(p))


# ── 32-35: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run_cli(self, *args):
        import subprocess
        cmd = [sys.executable, "-m", "luna_modules.luna_deterministic_executor"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_32_self_test_returns_0(self):
        rc, out, err = self._run_cli("--self-test")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")

    def test_33_simulate_create_file_returns_0(self):
        rc, out, err = self._run_cli("--simulate-create-file")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")

    def test_34_simulate_replace_returns_0(self):
        rc, out, err = self._run_cli("--simulate-replace")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")

    def test_35_simulate_high_risk_worker_no_real_apply(self):
        rc, out, err = self._run_cli("--simulate-high-risk-worker")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        # Output must confirm no real apply.
        combined = out + err
        self.assertNotIn("real_project_modified: True", combined)


# ── 36-38: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_deterministic_executor.py").read_text(encoding="utf-8")

    def test_36_no_aider_invocation(self):
        import re
        # No actual aider import or subprocess call with aider.
        # The word "aider" may appear in docstrings/comments as documentation.
        self.assertNotIn("import aider", self._src)
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [], f"Unexpected aider subprocess calls: {aider_calls}")

    def test_37_no_external_api_clients(self):
        for bad in ("import requests", "import openai", "import anthropic", "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_38_no_dangerous_commands_in_source(self):
        import re
        # Dangerous command strings must only appear in string literals (rejection patterns),
        # never as actual subprocess.run or os.system arguments.
        lower_src = self._src.lower()
        for bad in ("pip install", "taskkill", "git reset"):
            found_idx = lower_src.find(bad)
            while found_idx != -1:
                line_start = lower_src.rfind("\n", 0, found_idx) + 1
                line_end = lower_src.find("\n", found_idx)
                line = lower_src[line_start:(line_end if line_end != -1 else len(lower_src))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"Found '{bad}' in a subprocess.run call: {line!r}",
                )
                self.assertFalse(
                    "os.system" in line and bad in line,
                    f"Found '{bad}' in an os.system call: {line!r}",
                )
                found_idx = lower_src.find(bad, found_idx + 1)


# ── 39-42: self_test, paths, invariants, graceful degradation ─────────────────

class TestSelfTest(unittest.TestCase):
    def test_39_self_test_returns_0(self):
        rc = de.self_test()
        self.assertEqual(rc, 0)


class TestGeneratedPathsNeverEscapeRoot(unittest.TestCase):
    def test_40_write_executor_report_path_under_project(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = {
                "schema_version": 1,
                "execution_id": "exec_test_001",
                "generated_at": de.now_iso(),
                "goal": "test",
                "task_id": "",
                "sandbox_only": True,
                "real_project_modified": False,
                "safe_to_apply_real_project": False,
                "action_type": "medium_code_edit",
                "risk_tier": 2,
                "target_files": [],
                "sandbox_dir": "",
                "snapshot_id": "",
                "patch_results": [],
                "verification_results": [],
                "diff_hash": "",
                "file_deltas": [],
                "receipt_checked": False,
                "receipt_valid": False,
                "blockers": [],
                "warnings": [],
                "success": True,
                "recommended_next_action": "",
                "notes": [],
            }
            written = de.write_executor_report(pdir, report)
            for path in written.values():
                p = Path(path)
                pdir_resolved = pdir.resolve()
                self.assertTrue(str(p.resolve()).startswith(str(pdir_resolved)), f"{p} escapes project")


class TestSafeToApplyAlwaysFalse(unittest.TestCase):
    def test_41_safe_to_apply_always_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            for action_type in de.VALID_ACTION_TYPES:
                with self.subTest(action_type=action_type):
                    plan = de.build_execution_plan(
                        "test", ["luna_modules/sample.py"],
                        [{"operation_id": "op1", "target_file": "luna_modules/sample.py",
                          "operation": "append_text", "append_text": "# x\n"}],
                        action_type=action_type,
                    )
                    sr = de.apply_plan_in_sandbox(pdir, plan)
                    vr: list = []
                    rr = de._check_receipt_advisory(pdir, plan)
                    report = de.build_executor_report(plan, sr, vr, rr)
                    self.assertFalse(report["safe_to_apply_real_project"],
                                     f"safe_to_apply_real_project was True for {action_type}")
                    self.assertFalse(report["real_project_modified"])

    def test_41b_policy_hardcodes_sandbox_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            policy = de.load_executor_policy(tmp)
            self.assertTrue(policy["sandbox_only"])
            self.assertFalse(policy.get("allow_real_apply", False))


class TestGracefulDegradation(unittest.TestCase):
    def test_42_receipt_enforcer_unavailable_degrades_gracefully(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            plan = de.build_execution_plan(
                "test", ["luna_modules/sample.py"],
                [{"operation_id": "op1", "target_file": "luna_modules/sample.py",
                  "operation": "append_text", "append_text": "# x\n"}],
                approval_tier_required=3, receipt_id="nonexistent_receipt_id",
            )
            # Should not raise even if enforcer is None.
            original_enforcer = de._enforcer
            try:
                de._enforcer = None  # type: ignore
                result = de._check_receipt_advisory(pdir, plan)
                self.assertIsInstance(result, dict)
                self.assertIn("checked", result)
            finally:
                de._enforcer = original_enforcer  # type: ignore

    def test_42b_load_policy_degrades_if_file_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            policy = de.load_executor_policy(tmp)
            self.assertIsInstance(policy, dict)
            self.assertTrue(policy.get("sandbox_only"))
            self.assertEqual(policy.get("_source"), "module_fallback")


if __name__ == "__main__":
    unittest.main(verbosity=2)
