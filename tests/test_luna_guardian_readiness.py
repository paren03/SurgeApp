"""Phase 5RS tests: Luna Guardian Enforcement Readiness.

20+ test cases covering advisory-only invariants, action evaluation,
soak cycles, report writing, and CLI.
All tests use TemporaryDirectory. No real project files are modified.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_guardian_readiness as gr


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    (tmp / "luna_modules").mkdir(parents=True, exist_ok=True)
    return tmp


# ── 1-3: helpers and policy ──────────────────────────────────────────────────

class TestMakeReadinessId(unittest.TestCase):
    def test_01_shape(self):
        rid = gr.make_readiness_id()
        self.assertTrue(rid.startswith("grd_"), rid)
        self.assertGreater(len(rid), 8)

    def test_01b_custom_prefix(self):
        rid = gr.make_readiness_id("test")
        self.assertTrue(rid.startswith("test_"), rid)


class TestPolicyDefaults(unittest.TestCase):
    def test_02_policy_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            policy = gr.load_readiness_policy(tmp)
            self.assertTrue(policy["advisory_only"])
            self.assertFalse(policy["guardian_enforcing_live"])
            self.assertTrue(policy["ready_for_live_guardian_enforcement_always_false_in_phase5rs"])
            self.assertIsInstance(policy["required_before_live_enforcement"], list)
            self.assertGreater(len(policy["required_before_live_enforcement"]), 0)

    def test_03_malformed_policy_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = Path(tmp)
            (pdir / "memory").mkdir(exist_ok=True)
            bad_policy = pdir / "memory" / "luna_guardian_readiness_policy.json"
            bad_policy.write_text("not valid json {{}", encoding="utf-8")
            policy = gr.load_readiness_policy(pdir)
            # Must still return valid defaults with hard rules enforced.
            self.assertTrue(policy["advisory_only"])
            self.assertFalse(policy["guardian_enforcing_live"])


# ── 4-5: status file readers ─────────────────────────────────────────────────

class TestStatusFileReaders(unittest.TestCase):
    def test_04_missing_status_files_degrade_gracefully(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            for fn in (gr.read_guardian_status, gr.read_enforcer_status,
                       gr.read_executor_report, gr.read_resource_status, gr.read_scorecard):
                with self.subTest(fn=fn.__name__):
                    result = fn(pdir)
                    self.assertIsInstance(result, dict)
                    self.assertFalse(result["found"])

    def test_04b_present_status_file_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            (pdir / "memory" / "luna_deterministic_executor_report.json").write_text(
                json.dumps({"schema_version": 1, "success": True, "safe_to_apply_real_project": False}),
                encoding="utf-8",
            )
            result = gr.read_executor_report(pdir)
            self.assertTrue(result["found"])
            self.assertEqual(result["data"]["success"], True)


# ── 5-11: synthetic pending actions and evaluation ───────────────────────────

class TestSyntheticPendingActions(unittest.TestCase):
    def test_05_build_synthetic_actions_includes_tiers(self):
        actions = gr.build_synthetic_pending_actions()
        tiers = {a["risk_tier"] for a in actions}
        self.assertIn(0, tiers)   # tier 0
        self.assertIn(3, tiers)   # tier 3 (medium code edit)
        self.assertIn(4, tiers)   # tier 4 (high risk)
        self.assertIn(5, tiers)   # tier 5 (non-delegable)
        # Check non-delegable action type is present.
        types = {a["action_type"] for a in actions}
        self.assertTrue(any(t in gr._NON_DELEGABLE_ACTION_TYPES for t in types))


class TestBuildGuardianReadinessStatus(unittest.TestCase):
    def test_06_status_schema_includes_advisory_true(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            status = gr.build_guardian_readiness_status(pdir)
            self.assertTrue(status["advisory_only"])

    def test_07_guardian_enforcing_live_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            status = gr.build_guardian_readiness_status(pdir)
            self.assertFalse(status["guardian_enforcing_live"])

    def test_08_ready_for_live_enforcement_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            status = gr.build_guardian_readiness_status(pdir)
            self.assertIs(status["ready_for_live_guardian_enforcement"], False)

    def test_09_action_safe_to_execute_now_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            pending = gr.build_synthetic_pending_actions()
            status = gr.build_guardian_readiness_status(pdir, pending_actions=pending)
            for action in status["actions"]:
                self.assertFalse(
                    action.get("safe_to_execute_now"),
                    f"safe_to_execute_now must be False for {action.get('action_id')}"
                )


class TestEvaluateReadinessAction(unittest.TestCase):
    def test_10_missing_receipt_tier2_would_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            action = {
                "action_id": gr.make_readiness_id("t"),
                "action_type": "medium_code_edit",
                "risk_tier": 3,
                "receipt_id": "",
                "target_files": ["luna_modules/sample.py"],
            }
            result = gr.evaluate_readiness_action(pdir, action)
            self.assertFalse(result["safe_to_execute_now"])
            self.assertTrue(result["would_block"])
            self.assertFalse(result["would_allow"])

    def test_11_non_delegable_needs_human(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            action = {
                "action_id": gr.make_readiness_id("t"),
                "action_type": "process_kill",
                "risk_tier": 5,
                "receipt_id": "",
                "target_files": [],
            }
            result = gr.evaluate_readiness_action(pdir, action)
            self.assertFalse(result["safe_to_execute_now"])
            self.assertTrue(result["needs_human"])
            self.assertTrue(result["non_delegable"])
            self.assertFalse(result["would_allow"])


# ── 12-14: status content and rendering ──────────────────────────────────────

class TestStatusContent(unittest.TestCase):
    def test_12_healthy_no_pending_state_watch_or_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            status = gr.build_guardian_readiness_status(pdir, pending_actions=[])
            self.assertIn(status["overall_status"], ("healthy", "watch", "unknown"))
            self.assertTrue(status["advisory_only"])

    def test_13_markdown_includes_advisory_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            pending = gr.build_synthetic_pending_actions()
            status = gr.build_guardian_readiness_status(pdir, pending_actions=pending)
            md = gr.render_guardian_readiness_markdown(status)
            self.assertIn("advisory_only", md)
            self.assertIn("False", md)  # ready_for_live_guardian_enforcement: False
            self.assertIn("guardian_enforcing_live", md)

    def test_14_write_report_stays_under_temp_memory_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            status = gr.build_guardian_readiness_status(pdir)
            written = gr.write_guardian_readiness_report(pdir, status)
            pdir_resolved = pdir.resolve()
            for key, path in written.items():
                p = Path(path)
                self.assertTrue(
                    str(p.resolve()).startswith(str(pdir_resolved)),
                    f"{p} escapes project dir"
                )
                self.assertIn("memory", str(p))


# ── 15-16: soak cycles ───────────────────────────────────────────────────────

class TestSoakCycles(unittest.TestCase):
    def test_15_soak_cycles_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            result = gr.run_readiness_soak(pdir, cycles=2, sleep_seconds=0)
            self.assertEqual(result["cycles_run"], 2)
            self.assertTrue(result["advisory_only"])
            self.assertFalse(result["guardian_enforcing_live"])
            self.assertFalse(result["ready_for_live_guardian_enforcement"])
            self.assertEqual(len(result["cycle_results"]), 2)

    def test_16_soak_writes_only_under_temp_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            gr.run_readiness_soak(pdir, cycles=1, sleep_seconds=0)
            soak_file = pdir / "memory" / "luna_guardian_enforcement_soak.jsonl"
            self.assertTrue(soak_file.exists(), "soak jsonl must exist under memory/")
            report_file = pdir / "memory" / "luna_guardian_readiness_report.json"
            self.assertTrue(report_file.exists())


# ── 17-19: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run_cli(self, *args):
        import subprocess
        cmd = [sys.executable, "-m", "luna_modules.luna_guardian_readiness"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_17_self_test_returns_0(self):
        rc, out, err = self._run_cli("--self-test")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        self.assertIn("PASS", out)

    def test_18_status_returns_0(self):
        rc, out, err = self._run_cli("--status")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        data = json.loads(out)
        self.assertTrue(data["advisory_only"])
        self.assertFalse(data["guardian_enforcing_live"])
        self.assertIs(data["ready_for_live_guardian_enforcement"], False)

    def test_19_soak_returns_0(self):
        rc, out, err = self._run_cli("--soak", "--cycles", "2", "--sleep-seconds", "0")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        data = json.loads(out)
        self.assertTrue(data["advisory_only"])
        self.assertEqual(data["cycles_run"], 2)


# ── 20: source safety ────────────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_guardian_readiness.py").read_text(encoding="utf-8")

    def test_20_no_process_kill_start_stop_service_commands(self):
        lower = self._src.lower()
        for bad in ("subprocess.run.*taskkill", "subprocess.run.*stop-process",
                    "os.system.*taskkill", "startservice", "stopservice"):
            import re
            key = bad.split(".*")[0]
            if key in lower:
                # Only fail if it's a live call, not a string literal.
                matches = re.findall(r'(?<!["\'])' + re.escape(key), lower)
                # Allow any occurrence inside string tuples/dicts.
                for m in matches:
                    idx = lower.find(key)
                    line_start = lower.rfind("\n", 0, idx) + 1
                    line_end = lower.find("\n", idx)
                    line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                    self.assertFalse(
                        "subprocess.run" in line or "os.system" in line,
                        f"Found dangerous call '{key}' in: {line!r}"
                    )

    def test_20b_no_external_api_imports(self):
        for bad in ("import requests", "import openai", "import anthropic"):
            self.assertNotIn(bad, self._src)

    def test_20c_no_guardian_edit(self):
        import re
        # Must not open or write luna_guardian.py.
        edits = re.findall(r'open\([^)]*luna_guardian[^)]*["\'][wxa]', self._src)
        self.assertEqual(edits, [], f"Found luna_guardian.py write attempt: {edits}")
        self.assertNotIn("luna_guardian.py\", \"w\"", self._src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
