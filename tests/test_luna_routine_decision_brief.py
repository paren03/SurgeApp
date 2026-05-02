"""Phase 5XY tests: morning decision brief wiring into Limited Routine Autonomy.

20+ tests covering refresh_morning_decision_brief, daily_brief_report task
integration, cycle report decision_brief block, hard safety invariants,
formal soak policy, CLI rc=0, and source-code safety.
All tests use TemporaryDirectory or read-only inspections; no real project
files are modified.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_limited_autonomy as la
import luna_modules.luna_decision_brief as db


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    return tmp


def _seed_router_card(pdir: Path, recommendation: str = "APPROVE_RECOMMENDED") -> None:
    rep = {
        "schema_version": 1, "request_id": "rq_x",
        "goal": "Refresh Luna scorecard memory (test)",
        "action_type": "generated_artifact", "approval_tier_required": 1,
        "routing_decision": "not_required", "safe_to_execute_now": False,
        "decision_card": {
            "schema_version": 1, "card_id": "c_x",
            "recommendation": recommendation, "risk_level": "low",
            "goal_alignment": "aligned", "safe_to_execute_now": False,
            "plain_english_final_recommendation": f"plain english for {recommendation}",
        },
        "decision_card_recommendation": recommendation,
        "serge_plain_english_summary": f"plain english for {recommendation}",
    }
    (pdir / "memory" / "luna_approval_router_report.json").write_text(
        json.dumps(rep), encoding="utf-8"
    )


# ── 1-3: refresh helper ──────────────────────────────────────────────────────

class TestRefreshHelper(unittest.TestCase):
    def test_01_refresh_uses_decision_brief_when_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            result = la.refresh_morning_decision_brief(pdir, write=True)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "refreshed")
            self.assertEqual(result["overall_recommendation"], "continue_safe_routine")
            self.assertIs(result["safe_to_execute_now"], False)
            self.assertIs(result["safe_to_apply_real_project"], False)
            self.assertIs(result["guardian_enforcing_live"], False)
            # Verify the brief was actually written.
            self.assertTrue((pdir / "memory" / "luna_morning_decision_brief.json").exists())

    def test_02_refresh_dry_run_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            result = la.refresh_morning_decision_brief(pdir, write=False)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "built")
            self.assertFalse((pdir / "memory" / "luna_morning_decision_brief.json").exists())
            self.assertIs(result["safe_to_execute_now"], False)

    def test_03_refresh_degrades_gracefully_when_brief_module_missing(self):
        # Simulate missing luna_decision_brief by patching builtins.__import__
        # to raise for that specific module.
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            import builtins
            real_import = builtins.__import__

            def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
                if name == "luna_modules.luna_decision_brief":
                    raise ImportError("simulated missing module")
                return real_import(name, globals, locals, fromlist, level)
            try:
                builtins.__import__ = fake_import
                result = la.refresh_morning_decision_brief(pdir, write=True)
            finally:
                builtins.__import__ = real_import
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "unavailable")
            self.assertIn("error", result)
            self.assertIs(result["safe_to_execute_now"], False)


# ── 4-7: daily_brief_report task uses brief ──────────────────────────────────

class TestDailyBriefTask(unittest.TestCase):
    def test_04_task_calls_decision_brief(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            tr = la.run_allowed_foundation_task(pdir, "daily_brief_report", dry_run=True)
            self.assertEqual(tr["status"], "ok")
            details = tr["details"]
            self.assertTrue(details["decision_brief_ok"])
            self.assertEqual(details["decision_brief_status"], "built")
            self.assertEqual(details["overall_recommendation"], "continue_safe_routine")
            self.assertIn("decision_card_counts", details)
            self.assertIn("next_safe_action", details)
            self.assertIn("serge_summary", details)

    def test_05_task_safe_flags_always_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            tr = la.run_allowed_foundation_task(pdir, "daily_brief_report", dry_run=True)
            d = tr["details"]
            self.assertIs(d["safe_to_execute_now"], False)
            self.assertIs(d["safe_to_apply_real_project"], False)
            self.assertIs(d["guardian_enforcing_live"], False)

    def test_06_task_does_not_modify_source_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Seed a fake source file outside memory/.
            fake_src_dir = pdir / "luna_modules"
            fake_src_dir.mkdir(parents=True, exist_ok=True)
            src = fake_src_dir / "fake_protected.py"
            original = "# protected\n"
            src.write_text(original, encoding="utf-8")
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            la.run_allowed_foundation_task(pdir, "daily_brief_report", dry_run=False)
            self.assertEqual(src.read_text(encoding="utf-8"), original)

    def test_07_task_in_non_dry_run_writes_brief(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            tr = la.run_allowed_foundation_task(pdir, "daily_brief_report", dry_run=False)
            self.assertEqual(tr["status"], "ok")
            self.assertTrue((pdir / "memory" / "luna_morning_decision_brief.json").exists())
            arts = tr.get("artifacts") or []
            self.assertTrue(any("luna_morning_decision_brief" in a for a in arts))


# ── 8-11: cycle report decision_brief block ──────────────────────────────────

class TestCycleReportDecisionBriefBlock(unittest.TestCase):
    def test_08_cycle_report_includes_decision_brief_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            report = la.run_limited_autonomy_cycle(
                pdir, goal="test", dry_run=True, write_report=False,
            )
            self.assertIn("decision_brief", report)
            db_block = report["decision_brief"]
            for k in ("enabled", "refreshed", "overall_recommendation", "counts",
                      "next_safe_action", "serge_summary", "path_json", "path_md",
                      "error"):
                self.assertIn(k, db_block)

    def test_09_cycle_report_safe_flags_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            report = la.run_limited_autonomy_cycle(
                pdir, goal="test", dry_run=True, write_report=False,
            )
            self.assertIs(report["safe_to_run_routine_code_edits"], False)
            self.assertIs(report["safe_to_run_overnight_code_edits"], False)

    def test_10_cycle_report_brief_has_counts_when_routine_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            report = la.run_limited_autonomy_cycle(
                pdir, goal="test", dry_run=True, write_report=False,
            )
            db_block = report["decision_brief"]
            # daily_brief_report is in the default plan; should produce counts.
            attempted_classes = {t.get("task_class") for t in report.get("tasks_attempted") or []}
            if "daily_brief_report" in attempted_classes:
                self.assertTrue(db_block["refreshed"])
                self.assertIn("approve_recommended", db_block["counts"])

    def test_11_cycle_brief_overall_matches_synthetic_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            report = la.run_limited_autonomy_cycle(
                pdir, goal="test", dry_run=True, write_report=False,
            )
            attempted_classes = {t.get("task_class") for t in report.get("tasks_attempted") or []}
            if "daily_brief_report" in attempted_classes:
                self.assertEqual(
                    report["decision_brief"]["overall_recommendation"],
                    "continue_safe_routine",
                )


# ── 12-13: formal advisory soak policy ───────────────────────────────────────

class TestFormalAdvisorySoakPolicy(unittest.TestCase):
    POLICY_PATH = _PROJECT_ROOT / "memory" / "luna_formal_advisory_soak_policy.json"

    def test_12_policy_exists_and_validates(self):
        self.assertTrue(self.POLICY_PATH.exists())
        data = json.loads(self.POLICY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(data["phase"], "5XY")
        self.assertIs(data["advisory_only"], True)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIs(data["safe_to_apply_real_project"], False)
        self.assertIs(data["guardian_enforcing_live"], False)
        self.assertIn("command_template", data)
        # 144 * 600 = 86400 = 24h
        self.assertEqual(
            data["recommended_cycles_for_24h"] * data["recommended_sleep_seconds"],
            86400,
        )

    def test_13_24h_command_template_is_advisory_only(self):
        data = json.loads(self.POLICY_PATH.read_text(encoding="utf-8"))
        cmd = data["command_template"]
        self.assertIn("luna_decision_brief", cmd)
        self.assertIn("--soak", cmd)
        # Strip the venv path so the .aider_venv folder doesn't false-positive on "aider".
        cmd_lower_after_python = cmd.lower().split("python.exe", 1)[-1]
        self.assertNotIn(" aider ", cmd_lower_after_python)
        self.assertNotIn("aider_bridge", cmd_lower_after_python)
        self.assertNotIn("pip install", cmd_lower_after_python)
        self.assertNotIn("git reset", cmd_lower_after_python)
        self.assertNotIn("taskkill", cmd_lower_after_python)
        self.assertNotIn("--apply", cmd_lower_after_python)


# ── 14-16: source-code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_limited_autonomy.py").read_text(encoding="utf-8")

    def test_14_no_aider_invocation_added(self):
        # The module imports luna_modules.luna_decision_brief at function-call
        # time defensively. No aider subprocess calls should appear.
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [])
        self.assertNotIn("import aider", self._src)

    def test_15_no_external_api_imports_added(self):
        for bad in ("import requests", "import openai", "import anthropic",
                    "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_16_no_dangerous_subprocess_commands_added(self):
        lower = self._src.lower()
        for bad in ("pip install", "taskkill", "git reset", "delete_queue"):
            found = lower.find(bad)
            while found != -1:
                line_start = lower.rfind("\n", 0, found) + 1
                line_end = lower.find("\n", found)
                line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"subprocess.run with '{bad}': {line!r}"
                )
                self.assertFalse(
                    "os.system" in line and bad in line,
                    f"os.system with '{bad}': {line!r}"
                )
                found = lower.find(bad, found + 1)


# ── 17-19: CLI smoke ─────────────────────────────────────────────────────────

class TestCLISmoke(unittest.TestCase):
    def _run(self, *args, timeout=120):
        cmd = [sys.executable, "-m", "luna_modules.luna_limited_autonomy"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                              cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_17_routine_dry_run_returns_0(self):
        rc, out, err = self._run(
            "--routine-dry-run", "--max-cycles", "1", "--sleep-seconds", "1",
        )
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")

    def test_18_overnight_dry_run_alias_returns_0(self):
        rc, out, err = self._run(
            "--overnight-dry-run", "--max-cycles", "1", "--sleep-seconds", "1",
        )
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")

    def test_19_decision_brief_soak_short_smoke_returns_0(self):
        cmd = [
            sys.executable, "-m", "luna_modules.luna_decision_brief",
            "--soak", "--cycles", "2", "--sleep-seconds", "0",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                              cwd=str(_PROJECT_ROOT))
        self.assertEqual(proc.returncode, 0, f"out={proc.stdout}\nerr={proc.stderr}")
        data = json.loads(proc.stdout)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIs(data["safe_to_apply_real_project"], False)


# ── 20-21: invariants ────────────────────────────────────────────────────────

class TestInvariants(unittest.TestCase):
    def test_20_self_test_still_returns_0(self):
        # Self-test inside limited_autonomy must still pass.
        rc = la.self_test()
        self.assertEqual(rc, 0)

    def test_21_decision_brief_self_test_returns_0(self):
        rc = db.self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
