"""Phase 5U tests: Decision Card Integration across the advisory chain.

25+ tests covering router/enforcer/guardian-readiness/limited-autonomy
integration with luna_serge_policy decision cards.
All tests use TemporaryDirectory or read-only inspections; no real
project files are modified.
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

import luna_modules.luna_approval_router as ar
import luna_modules.luna_council_enforcer as ce
import luna_modules.luna_guardian_readiness as gr
import luna_modules.luna_limited_autonomy as la
import luna_modules.luna_serge_policy as sp


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    (tmp / "luna_modules").mkdir(parents=True, exist_ok=True)
    return tmp


def _build_router_request(action: str, target: str, *, goal: str = "") -> dict:
    return ar.build_router_request(
        goal=goal or f"Phase 5U test for {action}",
        target_files=[target] if target else [],
        requested_action=action,
        source="test",
        task_id=f"task_{action}",
        planned_change_summary=f"phase5u test {action}",
    )


# ── 1-4: router integration ──────────────────────────────────────────────────

class TestRouterIntegration(unittest.TestCase):
    def test_01_router_generated_artifact_includes_decision_card(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            req = _build_router_request("generated_artifact", "memory/luna_capability_scorecard.json")
            report = ar.route_approval_request(pdir, req, dry_run=True)
            self.assertIn("decision_card", report)
            self.assertIn("decision_card_recommendation", report)
            self.assertIn("serge_plain_english_summary", report)
            self.assertIs(report["safe_to_execute_now"], False)

    def test_02_router_green_generated_artifact_card_can_be_approve_but_not_execute(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            req = _build_router_request(
                "generated_artifact", "memory/luna_capability_scorecard.json",
                goal="Refresh Luna scorecard memory",
            )
            report = ar.route_approval_request(pdir, req, dry_run=True)
            # Generated artifact / not_required should pass router and produce a card.
            self.assertIs(report["safe_to_execute_now"], False)
            rec = report["decision_card_recommendation"]
            self.assertIn(rec, (sp.APPROVE_RECOMMENDED, sp.WAIT_FOR_MORE_EVIDENCE))

    def test_03_router_high_risk_missing_evidence_card_is_wait_or_deny(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            req = _build_router_request("high_risk_core_edit", "worker.py",
                                        goal="Edit worker.py continues_update")
            report = ar.route_approval_request(pdir, req, dry_run=True)
            rec = report["decision_card_recommendation"]
            self.assertIn(rec, (sp.WAIT_FOR_MORE_EVIDENCE, sp.DO_NOT_APPROVE, sp.SERGE_ONLY))
            self.assertIs(report["safe_to_execute_now"], False)

    def test_04_router_non_delegable_card_is_serge_only_or_deny(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            req = _build_router_request("non_delegable", "memory/nightly_updates.md",
                                        goal="Delete memory logs")
            report = ar.route_approval_request(pdir, req, dry_run=True)
            rec = report["decision_card_recommendation"]
            self.assertIn(rec, (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))
            self.assertIs(report["safe_to_execute_now"], False)


# ── 5-7: council enforcer integration ────────────────────────────────────────

class TestEnforcerIntegration(unittest.TestCase):
    def test_05_enforcer_result_includes_decision_card(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            action = {
                "action_id": "a1", "task_id": "t1",
                "action_type": "generated_artifact", "risk_tier": 1,
                "target_files": ["memory/scorecard.json"], "goal": "Refresh scorecard",
            }
            result = ce.evaluate_action_enforcement(str(pdir), action)
            self.assertIn("decision_card", result)
            self.assertIn("decision_card_recommendation", result)
            self.assertIn("plain_english_decision", result)
            self.assertIs(result["safe_to_execute_now"], False)
            self.assertIs(result["advisory_only"], True)

    def test_06_enforcer_missing_receipt_gives_wait_or_deny(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            action = {
                "action_id": "a2", "task_id": "t2",
                "action_type": "medium_code_edit", "risk_tier": 3,
                "target_files": ["luna_modules/example.py"],
                "goal": "Improve Luna playbook matcher",
            }
            result = ce.evaluate_action_enforcement(str(pdir), action)
            self.assertEqual(result["decision"], "would_block")
            rec = result["decision_card_recommendation"]
            self.assertIn(rec, (sp.WAIT_FOR_MORE_EVIDENCE, sp.DO_NOT_APPROVE, sp.SERGE_ONLY))
            self.assertIs(result["safe_to_execute_now"], False)

    def test_07_enforcer_non_delegable_gives_serge_only_or_deny(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            action = {
                "action_id": "a3", "task_id": "t3",
                "action_type": "delete_memory", "risk_tier": 5,
                "target_files": ["memory/nightly_updates.md"],
                "goal": "Delete memory logs",
            }
            result = ce.evaluate_action_enforcement(str(pdir), action)
            rec = result["decision_card_recommendation"]
            self.assertIn(rec, (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))
            self.assertIs(result["safe_to_execute_now"], False)


# ── 8-9: guardian readiness integration ──────────────────────────────────────

class TestGuardianReadinessIntegration(unittest.TestCase):
    def test_08_guardian_readiness_includes_decision_card_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            pending = gr.build_synthetic_pending_actions()
            status = gr.build_guardian_readiness_status(pdir, pending_actions=pending)
            self.assertIn("decision_card_summary", status)
            cs = status["decision_card_summary"]
            for key in ("approve_recommended", "wait_for_more_evidence",
                        "do_not_approve", "serge_only"):
                self.assertIn(key, cs)
            total = sum(cs.values())
            self.assertEqual(total, status["pending_action_count"])

    def test_09_guardian_readiness_actions_all_safe_to_execute_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            pending = gr.build_synthetic_pending_actions()
            status = gr.build_guardian_readiness_status(pdir, pending_actions=pending)
            for action in status["actions"]:
                self.assertIs(action["safe_to_execute_now"], False)


# ── 10-11: limited autonomy integration ──────────────────────────────────────

class TestLimitedAutonomyIntegration(unittest.TestCase):
    def test_10_routing_summary_includes_decision_card_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            r1 = la.route_blocked_task_for_approval(
                pdir, goal="Refactor Luna playbook matcher",
                task_class="code_edit", target_files=["luna_modules/example.py"],
                dry_run=True,
            )
            r2 = la.route_blocked_task_for_approval(
                pdir, goal="Edit worker.py continues_update",
                task_class="worker_edit", target_files=["worker.py"],
                dry_run=True,
            )
            summary = la.summarize_approval_routing([r1, r2])
            self.assertIn("decision_card_summary", summary)
            cs = summary["decision_card_summary"]
            for key in ("approve_recommended", "wait_for_more_evidence",
                        "do_not_approve", "serge_only", "unavailable"):
                self.assertIn(key, cs)

    def test_11_routing_result_includes_card_recommendation(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            r = la.route_blocked_task_for_approval(
                pdir, goal="Edit worker.py for new feature",
                task_class="worker_edit", target_files=["worker.py"],
                dry_run=True,
            )
            self.assertIn("decision_card_recommendation", r)
            self.assertIn("serge_plain_english_summary", r)
            self.assertIs(r["safe_to_execute_now"], False)


# ── 12: graceful degradation ─────────────────────────────────────────────────

class TestGracefulDegradation(unittest.TestCase):
    def test_12_router_degrades_when_serge_policy_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Monkey-patch the import getter to return None.
            original = ar._serge_policy_module
            try:
                ar._serge_policy_module = lambda: None
                req = _build_router_request("generated_artifact", "memory/scorecard.json")
                report = ar.route_approval_request(pdir, req, dry_run=True)
                self.assertEqual(report.get("decision_card_recommendation"), "UNAVAILABLE")
                self.assertIs(report["safe_to_execute_now"], False)
            finally:
                ar._serge_policy_module = original

    def test_12b_enforcer_degrades_when_serge_policy_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            original = ce._serge_policy_module
            try:
                ce._serge_policy_module = lambda: None
                action = {
                    "action_id": "a1", "task_id": "t1",
                    "action_type": "generated_artifact", "risk_tier": 1,
                    "target_files": ["memory/scorecard.json"], "goal": "Refresh",
                }
                result = ce.evaluate_action_enforcement(str(pdir), action)
                self.assertEqual(result.get("decision_card_recommendation"), "UNAVAILABLE")
                self.assertIs(result["safe_to_execute_now"], False)
            finally:
                ce._serge_policy_module = original


# ── 13-14: routine and overnight code edits invariants ───────────────────────

class TestSafetyFlagsInvariant(unittest.TestCase):
    def test_13_routine_code_edits_remain_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            r = la.route_blocked_task_for_approval(
                pdir, goal="Refactor playbook matcher",
                task_class="code_edit", target_files=["luna_modules/example.py"],
                dry_run=True,
            )
            self.assertIs(r["safe_to_execute_now"], False)
            # router_report (if present) must also have safe_to_execute_now=False.
            rr = r.get("router_report") or {}
            if rr:
                self.assertIs(rr.get("safe_to_execute_now"), False)

    def test_14_overnight_code_edits_remain_false_in_cli(self):
        cmd = [
            sys.executable, "-m", "luna_modules.luna_limited_autonomy",
            "--request-approval", "Edit worker.py",
            "--action", "high_risk_core_edit",
            "--target", "worker.py",
            "--dry-run",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                              cwd=str(_PROJECT_ROOT))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        data = json.loads(proc.stdout)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIs(data["safe_to_run_routine_code_edits"], False)
        self.assertIs(data["safe_to_run_overnight_code_edits"], False)


# ── 15-18: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _modules = (
        _PROJECT_ROOT / "luna_modules" / "luna_approval_router.py",
        _PROJECT_ROOT / "luna_modules" / "luna_council_enforcer.py",
        _PROJECT_ROOT / "luna_modules" / "luna_guardian_readiness.py",
        _PROJECT_ROOT / "luna_modules" / "luna_limited_autonomy.py",
    )

    def _read(self, p: Path) -> str:
        return p.read_text(encoding="utf-8")

    def test_15_no_source_modifications_during_card_attach(self):
        # Decision-card attach helpers must not write to source files.
        # Inspect the helper code for write/open(..., 'w'/'x'/'a').
        for mod in self._modules:
            with self.subTest(mod=str(mod)):
                src = self._read(mod)
                # Look for any open(..., 'w') touching .py source paths in card helpers.
                # Acceptable writes are to memory/ artifacts only.
                attach_writes = re.findall(
                    r'open\([^)]*\.py[^)]*["\'][wax]',
                    src
                )
                self.assertEqual(attach_writes, [], f"Unexpected .py writes: {attach_writes}")

    def test_16_no_aider_invocation_in_modified_modules(self):
        for mod in self._modules:
            with self.subTest(mod=str(mod)):
                src = self._read(mod)
                self.assertNotIn("import aider", src)
                aider_calls = re.findall(r'subprocess\.[^\n]*aider', src)
                self.assertEqual(aider_calls, [])

    def test_17_no_external_api_calls(self):
        for mod in self._modules:
            with self.subTest(mod=str(mod)):
                src = self._read(mod)
                for bad in ("import requests", "import openai", "import anthropic",
                            "import xai", "import httpx"):
                    self.assertNotIn(bad, src)

    def test_18_no_dangerous_subprocess_commands(self):
        for mod in self._modules:
            with self.subTest(mod=str(mod)):
                src = self._read(mod)
                lower = src.lower()
                for bad in ("pip install", "taskkill", "git reset", "delete_queue"):
                    found = lower.find(bad)
                    while found != -1:
                        line_start = lower.rfind("\n", 0, found) + 1
                        line_end = lower.find("\n", found)
                        line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                        self.assertFalse(
                            "subprocess.run" in line and bad in line,
                            f"Found '{bad}' in subprocess.run in {mod.name}: {line!r}"
                        )
                        self.assertFalse(
                            "os.system" in line and bad in line,
                            f"Found '{bad}' in os.system in {mod.name}: {line!r}"
                        )
                        found = lower.find(bad, found + 1)


# ── 19-22: CLI smoke ─────────────────────────────────────────────────────────

class TestCLISmoke(unittest.TestCase):
    def _run(self, *args):
        proc = subprocess.run(
            [sys.executable, "-m"] + list(args),
            capture_output=True, text=True, timeout=60,
            cwd=str(_PROJECT_ROOT),
        )
        return proc.returncode, proc.stdout, proc.stderr

    def test_19_cli_router_sample_returns_0(self):
        rc, out, err = self._run(
            "luna_modules.luna_approval_router",
            "--request", "Refresh scorecard",
            "--action", "generated_artifact",
            "--target", "memory/luna_capability_scorecard.json",
            "--dry-run",
        )
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIn("decision_card_recommendation", data)

    def test_20_cli_enforcer_status_returns_0(self):
        rc, out, err = self._run("luna_modules.luna_council_enforcer", "--status")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["advisory_only"], True)
        self.assertIn("decision_card_summary", data)

    def test_21_cli_guardian_readiness_status_returns_0(self):
        rc, out, err = self._run("luna_modules.luna_guardian_readiness", "--status")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["advisory_only"], True)
        self.assertIs(data["guardian_enforcing_live"], False)
        self.assertIn("decision_card_summary", data)

    def test_22_cli_routine_request_approval_returns_0(self):
        rc, out, err = self._run(
            "luna_modules.luna_limited_autonomy",
            "--request-approval", "Edit worker.py continues_update",
            "--action", "high_risk_core_edit",
            "--target", "worker.py",
            "--dry-run",
        )
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIn("decision_card_recommendation", data)


# ── 23-25: cross-suite invariants ────────────────────────────────────────────

class TestCrossSuiteInvariants(unittest.TestCase):
    def test_23_phase5t_serge_policy_tests_still_pass(self):
        proc = subprocess.run(
            [sys.executable, "-m", "unittest", "tests.test_luna_serge_policy"],
            capture_output=True, text=True, timeout=180, cwd=str(_PROJECT_ROOT),
        )
        self.assertEqual(proc.returncode, 0, f"stderr={proc.stderr}")

    def test_24_all_modified_modules_compile(self):
        for mod in (
            "luna_modules/luna_approval_router.py",
            "luna_modules/luna_council_enforcer.py",
            "luna_modules/luna_guardian_readiness.py",
            "luna_modules/luna_limited_autonomy.py",
        ):
            with self.subTest(mod=mod):
                proc = subprocess.run(
                    [sys.executable, "-m", "py_compile", str(_PROJECT_ROOT / mod)],
                    capture_output=True, text=True, timeout=30, cwd=str(_PROJECT_ROOT),
                )
                self.assertEqual(proc.returncode, 0, f"py_compile failed for {mod}: {proc.stderr}")

    def test_25_wipe_computer_decision_card_never_approve(self):
        # End-to-end: build a synthetic wipe-computer context and confirm the
        # card is SERGE_ONLY/DO_NOT_APPROVE through the policy module.
        ctx = sp._sample_wipe_computer_context()
        card = sp.build_decision_card(ctx)
        self.assertNotEqual(card["recommendation"], sp.APPROVE_RECOMMENDED)
        self.assertIn(card["recommendation"], (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))
        self.assertIs(card["safe_to_execute_now"], False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
