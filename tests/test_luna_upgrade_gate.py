"""Phase 5F tests: luna_upgrade_gate.

Stdlib unittest only. Tests use either pure-function paths or
TemporaryDirectory for any disk artifact. The real
memory/luna_upgrade_gate_policy.json is loaded read-only.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_upgrade_gate import (  # noqa: E402
    _DEFAULT_POLICY,
    DEFAULT_POLICY_PATH,
    SCHEMA_VERSION,
    classify_target_risk,
    evaluate_diff_size,
    evaluate_forbidden_paths,
    evaluate_git_clean,
    evaluate_install_or_external_actions,
    evaluate_personality_goal_safety,
    evaluate_plan_contract,
    evaluate_upgrade_proposal,
    load_policy,
    match_relevant_playbooks,
    normalize_target,
    recall_similar_failures,
    render_gate_report,
    self_test,
    write_gate_report,
)


def _good_low_risk() -> dict:
    return {
        "plan_id": "plan_test_low",
        "title": "tiny logging-comment edit",
        "actor": "test",
        "target_files": ["luna_modules/luna_logging.py"],
        "line_ranges": {"luna_modules/luna_logging.py": [[60, 65]]},
        "action_type": "edit",
        "expected_diff_type": "small_edit",
        "risk_level": "low",
        "approval_tier": 2,
        "diff_stats": {"files_changed": 1, "insertions": 4, "deletions": 1},
        "verification_commands": ["python -m py_compile luna_modules/luna_logging.py"],
        "rollback_plan": "git checkout HEAD -- luna_modules/luna_logging.py",
        "install_commands": [],
        "external_network": False,
        "touches_personality_or_goals": False,
        "touches_memory_content": False,
        "touches_runtime_queue": False,
        "operator_approved": False,
    }


class _PolicyTests(unittest.TestCase):

    def test_01_load_policy_fallback_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pol = load_policy(Path(td) / "missing_policy.json")
            self.assertEqual(pol["schema_version"], 1)
            self.assertEqual(pol["allow_package_installs"], False)
            self.assertEqual(pol["_loaded_from_file"], False)
            self.assertEqual(pol["_source"], "module_fallback")

    def test_02_load_policy_handles_malformed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bad = Path(td) / "bad_policy.json"
            bad.write_text("{this is not valid json", encoding="utf-8")
            pol = load_policy(bad)
            self.assertEqual(pol["schema_version"], 1)
            self.assertEqual(pol["_loaded_from_file"], False)


class _LowRiskAndContractTests(unittest.TestCase):

    def test_03_low_risk_proposal_allowed_or_needs_approval(self) -> None:
        d = evaluate_upgrade_proposal(_good_low_risk())
        self.assertIn(d["decision"], ("allow", "needs_approval"))
        self.assertNotEqual(d["decision"], "deny",
                            f"low-risk denied unexpectedly: {d['reasons']}")

    def test_04_missing_plan_id_denies(self) -> None:
        p = _good_low_risk()
        p.pop("plan_id")
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("plan_id" in r for r in d["reasons"]))

    def test_05_missing_rollback_plan_denies(self) -> None:
        p = _good_low_risk()
        p.pop("rollback_plan")
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("rollback_plan" in r for r in d["reasons"]))

    def test_06_missing_verification_commands_denies(self) -> None:
        p = _good_low_risk()
        p["verification_commands"] = []
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("verification_commands" in r for r in d["reasons"]))


class _RiskClassificationTests(unittest.TestCase):

    def test_07_worker_py_high_or_critical(self) -> None:
        tr = classify_target_risk("worker.py")
        self.assertIn(tr["risk_level"], ("high", "critical"))

    def test_08_worker_py_proposal_not_auto_allowed(self) -> None:
        p = _good_low_risk()
        p["target_files"] = ["worker.py"]
        p["line_ranges"] = {"worker.py": [[12200, 12210]]}
        d = evaluate_upgrade_proposal(p)
        self.assertNotEqual(d["decision"], "allow",
                            "worker.py must not auto-allow")

    def test_09_luna_hygiene_critical_and_denied(self) -> None:
        tr = classify_target_risk("luna_modules/luna_hygiene.py")
        self.assertEqual(tr["risk_level"], "critical")
        p = _good_low_risk()
        p["target_files"] = ["luna_modules/luna_hygiene.py"]
        p["line_ranges"] = {"luna_modules/luna_hygiene.py": [[1, 5]]}
        d = evaluate_upgrade_proposal(p)
        self.assertIn(d["decision"], ("deny", "needs_approval"))


class _ForbiddenActionTests(unittest.TestCase):

    def test_10_install_command_denies(self) -> None:
        p = _good_low_risk()
        p["install_commands"] = ["pip install requests"]
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("install" in r.lower() for r in d["reasons"]))

    def test_11_external_network_denies(self) -> None:
        p = _good_low_risk()
        p["external_network"] = True
        # Also include a non-localhost URL in the description so the pattern fires
        p["title"] = "fetch dataset from https://example.com/data.json"
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("external" in r.lower() or "install" in r.lower()
                            for r in d["reasons"]))

    def test_12_memory_deletion_denies(self) -> None:
        p = _good_low_risk()
        p["touches_memory_content"] = True
        p["delete_paths"] = ["memory/luna_change_ledger.jsonl"]
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("memory" in r.lower() or "forbidden" in r.lower()
                            for r in d["reasons"]))

    def test_13_personality_change_denied_without_approval(self) -> None:
        p = _good_low_risk()
        p["title"] = "tweak Luna's personality module"
        p["target_files"] = ["memory/luna_personality_state.json"]
        p["line_ranges"] = {"memory/luna_personality_state.json": [[1, 5]]}
        p["touches_personality_or_goals"] = True
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("personality" in r.lower() or "identity" in r.lower()
                            or "goals" in r.lower() for r in d["reasons"]))


class _DiffSizeTests(unittest.TestCase):

    def test_14_large_diff_denies(self) -> None:
        p = _good_low_risk()
        p["diff_stats"] = {"files_changed": 5, "insertions": 5000, "deletions": 5000}
        d = evaluate_upgrade_proposal(p)
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("diff" in r.lower() or "files_changed" in r.lower()
                            or "insertions" in r.lower() for r in d["reasons"]))


class _GitStatusTests(unittest.TestCase):

    def test_15_dirty_git_status_denies(self) -> None:
        p = _good_low_risk()
        d = evaluate_upgrade_proposal(p, context={
            "git_status": " M worker.py\n M aider_bridge.py\n",
        })
        self.assertEqual(d["decision"], "deny")
        self.assertTrue(any("git_clean" in r or "tree has changes" in r
                            for r in d["reasons"]))


class _OptionalIntegrationTests(unittest.TestCase):

    def test_16_playbook_import_failure_tolerated(self) -> None:
        import sys as _sys
        original = _sys.modules.get("luna_modules.luna_playbook_engine")
        try:
            _sys.modules["luna_modules.luna_playbook_engine"] = None
            results = match_relevant_playbooks(_good_low_risk(), limit=2)
            self.assertEqual(results, [])
            # And the gate still works
            d = evaluate_upgrade_proposal(_good_low_risk())
            self.assertIn(d["decision"], ("allow", "needs_approval"))
        finally:
            if original is not None:
                _sys.modules["luna_modules.luna_playbook_engine"] = original
            elif "luna_modules.luna_playbook_engine" in _sys.modules:
                del _sys.modules["luna_modules.luna_playbook_engine"]

    def test_17_memory_index_import_failure_tolerated(self) -> None:
        import sys as _sys
        original = _sys.modules.get("luna_modules.luna_memory_index")
        try:
            _sys.modules["luna_modules.luna_memory_index"] = None
            results = recall_similar_failures(_good_low_risk(), limit=2)
            self.assertEqual(results, [])
            d = evaluate_upgrade_proposal(_good_low_risk())
            self.assertIn(d["decision"], ("allow", "needs_approval"))
        finally:
            if original is not None:
                _sys.modules["luna_modules.luna_memory_index"] = original
            elif "luna_modules.luna_memory_index" in _sys.modules:
                del _sys.modules["luna_modules.luna_memory_index"]


class _RenderTests(unittest.TestCase):

    def test_18_markdown_includes_decision_and_checks(self) -> None:
        d = evaluate_upgrade_proposal(_good_low_risk())
        md = render_gate_report(d, out_format="markdown")
        self.assertIn("Luna Safe Self-Upgrade Gate", md)
        self.assertIn("Decision", md)
        self.assertIn("Checks", md)
        # Each check name appears
        for c in d["checks"]:
            self.assertIn(c["name"], md)

    def test_19_json_parses(self) -> None:
        d = evaluate_upgrade_proposal(_good_low_risk())
        rendered = render_gate_report(d, out_format="json")
        parsed = json.loads(rendered)
        self.assertEqual(parsed["schema_version"], SCHEMA_VERSION)
        self.assertIn(parsed["decision"], ("allow", "deny", "needs_approval"))


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_upgrade_gate", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

    def test_20_cli_self_test_exits_clean(self) -> None:
        result = self._run("--self-test")
        self.assertEqual(result.returncode, 0,
                         f"--self-test rc={result.returncode}: stderr={result.stderr!r}")
        self.assertIn("\"ok\": true", result.stdout)

    def test_21_cli_proposal_json_returns_valid_json(self) -> None:
        proposal_json = json.dumps(_good_low_risk())
        result = self._run("--proposal-json", proposal_json, "--format", "json")
        # rc 0 only when allow; for low-risk it is allow. rc 4 if not allow.
        # Either way, stdout must be valid JSON.
        self.assertIn(result.returncode, (0, 4))
        parsed = json.loads(result.stdout)
        self.assertEqual(parsed["schema_version"], SCHEMA_VERSION)

    def test_22_self_test_function_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
