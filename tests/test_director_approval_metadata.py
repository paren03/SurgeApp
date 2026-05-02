"""Tests for Phase 5P Director approval metadata. 25+ tests."""
from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from director_agent import (
    _build_approval_packet_hint,
    _build_planned_change_template,
    _mission_is_non_delegable,
    _mission_requires_council,
    _normalize_target_file,
    _quorum_for_tier,
    _reviewer_pool_for_tier,
    _risk_to_approval_tier,
    build_director_missions,
    enrich_mission_with_approval_metadata,
    enrich_missions_with_approval_metadata,
    parse_ceo_command,
    validate_director_approval_metadata,
    write_director_job,
    write_director_refresh_job,
)

PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent)

_APPROVAL_METADATA_FIELDS = [
    "approval_tier_required", "council_required", "receipt_required",
    "needs_human", "non_delegable", "non_delegable_reasons",
    "reviewer_pool", "quorum_required", "approval_packet_hint",
    "planned_change_template", "enforcement_mode", "executor_allowed",
]


def _make_mission(
    mission_id: str = "test",
    purpose: str = "test purpose",
    target_files: list = None,
    risk_level: str = "medium",
    diff_type: str = "safety_guard",
    max_lines: int = 80,
) -> dict:
    return {
        "id": mission_id,
        "purpose": purpose,
        "target_files": target_files or ["luna_modules/test_feature.py"],
        "risk_level": risk_level,
        "acceptance_test": "tests pass",
        "rollback_stage_plan": "stage only",
        "expected_diff_type": diff_type,
        "max_lines_changed": max_lines,
        "function_scope_required": False,
    }


class TestParseCeoCommand(unittest.TestCase):
    def test_01_accepts_ceo(self):
        r = parse_ceo_command("/ceo safely improve Luna runtime")
        self.assertTrue(r["accepted"])
        self.assertEqual(r["goal"], "safely improve Luna runtime")

    def test_02_rejects_non_ceo(self):
        r = parse_ceo_command("do something else")
        self.assertFalse(r["accepted"])
        self.assertEqual(r["reason"], "not_ceo_command")


class TestBuildDirectorMissions(unittest.TestCase):
    def test_03_returns_missions(self):
        missions = build_director_missions("improve Luna")
        self.assertGreaterEqual(len(missions), 8)

    def test_04_every_mission_has_approval_metadata(self):
        missions = build_director_missions("improve Luna")
        for m in missions:
            for field in _APPROVAL_METADATA_FIELDS:
                self.assertIn(field, m, f"Mission '{m.get('id')}' missing field '{field}'")

    def test_05_high_risk_worker_mission_gets_tier_4(self):
        missions = build_director_missions("improve Luna")
        worker_missions = [m for m in missions if "worker.py" in m.get("target_files", [])]
        self.assertTrue(worker_missions, "Expected at least one worker.py mission")
        for m in worker_missions:
            self.assertGreaterEqual(m["approval_tier_required"], 4)

    def test_06_medium_risk_non_core_mission_tier_3(self):
        # self_test_harness targets test files — medium risk, no core files → tier 3
        missions = build_director_missions("improve Luna")
        test_missions = [m for m in missions if m.get("id") == "self_test_harness"]
        self.assertTrue(test_missions)
        self.assertEqual(test_missions[0]["approval_tier_required"], 3)

    def test_07_low_risk_memory_summary_tier_1(self):
        missions = build_director_missions("improve Luna")
        summary = [m for m in missions if m.get("id") == "nightly_summary_learning"]
        self.assertTrue(summary)
        self.assertLessEqual(summary[0]["approval_tier_required"], 1)

    def test_08_reviewer_pool_present_for_tier_2_plus(self):
        missions = build_director_missions("improve Luna")
        for m in missions:
            if m["approval_tier_required"] >= 2:
                self.assertGreater(len(m["reviewer_pool"]), 0,
                                   f"Mission '{m['id']}' tier {m['approval_tier_required']} has empty reviewer_pool")

    def test_09_quorum_unanimous_for_tier_4_plus(self):
        missions = build_director_missions("improve Luna")
        for m in missions:
            if m["approval_tier_required"] >= 4 and not m["non_delegable"]:
                self.assertTrue(m["quorum_required"]["unanimous"],
                                f"Mission '{m['id']}' tier 4 should be unanimous")

    def test_10_receipt_required_true_for_tier_2_plus(self):
        missions = build_director_missions("improve Luna")
        for m in missions:
            if m["approval_tier_required"] >= 2 and not m["non_delegable"]:
                self.assertTrue(m["receipt_required"],
                                f"Mission '{m['id']}' tier >= 2 must have receipt_required=True")

    def test_11_executor_allowed_always_false(self):
        missions = build_director_missions("improve Luna")
        for m in missions:
            self.assertIs(m["executor_allowed"], False,
                          f"Mission '{m['id']}' executor_allowed must be False")


class TestNonDelegable(unittest.TestCase):
    def test_12_delete_memory_is_human_only(self):
        m = _make_mission("del_mem", purpose="delete memory and wipe logs")
        enriched = enrich_mission_with_approval_metadata(m)
        self.assertTrue(enriched["non_delegable"])
        self.assertTrue(enriched["needs_human"])
        self.assertEqual(enriched["enforcement_mode"], "human_only")
        self.assertIs(enriched["executor_allowed"], False)

    def test_13_package_install_is_human_only(self):
        m = _make_mission("pkg", purpose="pip install some package")
        enriched = enrich_mission_with_approval_metadata(m)
        self.assertTrue(enriched["non_delegable"])
        self.assertEqual(enriched["enforcement_mode"], "human_only")

    def test_14_verifier_weakening_is_human_only(self):
        m = _make_mission("weak", purpose="disable verifier and weaken checks")
        enriched = enrich_mission_with_approval_metadata(m)
        self.assertTrue(enriched["non_delegable"])
        self.assertEqual(enriched["enforcement_mode"], "human_only")


class TestApprovalPacketHint(unittest.TestCase):
    def test_15_packet_hint_has_target_files_and_question(self):
        m = _make_mission(target_files=["luna_modules/feat.py"])
        enriched = enrich_mission_with_approval_metadata(m)
        hint = enriched["approval_packet_hint"]
        self.assertIn("target_files", hint)
        self.assertIn("question", hint)
        self.assertIn("yes/no", hint["question"])

    def test_16_planned_change_template_has_rollback_and_verification(self):
        m = _make_mission(target_files=["luna_modules/feat.py"])
        enriched = enrich_mission_with_approval_metadata(m)
        tpl = enriched["planned_change_template"]
        self.assertIn("rollback_plan", tpl)
        self.assertIn("verification_commands", tpl)


class TestWriteDirectorJob(unittest.TestCase):
    def test_17_write_job_embeds_metadata_in_active_file(self):
        with tempfile.TemporaryDirectory() as t:
            job = write_director_job(t, "/ceo safely improve Luna runtime")
            self.assertEqual(job["state"], "active")
            payload = json.loads(pathlib.Path(job["path"]).read_text(encoding="utf-8"))
            for m in payload["missions"]:
                for field in _APPROVAL_METADATA_FIELDS:
                    self.assertIn(field, m, f"Mission '{m.get('id')}' missing '{field}' in written file")

    def test_18_write_job_kill_switch_writes_failed_with_safe_policy(self):
        with tempfile.TemporaryDirectory() as t:
            # Create kill switch
            pathlib.Path(t, "LUNA_STOP_NOW.flag").touch()
            job = write_director_job(t, "/ceo safely improve Luna runtime")
            self.assertEqual(job["state"], "failed")
            payload = json.loads(pathlib.Path(job["path"]).read_text(encoding="utf-8"))
            pol = payload.get("policy", {})
            self.assertTrue(pol.get("stage_only"))
            self.assertEqual(pol.get("delete"), "never")
            self.assertIs(pol.get("executor_allowed"), False)


class TestWriteDirectorRefreshJob(unittest.TestCase):
    def test_19_refresh_enriches_stale_missions(self):
        with tempfile.TemporaryDirectory() as t:
            root = pathlib.Path(t)
            qdir = root / "director_jobs" / "quarantine"
            qdir.mkdir(parents=True, exist_ok=True)
            stale = qdir / "stale.json"
            # Old mission without approval metadata
            stale.write_text(json.dumps({
                "state": "quarantine",
                "missions": [{
                    "id": "old_mission",
                    "purpose": "add a feature",
                    "target_files": ["luna_modules/new_feat.py"],
                    "risk_level": "medium",
                    "acceptance_test": "passes",
                    "rollback_stage_plan": "stage only",
                    "expected_diff_type": "safety_guard",
                    "max_lines_changed": 80,
                }]
            }), encoding="utf-8")
            job = write_director_refresh_job(t, stale)
            self.assertEqual(job["state"], "active")
            m = job["missions"][0]
            for field in _APPROVAL_METADATA_FIELDS:
                self.assertIn(field, m, f"Refreshed mission missing '{field}'")


class TestValidateMetadata(unittest.TestCase):
    def test_20_validate_catches_missing_fields(self):
        m = {"id": "bare", "purpose": "no metadata"}
        ok, errors = validate_director_approval_metadata(m)
        self.assertFalse(ok)
        self.assertGreater(len(errors), 0)

    def test_21_validate_passes_enriched_mission(self):
        m = enrich_mission_with_approval_metadata(_make_mission())
        ok, errors = validate_director_approval_metadata(m)
        self.assertTrue(ok, f"Validation errors: {errors}")


class TestNoExternalDeps(unittest.TestCase):
    def _src(self) -> str:
        return (pathlib.Path(PROJECT_ROOT) / "director_agent.py").read_text(encoding="utf-8")

    def test_21b_no_external_api_calls(self):
        src = self._src()
        for banned in ("import requests", "import httpx", "import openai", "import anthropic"):
            self.assertNotIn(banned, src)

    def test_22_no_aider_invocation(self):
        src = self._src()
        self.assertNotIn('"aider"', src)

    def test_23_no_source_execution_enabled_by_metadata(self):
        missions = build_director_missions("improve Luna runtime")
        for m in missions:
            self.assertIs(m["executor_allowed"], False)
            # enforcement_mode must not be "execute"
            self.assertNotEqual(m.get("enforcement_mode"), "execute")


class TestPolicyPreservation(unittest.TestCase):
    def test_24_existing_policy_stage_only_delete_never_quarantine(self):
        with tempfile.TemporaryDirectory() as t:
            job = write_director_job(t, "/ceo improve Luna")
            pol = json.loads(pathlib.Path(job["path"]).read_text())["policy"]
            self.assertTrue(pol["stage_only"])
            self.assertEqual(pol["delete"], "never")
            self.assertTrue(pol["quarantine_bad_items"])

    def test_25_self_contained_import(self):
        # director_agent is already imported at module scope; verify key symbols exist
        import director_agent as da
        self.assertTrue(hasattr(da, "build_director_missions"))
        self.assertTrue(hasattr(da, "enrich_mission_with_approval_metadata"))
        self.assertTrue(hasattr(da, "validate_director_approval_metadata"))
        self.assertTrue(hasattr(da, "write_director_job"))
        self.assertTrue(hasattr(da, "write_director_refresh_job"))


if __name__ == "__main__":
    unittest.main()
