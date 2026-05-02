"""Phase 5T tests: Luna Serge Standing Approval Policy + Decision Cards.

33+ test cases covering north star, standing approval policy, Aider Tutor
roadmap, destructive intent detection, goal alignment, decision-card
classification, validation, markdown rendering, write paths, and CLI.
All tests use TemporaryDirectory. No real project files are modified.
"""
from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_serge_policy as sp


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    return tmp


# ── 1-3: policy / roadmap loading ────────────────────────────────────────────

class TestPolicyLoaders(unittest.TestCase):
    def test_01_north_star_policy_loads(self):
        # Real project should have the tracked file.
        ns = sp.load_north_star_policy(_PROJECT_ROOT)
        self.assertIsInstance(ns, dict)
        self.assertIn("north_star", ns)
        self.assertIn("Super AI", ns["north_star"])

    def test_02_standing_approval_policy_loads(self):
        sap = sp.load_standing_approval_policy(_PROJECT_ROOT)
        self.assertIsInstance(sap, dict)
        self.assertTrue(sap["serge_is_not_expected_to_review_code_line_by_line"])
        self.assertTrue(sap["non_delegable_always_waits_for_serge"])
        self.assertIn("non_delegable_actions", sap)
        self.assertIn("wipe_computer", sap["non_delegable_actions"])

    def test_03_aider_tutor_roadmap_loads(self):
        roadmap = sp.load_aider_tutor_roadmap(_PROJECT_ROOT)
        self.assertIsInstance(roadmap, dict)
        self.assertEqual(roadmap["status"], "planned_after_safety_chain")
        self.assertIn("teaches", roadmap["core_rule"].lower())


# ── 4-7: intent and alignment ────────────────────────────────────────────────

class TestDestructiveIntent(unittest.TestCase):
    def test_04_detect_wipe_computer(self):
        result = sp.detect_destructive_intent(goal="wipe computer to reclaim disk")
        self.assertTrue(result["destructive"])
        self.assertGreater(len(result["flags"]), 0)

    def test_05_detect_delete_memory_logs_backups(self):
        for goal in ("delete memory", "wipe logs", "delete backup"):
            with self.subTest(goal=goal):
                result = sp.detect_destructive_intent(goal=goal)
                self.assertTrue(result["destructive"], f"Should detect: {goal}")


class TestGoalAlignment(unittest.TestCase):
    def test_06_super_ai_goal_aligned(self):
        result = sp.detect_goal_alignment(
            goal="Improve Luna sandbox simulation safety", summary=""
        )
        self.assertEqual(result["alignment"], "aligned")

    def test_07_unrelated_goal_watch_or_misaligned(self):
        result = sp.detect_goal_alignment(goal="Order pizza for the team", summary="")
        self.assertIn(result["alignment"], ("watch", "misaligned", "unknown"))

    def test_07b_destructive_goal_misaligned(self):
        result = sp.detect_goal_alignment(goal="wipe computer to free space", summary="")
        self.assertEqual(result["alignment"], "misaligned")


# ── 8-17: classifier on contexts ─────────────────────────────────────────────

class TestClassifier(unittest.TestCase):
    def test_08_green_context_returns_approve(self):
        ctx = sp._sample_green_context()
        result = sp.classify_serge_standing_intent(ctx)
        self.assertEqual(result["recommendation"], sp.APPROVE_RECOMMENDED)

    def test_09_missing_evidence_returns_wait(self):
        ctx = sp._sample_yellow_context()  # all unknowns
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.WAIT_FOR_MORE_EVIDENCE, sp.DO_NOT_APPROVE))

    def test_10_verifier_fail_returns_do_not_approve(self):
        ctx = dict(sp._sample_green_context())
        ctx["verifier_result"] = "fail"
        result = sp.classify_serge_standing_intent(ctx)
        self.assertEqual(result["recommendation"], sp.DO_NOT_APPROVE)

    def test_11_sandbox_fail_returns_do_not_approve(self):
        ctx = dict(sp._sample_green_context())
        ctx["sandbox_result"] = "fail"
        result = sp.classify_serge_standing_intent(ctx)
        self.assertEqual(result["recommendation"], sp.DO_NOT_APPROVE)

    def test_12_secrets_fail_returns_do_not_approve(self):
        ctx = dict(sp._sample_green_context())
        ctx["secrets_scan"] = "fail"
        result = sp.classify_serge_standing_intent(ctx)
        self.assertEqual(result["recommendation"], sp.DO_NOT_APPROVE)

    def test_13_rollback_missing_high_tier_blocks(self):
        ctx = dict(sp._sample_green_context())
        ctx["rollback_exists"] = False
        ctx["risk_tier"] = 3
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.WAIT_FOR_MORE_EVIDENCE, sp.DO_NOT_APPROVE))

    def test_14_non_delegable_returns_serge_only(self):
        ctx = dict(sp._sample_green_context())
        ctx["action_type"] = "wipe_computer"
        ctx["non_delegable_flags"] = ["wipe_computer"]
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))

    def test_15_policy_weakening_returns_serge_only_or_deny(self):
        ctx = dict(sp._sample_green_context())
        ctx["goal"] = "edit policy to approve itself and disable verifier"
        ctx["action_type"] = "edit_policy_to_approve_itself"
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))

    def test_16_reviewer_denial_returns_do_not_approve(self):
        ctx = dict(sp._sample_green_context())
        ctx["reviewer_votes"] = ["deny", "deny"]
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.DO_NOT_APPROVE, sp.SERGE_ONLY))

    def test_17_resource_blocked_blocks_or_waits(self):
        ctx = dict(sp._sample_green_context())
        ctx["resource_status"] = "blocked"
        result = sp.classify_serge_standing_intent(ctx)
        self.assertIn(result["recommendation"], (sp.DO_NOT_APPROVE, sp.WAIT_FOR_MORE_EVIDENCE))


# ── 18-22: card validation, rendering, invariants ────────────────────────────

class TestDecisionCard(unittest.TestCase):
    def test_18_decision_card_validates(self):
        card = sp.build_decision_card(sp._sample_green_context())
        ok, errs = sp.validate_decision_card(card)
        self.assertTrue(ok, f"errors: {errs}")

    def test_19_markdown_includes_recommendation_risk_undo(self):
        card = sp.build_decision_card(sp._sample_green_context())
        md = sp.render_decision_card_markdown(card)
        self.assertIn("Recommendation", md)
        self.assertIn("Risk", md.replace("risk_level", "Risk"))  # tolerant
        self.assertIn("Undo Plan", md)

    def test_20_wipe_computer_never_approve(self):
        card = sp.build_decision_card(sp._sample_wipe_computer_context())
        self.assertNotEqual(card["recommendation"], sp.APPROVE_RECOMMENDED)
        self.assertIn(card["recommendation"], (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))

    def test_21_safe_to_execute_now_always_false(self):
        for ctx_fn in (
            sp._sample_green_context, sp._sample_yellow_context,
            sp._sample_red_context, sp._sample_wipe_computer_context,
        ):
            with self.subTest(ctx=ctx_fn.__name__):
                card = sp.build_decision_card(ctx_fn())
                self.assertIs(card["safe_to_execute_now"], False)

    def test_22_serge_should_need_to_review_code_false(self):
        for ctx_fn in (
            sp._sample_green_context, sp._sample_yellow_context,
            sp._sample_red_context, sp._sample_wipe_computer_context,
        ):
            with self.subTest(ctx=ctx_fn.__name__):
                card = sp.build_decision_card(ctx_fn())
                self.assertIs(card["serge_should_need_to_review_code"], False)


# ── 23: Aider Tutor Mode roadmap content ─────────────────────────────────────

class TestAiderTutorRoadmap(unittest.TestCase):
    def test_23_roadmap_says_aider_teaches_no_blind_edit(self):
        roadmap = sp.load_aider_tutor_roadmap(_PROJECT_ROOT)
        core_rule = (roadmap.get("core_rule") or "").lower()
        self.assertIn("teach", core_rule)
        self.assertTrue(
            "blind" in core_rule or "not let aider" in core_rule
            or "does not let aider" in core_rule,
            f"core_rule must say Aider does not blindly edit Luna: {core_rule!r}"
        )
        forbidden = roadmap.get("forbidden_behavior") or []
        # Must explicitly forbid direct/blind Aider edits.
        forbidden_blob = " ".join(forbidden).lower()
        self.assertTrue(
            "aider" in forbidden_blob and ("edit" in forbidden_blob or "rewrite" in forbidden_blob),
            f"forbidden_behavior must mention restrictions on Aider edits: {forbidden!r}"
        )


# ── 24-27: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run_cli(self, *args):
        import subprocess
        cmd = [sys.executable, "-m", "luna_modules.luna_serge_policy"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_24_cli_self_test_returns_0(self):
        rc, out, err = self._run_cli("--self-test")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        self.assertIn("PASS", out)

    def test_25_cli_sample_green_returns_0(self):
        rc, out, err = self._run_cli("--sample-green-card")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        data = json.loads(out)
        self.assertEqual(data["recommendation"], sp.APPROVE_RECOMMENDED)
        self.assertIs(data["safe_to_execute_now"], False)

    def test_26_cli_sample_wipe_returns_0_not_approve(self):
        rc, out, err = self._run_cli("--sample-wipe-computer-card")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        data = json.loads(out)
        self.assertNotEqual(data["recommendation"], sp.APPROVE_RECOMMENDED)
        self.assertIn(data["recommendation"], (sp.SERGE_ONLY, sp.DO_NOT_APPROVE))

    def test_27_cli_print_aider_tutor_roadmap_returns_0(self):
        rc, out, err = self._run_cli("--print-aider-tutor-roadmap")
        self.assertEqual(rc, 0, f"stdout={out}\nstderr={err}")
        data = json.loads(out)
        self.assertEqual(data["status"], "planned_after_safety_chain")


# ── 28-30: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_serge_policy.py").read_text(encoding="utf-8")

    def test_28_no_external_api_imports(self):
        for bad in ("import requests", "import openai", "import anthropic",
                    "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_29_no_aider_invocation(self):
        # No actual aider import or aider subprocess call.
        self.assertNotIn("import aider", self._src)
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [], f"aider subprocess calls: {aider_calls}")

    def test_30_no_dangerous_commands_in_source(self):
        lower_src = self._src.lower()
        for bad in ("pip install", "taskkill", "git reset"):
            found_idx = lower_src.find(bad)
            while found_idx != -1:
                line_start = lower_src.rfind("\n", 0, found_idx) + 1
                line_end = lower_src.find("\n", found_idx)
                line = lower_src[line_start:(line_end if line_end != -1 else len(lower_src))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"Found '{bad}' in subprocess.run: {line!r}"
                )
                self.assertFalse(
                    "os.system" in line and bad in line,
                    f"Found '{bad}' in os.system: {line!r}"
                )
                found_idx = lower_src.find(bad, found_idx + 1)


# ── 31-33: write paths and fallback ──────────────────────────────────────────

class TestWritePathsAndFallback(unittest.TestCase):
    def test_31_card_writes_only_under_temp_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            card = sp.build_decision_card(sp._sample_green_context())
            written = sp.write_decision_card(pdir, card)
            pdir_resolved = pdir.resolve()
            for key, p in written.items():
                pp = Path(p)
                self.assertTrue(
                    str(pp.resolve()).startswith(str(pdir_resolved)),
                    f"{pp} escapes temp project"
                )
                self.assertIn("memory", str(pp))

    def test_32_malformed_policy_falls_back_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Write malformed JSON.
            (pdir / "memory" / "luna_serge_standing_approval_policy.json").write_text(
                "this is not json {{}", encoding="utf-8"
            )
            (pdir / "memory" / "luna_super_ai_north_star.json").write_text(
                "still not json", encoding="utf-8"
            )
            (pdir / "memory" / "luna_aider_tutor_mode_roadmap.json").write_text(
                "broken", encoding="utf-8"
            )
            sap = sp.load_standing_approval_policy(pdir)
            ns = sp.load_north_star_policy(pdir)
            roadmap = sp.load_aider_tutor_roadmap(pdir)
            # Must still return safe defaults with hard rules.
            self.assertTrue(sap["serge_is_not_expected_to_review_code_line_by_line"])
            self.assertTrue(sap["non_delegable_always_waits_for_serge"])
            self.assertEqual(sap.get("_source"), "module_fallback")
            self.assertIn("north_star", ns)
            self.assertEqual(ns.get("_source"), "module_fallback")
            self.assertIn("teach", (roadmap.get("core_rule") or "").lower())

    def test_33_self_test_returns_0(self):
        rc = sp.self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
