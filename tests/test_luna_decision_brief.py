"""Phase 5VW tests: Luna Morning Decision Brief + Advisory Soak.

30+ tests covering extraction, normalization, aggregation, digest building,
brief building, classification, markdown, write paths, soak cycles, and CLI.
All tests use TemporaryDirectory. No real project files are modified.
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

import luna_modules.luna_decision_brief as db


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    return tmp


def _seed_router_card(pdir: Path, recommendation: str = "APPROVE_RECOMMENDED",
                      goal: str = "test", action_type: str = "generated_artifact",
                      tier: int = 1) -> None:
    rep = {
        "schema_version": 1, "request_id": "rq_x", "goal": goal,
        "action_type": action_type, "approval_tier_required": tier,
        "routing_decision": "not_required", "safe_to_execute_now": False,
        "decision_card": {
            "schema_version": 1, "card_id": "c_x",
            "recommendation": recommendation, "risk_level": "low",
            "goal_alignment": "aligned", "safe_to_execute_now": False,
            "plain_english_final_recommendation": f"plain english for {recommendation}",
            "goal": goal, "action_type": action_type, "risk_tier": tier,
        },
        "decision_card_recommendation": recommendation,
        "serge_plain_english_summary": f"plain english for {recommendation}",
    }
    (pdir / "memory" / "luna_approval_router_report.json").write_text(
        json.dumps(rep), encoding="utf-8"
    )


def _seed_readiness_actions(pdir: Path, recs: list[str]) -> None:
    actions = []
    for i, r in enumerate(recs):
        actions.append({
            "action_id": f"a{i}",
            "action_type": "process_kill" if r == "SERGE_ONLY" else "medium_code_edit",
            "risk_tier": 5 if r == "SERGE_ONLY" else 3,
            "safe_to_execute_now": False,
            "decision_card_recommendation": r,
            "decision_card": {
                "card_id": f"c{i}",
                "recommendation": r,
                "safe_to_execute_now": False,
                "plain_english_final_recommendation": f"pe {r}",
            },
        })
    rep = {
        "schema_version": 1, "advisory_only": True,
        "guardian_enforcing_live": False,
        "ready_for_live_guardian_enforcement": False,
        "actions": actions,
        "decision_card_summary": {
            "approve_recommended": sum(1 for r in recs if r == "APPROVE_RECOMMENDED"),
            "wait_for_more_evidence": sum(1 for r in recs if r == "WAIT_FOR_MORE_EVIDENCE"),
            "do_not_approve": sum(1 for r in recs if r == "DO_NOT_APPROVE"),
            "serge_only": sum(1 for r in recs if r == "SERGE_ONLY"),
            "unavailable": 0,
        },
    }
    (pdir / "memory" / "luna_guardian_readiness_report.json").write_text(
        json.dumps(rep), encoding="utf-8"
    )


# ── 1-5: extraction and normalization ────────────────────────────────────────

class TestExtraction(unittest.TestCase):
    def test_01_extract_single_decision_card(self):
        report = {
            "decision_card": {"recommendation": "APPROVE_RECOMMENDED",
                              "card_id": "c1", "goal": "g1"},
            "decision_card_recommendation": "APPROVE_RECOMMENDED",
        }
        cards = db.extract_decision_cards(report, source="r1")
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["recommendation"], "APPROVE_RECOMMENDED")
        self.assertEqual(cards[0]["source"], "r1")

    def test_02_extract_nested_router_like_report(self):
        report = {
            "decision_card": {"recommendation": "WAIT_FOR_MORE_EVIDENCE"},
            "decision_card_recommendation": "WAIT_FOR_MORE_EVIDENCE",
            "actions": [],
        }
        cards = db.extract_decision_cards(report, source="router")
        self.assertEqual(len(cards), 1)

    def test_03_extract_cards_from_enforcer_action_list(self):
        report = {
            "advisory_only": True,
            "actions": [
                {"decision_card_recommendation": "SERGE_ONLY",
                 "decision_card": {"recommendation": "SERGE_ONLY"}},
                {"decision_card_recommendation": "DO_NOT_APPROVE",
                 "decision_card": {"recommendation": "DO_NOT_APPROVE"}},
            ],
        }
        cards = db.extract_decision_cards(report, source="enforcer")
        self.assertEqual(len(cards), 2)
        recs = [c["recommendation"] for c in cards]
        self.assertIn("SERGE_ONLY", recs)
        self.assertIn("DO_NOT_APPROVE", recs)

    def test_04_normalize_recommendation_variants(self):
        cases = {
            "APPROVE_RECOMMENDED": "APPROVE_RECOMMENDED",
            "approve": "APPROVE_RECOMMENDED",
            "APPROVE": "APPROVE_RECOMMENDED",
            "wait": "WAIT_FOR_MORE_EVIDENCE",
            "WAIT_FOR_MORE_EVIDENCE": "WAIT_FOR_MORE_EVIDENCE",
            "deny": "DO_NOT_APPROVE",
            "DO_NOT_APPROVE": "DO_NOT_APPROVE",
            "serge": "SERGE_ONLY",
            "SERGE_ONLY": "SERGE_ONLY",
            "UNAVAILABLE": "UNKNOWN",
            "": "UNKNOWN",
            "garbage_string": "UNKNOWN",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(db.normalize_recommendation(raw), expected)

    def test_05_aggregate_counts(self):
        cards = [
            {"recommendation": "APPROVE_RECOMMENDED"},
            {"recommendation": "APPROVE_RECOMMENDED"},
            {"recommendation": "WAIT_FOR_MORE_EVIDENCE"},
            {"recommendation": "DO_NOT_APPROVE"},
            {"recommendation": "SERGE_ONLY"},
            {"recommendation": "UNKNOWN"},
        ]
        counts = db.aggregate_decision_cards(cards)
        self.assertEqual(counts["approve_recommended"], 2)
        self.assertEqual(counts["wait_for_more_evidence"], 1)
        self.assertEqual(counts["do_not_approve"], 1)
        self.assertEqual(counts["serge_only"], 1)
        self.assertEqual(counts["unknown"], 1)


# ── 6-10: digest and brief building ──────────────────────────────────────────

class TestDigestAndBrief(unittest.TestCase):
    def test_06_missing_artifacts_degrade_gracefully(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # No artifacts seeded; digest should still build.
            digest = db.build_decision_digest(pdir)
            self.assertEqual(digest["counts"]["approve_recommended"], 0)
            self.assertGreater(len(digest["missing_artifacts"]), 0)
            self.assertIs(digest["safe_to_execute_now"], False)

    def test_07_build_digest_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            digest = db.build_decision_digest(pdir)
            self.assertIn("counts", digest)
            self.assertIn("top_items", digest)
            self.assertIn("files_checked", digest)
            self.assertEqual(digest["counts"]["approve_recommended"], 1)

    def test_08_morning_brief_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            ok, errs = db.validate_morning_brief(brief)
            self.assertTrue(ok, f"errors: {errs}")

    def test_09_markdown_includes_overall_and_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            md = db.render_morning_brief_markdown(brief)
            self.assertIn("Overall Recommendation", md)
            self.assertIn("Decision-Card Counts", md)
            self.assertIn("approve_recommended", md)

    def test_10_green_cards_produce_continue_safe_routine(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            self.assertEqual(brief["overall_recommendation"], "continue_safe_routine")


class TestClassifications(unittest.TestCase):
    def test_11_wait_cards_produce_wait_for_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_readiness_actions(pdir, ["WAIT_FOR_MORE_EVIDENCE", "WAIT_FOR_MORE_EVIDENCE"])
            brief = db.build_morning_decision_brief(pdir)
            self.assertEqual(brief["overall_recommendation"], "wait_for_evidence")

    def test_12_red_cards_produce_do_not_approve(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_readiness_actions(pdir, ["DO_NOT_APPROVE"])
            brief = db.build_morning_decision_brief(pdir)
            self.assertEqual(brief["overall_recommendation"], "do_not_approve")

    def test_13_serge_only_cards_produce_serge_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_readiness_actions(pdir, ["SERGE_ONLY", "APPROVE_RECOMMENDED"])
            brief = db.build_morning_decision_brief(pdir)
            self.assertEqual(brief["overall_recommendation"], "serge_only")

    def test_14_no_cards_produce_no_actions(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            brief = db.build_morning_decision_brief(pdir)
            self.assertEqual(brief["overall_recommendation"], "no_actions")


# ── 15-17: invariants and write paths ────────────────────────────────────────

class TestInvariants(unittest.TestCase):
    def test_15_hard_safety_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            self.assertIs(brief["advisory_only"], True)
            self.assertIs(brief["safe_to_execute_now"], False)
            self.assertIs(brief["safe_to_apply_real_project"], False)
            self.assertIs(brief["guardian_enforcing_live"], False)

    def test_16_next_safe_action_is_plain_english(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            nsa = brief["next_safe_action"]
            self.assertGreater(len(nsa), 10)
            self.assertLess(len(nsa), 1000)
            # No code-style identifiers as primary content (loose check).
            self.assertNotIn("luna_modules.", nsa)

    def test_17_write_brief_under_temp_memory_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            written = db.write_morning_brief(pdir, brief)
            pdir_resolved = pdir.resolve()
            for key, p in written.items():
                with self.subTest(key=key):
                    pp = Path(p)
                    self.assertTrue(
                        str(pp.resolve()).startswith(str(pdir_resolved)),
                        f"{pp} escapes project"
                    )
                    self.assertIn("memory", str(pp))


# ── 18-20: soak harness ──────────────────────────────────────────────────────

class TestSoak(unittest.TestCase):
    def test_18_advisory_soak_runs_bounded_cycles(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = db.run_advisory_soak(pdir, cycles=2, sleep_seconds=0)
            self.assertEqual(report["cycles"], 2)
            self.assertEqual(len(report["cycle_results"]), 2)

    def test_18b_soak_clamps_max_cycles(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = db.run_advisory_soak(pdir, cycles=10000, sleep_seconds=0)
            self.assertLessEqual(report["cycles"], 20)

    def test_19_soak_report_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = db.run_advisory_soak(pdir, cycles=2, sleep_seconds=0)
            ok, errs = db.validate_soak_report(report)
            self.assertTrue(ok, f"errors: {errs}")

    def test_20_soak_never_sets_execution_true(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            report = db.run_advisory_soak(pdir, cycles=3, sleep_seconds=0)
            self.assertIs(report["safe_to_execute_now"], False)
            self.assertIs(report["safe_to_apply_real_project"], False)
            self.assertIs(report["guardian_enforcing_live"], False)
            for c in report["cycle_results"]:
                self.assertIs(c["safe_to_execute_now"], False)
                self.assertIs(c["safe_to_apply_real_project"], False)
                self.assertIs(c["guardian_enforcing_live"], False)


# ── 21-23: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run(self, *args):
        cmd = [sys.executable, "-m", "luna_modules.luna_decision_brief"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                              cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_21_cli_self_test_returns_0(self):
        rc, out, err = self._run("--self-test")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        self.assertIn("PASS", out)

    def test_22_cli_build_returns_0(self):
        rc, out, err = self._run("--build")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertIs(data["safe_to_apply_real_project"], False)
        self.assertIn("counts", data)
        self.assertIn("overall_recommendation", data)

    def test_23_cli_soak_returns_0(self):
        rc, out, err = self._run("--soak", "--cycles", "2", "--sleep-seconds", "0")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertIs(data["advisory_only"], True)
        self.assertIs(data["safe_to_execute_now"], False)
        self.assertEqual(data["cycles"], 2)


# ── 24-28: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_decision_brief.py").read_text(encoding="utf-8")

    def test_24_no_aider_invocation(self):
        self.assertNotIn("import aider", self._src)
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [])

    def test_25_no_external_api_imports(self):
        for bad in ("import requests", "import openai", "import anthropic",
                    "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_26_no_dangerous_subprocess_commands(self):
        lower = self._src.lower()
        for bad in ("pip install", "taskkill", "git reset", "delete_queue"):
            found = lower.find(bad)
            while found != -1:
                line_start = lower.rfind("\n", 0, found) + 1
                line_end = lower.find("\n", found)
                line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"Found '{bad}' in subprocess.run: {line!r}"
                )
                self.assertFalse(
                    "os.system" in line and bad in line,
                    f"Found '{bad}' in os.system: {line!r}"
                )
                found = lower.find(bad, found + 1)

    def test_27_no_source_writes_outside_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Seed a fake "source file" outside memory and confirm it stays untouched.
            src_file = pdir / "luna_modules" / "fake_source.py"
            src_file.parent.mkdir(parents=True, exist_ok=True)
            original = "# original content\n"
            src_file.write_text(original, encoding="utf-8")
            _seed_router_card(pdir, "APPROVE_RECOMMENDED")
            brief = db.build_morning_decision_brief(pdir)
            db.write_morning_brief(pdir, brief)
            self.assertEqual(src_file.read_text(encoding="utf-8"), original)

    def test_28_generated_paths_never_escape_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            brief = db.build_morning_decision_brief(pdir)
            written = db.write_morning_brief(pdir, brief)
            pdir_resolved = pdir.resolve()
            for key, p in written.items():
                with self.subTest(key=key):
                    self.assertTrue(
                        str(Path(p).resolve()).startswith(str(pdir_resolved))
                    )


# ── 29-30: self_test rc and policy fallback ──────────────────────────────────

class TestSelfTestAndFallback(unittest.TestCase):
    def test_29_self_test_returns_0(self):
        rc = db.self_test()
        self.assertEqual(rc, 0)

    def test_30_policy_malformed_json_falls_back_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            (pdir / "memory" / "luna_decision_brief_policy.json").write_text(
                "not valid json {{}", encoding="utf-8"
            )
            (pdir / "memory" / "luna_advisory_soak_policy.json").write_text(
                "broken", encoding="utf-8"
            )
            policy = db.load_decision_brief_policy(pdir)
            self.assertIs(policy["advisory_only"], True)
            self.assertIs(policy["safe_to_execute_now"], False)
            self.assertEqual(policy.get("_source"), "module_fallback")
            # Soak should still run with default policy.
            report = db.run_advisory_soak(pdir, cycles=1, sleep_seconds=0)
            self.assertIs(report["safe_to_execute_now"], False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
