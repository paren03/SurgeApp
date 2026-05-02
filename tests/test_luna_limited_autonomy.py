"""Phase 5K tests: luna_limited_autonomy.

Stdlib unittest only. All tests run against TemporaryDirectory fixtures so
they never modify the live D:\\SurgeApp tree.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_limited_autonomy import (  # noqa: E402
    ALLOWED_TASK_CLASSES,
    FORBIDDEN_TASK_CLASSES,
    SCHEMA_VERSION,
    _SOURCE_FILES_TO_PROTECT,
    acquire_autonomy_lock,
    build_autonomy_cycle_plan,
    build_overnight_brief,
    build_runtime_context,
    check_git_clean,
    check_operator_stop,
    check_verifier_clean,
    classify_allowed_task_classes,
    evaluate_task_with_gate,
    load_autonomy_tiers,
    load_overnight_policy,
    release_autonomy_lock,
    render_cycle_report_markdown,
    run_allowed_foundation_task,
    run_limited_autonomy_cycle,
    run_limited_autonomy_loop,
    self_test,
    write_cycle_report,
)


def _seed_clean_verifier(td: Path) -> None:
    (td / "logs").mkdir(parents=True, exist_ok=True)
    (td / "logs" / "luna_post_repair_verify_20260101_000000.txt").write_text(
        "[PASS] No hard failures found.\n[PASS] No warnings found.\n",
        encoding="utf-8",
    )


def _seed_min_project(td: Path) -> None:
    (td / "memory").mkdir(parents=True, exist_ok=True)
    _seed_clean_verifier(td)


class _PolicyTierTests(unittest.TestCase):

    def test_01_policy_loads_with_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            pol = load_overnight_policy(td)
            self.assertEqual(pol["schema_version"], 1)
            self.assertFalse(pol["allow_code_edits"])
            self.assertFalse(pol["allow_aider"])
            self.assertFalse(pol["allow_installs"])
            self.assertIn("read_only_health_check", pol["allowed_task_classes"])

    def test_02_tiers_load_with_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            tiers = load_autonomy_tiers(td)
            self.assertIn("tiers", tiers)
            self.assertEqual(tiers["active_tier_max_for_phase_5k"], 1)
            names = [t.get("name") for t in tiers["tiers"]]
            self.assertIn("read_only", names)
            self.assertIn("reports_and_memory_refresh", names)


class _LockTests(unittest.TestCase):

    def test_03_lock_acquire_release(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            r = acquire_autonomy_lock(td)
            self.assertTrue(r["acquired"])
            rel = release_autonomy_lock(td, lock_id=r["lock_id"])
            self.assertTrue(rel["released"])

    def test_04_fresh_lock_blocks_overlapping_run(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            r1 = acquire_autonomy_lock(td)
            self.assertTrue(r1["acquired"])
            r2 = acquire_autonomy_lock(td)
            self.assertFalse(r2["acquired"])
            self.assertEqual(r2["reason"], "fresh_lock")

    def test_05_stale_lock_can_be_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            r1 = acquire_autonomy_lock(td, stale_seconds=1)
            self.assertTrue(r1["acquired"])
            time.sleep(1.2)
            r2 = acquire_autonomy_lock(td, stale_seconds=1)
            self.assertTrue(r2["acquired"])


class _OperatorStopTests(unittest.TestCase):

    def test_06_operator_stop_blocks_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            (td / "LUNA_STOP_NOW.flag").write_text("stop", encoding="utf-8")
            rep = run_limited_autonomy_cycle(td, dry_run=True, write_report=False)
            self.assertFalse(rep["safe_to_continue"])
            self.assertFalse(rep["safe_to_run_overnight_readonly"])
            self.assertTrue(any("operator_stop" in b for b in rep["blockers"]))


class _GitClassifyTests(unittest.TestCase):

    def test_07_git_dirty_blocks_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            subprocess.run(["git", "init"], cwd=str(td), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=str(td), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=str(td), check=True, capture_output=True)
            (td / "tracked.py").write_text("x=1\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.py"], cwd=str(td), check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "init"], cwd=str(td), check=True, capture_output=True)
            (td / "tracked.py").write_text("x=2\n", encoding="utf-8")
            ctx = build_runtime_context(td)
            self.assertFalse((ctx.get("git") or {}).get("tracked_dirty_clean"))
            cls = classify_allowed_task_classes(ctx, load_overnight_policy(td))
            self.assertTrue(any("git_dirty" in b for b in cls["blockers"]))

    def test_08_allowed_list_excludes_code_edits(self) -> None:
        for forbidden in FORBIDDEN_TASK_CLASSES:
            self.assertNotIn(forbidden, ALLOWED_TASK_CLASSES)

    def test_09_forbidden_class_refused(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            r = run_allowed_foundation_task(td, "code_edit", dry_run=True)
            self.assertEqual(r["status"], "blocked")


class _RuntimeContextTests(unittest.TestCase):

    def test_10_context_degrades_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            ctx = build_runtime_context(td)
            self.assertIn("git", ctx)
            self.assertIn("verifier", ctx)
            self.assertIn("operator_stop", ctx)
            # All keys exist; no exception even with empty fixture.


class _CyclePlanTests(unittest.TestCase):

    def test_11_cycle_plan_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            plan = build_autonomy_cycle_plan(td, goal="g", dry_run=True)
            for k in (
                "schema_version", "cycle_id", "created_at", "goal", "dry_run",
                "allowed_task_classes", "forbidden_task_classes",
                "selected_tasks", "skipped_tasks", "approval_required",
                "blockers", "expected_artifacts", "safety_checks",
                "exit_criteria", "max_runtime_seconds", "one_task_at_a_time",
            ):
                self.assertIn(k, plan)

    def test_12_cycle_plan_one_task_at_a_time(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            plan = build_autonomy_cycle_plan(td, goal="g", dry_run=True)
            self.assertTrue(plan["one_task_at_a_time"])


class _GateEvalTests(unittest.TestCase):

    def test_13_evaluate_low_risk_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            low = {
                "plan_id": "low",
                "title": "tiny edit",
                "actor": "test",
                "target_files": ["luna_modules/luna_logging.py"],
                "line_ranges": {"luna_modules/luna_logging.py": [[60, 65]]},
                "action_type": "edit",
                "expected_diff_type": "small_edit",
                "risk_level": "low",
                "approval_tier": 2,
                "diff_stats": {"files_changed": 1, "insertions": 1, "deletions": 1},
                "verification_commands": ["python -m py_compile luna_modules/luna_logging.py"],
                "rollback_plan": "git checkout HEAD -- luna_modules/luna_logging.py",
                "install_commands": [],
                "external_network": False,
                "touches_personality_or_goals": False,
                "touches_memory_content": False,
                "touches_runtime_queue": False,
                "operator_approved": False,
            }
            d = evaluate_task_with_gate(td, low)
            self.assertIn(d["decision"], ("allow", "needs_approval", "deny"))

    def test_14_evaluate_high_risk_proposal_needs_approval_or_deny(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            high = {
                "plan_id": "high",
                "title": "edit worker.py",
                "actor": "test",
                "target_files": ["worker.py"],
                "line_ranges": {"worker.py": [[12200, 12210]]},
                "action_type": "edit",
                "expected_diff_type": "small_edit",
                "risk_level": "high",
                "approval_tier": 4,
                "diff_stats": {"files_changed": 1, "insertions": 1, "deletions": 1},
                "verification_commands": ["python -m py_compile worker.py"],
                "rollback_plan": "git checkout HEAD -- worker.py",
                "install_commands": [],
                "external_network": False,
                "touches_personality_or_goals": False,
                "touches_memory_content": False,
                "touches_runtime_queue": False,
                "operator_approved": False,
            }
            d = evaluate_task_with_gate(td, high)
            self.assertIn(d["decision"], ("needs_approval", "deny"))


class _FoundationTaskTests(unittest.TestCase):

    def test_15_health_check_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_allowed_foundation_task(td, "read_only_health_check", dry_run=True)
            self.assertEqual(r["status"], "ok")
            self.assertIn("verifier_clean", r["details"])

    def test_16_file_map_refresh_dry_run_writes_no_maps(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_allowed_foundation_task(td, "file_map_refresh", dry_run=True)
            self.assertEqual(r["status"], "ok")
            self.assertFalse((td / "memory" / "luna_file_map.json").exists())

    def test_17_memory_index_refresh_dry_run_writes_no_index(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_allowed_foundation_task(td, "memory_index_refresh", dry_run=True)
            self.assertEqual(r["status"], "ok")
            self.assertFalse((td / "memory" / "luna_memory_index.json").exists())

    def test_18_task_graph_plan_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_allowed_foundation_task(td, "task_graph_plan_only", dry_run=True)
            self.assertIn(r["status"], ("ok", "skipped"))
            if r["status"] == "ok":
                self.assertIn("graph_id", r["details"])

    def test_19_sandbox_self_test(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_allowed_foundation_task(td, "sandbox_self_test", dry_run=True)
            self.assertIn(r["status"], ("ok", "skipped"))


class _CycleRunTests(unittest.TestCase):

    def test_20_dry_run_returns_report(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            r = run_limited_autonomy_cycle(td, dry_run=True, write_report=False)
            self.assertEqual(r["schema_version"], SCHEMA_VERSION)
            self.assertFalse(r["safe_to_run_overnight_code_edits"])

    def test_21_dry_run_writes_no_source_files(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            for rel in ("worker.py", "aider_bridge.py", "luna_guardian.py"):
                (td / rel).write_text(f"# {rel}\n", encoding="utf-8")
            before = {
                rel: hashlib.sha256((td / rel).read_bytes()).hexdigest()
                for rel in ("worker.py", "aider_bridge.py", "luna_guardian.py")
            }
            run_limited_autonomy_cycle(td, dry_run=True, write_report=True)
            after = {
                rel: hashlib.sha256((td / rel).read_bytes()).hexdigest()
                for rel in ("worker.py", "aider_bridge.py", "luna_guardian.py")
            }
            self.assertEqual(before, after)

    def test_22_generated_artifacts_under_memory_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            run_limited_autonomy_cycle(td, dry_run=True, write_report=True)
            for rel in (
                "memory/luna_limited_autonomy_report.json",
                "memory/luna_limited_autonomy_report.md",
                "memory/luna_limited_autonomy_state.json",
                "memory/luna_limited_autonomy_cycle.jsonl",
                "memory/luna_overnight_brief.md",
                "memory/luna_recommended_next_actions.json",
            ):
                self.assertTrue((td / rel).is_file(), f"missing {rel}")


class _OvernightLoopTests(unittest.TestCase):

    def test_23_loop_honors_max_cycles(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            reports = run_limited_autonomy_loop(td, max_cycles=2, sleep_seconds=0, dry_run=True)
            self.assertLessEqual(len(reports), 2)
            self.assertGreaterEqual(len(reports), 1)

    def test_24_loop_honors_stop_file(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            (td / "memory" / "limited_autonomy.stop").write_text("stop", encoding="utf-8")
            reports = run_limited_autonomy_loop(td, max_cycles=3, sleep_seconds=0, dry_run=True)
            self.assertGreaterEqual(len(reports), 1)
            first = reports[0]
            if first.get("halted"):
                self.assertEqual(first.get("reason"), "operator_stop")
            else:
                self.assertTrue(any("operator_stop" in b for b in first.get("blockers", [])))


class _ReportRenderTests(unittest.TestCase):

    def test_25_markdown_includes_safe_to_run_overnight_readonly(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            rep = run_limited_autonomy_cycle(td, dry_run=True, write_report=False)
            md = render_cycle_report_markdown(rep)
            self.assertIn("safe_to_run_overnight_readonly", md)
            self.assertIn("Luna Limited Autonomy Cycle Report", md)

    def test_26_report_always_says_safe_to_run_code_edits_false(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            rep = run_limited_autonomy_cycle(td, dry_run=True, write_report=False)
            self.assertIs(rep["safe_to_run_overnight_code_edits"], False)


class _SelfTestTests(unittest.TestCase):

    def test_27_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_limited_autonomy", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )

    def test_28_cli_self_test_zero(self) -> None:
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_29_cli_run_once_dry_run_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            r = self._run("--run-once", "--dry-run", "--project-dir", td_str)
            self.assertEqual(r.returncode, 0, r.stderr)
            try:
                payload = json.loads(r.stdout)
                self.assertFalse(payload.get("safe_to_run_overnight_code_edits"))
            except ValueError:
                self.fail(f"non-json stdout: {r.stdout!r}")


class _NoNetworkTests(unittest.TestCase):

    def test_30_no_network_imports(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_limited_autonomy.py").read_text(encoding="utf-8")
        self.assertNotIn("import socket", text)
        self.assertNotIn("import urllib", text)
        self.assertNotIn("import requests", text)
        self.assertNotIn("http.client", text)


class _NoUnsafeCommandsTests(unittest.TestCase):

    def test_31_source_has_no_unsafe_commands(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_limited_autonomy.py").read_text(encoding="utf-8")
        for token in (
            "taskkill",
            "Stop-Process",
            "pip install",
            "git reset",
            "git clean",
            "Remove-Item",
            "rm -rf",
        ):
            self.assertNotIn(token, text, f"forbidden token present: {token!r}")


class _VerifierBlockTests(unittest.TestCase):

    def test_32_verifier_unclean_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            (td / "logs").mkdir(parents=True)
            (td / "logs" / "luna_post_repair_verify_20260101_000000.txt").write_text(
                "[FAIL] something is broken\n",
                encoding="utf-8",
            )
            ctx = build_runtime_context(td)
            cls = classify_allowed_task_classes(ctx, load_overnight_policy(td))
            self.assertTrue(any("verifier_not_clean" in b for b in cls["blockers"]))


class _ResourceBlockTests(unittest.TestCase):

    def test_33_resource_blocked_skips_high_intensity(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            ctx = build_runtime_context(td)
            ctx["resource_mode"] = "blocked"
            ctx["resource_blockers"] = ["disk free 0GB"]
            cls = classify_allowed_task_classes(ctx, load_overnight_policy(td))
            self.assertEqual(cls["allowed"], [])
            self.assertTrue(any("resource_mode_blocked" in b for b in cls["blockers"]))


class _RecommendedActionsTests(unittest.TestCase):

    def test_34_recommended_next_actions_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            rep = run_limited_autonomy_cycle(td, dry_run=True, write_report=False)
            actions = rep.get("recommended_next_actions") or []
            self.assertLessEqual(len(actions), 8)
            wait_action = [a for a in actions if a.get("action") == "wait_for_phase_5L_council_before_any_code_edits"]
            self.assertEqual(len(wait_action), 1)


class _PathSafetyTests(unittest.TestCase):

    def test_35_generated_paths_stay_under_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed_min_project(td)
            rep = run_limited_autonomy_cycle(td, dry_run=True, write_report=True)
            written = write_cycle_report(td, rep)
            for v in written.values():
                Path(v).resolve().relative_to(td.resolve())


if __name__ == "__main__":
    unittest.main(verbosity=2)
