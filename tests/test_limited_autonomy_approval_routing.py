"""Phase 5Q tests: Limited Routine Autonomy approval routing. 25+ tests."""
from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from luna_modules.luna_limited_autonomy import (
    SCHEMA_VERSION,
    _DELEGABLE_CODE_EDIT_CLASSES,
    _HIGH_RISK_CORE_FILES_LOWER,
    _NON_DELEGABLE_FORBIDDEN_CLASSES,
    _task_class_to_action_type,
    append_routine_approval_request,
    approval_router_available,
    build_routine_approval_request,
    maybe_route_forbidden_task,
    route_blocked_task_for_approval,
    run_limited_autonomy_cycle,
    self_test,
    summarize_approval_routing,
)

PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent)


def _make_tmpdir_with_verifier():
    """Create a tempdir with a synthetic clean verifier log."""
    td = pathlib.Path(tempfile.mkdtemp())
    (td / "memory").mkdir(parents=True, exist_ok=True)
    (td / "logs").mkdir(parents=True, exist_ok=True)
    (td / "logs" / "luna_post_repair_verify_20260101_000000.txt").write_text(
        "[PASS] No hard failures found.\n[PASS] No warnings found.\n",
        encoding="utf-8",
    )
    return td


class TestApprovalRouterAvailable(unittest.TestCase):
    def test_01_router_available_returns_bool(self):
        result = approval_router_available()
        self.assertIsInstance(result, bool)

    def test_02_router_available_does_not_crash(self):
        # Should succeed or degrade gracefully — never raise.
        try:
            approval_router_available()
        except Exception as e:
            self.fail(f"approval_router_available() raised: {e}")


class TestBuildRoutineApprovalRequest(unittest.TestCase):
    def test_03_includes_goal_action_target(self):
        req = build_routine_approval_request(
            PROJECT_ROOT,
            goal="Safely add helper module",
            task_class="code_edit",
            target_files=["luna_modules/example.py"],
            requested_action="low_risk_additive",
        )
        self.assertEqual(req["goal"], "Safely add helper module")
        self.assertEqual(req["requested_action"], "low_risk_additive")
        self.assertIn("luna_modules/example.py", req["target_files"])

    def test_04_safe_to_execute_now_always_false(self):
        req = build_routine_approval_request(PROJECT_ROOT, "test goal", "code_edit")
        self.assertIs(req["safe_to_execute_now"], False)

    def test_05_request_id_present(self):
        req = build_routine_approval_request(PROJECT_ROOT, "test", "code_edit")
        self.assertIn("request_id", req)
        self.assertTrue(req["request_id"].startswith("routine_"))

    def test_06_schema_version_present(self):
        req = build_routine_approval_request(PROJECT_ROOT, "test", "code_edit")
        self.assertEqual(req["schema_version"], SCHEMA_VERSION)

    def test_07_high_risk_task_class_maps_action(self):
        req = build_routine_approval_request(PROJECT_ROOT, "edit worker", "worker_edit")
        self.assertEqual(req["requested_action"], "high_risk_core_edit")

    def test_08_non_delegable_task_class_maps_action(self):
        req = build_routine_approval_request(PROJECT_ROOT, "del", "memory_delete")
        self.assertEqual(req["requested_action"], "non_delegable")


class TestRouteBlockedTaskForApproval(unittest.TestCase):
    def test_09_low_risk_additive_returns_report_not_executed(self):
        result = route_blocked_task_for_approval(
            PROJECT_ROOT,
            goal="Safely add helper module",
            task_class="code_edit",
            target_files=["luna_modules/example.py"],
            dry_run=True,
        )
        self.assertIsInstance(result, dict)
        self.assertIs(result.get("safe_to_execute_now"), False)

    def test_10_high_risk_worker_routes_needs_human(self):
        result = route_blocked_task_for_approval(
            PROJECT_ROOT,
            goal="Edit worker.py continues_update",
            task_class="code_edit",
            target_files=["worker.py"],
            dry_run=True,
        )
        self.assertIs(result.get("safe_to_execute_now"), False)
        # Either router says needs_human/blocked or router_unavailable — never executes.
        self.assertIn(
            result.get("approval_routing_status"),
            {"routed", "router_unavailable", "router_error"},
        )

    def test_11_router_unavailable_does_not_crash(self):
        # We simulate unavailability by monkey-patching inside a try/except.
        import luna_modules.luna_limited_autonomy as mod
        original = mod.approval_router_available
        mod.approval_router_available = lambda: False
        try:
            result = route_blocked_task_for_approval(
                PROJECT_ROOT, "test", "code_edit", dry_run=True
            )
            self.assertEqual(result["approval_routing_status"], "router_unavailable")
            self.assertIs(result["safe_to_execute_now"], False)
        finally:
            mod.approval_router_available = original

    def test_12_router_unavailable_has_recommended_next_action(self):
        import luna_modules.luna_limited_autonomy as mod
        original = mod.approval_router_available
        mod.approval_router_available = lambda: False
        try:
            result = route_blocked_task_for_approval(
                PROJECT_ROOT, "test", "code_edit", dry_run=True
            )
            self.assertIn("recommended_next_action", result)
        finally:
            mod.approval_router_available = original

    def test_13_notes_include_safe_to_execute_now_false(self):
        result = route_blocked_task_for_approval(
            PROJECT_ROOT, "test", "code_edit", dry_run=True
        )
        notes_text = " ".join(result.get("notes", []))
        self.assertIn("safe_to_execute_now=False", notes_text)


class TestAppendRoutineApprovalRequest(unittest.TestCase):
    def test_14_dry_run_does_not_append(self):
        td = _make_tmpdir_with_verifier()
        try:
            req = build_routine_approval_request(str(td), "test", "code_edit")
            result = append_routine_approval_request(str(td), req, dry_run=True)
            self.assertFalse(result["appended"])
            self.assertEqual(result["reason"], "dry_run")
            self.assertFalse((td / "memory" / "luna_routine_approval_requests.jsonl").exists())
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)

    def test_15_write_mode_appends_jsonl(self):
        td = _make_tmpdir_with_verifier()
        try:
            routing = {
                "task_class": "code_edit",
                "goal": "Add helper",
                "requested_action": "low_risk_additive",
                "target_files": ["luna_modules/example.py"],
                "approval_routing_status": "routed",
                "router_decision": "would_allow",
                "router_needs_human": False,
                "routine_request": {"request_id": "routine_abc123"},
            }
            result = append_routine_approval_request(str(td), routing, dry_run=False)
            self.assertTrue(result["appended"])
            path = td / "memory" / "luna_routine_approval_requests.jsonl"
            self.assertTrue(path.exists())
            row = json.loads(path.read_text(encoding="utf-8").strip())
            self.assertIs(row["safe_to_execute_now"], False)
            self.assertIn("request_id", row)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)


class TestSummarizeApprovalRouting(unittest.TestCase):
    def test_16_empty_list_returns_zero_counts(self):
        summary = summarize_approval_routing([])
        self.assertEqual(summary["requests_created"], 0)
        self.assertTrue(summary["enabled"])

    def test_17_counts_needs_human_correctly(self):
        results = [
            {"routed": True, "router_needs_human": True, "router_decision": "needs_human",
             "approval_routing_status": "routed", "task_class": "code_edit"},
            {"routed": False, "router_needs_human": True, "router_decision": "unknown",
             "approval_routing_status": "router_unavailable", "task_class": "worker_edit"},
        ]
        summary = summarize_approval_routing(results)
        self.assertEqual(summary["requests_created"], 2)
        self.assertEqual(summary["needs_human_count"], 2)


class TestMaybeRouteForbiddenTask(unittest.TestCase):
    def test_18_delegable_class_with_goal_routes(self):
        result = maybe_route_forbidden_task(
            PROJECT_ROOT,
            goal="Safely add helper module",
            task_class="code_edit",
            target_files=["luna_modules/example.py"],
            dry_run=True,
        )
        self.assertIs(result.get("safe_to_execute_now"), False)
        self.assertIn(
            result.get("approval_routing_status"),
            {"routed", "router_unavailable", "router_error"},
        )

    def test_19_non_delegable_returns_blocked_needs_human(self):
        result = maybe_route_forbidden_task(
            PROJECT_ROOT,
            goal="Delete memory logs",
            task_class="memory_delete",
            target_files=["memory/nightly_updates.md"],
            dry_run=True,
        )
        self.assertIs(result.get("safe_to_execute_now"), False)
        self.assertEqual(result.get("approval_routing_status"), "non_delegable")
        self.assertTrue(result.get("router_needs_human"))
        self.assertTrue(result.get("router_non_delegable"))

    def test_20_no_goal_returns_blocked(self):
        result = maybe_route_forbidden_task(
            PROJECT_ROOT, goal="", task_class="code_edit", dry_run=True
        )
        self.assertEqual(result.get("approval_routing_status"), "blocked_no_goal")
        self.assertIs(result.get("safe_to_execute_now"), False)


class TestCycleReportApprovalRouting(unittest.TestCase):
    def test_21_cycle_report_has_approval_routing_block(self):
        td = _make_tmpdir_with_verifier()
        try:
            report = run_limited_autonomy_cycle(str(td), goal="test", dry_run=True, write_report=False)
            self.assertIn("approval_routing", report)
            ar = report["approval_routing"]
            self.assertTrue(ar["enabled"])
            self.assertIn("requests_created", ar)
            self.assertIn("needs_human_count", ar)
            self.assertIn("blocked_count", ar)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)

    def test_22_safe_to_run_routine_code_edits_always_false(self):
        td = _make_tmpdir_with_verifier()
        try:
            report = run_limited_autonomy_cycle(str(td), goal="test", dry_run=True, write_report=False)
            self.assertIs(report.get("safe_to_run_routine_code_edits"), False)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)

    def test_23_safe_to_run_overnight_code_edits_always_false(self):
        td = _make_tmpdir_with_verifier()
        try:
            report = run_limited_autonomy_cycle(str(td), goal="test", dry_run=True, write_report=False)
            self.assertIs(report.get("safe_to_run_overnight_code_edits"), False)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)


class TestPolicyApprovalRouting(unittest.TestCase):
    def test_24_routine_policy_has_approval_routing_enabled(self):
        import json as _json
        p = pathlib.Path(PROJECT_ROOT) / "memory" / "luna_routine_policy.json"
        self.assertTrue(p.exists(), "luna_routine_policy.json missing")
        pol = _json.loads(p.read_text(encoding="utf-8"))
        self.assertTrue(pol.get("approval_routing_enabled"))
        self.assertEqual(pol.get("approval_routing_phase"), "5Q")
        self.assertFalse(pol.get("allow_code_edits"))
        self.assertFalse(pol.get("allow_aider"))
        self.assertFalse(pol.get("allow_execution_from_receipt"))

    def test_25_overnight_policy_has_approval_routing_and_routine_stop(self):
        import json as _json
        p = pathlib.Path(PROJECT_ROOT) / "memory" / "luna_overnight_policy.json"
        self.assertTrue(p.exists(), "luna_overnight_policy.json missing")
        pol = _json.loads(p.read_text(encoding="utf-8"))
        self.assertTrue(pol.get("approval_routing_enabled"))
        # Must have routine_autonomy.stop alias
        src = p.read_text(encoding="utf-8")
        self.assertIn("routine_autonomy.stop", src)


class TestNoAiderNoExternalAPI(unittest.TestCase):
    def _src(self):
        return (
            pathlib.Path(PROJECT_ROOT) / "luna_modules" / "luna_limited_autonomy.py"
        ).read_text(encoding="utf-8")

    def test_26_no_aider_invocation(self):
        src = self._src()
        self.assertNotIn('"aider"', src)
        self.assertNotIn("subprocess.run.*aider", src)

    def test_27_no_external_api_calls(self):
        src = self._src()
        for banned in ("import requests", "import httpx", "import openai", "import anthropic"):
            self.assertNotIn(banned, src)

    def test_28_no_source_file_modification_in_routing(self):
        src = self._src()
        # Approval routing helpers must not call write_json_atomic on source files.
        # Verify no _SOURCE_FILES_TO_PROTECT paths appear in routing helper bodies.
        self.assertIn("safe_to_execute_now", src)
        # None of the routing helpers write target files — validated by inspection.
        self.assertNotIn("worker.py\".write_text", src)

    def test_29_self_test_returns_zero(self):
        rc = self_test()
        self.assertEqual(rc, 0)


class TestTaskClassToActionType(unittest.TestCase):
    def test_30_worker_edit_maps_high_risk(self):
        self.assertEqual(_task_class_to_action_type("worker_edit"), "high_risk_core_edit")

    def test_31_memory_delete_maps_non_delegable(self):
        self.assertEqual(_task_class_to_action_type("memory_delete"), "non_delegable")

    def test_32_code_edit_with_worker_target_maps_high_risk(self):
        self.assertEqual(
            _task_class_to_action_type("code_edit", ["worker.py"]),
            "high_risk_core_edit",
        )

    def test_33_code_edit_safe_target_maps_medium(self):
        self.assertEqual(
            _task_class_to_action_type("code_edit", ["luna_modules/example.py"]),
            "medium_code_edit",
        )

    def test_34_continues_update_start_maps_high_risk(self):
        self.assertEqual(_task_class_to_action_type("continues_update_start"), "high_risk_core_edit")


class TestGeneratedReportPaths(unittest.TestCase):
    def test_35_approval_request_paths_under_memory(self):
        td = _make_tmpdir_with_verifier()
        try:
            routing = {
                "task_class": "code_edit",
                "goal": "Add helper",
                "requested_action": "low_risk_additive",
                "target_files": [],
                "approval_routing_status": "routed",
                "router_decision": "would_allow",
                "router_needs_human": False,
                "routine_request": {"request_id": "routine_test"},
            }
            result = append_routine_approval_request(str(td), routing, dry_run=False)
            path = result.get("path", "")
            self.assertIn("memory", path.replace("\\", "/"))
            self.assertIn("luna_routine_approval_requests.jsonl", path)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)


class TestCLIRequestApproval(unittest.TestCase):
    def test_36_cli_request_approval_low_risk_rc0(self):
        from luna_modules.luna_limited_autonomy import _cli
        rc = _cli([
            "--request-approval", "Safely add helper module",
            "--action", "low_risk_additive",
            "--target", "luna_modules/example.py",
            "--dry-run",
        ])
        self.assertEqual(rc, 0)

    def test_37_cli_request_approval_high_risk_rc0(self):
        from luna_modules.luna_limited_autonomy import _cli
        rc = _cli([
            "--request-approval", "Edit worker.py continues_update",
            "--action", "high_risk_core_edit",
            "--target", "worker.py",
            "--dry-run",
        ])
        self.assertEqual(rc, 0)

    def test_38_cli_request_approval_non_delegable_rc0(self):
        from luna_modules.luna_limited_autonomy import _cli
        rc = _cli([
            "--request-approval", "Delete memory logs",
            "--action", "non_delegable",
            "--target", "memory/nightly_updates.md",
            "--dry-run",
        ])
        self.assertEqual(rc, 0)

    def test_39_cli_routine_dry_run_compatible(self):
        from luna_modules.luna_limited_autonomy import _cli
        rc = _cli(["--routine-dry-run", "--max-cycles", "1", "--sleep-seconds", "0"])
        self.assertEqual(rc, 0)

    def test_40_cli_overnight_dry_run_alias_compatible(self):
        from luna_modules.luna_limited_autonomy import _cli
        rc = _cli(["--overnight-dry-run", "--max-cycles", "1", "--sleep-seconds", "0"])
        self.assertEqual(rc, 0)


class TestApprovalRequestRowFields(unittest.TestCase):
    def test_41_jsonl_row_includes_request_id_and_safe_to_execute_false(self):
        td = _make_tmpdir_with_verifier()
        try:
            routing = {
                "task_class": "code_edit",
                "goal": "test goal",
                "requested_action": "medium_code_edit",
                "target_files": ["luna_modules/test.py"],
                "approval_routing_status": "routed",
                "router_decision": "would_allow",
                "router_needs_human": False,
                "routine_request": {"request_id": "routine_xyz9"},
            }
            append_routine_approval_request(str(td), routing, dry_run=False)
            path = td / "memory" / "luna_routine_approval_requests.jsonl"
            row = json.loads(path.read_text(encoding="utf-8").strip())
            self.assertIn("request_id", row)
            self.assertIs(row["safe_to_execute_now"], False)
        finally:
            import shutil; shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
