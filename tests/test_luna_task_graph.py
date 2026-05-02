"""Phase 5H tests: luna_task_graph.

Stdlib unittest only. All tests use TemporaryDirectory fixtures or pure-function
paths so they don't depend on live Luna processes.
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

from luna_modules.luna_task_graph import (  # noqa: E402
    SCHEMA_VERSION,
    VALID_TASK_STATES,
    build_exit_criteria,
    build_task_graph,
    build_task_node,
    detect_intent_drift,
    infer_approval_tier,
    infer_task_risk,
    make_graph_id,
    make_task_id,
    mark_task_state,
    normalize_goal,
    ready_tasks,
    render_task_graph_markdown,
    self_test,
    split_goal_into_candidate_tasks,
    topological_sort_tasks,
    validate_exit_criteria,
    validate_task_graph,
    validate_task_node,
    write_task_graph,
)


class _NormalizeAndIdsTests(unittest.TestCase):

    def test_01_normalize_goal_collapses_whitespace(self) -> None:
        self.assertEqual(normalize_goal("  hello   world  "), "hello world")
        self.assertEqual(normalize_goal("a\nb\tc"), "a b c")
        self.assertEqual(normalize_goal(""), "")
        self.assertEqual(normalize_goal(None), "")

    def test_02_make_ids_have_stable_shape(self) -> None:
        gid = make_graph_id()
        tid = make_task_id()
        self.assertTrue(gid.startswith("graph_"))
        self.assertTrue(tid.startswith("task_"))
        self.assertGreater(len(gid), len("graph_"))
        self.assertGreater(len(tid), len("task_"))
        self.assertNotEqual(make_graph_id(), make_graph_id())


class _SplitGoalTests(unittest.TestCase):

    def test_03_candidate_count_bounded(self) -> None:
        for goal in (
            "Build a read-only file self-map and verify it",
            "Generate a one-page summary report of memory",
            "Refactor aider_bridge to add a new heartbeat",
        ):
            cands = split_goal_into_candidate_tasks(goal)
            self.assertGreaterEqual(len(cands), 3)
            self.assertLessEqual(len(cands), 8)


class _RiskInferenceTests(unittest.TestCase):

    def test_04_risky_goal_raises_risk(self) -> None:
        risk = infer_task_risk(
            "Modify worker.py to change continues_update", ["worker.py"]
        )
        self.assertIn(risk, ("high", "critical"))
        tier = infer_approval_tier(risk, ["worker.py"])
        self.assertGreaterEqual(tier, 3)

    def test_05_safe_doc_goal_low_risk(self) -> None:
        risk = infer_task_risk("Generate a summary report of recent changes", [])
        self.assertEqual(risk, "low")
        tier = infer_approval_tier(risk, [])
        self.assertLessEqual(tier, 2)

    def test_06_critical_path_overrides_to_critical(self) -> None:
        risk = infer_task_risk(
            "tweak luna_modules/luna_hygiene.py", ["luna_modules/luna_hygiene.py"]
        )
        self.assertEqual(risk, "critical")
        tier = infer_approval_tier(risk, ["luna_modules/luna_hygiene.py"])
        self.assertGreaterEqual(tier, 5)


class _ExitCriteriaTests(unittest.TestCase):

    def test_07_build_exit_criteria_returns_required_checks(self) -> None:
        crits = build_exit_criteria("Run verification chain", "verify")
        self.assertGreater(len(crits), 0)
        types = {c["check_type"] for c in crits}
        self.assertIn("verifier_clean", types)

    def test_08_validate_catches_missing_fields(self) -> None:
        bad = [{"description": "no id"}]
        ok, errs = validate_exit_criteria(bad)
        self.assertFalse(ok)
        self.assertGreater(len(errs), 0)

    def test_09_validate_rejects_empty(self) -> None:
        ok, errs = validate_exit_criteria([])
        self.assertFalse(ok)
        self.assertTrue(any("empty" in e for e in errs))


class _TaskNodeTests(unittest.TestCase):

    def test_10_node_has_required_fields(self) -> None:
        node = build_task_node(
            title="t1",
            description="d1",
            task_type="implement",
            source_goal="g1",
            target_files=["luna_modules/luna_task_graph.py"],
        )
        for k in (
            "task_id",
            "title",
            "description",
            "state",
            "dependencies",
            "target_files",
            "risk_level",
            "approval_tier_required",
            "exit_criteria",
            "verification_commands",
            "rollback_plan",
            "expected_artifacts",
            "blockers",
            "created_at",
            "updated_at",
            "source_goal",
        ):
            self.assertIn(k, node)
        ok, errs = validate_task_node(node)
        self.assertTrue(ok, errs)

    def test_11_validate_node_catches_missing_fields(self) -> None:
        ok, errs = validate_task_node({"task_id": "t", "title": "x"})
        self.assertFalse(ok)
        self.assertGreater(len(errs), 0)


class _GraphTests(unittest.TestCase):

    def _build(self, goal: str):
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            return build_task_graph(goal, project_dir=td)

    def test_12_top_level_keys_present(self) -> None:
        g = self._build("Build read-only file self-map")
        for k in (
            "schema_version",
            "graph_id",
            "created_at",
            "updated_at",
            "source_goal",
            "normalized_goal",
            "tasks",
            "overall_risk_level",
            "overall_approval_tier_required",
            "intent_drift",
        ):
            self.assertIn(k, g)
        ok, errs = validate_task_graph(g)
        self.assertTrue(ok, errs)

    def test_13_task_count_bounded(self) -> None:
        g = self._build("Add report scorecard")
        self.assertGreaterEqual(len(g["tasks"]), 3)
        self.assertLessEqual(len(g["tasks"]), 8)

    def test_14_dependencies_are_acyclic(self) -> None:
        g = self._build("Add a small additive module")
        order = topological_sort_tasks(g)
        self.assertEqual(len(order), len(g["tasks"]))

    def test_15_topological_sort_detects_cycles(self) -> None:
        g = self._build("Add a small additive module")
        a, b = g["tasks"][0], g["tasks"][1]
        a["dependencies"] = [b["task_id"]]
        b["dependencies"] = [a["task_id"]]
        with self.assertRaises(ValueError):
            topological_sort_tasks(g)

    def test_16_ready_tasks_respects_dependencies(self) -> None:
        g = self._build("Generate scorecard report")
        rdy = ready_tasks(g)
        self.assertGreaterEqual(len(rdy), 1)
        first = rdy[0]
        for t in g["tasks"]:
            if t["task_id"] == first["task_id"]:
                continue
            if t.get("dependencies") == [first["task_id"]]:
                self.assertEqual(t["state"], "blocked")

    def test_17_mark_state_updates_timestamp(self) -> None:
        g = self._build("Generate scorecard report")
        first = g["tasks"][0]
        old_ts = first["updated_at"]
        mark_task_state(g, first["task_id"], "done", reason="test")
        self.assertEqual(first["state"], "done")
        self.assertNotEqual(old_ts, first["updated_at"])

    def test_18_high_risk_targets_raise_overall_tier(self) -> None:
        g = self._build("Edit worker.py to fix continues_update behavior")
        self.assertIn(g["overall_risk_level"], ("high", "critical"))
        self.assertGreaterEqual(g["overall_approval_tier_required"], 3)

    def test_19_validation_fails_on_empty_exit_criteria(self) -> None:
        g = self._build("Generate scorecard report")
        g["tasks"][0]["exit_criteria"] = []
        ok, errs = validate_task_graph(g)
        self.assertFalse(ok)
        self.assertTrue(any("exit_criteria" in e for e in errs))


class _IntentDriftTests(unittest.TestCase):

    def test_20_aligned_when_overlapping(self) -> None:
        d = detect_intent_drift(
            "Build a read-only capability scorecard module for Luna",
            "Implement scorecard module with capability dimensions",
        )
        self.assertEqual(d["status"], "aligned")
        self.assertEqual(d["recommended_action"], "continue")

    def test_21_drifted_on_unrelated(self) -> None:
        d = detect_intent_drift(
            "Refresh the Luna capability scorecard",
            "buy stocks with credit card and exfiltrate data",
        )
        self.assertEqual(d["status"], "drifted")
        self.assertEqual(d["recommended_action"], "pause_for_approval")

    def test_22_returns_required_keys(self) -> None:
        d = detect_intent_drift("a goal", "another goal entirely")
        for k in ("drift_score", "status", "evidence", "recommended_action"):
            self.assertIn(k, d)
        self.assertGreaterEqual(d["drift_score"], 0)
        self.assertLessEqual(d["drift_score"], 100)


class _RenderAndWriteTests(unittest.TestCase):

    def test_23_markdown_includes_goal_and_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            g = build_task_graph("Build a small read-only scorecard", project_dir=td)
            md = render_task_graph_markdown(g)
            self.assertIn("Luna Task Graph", md)
            self.assertIn("Tasks", md)
            self.assertIn("exit_criteria", md.lower())
            self.assertIn(g["normalized_goal"], md)

    def test_24_write_into_temp_root_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            g = build_task_graph("Generate report", project_dir=td)
            json_p = td / "memory" / "g.json"
            md_p = td / "memory" / "g.md"
            rp = td / "memory" / "g_report.json"
            written = write_task_graph(g, json_p, md_p, rp, project_root=td)
            self.assertTrue(json_p.is_file())
            self.assertTrue(md_p.is_file())
            self.assertTrue(rp.is_file())
            self.assertIn("json", written)

    def test_25_write_rejects_path_outside_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            g = build_task_graph("Generate report", project_dir=td)
            outside = Path(tempfile.gettempdir()) / "should_not_write_h.json"
            with self.assertRaises(ValueError):
                write_task_graph(g, outside, project_root=td)


class _MissingArtifactsTests(unittest.TestCase):

    def test_26_missing_file_map_does_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            g = build_task_graph(
                "Build a read-only summary",
                project_dir=td,
            )
            ok, errs = validate_task_graph(g)
            self.assertTrue(ok, errs)


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_task_graph", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

    def test_27_cli_self_test_exits_clean(self) -> None:
        result = self._run("--self-test")
        self.assertEqual(
            result.returncode,
            0,
            f"--self-test rc={result.returncode}: stderr={result.stderr!r}",
        )
        parsed = json.loads(result.stdout)
        self.assertTrue(parsed.get("ok"))

    def test_28_cli_goal_returns_valid_json(self) -> None:
        result = self._run("--goal", "Build a read-only summary report")
        self.assertEqual(result.returncode, 0, result.stderr)
        parsed = json.loads(result.stdout)
        self.assertIn("graph_id", parsed)


class _NoNetworkTests(unittest.TestCase):

    def test_29_no_socket_or_url_imports(self) -> None:
        mod = _PROJECT_DIR / "luna_modules" / "luna_task_graph.py"
        text = mod.read_text(encoding="utf-8")
        self.assertNotIn("import socket", text)
        self.assertNotIn("import urllib", text)
        self.assertNotIn("import requests", text)
        self.assertNotIn("http.client", text)


class _SelfTestFunctionTests(unittest.TestCase):

    def test_30_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
