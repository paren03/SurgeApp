"""Tests for staged Luna Autonomy Control v1 quality gate."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path


from luna_modules.luna_autonomy_control import (
    build_autonomy_control_summary,
    classify_autonomy_artifact,
    evaluate_cycle_budget,
    evaluate_done_policy,
    quarantine_autonomy_artifact,
    record_budget_violation,
    scan_autonomy_quality,
)


class TestAutonomyQualityGate(unittest.TestCase):
    def setUp(self) -> None:
        "Set up test environment"
        self.root = Path("temp_test_zone") / f"autonomy_control_{uuid.uuid4().hex[:8]}"
        (self.root / "aider_jobs" / "failed").mkdir(parents=True, exist_ok=True)
        (self.root / "aider_jobs" / "done").mkdir(parents=True, exist_ok=True)
        (self.root / "tasks" / "failed").mkdir(parents=True, exist_ok=True)
        (self.root / "solutions").mkdir(parents=True, exist_ok=True)
        (self.root / "memory").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

    def test_classifies_no_diff_without_deleting(self) -> None:
        item = self.root / "aider_jobs" / "done" / "job.json"
        item.write_text(json.dumps({"status": "done", "summary": "Diff empty; no changes"}), encoding="utf-8")

        row = classify_autonomy_artifact(item)

        self.assertTrue(row["no_diff"])
        self.assertEqual(row["recommended_action"], "quarantine")
        self.assertTrue(item.exists())

    def test_scan_writes_inspector_report(self) -> None:
        failed = self.root / "tasks" / "failed" / "task.json"
        failed.write_text(json.dumps({"status": "failed", "error": "verification failed"}), encoding="utf-8")

        report = scan_autonomy_quality(self.root)

        self.assertEqual(report["quarantine_candidate_count"], 1)
        self.assertTrue((self.root / "memory" / "luna_autonomy_quality_gate.json").exists())
        self.assertIn("never", report["policy"]["delete"])

    def test_quarantine_moves_and_preserves_manifest(self) -> None:
        failed = self.root / "tasks" / "failed" / "task.json"
        failed.write_text(json.dumps({"status": "failed"}), encoding="utf-8")

        result = quarantine_autonomy_artifact(self.root, failed, "failure_detected")

        self.assertTrue(result["ok"])
        self.assertFalse(failed.exists())
        destination = Path(result["destination"])
        self.assertTrue(destination.exists())
        self.assertTrue(destination.with_suffix(destination.suffix + ".quarantine.json").exists())

    def test_kill_switch_blocks_quarantine_action(self) -> None:
        failed = self.root / "tasks" / "failed" / "task.json"
        failed.write_text(json.dumps({"status": "failed"}), encoding="utf-8")
        (self.root / "LUNA_STOP_NOW.flag").write_text("stop", encoding="utf-8")

        result = quarantine_autonomy_artifact(self.root, failed, "failure_detected")

        self.assertFalse(result["ok"])
        self.assertTrue(result["blocked"])
        self.assertTrue(failed.exists())

    def test_summary_is_main_chat_safe_and_inspector_ready(self) -> None:
        solution = self.root / "solutions" / "job.txt"
        solution.write_text("No diff. Already implemented.", encoding="utf-8")

        summary = build_autonomy_control_summary(self.root)

        self.assertIn("[LUNA AUTONOMY CONTROL V1]", summary)
        self.assertIn("quarantine_candidates: 1", summary)
        self.assertIn("policy: never delete", summary)

    def test_done_requires_diff_analysis_or_compliance_evidence(self) -> None:
        self.assertEqual(evaluate_done_policy({"diff_exists": True})["status"], "done")
        self.assertTrue(evaluate_done_policy({"analysis_only": True})["done_allowed"])
        self.assertTrue(
            evaluate_done_policy({"already_compliant": True, "compliance_evidence": "verified target"})["done_allowed"]
        )

        result = evaluate_done_policy({"diff_exists": False, "analysis_only": False})

        self.assertEqual(result["status"], "noop")
        self.assertEqual(result["reason"], "no_diff")
        self.assertFalse(result["counts_as_successful_upgrade"])

    def test_cycle_budget_exceeded_pauses_and_writes_summaries(self) -> None:
        jobs = [
            {"status": "failed", "target_file": "worker.py", "prompt": "one-line docstring"}
            for _ in range(6)
        ]

        budget = evaluate_cycle_budget(jobs)
        action = record_budget_violation(self.root, budget)

        self.assertTrue(budget["exceeded"])
        self.assertTrue(action["ok"])
        self.assertTrue((self.root / "memory" / "continues_update.stop").exists())
        self.assertTrue((self.root / "logs" / "luna_live_feed.jsonl").exists())
        self.assertTrue((self.root / "memory" / "nightly_updates.md").exists())
        self.assertTrue((self.root / "memory" / "nightly_updates.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
