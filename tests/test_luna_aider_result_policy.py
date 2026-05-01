"""Tests for staged Aider Bridge result discipline."""

from __future__ import annotations

import unittest


from luna_modules.luna_aider_result_policy import (
    build_aider_completion_record,
    build_aider_report,
)


class TestAiderResultPolicy(unittest.TestCase):
    def test_empty_diff_without_analysis_only_becomes_noop(self) -> None:
        record = build_aider_completion_record(
            task_id="job-1",
            target_file="worker.py",
            diff_text="",
            diff_path="",
            log_path="logs/aider_bridge.log",
            verification_passed=True,
            applied=False,
            failure_reason="",
            analysis_only=False,
            model_used="test-model",
            started_at=100.0,
            finished_at=112.5,
        )

        self.assertEqual(record["status"], "noop")
        self.assertFalse(record["diff_exists"])
        self.assertEqual(record["noop_reason"], "no_diff")
        self.assertFalse(record["counts_as_successful_upgrade"])
        self.assertEqual(record["duration_seconds"], 12.5)
        self.assertEqual(record["live_feed_event"], "NOOP")

    def test_analysis_only_empty_diff_can_finish_done_without_upgrade_credit(self) -> None:
        record = build_aider_completion_record(
            task_id="job-2",
            target_file="worker.py",
            diff_text="",
            diff_path="",
            log_path="logs/aider_bridge.log",
            verification_passed=True,
            applied=False,
            failure_reason="",
            analysis_only=True,
            model_used="test-model",
            started_at=10.0,
            finished_at=11.0,
        )

        self.assertEqual(record["status"], "done")
        self.assertEqual(record["done_reason"], "analysis_only")
        self.assertFalse(record["counts_as_successful_upgrade"])
        self.assertEqual(record["live_feed_event"], "DONE")

    def test_real_diff_done_records_required_fields(self) -> None:
        record = build_aider_completion_record(
            task_id="job-3",
            target_file="worker.py",
            diff_text="--- a\n+++ b\n@@\n-old\n+new\n",
            diff_path="logic_updates/job-3/job-3.diff",
            log_path="logs/aider_bridge.log",
            verification_passed=True,
            applied=False,
            failure_reason="",
            analysis_only=False,
            model_used="test-model",
            started_at=1.0,
            finished_at=4.0,
        )

        for field in [
            "status",
            "diff_exists",
            "diff_path",
            "log_path",
            "target_file",
            "verification_passed",
            "applied",
            "failure_reason",
            "noop_reason",
            "model_used",
            "duration_seconds",
        ]:
            self.assertIn(field, record)
        self.assertEqual(record["status"], "done")
        self.assertTrue(record["diff_exists"])
        self.assertEqual(record["done_reason"], "real_diff")

    def test_real_diff_done_records_required_fields_refactored(self) -> None:
        record = build_aider_completion_record(
            task_id="job-3",
            target_file="worker.py",
            diff_text="--- a\n+++ b\n@@\n-old\n+new\n",
            diff_path="logic_updates/job-3/job-3.diff",
            log_path="logs/aider_bridge.log",
            verification_passed=True,
            applied=False,
            failure_reason="",
            analysis_only=False,
            model_used="test-model",
            started_at=1.0,
            finished_at=4.0,
        )

        for field in [
            "status",
            "diff_exists",
            "diff_path",
            "log_path",
            "target_file",
            "verification_passed",
            "applied",
            "failure_reason",
            "noop_reason",
            "model_used",
            "duration_seconds",
        ]:
            self.assertIn(field, record)
        self.assertEqual(record["status"], "done")
        self.assertTrue(record["diff_exists"])
        self.assertEqual(record["done_reason"], "real_diff")

    def test_failed_verification_records_failure(self) -> None:
        record = build_aider_completion_record(
            task_id="job-4",
            target_file="worker.py",
            diff_text="--- a\n+++ b\n",
            diff_path="logic_updates/job-4/job-4.diff",
            log_path="logs/aider_bridge.log",
            verification_passed=False,
            applied=False,
            failure_reason="syntax error",
            analysis_only=False,
            model_used="test-model",
            started_at=1.0,
            finished_at=2.0,
        )

        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["failure_reason"], "syntax error")
        self.assertEqual(record["live_feed_event"], "FAILED")

    def test_quarantined_record_is_not_successful(self) -> None:
        record = build_aider_completion_record(
            task_id="job-quarantine",
            target_file="worker.py",
            diff_text="",
            diff_path="",
            log_path="solutions/logs/job-quarantine.log",
            verification_passed=False,
            applied=False,
            failure_reason="oversized_target_requires_scope",
            analysis_only=False,
            model_used="test-model",
            started_at=1.0,
            finished_at=2.0,
            quarantined_reason="oversized_target_requires_scope",
        )

        self.assertEqual(record["status"], "quarantined")
        self.assertEqual(record["failure_reason"], "oversized_target_requires_scope")
        self.assertEqual(record["live_feed_event"], "QUARANTINED")
        self.assertFalse(record["counts_as_successful_upgrade"])

    def test_report_explains_noop_reason(self) -> None:
        record = build_aider_completion_record(
            task_id="job-5",
            target_file="worker.py",
            diff_text="",
            diff_path="",
            log_path="logs/aider_bridge.log",
            verification_passed=True,
            applied=False,
            failure_reason="",
            analysis_only=False,
            model_used="test-model",
            started_at=1.0,
            finished_at=2.0,
        )

        report = build_aider_report(record, prompt="make it better", diff_text="", stdout="", stderr="")

        self.assertIn("status=noop", report)
        self.assertIn("noop_reason=no_diff", report)
        self.assertIn("Empty diff is not a successful upgrade", report)


if __name__ == "__main__":
    unittest.main()
