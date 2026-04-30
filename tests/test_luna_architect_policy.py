"""Tests for Architect autonomy policy guardrails."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from luna_modules import luna_architect


PROJECT_TEMP = Path(r"D:\SurgeApp\temp_test_zone")


def _test_dir(name: str) -> Path:
    path = PROJECT_TEMP / f"{name}_{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=True)
    return path


class TestLunaArchitectPolicy(unittest.TestCase):
    def test_low_value_issue_types_are_not_meaningful(self) -> None:
        self.assertFalse(luna_architect.is_meaningful_issue("no_docstring"))
        self.assertFalse(luna_architect.is_meaningful_issue("no_comments"))

    def test_generate_instruction_blocks_low_value_prompts(self) -> None:
        self.assertEqual(
            luna_architect.generate_instruction("no_docstring", "aider_bridge.py", "_write_json", 140),
            "",
        )
        self.assertEqual(
            luna_architect.generate_instruction("no_comments", "aider_bridge.py", "_write_json", 140),
            "",
        )

    def test_scan_file_does_not_emit_docstring_cleanup_jobs(self) -> None:
        project = _test_dir("architect_scan")
        target = project / "sample.py"
        target.write_text("def helper():\n    return 1\n", encoding="utf-8")

        with patch.object(luna_architect, "_PROJECT_DIR", project):
            self.assertEqual(luna_architect.scan_file("sample.py"), [])

    def test_submit_job_is_stage_only(self) -> None:
        project = _test_dir("architect_submit")
        active = project / "aider_jobs" / "active"

        with patch.object(luna_architect, "_PROJECT_DIR", project), patch.object(
            luna_architect, "_AIDER_ACTIVE", active
        ):
            task_id = luna_architect._submit_job("Fix open encoding", "sample.py")
            payload = json.loads((active / f"{task_id}.json").read_text(encoding="utf-8"))

        self.assertFalse(payload["apply_on_pass"])
        self.assertEqual(payload["expected_diff_type"], "safety_fix")

    def test_recent_noop_result_marks_issue_already_compliant(self) -> None:
        project = _test_dir("architect_noop")
        failed = project / "aider_jobs" / "failed"
        failed.mkdir(parents=True, exist_ok=True)
        result = {
            "origin": "luna_architect",
            "status": "noop",
            "noop_reason": "no_diff",
            "target_files": ["luna_guardian.py"],
            "instructions": "At line 210 there is an open() call that is missing encoding.",
        }
        (failed / "task.json").write_text(json.dumps(result), encoding="utf-8")

        issue = ("open_no_encoding", "luna_guardian.py", "open_at_line_210", 210)
        with patch.object(luna_architect, "_AIDER_FAILED", failed), patch.object(
            luna_architect, "_AIDER_DONE", project / "aider_jobs" / "done"
        ), patch.object(luna_architect, "_AIDER_QUARANTINE", project / "aider_jobs" / "quarantine"):
            self.assertTrue(luna_architect._was_recent_noop(issue))

    def test_wait_for_job_treats_quarantine_as_terminal(self) -> None:
        project = _test_dir("architect_quarantine")
        quarantine = project / "aider_jobs" / "quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        (quarantine / "job.json").write_text(
            json.dumps({"status": "quarantined", "failure_reason": "target_has_staged_or_unstaged_edits"}),
            encoding="utf-8",
        )

        with patch.object(luna_architect, "_AIDER_DONE", project / "aider_jobs" / "done"), patch.object(
            luna_architect, "_AIDER_FAILED", project / "aider_jobs" / "failed"
        ), patch.object(luna_architect, "_AIDER_QUARANTINE", quarantine):
            self.assertEqual(luna_architect._wait_for_job("job"), ("quarantined", False))

    def test_recent_quarantine_defers_immediate_architect_retry(self) -> None:
        project = _test_dir("architect_recent_quarantine")
        quarantine = project / "aider_jobs" / "quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        result = {
            "origin": "luna_architect",
            "status": "quarantined",
            "failure_reason": "target_has_staged_or_unstaged_edits",
            "target_files": ["luna_guardian.py"],
            "instructions": "At line 259 there is an open() call that is missing encoding.",
        }
        (quarantine / "task.json").write_text(json.dumps(result), encoding="utf-8")

        issue = ("open_no_encoding", "luna_guardian.py", "open_at_line_259", 259)
        with patch.object(luna_architect, "_AIDER_FAILED", project / "aider_jobs" / "failed"), patch.object(
            luna_architect, "_AIDER_DONE", project / "aider_jobs" / "done"
        ), patch.object(luna_architect, "_AIDER_QUARANTINE", quarantine):
            self.assertEqual(luna_architect._recent_architect_skip_reason(issue), "quarantined")


if __name__ == "__main__":
    unittest.main()
