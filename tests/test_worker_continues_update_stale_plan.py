"""Tests for continues_update stale-plan handling in worker."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import worker


class TestWorkerContinuesUpdateStalePlan(unittest.TestCase):
    def test_stale_plan_report_separates_fresh_and_blocked_jobs(self) -> None:
        original_recent = worker._cu_recent_non_success_for_plan_job
        try:
            def fake_recent(job):
                if job["id"] == "blocked":
                    return {"task_id": "old-task", "status": "noop", "noop_reason": "no_diff"}
                return None

            worker._cu_recent_non_success_for_plan_job = fake_recent
            plan = {
                "jobs": [
                    {"id": "fresh", "target_files": ["fresh.py"], "prompt_family": "fresh_family"},
                    {"id": "blocked", "target_files": ["blocked.py"], "prompt_family": "blocked_family"},
                ]
            }

            fresh_jobs, blocked_jobs = worker._cu_stale_plan_report(plan)

            self.assertEqual([job["id"] for job in fresh_jobs], ["fresh"])
            self.assertEqual(len(blocked_jobs), 1)
            self.assertEqual(blocked_jobs[0]["prior_task_id"], "old-task")
            self.assertEqual(blocked_jobs[0]["prior_reason"], "no_diff")
        finally:
            worker._cu_recent_non_success_for_plan_job = original_recent

    def test_wait_for_completion_treats_quarantine_as_terminal(self) -> None:
        original_quarantine = worker._CU_AIDER_QUARANTINE_DIR
        original_should_stop = worker._cu_should_stop
        try:
            with tempfile.TemporaryDirectory() as tmp:
                quarantine = Path(tmp)
                worker._CU_AIDER_QUARANTINE_DIR = quarantine
                worker._cu_should_stop = lambda: False
                quarantine.mkdir(parents=True, exist_ok=True)
                (quarantine / "job-quarantine.json").write_text(
                    json.dumps(
                        {
                            "task_id": "job-quarantine",
                            "status": "quarantined",
                            "failure_reason": "oversized_target_requires_scope",
                        }
                    ),
                    encoding="utf-8",
                )

                status, payload = worker._cu_wait_for_completion("job-quarantine", 30)

                self.assertEqual(status, "quarantined")
                self.assertEqual(payload["failure_reason"], "oversized_target_requires_scope")
        finally:
            worker._CU_AIDER_QUARANTINE_DIR = original_quarantine
            worker._cu_should_stop = original_should_stop

    def test_recent_non_success_includes_quarantined_results(self) -> None:
        original_done = worker._CU_AIDER_DONE_DIR
        original_failed = worker._CU_AIDER_FAILED_DIR
        original_quarantine = worker._CU_AIDER_QUARANTINE_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                done = root / "done"
                failed = root / "failed"
                quarantine = root / "quarantine"
                done.mkdir()
                failed.mkdir()
                quarantine.mkdir()
                worker._CU_AIDER_DONE_DIR = done
                worker._CU_AIDER_FAILED_DIR = failed
                worker._CU_AIDER_QUARANTINE_DIR = quarantine
                (quarantine / "job.json").write_text(
                    json.dumps(
                        {
                            "origin": "continues_update",
                            "task_id": "job",
                            "status": "quarantined",
                            "failure_reason": "oversized_target_requires_scope",
                            "target_files": ["SurgeApp_Claude_Terminal.py"],
                            "plan_job": {"prompt_family": "inspector_ui"},
                        }
                    ),
                    encoding="utf-8",
                )

                recent = worker._cu_recent_non_success_for_plan_job(
                    {
                        "target_files": ["SurgeApp_Claude_Terminal.py"],
                        "prompt_family": "inspector_ui",
                    }
                )

                self.assertIsNotNone(recent)
                self.assertEqual(recent["status"], "quarantined")
        finally:
            worker._CU_AIDER_DONE_DIR = original_done
            worker._CU_AIDER_FAILED_DIR = original_failed
            worker._CU_AIDER_QUARANTINE_DIR = original_quarantine

    def test_stale_plan_backlog_quarantines_without_deleting_queues(self) -> None:
        original_project_dir = worker._CU_PROJECT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                worker._CU_PROJECT_DIR = Path(tmp)
                plan = {"goal": "continues update", "jobs": []}
                blocked_jobs = [
                    {
                        "job": {
                            "id": "cu_01_director_agent",
                            "impact_area": "director_agent",
                            "target_files": ["director_agent.py"],
                            "prompt_family": "director_mission_planning",
                            "acceptance_test": "fresh mission is written",
                            "expected_diff_type": "orchestration",
                            "max_lines_changed": 180,
                        },
                        "prior_task_id": "old-task",
                        "prior_reason": "no_diff",
                    }
                ]

                backlog_path = Path(worker._cu_write_stale_plan_backlog(plan, blocked_jobs))

                self.assertTrue(backlog_path.exists())
                self.assertIn("director_jobs", str(backlog_path))
                self.assertIn("quarantine", str(backlog_path))
                payload = json.loads(backlog_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["state"], "quarantine")
                self.assertEqual(payload["reason"], "stale_plan_recent_non_success")
                self.assertTrue(payload["policy"]["quarantine_bad_items"])
                self.assertEqual(payload["missions"][0]["prior_task_id"], "old-task")
        finally:
            worker._CU_PROJECT_DIR = original_project_dir

    def test_director_refresh_missions_become_fresh_cu_jobs(self) -> None:
        original_concrete = worker._cu_build_concrete_instruction
        original_recent_timeout = worker._cu_recent_target_timeout_without_newer_success
        director_job = {
            "goal": "Refresh stale continues_update plan",
            "path": "director_jobs/active/refresh.json",
            "source_quarantine": "director_jobs/quarantine/stale.json",
            "missions": [
                {
                    "id": "refresh_worker",
                    "purpose": "Improve cycle telemetry",
                    "target_files": ["worker.py"],
                    "acceptance_test": "worker compiles",
                    "expected_diff_type": "telemetry",
                    "max_lines_changed": 300,
                    "prior_reason": "aider_timeout",
                }
            ],
        }

        try:
            worker._cu_build_concrete_instruction = lambda target: "Edit line 10 only."
            worker._cu_recent_target_timeout_without_newer_success = lambda targets: False

            jobs = worker._cu_jobs_from_director_refresh(director_job)
        finally:
            worker._cu_build_concrete_instruction = original_concrete
            worker._cu_recent_target_timeout_without_newer_success = original_recent_timeout

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["origin"], "director_refresh")
        self.assertEqual(jobs[0]["target_files"], ["worker.py"])
        self.assertLessEqual(jobs[0]["max_lines_changed"], 220)
        self.assertIn("aider_timeout", jobs[0]["prompt"])
        self.assertIn("Concrete edit requirement", jobs[0]["prompt"])
        self.assertTrue(jobs[0]["prompt_family"].startswith("director_refresh_"))

    def test_director_refresh_keeps_prior_no_diff_missions_as_fresh_work(self) -> None:
        original_recent_timeout = worker._cu_recent_target_timeout_without_newer_success
        director_job = {
            "goal": "Refresh stale continues_update plan",
            "missions": [
                {
                    "id": "old_noop",
                    "target_files": ["director_agent.py"],
                    "prior_reason": "no_diff",
                },
                {
                    "id": "real_failure",
                    "target_files": ["worker.py"],
                    "prior_reason": "aider_timeout",
                },
            ],
        }

        try:
            worker._cu_recent_target_timeout_without_newer_success = lambda targets: False

            jobs = worker._cu_jobs_from_director_refresh(director_job)
        finally:
            worker._cu_recent_target_timeout_without_newer_success = original_recent_timeout

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["target_files"], ["worker.py"])
        self.assertEqual(jobs[1]["target_files"], ["director_agent.py"])
        self.assertIn("aider_timeout", jobs[0]["prompt"])
        self.assertIn("no_diff", jobs[1]["prompt"])

    def test_latest_active_director_refresh_can_be_reused(self) -> None:
        original_project_dir = worker._CU_PROJECT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                worker._CU_PROJECT_DIR = root
                active = root / "director_jobs" / "active"
                active.mkdir(parents=True)
                plan_path = active / "20260429_010101_refresh_continues_update.json"
                plan_path.write_text(
                    json.dumps(
                        {
                            "state": "active",
                            "goal": "Refresh stale continues_update plan with executable missions",
                            "source_quarantine": "stale.json",
                            "missions": [{"id": "m1", "target_files": ["worker.py"]}],
                        }
                    ),
                    encoding="utf-8",
                )

                plan = worker._cu_latest_active_director_refresh()

                self.assertEqual(plan["path"], str(plan_path))
                self.assertEqual(plan["source_quarantine"], "stale.json")
        finally:
            worker._CU_PROJECT_DIR = original_project_dir

    def test_supplement_fresh_jobs_uses_director_when_stale_filter_leaves_one_job(self) -> None:
        original_latest = worker._cu_latest_active_director_refresh
        original_from_director = worker._cu_jobs_from_director_refresh
        original_dirty_targets = worker._cu_dirty_targets
        try:
            worker._cu_latest_active_director_refresh = lambda: {
                "path": "director_jobs/active/refresh.json",
                "source_quarantine": "director_jobs/quarantine/stale.json",
                "missions": [{"id": "m1", "target_files": ["worker.py"]}],
            }
            worker._cu_jobs_from_director_refresh = lambda director_job: [
                {
                    "id": "director_extra",
                    "origin": "director_refresh",
                    "target_files": ["worker.py"],
                    "prompt_family": "director_refresh_test",
                }
            ]
            worker._cu_dirty_targets = lambda targets: []
            fresh = [{"id": "fresh", "target_files": ["luna_modules/luna_two_pass_review.py"]}]
            blocked = [{"job": {"id": "blocked", "target_files": ["worker.py"]}, "prior_reason": "aider_timeout"}]

            jobs, source = worker._cu_supplement_fresh_jobs(fresh, blocked)

            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[1]["id"], "director_extra")
            self.assertEqual(source, "director_jobs/active/refresh.json")
        finally:
            worker._cu_latest_active_director_refresh = original_latest
            worker._cu_jobs_from_director_refresh = original_from_director
            worker._cu_dirty_targets = original_dirty_targets

    def test_supplement_fresh_jobs_skips_dirty_director_targets(self) -> None:
        original_latest = worker._cu_latest_active_director_refresh
        original_from_director = worker._cu_jobs_from_director_refresh
        original_dirty_targets = worker._cu_dirty_targets
        try:
            worker._cu_latest_active_director_refresh = lambda: {"path": "refresh.json", "missions": []}
            worker._cu_jobs_from_director_refresh = lambda director_job: [
                {"id": "dirty", "target_files": ["worker.py"]},
                {"id": "clean", "target_files": ["aider_bridge.py"]},
            ]
            worker._cu_dirty_targets = lambda targets: (
                [{"status": "M", "path": "worker.py"}] if targets == ["worker.py"] else []
            )

            jobs, _source = worker._cu_supplement_fresh_jobs(
                [{"id": "fresh", "target_files": ["luna_modules/luna_two_pass_review.py"]}],
                [{"job": {"id": "blocked"}, "prior_reason": "aider_timeout"}],
            )

            self.assertEqual([job["id"] for job in jobs], ["fresh", "clean"])
        finally:
            worker._cu_latest_active_director_refresh = original_latest
            worker._cu_jobs_from_director_refresh = original_from_director
            worker._cu_dirty_targets = original_dirty_targets

    def test_director_refresh_skips_recent_successful_mission(self) -> None:
        original_recent_success = worker._cu_recent_success_for_director_mission
        original_recent_timeout = worker._cu_recent_target_timeout_without_newer_success
        try:
            worker._cu_recent_success_for_director_mission = lambda mission, director_job: (
                {"task_id": "done"} if mission.get("id") == "already_done" else None
            )
            worker._cu_recent_target_timeout_without_newer_success = lambda targets: False
            director_job = {
                "goal": "Refresh stale continues_update plan",
                "missions": [
                    {"id": "already_done", "target_files": ["luna_modules/luna_memory_router.py"], "prior_reason": "target_not_found"},
                    {"id": "still_needed", "target_files": ["worker.py"], "prior_reason": "aider_timeout"},
                ],
            }

            jobs = worker._cu_jobs_from_director_refresh(director_job)

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["target_files"], ["worker.py"])
        finally:
            worker._cu_recent_success_for_director_mission = original_recent_success
            worker._cu_recent_target_timeout_without_newer_success = original_recent_timeout

    def test_director_refresh_skips_recent_target_timeout(self) -> None:
        original_recent_timeout = worker._cu_recent_target_timeout_without_newer_success
        try:
            worker._cu_recent_target_timeout_without_newer_success = lambda targets: targets == ["aider_bridge.py"]
            director_job = {
                "missions": [
                    {"id": "stuck", "target_files": ["aider_bridge.py"], "prior_reason": "no_diff"},
                    {"id": "safe", "target_files": ["director_agent.py"], "prior_reason": "no_diff"},
                ]
            }

            jobs = worker._cu_jobs_from_director_refresh(director_job)

            self.assertEqual([job["target_files"] for job in jobs], [["director_agent.py"]])
        finally:
            worker._cu_recent_target_timeout_without_newer_success = original_recent_timeout

    def test_dirty_target_guard_reads_git_status(self) -> None:
        original_run = worker.subprocess.run

        class FakeResult:
            returncode = 0
            stdout = "MM worker.py\n M aider_bridge.py\n?? luna_start.pyw\n"

        try:
            worker.subprocess.run = lambda *args, **kwargs: FakeResult()

            dirty = worker._cu_dirty_targets(["worker.py", "aider_bridge.py"])

            self.assertEqual(
                dirty,
                [
                    {"status": "MM", "path": "worker.py"},
                    {"status": "M", "path": "aider_bridge.py"},
                ],
            )
        finally:
            worker.subprocess.run = original_run

    def test_activate_rebuilt_plan_resets_recovery_state(self) -> None:
        plan = {"jobs": [{"id": "old"}]}
        file_empty_streak = {"worker.py": 2}

        reset = worker._cu_activate_rebuilt_plan(
            plan,
            [{"id": "fresh"}],
            file_empty_streak,
        )

        self.assertEqual(plan["jobs"], [{"id": "fresh"}])
        self.assertEqual(file_empty_streak, {})
        self.assertEqual(reset["cycle"], 0)
        self.assertEqual(reset["deferred_count"], 0)
        self.assertEqual(reset["queued_count"], 0)
        self.assertFalse(reset["dirty_recovery_added"])
        self.assertEqual(reset["deferred_targets"], [])

    def test_concrete_instruction_does_not_treat_urlopen_as_file_open(self) -> None:
        original_project_dir = worker.PROJECT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                worker.PROJECT_DIR = root
                target = root / "sample.py"
                target.write_text(
                    "import urllib.request\n\n"
                    "def fetch():\n"
                    "    with urllib.request.urlopen('http://example.test') as response:\n"
                    "        return response.read()\n",
                    encoding="utf-8",
                )

                instruction = worker._cu_build_concrete_instruction("sample.py")

                self.assertNotIn("encoding", instruction or "")
        finally:
            worker.PROJECT_DIR = original_project_dir

    def test_concrete_instruction_rejects_docstring_only_work(self) -> None:
        original_project_dir = worker.PROJECT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                worker.PROJECT_DIR = root
                target = root / "sample.py"
                target.write_text(
                    "def useful_public_function(value: int) -> int:\n"
                    "    return value + 1\n",
                    encoding="utf-8",
                )

                instruction = worker._cu_build_concrete_instruction("sample.py")

                self.assertIsNone(instruction)
                combined_patterns = "\n".join(worker._CU_INSTRUCTION_PATTERNS).lower()
                self.assertNotIn("add a one-sentence docstring", combined_patterns)
                self.assertNotIn("no docstring", combined_patterns)
        finally:
            worker.PROJECT_DIR = original_project_dir

    def test_dirty_plan_recovery_builds_jobs_for_clean_existing_targets(self) -> None:
        original_candidates = worker._CU_RECOVERY_FILES
        original_dirty_targets = worker._cu_dirty_targets
        original_project_dir = worker._CU_PROJECT_DIR
        original_concrete = worker._cu_build_concrete_instruction
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                worker._CU_PROJECT_DIR = root
                clean = root / "luna_modules" / "luna_heartbeat.py"
                dirty = root / "luna_modules" / "luna_logging.py"
                clean.parent.mkdir(parents=True)
                clean.write_text("x = 1\n", encoding="utf-8")
                dirty.write_text("x = 2\n", encoding="utf-8")
                worker._CU_RECOVERY_FILES = [
                    "luna_modules/luna_logging.py",
                    "luna_modules/luna_heartbeat.py",
                    "missing.py",
                ]
                worker._cu_dirty_targets = lambda targets: (
                    [{"status": "M", "path": "luna_modules/luna_logging.py"}]
                    if targets == ["luna_modules/luna_logging.py"]
                    else []
                )
                worker._cu_build_concrete_instruction = lambda target: "Edit this target line only."

                jobs = worker._cu_dirty_plan_recovery_jobs([{"target_files": ["worker.py"]}])

                self.assertEqual(len(jobs), 1)
                self.assertEqual(jobs[0]["origin"], "dirty_target_recovery")
                self.assertEqual(jobs[0]["target_files"], ["luna_modules/luna_heartbeat.py"])
                self.assertFalse(jobs[0]["apply_on_pass"])
                self.assertIn("Edit ONLY `luna_modules/luna_heartbeat.py`", jobs[0]["prompt"])
                self.assertIn("Concrete edit requirement", jobs[0]["prompt"])
                self.assertNotIn("worker.py", jobs[0]["prompt"])
        finally:
            worker._CU_RECOVERY_FILES = original_candidates
            worker._cu_dirty_targets = original_dirty_targets
            worker._CU_PROJECT_DIR = original_project_dir
            worker._cu_build_concrete_instruction = original_concrete

    def test_dirty_plan_recovery_offers_enough_fallback_targets(self) -> None:
        original_candidates = worker._CU_RECOVERY_FILES
        original_dirty_targets = worker._cu_dirty_targets
        original_project_dir = worker._CU_PROJECT_DIR
        original_concrete = worker._cu_build_concrete_instruction
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                worker._CU_PROJECT_DIR = root
                worker._CU_RECOVERY_FILES = [f"mod_{index}.py" for index in range(8)]
                for rel in worker._CU_RECOVERY_FILES:
                    (root / rel).write_text("value = 1\n", encoding="utf-8")
                worker._cu_dirty_targets = lambda targets: []
                worker._cu_build_concrete_instruction = lambda target: "Edit this target line only."

                jobs = worker._cu_dirty_plan_recovery_jobs([{"target_files": ["worker.py"]}])

                self.assertEqual(len(jobs), 6)
        finally:
            worker._CU_RECOVERY_FILES = original_candidates
            worker._cu_dirty_targets = original_dirty_targets
            worker._CU_PROJECT_DIR = original_project_dir
            worker._cu_build_concrete_instruction = original_concrete

    def test_status_reports_cooldown_remaining(self) -> None:
        original_load_state = worker._cu_load_state
        original_should_stop = worker._cu_should_stop
        original_lock_alive = worker._cu_is_alive_via_lock
        future = (worker.datetime.now() + worker.timedelta(seconds=35)).isoformat(timespec="seconds")
        try:
            worker._cu_load_state = lambda: {"running": True, "phase": "cooldown", "cooldown_until": future}
            worker._cu_should_stop = lambda: False
            worker._cu_is_alive_via_lock = lambda: True

            status = worker.continues_update_status()

            self.assertTrue(status["running"])
            self.assertEqual(status["phase"], "cooldown")
            self.assertGreater(status["cooldown_remaining_seconds"], 0)
            self.assertEqual(status["next_cycle_at"], future)
        finally:
            worker._cu_load_state = original_load_state
            worker._cu_should_stop = original_should_stop
            worker._cu_is_alive_via_lock = original_lock_alive

    def test_cleanup_old_proposals_quarantines_without_deleting_staged_dirs(self) -> None:
        original_logic_updates = worker.LOGIC_UPDATES_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                logic_updates = root / "logic_updates"
                logic_updates.mkdir()
                for name in ("001_staged", "002_old", "003_new"):
                    folder = logic_updates / name
                    folder.mkdir()
                    (folder / "note.txt").write_text(name, encoding="utf-8")

                worker.LOGIC_UPDATES_DIR = logic_updates
                with patch.object(worker, "_logic_update_has_git_state", side_effect=lambda path: path.name == "001_staged"):
                    moved = worker._cleanup_old_proposals(max_keep=1)

                self.assertEqual(moved, 1)
                self.assertTrue((logic_updates / "001_staged").exists())
                self.assertTrue((logic_updates / "003_new").exists())
                self.assertTrue((logic_updates / "quarantine" / "002_old" / "note.txt").exists())
        finally:
            worker.LOGIC_UPDATES_DIR = original_logic_updates


if __name__ == "__main__":
    unittest.main()
