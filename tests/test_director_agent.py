"""Tests for staged Luna Director Agent v1."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path


from director_agent import (
    build_director_missions,
    emit_director_event,
    ensure_director_folders,
    parse_ceo_command,
    write_director_job,
    write_director_refresh_job,
)


class TestDirectorAgent(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("temp_test_zone") / f"director_agent_{uuid.uuid4().hex[:8]}"
        self.root.mkdir(parents=True, exist_ok=True)

    def test_parse_ceo_command_accepts_goal(self) -> None:
        parsed = parse_ceo_command("/ceo Build Luna Autonomy Control v1")

        self.assertTrue(parsed["accepted"])
        self.assertEqual(parsed["goal"], "Build Luna Autonomy Control v1")

    def test_parse_ceo_command_rejects_non_ceo_input(self) -> None:
        parsed = parse_ceo_command("continues update")

        self.assertFalse(parsed["accepted"])
        self.assertEqual(parsed["reason"], "not_ceo_command")

    def test_build_missions_include_required_fields(self) -> None:
        missions = build_director_missions("Build Luna Autonomy Control v1")

        self.assertGreaterEqual(len(missions), 8)
        self.assertLessEqual(len(missions), 12)
        for mission in missions:
            self.assertIn("id", mission)
            self.assertIn("purpose", mission)
            self.assertIn("target_files", mission)
            self.assertIn("risk_level", mission)
            self.assertIn("acceptance_test", mission)
            self.assertIn("rollback_stage_plan", mission)
            self.assertIn("expected_diff_type", mission)
            self.assertIn("max_lines_changed", mission)
            self.assertIn("function_scope_required", mission)

    def test_worker_missions_require_function_scope(self) -> None:
        missions = build_director_missions("stabilize Luna autonomy")

        worker_missions = [
            mission for mission in missions
            if "worker.py" in [str(item) for item in mission.get("target_files", [])]
        ]

        self.assertTrue(worker_missions)
        self.assertTrue(all(mission["function_scope_required"] for mission in worker_missions))

    def test_write_job_creates_active_job_and_folders(self) -> None:
        folders = ensure_director_folders(self.root)
        job = write_director_job(self.root, "/ceo Build Luna Autonomy Control v1")

        self.assertTrue(Path(folders["active"]).exists())
        self.assertTrue(Path(folders["done"]).exists())
        self.assertTrue(Path(folders["failed"]).exists())
        self.assertTrue(Path(folders["quarantine"]).exists())
        self.assertEqual(job["state"], "active")
        self.assertTrue(Path(job["path"]).exists())
        payload = json.loads(Path(job["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["role"], "director")
        self.assertEqual(payload["goal"], "Build Luna Autonomy Control v1")
        self.assertGreaterEqual(len(payload["missions"]), 3)

    def test_emit_director_event_writes_live_feed_role(self) -> None:
        event = emit_director_event(self.root, "DIRECTOR_PLAN_CREATED", {"goal": "test"})

        feed_path = self.root / "logs" / "luna_live_feed.jsonl"
        self.assertTrue(feed_path.exists())
        self.assertEqual(event["role"], "director")
        self.assertIn("DIRECTOR_PLAN_CREATED", feed_path.read_text(encoding="utf-8"))

    def test_write_refresh_job_promotes_quarantine_to_active_plan(self) -> None:
        folders = ensure_director_folders(self.root)
        quarantine_path = Path(folders["quarantine"]) / "stale_plan.json"
        quarantine_path.write_text(
            json.dumps(
                {
                    "state": "quarantine",
                    "missions": [
                        {
                            "id": "refresh_worker",
                            "purpose": "Refresh worker telemetry",
                            "target_files": ["worker.py"],
                            "risk_level": "high",
                            "acceptance_test": "worker compiles",
                            "rollback_stage_plan": "stage only",
                            "expected_diff_type": "telemetry",
                            "max_lines_changed": 260,
                            "avoid_prompt_family": "cu_budget_telemetry",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        job = write_director_refresh_job(self.root, quarantine_path)

        self.assertEqual(job["state"], "active")
        self.assertTrue(Path(job["path"]).exists())
        self.assertEqual(job["source_quarantine"], str(quarantine_path))
        self.assertEqual(job["missions"][0]["max_lines_changed"], 220)
        self.assertIn("cu_budget_telemetry", job["policy"]["avoid_prompt_families"])

    def test_operating_vision_file_contains_required_roles(self) -> None:
        vision = Path(__file__).resolve().parents[1] / "memory" / "luna_operating_vision.md"
        text = vision.read_text(encoding="utf-8")

        for required in [
            "Serge is CEO / final authority",
            "Luna is supervisor, narrator, and safety gatekeeper",
            "Director converts goals into executable missions",
            "Architect designs safe implementation plans",
            "Engineer prepares code edits through Aider Bridge",
            "QA verifies and recommends stage/apply",
            "Apprentice studies logs/results and improves future plans",
            "Guardian keeps services alive and prevents duplicate chaos",
            "fully local autonomous engineering system",
        ]:
            self.assertIn(required, text)


if __name__ == "__main__":
    unittest.main()
