"""Tests for staged Inspector autonomy feed aggregation."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path


from luna_modules.luna_inspector_autonomy_feed import build_inspector_autonomy_snapshot


class TestInspectorAutonomyFeed(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("temp_test_zone") / f"inspector_feed_{uuid.uuid4().hex[:8]}"
        (self.root / "director_jobs" / "active").mkdir(parents=True, exist_ok=True)
        (self.root / "aider_jobs" / "failed").mkdir(parents=True, exist_ok=True)
        (self.root / "logic_updates" / "job1").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)
        (self.root / "memory").mkdir(parents=True, exist_ok=True)

    def test_snapshot_contains_required_sections(self) -> None:
        (self.root / "director_jobs" / "active" / "plan.json").write_text(
            json.dumps({"goal": "Build Luna Autonomy Control v1", "missions": [{"id": "quality_gate"}]}),
            encoding="utf-8",
        )
        (self.root / "aider_jobs" / "failed" / "job1.json").write_text(
            json.dumps({"task_id": "job1", "status": "noop", "noop_reason": "no_diff"}),
            encoding="utf-8",
        )
        (self.root / "logic_updates" / "job1" / "job1.diff").write_text("--- a\n+++ b\n", encoding="utf-8")
        (self.root / "logs" / "luna_live_feed.jsonl").write_text(
            json.dumps({"event": "NOOP", "role": "aider_bridge"}) + "\n",
            encoding="utf-8",
        )
        (self.root / "memory" / "nightly_updates.md").write_text("## Summary\n- what changed: none\n", encoding="utf-8")

        snapshot = build_inspector_autonomy_snapshot(self.root)

        for section in ["plans", "jobs", "logs", "diffs", "verification", "failures", "summaries", "continues_update", "running"]:
            self.assertIn(section, snapshot)
        self.assertEqual(snapshot["plans"][0]["goal"], "Build Luna Autonomy Control v1")
        self.assertEqual(snapshot["jobs"][0]["status"], "noop")
        self.assertEqual(snapshot["diffs"][0]["path"].endswith("job1.diff"), True)
        self.assertEqual(snapshot["logs"][0]["event"], "NOOP")
        self.assertIn("nightly_updates.md", snapshot["summaries"][0]["path"])

    def test_snapshot_reports_continues_update_running_without_active_job(self) -> None:
        (self.root / "memory" / "continues_update_state.json").write_text(
            json.dumps({
                "running": True,
                "started_at": "2026-04-29T17:30:29",
                "last_cycle_at": "2026-04-29T17:44:16",
                "last_status": "noop",
                "cycles": 5,
                "noop_count": 3,
            }),
            encoding="utf-8",
        )

        snapshot = build_inspector_autonomy_snapshot(self.root)

        self.assertTrue(snapshot["continues_update"]["running"])
        self.assertEqual(snapshot["continues_update"]["display_status"], "running_sleeping")
        self.assertEqual(snapshot["running"]["continues_update"]["cycles"], 5)


if __name__ == "__main__":
    # unittest.main()
    unittest.main(timeout=30)
