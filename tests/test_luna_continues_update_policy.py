"""Tests for staged smart continues_update planning."""

from __future__ import annotations

import unittest


from luna_modules.luna_continues_update_policy import (
    HIGH_IMPACT_AREAS,
    LOW_VALUE_MARKERS,
    build_continues_update_plan,
    build_morning_summary,
    validate_continues_update_plan,
)


class TestContinuesUpdatePolicy(unittest.TestCase):
    def test_plan_contains_8_to_12_meaningful_tasks(self) -> None:
        plan = build_continues_update_plan("continues update")

        self.assertGreaterEqual(len(plan["jobs"]), 8)
        self.assertLessEqual(len(plan["jobs"]), 12)
        self.assertEqual(plan["queue_mode"], "one_by_one")
        self.assertTrue(plan["stage_only"])

    def test_plan_prioritizes_required_high_impact_areas(self) -> None:
        plan = build_continues_update_plan("continues update")
        areas = {job["impact_area"] for job in plan["jobs"]}

        for area in HIGH_IMPACT_AREAS:
            self.assertIn(area, areas)

    def test_plan_blocks_low_value_prompt_loops(self) -> None:
        plan = build_continues_update_plan("continues update")
        combined = "\n".join(job["prompt"].lower() for job in plan["jobs"])

        for marker in LOW_VALUE_MARKERS:
            self.assertNotIn(marker, combined)

    def test_plan_respects_same_file_and_prompt_family_limits(self) -> None:
        plan = build_continues_update_plan("continues update")
        report = validate_continues_update_plan(plan)

        self.assertTrue(report["ok"])
        self.assertEqual(report["violations"], [])

    def test_jobs_include_verification_and_summary_requirements(self) -> None:
        plan = build_continues_update_plan("continues update")

        for job in plan["jobs"]:
            self.assertIn("acceptance_test", job)
            self.assertIn("verify", job)
            self.assertIn("expected_diff_type", job)
            self.assertIn("max_lines_changed", job)
            self.assertFalse(job.get("apply_on_pass"))

    def test_morning_summary_covers_learning_fields(self) -> None:
        summary = build_morning_summary(
            {
                "attempted": ["director agent"],
                "changed": ["director_agent.py"],
                "failed": ["worker hook"],
                "noop": ["docstring loop"],
                "learned": ["no diff is not success"],
                "next": ["aider bridge discipline"],
                "risky_files": ["worker.py"],
                "prompts_worked": ["bounded mission prompt"],
                "prompts_failed": ["one-line docstring"],
            }
        )

        for heading in [
            "what was attempted",
            "what changed",
            "what failed",
            "what produced no diff",
            "what Luna learned",
            "what should be tried next",
            "which files are risky",
            "which prompts worked",
            "which prompts failed",
        ]:
            self.assertIn(heading, summary)


if __name__ == "__main__":
    unittest.main()
