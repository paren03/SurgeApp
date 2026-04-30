"""Tests for Luna's two-pass continues_update review gate."""

from __future__ import annotations

import unittest

from luna_modules.luna_two_pass_review import build_two_pass_review


class TestLunaTwoPassReview(unittest.TestCase):
    def test_real_verified_diff_can_continue(self) -> None:
        review = build_two_pass_review({
            "status": "done",
            "diff_path": "logic_updates/job/job.diff",
            "verify_passed": True,
        })

        self.assertTrue(review["satisfied"])
        self.assertEqual(review["required_reviews"], 2)
        self.assertEqual(review["action"], "continue_next_task")

    def test_noop_pauses_for_inspection(self) -> None:
        review = build_two_pass_review({"status": "noop", "diff_path": "", "verify_passed": False})

        self.assertFalse(review["satisfied"])
        self.assertEqual(review["action"], "pause_for_inspection")
        self.assertEqual(review["reviews"][0]["reason"], "noop_is_not_upgrade")

    def test_failure_pauses_for_inspection(self) -> None:
        review = build_two_pass_review({"status": "failed", "summary": "target not found"})

        self.assertFalse(review["satisfied"])
        self.assertIn("target not found", review["reviews"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
