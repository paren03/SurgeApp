"""Tests for append-only autonomy memory routing."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path

from luna_modules.luna_memory_router import route_autonomy_summary


class TestLunaMemoryRouter(unittest.TestCase):
    def test_routes_summary_to_nightly_files(self) -> None:
        root = Path("temp_test_zone") / f"memory_router_{uuid.uuid4().hex[:8]}"

        result = route_autonomy_summary(root, {"attempted": "review gate", "learned": "pause on noop"})

        self.assertTrue(result["ok"])
        jsonl_path = root / "memory" / "nightly_updates.jsonl"
        md_path = root / "memory" / "nightly_updates.md"
        self.assertTrue(jsonl_path.exists())
        self.assertTrue(md_path.exists())
        record = json.loads(jsonl_path.read_text(encoding="utf-8").splitlines()[-1])
        self.assertEqual(record["kind"], "autonomy_summary")
        self.assertIn("pause on noop", md_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import subprocess

    subprocess.run(["pytest", "-v"], check=True)
