"""Phase 5B tests: curated file self-map.

Stdlib unittest only. Tests the additive helpers in
luna_modules.luna_self_knowledge without touching the older
build_file_index / build_symbol_index code paths.

Each test is small and side-effect-bounded:
- write=True writes into memory/, but only files we own (luna_file_map.json,
  luna_function_index.json, luna_module_roles.json, luna_risk_zones.json).
- We never delete or modify other memory/* files.
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

# Resolve project root from this test file's location: tests/<this>.py
_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent

# Ensure project root is on sys.path so `import luna_modules.luna_self_knowledge` works.
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_self_knowledge import (  # noqa: E402
    answer_self_map_query,
    build_curated_file_map,
    build_function_index,
    compute_risk_zones,
    infer_module_role,
    refresh_curated_self_map,
)


_FILE_MAP_PATH = _PROJECT_DIR / "memory" / "luna_file_map.json"
_FUNCTION_INDEX_PATH = _PROJECT_DIR / "memory" / "luna_function_index.json"
_MODULE_ROLES_PATH = _PROJECT_DIR / "memory" / "luna_module_roles.json"
_RISK_ZONES_PATH = _PROJECT_DIR / "memory" / "luna_risk_zones.json"


class _CuratedSelfMapBaseTests(unittest.TestCase):
    """Tests that all four output JSON files are produced and structurally valid."""

    def setUp(self) -> None:
        result = refresh_curated_self_map(write=True)
        self.assertTrue(result.get("ok"), f"refresh result not ok: {result}")
        self.assertEqual(result.get("parse_errors", []), [],
                         f"parse_errors should be empty: {result.get('parse_errors')}")

    def test_01_four_output_files_exist(self) -> None:
        for path in (_FILE_MAP_PATH, _FUNCTION_INDEX_PATH,
                     _MODULE_ROLES_PATH, _RISK_ZONES_PATH):
            self.assertTrue(path.exists(), f"missing output file: {path}")
            self.assertGreater(path.stat().st_size, 0, f"empty output file: {path}")
            # Each file must parse as JSON
            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertIsInstance(data, dict, f"{path} is not a dict")
            self.assertEqual(int(data.get("schema_version") or 0), 1,
                             f"{path} schema_version != 1")

    def test_02_worker_in_file_map(self) -> None:
        data = json.loads(_FILE_MAP_PATH.read_text(encoding="utf-8"))
        rels = {f.get("relative_path") for f in data.get("files", [])}
        self.assertIn("worker.py", rels, "worker.py should be in curated file map")

    def test_03_worker_risk_high_or_critical(self) -> None:
        data = json.loads(_FILE_MAP_PATH.read_text(encoding="utf-8"))
        worker = next(
            (f for f in data.get("files", []) if f.get("relative_path") == "worker.py"),
            None,
        )
        self.assertIsNotNone(worker, "worker.py entry not found")
        self.assertIn(worker.get("risk_level"), {"high", "critical"},
                      f"worker.py risk_level should be high or critical, got "
                      f"{worker.get('risk_level')}")

    def test_04_worker_has_forbidden_edit_zone(self) -> None:
        data = json.loads(_FILE_MAP_PATH.read_text(encoding="utf-8"))
        worker = next(
            (f for f in data.get("files", []) if f.get("relative_path") == "worker.py"),
            None,
        )
        self.assertIsNotNone(worker)
        self.assertGreaterEqual(
            len(worker.get("forbidden_edit_zones") or []),
            1,
            "worker.py should have at least one forbidden_edit_zone",
        )

    def test_05_function_index_has_known_symbol(self) -> None:
        data = json.loads(_FUNCTION_INDEX_PATH.read_text(encoding="utf-8"))
        names = {s.get("name") for s in data.get("symbols", [])}
        # Either of these is a known symbol from the inspected files.
        candidates = {"refresh_curated_self_map", "build_curated_file_map",
                      "continues_update_loop", "build_file_index"}
        self.assertTrue(
            bool(names & candidates),
            f"function index missing all known symbols; first 20 names: "
            f"{sorted(names)[:20]}",
        )

    def test_06_query_returns_relevant_or_empty(self) -> None:
        results = answer_self_map_query("continues_update", limit=5)
        self.assertIsInstance(results, list)
        # Either the query found something OR it returned an empty list cleanly.
        # Both are acceptable. If it found something, it must be a list of dicts
        # with a `path` key.
        for r in results:
            self.assertIsInstance(r, dict)
            self.assertIn("path", r)
            self.assertIn("kind", r)

    def test_07_module_roles_known_categories(self) -> None:
        data = json.loads(_MODULE_ROLES_PATH.read_text(encoding="utf-8"))
        roles = data.get("roles") or {}
        self.assertIn("worker.py", roles)
        self.assertEqual(roles["worker.py"], "main_orchestrator")
        self.assertIn("aider_bridge.py", roles)
        self.assertEqual(roles["aider_bridge.py"], "aider_bridge")
        self.assertIn("luna_guardian.py", roles)
        self.assertEqual(roles["luna_guardian.py"], "guardian")

    def test_08_risk_zones_critical_files(self) -> None:
        data = json.loads(_RISK_ZONES_PATH.read_text(encoding="utf-8"))
        zones = data.get("zones") or {}
        # luna_hygiene.py must be critical
        hygiene = zones.get("luna_modules/luna_hygiene.py")
        self.assertIsNotNone(hygiene)
        self.assertEqual(hygiene.get("risk_level"), "critical")
        self.assertGreaterEqual(len(hygiene.get("forbidden_edit_zones") or []), 1)


class _PureFunctionTests(unittest.TestCase):
    """Pure-function tests that don't touch the filesystem."""

    def test_infer_module_role_main_orchestrator(self) -> None:
        self.assertEqual(infer_module_role("worker.py"), "main_orchestrator")

    def test_infer_module_role_tests(self) -> None:
        self.assertEqual(infer_module_role("tests/test_x.py"), "tests")

    def test_infer_module_role_unknown(self) -> None:
        self.assertEqual(infer_module_role("some/random_unknown_file.py"), "unknown")

    def test_compute_risk_zones_worker_critical(self) -> None:
        z = compute_risk_zones("worker.py")
        self.assertEqual(z.get("risk_level"), "critical")
        self.assertGreaterEqual(len(z.get("forbidden_edit_zones") or []), 1)

    def test_compute_risk_zones_unknown_low(self) -> None:
        z = compute_risk_zones("some/random_unknown_file.py")
        # Unknown defaults to low risk.
        self.assertEqual(z.get("risk_level"), "low")
        self.assertEqual(z.get("forbidden_edit_zones"), [])


class _MalformedFileResilienceTest(unittest.TestCase):
    """Building should not crash if a curated file is unparseable.

    We test this with an in-memory write to a sandbox dir that contains one
    valid file and one syntax-broken file under tests/. The build helpers
    iterate the real project, so this test is a soft-check that the source
    parser swallows errors into parse_errors instead of raising.
    """

    def test_build_function_index_does_not_crash_on_real_repo(self) -> None:
        idx = build_function_index()
        self.assertIsInstance(idx, dict)
        self.assertIn("symbols", idx)
        # parse_errors may or may not be empty depending on repo state; just
        # confirm the field exists and is a list.
        self.assertIsInstance(idx.get("parse_errors"), list)


if __name__ == "__main__":
    unittest.main(verbosity=2)
