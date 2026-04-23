"""Unit tests for luna_modules.luna_missions.

Run with:  python -m pytest tests/test_luna_missions.py -v
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# ── Point MISSIONS_DIR at a temp directory for every test ────────────────────
_TEMP_DIR: Path = Path(tempfile.mkdtemp(prefix="luna_test_missions_"))


def setUpModule() -> None:
    _TEMP_DIR.mkdir(parents=True, exist_ok=True)


def tearDownModule() -> None:
    shutil.rmtree(str(_TEMP_DIR), ignore_errors=True)


# Patch MISSIONS_DIR before importing the module under test so every
# internal Path reference uses the temp directory.
with patch("luna_modules.luna_paths.MISSIONS_DIR", _TEMP_DIR):
    import luna_modules.luna_missions as _lm_module
    # Re-bind the module-level constant so helpers pick up the patch.
    _lm_module.MISSIONS_DIR = _TEMP_DIR
    from luna_modules.luna_missions import (
        LunaMission,
        list_missions,
        load_mission,
        start_new_mission,
        update_mission_status,
        _mission_path,
    )


class TestLunaMissionClass(unittest.TestCase):
    """LunaMission dataclass and to_dict()."""

    def test_mission_id_format(self) -> None:
        m = LunaMission("Title", "Objective")
        self.assertTrue(m.mission_id.startswith("MSN-"), m.mission_id)
        self.assertEqual(len(m.mission_id), 10)  # "MSN-" + 6 hex chars

    def test_unique_ids(self) -> None:
        ids = {LunaMission("t", "o").mission_id for _ in range(50)}
        self.assertEqual(len(ids), 50, "mission_ids should be unique")

    def test_default_status(self) -> None:
        m = LunaMission("t", "o")
        self.assertEqual(m.status, "INITIALIZING")

    def test_default_tasks_empty(self) -> None:
        m = LunaMission("t", "o")
        self.assertEqual(m.tasks, [])

    def test_to_dict_keys(self) -> None:
        m = LunaMission("Deploy", "Improve coverage")
        d = m.to_dict()
        self.assertEqual(
            set(d.keys()),
            {"mission_id", "title", "objective", "tasks", "status"},
        )

    def test_to_dict_values(self) -> None:
        m = LunaMission("Deploy", "Improve coverage")
        d = m.to_dict()
        self.assertEqual(d["title"], "Deploy")
        self.assertEqual(d["objective"], "Improve coverage")
        self.assertEqual(d["status"], "INITIALIZING")
        self.assertIsInstance(d["tasks"], list)

    def test_to_dict_is_serialisable(self) -> None:
        m = LunaMission("T", "O")
        # Must not raise
        json.dumps(m.to_dict())


class TestMissionPath(unittest.TestCase):
    """_mission_path() returns a Path under MISSIONS_DIR."""

    def test_returns_path_object(self) -> None:
        p = _mission_path("MSN-AABBCC")
        self.assertIsInstance(p, Path)

    def test_filename(self) -> None:
        p = _mission_path("MSN-AABBCC")
        self.assertEqual(p.name, "MSN-AABBCC.json")

    def test_parent_is_missions_dir(self) -> None:
        p = _mission_path("MSN-AABBCC")
        self.assertEqual(p.parent, _TEMP_DIR)


class TestStartNewMission(unittest.TestCase):
    """start_new_mission() — creation and persistence."""

    def test_returns_mission_id(self) -> None:
        mid = start_new_mission("Alpha", "Objective A")
        self.assertTrue(mid.startswith("MSN-"))

    def test_creates_json_file(self) -> None:
        mid = start_new_mission("Beta", "Objective B")
        self.assertTrue(_mission_path(mid).exists())

    def test_persisted_content(self) -> None:
        mid = start_new_mission("Gamma", "Objective G")
        data = json.loads(_mission_path(mid).read_text(encoding="utf-8"))
        self.assertEqual(data["mission_id"], mid)
        self.assertEqual(data["title"], "Gamma")
        self.assertEqual(data["objective"], "Objective G")
        self.assertEqual(data["status"], "INITIALIZING")
        self.assertEqual(data["tasks"], [])

    def test_each_call_creates_unique_file(self) -> None:
        ids = [start_new_mission("T", "O") for _ in range(5)]
        self.assertEqual(len(set(ids)), 5)
        for mid in ids:
            self.assertTrue(_mission_path(mid).exists())

    def test_missions_dir_created_if_absent(self) -> None:
        nested = _TEMP_DIR / "sub" / "nested"
        with patch.object(_lm_module, "MISSIONS_DIR", nested):
            mid = _lm_module.start_new_mission("Nested", "O")
        self.assertTrue((nested / f"{mid}.json").exists())
        shutil.rmtree(str(nested), ignore_errors=True)


class TestUpdateMissionStatus(unittest.TestCase):
    """update_mission_status() — mutation and persistence."""

    def setUp(self) -> None:
        self.mid = start_new_mission("Delta", "Objective D")

    def test_returns_true_on_success(self) -> None:
        result = update_mission_status(self.mid, "IN_PROGRESS")
        self.assertTrue(result)

    def test_status_persisted(self) -> None:
        update_mission_status(self.mid, "IN_PROGRESS")
        data = json.loads(_mission_path(self.mid).read_text(encoding="utf-8"))
        self.assertEqual(data["status"], "IN_PROGRESS")

    def test_multiple_transitions(self) -> None:
        for status in ("IN_PROGRESS", "BLOCKED", "COMPLETE"):
            self.assertTrue(update_mission_status(self.mid, status))
        data = json.loads(_mission_path(self.mid).read_text(encoding="utf-8"))
        self.assertEqual(data["status"], "COMPLETE")

    def test_other_fields_preserved(self) -> None:
        update_mission_status(self.mid, "IN_PROGRESS")
        data = json.loads(_mission_path(self.mid).read_text(encoding="utf-8"))
        self.assertEqual(data["title"], "Delta")
        self.assertEqual(data["objective"], "Objective D")
        self.assertEqual(data["tasks"], [])

    def test_returns_false_for_nonexistent_mission(self) -> None:
        result = update_mission_status("MSN-DOESNT", "COMPLETE")
        self.assertFalse(result)

    def test_arbitrary_status_string(self) -> None:
        update_mission_status(self.mid, "CUSTOM_STATE_XYZ")
        data = json.loads(_mission_path(self.mid).read_text(encoding="utf-8"))
        self.assertEqual(data["status"], "CUSTOM_STATE_XYZ")


class TestLoadMission(unittest.TestCase):
    """load_mission() — read-back from disk."""

    def test_returns_dict_for_existing(self) -> None:
        mid = start_new_mission("Epsilon", "Objective E")
        data = load_mission(mid)
        self.assertIsInstance(data, dict)
        self.assertEqual(data["mission_id"], mid)

    def test_returns_none_for_missing(self) -> None:
        result = load_mission("MSN-MISSING")
        self.assertIsNone(result)

    def test_reflects_status_update(self) -> None:
        mid = start_new_mission("Zeta", "Objective Z")
        update_mission_status(mid, "COMPLETE")
        data = load_mission(mid)
        self.assertEqual(data["status"], "COMPLETE")


class TestListMissions(unittest.TestCase):
    """list_missions() — directory scan and ordering."""

    def setUp(self) -> None:
        # Use a fresh sub-directory so counts are predictable
        self._sub = _TEMP_DIR / "list_test"
        self._sub.mkdir(parents=True, exist_ok=True)
        self._original = _lm_module.MISSIONS_DIR
        _lm_module.MISSIONS_DIR = self._sub

    def tearDown(self) -> None:
        _lm_module.MISSIONS_DIR = self._original
        shutil.rmtree(str(self._sub), ignore_errors=True)

    def test_empty_when_no_missions(self) -> None:
        self.assertEqual(list_missions(), [])

    def test_returns_all_missions(self) -> None:
        ids = [_lm_module.start_new_mission(f"T{i}", "O") for i in range(4)]
        missions = _lm_module.list_missions()
        returned_ids = {m["mission_id"] for m in missions}
        self.assertEqual(returned_ids, set(ids))

    def test_ignores_non_mission_files(self) -> None:
        (self._sub / "random_file.json").write_text("{}", encoding="utf-8")
        (self._sub / "README.txt").write_text("hi", encoding="utf-8")
        _lm_module.start_new_mission("Real", "O")
        missions = _lm_module.list_missions()
        self.assertEqual(len(missions), 1)

    def test_returns_none_when_dir_absent(self) -> None:
        shutil.rmtree(str(self._sub))
        self.assertEqual(_lm_module.list_missions(), [])


class TestIntegration(unittest.TestCase):
    """End-to-end workflow."""

    def test_full_lifecycle(self) -> None:
        mid = start_new_mission("Integration", "Full lifecycle test")
        self.assertEqual(load_mission(mid)["status"], "INITIALIZING")

        update_mission_status(mid, "IN_PROGRESS")
        self.assertEqual(load_mission(mid)["status"], "IN_PROGRESS")

        update_mission_status(mid, "COMPLETE")
        self.assertEqual(load_mission(mid)["status"], "COMPLETE")

        missions = list_missions()
        found = next((m for m in missions if m["mission_id"] == mid), None)
        self.assertIsNotNone(found)
        self.assertEqual(found["status"], "COMPLETE")

    def test_multiple_missions_independent(self) -> None:
        m1 = start_new_mission("Mission 1", "O1")
        m2 = start_new_mission("Mission 2", "O2")

        update_mission_status(m1, "COMPLETE")

        self.assertEqual(load_mission(m1)["status"], "COMPLETE")
        self.assertEqual(load_mission(m2)["status"], "INITIALIZING")


if __name__ == "__main__":
    unittest.main(verbosity=2)
