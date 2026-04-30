"""Tests for Guardian duplicate-process prevention."""

from __future__ import annotations

import json
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import luna_guardian


PROJECT_TEMP = Path(r"D:\SurgeApp\temp_test_zone")


def _test_dir(name: str) -> Path:
    path = PROJECT_TEMP / f"{name}_{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=True)
    return path


class TestLunaGuardian(unittest.TestCase):
    def test_pid_alive_uses_tasklist_output(self) -> None:
        fake = SimpleNamespace(stdout=" 1234 pythonw.exe", returncode=0)
        with patch.object(luna_guardian.subprocess, "run", return_value=fake):
            self.assertTrue(luna_guardian._pid_alive(1234))

    def test_process_scan_prevents_duplicate_launch(self) -> None:
        project = _test_dir("guardian_scan")
        script = project / "worker.py"
        script.write_text("print('worker')\n", encoding="utf-8")
        lock = project / "logs" / "worker.lock.json"
        rows = [{"command": "pythonw.exe D:\\SurgeApp\\worker.py", "name": "pythonw.exe", "pid": "4321"}]

        with patch.object(luna_guardian, "PROJECT_DIR", project), patch.object(
            luna_guardian, "SERVICE_LOCKS", {"worker": lock}
        ), patch.object(luna_guardian, "_process_rows", return_value=rows), patch.object(
            luna_guardian.subprocess, "Popen"
        ) as popen:
            started = luna_guardian._start_service("worker", "worker.py")

        self.assertFalse(started)
        self.assertFalse(popen.called)
        self.assertEqual(json.loads(lock.read_text(encoding="utf-8"))["pid"], 4321)

    def test_process_scan_filters_to_python_processes(self) -> None:
        fake = SimpleNamespace(stdout="[]", returncode=0)
        with patch.object(luna_guardian.subprocess, "run", return_value=fake) as run:
            luna_guardian._process_rows()

        command = " ".join(run.call_args.args[0])
        self.assertIn("$_.Name -match '^python'", command)

    def test_matching_processes_ignores_aider_child_target_file_mentions(self) -> None:
        rows = [
            {
                "command": (
                    "python.exe -m aider --file "
                    "D:\\SurgeApp\\logic_updates\\job\\aider_bridge.py "
                    "--message \"Target files: aider_bridge.py\""
                ),
                "name": "python.exe",
                "pid": "2001",
            },
            {
                "command": "pythonw.exe D:\\SurgeApp\\aider_bridge.py",
                "name": "pythonw.exe",
                "pid": "2002",
            },
        ]

        with patch.object(luna_guardian, "_process_rows", return_value=rows):
            matches = luna_guardian._matching_processes("aider_bridge.py")

        self.assertEqual([row["pid"] for row in matches], ["2002"])

    def test_status_file_records_service_snapshot(self) -> None:
        project = _test_dir("guardian_status")
        status_path = project / "memory" / "luna_guardian_status.json"
        rows = [
            {"command": "pythonw.exe D:\\SurgeApp\\worker.py", "name": "pythonw.exe", "pid": "1001"},
            {"command": "pythonw.exe D:\\SurgeApp\\aider_bridge.py", "name": "pythonw.exe", "pid": "1002"},
        ]

        with patch.object(luna_guardian, "PROJECT_DIR", project), patch.object(
            luna_guardian, "GUARDIAN_STATUS_PATH", status_path
        ), patch.object(
            luna_guardian,
            "SERVICE_SCRIPTS",
            {"worker": "worker.py", "aider_bridge": "aider_bridge.py"},
        ), patch.object(
            luna_guardian,
            "SERVICE_LOCKS",
            {"worker": project / "logs" / "worker.lock.json", "aider_bridge": project / "logs" / "aider_bridge.pid"},
        ), patch.object(luna_guardian, "_process_rows", return_value=rows), patch.object(
            luna_guardian, "_pid_alive", return_value=False
        ):
            luna_guardian._write_status({"worker": False, "aider_bridge": False})

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "services_healthy")
        self.assertEqual(payload["services"]["worker"]["pid"], 1001)
        self.assertEqual(payload["services"]["aider_bridge"]["pid"], 1002)

    def test_duplicate_service_processes_keep_lock_pid_and_stop_extras(self) -> None:
        project = _test_dir("guardian_dedupe")
        lock = project / "logs" / "aider_bridge.pid"
        lock.parent.mkdir(parents=True)
        lock.write_text("1001", encoding="utf-8")
        rows = [
            {"command": "pythonw.exe D:\\SurgeApp\\aider_bridge.py", "name": "pythonw.exe", "pid": "1002"},
            {"command": "pythonw.exe D:\\SurgeApp\\aider_bridge.py", "name": "pythonw.exe", "pid": "1001"},
        ]
        killed: list[str] = []

        def fake_run(args, **kwargs):
            if args[:3] == ["taskkill", "/F", "/PID"]:
                killed.append(args[3])
            return SimpleNamespace(stdout="", stderr="", returncode=0)

        with patch.object(luna_guardian, "SERVICE_LOCKS", {"aider_bridge": lock}), patch.object(
            luna_guardian, "_process_rows", return_value=rows
        ), patch.object(luna_guardian, "_pid_alive", side_effect=lambda pid, marker="": pid == 1001), patch.object(
            luna_guardian.subprocess, "run", side_effect=fake_run
        ):
            stopped = luna_guardian._dedupe_service_processes("aider_bridge", "aider_bridge.py")

        self.assertEqual(stopped, [1002])
        self.assertEqual(lock.read_text(encoding="utf-8"), "1001")

    def test_aider_wrapper_child_pair_is_not_treated_as_duplicate(self) -> None:
        project = _test_dir("guardian_aider_pair")
        lock = project / "logs" / "aider_bridge.pid"
        lock.parent.mkdir(parents=True)
        rows = [
            {
                "command": "D:\\SurgeApp\\.aider_venv\\Scripts\\pythonw.exe D:\\SurgeApp\\aider_bridge.py",
                "name": "pythonw.exe",
                "parent_pid": "9500",
                "pid": "41012",
            },
            {
                "command": "pythonw.exe D:\\SurgeApp\\aider_bridge.py",
                "name": "pythonw.exe",
                "parent_pid": "41012",
                "pid": "16652",
            },
        ]

        with patch.object(luna_guardian, "SERVICE_LOCKS", {"aider_bridge": lock}), patch.object(
            luna_guardian, "_process_rows", return_value=rows
        ), patch.object(luna_guardian, "_terminate_pid") as terminate:
            stopped = luna_guardian._dedupe_service_processes("aider_bridge", "aider_bridge.py")

        self.assertEqual(stopped, [])
        self.assertFalse(terminate.called)
        self.assertEqual(lock.read_text(encoding="utf-8"), "16652")

    def test_worker_dedupe_ignores_continues_update_process(self) -> None:
        project = _test_dir("guardian_worker_cu")
        lock = project / "logs" / "luna_worker.lock.json"
        lock.parent.mkdir(parents=True)
        lock.write_text(json.dumps({"pid": 1001}), encoding="utf-8")
        rows = [
            {"command": "pythonw.exe D:\\SurgeApp\\worker.py", "name": "pythonw.exe", "pid": "1001"},
        ]

        def fake_matching(marker: str, *, exclude: str = ""):
            self.assertEqual(exclude, "--continues-update-start")
            return rows

        with patch.object(luna_guardian, "SERVICE_LOCKS", {"worker": lock}), patch.object(
            luna_guardian, "_matching_processes", side_effect=fake_matching
        ), patch.object(luna_guardian, "_terminate_pid") as terminate:
            stopped = luna_guardian._dedupe_service_processes("worker", "worker.py")

        self.assertEqual(stopped, [])
        self.assertFalse(terminate.called)

    def test_pid_alive_requires_matching_command_marker_when_requested(self) -> None:
        with patch.object(luna_guardian, "_process_command_line_for_pid", return_value="python.exe D:\\Other\\tool.py"):
            self.assertFalse(luna_guardian._pid_alive(14280, "aider_bridge.py"))

        with patch.object(
            luna_guardian,
            "_process_command_line_for_pid",
            return_value="pythonw.exe D:\\SurgeApp\\aider_bridge.py",
        ):
            self.assertTrue(luna_guardian._pid_alive(14280, "aider_bridge.py"))


if __name__ == "__main__":
    unittest.main()
