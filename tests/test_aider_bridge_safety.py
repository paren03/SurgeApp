"""Safety tests for Luna's Aider Bridge rescue hardening."""

from __future__ import annotations

import tempfile
import unittest
import subprocess
import sys
from pathlib import Path

import aider_bridge


class TestAiderBridgeSafety(unittest.TestCase):
    def test_windowsapps_python_is_rejected(self) -> None:
        self.assertFalse(
            aider_bridge._is_safe_aider_python(
                r"C:\Users\paren\AppData\Local\Microsoft\WindowsApps\python.exe"
            )
        )
        self.assertFalse(
            aider_bridge._is_safe_aider_python(
                r"C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_x64\pythonw.exe"
            )
        )

    def test_aider_environment_sets_ollama_base(self) -> None:
        env = aider_bridge._aider_subprocess_env()

        self.assertEqual(env["OLLAMA_API_BASE"], "http://127.0.0.1:11434")
        self.assertEqual(env["PYTHONIOENCODING"], "utf-8")

    def test_hidden_safe_flags_are_present(self) -> None:
        flags = aider_bridge.AIDER_FLAGS

        for expected in [
            "--no-pretty",
            "--no-stream",
            "--no-detect-urls",
            "--no-restore-chat-history",
            "--no-gitignore",
            "--no-auto-commits",
            "--no-show-model-warnings",
            "--map-tokens",
            "0",
            "--map-refresh",
            "manual",
            "--max-chat-history-tokens",
            "512",
            "--edit-format",
            "diff",
        ]:
            self.assertIn(expected, flags)

    def test_oversized_target_requires_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "large.py"
            target.write_text("x = 1\n" * 40000, encoding="utf-8")

            allowed, reason = aider_bridge._target_scope_allowed(target, {})
            scoped, scoped_reason = aider_bridge._target_scope_allowed(
                target, {"function_scope": "run_aider_patch"}
            )

            self.assertFalse(allowed)
            self.assertEqual(reason, "oversized_target_requires_scope")
            self.assertTrue(scoped)
            self.assertEqual(scoped_reason, "")

    def test_dirty_target_is_detected_from_git_status(self) -> None:
        original_run = aider_bridge.subprocess.run

        class FakeResult:
            returncode = 0
            stdout = "MM aider_bridge.py\n"

        try:
            aider_bridge.subprocess.run = lambda *args, **kwargs: FakeResult()

            self.assertTrue(aider_bridge._target_has_local_edits(Path("aider_bridge.py")))
        finally:
            aider_bridge.subprocess.run = original_run

    def test_local_service_urls_are_blocked_from_prompts(self) -> None:
        self.assertTrue(
            aider_bridge._prompt_has_blocked_url(
                "please scrape http://127.0.0.1:11434/api/tags"
            )
        )
        self.assertTrue(aider_bridge._prompt_has_blocked_url("open http://localhost:11434"))
        self.assertFalse(aider_bridge._prompt_has_blocked_url("improve retry handling"))

    def test_pid_alive_requires_bridge_command_marker_when_requested(self) -> None:
        original_command_line = aider_bridge._process_command_line

        try:
            aider_bridge._process_command_line = lambda pid: "python.exe D:\\Other\\not_luna.py"
            self.assertFalse(aider_bridge._pid_alive(14280, "aider_bridge.py"))

            aider_bridge._process_command_line = lambda pid: "pythonw.exe D:\\SurgeApp\\aider_bridge.py"
            self.assertTrue(aider_bridge._pid_alive(14280, "aider_bridge.py"))
        finally:
            aider_bridge._process_command_line = original_command_line

    def test_verify_smoke_exits_without_watch_loop(self) -> None:
        result = subprocess.run(
            [sys.executable, str(Path(aider_bridge.__file__).resolve()), "--verify-smoke"],
            cwd=str(Path(aider_bridge.__file__).resolve().parent),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("aider bridge smoke ok", result.stdout)

    def test_parent_launcher_pid_does_not_block_bridge_start(self) -> None:
        self.assertFalse(aider_bridge._bridge_pid_blocks_start(200, my_pid=300, parent_pid=200))


if __name__ == "__main__":
    unittest.main()
