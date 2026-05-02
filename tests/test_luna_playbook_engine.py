"""Phase 5E tests: luna_playbook_engine.

Stdlib unittest only. Tests load the curated tracked seed file from the
real project (memory/luna_self_healing_playbooks_seed.json) — that is a
read-only, tracked config artifact. Every test that writes uses
TemporaryDirectory.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_playbook_engine import (  # noqa: E402
    ALLOWED_SEVERITIES,
    DEFAULT_SEED_PATH,
    REQUIRED_FIELDS,
    SCHEMA_VERSION,
    _FALLBACK_SEED,
    load_playbooks,
    match_playbooks,
    render_match_report,
    score_playbook,
    self_test,
    validate_playbook,
)


class _LoadAndValidateTests(unittest.TestCase):

    def test_01_seed_loads_and_has_at_least_16(self) -> None:
        loaded = load_playbooks()
        self.assertGreaterEqual(
            len(loaded["playbooks"]), 16,
            f"expected >=16 seed playbooks; got {len(loaded['playbooks'])}",
        )
        self.assertFalse(loaded["used_fallback"],
                         f"unexpected fallback; source={loaded['source']}")

    def test_02_every_seed_validates(self) -> None:
        loaded = load_playbooks()
        for v in loaded["validation"]:
            self.assertTrue(v["ok"],
                            f"playbook {v['playbook_id']} failed validation: {v['errors']}")

    def test_03_required_playbook_ids_present(self) -> None:
        ids = {p["playbook_id"] for p in load_playbooks()["playbooks"]}
        required = {
            "worker_import_failure",
            "py_compile_failure",
            "guardian_restart_storm",
            "stale_worker_heartbeat",
            "stale_aider_active_jobs",
            "aider_timeout",
            "aider_context_overflow",
            "cu_fake_busy_loop",
            "dirty_core_gate",
            "stale_status_file",
            "duplicate_process_false_positive",
            "ollama_unavailable",
            "ui_running_but_no_work",
            "oversized_runtime_logs",
            "unsafe_package_install_request",
            "broad_refactor_or_architecture_drift",
        }
        missing = required - ids
        self.assertFalse(missing, f"missing required playbooks: {missing}")

    def test_04_validate_catches_missing_fields(self) -> None:
        bad = {
            "playbook_id": "x",
            "title": "X",
            # missing failure_class, severity, etc.
        }
        ok, errors = validate_playbook(bad)
        self.assertFalse(ok)
        self.assertTrue(any("failure_class" in e for e in errors))
        self.assertTrue(any("severity" in e for e in errors))

    def test_05_validate_catches_bad_severity(self) -> None:
        loaded = load_playbooks()
        record = dict(loaded["playbooks"][0])
        record["severity"] = "kinda-bad"
        ok, errors = validate_playbook(record)
        self.assertFalse(ok)
        self.assertTrue(any("severity" in e for e in errors))

    def test_06_validate_catches_empty_safe_first_actions(self) -> None:
        loaded = load_playbooks()
        record = dict(loaded["playbooks"][0])
        record["safe_first_actions"] = []
        ok, errors = validate_playbook(record)
        self.assertFalse(ok)
        self.assertTrue(any("safe_first_actions" in e for e in errors))

    def test_07_module_fallback_seed_validates(self) -> None:
        # Sanity: even the tiny in-module fallback set is well-formed
        for record in _FALLBACK_SEED:
            ok, errors = validate_playbook(record)
            self.assertTrue(ok, f"fallback playbook invalid: {errors}")


class _MatchTests(unittest.TestCase):

    def test_08_match_worker_import_failed(self) -> None:
        matches = match_playbooks(
            "worker import failed ImportError hygiene", limit=3,
        )
        self.assertGreater(len(matches), 0)
        self.assertEqual(matches[0]["playbook"]["playbook_id"],
                         "worker_import_failure")
        self.assertGreater(matches[0]["score"], 0)

    def test_09_match_cu_fake_busy(self) -> None:
        matches = match_playbooks("CU_START CU_STOP fake busy loop", limit=3)
        self.assertGreater(len(matches), 0)
        self.assertEqual(matches[0]["playbook"]["playbook_id"],
                         "cu_fake_busy_loop")

    def test_10_match_aider_context_overflow(self) -> None:
        matches = match_playbooks("aider context limit exceeded", limit=3)
        self.assertGreater(len(matches), 0)
        self.assertEqual(matches[0]["playbook"]["playbook_id"],
                         "aider_context_overflow")

    def test_11_match_ollama_unavailable(self) -> None:
        matches = match_playbooks(
            "ollama /api/tags connection refused", limit=3,
        )
        self.assertGreater(len(matches), 0)
        self.assertEqual(matches[0]["playbook"]["playbook_id"],
                         "ollama_unavailable")

    def test_12_match_aider_timeout(self) -> None:
        matches = match_playbooks(
            "subprocess.TimeoutExpired aider_timeout long silence", limit=3,
        )
        self.assertGreater(len(matches), 0)
        # Top hit should be aider_timeout, but accept aider_context_overflow
        # as a near-miss only if both are tied (the tag overlap is real).
        top_id = matches[0]["playbook"]["playbook_id"]
        self.assertEqual(top_id, "aider_timeout",
                         f"expected aider_timeout, got {top_id}")

    def test_13_score_breakdown_exposed(self) -> None:
        loaded = load_playbooks()
        worker_pb = next(p for p in loaded["playbooks"]
                         if p["playbook_id"] == "worker_import_failure")
        score, breakdown = score_playbook(
            "ImportError importing worker hygiene", worker_pb,
        )
        self.assertGreater(score, 0)
        self.assertIn("signal", breakdown)
        self.assertIn("tag", breakdown)
        self.assertIn("token", breakdown)


class _ReportTests(unittest.TestCase):

    def test_14_markdown_report_includes_required_sections(self) -> None:
        matches = match_playbooks("worker import failed", limit=2)
        report = render_match_report(matches, query="worker import failed",
                                     out_format="markdown")
        # Required sections
        self.assertIn("Luna Self-Healing Playbook Match Report", report)
        # Top match — its title and class should appear
        top = matches[0]["playbook"]
        self.assertIn(top["title"], report)
        self.assertIn(top["failure_class"], report)
        # The "Safe first actions" header must be present
        self.assertIn("Safe first actions", report)
        # And at least one unsafe-actions header (worker_import_failure has them)
        self.assertIn("Unsafe actions", report)

    def test_15_json_report_is_valid_json(self) -> None:
        matches = match_playbooks("aider context limit", limit=2)
        report = render_match_report(matches, out_format="json")
        parsed = json.loads(report)
        self.assertEqual(parsed["schema_version"], SCHEMA_VERSION)
        self.assertGreaterEqual(parsed["match_count"], 1)
        self.assertIsInstance(parsed["matches"], list)


class _OptionalIntegrationTests(unittest.TestCase):

    def test_16_memory_index_failure_is_tolerated(self) -> None:
        # Force the optional luna_memory_index import to fail at module level.
        # We simulate this by patching sys.modules so that a 'broken' replacement
        # raises on attribute access. Even with that broken state, match_playbooks
        # must still return results.
        import sys as _sys
        original = _sys.modules.get("luna_modules.luna_memory_index")
        try:
            _sys.modules["luna_modules.luna_memory_index"] = None  # makes import fail
            matches = match_playbooks("worker import failed", limit=2)
            self.assertGreater(len(matches), 0,
                               "matching must still work without memory index")
        finally:
            if original is not None:
                _sys.modules["luna_modules.luna_memory_index"] = original
            elif "luna_modules.luna_memory_index" in _sys.modules:
                del _sys.modules["luna_modules.luna_memory_index"]


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_playbook_engine", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

    def test_17_cli_self_test_exits_clean(self) -> None:
        result = self._run("--self-test")
        self.assertEqual(result.returncode, 0,
                         f"--self-test exit != 0: stdout={result.stdout!r} "
                         f"stderr={result.stderr!r}")
        self.assertIn("\"ok\": true", result.stdout)

    def test_18_cli_match_returns_zero_with_text(self) -> None:
        result = self._run("--match", "worker import failed")
        self.assertEqual(result.returncode, 0,
                         f"--match exit != 0: stderr={result.stderr!r}")
        self.assertIn("worker", result.stdout.lower())
        self.assertIn("Safe first actions", result.stdout)

    def test_19_cli_match_returns_zero_for_cu_fake_busy(self) -> None:
        result = self._run("--match", "CU_START CU_STOP rapid loop")
        self.assertEqual(result.returncode, 0)
        self.assertIn("cu_fake_busy_loop", result.stdout)

    def test_20_self_test_function_returns_zero(self) -> None:
        # Direct call (no subprocess) — proves the function path also works.
        rc = self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
