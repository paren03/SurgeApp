"""Phase 5G tests: luna_capability_scorecard.

Stdlib unittest only. Tests run against TemporaryDirectory fixtures so they
don't depend on the live project state.
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

from luna_modules.luna_capability_scorecard import (  # noqa: E402
    REQUIRED_DIMENSIONS,
    SCHEMA_VERSION,
    _build_temp_fixture,
    build_capability_scorecard,
    clamp_score,
    load_config,
    parse_verifier_report,
    render_scorecard_markdown,
    self_test,
    validate_scorecard,
    weighted_average,
    write_scorecard,
)


def _scorecard_for_temp(td: Path):
    cfg = _build_temp_fixture(td)
    return build_capability_scorecard(td, cfg), cfg


class _PureHelperTests(unittest.TestCase):

    def test_01_clamp_lower_bound(self) -> None:
        self.assertEqual(clamp_score(-5), 0)
        self.assertEqual(clamp_score(0), 0)
        self.assertEqual(clamp_score("not a number"), 0)
        self.assertEqual(clamp_score(None), 0)

    def test_02_clamp_upper_bound(self) -> None:
        self.assertEqual(clamp_score(100), 100)
        self.assertEqual(clamp_score(999), 100)
        self.assertEqual(clamp_score(73.7), 74)

    def test_03_weighted_average(self) -> None:
        self.assertEqual(weighted_average([(100, 1), (0, 1)]), 50)
        self.assertEqual(weighted_average([(80, 2), (40, 1)]), 67)
        self.assertEqual(weighted_average([]), 0)
        self.assertEqual(weighted_average([(50, 0)]), 0)


class _VerifierParseTests(unittest.TestCase):

    def test_04_clean_report(self) -> None:
        text = (
            "============================================================\n"
            "8. Summary\n"
            "============================================================\n"
            "[PASS] No hard failures found.\n"
            "[PASS] No warnings found.\n"
        )
        result = parse_verifier_report(text)
        self.assertTrue(result["found"])
        self.assertEqual(result["hard_failures"], 0)
        self.assertEqual(result["warnings"], 0)
        self.assertEqual(result["summary"], "clean")

    def test_05_failure_report(self) -> None:
        text = "[FAIL] worker.py compile error\n[FAIL] aider_bridge.py syntax\n"
        result = parse_verifier_report(text)
        self.assertTrue(result["found"])
        self.assertGreater(result["hard_failures"], 0)
        self.assertEqual(result["summary"], "failures_present")


class _MissingFilesGracefulTests(unittest.TestCase):

    def test_06_empty_temp_dir_returns_record(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record = build_capability_scorecard(td)
            ok, errs = validate_scorecard(record)
            self.assertTrue(ok, f"validation errors: {errs}")
            self.assertEqual(len(record["dimensions"]), len(REQUIRED_DIMENSIONS))

    def test_07_status_derivation_blocked_when_evidence_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record = build_capability_scorecard(td)
            blocked_or_degraded = [
                d for d in record["dimensions"] if d["status"] in ("blocked", "degraded", "watch")
            ]
            self.assertGreater(len(blocked_or_degraded), 0)


class _BuildScorecardTests(unittest.TestCase):

    def test_08_build_with_temp_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            self.assertEqual(record["schema_version"], SCHEMA_VERSION)
            self.assertEqual(len(record["dimensions"]), len(REQUIRED_DIMENSIONS))
            self.assertIn(record["overall_status"], ("excellent", "healthy", "watch", "degraded", "blocked", "unknown"))
            self.assertIn(
                record["readiness_level"],
                ("read_only", "safe_foundation", "controlled_autonomy_ready", "limited_self_upgrade_ready", "not_ready"),
            )

    def test_09_required_dimensions_present(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            names = [d["name"] for d in record["dimensions"]]
            for required in REQUIRED_DIMENSIONS:
                self.assertIn(required, names)


class _ValidationTests(unittest.TestCase):

    def test_10_validate_catches_missing_fields(self) -> None:
        bad = {"schema_version": 1}
        ok, errs = validate_scorecard(bad)
        self.assertFalse(ok)
        self.assertGreater(len(errs), 0)

    def test_11_validate_catches_bad_status(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            record["overall_status"] = "BOGUS"
            ok, errs = validate_scorecard(record)
            self.assertFalse(ok)
            self.assertTrue(any("overall_status" in e for e in errs))

    def test_12_validate_catches_score_out_of_range(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            record["dimensions"][0]["score"] = 999
            ok, errs = validate_scorecard(record)
            self.assertFalse(ok)


class _RenderTests(unittest.TestCase):

    def test_13_markdown_contains_required_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            md = render_scorecard_markdown(record)
            self.assertIn("Luna Capability Scorecard", md)
            self.assertIn("Overall score", md)
            for required in REQUIRED_DIMENSIONS:
                self.assertIn(required, md)


class _WritePathSafetyTests(unittest.TestCase):

    def test_14_write_rejects_path_outside_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            outside = Path(tempfile.gettempdir()) / "should_not_write.json"
            with self.assertRaises(ValueError):
                write_scorecard(record, outside, project_root=td)

    def test_15_write_into_temp_root_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record, _cfg = _scorecard_for_temp(td)
            json_p = td / "memory" / "scorecard.json"
            md_p = td / "memory" / "scorecard.md"
            rp = td / "memory" / "scorecard_report.json"
            written = write_scorecard(record, json_p, md_p, rp, project_root=td)
            self.assertTrue(json_p.is_file())
            self.assertTrue(md_p.is_file())
            self.assertTrue(rp.is_file())
            self.assertIn("json", written)


class _ReadinessTests(unittest.TestCase):

    def test_16_conservative_when_evidence_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record = build_capability_scorecard(td)
            self.assertIn(
                record["readiness_level"],
                ("not_ready", "safe_foundation"),
                f"got readiness={record['readiness_level']!r} for empty fixture",
            )

    def test_17_critical_blocker_lowers_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            record = build_capability_scorecard(td)
            blockers = sum(1 for d in record["dimensions"] if d["status"] == "blocked")
            if blockers > 0:
                self.assertIn(
                    record["readiness_level"],
                    ("not_ready", "safe_foundation"),
                )


class _SelfTestTests(unittest.TestCase):

    def test_18_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_capability_scorecard", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

    def test_19_cli_self_test_exits_clean(self) -> None:
        result = self._run("--self-test")
        self.assertEqual(
            result.returncode,
            0,
            f"--self-test rc={result.returncode}: stderr={result.stderr!r}",
        )
        parsed = json.loads(result.stdout)
        self.assertTrue(parsed.get("ok"))


class _NoNetworkTests(unittest.TestCase):

    def test_20_no_socket_imports_in_module(self) -> None:
        mod = _PROJECT_DIR / "luna_modules" / "luna_capability_scorecard.py"
        text = mod.read_text(encoding="utf-8")
        self.assertNotIn("import socket", text)
        self.assertNotIn("import urllib", text)
        self.assertNotIn("import requests", text)
        self.assertNotIn("http.client", text)


class _LoadConfigTests(unittest.TestCase):

    def test_21_load_config_fallback_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            cfg = load_config(Path(td_str) / "nope.json")
            self.assertEqual(cfg["schema_version"], 1)
            self.assertFalse(cfg.get("_loaded_from_file"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
