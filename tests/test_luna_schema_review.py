"""Phase 6A tests: Luna Schema/Policy Review Helper.

30+ tests covering scan/classify/detect/report/verdict/CLI/source-safety paths.
All tests use TemporaryDirectory or read-only inspections; no real project
files are modified.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_schema_review as sr


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    (tmp / "Luna New UpGrades").mkdir(parents=True, exist_ok=True)
    return tmp


def _write_clean_seed(pdir: Path) -> None:
    (pdir / "memory" / "luna_clean_policy.json").write_text(
        json.dumps({
            "schema_version": 1,
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
            "live_enforcement_enabled": False,
            "allow_aider": False,
            "allow_code_edits": False,
        }),
        encoding="utf-8",
    )


def _write_all_expected(pdir: Path) -> None:
    """Seed all expected files from sr._EXPECTED_FILES with safe content."""
    for rel in sr._EXPECTED_FILES:
        p = pdir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps({
                "schema_version": 1,
                "advisory_only": True,
                "safe_to_execute_now": False,
                "safe_to_apply_real_project": False,
                "guardian_enforcing_live": False,
                "live_enforcement_enabled": False,
                "allow_aider": False,
                "allow_code_edits": False,
            }),
            encoding="utf-8",
        )


# ── 1: scanning ──────────────────────────────────────────────────────────────

class TestScanning(unittest.TestCase):
    def test_01_scans_temp_memory_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_clean_seed(pdir)
            files = sr.iter_candidate_policy_schema_files(pdir)
            self.assertIn("memory/luna_clean_policy.json", files)


# ── 2-5: classification ──────────────────────────────────────────────────────

class TestClassification(unittest.TestCase):
    def test_02_classifies_schema(self):
        kind = sr.classify_file_kind("memory/luna_x.schema.json", {"schema_version": 1})
        self.assertEqual(kind, "schema")

    def test_03_classifies_policy(self):
        kind = sr.classify_file_kind("memory/luna_x_policy.json", {})
        self.assertEqual(kind, "policy")

    def test_04_classifies_checklist(self):
        kind = sr.classify_file_kind(
            "memory/luna_live_enforcement_readiness_checklist.json",
            {"required_before_live_guardian_enforcement": []},
        )
        self.assertEqual(kind, "checklist")

    def test_05_classifies_roadmap(self):
        kind = sr.classify_file_kind("memory/luna_aider_tutor_mode_roadmap.json", {})
        self.assertEqual(kind, "roadmap")


# ── 6-10: dangerous flag detection ───────────────────────────────────────────

class TestDangerousFlags(unittest.TestCase):
    def test_06_detects_allow_execution_true(self):
        flags = sr.detect_dangerous_policy_flags(
            "x.json", {"allow_execution": True},
        )
        self.assertTrue(any("allow_execution" in d for d in flags["dangerous"]))

    def test_07_detects_safe_to_execute_now_true(self):
        flags = sr.detect_dangerous_policy_flags(
            "x.json", {"safe_to_execute_now": True},
        )
        self.assertTrue(any("safe_to_execute_now" in d for d in flags["dangerous"]))

    def test_08_detects_guardian_enforcing_live_true(self):
        flags = sr.detect_dangerous_policy_flags(
            "x.json", {"guardian_enforcing_live": True},
        )
        self.assertTrue(any("guardian_enforcing_live" in d for d in flags["dangerous"]))

    def test_09_detects_allow_aider_true(self):
        flags = sr.detect_dangerous_policy_flags(
            "x.json", {"allow_aider": True},
        )
        self.assertTrue(any("allow_aider" in d for d in flags["dangerous"]))

    def test_10_detects_safe_false_fields(self):
        flags = sr.detect_dangerous_policy_flags(
            "x.json", {
                "safe_to_execute_now": False,
                "guardian_enforcing_live": False,
                "advisory_only": True,  # safe positive
            },
        )
        # Both False flags should be recorded as safe.
        self.assertTrue(any("safe_to_execute_now" in s for s in flags["safe"]))
        self.assertTrue(any("guardian_enforcing_live" in s for s in flags["safe"]))


# ── 11-12: hashing + parsing ─────────────────────────────────────────────────

class TestHashingAndParsing(unittest.TestCase):
    def test_11_computes_sha256(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x.json"
            p.write_text("{}", encoding="utf-8")
            h = sr.sha256_file(p)
            self.assertEqual(len(h), 64)
            self.assertTrue(all(c in "0123456789abcdef" for c in h))

    def test_12_handles_malformed_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            (pdir / "memory" / "luna_broken_policy.json").write_text(
                "this is not valid json {{}", encoding="utf-8",
            )
            rec = sr.review_policy_schema_file(
                "memory/luna_broken_policy.json", pdir,
            )
            self.assertFalse(rec["parse_ok"])
            self.assertIn("json_parse_failed", rec["parse_error"])


# ── 13-16: report verdicts ───────────────────────────────────────────────────

class TestReportVerdicts(unittest.TestCase):
    def test_13_reports_missing_expected_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_clean_seed(pdir)
            report = sr.build_schema_review_report(pdir)
            self.assertGreater(report["missing_expected_files_count"], 0)
            self.assertEqual(report["verdict"], "INCOMPLETE_MISSING_FILES")

    def test_14_pass_when_files_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            report = sr.build_schema_review_report(pdir)
            self.assertEqual(report["verdict"], "PASS_READY_FOR_SERGE_REVIEW")
            self.assertEqual(report["dangerous_flags_count"], 0)
            self.assertEqual(report["missing_expected_files_count"], 0)

    def test_15_fail_when_dangerous_flag_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            (pdir / "memory" / "luna_dangerous_policy.json").write_text(
                json.dumps({
                    "schema_version": 1,
                    "safe_to_execute_now": True,  # DANGEROUS
                    "allow_aider": True,
                }),
                encoding="utf-8",
            )
            report = sr.build_schema_review_report(pdir)
            self.assertEqual(report["verdict"], "FAIL_DANGEROUS_POLICY")
            self.assertGreaterEqual(report["dangerous_flags_count"], 2)
            self.assertTrue(report["dangerous_findings"])

    def test_16_incomplete_when_required_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Seed only one expected file -- rest are missing.
            (pdir / "memory" / "luna_serge_standing_approval_policy.json").write_text(
                json.dumps({"schema_version": 1, "advisory_only": True}),
                encoding="utf-8",
            )
            report = sr.build_schema_review_report(pdir)
            self.assertEqual(report["verdict"], "INCOMPLETE_MISSING_FILES")


# ── 17-19: rendering, write paths, archive ──────────────────────────────────

class TestRenderingAndWriting(unittest.TestCase):
    def test_17_markdown_includes_verdict_and_dangerous_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            (pdir / "memory" / "luna_dangerous_policy.json").write_text(
                json.dumps({"schema_version": 1, "safe_to_execute_now": True}),
                encoding="utf-8",
            )
            report = sr.build_schema_review_report(pdir)
            md = sr.render_schema_review_markdown(report)
            self.assertIn("FAIL_DANGEROUS_POLICY", md)
            self.assertIn("Dangerous Flags", md)
            self.assertIn("safe_to_execute_now", md)

    def test_18_generated_reports_stay_under_temp_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            report = sr.build_schema_review_report(pdir)
            written = sr.write_schema_review_report(pdir, report)
            pdir_resolved = pdir.resolve()
            for key, p in written.items():
                pp = Path(p)
                self.assertTrue(
                    str(pp.resolve()).startswith(str(pdir_resolved)),
                    f"{pp} escapes project"
                )
                self.assertIn("memory", str(pp))

    def test_19_archive_copy_writes_under_temp_upgrades_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            report = sr.build_schema_review_report(pdir)
            md = sr.render_schema_review_markdown(report)
            archive = sr.copy_report_to_phase_archive(
                pdir, md, archive_dir="Luna New UpGrades",
            )
            ap = Path(archive)
            self.assertTrue(ap.exists())
            # Must be under temp project.
            self.assertTrue(str(ap.resolve()).startswith(str(pdir.resolve())))
            self.assertIn("Luna New UpGrades", str(ap))
            self.assertTrue(ap.name.startswith("PHASE6A_SCHEMA_REVIEW_HELPER_REPORT_"))


# ── 20: self-test ────────────────────────────────────────────────────────────

class TestSelfTest(unittest.TestCase):
    def test_20_self_test_returns_0(self):
        rc = sr.self_test()
        self.assertEqual(rc, 0)


# ── 21-24: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run(self, *args, project_dir=None):
        cmd = [sys.executable, "-m", "luna_modules.luna_schema_review"] + list(args)
        if project_dir is not None:
            cmd += ["--project-dir", str(project_dir)]
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, cwd=str(_PROJECT_ROOT),
        )
        return proc.returncode, proc.stdout, proc.stderr

    def test_21_cli_self_test_returns_0(self):
        rc, out, err = self._run("--self-test")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        self.assertIn("PASS", out)

    def test_22_cli_scan_returns_0(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            rc, out, err = self._run("--scan", project_dir=pdir)
            self.assertEqual(rc, 0, f"out={out}\nerr={err}")
            data = json.loads(out)
            self.assertIs(data["advisory_only"], True)
            self.assertIs(data["live_enforcement_enabled"], False)
            self.assertIs(data["safe_to_execute_now"], False)

    def test_23_cli_write_returns_0(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            rc, out, err = self._run("--write", project_dir=pdir)
            self.assertEqual(rc, 0, f"out={out}\nerr={err}")
            data = json.loads(out)
            self.assertIs(data["live_enforcement_enabled"], False)
            self.assertIn("written", data)

    def test_24_cli_print_markdown_returns_0(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            rc, out, err = self._run("--print-markdown", project_dir=pdir)
            self.assertEqual(rc, 0, f"out={out}\nerr={err}")
            self.assertIn("Luna Schema & Policy Review", out)


# ── 25-28: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_schema_review.py").read_text(encoding="utf-8")

    def test_25_no_aider_invocation(self):
        self.assertNotIn("import aider", self._src)
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [])

    def test_26_no_external_api_imports(self):
        for bad in ("import requests", "import openai", "import anthropic",
                    "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_27_no_package_installs(self):
        # No subprocess invocation that contains pip/winget/npm install.
        lower = self._src.lower()
        for bad in ("pip install", "winget install", "npm install"):
            found = lower.find(bad)
            while found != -1:
                line_start = lower.rfind("\n", 0, found) + 1
                line_end = lower.find("\n", found)
                line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"subprocess.run with '{bad}': {line!r}"
                )
                found = lower.find(bad, found + 1)

    def test_28_no_process_kill_or_delete_commands(self):
        lower = self._src.lower()
        for bad in ("taskkill", "git reset --hard", "git clean -fd",
                    "delete_queue", "rm -rf", "stop-process"):
            found = lower.find(bad)
            while found != -1:
                line_start = lower.rfind("\n", 0, found) + 1
                line_end = lower.find("\n", found)
                line = lower[line_start:(line_end if line_end != -1 else len(lower))].strip()
                self.assertFalse(
                    "subprocess.run" in line and bad in line,
                    f"subprocess.run with '{bad}': {line!r}"
                )
                self.assertFalse(
                    "os.system" in line and bad in line,
                    f"os.system with '{bad}': {line!r}"
                )
                found = lower.find(bad, found + 1)


# ── 29-30: invariants ────────────────────────────────────────────────────────

class TestInvariants(unittest.TestCase):
    def test_29_no_runtime_files_modified_by_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Seed a fake source file outside memory/.
            fake = pdir / "luna_modules" / "fake.py"
            fake.parent.mkdir(parents=True, exist_ok=True)
            original = "# protected\n"
            fake.write_text(original, encoding="utf-8")
            _write_all_expected(pdir)
            report = sr.build_schema_review_report(pdir)
            sr.write_schema_review_report(pdir, report)
            md = sr.render_schema_review_markdown(report)
            sr.copy_report_to_phase_archive(pdir, md, archive_dir="Luna New UpGrades")
            self.assertEqual(fake.read_text(encoding="utf-8"), original)

    def test_30_hard_report_flags_remain_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _write_all_expected(pdir)
            # Add a dangerous file so verdict is FAIL.
            (pdir / "memory" / "luna_x_policy.json").write_text(
                json.dumps({"schema_version": 1, "live_enforcement_enabled": True}),
                encoding="utf-8",
            )
            report = sr.build_schema_review_report(pdir)
            self.assertEqual(report["verdict"], "FAIL_DANGEROUS_POLICY")
            # Even on FAIL, the report's own hard fields must stay False.
            self.assertIs(report["live_enforcement_enabled"], False)
            self.assertIs(report["safe_to_execute_now"], False)
            self.assertIs(report["safe_to_apply_real_project"], False)
            self.assertIs(report["advisory_only"], True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
