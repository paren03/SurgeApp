"""Phase 5Z tests: Luna 24-Hour Advisory Soak Verdict module.

28+ tests covering policy loading, cycle extraction, individual checks,
PASS/FAIL/INCOMPLETE/NO_SOAK_FOUND classifications, JSONL parsing,
recommendation stability, source modification detection, validation,
markdown render, write paths, CLI, and source-code safety.
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

import luna_modules.luna_soak_verdict as sv


def _make_project(tmp: Path) -> Path:
    (tmp / "memory").mkdir(parents=True, exist_ok=True)
    return tmp


def _seed_pass_report(pdir: Path, n_cycles: int = 144) -> Path:
    cycles = []
    for i in range(n_cycles):
        cycles.append({
            "cycle": i + 1,
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
            "overall_recommendation": "serge_only",
            "counts": {"approve_recommended": 0, "wait_for_more_evidence": 1,
                       "do_not_approve": 3, "serge_only": 1, "unknown": 0},
            "files_checked": 6,
            "missing_artifacts": 4,
        })
    rep = {
        "schema_version": 1, "soak_id": "test_pass",
        "started_at": "2026-05-01T00:00:00.000000Z",
        "finished_at": "2026-05-02T00:00:00.000000Z",
        "duration_seconds": 86400.0,
        "cycles": n_cycles,
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "cycle_results": cycles,
        "failures": [],
        "warnings": [],
    }
    p = pdir / "memory" / "luna_advisory_soak_report.json"
    p.write_text(json.dumps(rep), encoding="utf-8")
    return p


# ── 1-2: policy loading ──────────────────────────────────────────────────────

class TestPolicyLoading(unittest.TestCase):
    def test_01_policy_loads_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            policy = sv.load_formal_soak_policy(pdir)
            self.assertEqual(policy["recommended_cycles_for_24h"], 144)
            self.assertEqual(policy["recommended_sleep_seconds"], 600)
            self.assertIs(policy["advisory_only"], True)
            self.assertIs(policy["safe_to_execute_now"], False)
            self.assertEqual(policy.get("_source"), "module_fallback")

    def test_02_malformed_policy_falls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            (pdir / "memory" / "luna_formal_advisory_soak_policy.json").write_text(
                "this is not json {{}", encoding="utf-8",
            )
            policy = sv.load_formal_soak_policy(pdir)
            self.assertIs(policy["advisory_only"], True)
            self.assertIs(policy["safe_to_execute_now"], False)
            # Hard rules survive even if malformed.
            self.assertIs(policy["guardian_enforcing_live"], False)


# ── 3-7: verdict classification ──────────────────────────────────────────────

class TestVerdictClassification(unittest.TestCase):
    def test_03_no_report_returns_no_soak_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "NO_SOAK_FOUND")
            self.assertIs(v["checklist_item_24h_soak_satisfied"], False)
            self.assertIs(v["live_enforcement_ready"], False)

    def test_04_short_3_cycle_report_returns_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=3)
            # Adjust duration to look like a short smoke (not 24h).
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            rep["duration_seconds"] = 3.0
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "INCOMPLETE")
            self.assertIs(v["checklist_item_24h_soak_satisfied"], False)
            self.assertIs(v["live_enforcement_ready"], False)

    def test_05_144_cycle_24h_clean_report_returns_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "PASS")
            self.assertIs(v["checklist_item_24h_soak_satisfied"], True)
            # Even on PASS, live enforcement stays NOT ready.
            self.assertIs(v["live_enforcement_ready"], False)

    def test_06_report_with_failure_returns_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            rep["failures"] = ["something_broke"]
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            self.assertIs(v["checklist_item_24h_soak_satisfied"], False)

    def test_07_report_with_warning_returns_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            rep["warnings"] = ["unexplained_warning"]
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")


# ── 8-10: hard safety field violations ───────────────────────────────────────

class TestHardSafetyChecks(unittest.TestCase):
    def _seed_with_unsafe_field(self, pdir: Path, key: str) -> None:
        _seed_pass_report(pdir, n_cycles=144)
        rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
        rep = json.loads(rep_path.read_text(encoding="utf-8"))
        # Flip one cycle's field to True.
        rep["cycle_results"][7][key] = True
        rep_path.write_text(json.dumps(rep), encoding="utf-8")

    def test_08_safe_to_execute_now_true_anywhere_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            self._seed_with_unsafe_field(pdir, "safe_to_execute_now")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            self.assertIs(v["safe_to_execute_now"], False)  # verdict's own field
            failed_names = [c["name"] for c in v["checks"] if not c["ok"]]
            self.assertIn("safe_to_execute_now_false", failed_names)

    def test_09_safe_to_apply_real_project_true_anywhere_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            self._seed_with_unsafe_field(pdir, "safe_to_apply_real_project")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            failed_names = [c["name"] for c in v["checks"] if not c["ok"]]
            self.assertIn("safe_to_apply_real_project_false", failed_names)

    def test_10_guardian_enforcing_live_true_anywhere_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            self._seed_with_unsafe_field(pdir, "guardian_enforcing_live")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            failed_names = [c["name"] for c in v["checks"] if not c["ok"]]
            self.assertIn("guardian_enforcing_live_false", failed_names)


# ── 11-12: recommendation stability ──────────────────────────────────────────

class TestRecommendationStability(unittest.TestCase):
    def test_11_recommendation_change_without_explanation_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            # Flip cycle 70 to a different recommendation.
            rep["cycle_results"][70]["overall_recommendation"] = "continue_safe_routine"
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            failed_names = [c["name"] for c in v["checks"] if not c["ok"]]
            self.assertIn("stable_recommendation_or_explained_changes", failed_names)

    def test_12_recommendation_change_with_explanation_can_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            # Cycles 1-69: serge_only; cycle 70+: continue_safe_routine.
            for i in range(70, 144):
                rep["cycle_results"][i]["overall_recommendation"] = "continue_safe_routine"
            # Provide an explanation for the single transition.
            rep["explained_recommendation_changes"] = [
                {"from": "serge_only", "to": "continue_safe_routine",
                 "explanation": "Serge approved the SERGE_ONLY action manually mid-soak."}
            ]
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            stab_check = next(c for c in v["checks"]
                              if c["name"] == "stable_recommendation_or_explained_changes")
            self.assertTrue(stab_check["ok"], stab_check)


# ── 13-15: source-modification + JSONL parsing ───────────────────────────────

class TestSourceModificationAndJSONL(unittest.TestCase):
    def test_13_source_modification_flag_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            rep_path = pdir / "memory" / "luna_advisory_soak_report.json"
            rep = json.loads(rep_path.read_text(encoding="utf-8"))
            rep["source_files_modified"] = ["worker.py"]
            rep_path.write_text(json.dumps(rep), encoding="utf-8")
            v = sv.evaluate_soak_report(pdir)
            self.assertEqual(v["verdict"], "FAIL_WITH_REASONS")
            failed_names = [c["name"] for c in v["checks"] if not c["ok"]]
            self.assertIn("no_source_file_modifications", failed_names)

    def test_14_jsonl_cycle_rows_are_parsed(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            jp = pdir / "memory" / "luna_advisory_soak.jsonl"
            rows = [
                {"ts": "2026-05-01T00:00:00Z", "cycle": 1,
                 "safe_to_execute_now": False, "safe_to_apply_real_project": False,
                 "guardian_enforcing_live": False,
                 "overall_recommendation": "serge_only"},
                {"ts": "2026-05-01T00:10:00Z", "cycle": 2,
                 "safe_to_execute_now": False, "safe_to_apply_real_project": False,
                 "guardian_enforcing_live": False,
                 "overall_recommendation": "serge_only"},
            ]
            jp.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
            parsed, warns = sv.read_jsonl(jp)
            self.assertEqual(len(parsed), 2)
            self.assertEqual(warns, [])

    def test_15_corrupt_jsonl_rows_skipped_with_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            jp = pdir / "memory" / "luna_advisory_soak.jsonl"
            jp.write_text(
                json.dumps({"cycle": 1}) + "\n"
                + "this is not valid json\n"
                + json.dumps({"cycle": 2}) + "\n",
                encoding="utf-8",
            )
            parsed, warns = sv.read_jsonl(jp)
            self.assertEqual(len(parsed), 2)
            self.assertEqual(len(warns), 1)
            self.assertIn("jsonl_parse_error", warns[0])


# ── 16-18: rendering, write, checklist ───────────────────────────────────────

class TestRenderingAndChecklist(unittest.TestCase):
    def test_16_markdown_render_includes_verdict_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            v = sv.evaluate_soak_report(pdir)
            md = sv.render_soak_verdict_markdown(v)
            self.assertIn("PASS", md)
            self.assertIn("Plain-English Summary", md)
            self.assertIn("False", md)  # safe fields displayed false

    def test_17_write_report_under_temp_memory_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            _seed_pass_report(pdir, n_cycles=144)
            v = sv.evaluate_soak_report(pdir)
            written = sv.write_soak_verdict_report(pdir, v)
            pdir_resolved = pdir.resolve()
            for key, p in written.items():
                pp = Path(p)
                self.assertTrue(
                    str(pp.resolve()).startswith(str(pdir_resolved)),
                    f"{pp} escapes project"
                )
                self.assertIn("memory", str(pp))

    def test_18_readiness_checklist_exists_and_live_disabled(self):
        cp = _PROJECT_ROOT / "memory" / "luna_live_enforcement_readiness_checklist.json"
        self.assertTrue(cp.exists())
        data = json.loads(cp.read_text(encoding="utf-8"))
        self.assertIs(data["live_enforcement_enabled"], False)
        self.assertIn("required_before_live_guardian_enforcement", data)


# ── 19-22: CLI ───────────────────────────────────────────────────────────────

class TestCLI(unittest.TestCase):
    def _run(self, *args):
        cmd = [sys.executable, "-m", "luna_modules.luna_soak_verdict"] + list(args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                              cwd=str(_PROJECT_ROOT))
        return proc.returncode, proc.stdout, proc.stderr

    def test_19_cli_self_test_returns_0(self):
        rc, out, err = self._run("--self-test")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        self.assertIn("PASS", out)

    def test_20_cli_sample_pass_returns_0_and_says_pass(self):
        rc, out, err = self._run("--sample-pass")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertEqual(data["verdict"], "PASS")
        self.assertIs(data["live_enforcement_ready"], False)
        self.assertIs(data["safe_to_execute_now"], False)

    def test_21_cli_sample_fail_returns_0_and_says_fail(self):
        rc, out, err = self._run("--sample-fail")
        self.assertEqual(rc, 0, f"out={out}\nerr={err}")
        data = json.loads(out)
        self.assertEqual(data["verdict"], "FAIL_WITH_REASONS")
        self.assertIs(data["live_enforcement_ready"], False)
        self.assertIs(data["safe_to_execute_now"], False)

    def test_22_cli_evaluate_returns_0_when_no_report(self):
        # The real project may or may not have a report present; a safe way
        # to test NO_SOAK_FOUND is via a temp project and --project-dir.
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            cmd = [
                sys.executable, "-m", "luna_modules.luna_soak_verdict",
                "--evaluate", "--project-dir", str(pdir),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                                  cwd=str(_PROJECT_ROOT))
            self.assertEqual(proc.returncode, 0, f"err={proc.stderr}")
            data = json.loads(proc.stdout)
            self.assertEqual(data["verdict"], "NO_SOAK_FOUND")


# ── 23-26: source code safety ────────────────────────────────────────────────

class TestSourceCodeSafety(unittest.TestCase):
    _src = (_PROJECT_ROOT / "luna_modules" / "luna_soak_verdict.py").read_text(encoding="utf-8")

    def test_23_no_aider_invocation(self):
        self.assertNotIn("import aider", self._src)
        aider_calls = re.findall(r'subprocess\.[^\n]*aider', self._src)
        self.assertEqual(aider_calls, [])

    def test_24_no_external_api_imports(self):
        for bad in ("import requests", "import openai", "import anthropic",
                    "import xai", "import httpx"):
            with self.subTest(bad=bad):
                self.assertNotIn(bad, self._src)

    def test_25_no_dangerous_subprocess_commands(self):
        lower = self._src.lower()
        for bad in ("pip install", "taskkill", "git reset", "delete_queue"):
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

    def test_26_no_source_writes_outside_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # Seed a fake source file.
            fake = pdir / "luna_modules" / "fake.py"
            fake.parent.mkdir(parents=True, exist_ok=True)
            original = "# protected\n"
            fake.write_text(original, encoding="utf-8")
            _seed_pass_report(pdir, n_cycles=144)
            v = sv.evaluate_soak_report(pdir)
            sv.write_soak_verdict_report(pdir, v)
            self.assertEqual(fake.read_text(encoding="utf-8"), original)


# ── 27-28: invariants ────────────────────────────────────────────────────────

class TestInvariants(unittest.TestCase):
    def test_27_hard_fields_remain_false_on_all_verdict_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = _make_project(Path(tmp))
            # NO_SOAK_FOUND
            v1 = sv.evaluate_soak_report(pdir)
            for f in ("safe_to_execute_now", "safe_to_apply_real_project",
                      "guardian_enforcing_live", "live_enforcement_ready"):
                self.assertIs(v1[f], False, f"{f} must be False on NO_SOAK_FOUND")
            # PASS
            _seed_pass_report(pdir, n_cycles=144)
            v2 = sv.evaluate_soak_report(pdir)
            for f in ("safe_to_execute_now", "safe_to_apply_real_project",
                      "guardian_enforcing_live", "live_enforcement_ready"):
                self.assertIs(v2[f], False, f"{f} must be False on PASS")

    def test_28_self_test_returns_0(self):
        rc = sv.self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
