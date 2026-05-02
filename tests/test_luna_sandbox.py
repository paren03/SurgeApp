"""Phase 5I tests: luna_sandbox.

Stdlib unittest only. All tests use TemporaryDirectory fixtures so they don't
modify real D:\\SurgeApp source files.
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

from luna_modules.luna_sandbox import (  # noqa: E402
    SCHEMA_VERSION,
    apply_patch_to_sandbox,
    apply_unified_diff_to_text,
    build_simulation_plan,
    build_simulation_report,
    collect_target_metadata,
    compare_target_hashes,
    copy_targets_to_sandbox,
    create_filesystem_snapshot,
    create_sandbox_workspace,
    ensure_under_project,
    file_metadata,
    make_sandbox_id,
    make_snapshot_id,
    normalize_project_relative,
    render_simulation_report_markdown,
    restore_snapshot,
    run_safe_command,
    run_sandbox_verification,
    self_test,
    sha256_file,
    sha256_text,
    validate_simulation_plan,
    validate_simulation_report,
    validate_snapshot_manifest,
    write_simulation_report,
)


def _make_temp_project(td: Path) -> Path:
    (td / "memory").mkdir(parents=True, exist_ok=True)
    (td / "luna_modules").mkdir(parents=True, exist_ok=True)
    target = td / "luna_modules" / "sample.py"
    target.write_text("def hello():\n    return 'old'\n", encoding="utf-8")
    return target


class _IdAndPathTests(unittest.TestCase):

    def test_01_id_shapes(self) -> None:
        sid = make_snapshot_id()
        bid = make_sandbox_id()
        self.assertTrue(sid.startswith("snap_"))
        self.assertTrue(bid.startswith("sandbox_"))
        self.assertNotEqual(make_snapshot_id(), make_snapshot_id())

    def test_02_normalize_project_relative(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "a" / "b").mkdir(parents=True)
            (td / "a" / "b" / "c.py").write_text("x", encoding="utf-8")
            rel = normalize_project_relative(td / "a" / "b" / "c.py", td)
            self.assertEqual(rel, "a/b/c.py")

    def test_03_ensure_under_project_rejects_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            outside = Path(tempfile.gettempdir())
            with self.assertRaises(ValueError):
                ensure_under_project(outside / "x.txt", td)


class _HashTests(unittest.TestCase):

    def test_04_sha256_file_stable(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            p = td / "a.txt"
            p.write_text("hello", encoding="utf-8")
            h1 = sha256_file(p)
            h2 = sha256_file(p)
            self.assertEqual(h1, h2)
            self.assertEqual(len(h1), 64)
            self.assertEqual(sha256_text("hello"), h1)

    def test_05_file_metadata_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            meta = file_metadata(td / "nope.py", td)
            self.assertFalse(meta["exists_before"])
            self.assertEqual(meta["size_bytes"], 0)
            self.assertEqual(meta["sha256"], "")


class _SnapshotTests(unittest.TestCase):

    def test_06_snapshot_copies_and_records_hash(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            target = _make_temp_project(td)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"], reason="test")
            ok, errs = validate_snapshot_manifest(man)
            self.assertTrue(ok, errs)
            self.assertEqual(len(man["targets"]), 1)
            t0 = man["targets"][0]
            self.assertTrue(t0["exists_before"])
            self.assertEqual(t0["sha256"], sha256_file(target))
            self.assertTrue(Path(t0["snapshot_path"]).is_file())

    def test_07_restore_dry_run_does_not_modify(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            target = _make_temp_project(td)
            before_hash = sha256_file(target)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"])
            target.write_text("def hello():\n    return 'edited'\n", encoding="utf-8")
            mid_hash = sha256_file(target)
            self.assertNotEqual(before_hash, mid_hash)
            res = restore_snapshot(man, td, dry_run=True)
            self.assertFalse(res["applied"])
            self.assertTrue(res["dry_run"])
            after_hash = sha256_file(target)
            self.assertEqual(mid_hash, after_hash)

    def test_08_restore_requires_explicit_allow(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            target = _make_temp_project(td)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"])
            target.write_text("changed", encoding="utf-8")
            res = restore_snapshot(man, td, dry_run=False, allow_restore=False)
            self.assertFalse(res["applied"])
            self.assertIn("allow_restore", res.get("blocked_reason", ""))
            self.assertEqual(target.read_text(encoding="utf-8"), "changed")
            res2 = restore_snapshot(man, td, dry_run=False, allow_restore=True)
            self.assertTrue(res2["applied"])
            self.assertIn("luna_modules/sample.py", res2["restored"])
            self.assertIn("'old'", target.read_text(encoding="utf-8"))

    def test_29_manifest_has_restore_instructions(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _make_temp_project(td)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"])
            self.assertIn("restore_snapshot", man["restore_instructions"])
            self.assertIn("dry_run", man["restore_instructions"])


class _SandboxWorkspaceTests(unittest.TestCase):

    def test_09_sandbox_under_temp_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            sb = create_sandbox_workspace(td)
            sandbox_dir = Path(sb["sandbox_dir"]).resolve()
            sandbox_dir.relative_to(td.resolve())

    def test_10_copy_targets_only_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _make_temp_project(td)
            (td / "luna_modules" / "other.py").write_text("y = 1\n", encoding="utf-8")
            sb = create_sandbox_workspace(td)
            recs = copy_targets_to_sandbox(td, ["luna_modules/sample.py"], sb["sandbox_dir"])
            self.assertEqual(len(recs), 1)
            self.assertTrue(recs[0]["copied"])
            other_in_sandbox = Path(sb["sandbox_dir"]) / "luna_modules" / "other.py"
            self.assertFalse(other_in_sandbox.exists())


class _PlanTests(unittest.TestCase):

    def test_11_plan_shape(self) -> None:
        plan = build_simulation_plan(
            "rename hello",
            ["luna_modules/sample.py"],
            verification_commands=["python -m py_compile luna_modules/sample.py"],
            patch_records=[
                {
                    "target_file": "luna_modules/sample.py",
                    "patch_type": "replace_text",
                    "find_text": "old",
                    "replace_text": "new",
                }
            ],
        )
        ok, errs = validate_simulation_plan(plan)
        self.assertTrue(ok, errs)
        for k in (
            "plan_id",
            "goal",
            "target_files",
            "patch_records",
            "verification_commands",
            "expected_artifacts",
            "risk_level",
            "approval_tier_required",
            "rollback_required",
            "created_at",
        ):
            self.assertIn(k, plan)

    def test_12_validate_plan_catches_missing_fields(self) -> None:
        ok, errs = validate_simulation_plan({"plan_id": "x"})
        self.assertFalse(ok)
        self.assertGreater(len(errs), 0)


class _PatchTests(unittest.TestCase):

    def _setup(self):
        td = Path(tempfile.mkdtemp())
        target = _make_temp_project(td)
        sb = create_sandbox_workspace(td)
        copy_targets_to_sandbox(td, ["luna_modules/sample.py"], sb["sandbox_dir"])
        return td, target, sb

    def test_13_replace_text_inside_sandbox_only(self) -> None:
        td, target, sb = self._setup()
        try:
            real_before = target.read_text(encoding="utf-8")
            res = apply_patch_to_sandbox(
                sb["sandbox_dir"],
                td,
                [
                    {
                        "target_file": "luna_modules/sample.py",
                        "patch_type": "replace_text",
                        "find_text": "old",
                        "replace_text": "new",
                    }
                ],
            )
            self.assertEqual(len(res), 1)
            self.assertTrue(res[0]["applied"], res)
            self.assertEqual(target.read_text(encoding="utf-8"), real_before)
            sandbox_target = (
                Path(sb["sandbox_dir"]) / "luna_modules" / "sample.py"
            )
            self.assertIn("new", sandbox_target.read_text(encoding="utf-8"))
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_14_append_text_inside_sandbox_only(self) -> None:
        td, target, sb = self._setup()
        try:
            real_before = target.read_text(encoding="utf-8")
            res = apply_patch_to_sandbox(
                sb["sandbox_dir"],
                td,
                [
                    {
                        "target_file": "luna_modules/sample.py",
                        "patch_type": "append_text",
                        "append_text": "\n# extra\n",
                    }
                ],
            )
            self.assertTrue(res[0]["applied"], res)
            self.assertEqual(target.read_text(encoding="utf-8"), real_before)
            sandbox_target = Path(sb["sandbox_dir"]) / "luna_modules" / "sample.py"
            self.assertIn("# extra", sandbox_target.read_text(encoding="utf-8"))
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_15_unified_diff_explicitly_rejected(self) -> None:
        td, target, sb = self._setup()
        try:
            res = apply_patch_to_sandbox(
                sb["sandbox_dir"],
                td,
                [
                    {
                        "target_file": "luna_modules/sample.py",
                        "patch_type": "unified_diff",
                        "unified_diff": "--- a\n+++ b\n@@ -1 +1 @@\n-old\n+new\n",
                    }
                ],
            )
            self.assertFalse(res[0]["applied"])
            self.assertTrue(
                any("needs_external_patcher" in e for e in res[0].get("errors", [])),
                res,
            )
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_28_real_target_unchanged_after_simulation(self) -> None:
        td, target, sb = self._setup()
        try:
            before_hash = sha256_file(target)
            apply_patch_to_sandbox(
                sb["sandbox_dir"],
                td,
                [
                    {
                        "target_file": "luna_modules/sample.py",
                        "patch_type": "replace_text",
                        "find_text": "old",
                        "replace_text": "new",
                    }
                ],
            )
            self.assertEqual(sha256_file(target), before_hash)
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)


class _SafeCommandTests(unittest.TestCase):

    def test_16_unsafe_command_denied(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            r = run_safe_command("pip install something", cwd=td_str)
            self.assertFalse(r["applied"])
            self.assertIn("denied", r["stderr"])

    def test_30_other_unsafe_commands_denied(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            for cmd in (
                "pip install x",
                "git reset --hard",
                "Remove-Item D:\\foo",
                "rm -rf /",
                "winget install foo",
                "Invoke-WebRequest http://evil",
            ):
                r = run_safe_command(cmd, cwd=td_str)
                self.assertFalse(r["applied"], f"{cmd!r} should be denied")

    def test_17_safe_py_compile_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            p = td / "ok.py"
            p.write_text("x = 1\n", encoding="utf-8")
            r = run_safe_command(f"python -m py_compile {p.name}", cwd=td)
            self.assertTrue(r["applied"], r)
            self.assertEqual(r["rc"], 0, r)

    def test_18_run_sandbox_verification_captures_results(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            p = td / "ok.py"
            p.write_text("x = 1\n", encoding="utf-8")
            results = run_sandbox_verification(
                td, [f"python -m py_compile {p.name}"]
            )
            self.assertEqual(len(results), 1)
            self.assertIn("rc", results[0])
            self.assertIn("stdout", results[0])
            self.assertIn("stderr", results[0])


class _HashDeltaTests(unittest.TestCase):

    def test_19_compare_detects_changed(self) -> None:
        before = [{"relative_path": "a.py", "sha256": "x", "size_bytes": 1, "exists_before": True}]
        after = [{"relative_path": "a.py", "sha256": "y", "size_bytes": 1, "exists_before": True}]
        delta = compare_target_hashes(before, after)
        self.assertTrue(delta[0]["changed"])


class _ReportTests(unittest.TestCase):

    def _make_report(self) -> dict:
        plan = build_simulation_plan(
            "smoke",
            ["sample.py"],
            verification_commands=["python -c \"print('ok')\""],
            patch_records=[],
        )
        snapshot = {
            "schema_version": SCHEMA_VERSION,
            "snapshot_id": "snap_x",
            "created_at": "2026-01-01T00:00:00.000000Z",
            "project_dir": "/tmp",
            "snapshot_dir": "/tmp/backups/sandbox_x",
            "targets": [],
            "restore_instructions": "...",
        }
        sandbox = {"sandbox_id": "sandbox_x", "sandbox_dir": "/tmp/sb"}
        verification = [{"command": "python -c print", "rc": 0, "applied": True, "stdout": "", "stderr": ""}]
        delta = []
        return build_simulation_report(plan, snapshot, sandbox, verification, delta)

    def test_20_validate_report(self) -> None:
        rep = self._make_report()
        ok, errs = validate_simulation_report(rep)
        self.assertTrue(ok, errs)
        self.assertTrue(rep["real_project_unchanged"])

    def test_21_markdown_renders_required(self) -> None:
        rep = self._make_report()
        md = render_simulation_report_markdown(rep)
        self.assertIn("Luna Simulation Report", md)
        self.assertIn("Target files", md)
        self.assertIn("real_project_unchanged", md)
        self.assertIn("Hash delta", md)

    def test_22_write_report_under_temp_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            rep = self._make_report()
            json_p = td / "memory" / "rep.json"
            md_p = td / "memory" / "rep.md"
            written = write_simulation_report(rep, json_p, md_p, project_root=td)
            self.assertIn("json", written)
            self.assertTrue(json_p.is_file())
            self.assertTrue(md_p.is_file())
            outside = Path(tempfile.gettempdir()) / "should_not.json"
            with self.assertRaises(ValueError):
                write_simulation_report(rep, outside, project_root=td)


class _MissingArtifactsTests(unittest.TestCase):

    def test_23_missing_phase5_artifacts_does_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _make_temp_project(td)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"])
            self.assertEqual(len(man["targets"]), 1)


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_sandbox", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

    def test_24_cli_self_test_zero(self) -> None:
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)
        parsed = json.loads(r.stdout)
        self.assertTrue(parsed.get("ok"))


class _SelfTestFunctionTests(unittest.TestCase):

    def test_25_self_test_function_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


class _NoNetworkTests(unittest.TestCase):

    def test_26_no_network_imports(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_sandbox.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("import socket", text)
        self.assertNotIn("import urllib", text)
        self.assertNotIn("import requests", text)
        self.assertNotIn("http.client", text)


class _PathSafetyTests(unittest.TestCase):

    def test_27_generated_paths_stay_under_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _make_temp_project(td)
            man = create_filesystem_snapshot(td, ["luna_modules/sample.py"])
            sd = Path(man["snapshot_dir"]).resolve()
            sd.relative_to(td.resolve())
            sb = create_sandbox_workspace(td)
            Path(sb["sandbox_dir"]).resolve().relative_to(td.resolve())


if __name__ == "__main__":
    unittest.main(verbosity=2)
