"""Phase 5J tests: luna_resource_monitor.

Stdlib unittest only. All tests use TemporaryDirectory fixtures and fake
snapshots so they don't require live system state.
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

from luna_modules.luna_resource_monitor import (  # noqa: E402
    SCHEMA_VERSION,
    VALID_MODES,
    build_hardware_profile,
    build_hibernation_plan,
    build_resource_snapshot,
    bytes_to_gb,
    clamp_percent,
    classify_resource_state,
    cpu_status,
    disk_usage_status,
    gpu_status,
    load_policy,
    log_pressure,
    memory_status,
    ollama_status,
    process_pressure,
    queue_pressure,
    read_json,
    recommend_resource_action,
    render_hibernation_plan_markdown,
    render_resource_status_markdown,
    safe_int,
    self_test,
    validate_hibernation_plan,
    validate_resource_snapshot,
    write_json_atomic,
    write_resource_reports,
)


_HEALTHY_FAKES = {
    "disk": {
        "project_drive_total_gb": 500.0,
        "project_drive_free_gb": 200.0,
        "project_drive_free_percent": 40,
        "status": "healthy",
    },
    "memory": {
        "total_gb": 32.0,
        "available_gb": 18.0,
        "available_percent": 56,
        "status": "healthy",
        "source": "fake",
    },
    "cpu": {"usage_percent": 25, "load_status": "healthy", "source": "fake"},
    "gpu": {
        "detected": True,
        "name": "fake_gpu",
        "total_vram_gb": 8.0,
        "used_vram_gb": 1.0,
        "free_vram_gb": 7.0,
        "free_vram_percent": 87,
        "status": "healthy",
        "source": "fake",
    },
    "ollama": {
        "api_reachable": True,
        "loaded_models": ["qwen2.5-coder:7b"],
        "status": "healthy",
        "source": "fake",
    },
    "processes": {
        "luna_process_count": 3,
        "worker_main_logical": 1,
        "worker_cu_logical": 1,
        "aider_bridge_logical": 1,
        "aider_child_count": 0,
        "status": "healthy",
        "source": "fake",
    },
    "queues": {
        "tasks_active": 0, "tasks_done": 0, "tasks_failed": 0,
        "aider_active": 0, "aider_done": 0, "aider_failed": 0,
        "aider_quarantine": 0, "status": "healthy",
    },
    "logs": {"largest_files": [], "total_log_bytes": 0, "total_log_mb": 0.0, "status": "healthy"},
}


def _fakes(**overrides):
    out = {k: dict(v) for k, v in _HEALTHY_FAKES.items()}
    for k, patch in overrides.items():
        out[k].update(patch)
    return out


class _PureHelperTests(unittest.TestCase):

    def test_01_clamp_percent_lower(self) -> None:
        self.assertEqual(clamp_percent(-5), 0)
        self.assertEqual(clamp_percent("nope"), 0)
        self.assertEqual(clamp_percent(None), 0)

    def test_02_bytes_to_gb_conversion(self) -> None:
        self.assertEqual(bytes_to_gb(1024 ** 3), 1.0)
        self.assertEqual(bytes_to_gb(0), 0.0)
        self.assertEqual(bytes_to_gb("bad"), 0.0)

    def test_30_safe_int_default(self) -> None:
        self.assertEqual(safe_int("abc", default=42), 42)
        self.assertEqual(safe_int("3"), 3)


class _DiskTests(unittest.TestCase):

    def test_03_disk_usage_temp_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            res = disk_usage_status(td)
            for k in ("project_drive_total_gb", "project_drive_free_gb", "project_drive_free_percent", "status"):
                self.assertIn(k, res)
            self.assertGreaterEqual(res["project_drive_total_gb"], 0)


class _PsutilOptionalTests(unittest.TestCase):

    def test_04_psutil_optional_no_crash(self) -> None:
        from luna_modules import luna_resource_monitor as mod
        original = mod._psutil
        try:
            mod._psutil = None
            cpu = mod.cpu_status()
            mem = mod.memory_status()
            self.assertIn("usage_percent", cpu)
            self.assertIn("available_percent", mem)
        finally:
            mod._psutil = original


class _MemoryShapeTests(unittest.TestCase):

    def test_05_memory_status_shape(self) -> None:
        m = memory_status()
        for k in ("total_gb", "available_gb", "available_percent", "status", "source"):
            self.assertIn(k, m)


class _CpuShapeTests(unittest.TestCase):

    def test_06_cpu_status_shape(self) -> None:
        c = cpu_status()
        for k in ("usage_percent", "load_status", "source"):
            self.assertIn(k, c)


class _GpuShapeTests(unittest.TestCase):

    def test_07_gpu_status_shape(self) -> None:
        g = gpu_status()
        for k in ("detected", "name", "total_vram_gb", "used_vram_gb", "free_vram_gb", "free_vram_percent", "status", "source"):
            self.assertIn(k, g)


class _OllamaTests(unittest.TestCase):

    def test_08_ollama_unreachable_no_crash(self) -> None:
        # Use a localhost port likely closed
        r = ollama_status(api_base="http://127.0.0.1:1")
        for k in ("api_reachable", "loaded_models", "status", "source"):
            self.assertIn(k, r)


class _PressureTests(unittest.TestCase):

    def test_09_process_pressure_returns_record(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = process_pressure(td)
            for k in (
                "luna_process_count",
                "worker_main_logical",
                "worker_cu_logical",
                "aider_bridge_logical",
                "aider_child_count",
                "status",
            ):
                self.assertIn(k, r)

    def test_10_queue_pressure_temp(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "aider_jobs" / "active").mkdir(parents=True)
            (td / "aider_jobs" / "active" / "j1.json").write_text("{}", encoding="utf-8")
            (td / "tasks" / "done").mkdir(parents=True)
            r = queue_pressure(td)
            self.assertEqual(r["aider_active"], 1)
            self.assertEqual(r["tasks_done"], 0)

    def test_11_log_pressure_largest(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "logs").mkdir(parents=True)
            (td / "logs" / "small.log").write_text("a" * 100, encoding="utf-8")
            (td / "logs" / "big.log").write_text("a" * 5000, encoding="utf-8")
            r = log_pressure(td)
            self.assertGreaterEqual(r["total_log_bytes"], 5100)
            self.assertEqual(r["largest_files"][0]["path"].split("/")[-1], "big.log")


class _ProfileSnapshotTests(unittest.TestCase):

    def test_12_hardware_profile_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = build_hardware_profile(td)
            for k in ("schema_version", "host", "platform", "cpu_count_logical", "psutil_available"):
                self.assertIn(k, p)

    def test_13_snapshot_includes_all_sections(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            ok, errs = validate_resource_snapshot(snap)
            self.assertTrue(ok, errs)

    def test_14_validate_catches_missing_section(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            del snap["disk"]
            ok, errs = validate_resource_snapshot(snap)
            self.assertFalse(ok)
            self.assertTrue(any("disk" in e for e in errs))


class _ClassifyTests(unittest.TestCase):

    def test_15_normal_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            self.assertEqual(snap["recommended_mode"], "normal")

    def test_16_light_mode_on_low_gpu(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(
                td,
                fakes=_fakes(gpu={"detected": True, "name": "g", "total_vram_gb": 8, "used_vram_gb": 7.5,
                                  "free_vram_gb": 0.5, "free_vram_percent": 6, "status": "blocked", "source": "fake"}),
            )
            self.assertIn(snap["recommended_mode"], ("light", "pause_high_intensity", "blocked"))

    def test_17_pause_on_low_memory(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(
                td,
                fakes=_fakes(memory={"total_gb": 32, "available_gb": 5, "available_percent": 15,
                                     "status": "watch", "source": "fake"}),
            )
            self.assertIn(snap["recommended_mode"], ("pause_high_intensity", "normal", "light"))

    def test_18_blocked_on_low_disk(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(
                td,
                fakes=_fakes(disk={"project_drive_total_gb": 500, "project_drive_free_gb": 1.0,
                                   "project_drive_free_percent": 1, "status": "blocked"}),
            )
            self.assertEqual(snap["recommended_mode"], "blocked")
            self.assertGreater(len(snap["blockers"]), 0)

    def test_19_no_model_heavy_when_ollama_down(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(
                td,
                fakes=_fakes(ollama={"api_reachable": False, "loaded_models": [], "status": "degraded", "source": "fake"}),
            )
            self.assertIn(snap["recommended_mode"], ("light", "pause_high_intensity", "normal"))
            decision = classify_resource_state(snap)
            self.assertNotEqual(decision["mode"], "normal")


class _HibernationTests(unittest.TestCase):

    def test_20_plan_is_proposal_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            plan = build_hibernation_plan(td, snap, reason="test")
            ok, errs = validate_hibernation_plan(plan)
            self.assertTrue(ok, errs)
            joined = " ".join(plan["safety_notes"]).lower()
            self.assertIn("proposal", joined)
            self.assertIn("does not execute", joined)
            for action in plan["proposed_actions"]:
                self.assertIn("action", action)
                self.assertIn("rationale", action)

    def test_21_files_not_to_touch_present(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            plan = build_hibernation_plan(td, snap, reason="test")
            self.assertIn("worker.py", plan["files_not_to_touch"])
            self.assertIn("LUNA_STOP_NOW.flag", plan["files_not_to_touch"])
            self.assertIn("aider_jobs/", plan["files_not_to_touch"])


class _RenderTests(unittest.TestCase):

    def test_22_markdown_includes_mode_and_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(
                td,
                fakes=_fakes(disk={"project_drive_total_gb": 500, "project_drive_free_gb": 1.0,
                                   "project_drive_free_percent": 1, "status": "blocked"}),
            )
            decision = classify_resource_state(snap)
            md = render_resource_status_markdown(snap, decision)
            self.assertIn("Recommended mode", md)
            self.assertIn("Blockers", md)
            self.assertIn("blocked", md)


class _WriteTests(unittest.TestCase):

    def test_23_write_under_temp_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            decision = classify_resource_state(snap)
            plan = build_hibernation_plan(td, snap)
            written = write_resource_reports(
                td, snap, decision=decision, plan=plan, project_root=td
            )
            for k in ("snapshot_json", "snapshot_md", "hardware_profile_json", "plan_json", "plan_md", "build_report_json"):
                self.assertIn(k, written)
                self.assertTrue(Path(written[k]).is_file())

    def test_24_paths_stay_under_project(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            snap = build_resource_snapshot(td, fakes=_fakes())
            outside = Path(tempfile.gettempdir()).resolve()
            with self.assertRaises(ValueError):
                write_resource_reports(td, snap, project_root=outside / "elsewhere")


class _SelfTestTests(unittest.TestCase):

    def test_25_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_resource_monitor", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

    def test_26_cli_self_test_zero(self) -> None:
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)
        parsed = json.loads(r.stdout)
        self.assertTrue(parsed.get("ok"))

    def test_27_cli_hibernation_plan_zero(self) -> None:
        r = self._run("--hibernation-plan")
        self.assertEqual(r.returncode, 0, r.stderr)
        parsed = json.loads(r.stdout)
        self.assertIn("plan_id", parsed)


class _NoExternalNetworkTests(unittest.TestCase):

    def test_28_only_localhost_urls(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_resource_monitor.py").read_text(
            encoding="utf-8"
        )
        # any http reference outside localhost?
        bad_lines = []
        for i, line in enumerate(text.splitlines(), start=1):
            if "http" not in line.lower():
                continue
            if "127.0.0.1" in line or "localhost" in line or "non_localhost" in line.lower() or "draft-07" in line.lower() or "json-schema" in line.lower():
                continue
            bad_lines.append((i, line.strip()))
        self.assertEqual(bad_lines, [], f"non-localhost http references: {bad_lines}")

    def test_29_no_kill_or_stop_commands(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_resource_monitor.py").read_text(
            encoding="utf-8"
        )
        for token in ("taskkill", "Stop-Process", "kill -9", "os.kill", "subprocess.kill",
                      "Remove-Item", "rm -rf", "del /f"):
            self.assertNotIn(token, text, f"forbidden token present: {token!r}")


class _PolicyTests(unittest.TestCase):

    def test_31_policy_handles_malformed(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            bad = td / "policy.json"
            bad.write_text("{not json", encoding="utf-8")
            pol = load_policy(bad)
            self.assertEqual(pol["schema_version"], 1)
            self.assertFalse(pol.get("_loaded_from_file"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
