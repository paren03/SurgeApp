"""Luna Self-Improvement Engine — bounded, deterministic, reversible, audited.

Luna improves HERSELF overnight by raising her own self-verification
coverage: for each cognitive module that lacks a smoke test, generate a
deterministic smoke test (import + report() + never-raise probe), run it,
and keep it only if it PASSES. This can never break the running system —
it only ADDS test files under a dedicated directory.

HARD GUARDRAILS (every cycle):
* NEVER raises (every public fn wrapped).
* Kill-switch: if memory/kill_switches/luna_self_improvement.disabled
  exists, the engine does nothing.
* Bounded: per-cycle wall budget + max targets per cycle + total cycle cap.
* Reversible: generated tests live ONLY in D:\\SurgeApp\\self_tests\\ —
  delete that folder to undo everything.
* Forbidden: never writes outside self_tests/, never touches the vocabulary
  DB, feature_flags.json, or any cognitive_*.py source. Generation is
  DETERMINISTIC (template-based) — no LLM, no cloud, no self-rewrite of
  live cognition.
* Audit-first: every decision logged to luna_self_improvement_audit.jsonl
  BEFORE the test file is written.

Public API:
* measure_self() -> dict                 (the concrete before/after metric)
* run_improvement_cycle(...) -> dict     (one bounded cycle)
* run_overnight(...) -> dict             (loop + morning report)
* report() -> dict
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List

ROOT = r"D:\SurgeApp"
COG_GLOB = os.path.join(ROOT, "luna_modules", "cognitive_*.py")
SELF_TESTS_DIR = os.path.join(ROOT, "self_tests")
AUDIT_PATH = os.path.join(ROOT, "memory", "cognitive",
                          "luna_self_improvement_audit.jsonl")
REPORT_PATH = os.path.join(ROOT, "memory", "cognitive",
                           "luna_self_improvement_report.md")
METRICS_PATH = os.path.join(ROOT, "memory", "cognitive",
                            "luna_self_improvement_metrics.json")
KILL_SWITCH = os.path.join(ROOT, "memory", "kill_switches",
                           "luna_self_improvement.disabled")
VENV_PY = os.path.join(ROOT, ".aider_venv", "Scripts", "python.exe")

# Bounds
_PER_CYCLE_TARGET_MAX = 12        # modules per cycle
_PER_TARGET_BUDGET_S = 90.0       # per generated-test run budget
_TOTAL_CYCLE_CAP = 100            # overnight hard cap on cycles
_OVERNIGHT_BUDGET_S = 6 * 3600    # 6h overnight wall budget


def _now_iso() -> str:
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    except Exception:  # noqa: BLE001
        return ""


def _killed() -> bool:
    try:
        return os.path.exists(KILL_SWITCH)
    except Exception:  # noqa: BLE001
        return False


def _audit(event: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
        rec = dict(event)
        rec.setdefault("ts_iso", _now_iso())
        line = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
        with open(AUDIT_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:  # noqa: BLE001
        return


def _module_names() -> List[str]:
    try:
        out = []
        for p in sorted(glob.glob(COG_GLOB)):
            base = os.path.splitext(os.path.basename(p))[0]
            out.append(base)
        return out
    except Exception:  # noqa: BLE001
        return []


def _smoke_test_path(module_base: str) -> str:
    return os.path.join(SELF_TESTS_DIR, f"smoke_{module_base}.py")


def _has_smoke_test(module_base: str) -> bool:
    try:
        return os.path.exists(_smoke_test_path(module_base))
    except Exception:  # noqa: BLE001
        return False


def _module_path(module_base: str) -> str:
    return os.path.join(ROOT, "luna_modules", f"{module_base}.py")


def _defines_report(module_base: str) -> bool:
    """Static text scan (NO import) for a top-level `def report(`.
    Fast and side-effect-free. NEVER raises."""
    try:
        with open(_module_path(module_base), "r", encoding="utf-8",
                  errors="replace") as fh:
            for line in fh:
                s = line.lstrip()
                if s.startswith("def report(") or s.startswith("def report ("):
                    # top-level only (no leading indent on the original line)
                    if line[:1] not in (" ", "\t"):
                        return True
        return False
    except Exception:  # noqa: BLE001
        return False


def measure_self() -> Dict[str, Any]:
    """Concrete self-health metric. NEVER raises.

    Two honest coverage axes:
      * smoke_coverage_pct  — % of cognitive modules with a PASSING
        per-module smoke test (import + report()-never-raise + reimport).
      * report_coverage_pct — % that expose a report() introspection
        surface at all (static scan, no import)."""
    try:
        mods = _module_names()
        total = len(mods)
        smoke = sum(1 for m in mods if _has_smoke_test(m))
        with_report = sum(1 for m in mods if _defines_report(m))
        smoke_pct = (round(100.0 * smoke / total, 1) if total else 0.0)
        report_pct = (round(100.0 * with_report / total, 1) if total else 0.0)
        return {
            "ts_iso": _now_iso(),
            "total_cognitive_modules": total,
            "smoke_tested": smoke,
            "smoke_coverage_pct": smoke_pct,
            "report_defined": with_report,
            "report_coverage_pct": report_pct,
            "self_tests_dir": SELF_TESTS_DIR,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": f"measure_failed:{type(exc).__name__}"}


_SMOKE_TEMPLATE = '''"""Auto-generated smoke test for luna_modules.{mod}.
Generated by luna_self_improvement (deterministic template — no LLM).
Proves: module imports cleanly, report() (if present) never raises,
and a re-import is stable. Reversible: delete D:\\\\SurgeApp\\\\self_tests\\\\.
"""
import sys
sys.path.insert(0, r"D:\\SurgeApp")


def test_imports_clean():
    import importlib
    m = importlib.import_module("luna_modules.{mod}")
    assert m is not None


def test_report_never_raises():
    import importlib
    m = importlib.import_module("luna_modules.{mod}")
    fn = getattr(m, "report", None)
    if callable(fn):
        try:
            out = fn()
        except Exception as exc:  # report() must never raise
            raise AssertionError(
                "{mod}.report() raised " + type(exc).__name__ + ": " + str(exc))
        assert out is None or isinstance(out, (dict, list, str))


def test_reimport_stable():
    import importlib
    m1 = importlib.import_module("luna_modules.{mod}")
    m2 = importlib.import_module("luna_modules.{mod}")
    assert m1 is m2


if __name__ == "__main__":
    test_imports_clean()
    test_report_never_raises()
    test_reimport_stable()
    print("SMOKE_OK:{mod}")
'''


def _generate_and_verify(module_base: str,
                          budget_s: float = _PER_TARGET_BUDGET_S) -> Dict[str, Any]:
    """Generate a deterministic smoke test for one module, run it, keep it
    only if it passes. NEVER raises. Returns outcome dict."""
    try:
        os.makedirs(SELF_TESTS_DIR, exist_ok=True)
        path = _smoke_test_path(module_base)
        tmp = path + ".tmp"
        content = _SMOKE_TEMPLATE.format(mod=module_base)
        # Write to a temp path first; run it; only rename into place if pass.
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(content)
        except Exception as exc:  # noqa: BLE001
            return {"module": module_base, "ok": False,
                    "reason": f"write_failed:{type(exc).__name__}"}
        # Run the smoke test as __main__ in a subprocess (isolated).
        try:
            proc = subprocess.run(
                [VENV_PY, tmp],
                capture_output=True, text=True,
                timeout=budget_s, cwd=ROOT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            passed = (f"SMOKE_OK:{module_base}" in (proc.stdout or ""))
            err_tail = (proc.stderr or "")[-200:]
        except Exception as exc:  # noqa: BLE001
            passed = False
            err_tail = f"run_exception:{type(exc).__name__}"
        if passed:
            try:
                os.replace(tmp, path)
            except Exception as exc:  # noqa: BLE001
                return {"module": module_base, "ok": False,
                        "reason": f"rename_failed:{type(exc).__name__}"}
            return {"module": module_base, "ok": True, "kept": True}
        else:
            # Test failed -> this module's report() raised or import broke.
            # Do NOT keep the test; FLAG the module for operator review.
            try:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            except Exception:  # noqa: BLE001
                pass
            return {"module": module_base, "ok": False, "kept": False,
                    "reason": "smoke_failed", "err_tail": err_tail}
    except Exception as exc:  # noqa: BLE001
        return {"module": module_base, "ok": False,
                "reason": f"generate_exception:{type(exc).__name__}"}


def run_improvement_cycle(*, max_targets: int = _PER_CYCLE_TARGET_MAX,
                           per_target_budget_s: float = _PER_TARGET_BUDGET_S
                           ) -> Dict[str, Any]:
    """One bounded self-improvement cycle. NEVER raises."""
    started = time.time()
    if _killed():
        _audit({"event": "cycle_skipped", "reason": "kill_switch"})
        return {"ok": False, "reason": "kill_switch", "improved": 0}
    try:
        mods = _module_names()
        targets = [m for m in mods if not _has_smoke_test(m)][:max(1, int(max_targets))]
        improved = 0
        flagged: List[Dict[str, Any]] = []
        for m in targets:
            if _killed():
                break
            _audit({"event": "target_start", "module": m})
            res = _generate_and_verify(m, budget_s=per_target_budget_s)
            _audit({"event": "target_done", **res})
            if res.get("ok") and res.get("kept"):
                improved += 1
            elif res.get("reason") == "smoke_failed":
                flagged.append({"module": m, "err_tail": res.get("err_tail")})
        return {
            "ok": True,
            "targets_attempted": len(targets),
            "improved": improved,
            "flagged_modules": flagged,
            "elapsed_s": round(time.time() - started, 1),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"cycle_exception:{type(exc).__name__}",
                "improved": 0}


def verify_all(*, per_test_budget_s: float = 60.0,
                max_tests: int = 1000) -> Dict[str, Any]:
    """Run every EXISTING smoke test and tally pass/fail — a live
    'is my brain currently healthy?' verdict. Does NOT generate or modify
    anything. Read-only over self_tests\\. NEVER raises."""
    started = time.time()
    try:
        paths = sorted(glob.glob(os.path.join(SELF_TESTS_DIR, "smoke_*.py")))[:max_tests]
        passed = 0
        failed: List[str] = []
        for p in paths:
            base = os.path.splitext(os.path.basename(p))[0]
            mod = base[len("smoke_"):] if base.startswith("smoke_") else base
            ok = False
            try:
                proc = subprocess.run(
                    [VENV_PY, p], capture_output=True, text=True,
                    timeout=per_test_budget_s, cwd=ROOT,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                ok = (f"SMOKE_OK:{mod}" in (proc.stdout or ""))
            except Exception:  # noqa: BLE001
                ok = False
            if ok:
                passed += 1
            else:
                failed.append(mod)
        total = len(paths)
        verdict = {
            "ts_iso": _now_iso(),
            "tests_run": total,
            "passed": passed,
            "failed": len(failed),
            "failed_modules": failed[:50],
            "pass_rate_pct": (round(100.0 * passed / total, 1) if total else 0.0),
            "elapsed_s": round(time.time() - started, 1),
        }
        _audit({"event": "verify_all", **{k: verdict[k] for k in
                ("tests_run", "passed", "failed")}})
        return verdict
    except Exception as exc:  # noqa: BLE001
        return {"error": f"verify_failed:{type(exc).__name__}"}


def run_overnight(*, max_cycles: int = _TOTAL_CYCLE_CAP,
                   total_budget_s: float = _OVERNIGHT_BUDGET_S,
                   per_cycle_targets: int = _PER_CYCLE_TARGET_MAX
                   ) -> Dict[str, Any]:
    """Loop improvement cycles until coverage is complete, budget exhausted,
    cycle cap hit, or kill-switch. Writes a morning report. NEVER raises."""
    started = time.time()
    before = measure_self()
    _audit({"event": "overnight_start", "before": before})
    cycles = 0
    total_improved = 0
    all_flagged: List[Dict[str, Any]] = []
    try:
        while cycles < max_cycles and (time.time() - started) < total_budget_s:
            if _killed():
                _audit({"event": "overnight_killed", "cycle": cycles})
                break
            res = run_improvement_cycle(max_targets=per_cycle_targets)
            cycles += 1
            total_improved += int(res.get("improved") or 0)
            for f in (res.get("flagged_modules") or []):
                all_flagged.append(f)
            # Nothing left to improve -> done.
            if (res.get("targets_attempted") or 0) == 0:
                break
            if not res.get("ok"):
                break
    except Exception as exc:  # noqa: BLE001
        _audit({"event": "overnight_exception",
                "exc": type(exc).__name__})
    after = measure_self()
    summary = {
        "started_utc": _now_iso(),
        "cycles_run": cycles,
        "modules_improved": total_improved,
        "flagged_count": len(all_flagged),
        "flagged": all_flagged[:50],
        "before": before,
        "after": after,
        "elapsed_s": round(time.time() - started, 1),
    }
    _audit({"event": "overnight_done", **{k: summary[k] for k in
            ("cycles_run", "modules_improved", "flagged_count")}})
    try:
        _write_report(summary)
        with open(METRICS_PATH, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(summary, indent=2))
    except Exception:  # noqa: BLE001
        pass
    return summary


def _write_report(summary: Dict[str, Any]) -> None:
    try:
        b = summary.get("before") or {}
        a = summary.get("after") or {}
        bc = b.get("smoke_coverage_pct", 0)
        ac = a.get("smoke_coverage_pct", 0)
        lines = [
            "# Luna Self-Improvement — Overnight Report",
            "",
            f"Run finished: {summary.get('started_utc')}",
            f"Cycles run: {summary.get('cycles_run')}",
            f"Wall time: {summary.get('elapsed_s')}s",
            "",
            "## Self-verification coverage (the concrete metric)",
            "",
            f"* BEFORE: {b.get('smoke_tested')}/{b.get('total_cognitive_modules')} "
            f"modules smoke-verified ({bc}%)",
            f"* AFTER:  {a.get('smoke_tested')}/{a.get('total_cognitive_modules')} "
            f"modules smoke-verified ({ac}%)",
            f"* Modules newly self-verified this run: {summary.get('modules_improved')}",
            "",
            "## report() introspection coverage (secondary metric)",
            "",
            f"* {a.get('report_defined')}/{a.get('total_cognitive_modules')} "
            f"modules expose a report() surface ({a.get('report_coverage_pct')}%)",
            "",
            "## Modules Luna FLAGGED for operator review",
            "(smoke test failed — import broke or report() raised; test NOT kept)",
            "",
        ]
        flagged = summary.get("flagged") or []
        if flagged:
            for f in flagged:
                lines.append(f"* `{f.get('module')}` — {str(f.get('err_tail'))[:160]}")
        else:
            lines.append("* (none — every attempted module passed its smoke test)")
        lines += [
            "",
            "## Reversibility",
            "All generated tests live in `D:\\SurgeApp\\self_tests\\`. "
            "Delete that folder to undo everything. No cognition source, "
            "no flags, no DB were touched.",
            "",
            "## Kill-switch",
            "Create `memory/kill_switches/luna_self_improvement.disabled` to "
            "stop the engine on its next cycle.",
        ]
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception:  # noqa: BLE001
        return


def report() -> Dict[str, Any]:
    return {
        "module": "luna_self_improvement",
        "self_tests_dir": SELF_TESTS_DIR,
        "audit_path": AUDIT_PATH,
        "report_path": REPORT_PATH,
        "kill_switch": KILL_SWITCH,
        "kill_switch_active": _killed(),
        "bounds": {
            "per_cycle_target_max": _PER_CYCLE_TARGET_MAX,
            "per_target_budget_s": _PER_TARGET_BUDGET_S,
            "total_cycle_cap": _TOTAL_CYCLE_CAP,
            "overnight_budget_s": _OVERNIGHT_BUDGET_S,
        },
        "doctrine": ["never_raises", "deterministic_no_llm", "reversible",
                     "adds_tests_only", "kill_switchable", "audit_first",
                     "no_cloud", "no_live_cognition_rewrite"],
        "current_metric": measure_self(),
    }


__all__ = ["measure_self", "verify_all", "run_improvement_cycle",
           "run_overnight", "report"]


def _safe_emit(obj: Any) -> None:
    """Print only if a real stdout exists. Under pythonw.exe sys.stdout is
    None and print() raises — that silent-crash trap is documented in this
    codebase, so the scheduled task uses run_overnight() directly (results
    go to files) and this CLI path guards itself."""
    try:
        if sys.stdout is not None:
            sys.stdout.write(json.dumps(obj, indent=2) + "\n")
            sys.stdout.flush()
    except Exception:  # noqa: BLE001
        return


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "measure"
    if cmd == "overnight":
        _safe_emit(run_overnight())
    elif cmd == "cycle":
        _safe_emit(run_improvement_cycle())
    elif cmd == "verify":
        _safe_emit(verify_all())
    else:
        _safe_emit(measure_self())
