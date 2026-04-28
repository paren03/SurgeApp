# File: D:\SurgeApp\aider_bridge.py
# Purpose: Aider Bridge — watches D:\SurgeApp\aider_jobs\active for jobs queued by Luna,
#          stages an edit on a copy in logic_updates\<task_id>\, runs Aider against the
#          copy via the project's .aider_venv, verifies syntax + import, optionally
#          applies the patch, and reports every stage to D:\SurgeApp\logs\luna_live_feed.jsonl.
#
# Stages emitted to the live feed (the right Inspector terminal in the UI):
#   CLAIM              -> picked up a job from active/
#   RUN_AIDER_START    -> aider command launched
#   RUN_AIDER_END      -> aider returned (rc captured)
#   DIFF_SAVED         -> unified diff written to solutions/<task_id>.diff
#   VERIFY_COMPILE     -> py_compile result on the staged copy
#   VERIFY_IMPORT      -> `python -c "import <module>"` on the staged copy (worker.py only)
#   APPLY              -> only when apply_on_pass=true AND both verifications pass
#   DONE / FAILED      -> terminal status; job json moved to done/ or failed/
#
# Safety:
#   - NEVER touches a real source file unless apply_on_pass is true AND both verifications pass.
#   - Never prints to stdout (this script runs under pythonw, no console).
#   - All file IO is UTF-8 with errors='replace'; JSON is ensure_ascii=True.

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime
from difflib import unified_diff
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------------
DEFAULT_PROJECT_DIR = r"D:\SurgeApp"
PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", DEFAULT_PROJECT_DIR))

AIDER_JOBS_DIR = PROJECT_DIR / "aider_jobs"
ACTIVE_DIR = AIDER_JOBS_DIR / "active"
DONE_DIR = AIDER_JOBS_DIR / "done"
FAILED_DIR = AIDER_JOBS_DIR / "failed"

LOGIC_DIR = PROJECT_DIR / "logic_updates"
SOLUTIONS_DIR = PROJECT_DIR / "solutions"
LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"
LIVE_FEED_PATH = LOGS_DIR / "luna_live_feed.jsonl"
BRIDGE_LOG_PATH = LOGS_DIR / "aider_bridge.log"

POLL_INTERVAL = float(os.environ.get("AIDER_BRIDGE_POLL", "1.5"))
APPLY_ON_PASS_DEFAULT = bool(int(os.environ.get("AIDER_APPLY_ON_PASS", "0") or "0"))
AIDER_MODEL = os.environ.get("AIDER_MODEL", "ollama_chat/llama3.1:8b-instruct-q4_K_M")
AIDER_TIMEOUT = int(os.environ.get("AIDER_TIMEOUT", "300"))

AIDER_FLAGS: List[str] = [
    "--no-auto-commits",
    "--no-pretty",
    "--yes-always",
    "--no-stream",
]

CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
def _ensure_layout() -> None:
    for d in (
        AIDER_JOBS_DIR, ACTIVE_DIR, DONE_DIR, FAILED_DIR,
        LOGIC_DIR, SOLUTIONS_DIR, LOGS_DIR, MEMORY_DIR,
    ):
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _hms() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Logging  (no stdout — pythonw will crash on cp1252 surrogates if we print)
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    try:
        BRIDGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with BRIDGE_LOG_PATH.open("a", encoding="utf-8", errors="replace") as f:
            f.write(f"[{_now_iso()}] {msg}\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Live feed helper — schema-compatible with luna_modules.luna_live_feed
# ---------------------------------------------------------------------------
_FEED_ICONS = {
    "CLAIM":           "[CLAIM] ",
    "RUN_AIDER_START": "[AIDER] ",
    "RUN_AIDER_END":   "[AIDER] ",
    "DIFF_SAVED":      "[DIFF]  ",
    "VERIFY_COMPILE":  "[VERIFY]",
    "VERIFY_IMPORT":   "[VERIFY]",
    "APPLY":           "[APPLY] ",
    "DONE":            "[DONE]  ",
    "FAILED":          "[FAIL]  ",
}


def _feed(event: str, msg: str, detail: str = "", task_id: str = "") -> None:
    """Append one JSON event line to luna_live_feed.jsonl. ensure_ascii=True (Windows-safe)."""
    try:
        row: Dict[str, Any] = {
            "ts":     _hms(),
            "event":  event,
            "icon":   _FEED_ICONS.get(event, "[INFO]  "),
            "msg":    str(msg or "")[:240],
            "source": "aider_bridge",
        }
        if detail:
            row["detail"] = str(detail)[:600]
        if task_id:
            row["task_id"] = str(task_id)
        LIVE_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LIVE_FEED_PATH.open("a", encoding="utf-8", errors="replace") as f:
            json.dump(row, f, ensure_ascii=True)
            f.write("\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Job IO
# ---------------------------------------------------------------------------
def _read_job(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex[:6]}.tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8", errors="replace")
        tmp.replace(path)
    except Exception:
        pass


def _update_job(path: Path, **updates: Any) -> Dict[str, Any]:
    data = _read_job(path)
    data.update(updates)
    _write_json(path, data)
    return data


# ---------------------------------------------------------------------------
# Python interpreter resolution (skip Microsoft Store alias stubs)
# ---------------------------------------------------------------------------
def _is_alias_stub(path: Path) -> bool:
    try:
        s = str(path).lower()
        if "\\windowsapps\\" in s and path.suffix.lower() == ".exe":
            if "pythonsoftwarefoundation" in s:
                return False
            return True
        return path.exists() and path.stat().st_size == 0
    except Exception:
        return False


def _resolve_python() -> str:
    """Prefer the project's aider venv; otherwise use a non-stub interpreter."""
    venv_py = PROJECT_DIR / ".aider_venv" / "Scripts" / "python.exe"
    if venv_py.exists() and not _is_alias_stub(venv_py):
        return str(venv_py)

    here = Path(sys.executable)
    if here.exists() and not _is_alias_stub(here):
        return str(here)

    fallback = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WindowsApps" \
        / "PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0" / "python.exe"
    if fallback.exists() and not _is_alias_stub(fallback):
        return str(fallback)

    return "python"


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------
def _verify_compile(staged: Path) -> Tuple[bool, str]:
    if staged.suffix.lower() != ".py":
        return True, "non-python: skipped py_compile"
    try:
        import py_compile
        py_compile.compile(str(staged), doraise=True)
        return True, "py_compile OK"
    except Exception as exc:
        return False, f"py_compile failed: {exc}"


def _verify_import(staged: Path, py: str) -> Tuple[bool, str]:
    """Run `python -c 'import worker'` against a temp dir containing the staged file
    so we don't disturb the live module on disk. Only attempted for worker.py."""
    if staged.name.lower() != "worker.py":
        return True, "import check skipped (non-worker target)"
    try:
        import tempfile
        with tempfile.TemporaryDirectory(prefix="aider_verify_") as td:
            tmp_dir = Path(td)
            shutil.copy2(staged, tmp_dir / "worker.py")
            sib = PROJECT_DIR / "luna_modules"
            if sib.exists() and sib.is_dir():
                try:
                    shutil.copytree(sib, tmp_dir / "luna_modules", dirs_exist_ok=True)
                except Exception:
                    pass
            env = {**os.environ, "LUNA_PROJECT_DIR": str(PROJECT_DIR), "PYTHONIOENCODING": "utf-8"}
            cmd = [py, "-c",
                   "import sys, importlib;"
                   "sys.path.insert(0, r'%s');"
                   "importlib.import_module('worker');"
                   "print('IMPORT_OK')" % str(tmp_dir)]
            proc = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=60, env=env, creationflags=CREATE_NO_WINDOW,
            )
            ok = proc.returncode == 0 and "IMPORT_OK" in (proc.stdout or "")
            detail = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
            return ok, detail.strip()[:600] or ("ok" if ok else "import failed")
    except Exception as exc:
        return False, f"import verify error: {exc}"


def _make_diff(original: Path, patched: Path) -> str:
    try:
        a = original.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        b = patched.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        return "".join(unified_diff(a, b, fromfile=f"a/{original.name}", tofile=f"b/{patched.name}"))
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------
def _move_job(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    try:
        os.replace(str(src), str(dest))
    except Exception:
        try:
            shutil.copy2(str(src), str(dest))
            try:
                src.unlink(missing_ok=True)
            except Exception:
                pass
        except Exception:
            pass
    return dest


def _finish(job_path: Path, task_id: str, success: bool, summary: Dict[str, Any]) -> None:
    final_status = "done" if success else "failed"
    summary.update({
        "status": final_status,
        "state": final_status,
        "phase": "complete",
        "progress": 100,
        "finished_at": _now_iso(),
    })
    _update_job(job_path, **summary)
    dest_dir = DONE_DIR if success else FAILED_DIR
    final_path = _move_job(job_path, dest_dir)
    _feed("DONE" if success else "FAILED", f"task {task_id} {final_status}",
          detail=str(summary.get("summary") or summary.get("error") or "")[:600],
          task_id=task_id)
    _log(f"finalized task={task_id} status={final_status} -> {final_path}")


def _instruction_text(job: Dict[str, Any]) -> str:
    return str(job.get("instructions") or job.get("prompt") or "").strip()


def _resolve_targets(job: Dict[str, Any]) -> List[str]:
    raw = job.get("target_files") or job.get("target")
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list) or not raw:
        return ["worker.py"]
    return [str(x).strip() for x in raw if str(x).strip()]


def _run_aider(staged_dir: Path, copy_paths: List[Path], instructions: str, py: str) -> Tuple[int, str, str]:
    cmd: List[str] = [py, "-m", "aider", "--model", AIDER_MODEL, *AIDER_FLAGS]
    for p in copy_paths:
        cmd += ["--file", str(p)]
    cmd += ["--message", instructions]

    env = {
        **os.environ,
        "LUNA_PROJECT_DIR": str(PROJECT_DIR),
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUTF8": "1",
        "AIDER_NO_GIT": "1",
    }
    try:
        proc = subprocess.run(
            cmd, cwd=str(staged_dir),
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=AIDER_TIMEOUT, env=env, creationflags=CREATE_NO_WINDOW,
        )
        return proc.returncode, proc.stdout or "", proc.stderr or ""
    except subprocess.TimeoutExpired:
        return 124, "", f"aider timed out after {AIDER_TIMEOUT}s"
    except FileNotFoundError as exc:
        return 127, "", f"aider not found in interpreter: {exc}"
    except Exception as exc:
        return 1, "", f"aider launch error: {exc}"


def process_job(job_path: Path) -> None:
    task_id = job_path.stem
    job = _read_job(job_path)
    instructions = _instruction_text(job)
    apply_on_pass = bool(job.get("apply_on_pass", APPLY_ON_PASS_DEFAULT))
    targets = _resolve_targets(job)

    _feed("CLAIM", f"Picked up {task_id}",
          detail=f"targets={targets} apply_on_pass={apply_on_pass}", task_id=task_id)
    _log(f"CLAIM task={task_id} targets={targets} apply_on_pass={apply_on_pass}")

    _update_job(job_path, status="running", state="running", phase="aider_patch",
                progress=10, started_at=_now_iso())

    if not instructions:
        _finish(job_path, task_id, False,
                {"error": "missing instructions", "summary": "no instructions provided"})
        return

    staged_dir = LOGIC_DIR / task_id
    staged_dir.mkdir(parents=True, exist_ok=True)
    copy_paths: List[Path] = []
    real_paths: List[Path] = []
    for rel in targets:
        real = (PROJECT_DIR / rel).resolve()
        if not real.exists():
            _finish(job_path, task_id, False,
                    {"error": f"target not found: {rel}",
                     "summary": f"target file missing: {rel}"})
            return
        real_paths.append(real)
        copy = staged_dir / real.name
        try:
            shutil.copy2(real, copy)
        except Exception as exc:
            _finish(job_path, task_id, False,
                    {"error": f"stage copy failed for {rel}: {exc}",
                     "summary": "could not stage copy"})
            return
        copy_paths.append(copy)

    py = _resolve_python()

    _feed("RUN_AIDER_START", f"aider editing {', '.join(p.name for p in copy_paths)}",
          detail=f"py={py} model={AIDER_MODEL}", task_id=task_id)
    _update_job(job_path, progress=30)
    rc, out, err = _run_aider(staged_dir, copy_paths, instructions, py)
    _feed("RUN_AIDER_END", f"aider rc={rc}",
          detail=(err or out or "").strip()[:400], task_id=task_id)
    _update_job(job_path, aider_rc=rc, progress=55)

    diffs: Dict[str, str] = {}
    for real, copy in zip(real_paths, copy_paths):
        d = _make_diff(real, copy)
        diffs[real.name] = d
        if d:
            try:
                (SOLUTIONS_DIR / f"{task_id}__{real.name}.diff").write_text(d, encoding="utf-8", errors="replace")
            except Exception:
                pass
    combined_diff_path = SOLUTIONS_DIR / f"{task_id}.diff"
    try:
        body = "\n\n".join(f"# === {n} ===\n{d}" for n, d in diffs.items() if d) or "(no changes)\n"
        combined_diff_path.write_text(body, encoding="utf-8", errors="replace")
        (SOLUTIONS_DIR / f"{task_id}.log").write_text(
            (out or "") + "\n--- STDERR ---\n" + (err or ""),
            encoding="utf-8", errors="replace",
        )
        _feed("DIFF_SAVED", f"diff written for {task_id}",
              detail=str(combined_diff_path), task_id=task_id)
    except Exception as exc:
        _log(f"diff persist failed: {exc}")

    if not any(diffs.values()):
        _finish(job_path, task_id, False, {
            "error": "no changes produced by aider",
            "aider_rc": rc,
            "summary": (err or out or "aider produced no diff").strip()[:400],
            "solution_path": str(combined_diff_path),
        })
        return

    compile_results: Dict[str, str] = {}
    import_results: Dict[str, str] = {}
    all_compile_ok = True
    all_import_ok = True
    for copy in copy_paths:
        ok, detail = _verify_compile(copy)
        compile_results[copy.name] = detail
        all_compile_ok = all_compile_ok and ok
        _feed("VERIFY_COMPILE", f"{copy.name}: {'PASS' if ok else 'FAIL'}",
              detail=detail, task_id=task_id)
        if ok:
            iok, idetail = _verify_import(copy, py)
            import_results[copy.name] = idetail
            all_import_ok = all_import_ok and iok
            _feed("VERIFY_IMPORT", f"{copy.name}: {'PASS' if iok else 'FAIL'}",
                  detail=idetail, task_id=task_id)
        else:
            import_results[copy.name] = "skipped (compile failed)"
            all_import_ok = False

    verification_passed = all_compile_ok and all_import_ok
    _update_job(job_path,
                verify_compile=compile_results,
                verify_import=import_results,
                verify_passed=verification_passed,
                progress=80)

    applied: List[str] = []
    apply_error = ""
    if apply_on_pass and verification_passed:
        for real, copy in zip(real_paths, copy_paths):
            try:
                bak = real.with_suffix(real.suffix + f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                shutil.copy2(real, bak)
                shutil.copy2(copy, real)
                applied.append(real.name)
                _feed("APPLY", f"applied to {real.name}",
                      detail=f"backup={bak.name}", task_id=task_id)
            except Exception as exc:
                apply_error = f"apply failed for {real.name}: {exc}"
                _feed("APPLY", apply_error, detail="", task_id=task_id)
                break

    status_line = (
        "APPLIED" if applied and not apply_error
        else ("PASS_STAGED" if verification_passed else "FAIL")
    )
    report_lines = [
        "# AIDER BRIDGE REPORT",
        f"# task_id={task_id}  status={status_line}",
        "",
        "## Targets",
        *(f"- {p}" for p in targets),
        "",
        "## Verification",
        f"compile  : {'PASS' if all_compile_ok else 'FAIL'}",
        f"import   : {'PASS' if all_import_ok else 'FAIL'}",
        f"applied  : {applied or '[]'}",
        "",
        "## Aider RC",
        f"rc       : {rc}",
        "",
        "## Diff (combined)",
        "```diff",
        (combined_diff_path.read_text(encoding="utf-8", errors="replace")[:8000]
         if combined_diff_path.exists() else "(no diff)"),
        "```",
    ]
    try:
        (SOLUTIONS_DIR / f"{task_id}.txt").write_text("\n".join(report_lines), encoding="utf-8", errors="replace")
    except Exception:
        pass

    summary = {
        "summary": status_line,
        "verify_passed": verification_passed,
        "applied": applied,
        "apply_error": apply_error,
        "solution_path": str(SOLUTIONS_DIR / f"{task_id}.txt"),
        "diff_path": str(combined_diff_path),
        "aider_rc": rc,
    }
    success = (status_line in ("APPLIED", "PASS_STAGED"))
    if apply_error:
        success = False
        summary["error"] = apply_error
    _finish(job_path, task_id, success, summary)


def main() -> int:
    _ensure_layout()
    _log(f"aider_bridge started; watching {ACTIVE_DIR}")
    _feed("CLAIM", "aider_bridge online (watching aider_jobs/active)", detail=f"poll={POLL_INTERVAL}s")

    seen: Dict[str, float] = {}
    while True:
        try:
            for job_path in sorted(ACTIVE_DIR.glob("*.json")):
                try:
                    st = job_path.stat()
                except Exception:
                    continue
                key = str(job_path)
                if seen.get(key) == st.st_mtime:
                    continue
                seen[key] = st.st_mtime
                try:
                    process_job(job_path)
                except Exception as exc:
                    _log(f"unhandled exception on {job_path.name}: {exc}")
                    try:
                        _feed("FAILED", f"unhandled exception on {job_path.stem}",
                              detail=str(exc), task_id=job_path.stem)
                        _finish(job_path, job_path.stem, False,
                                {"error": str(exc), "summary": "unhandled bridge exception"})
                    except Exception:
                        pass
        except Exception as exc:
            _log(f"watch loop error: {exc}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        raise SystemExit(main() or 0)
    except KeyboardInterrupt:
        raise SystemExit(0)
