"""Phase 5I: Luna Simulation Sandbox + Snapshot foundation.

Stdlib only. Read-mostly aside from snapshot/sandbox writes that stay strictly
inside `backups/sandbox_*`, `logic_updates/sandbox_*`, and the runtime report
files under memory/.

Key safety guarantees:
  * Real project source files are NEVER written by simulation.
  * restore_snapshot defaults to dry_run=True; non-dry-run requires
    allow_restore=True.
  * run_safe_command uses an allowlist; arbitrary shell strings are rejected
    and pip/install/git reset/delete/network commands are explicitly denied.
  * unified_diff patch records are validated but flagged
    `needs_external_patcher` rather than applied — Phase 5I refuses to
    silently corrupt files.

Tracked schema/config:
  memory/luna_sandbox.schema.json
  memory/luna_snapshot_manifest.schema.json
  memory/luna_sandbox_config.json

Generated runtime artifacts (gitignored):
  memory/luna_sandbox_report.json
  memory/luna_sandbox_report.md
  memory/luna_snapshot_manifest.jsonl
  backups/sandbox_<id>/
  logic_updates/sandbox_<id>/

CLI:
  python -m luna_modules.luna_sandbox --self-test
  python -m luna_modules.luna_sandbox --snapshot worker.py --reason "pre-edit"
  python -m luna_modules.luna_sandbox --simulate-replace
  python -m luna_modules.luna_sandbox --write-report
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

_DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "snapshot_root_relative": "backups",
    "sandbox_root_relative": "logic_updates",
    "manifest_path_relative": "memory/luna_snapshot_manifest.jsonl",
    "report_path_relative_json": "memory/luna_sandbox_report.json",
    "report_path_relative_md": "memory/luna_sandbox_report.md",
    "max_snapshot_file_bytes": 5_242_880,
    "default_command_timeout_seconds": 30,
    "max_command_timeout_seconds": 120,
    "default_command_allowlist": [
        "python -m py_compile",
        "python -m unittest",
        "python -c",
        "git status --short",
        "git diff --stat",
        "powershell -NoProfile -Command Test-Path",
    ],
    "denied_command_patterns": [
        r"(?i)\bpip(?:3)?\s+install\b",
        r"(?i)\bpython\s+-m\s+pip\s+install\b",
        r"(?i)\bnpm\s+install\b",
        r"(?i)\byarn\s+add\b",
        r"(?i)\bwinget\s+install\b",
        r"(?i)\bchoco\s+install\b",
        r"(?i)\bapt(?:-get)?\s+install\b",
        r"(?i)\bbrew\s+install\b",
        r"(?i)\bpacman\s+-S\b",
        r"(?i)\b(?:msiexec|setup\.exe)\b",
        r"(?i)Install-Module\b",
        r"(?i)Add-AppxPackage\b",
        r"(?i)\bgit\s+reset\b",
        r"(?i)\bgit\s+clean\b",
        r"(?i)\bgit\s+push\s+--force\b",
        r"(?i)\brm\s+-rf\b",
        r"(?i)\bdel\s+/[fqs]\b",
        r"(?i)\bRemove-Item\b",
        r"(?i)\brmdir\b",
        r"(?i)\btaskkill\b",
        r"(?i)\bcurl\s+(?!.*(?:127\.0\.0\.1|localhost))",
        r"(?i)\bwget\s+(?!.*(?:127\.0\.0\.1|localhost))",
        r"(?i)\bInvoke-WebRequest\b",
        r"(?i)\bInvoke-RestMethod\b",
    ],
    "supported_patch_types": ["replace_text", "append_text"],
    "unsupported_patch_types": ["unified_diff"],
    "unsupported_patch_note": (
        "Unified-diff records are validated for shape but not auto-applied. "
        "Phase 5I refuses to silently corrupt files; flag as "
        "needs_external_patcher."
    ),
    "restore_default_dry_run": True,
    "restore_requires_explicit_allow": True,
}

DEFAULT_CONFIG_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_sandbox_config.json"

VALID_RISK_LEVELS = ("low", "medium", "high", "critical")


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _short_uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def make_snapshot_id(prefix: str = "snap") -> str:
    return _short_uid(prefix)


def make_sandbox_id(prefix: str = "sandbox") -> str:
    return _short_uid(prefix)


def sha256_text(text: str) -> str:
    if isinstance(text, bytes):
        return hashlib.sha256(text).hexdigest()
    return hashlib.sha256(str(text).encode("utf-8", errors="replace")).hexdigest()


def sha256_file(path: Path | str) -> str:
    p = Path(path)
    if not p.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def normalize_project_relative(path: str | Path, project_dir: Path | str) -> str:
    p = Path(path)
    root = Path(project_dir).resolve()
    try:
        rel = p.resolve().relative_to(root)
    except (OSError, ValueError):
        return str(p).replace("\\", "/")
    return str(rel).replace("\\", "/")


def ensure_under_project(path: Path | str, project_dir: Path | str) -> Path:
    p = Path(path).resolve()
    root = Path(project_dir).resolve()
    try:
        p.relative_to(root)
    except ValueError:
        raise ValueError(f"path escapes project root: {p} not under {root}")
    return p


def safe_mkdir(path: Path | str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    p = config_path or DEFAULT_CONFIG_PATH
    cfg = load_json(p, default=None)
    if not isinstance(cfg, dict):
        merged = dict(_DEFAULT_CONFIG)
        merged["_source"] = "module_fallback"
        merged["_loaded_from_file"] = False
        return merged
    out = dict(_DEFAULT_CONFIG)
    for k, v in cfg.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    return out


# ---------- metadata + collection ----------


def file_metadata(
    path: Path | str, project_dir: Path | str | None = None
) -> dict[str, Any]:
    p = Path(path)
    rel = (
        normalize_project_relative(p, project_dir)
        if project_dir
        else str(p).replace("\\", "/")
    )
    if not p.exists():
        return {
            "relative_path": rel,
            "original_path": str(p).replace("\\", "/"),
            "exists_before": False,
            "size_bytes": 0,
            "sha256": "",
            "modified_at": "",
        }
    try:
        st = p.stat()
        mtime = _dt.datetime.fromtimestamp(st.st_mtime, _dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        return {
            "relative_path": rel,
            "original_path": str(p).replace("\\", "/"),
            "exists_before": True,
            "size_bytes": int(st.st_size),
            "sha256": sha256_file(p),
            "modified_at": mtime,
        }
    except OSError:
        return {
            "relative_path": rel,
            "original_path": str(p).replace("\\", "/"),
            "exists_before": False,
            "size_bytes": 0,
            "sha256": "",
            "modified_at": "",
        }


def collect_target_metadata(
    project_dir: Path | str, target_files: Iterable[str]
) -> list[dict[str, Any]]:
    pdir = Path(project_dir)
    out: list[dict[str, Any]] = []
    for rel in target_files:
        p = pdir / rel
        out.append(file_metadata(p, pdir))
    return out


# ---------- snapshots ----------


def create_filesystem_snapshot(
    project_dir: Path | str,
    target_files: Iterable[str],
    snapshot_root: Path | str | None = None,
    reason: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or load_config()
    pdir = Path(project_dir).resolve()
    root_rel = cfg.get("snapshot_root_relative", "backups")
    root = Path(snapshot_root).resolve() if snapshot_root else (pdir / root_rel).resolve()
    safe_mkdir(root)
    snap_id = make_snapshot_id()
    snap_dir = (root / f"sandbox_{snap_id}").resolve()
    safe_mkdir(snap_dir)
    max_bytes = int(cfg.get("max_snapshot_file_bytes", 5_242_880))
    targets: list[dict[str, Any]] = []
    for rel in target_files:
        rel_norm = rel.replace("\\", "/").lstrip("./")
        src = (pdir / rel_norm).resolve()
        meta = file_metadata(src, pdir)
        if meta["exists_before"] and meta["size_bytes"] > max_bytes:
            meta["snapshot_path"] = ""
            meta["safety_note"] = (
                f"file too large to snapshot ({meta['size_bytes']} > {max_bytes})"
            )
            targets.append(meta)
            continue
        snap_target = snap_dir / rel_norm
        safe_mkdir(snap_target.parent)
        if meta["exists_before"]:
            try:
                shutil.copy2(src, snap_target)
            except OSError as e:
                meta["safety_note"] = f"copy_failed:{type(e).__name__}:{e}"
                targets.append(meta)
                continue
        meta["snapshot_path"] = str(snap_target).replace("\\", "/")
        targets.append(meta)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snap_id,
        "created_at": now_iso(),
        "project_dir": str(pdir).replace("\\", "/"),
        "reason": reason,
        "snapshot_dir": str(snap_dir).replace("\\", "/"),
        "targets": targets,
        "restore_instructions": (
            "Use restore_snapshot(manifest, project_dir, dry_run=True) for a "
            "preview, or pass dry_run=False with allow_restore=True to actually "
            "overwrite the listed targets from the snapshot directory."
        ),
        "safety_notes": [
            "Snapshot directory is gitignored at runtime.",
            "Restore is dry-run by default and requires explicit allow_restore=True.",
        ],
    }
    return manifest


def append_manifest_row(
    manifest: dict[str, Any],
    manifest_path: Path | str,
    project_root: Path | str,
) -> Path:
    manifest_p = Path(manifest_path)
    ensure_under_project(manifest_p, project_root)
    safe_mkdir(manifest_p.parent)
    with manifest_p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(manifest, sort_keys=False) + "\n")
    return manifest_p


def validate_snapshot_manifest(manifest: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(manifest, dict):
        return False, ["manifest not a dict"]
    for k in (
        "schema_version",
        "snapshot_id",
        "created_at",
        "project_dir",
        "snapshot_dir",
        "targets",
        "restore_instructions",
    ):
        if k not in manifest:
            errors.append(f"manifest.{k} missing")
    if not isinstance(manifest.get("targets"), list):
        errors.append("manifest.targets must be list")
    return (not errors), errors


def restore_snapshot(
    snapshot_manifest: dict[str, Any],
    project_dir: Path | str,
    dry_run: bool = True,
    allow_restore: bool = False,
) -> dict[str, Any]:
    pdir = Path(project_dir).resolve()
    plan: list[dict[str, Any]] = []
    for tgt in snapshot_manifest.get("targets", []):
        rel = tgt.get("relative_path") or ""
        src_snap = tgt.get("snapshot_path") or ""
        dst = (pdir / rel).resolve()
        plan.append(
            {
                "relative_path": rel,
                "from": src_snap,
                "to": str(dst).replace("\\", "/"),
                "would_overwrite": dst.is_file(),
            }
        )
    if dry_run:
        return {
            "dry_run": True,
            "applied": False,
            "plan": plan,
            "note": "No files modified — pass dry_run=False AND allow_restore=True to actually restore.",
        }
    if not allow_restore:
        return {
            "dry_run": False,
            "applied": False,
            "plan": plan,
            "blocked_reason": "allow_restore=False — explicit override required",
        }
    restored: list[str] = []
    failed: list[dict[str, str]] = []
    for tgt in snapshot_manifest.get("targets", []):
        rel = tgt.get("relative_path") or ""
        src_snap = tgt.get("snapshot_path") or ""
        if not rel or not src_snap or not Path(src_snap).is_file():
            failed.append({"relative_path": rel, "reason": "missing_snapshot_source"})
            continue
        dst = (pdir / rel).resolve()
        try:
            ensure_under_project(dst, pdir)
        except ValueError as e:
            failed.append({"relative_path": rel, "reason": f"escapes_project:{e}"})
            continue
        try:
            safe_mkdir(dst.parent)
            shutil.copy2(src_snap, dst)
            restored.append(rel)
        except OSError as e:
            failed.append(
                {"relative_path": rel, "reason": f"{type(e).__name__}:{e}"}
            )
    return {
        "dry_run": False,
        "applied": True,
        "plan": plan,
        "restored": restored,
        "failed": failed,
    }


# ---------- sandbox workspace ----------


def create_sandbox_workspace(
    project_dir: Path | str,
    sandbox_root: Path | str | None = None,
    sandbox_id: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or load_config()
    pdir = Path(project_dir).resolve()
    root_rel = cfg.get("sandbox_root_relative", "logic_updates")
    root = Path(sandbox_root).resolve() if sandbox_root else (pdir / root_rel).resolve()
    safe_mkdir(root)
    sid = sandbox_id or make_sandbox_id()
    sandbox_dir = root / f"sandbox_{sid}"
    safe_mkdir(sandbox_dir)
    return {
        "sandbox_id": sid,
        "sandbox_dir": str(sandbox_dir).replace("\\", "/"),
        "project_dir": str(pdir).replace("\\", "/"),
        "created_at": now_iso(),
    }


def copy_targets_to_sandbox(
    project_dir: Path | str,
    target_files: Iterable[str],
    sandbox_dir: Path | str,
) -> list[dict[str, Any]]:
    pdir = Path(project_dir).resolve()
    sdir = Path(sandbox_dir).resolve()
    safe_mkdir(sdir)
    out: list[dict[str, Any]] = []
    for rel in target_files:
        rel_norm = rel.replace("\\", "/").lstrip("./")
        src = (pdir / rel_norm).resolve()
        dst = (sdir / rel_norm).resolve()
        rec: dict[str, Any] = {
            "relative_path": rel_norm,
            "src": str(src).replace("\\", "/"),
            "dst": str(dst).replace("\\", "/"),
            "copied": False,
            "exists_before": src.is_file(),
        }
        if src.is_file():
            try:
                safe_mkdir(dst.parent)
                shutil.copy2(src, dst)
                rec["copied"] = True
                rec["sha256_after_copy"] = sha256_file(dst)
            except OSError as e:
                rec["error"] = f"{type(e).__name__}:{e}"
        else:
            rec["note"] = "source missing — sandbox starts empty for this target"
        out.append(rec)
    return out


# ---------- simulation plan / patches ----------


def build_simulation_plan(
    goal: str,
    target_files: list[str],
    verification_commands: list[str] | None = None,
    expected_artifacts: list[str] | None = None,
    patch_records: list[dict[str, Any]] | None = None,
    risk_level: str = "low",
    approval_tier_required: int = 2,
    rollback_required: bool = True,
) -> dict[str, Any]:
    if risk_level not in VALID_RISK_LEVELS:
        risk_level = "low"
    if not isinstance(approval_tier_required, int):
        approval_tier_required = 2
    return {
        "plan_id": _short_uid("plan"),
        "goal": goal or "",
        "target_files": [t.replace("\\", "/") for t in (target_files or [])],
        "patch_records": list(patch_records or []),
        "verification_commands": list(verification_commands or []),
        "expected_artifacts": list(expected_artifacts or []),
        "risk_level": risk_level,
        "approval_tier_required": int(approval_tier_required),
        "rollback_required": bool(rollback_required),
        "created_at": now_iso(),
    }


def _validate_patch_record(rec: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(rec, dict):
        return ["patch_record not a dict"]
    if "target_file" not in rec or not isinstance(rec["target_file"], str):
        errors.append("patch_record.target_file missing or not str")
    pt = rec.get("patch_type")
    if pt not in {"replace_text", "append_text", "unified_diff"}:
        errors.append(f"patch_record.patch_type invalid: {pt!r}")
    if pt == "replace_text":
        if "find_text" not in rec or "replace_text" not in rec:
            errors.append("replace_text requires find_text + replace_text")
    if pt == "append_text" and "append_text" not in rec:
        errors.append("append_text requires append_text")
    if pt == "unified_diff" and "unified_diff" not in rec:
        errors.append("unified_diff requires unified_diff")
    return errors


def validate_simulation_plan(plan: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(plan, dict):
        return False, ["plan not a dict"]
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
        if k not in plan:
            errors.append(f"plan.{k} missing")
    if plan.get("risk_level") not in VALID_RISK_LEVELS:
        errors.append(f"plan.risk_level invalid: {plan.get('risk_level')!r}")
    tier = plan.get("approval_tier_required")
    if not isinstance(tier, int) or not (0 <= tier <= 5):
        errors.append("plan.approval_tier_required must be int in [0,5]")
    for i, pr in enumerate(plan.get("patch_records") or []):
        for e in _validate_patch_record(pr):
            errors.append(f"patch_records[{i}]: {e}")
    return (not errors), errors


def apply_unified_diff_to_text(original_text: str, diff_text: str) -> dict[str, Any]:
    """Phase 5I refuses to silently corrupt files. Validate shape only."""
    if not isinstance(original_text, str):
        return {"applied": False, "reason": "original_text not str"}
    if not isinstance(diff_text, str) or not diff_text.strip():
        return {"applied": False, "reason": "diff_text empty or not str"}
    looks_like_diff = bool(
        re.search(r"^@@ ", diff_text, re.MULTILINE)
        or re.search(r"^---\s", diff_text, re.MULTILINE)
    )
    return {
        "applied": False,
        "reason": "needs_external_patcher",
        "looks_like_unified_diff": looks_like_diff,
        "note": (
            "Phase 5I does not auto-apply unified diffs. Use replace_text "
            "or append_text patch records instead."
        ),
        "result_text": original_text,
    }


def _apply_replace_text(
    text: str, find_text: str, replace_text: str
) -> tuple[str, int]:
    if not find_text:
        return text, 0
    count = text.count(find_text)
    if count == 0:
        return text, 0
    return text.replace(find_text, replace_text), count


def apply_patch_to_sandbox(
    sandbox_dir: Path | str,
    project_dir: Path | str,
    patch_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    sdir = Path(sandbox_dir).resolve()
    pdir = Path(project_dir).resolve()
    results: list[dict[str, Any]] = []
    for rec in patch_records or []:
        errs = _validate_patch_record(rec)
        if errs:
            results.append({"applied": False, "errors": errs, "record": rec})
            continue
        target_rel = rec["target_file"].replace("\\", "/").lstrip("./")
        target_in_sandbox = (sdir / target_rel).resolve()
        try:
            ensure_under_project(target_in_sandbox, sdir)
        except ValueError as e:
            results.append(
                {
                    "applied": False,
                    "errors": [f"target escapes sandbox: {e}"],
                    "record": rec,
                }
            )
            continue
        original = ""
        if target_in_sandbox.is_file():
            try:
                original = target_in_sandbox.read_text(encoding="utf-8", errors="replace")
            except OSError as e:
                results.append(
                    {"applied": False, "errors": [f"read_failed:{e}"], "record": rec}
                )
                continue
        expected_before = rec.get("expected_hash_before")
        if expected_before and target_in_sandbox.is_file():
            actual_before = sha256_file(target_in_sandbox)
            if actual_before != expected_before:
                results.append(
                    {
                        "applied": False,
                        "errors": [
                            f"expected_hash_before mismatch: {actual_before} != {expected_before}"
                        ],
                        "record": rec,
                    }
                )
                continue
        new_text = original
        replacements = 0
        if rec["patch_type"] == "replace_text":
            new_text, replacements = _apply_replace_text(
                original, rec.get("find_text", ""), rec.get("replace_text", "")
            )
        elif rec["patch_type"] == "append_text":
            new_text = original + (rec.get("append_text", "") or "")
            replacements = 1
        elif rec["patch_type"] == "unified_diff":
            r = apply_unified_diff_to_text(original, rec.get("unified_diff", ""))
            results.append(
                {
                    "applied": False,
                    "errors": [r.get("reason", "needs_external_patcher")],
                    "record": rec,
                    "note": r.get("note", ""),
                }
            )
            continue
        if new_text == original:
            results.append(
                {
                    "applied": False,
                    "errors": ["patch produced no change"],
                    "record": rec,
                }
            )
            continue
        try:
            safe_mkdir(target_in_sandbox.parent)
            target_in_sandbox.write_text(new_text, encoding="utf-8")
        except OSError as e:
            results.append(
                {"applied": False, "errors": [f"write_failed:{e}"], "record": rec}
            )
            continue
        # Final guard: real project file at this rel path must NOT have changed.
        real_target = (pdir / target_rel).resolve()
        try:
            ensure_under_project(real_target, pdir)
        except ValueError:
            pass
        results.append(
            {
                "applied": True,
                "target_file": target_rel,
                "replacements": replacements,
                "sha256_before": sha256_text(original),
                "sha256_after": sha256_text(new_text),
                "sandbox_path": str(target_in_sandbox).replace("\\", "/"),
            }
        )
    return results


# ---------- safe command runner ----------


def _command_to_str(command: Any) -> str:
    if isinstance(command, str):
        return command
    if isinstance(command, (list, tuple)):
        return " ".join(str(c) for c in command)
    return str(command)


def _is_command_allowed(command_str: str, allowlist: Iterable[str]) -> bool:
    cs = command_str.strip().lower()
    for prefix in allowlist:
        if cs.startswith(str(prefix).strip().lower()):
            return True
    return False


def _is_command_denied(command_str: str, denylist: Iterable[str]) -> tuple[bool, str]:
    for pat in denylist:
        try:
            if re.search(pat, command_str):
                return True, pat
        except re.error:
            continue
    return False, ""


def run_safe_command(
    command: Any,
    cwd: Path | str,
    timeout_seconds: int | None = None,
    allowlist: Iterable[str] | None = None,
    denylist: Iterable[str] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or load_config()
    cmd_str = _command_to_str(command)
    if not cmd_str.strip():
        return {"applied": False, "rc": -1, "stdout": "", "stderr": "empty_command"}
    al = list(allowlist or cfg.get("default_command_allowlist", []))
    dl = list(denylist or cfg.get("denied_command_patterns", []))
    denied, pat = _is_command_denied(cmd_str, dl)
    if denied:
        return {
            "applied": False,
            "rc": -1,
            "stdout": "",
            "stderr": f"denied_by_pattern:{pat}",
            "command": cmd_str,
        }
    if not _is_command_allowed(cmd_str, al):
        return {
            "applied": False,
            "rc": -1,
            "stdout": "",
            "stderr": "not_in_allowlist",
            "command": cmd_str,
        }
    timeout = int(timeout_seconds or cfg.get("default_command_timeout_seconds", 30))
    max_to = int(cfg.get("max_command_timeout_seconds", 120))
    if timeout > max_to:
        timeout = max_to
    try:
        argv = (
            shlex.split(cmd_str, posix=False)
            if not isinstance(command, (list, tuple))
            else list(command)
        )
    except ValueError as e:
        return {
            "applied": False,
            "rc": -1,
            "stdout": "",
            "stderr": f"split_failed:{e}",
            "command": cmd_str,
        }
    try:
        proc = subprocess.run(
            argv,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
        )
        return {
            "applied": True,
            "rc": int(proc.returncode),
            "stdout": (proc.stdout or "")[-4096:],
            "stderr": (proc.stderr or "")[-4096:],
            "command": cmd_str,
        }
    except subprocess.TimeoutExpired:
        return {
            "applied": False,
            "rc": -1,
            "stdout": "",
            "stderr": f"timeout_after_{timeout}s",
            "command": cmd_str,
        }
    except (OSError, FileNotFoundError) as e:
        return {
            "applied": False,
            "rc": -1,
            "stdout": "",
            "stderr": f"{type(e).__name__}:{e}",
            "command": cmd_str,
        }


def run_sandbox_verification(
    sandbox_dir: Path | str,
    commands: Iterable[Any],
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    sdir = Path(sandbox_dir).resolve()
    out: list[dict[str, Any]] = []
    for cmd in commands or []:
        out.append(run_safe_command(cmd, cwd=sdir, config=config))
    return out


# ---------- hash delta ----------


def compare_target_hashes(
    before_records: list[dict[str, Any]],
    after_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_path_before = {r.get("relative_path"): r for r in before_records}
    out: list[dict[str, Any]] = []
    for after in after_records:
        rel = after.get("relative_path")
        before = by_path_before.get(rel) or {}
        out.append(
            {
                "relative_path": rel,
                "sha256_before": before.get("sha256", ""),
                "sha256_after": after.get("sha256", ""),
                "changed": (before.get("sha256", "") != after.get("sha256", "")),
                "size_before": before.get("size_bytes", 0),
                "size_after": after.get("size_bytes", 0),
                "exists_before": bool(before.get("exists_before", False)),
                "exists_after": bool(after.get("exists_before", False)),
            }
        )
    return out


# ---------- simulation report ----------


def build_simulation_report(
    plan: dict[str, Any],
    snapshot: dict[str, Any],
    sandbox: dict[str, Any],
    verification: list[dict[str, Any]],
    hash_delta: list[dict[str, Any]],
    files_changed_in_sandbox: list[str] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    real_unchanged = all(not d.get("changed", False) for d in hash_delta)
    success = (
        all((v.get("rc") == 0) for v in (verification or []))
        and bool(verification)
        and real_unchanged
    )
    blockers: list[str] = []
    if not real_unchanged:
        blockers.append("real project files changed during simulation")
    for v in verification or []:
        if v.get("rc") not in (0, None):
            blockers.append(f"verification failed: {v.get('command')!r} rc={v.get('rc')}")
        if not v.get("applied", True) and v.get("stderr"):
            blockers.append(f"command rejected: {v.get('command')!r} ({v.get('stderr')})")
    return {
        "schema_version": SCHEMA_VERSION,
        "simulation_id": _short_uid("sim"),
        "generated_at": now_iso(),
        "plan_id": plan.get("plan_id", ""),
        "goal": plan.get("goal", ""),
        "target_files": list(plan.get("target_files") or []),
        "snapshot_id": snapshot.get("snapshot_id", ""),
        "sandbox_dir": sandbox.get("sandbox_dir", ""),
        "commands_run": [v.get("command", "") for v in (verification or [])],
        "verification_results": list(verification or []),
        "hash_delta": list(hash_delta or []),
        "files_changed_in_sandbox": list(files_changed_in_sandbox or []),
        "real_project_unchanged": real_unchanged,
        "success": bool(success),
        "blockers": blockers,
        "recommended_next_action": (
            "Promote sandbox patch via upgrade-gate review" if success else
            "Review blockers and refine plan before retry"
        ),
        "rollback_path": str(snapshot.get("snapshot_dir", "")),
        "notes": list(notes or []),
    }


def validate_simulation_report(report: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(report, dict):
        return False, ["report not a dict"]
    for k in (
        "schema_version",
        "simulation_id",
        "generated_at",
        "plan_id",
        "goal",
        "target_files",
        "snapshot_id",
        "sandbox_dir",
        "commands_run",
        "verification_results",
        "hash_delta",
        "files_changed_in_sandbox",
        "real_project_unchanged",
        "success",
        "blockers",
        "recommended_next_action",
        "rollback_path",
    ):
        if k not in report:
            errors.append(f"report.{k} missing")
    if not isinstance(report.get("real_project_unchanged"), bool):
        errors.append("report.real_project_unchanged must be bool")
    if not isinstance(report.get("success"), bool):
        errors.append("report.success must be bool")
    return (not errors), errors


def render_simulation_report_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Simulation Report")
    lines.append("")
    lines.append(f"- **simulation_id**: `{report.get('simulation_id', '?')}`")
    lines.append(f"- **plan_id**: `{report.get('plan_id', '?')}`")
    lines.append(f"- **generated_at**: {report.get('generated_at', '?')}")
    lines.append(f"- **goal**: {report.get('goal', '')!r}")
    lines.append(f"- **success**: `{report.get('success')}`")
    lines.append(
        f"- **real_project_unchanged**: `{report.get('real_project_unchanged')}`"
    )
    lines.append(f"- **rollback_path**: `{report.get('rollback_path', '')}`")
    lines.append("")
    lines.append("## Target files")
    for tf in report.get("target_files") or []:
        lines.append(f"- `{tf}`")
    lines.append("")
    lines.append("## Hash delta")
    lines.append("| File | Before | After | Changed |")
    lines.append("|------|--------|-------|---------|")
    for d in report.get("hash_delta") or []:
        lines.append(
            f"| {d.get('relative_path')} | "
            f"`{(d.get('sha256_before') or '')[:10]}` | "
            f"`{(d.get('sha256_after') or '')[:10]}` | "
            f"{d.get('changed')} |"
        )
    lines.append("")
    lines.append("## Commands run")
    for c in report.get("commands_run") or []:
        lines.append(f"- `{c}`")
    lines.append("")
    lines.append("## Verification results")
    for v in report.get("verification_results") or []:
        lines.append(
            f"- `{v.get('command')}` -> rc={v.get('rc')} applied={v.get('applied')}"
        )
    blockers = report.get("blockers") or []
    if blockers:
        lines.append("")
        lines.append("## Blockers")
        for b in blockers:
            lines.append(f"- {b}")
    if report.get("recommended_next_action"):
        lines.append("")
        lines.append(f"## Recommended next action")
        lines.append(f"- {report['recommended_next_action']}")
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, data: str | bytes) -> None:
    safe_mkdir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding="utf-8")
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def write_simulation_report(
    report: dict[str, Any],
    json_path: Path | str,
    markdown_path: Path | str | None = None,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    json_p = Path(json_path)
    root = (
        Path(project_root).resolve() if project_root else _PROJECT_DIR_DEFAULT.resolve()
    )
    try:
        json_p.resolve().relative_to(root)
    except ValueError:
        raise ValueError(f"json_path must be inside project root: {json_p}")
    _atomic_write(json_p, json.dumps(report, indent=2, sort_keys=False))
    written: dict[str, Any] = {"json": str(json_p)}
    if markdown_path:
        md_p = Path(markdown_path)
        try:
            md_p.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"markdown_path must be inside project root: {md_p}")
        _atomic_write(md_p, render_simulation_report_markdown(report))
        written["markdown"] = str(md_p)
    return written


# ---------- self-test ----------


def _self_test_inner(td: Path) -> dict[str, Any]:
    target_rel = "luna_modules/sample_target.py"
    src = td / target_rel
    safe_mkdir(src.parent)
    src.write_text("def hello():\n    return 'old'\n", encoding="utf-8")
    real_hash_before = sha256_file(src)
    snapshot = create_filesystem_snapshot(
        td, [target_rel], reason="self-test"
    )
    sandbox = create_sandbox_workspace(td)
    copy_targets_to_sandbox(td, [target_rel], sandbox["sandbox_dir"])
    plan = build_simulation_plan(
        goal="rename hello() return value",
        target_files=[target_rel],
        verification_commands=[
            f"python -m py_compile {target_rel}",
        ],
        patch_records=[
            {
                "target_file": target_rel,
                "patch_type": "replace_text",
                "find_text": "old",
                "replace_text": "new",
            }
        ],
    )
    plan_ok, plan_errs = validate_simulation_plan(plan)
    if not plan_ok:
        return {"ok": False, "stage": "plan", "errors": plan_errs}
    apply_results = apply_patch_to_sandbox(
        sandbox["sandbox_dir"], td, plan["patch_records"]
    )
    if not all(r.get("applied") for r in apply_results):
        return {
            "ok": False,
            "stage": "apply_patch",
            "errors": [r.get("errors") for r in apply_results if not r.get("applied")],
        }
    real_hash_after = sha256_file(src)
    if real_hash_before != real_hash_after:
        return {
            "ok": False,
            "stage": "real_changed",
            "before": real_hash_before,
            "after": real_hash_after,
        }
    after_meta = collect_target_metadata(td, [target_rel])
    delta = compare_target_hashes(snapshot["targets"], after_meta)
    verification = [
        run_safe_command(
            f"python -m py_compile {target_rel}",
            cwd=Path(sandbox["sandbox_dir"]),
        )
    ]
    report = build_simulation_report(
        plan, snapshot, sandbox, verification, delta,
        files_changed_in_sandbox=[r.get("target_file") for r in apply_results if r.get("applied")],
    )
    rep_ok, rep_errs = validate_simulation_report(report)
    if not rep_ok:
        return {"ok": False, "stage": "report", "errors": rep_errs}
    write_simulation_report(
        report,
        td / "memory" / "luna_sandbox_report.json",
        td / "memory" / "luna_sandbox_report.md",
        project_root=td,
    )
    denied = run_safe_command("pip install evil", cwd=td)
    if denied.get("applied", True):
        return {"ok": False, "stage": "deny_check", "result": denied}
    restore_dry = restore_snapshot(snapshot, td, dry_run=True)
    if restore_dry.get("applied"):
        return {"ok": False, "stage": "restore_dry", "result": restore_dry}
    return {
        "ok": True,
        "snapshot_id": snapshot["snapshot_id"],
        "sandbox_id": sandbox["sandbox_id"],
        "real_project_unchanged": report["real_project_unchanged"],
        "patch_applied": True,
        "verification_rc": verification[0].get("rc"),
        "denied_pip": denied.get("stderr", ""),
    }


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        safe_mkdir(td / "memory")
        result = _self_test_inner(td)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Sandbox + Snapshot foundation (Phase 5I)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--snapshot",
        action="append",
        default=None,
        help="Project-relative file to snapshot. Repeatable.",
    )
    parser.add_argument("--reason", default="cli snapshot")
    parser.add_argument(
        "--simulate-replace",
        action="store_true",
        help="Run a sample replace_text simulation in a TemporaryDirectory.",
    )
    parser.add_argument("--write-report", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    pdir = Path(args.project_dir)
    cfg = load_config()

    if args.snapshot:
        manifest = create_filesystem_snapshot(
            pdir, args.snapshot, reason=args.reason, config=cfg
        )
        manifest_path = pdir / cfg.get(
            "manifest_path_relative", "memory/luna_snapshot_manifest.jsonl"
        )
        append_manifest_row(manifest, manifest_path, pdir)
        print(
            json.dumps(
                {
                    "snapshot_id": manifest["snapshot_id"],
                    "snapshot_dir": manifest["snapshot_dir"],
                    "targets": [t["relative_path"] for t in manifest["targets"]],
                    "manifest_appended_to": str(manifest_path),
                },
                indent=2,
            )
        )
        return 0

    if args.simulate_replace:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            safe_mkdir(td / "memory")
            r = _self_test_inner(td)
            print(json.dumps(r, indent=2))
            return 0 if r.get("ok") else 1

    if args.write_report:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            safe_mkdir(td / "memory")
            r = _self_test_inner(td)
            real_target_dir = pdir / "memory"
            safe_mkdir(real_target_dir)
            json_p = real_target_dir / cfg.get(
                "report_path_relative_json", "memory/luna_sandbox_report.json"
            ).split("/")[-1]
            md_p = real_target_dir / cfg.get(
                "report_path_relative_md", "memory/luna_sandbox_report.md"
            ).split("/")[-1]
            sample_report = {
                "schema_version": SCHEMA_VERSION,
                "simulation_id": _short_uid("sim"),
                "generated_at": now_iso(),
                "plan_id": "smoke",
                "goal": "phase5i smoke",
                "target_files": ["luna_modules/sample_target.py"],
                "snapshot_id": r.get("snapshot_id", ""),
                "sandbox_dir": "<temp>",
                "commands_run": ["python -m py_compile <target>"],
                "verification_results": [],
                "hash_delta": [],
                "files_changed_in_sandbox": [],
                "real_project_unchanged": bool(r.get("real_project_unchanged", True)),
                "success": bool(r.get("ok", False)),
                "blockers": [] if r.get("ok") else ["self-test failed"],
                "recommended_next_action": "review and proceed",
                "rollback_path": "",
                "notes": ["Phase 5I smoke run via CLI"],
            }
            write_simulation_report(sample_report, json_p, md_p, project_root=pdir)
            print(
                json.dumps(
                    {
                        "wrote_json": str(json_p),
                        "wrote_md": str(md_p),
                        "ok": bool(r.get("ok")),
                    },
                    indent=2,
                )
            )
            return 0 if r.get("ok") else 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
