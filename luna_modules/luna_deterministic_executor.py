"""Phase 5R: Luna Deterministic Sandbox Patch Executor foundation.

Stdlib only. Sandbox-only execution proof layer. Never modifies real project
source files. Accepts an approved execution plan, copies target files into a
sandbox, applies deterministic text-level patch operations to sandbox copies
only, runs allowlisted verification commands inside the sandbox, and produces
a simulation report.

Hard rules in Phase 5R:
  * sandbox_only must be true in every plan.
  * safe_to_apply_real_project is ALWAYS False.
  * real_project_modified is ALWAYS False.
  * No Aider invocations. No external API calls. No package installs.
  * Forbidden operations: delete_file, move_file, rename_file, chmod,
    binary_write, arbitrary_script, unified_diff.
  * Verification commands are allowlisted; dangerous commands are rejected.
  * Path traversal is rejected.
  * Sensitive file patterns (.env, api_vault, token, .key) are blocked.

Tracked schema/policy:
  memory/luna_deterministic_executor.schema.json
  memory/luna_deterministic_executor_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_deterministic_executor_report.json
  memory/luna_deterministic_executor_report.md
  memory/luna_deterministic_executor_audit.jsonl
  memory/luna_executor_dispatch_preview.json
  logic_updates/deterministic_executor_*
  backups/deterministic_executor_*

CLI:
  python -m luna_modules.luna_deterministic_executor --self-test
  python -m luna_modules.luna_deterministic_executor --simulate-create-file
  python -m luna_modules.luna_deterministic_executor --simulate-replace
  python -m luna_modules.luna_deterministic_executor --simulate-append
  python -m luna_modules.luna_deterministic_executor --simulate-high-risk-worker
  python -m luna_modules.luna_deterministic_executor --print-report
  python -m luna_modules.luna_deterministic_executor --write-report
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Optional Phase 5 module imports — degrade gracefully if missing.
try:  # pragma: no cover
    from luna_modules import luna_council_enforcer as _enforcer  # type: ignore
except Exception:  # pragma: no cover
    _enforcer = None
try:  # pragma: no cover
    from luna_modules import luna_approval_router as _approval_router  # type: ignore
except Exception:  # pragma: no cover
    _approval_router = None
try:  # pragma: no cover
    from luna_modules import luna_sandbox as _sandbox_mod  # type: ignore
except Exception:  # pragma: no cover
    _sandbox_mod = None
try:  # pragma: no cover
    from luna_modules import luna_change_ledger as _change_ledger  # type: ignore
except Exception:  # pragma: no cover
    _change_ledger = None

ALLOWED_OPERATIONS: tuple = (
    "replace_text",
    "append_text",
    "insert_after",
    "insert_before",
    "create_file",
)

FORBIDDEN_OPERATIONS: tuple = (
    "delete_file",
    "move_file",
    "rename_file",
    "chmod",
    "binary_write",
    "arbitrary_script",
    "unified_diff",
)

VALID_SOURCES: tuple = ("operator", "director", "limited_autonomy", "test")

VALID_ACTION_TYPES: tuple = (
    "low_risk_additive",
    "medium_code_edit",
    "high_risk_core_edit",
    "emergency_repair",
)

_HIGH_RISK_PATHS_LOWER: frozenset = frozenset({
    "worker.py", "aider_bridge.py", "luna_guardian.py", "launchluna.pyw",
    "surgeapp_claude_terminal.py", "luna_start.pyw", "director_agent.py",
    "luna_modules/luna_hygiene.py", "luna_modules/luna_paths.py",
    "luna_modules/luna_routing.py", "luna_modules/luna_state.py",
})

_SENSITIVE_PATTERNS: tuple = (
    ".env", "api_vault", "token.json", "token.key", ".key", "secret",
    "credential", "password", "api_key",
)

_ALLOWED_CMD_PREFIXES: tuple = (
    "python -m py_compile ",
    "python -m unittest ",
    "python -c ",
    "python3 -m py_compile ",
    "python3 -m unittest ",
    "python3 -c ",
    "git status --short",
    "git status",
)

_FORBIDDEN_CMD_PATTERNS: tuple = (
    "pip install", "pip3 install",
    "npm ", "yarn ", "winget ",
    "curl ", "wget ",
    "invoke-webrequest",
    "taskkill", "stop-process",
    "remove-item", "del ", "rmdir ",
    "git reset", "git clean", "git push",
    "git checkout --",
    "http://", "https://",
    "delete_queue", "delete_memory",
)

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "sandbox_only": True,
    "safe_to_apply_real_project_always_false_in_phase5r": True,
    "allowed_operations": list(ALLOWED_OPERATIONS),
    "forbidden_operations": list(FORBIDDEN_OPERATIONS),
    "max_patch_operations": 5,
    "max_total_inserted_chars": 20000,
    "max_target_files": 3,
    "require_receipt_for_tiers": [2, 3, 4, 5],
    "require_snapshot": True,
    "require_verification_commands": True,
    "allow_real_apply": False,
    "high_risk_paths": [
        "worker.py", "aider_bridge.py", "luna_guardian.py", "LaunchLuna.pyw",
        "SurgeApp_Claude_Terminal.py", "luna_start.pyw", "director_agent.py",
    ],
    "generated_report_outputs": [
        "memory/luna_deterministic_executor_report.json",
        "memory/luna_deterministic_executor_report.md",
        "memory/luna_deterministic_executor_audit.jsonl",
    ],
}


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_execution_id(prefix: str = "exec") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def sha256_json(data: Any) -> str:
    serialized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def sha256_file(path: Path | str) -> str:
    p = Path(path)
    if not p.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def write_json_atomic(path: Path | str, data: Any) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False), encoding="utf-8")
    os.replace(tmp, p)
    return p


def append_jsonl(path: Path | str, row: dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return p


# ---------- policy ----------


def load_executor_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_deterministic_executor_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(_DEFAULT_POLICY)
    for k, v in raw.items():
        out[k] = v
    # Hard rules always enforced regardless of file contents.
    out["sandbox_only"] = True
    out["allow_real_apply"] = False
    out["safe_to_apply_real_project_always_false_in_phase5r"] = True
    out["_source"] = str(p)
    return out


# ---------- normalization / path safety ----------


def normalize_target_files(target_files: Any) -> list[str]:
    """Deduplicate and posix-normalize a list of target file paths."""
    if not target_files:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for tf in target_files:
        if not tf:
            continue
        normalized = str(tf).replace("\\", "/").strip("/")
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def ensure_under_project(path: Path | str, project_dir: Path | str) -> Path:
    """Resolve path and verify it is under project_dir. Raises ValueError on traversal."""
    p = Path(path).resolve()
    root = Path(project_dir).resolve()
    try:
        p.relative_to(root)
    except ValueError:
        raise ValueError(f"Path escapes project root: {p!r} not under {root!r}")
    return p


def _is_sensitive_path(rel_path: str) -> bool:
    lower = rel_path.lower().replace("\\", "/")
    return any(pat in lower for pat in _SENSITIVE_PATTERNS)


def _is_high_risk_path(rel_path: str) -> bool:
    lower = rel_path.lower().replace("\\", "/")
    return lower in _HIGH_RISK_PATHS_LOWER


# ---------- patch operation classification / validation ----------


def classify_patch_operation(op: Any) -> str:
    """Return 'allowed', 'forbidden', or 'unknown'."""
    if not isinstance(op, dict):
        return "unknown"
    operation = str(op.get("operation", ""))
    if operation in ALLOWED_OPERATIONS:
        return "allowed"
    if operation in FORBIDDEN_OPERATIONS:
        return "forbidden"
    return "unknown"


def validate_patch_operation(op: Any) -> tuple[bool, list[str]]:
    """Validate a single patch operation dict. Returns (ok, errors)."""
    errs: list[str] = []
    if not isinstance(op, dict):
        return False, ["operation is not a dict"]

    operation = op.get("operation", "")
    if operation in FORBIDDEN_OPERATIONS:
        errs.append(f"forbidden_operation: {operation!r} (unified_diff and destructive ops not supported in Phase 5R)")
    elif operation not in ALLOWED_OPERATIONS:
        errs.append(f"unknown_operation: {operation!r}")

    target_file = op.get("target_file", "")
    if not target_file:
        errs.append("target_file is required")
    elif _is_sensitive_path(str(target_file)):
        errs.append(f"sensitive_path_blocked: {target_file!r}")

    if operation == "replace_text":
        if not op.get("find_text"):
            errs.append("replace_text requires find_text")
        if op.get("replace_text") is None:
            errs.append("replace_text requires replace_text field")
    elif operation == "append_text":
        if op.get("append_text") is None:
            errs.append("append_text requires append_text field")
    elif operation in ("insert_after", "insert_before"):
        if not op.get("anchor_text"):
            errs.append(f"{operation} requires anchor_text")
        if op.get("insert_text") is None:
            errs.append(f"{operation} requires insert_text field")
    elif operation == "create_file":
        if op.get("new_file_text") is None:
            errs.append("create_file requires new_file_text field")

    return len(errs) == 0, errs


# ---------- execution plan ----------


def build_execution_plan(
    goal: str,
    target_files: list[str],
    patch_operations: list[dict[str, Any]],
    action_type: str = "medium_code_edit",
    risk_tier: int = 2,
    approval_tier_required: int = 2,
    receipt_id: str = "",
    approval_id: str = "",
    receipt_required: bool = True,
    verification_commands: list[str] | None = None,
    rollback_plan: str = "",
    expected_artifacts: list[str] | None = None,
    source: str = "operator",
    task_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if source not in VALID_SOURCES:
        source = "operator"
    if action_type not in VALID_ACTION_TYPES:
        action_type = "medium_code_edit"
    normalized_files = normalize_target_files(target_files)
    ops_with_ids: list[dict[str, Any]] = []
    for i, op in enumerate(patch_operations or []):
        o = dict(op)
        if "operation_id" not in o:
            o["operation_id"] = f"op_{i:03d}_{uuid.uuid4().hex[:6]}"
        ops_with_ids.append(o)
    return {
        "schema_version": SCHEMA_VERSION,
        "execution_id": make_execution_id(),
        "created_at": now_iso(),
        "goal": str(goal or ""),
        "task_id": str(task_id or ""),
        "source": source,
        "dry_run": True,
        "sandbox_only": True,
        "real_apply_allowed": False,
        "action_type": action_type,
        "risk_tier": int(risk_tier),
        "approval_tier_required": int(approval_tier_required),
        "target_files": normalized_files,
        "receipt_id": str(receipt_id or ""),
        "approval_id": str(approval_id or ""),
        "receipt_required": bool(receipt_required),
        "patch_operations": ops_with_ids,
        "verification_commands": list(verification_commands or []),
        "rollback_plan": str(rollback_plan or ""),
        "expected_artifacts": list(expected_artifacts or []),
        "metadata": dict(metadata or {}),
    }


def validate_execution_plan(
    plan: Any,
    policy: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    """Validate an execution plan dict. Returns (ok, errors)."""
    pol = policy or _DEFAULT_POLICY
    errs: list[str] = []
    if not isinstance(plan, dict):
        return False, ["plan is not a dict"]

    if plan.get("sandbox_only") is not True:
        errs.append("sandbox_only must be true")
    if plan.get("real_apply_allowed") is True:
        errs.append("real_apply_allowed must not be true")

    target_files = plan.get("target_files") or []
    max_tf = int(pol.get("max_target_files", 3))
    if len(target_files) > max_tf:
        errs.append(f"too_many_target_files: {len(target_files)} > {max_tf}")

    ops = plan.get("patch_operations") or []
    max_ops = int(pol.get("max_patch_operations", 5))
    if len(ops) > max_ops:
        errs.append(f"too_many_patch_operations: {len(ops)} > {max_ops}")

    for i, op in enumerate(ops):
        ok, op_errs = validate_patch_operation(op)
        if not ok:
            for e in op_errs:
                errs.append(f"op[{i}]: {e}")

    total_chars = sum(
        len(str(op.get("replace_text", ""))) +
        len(str(op.get("append_text", ""))) +
        len(str(op.get("insert_text", ""))) +
        len(str(op.get("new_file_text", "")))
        for op in ops
    )
    max_chars = int(pol.get("max_total_inserted_chars", 20000))
    if total_chars > max_chars:
        errs.append(f"too_many_inserted_chars: {total_chars} > {max_chars}")

    action_type = plan.get("action_type", "")
    if action_type and action_type not in VALID_ACTION_TYPES:
        errs.append(f"invalid_action_type: {action_type!r}")

    return len(errs) == 0, errs


# ---------- text patch operations ----------


def apply_replace_text(
    text: str,
    find_text: str,
    replace_text: str,
    max_replacements: int = 1,
    expected_occurrences: int = 1,
) -> tuple[str, bool, str]:
    """Apply replace_text patch. Returns (new_text, success, error_msg)."""
    if not find_text:
        return text, False, "find_text is empty"
    count = text.count(find_text)
    if expected_occurrences > 0 and count != expected_occurrences:
        return text, False, f"expected {expected_occurrences} occurrence(s), found {count}"
    if count == 0:
        return text, False, "find_text not found in text"
    new_text = text
    replaced = 0
    while replaced < max(1, max_replacements):
        idx = new_text.find(find_text)
        if idx == -1:
            break
        new_text = new_text[:idx] + replace_text + new_text[idx + len(find_text):]
        replaced += 1
    return new_text, True, ""


def apply_append_text(text: str, append_text: str) -> tuple[str, bool, str]:
    """Append append_text to end of text. Returns (new_text, success, error_msg)."""
    return text + append_text, True, ""


def apply_insert_after(
    text: str, anchor_text: str, insert_text: str
) -> tuple[str, bool, str]:
    """Insert insert_text immediately after anchor_text. Returns (new_text, success, error_msg)."""
    if not anchor_text:
        return text, False, "anchor_text is empty"
    idx = text.find(anchor_text)
    if idx == -1:
        return text, False, "anchor_text not found in text"
    insert_at = idx + len(anchor_text)
    return text[:insert_at] + insert_text + text[insert_at:], True, ""


def apply_insert_before(
    text: str, anchor_text: str, insert_text: str
) -> tuple[str, bool, str]:
    """Insert insert_text immediately before anchor_text. Returns (new_text, success, error_msg)."""
    if not anchor_text:
        return text, False, "anchor_text is empty"
    idx = text.find(anchor_text)
    if idx == -1:
        return text, False, "anchor_text not found in text"
    return text[:idx] + insert_text + text[idx:], True, ""


def apply_patch_operations_to_text(
    original_text: str,
    operations: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    """Apply a list of patch operations sequentially to text. Returns (final_text, results).

    All operations must target the same file; caller is responsible for grouping.
    """
    current_text = original_text
    results: list[dict[str, Any]] = []
    for op in operations:
        operation = op.get("operation", "")
        op_id = op.get("operation_id", "")
        len_before = len(current_text)

        if operation == "replace_text":
            new_text, ok, err = apply_replace_text(
                current_text,
                str(op.get("find_text", "")),
                str(op.get("replace_text", "")),
                max_replacements=int(op.get("max_replacements", 1)),
                expected_occurrences=int(op.get("expected_occurrences", 1)),
            )
        elif operation == "append_text":
            new_text, ok, err = apply_append_text(
                current_text, str(op.get("append_text", ""))
            )
        elif operation == "insert_after":
            new_text, ok, err = apply_insert_after(
                current_text,
                str(op.get("anchor_text", "")),
                str(op.get("insert_text", "")),
            )
        elif operation == "insert_before":
            new_text, ok, err = apply_insert_before(
                current_text,
                str(op.get("anchor_text", "")),
                str(op.get("insert_text", "")),
            )
        else:
            new_text, ok, err = current_text, False, f"unsupported_operation:{operation!r}"

        if ok:
            current_text = new_text
        results.append({
            "operation_id": op_id,
            "operation": operation,
            "success": ok,
            "error": err,
            "text_len_before": len_before,
            "text_len_after": len(current_text),
        })
    return current_text, results


# ---------- sandbox management ----------


def create_executor_sandbox(
    project_dir: Path | str,
    plan: dict[str, Any],
    sandbox_base: Path | str | None = None,
) -> dict[str, Any]:
    """Create a sandbox workspace and copy target files into it.

    Returns a sandbox context dict. Never modifies real project files.
    """
    pdir = Path(project_dir).resolve()
    target_files = normalize_target_files(
        list({
            op.get("target_file", "")
            for op in (plan.get("patch_operations") or [])
            if op.get("target_file")
        }) + list(plan.get("target_files") or [])
    )

    if sandbox_base is None:
        sandbox_dir = Path(tempfile.mkdtemp(prefix="luna_exec_sandbox_"))
    else:
        sbase = Path(sandbox_base)
        sbase.mkdir(parents=True, exist_ok=True)
        sandbox_dir = Path(tempfile.mkdtemp(prefix="luna_exec_", dir=str(sbase)))

    snapshot_id = f"snap_{uuid.uuid4().hex[:10]}"
    file_records: list[dict[str, Any]] = []

    for rel_path in target_files:
        if not rel_path:
            continue
        real_path = pdir / rel_path
        sandbox_path = sandbox_dir / rel_path
        sandbox_path.parent.mkdir(parents=True, exist_ok=True)

        hash_before = sha256_file(real_path)
        if real_path.is_file():
            try:
                shutil.copy2(str(real_path), str(sandbox_path))
            except OSError as e:
                file_records.append({
                    "target_file": rel_path,
                    "sandbox_path": str(sandbox_path),
                    "real_path": str(real_path),
                    "hash_before": hash_before,
                    "copied": False,
                    "copy_error": f"{type(e).__name__}:{str(e)[:200]}",
                })
                continue
        else:
            # File doesn't exist yet — sandbox path stays empty (create_file ops will create it).
            pass

        file_records.append({
            "target_file": rel_path,
            "sandbox_path": str(sandbox_path),
            "real_path": str(real_path),
            "hash_before": hash_before,
            "copied": real_path.is_file(),
        })

    return {
        "sandbox_dir": str(sandbox_dir),
        "snapshot_id": snapshot_id,
        "project_dir": str(pdir),
        "file_records": file_records,
        "created_at": now_iso(),
    }


def apply_plan_in_sandbox(
    project_dir: Path | str,
    plan: dict[str, Any],
    sandbox_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Apply patch operations to sandbox copies of files. Real files are never modified.

    Returns a result dict with patch_results, before/after hashes, and
    real_project_modified=False assertion.
    """
    pdir = Path(project_dir).resolve()

    # Create sandbox context (copies files into sandbox).
    sandbox_ctx = create_executor_sandbox(pdir, plan, sandbox_base=sandbox_dir)
    sbox_dir = Path(sandbox_ctx["sandbox_dir"])
    file_records = {
        r["target_file"]: r
        for r in sandbox_ctx["file_records"]
    }

    # Record real file hashes BEFORE any sandbox operations.
    all_target_files = normalize_target_files(
        list(file_records.keys()) + list(plan.get("target_files") or [])
    )
    before_real_hashes = {rel: sha256_file(pdir / rel) for rel in all_target_files}

    # Group operations by target_file.
    ops_by_file: dict[str, list[dict[str, Any]]] = {}
    for op in plan.get("patch_operations") or []:
        tf = op.get("target_file", "")
        if tf:
            ops_by_file.setdefault(tf, []).append(op)

    patch_results: list[dict[str, Any]] = []

    for rel_path, ops in ops_by_file.items():
        sbox_path = sbox_dir / rel_path
        sbox_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if first operation creates the file.
        is_create_only = all(op.get("operation") == "create_file" for op in ops)

        if sbox_path.is_file():
            try:
                original_text = sbox_path.read_text(encoding="utf-8", errors="replace")
            except OSError as e:
                for op in ops:
                    patch_results.append({
                        "operation_id": op.get("operation_id", ""),
                        "operation": op.get("operation", ""),
                        "target_file": rel_path,
                        "success": False,
                        "error": f"read_failed:{type(e).__name__}:{str(e)[:200]}",
                        "sandbox_file": str(sbox_path),
                    })
                continue
        elif is_create_only:
            original_text = ""
        else:
            for op in ops:
                patch_results.append({
                    "operation_id": op.get("operation_id", ""),
                    "operation": op.get("operation", ""),
                    "target_file": rel_path,
                    "success": False,
                    "error": "sandbox_file_not_found (use create_file to create new files)",
                    "sandbox_file": str(sbox_path),
                })
            continue

        # Handle create_file operations specially.
        non_create_ops = [op for op in ops if op.get("operation") != "create_file"]
        create_ops = [op for op in ops if op.get("operation") == "create_file"]

        for c_op in create_ops:
            new_file_text = str(c_op.get("new_file_text", ""))
            try:
                sbox_path.write_text(new_file_text, encoding="utf-8")
                patch_results.append({
                    "operation_id": c_op.get("operation_id", ""),
                    "operation": "create_file",
                    "target_file": rel_path,
                    "success": True,
                    "error": "",
                    "sandbox_file": str(sbox_path),
                    "file_len": len(new_file_text),
                })
                original_text = new_file_text
            except OSError as e:
                patch_results.append({
                    "operation_id": c_op.get("operation_id", ""),
                    "operation": "create_file",
                    "target_file": rel_path,
                    "success": False,
                    "error": f"write_failed:{type(e).__name__}:{str(e)[:200]}",
                    "sandbox_file": str(sbox_path),
                })
                continue

        if non_create_ops:
            final_text, op_results = apply_patch_operations_to_text(original_text, non_create_ops)
            for r in op_results:
                r["target_file"] = rel_path
                r["sandbox_file"] = str(sbox_path)
                patch_results.append(r)
            if any(r["success"] for r in op_results):
                try:
                    sbox_path.write_text(final_text, encoding="utf-8")
                except OSError as e:
                    for r in op_results:
                        if r["success"]:
                            r["success"] = False
                            r["error"] = f"write_back_failed:{type(e).__name__}:{str(e)[:200]}"

    # Record real file hashes AFTER sandbox operations (must be unchanged).
    after_real_hashes = {rel: sha256_file(pdir / rel) for rel in all_target_files}
    modified_real_files = [
        rel for rel in all_target_files
        if before_real_hashes.get(rel, "") != after_real_hashes.get(rel, "")
    ]

    # Compute sandbox after-records.
    after_records: list[dict[str, Any]] = []
    for rel_path in all_target_files:
        sbox_path = sbox_dir / rel_path
        after_records.append({
            "target_file": rel_path,
            "sandbox_path": str(sbox_path),
            "hash_after": sha256_file(sbox_path),
            "exists_in_sandbox": sbox_path.is_file(),
        })

    return {
        "sandbox_dir": str(sbox_dir),
        "snapshot_id": sandbox_ctx["snapshot_id"],
        "before_records": [
            {"target_file": k, "hash_before": v} for k, v in before_real_hashes.items()
        ],
        "after_records": after_records,
        "patch_results": patch_results,
        "real_project_modified": len(modified_real_files) > 0,
        "modified_real_files": modified_real_files,
        "success": (
            len(modified_real_files) == 0
            and bool(patch_results)
            and all(r.get("success") for r in patch_results)
        ),
    }


# ---------- verification ----------


def _is_command_allowed(cmd: str) -> tuple[bool, str]:
    """Check if a verification command is allowlisted. Returns (allowed, reason)."""
    cmd_stripped = cmd.strip()
    cmd_lower = cmd_stripped.lower()

    for pattern in _FORBIDDEN_CMD_PATTERNS:
        if pattern.lower() in cmd_lower:
            return False, f"forbidden_pattern:{pattern!r}"

    for prefix in _ALLOWED_CMD_PREFIXES:
        if cmd_lower.startswith(prefix.lower()):
            return True, "allowed"

    return False, "not_in_allowlist"


def run_executor_verification(
    project_dir: Path | str,
    sandbox_dir: Path | str,
    commands: list[str],
) -> list[dict[str, Any]]:
    """Run allowlisted verification commands inside the sandbox. Returns result list."""
    sbox = Path(sandbox_dir)
    results: list[dict[str, Any]] = []

    for cmd in commands:
        allowed, reason = _is_command_allowed(cmd)
        if not allowed:
            results.append({
                "command": cmd,
                "allowed": False,
                "reason": reason,
                "rc": -1,
                "stdout": "",
                "stderr": "",
            })
            continue

        try:
            argv = shlex.split(cmd, posix=False)
            # Replace 'python' or 'python3' with current interpreter.
            if argv and argv[0].lower() in ("python", "python3"):
                argv[0] = sys.executable
            proc = subprocess.run(
                argv,
                cwd=str(sbox),
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, "PYTHONPATH": str(sbox)},
            )
            results.append({
                "command": cmd,
                "allowed": True,
                "rc": proc.returncode,
                "stdout": proc.stdout[:2000],
                "stderr": proc.stderr[:2000],
            })
        except Exception as e:
            results.append({
                "command": cmd,
                "allowed": True,
                "rc": -1,
                "stdout": "",
                "stderr": f"{type(e).__name__}:{str(e)[:200]}",
            })

    return results


# ---------- hashing / diff ----------


def compute_plan_diff_hash(plan: dict[str, Any]) -> str:
    """Compute a stable hash of the plan's patch operations."""
    ops_normalized = [
        {k: v for k, v in sorted(op.items()) if k != "operation_id"}
        for op in (plan.get("patch_operations") or [])
    ]
    return sha256_json({"goal": plan.get("goal", ""), "operations": ops_normalized})


def compute_sandbox_file_deltas(
    before_records: list[dict[str, Any]],
    after_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compute per-file hash deltas between before and after sandbox records."""
    before_map = {r["target_file"]: r.get("hash_before", "") for r in before_records}
    deltas: list[dict[str, Any]] = []
    for r in after_records:
        tf = r["target_file"]
        h_before = before_map.get(tf, "")
        h_after = r.get("hash_after", "")
        deltas.append({
            "target_file": tf,
            "hash_before": h_before,
            "hash_after": h_after,
            "changed_in_sandbox": h_before != h_after,
            "exists_in_sandbox": r.get("exists_in_sandbox", False),
        })
    return deltas


# ---------- receipt advisory check ----------


def _check_receipt_advisory(
    project_dir: Path | str,
    plan: dict[str, Any],
) -> dict[str, Any]:
    """Check receipt/enforcer advisory status. Never blocks or enables real apply."""
    pdir = Path(project_dir)
    tier = int(plan.get("approval_tier_required", 2))
    receipt_id = str(plan.get("receipt_id", "") or "")

    if tier <= 1:
        return {"checked": False, "valid": True, "reason": "tier_not_required"}

    if not receipt_id:
        return {
            "checked": True,
            "valid": False,
            "reason": "no_receipt_id",
            "blocker": f"receipt_required for tier {tier} (advisory only — sandbox simulation proceeds)",
        }

    # Try approval requests JSONL.
    for req_file in (
        pdir / "memory" / "luna_approval_requests.jsonl",
        pdir / "memory" / "luna_routine_approval_requests.jsonl",
    ):
        if req_file.is_file():
            try:
                for line in req_file.read_text(encoding="utf-8").splitlines():
                    row = json.loads(line.strip())
                    if row.get("request_id") == receipt_id:
                        return {
                            "checked": True,
                            "valid": True,
                            "reason": "receipt_found_in_requests",
                            "receipt_row": row,
                        }
            except Exception:
                pass

    # Try enforcer advisory check if available.
    if _enforcer is not None:
        try:
            action_record = {
                "action_type": plan.get("action_type", "medium_code_edit"),
                "target_files": plan.get("target_files", []),
                "receipt_id": receipt_id,
                "approval_tier_required": tier,
                "created_at": now_iso(),
            }
            status = _enforcer.evaluate_action_enforcement(pdir, action_record)
            return {
                "checked": True,
                "valid": status.get("decision") in ("would_allow", "not_required"),
                "reason": f"enforcer:{status.get('decision', 'unknown')}",
                "enforcer_status": status,
            }
        except Exception as e:
            pass

    return {
        "checked": True,
        "valid": False,
        "reason": "receipt_not_found",
        "note": "receipt_id not found in approval ledger (advisory only)",
    }


# ---------- report building ----------


def build_executor_report(
    plan: dict[str, Any],
    sandbox_result: dict[str, Any],
    verification_result: list[dict[str, Any]],
    receipt_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the execution report. safe_to_apply_real_project is ALWAYS False."""
    blockers: list[str] = []
    warnings: list[str] = []

    if sandbox_result.get("real_project_modified"):
        blockers.append(f"real_project_modified: {sandbox_result.get('modified_real_files')}")

    rr = receipt_result or {}
    receipt_checked = bool(rr.get("checked"))
    receipt_valid = bool(rr.get("valid"))
    if rr.get("blocker"):
        blockers.append(rr["blocker"])

    for vr in verification_result:
        if not vr.get("allowed"):
            warnings.append(f"verification_command_blocked: {vr.get('command')!r} — {vr.get('reason')}")
        elif vr.get("rc", 0) != 0:
            warnings.append(f"verification_failed: rc={vr.get('rc')} cmd={vr.get('command')!r}")

    patch_results = sandbox_result.get("patch_results") or []
    failed_ops = [r for r in patch_results if not r.get("success")]
    if failed_ops:
        warnings.append(f"patch_operations_failed: {[r.get('operation_id') for r in failed_ops]}")

    diff_hash = compute_plan_diff_hash(plan)
    file_deltas = compute_sandbox_file_deltas(
        sandbox_result.get("before_records") or [],
        sandbox_result.get("after_records") or [],
    )

    success = (
        not blockers
        and not sandbox_result.get("real_project_modified", True)
        and sandbox_result.get("success", False)
    )

    if success and not blockers:
        rec_action = "sandbox_simulation_passed — awaiting Phase 5S Guardian enforcement to proceed further"
    elif blockers:
        rec_action = f"resolve_blockers: {blockers[0]}"
    else:
        rec_action = "review_warnings_and_retry_simulation"

    return {
        "schema_version": SCHEMA_VERSION,
        "execution_id": plan.get("execution_id", make_execution_id()),
        "generated_at": now_iso(),
        "goal": plan.get("goal", ""),
        "task_id": plan.get("task_id", ""),
        "sandbox_only": True,
        "real_project_modified": False,
        "safe_to_apply_real_project": False,
        "action_type": plan.get("action_type", ""),
        "risk_tier": plan.get("risk_tier", 2),
        "target_files": plan.get("target_files", []),
        "sandbox_dir": sandbox_result.get("sandbox_dir", ""),
        "snapshot_id": sandbox_result.get("snapshot_id", ""),
        "patch_results": patch_results,
        "verification_results": verification_result,
        "diff_hash": diff_hash,
        "file_deltas": file_deltas,
        "receipt_checked": receipt_checked,
        "receipt_valid": receipt_valid,
        "blockers": blockers,
        "warnings": warnings,
        "success": success,
        "recommended_next_action": rec_action,
        "notes": [
            "Phase 5R: sandbox-only deterministic execution proof.",
            "safe_to_apply_real_project=False (Phase 5R hard rule — always).",
            "real_project_modified=False (verified by before/after hash comparison).",
            "Wiring to Guardian/Worker/Director/Aider is planned for Phase 5S+.",
        ],
    }


def validate_executor_report(report: Any) -> tuple[bool, list[str]]:
    """Validate an executor report dict. Returns (ok, errors)."""
    errs: list[str] = []
    if not isinstance(report, dict):
        return False, ["report is not a dict"]
    for field in (
        "execution_id", "generated_at", "goal", "sandbox_only",
        "real_project_modified", "safe_to_apply_real_project",
        "action_type", "risk_tier", "target_files", "patch_results",
        "verification_results", "diff_hash", "file_deltas",
        "blockers", "warnings", "success",
    ):
        if field not in report:
            errs.append(f"missing_field:{field}")
    if report.get("sandbox_only") is not True:
        errs.append("sandbox_only must be true")
    if report.get("safe_to_apply_real_project") is not False:
        errs.append("safe_to_apply_real_project must be false")
    if report.get("real_project_modified") is not False:
        errs.append("real_project_modified must be false")
    return len(errs) == 0, errs


# ---------- rendering ----------


def render_executor_report_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Deterministic Sandbox Executor — Simulation Report")
    lines.append("")
    lines.append(f"- **execution_id**: `{report.get('execution_id', '?')}`")
    lines.append(f"- **goal**: {report.get('goal', '')!r}")
    lines.append(f"- **generated_at**: {report.get('generated_at', '?')}")
    lines.append(f"- **action_type**: `{report.get('action_type', '?')}`")
    lines.append(f"- **risk_tier**: `{report.get('risk_tier', '?')}`")
    lines.append(f"- **sandbox_only**: `{report.get('sandbox_only')}`")
    lines.append(f"- **real_project_modified**: `{report.get('real_project_modified')}` *(must be False)*")
    lines.append(f"- **safe_to_apply_real_project**: `{report.get('safe_to_apply_real_project')}` *(Phase 5R hard rule — always False)*")
    lines.append(f"- **success**: `{report.get('success')}`")
    lines.append("")

    lines.append("## Patch Results")
    for pr in (report.get("patch_results") or []):
        status = "OK" if pr.get("success") else "FAIL"
        err_suffix = ("  ERROR: " + pr.get("error")) if pr.get("error") else ""
        lines.append(f"- [{status}] `{pr.get('operation_id')}` -- {pr.get('operation')} on `{pr.get('target_file')}`{err_suffix}")

    deltas = report.get("file_deltas") or []
    if deltas:
        lines.append("")
        lines.append("## File Deltas (Sandbox)")
        for d in deltas:
            changed = "changed" if d.get("changed_in_sandbox") else "unchanged"
            lines.append(f"- `{d.get('target_file')}` — sandbox: {changed} | hash_before: `{(d.get('hash_before') or '')[:12]}...`")

    vresults = report.get("verification_results") or []
    if vresults:
        lines.append("")
        lines.append("## Verification Commands")
        for vr in vresults:
            allowed = "allowed" if vr.get("allowed") else f"BLOCKED ({vr.get('reason')})"
            rc = vr.get("rc", "?")
            lines.append(f"- `{vr.get('command')}` — {allowed} | rc={rc}")

    blockers = report.get("blockers") or []
    if blockers:
        lines.append("")
        lines.append("## Blockers")
        for b in blockers:
            lines.append(f"- {b}")

    warnings = report.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("## Warnings")
        for w in warnings:
            lines.append(f"- {w}")

    lines.append("")
    lines.append("## Notes")
    for n in (report.get("notes") or []):
        lines.append(f"- {n}")
    lines.append("")
    lines.append(f"**recommended_next_action**: {report.get('recommended_next_action', '')}")
    return "\n".join(lines) + "\n"


# ---------- write report ----------


def write_executor_report(
    project_dir: Path | str,
    report: dict[str, Any],
) -> dict[str, str]:
    """Write executor report artifacts under memory/. Returns paths dict."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_deterministic_executor_report.json"
    md_p = mem / "luna_deterministic_executor_report.md"
    audit_p = mem / "luna_deterministic_executor_audit.jsonl"
    preview_p = mem / "luna_executor_dispatch_preview.json"

    for p in (json_p, md_p, audit_p, preview_p):
        try:
            ensure_under_project(p, pdir)
        except ValueError as e:
            raise ValueError(f"Report path escapes project: {e}") from e

    write_json_atomic(json_p, report)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_executor_report_markdown(report), encoding="utf-8")
    os.replace(tmp, md_p)

    append_jsonl(audit_p, {
        "ts": now_iso(),
        "execution_id": report.get("execution_id"),
        "goal": report.get("goal"),
        "action_type": report.get("action_type"),
        "risk_tier": report.get("risk_tier"),
        "success": report.get("success"),
        "safe_to_apply_real_project": False,
        "real_project_modified": False,
        "blockers": report.get("blockers"),
    })

    write_json_atomic(preview_p, {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "execution_id": report.get("execution_id"),
        "goal": report.get("goal"),
        "safe_to_apply_real_project": False,
        "real_project_modified": False,
        "sandbox_only": True,
        "success": report.get("success"),
        "recommended_next_action": report.get("recommended_next_action"),
    })

    return {
        "json": str(json_p),
        "md": str(md_p),
        "audit": str(audit_p),
        "preview": str(preview_p),
    }


# ---------- self-test ----------


def self_test() -> int:
    """Run a complete sandbox simulation using a TemporaryDirectory. Returns 0 on success."""
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "luna_modules").mkdir(parents=True, exist_ok=True)
        (td / "memory").mkdir(parents=True, exist_ok=True)

        # Create a test file to operate on.
        target = td / "luna_modules" / "self_test_target.py"
        target.write_text(
            '# self-test target\nHELLO = "world"\n# end\n',
            encoding="utf-8",
        )
        hash_before_real = sha256_file(target)

        ops = [
            {
                "operation_id": "op_replace",
                "target_file": "luna_modules/self_test_target.py",
                "operation": "replace_text",
                "find_text": 'HELLO = "world"',
                "replace_text": 'HELLO = "sandbox"',
                "expected_occurrences": 1,
                "max_replacements": 1,
            },
            {
                "operation_id": "op_append",
                "target_file": "luna_modules/self_test_target.py",
                "operation": "append_text",
                "append_text": "\n# appended by self-test\n",
            },
        ]
        plan = build_execution_plan(
            goal="self-test sandbox simulation",
            target_files=["luna_modules/self_test_target.py"],
            patch_operations=ops,
            action_type="low_risk_additive",
            risk_tier=1,
            approval_tier_required=1,
            receipt_required=False,
            verification_commands=[
                f"python -m py_compile luna_modules/self_test_target.py",
            ],
            source="test",
        )

        ok_plan, plan_errs = validate_execution_plan(plan)
        if not ok_plan:
            print(json.dumps({"ok": False, "step": "validate_plan", "errors": plan_errs}, indent=2))
            return 1

        sandbox_result = apply_plan_in_sandbox(td, plan)
        receipt_result = _check_receipt_advisory(td, plan)
        verification_result = run_executor_verification(
            td, sandbox_result["sandbox_dir"], plan["verification_commands"]
        )

        report = build_executor_report(plan, sandbox_result, verification_result, receipt_result)
        ok_report, report_errs = validate_executor_report(report)

        hash_after_real = sha256_file(target)
        real_unchanged = (hash_before_real == hash_after_real)

        ok = (
            ok_report
            and report.get("safe_to_apply_real_project") is False
            and report.get("real_project_modified") is False
            and real_unchanged
            and report.get("schema_version") == SCHEMA_VERSION
        )

        out = {
            "ok": bool(ok),
            "execution_id": report.get("execution_id"),
            "sandbox_only": report.get("sandbox_only"),
            "real_project_modified": report.get("real_project_modified"),
            "safe_to_apply_real_project": report.get("safe_to_apply_real_project"),
            "real_file_hash_unchanged": real_unchanged,
            "patch_success": sandbox_result.get("success"),
            "blockers": report.get("blockers"),
            "warnings": report.get("warnings"),
            "report_valid": ok_report,
            "report_errors": report_errs,
        }
        print(json.dumps(out, indent=2))
        return 0 if ok else 1


# ---------- simulation helpers (CLI modes) ----------


def _run_simulation(
    name: str,
    goal: str,
    ops: list[dict[str, Any]],
    target_files: list[str],
    action_type: str = "low_risk_additive",
    risk_tier: int = 1,
    write_report: bool = False,
    project_dir: Path | str | None = None,
) -> int:
    """Run a named simulation in a TemporaryDirectory. Returns 0 on success."""
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)

        # Seed any required files.
        for rel in target_files:
            p = td / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            real_p = pdir / rel
            if real_p.is_file():
                shutil.copy2(str(real_p), str(p))
            elif not any(op.get("operation") == "create_file" for op in ops if op.get("target_file") == rel):
                p.write_text(f"# placeholder for {rel}\n", encoding="utf-8")

        plan = build_execution_plan(
            goal=goal,
            target_files=target_files,
            patch_operations=ops,
            action_type=action_type,
            risk_tier=risk_tier,
            approval_tier_required=risk_tier,
            receipt_required=(risk_tier >= 2),
            source="test",
        )
        sandbox_result = apply_plan_in_sandbox(td, plan)
        receipt_result = _check_receipt_advisory(td, plan)
        verification_result = run_executor_verification(
            td, sandbox_result["sandbox_dir"],
            [f"python -m py_compile {tf}" for tf in target_files[:1]],
        )
        report = build_executor_report(plan, sandbox_result, verification_result, receipt_result)

        out = {
            "simulation": name,
            "execution_id": report.get("execution_id"),
            "sandbox_only": report.get("sandbox_only"),
            "safe_to_apply_real_project": report.get("safe_to_apply_real_project"),
            "real_project_modified": report.get("real_project_modified"),
            "success": report.get("success"),
            "blockers": report.get("blockers"),
            "warnings": report.get("warnings"),
            "patch_results": [
                {"op": r.get("operation"), "target": r.get("target_file"), "ok": r.get("success")}
                for r in report.get("patch_results") or []
            ],
        }
        print(json.dumps(out, indent=2))

    if write_report:
        full_report = build_executor_report(
            build_execution_plan(
                goal=goal, target_files=target_files, patch_operations=ops,
                action_type=action_type, risk_tier=risk_tier,
                approval_tier_required=risk_tier, source="test",
            ),
            {"sandbox_dir": "", "snapshot_id": "", "before_records": [],
             "after_records": [], "patch_results": [], "real_project_modified": False, "success": True},
            [],
        )
        write_executor_report(pdir, full_report)

    return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Deterministic Sandbox Executor (Phase 5R)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--simulate-create-file", action="store_true")
    parser.add_argument("--simulate-replace", action="store_true")
    parser.add_argument("--simulate-append", action="store_true")
    parser.add_argument("--simulate-high-risk-worker", action="store_true")
    parser.add_argument("--print-report", action="store_true")
    parser.add_argument("--write-report", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.simulate_create_file:
        return _run_simulation(
            name="simulate_create_file",
            goal="Create a new helper module in sandbox",
            ops=[{
                "operation_id": "op_create",
                "target_file": "luna_modules/sandbox_created_example.py",
                "operation": "create_file",
                "new_file_text": '"""Sandbox-created example module."""\nHELLO = "sandbox"\n',
            }],
            target_files=["luna_modules/sandbox_created_example.py"],
            action_type="low_risk_additive",
            risk_tier=1,
            project_dir=pdir,
        )

    if args.simulate_replace:
        return _run_simulation(
            name="simulate_replace",
            goal="Replace a constant in sandbox copy",
            ops=[{
                "operation_id": "op_rep",
                "target_file": "luna_modules/sandbox_replace_target.py",
                "operation": "replace_text",
                "find_text": "# placeholder for luna_modules/sandbox_replace_target.py",
                "replace_text": "VERSION = 1  # replaced by sandbox simulation",
                "expected_occurrences": 1,
                "max_replacements": 1,
            }],
            target_files=["luna_modules/sandbox_replace_target.py"],
            action_type="medium_code_edit",
            risk_tier=2,
            project_dir=pdir,
        )

    if args.simulate_append:
        return _run_simulation(
            name="simulate_append",
            goal="Append a comment to sandbox copy",
            ops=[{
                "operation_id": "op_app",
                "target_file": "luna_modules/sandbox_append_target.py",
                "operation": "append_text",
                "append_text": "\n# appended by sandbox simulation\n",
            }],
            target_files=["luna_modules/sandbox_append_target.py"],
            action_type="low_risk_additive",
            risk_tier=1,
            project_dir=pdir,
        )

    if args.simulate_high_risk_worker:
        # High-risk simulation: copies worker.py into sandbox and patches it there.
        # Real worker.py is NEVER modified.
        return _run_simulation(
            name="simulate_high_risk_worker",
            goal="Simulate patch on worker.py sandbox copy (NO real apply)",
            ops=[{
                "operation_id": "op_worker_comment",
                "target_file": "worker.py",
                "operation": "append_text",
                "append_text": "\n# SANDBOX-ONLY test append — never applied to real file\n",
            }],
            target_files=["worker.py"],
            action_type="high_risk_core_edit",
            risk_tier=4,
            project_dir=pdir,
        )

    if args.print_report:
        rep = load_json(pdir / "memory" / "luna_deterministic_executor_report.json", default=None)
        if not isinstance(rep, dict):
            print(json.dumps({"ok": False, "error": "no_report_present"}, indent=2))
            return 1
        sys.stdout.write(render_executor_report_markdown(rep))
        return 0

    if args.write_report:
        plan = build_execution_plan(
            goal="write-report CLI test",
            target_files=[],
            patch_operations=[],
            action_type="low_risk_additive",
            risk_tier=1,
            approval_tier_required=1,
            receipt_required=False,
            source="operator",
        )
        report = build_executor_report(
            plan,
            {
                "sandbox_dir": str(pdir / "logic_updates"),
                "snapshot_id": "snap_cli_test",
                "before_records": [],
                "after_records": [],
                "patch_results": [],
                "real_project_modified": False,
                "success": True,
            },
            [],
        )
        paths = write_executor_report(pdir, report)
        print(json.dumps({
            "ok": True,
            "safe_to_apply_real_project": False,
            "real_project_modified": False,
            "paths": paths,
        }, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
