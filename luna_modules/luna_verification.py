"""Verification pipeline — module integrity, hygiene gate, smoke boot, and baseline hashing.

Extracted from ``worker.py`` (step 6 of modularity refactor).
No logic changes; all public names are re-exported from ``worker.py``.

Note: ``_log_hygiene_violation`` stays in ``worker.py`` until
``append_sovereign_journal`` is extracted in a later step.
"""

from __future__ import annotations

import ast
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from luna_modules.luna_hygiene import (
    HYGIENE_ASSIGN_BANNED_FRAGMENTS,
    HYGIENE_BANNED_NAME_FRAGMENTS,
    HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES,
    HYGIENE_NESTED_FUNCTION_MAX_LINES,
    HygieneVisitor,
    LEGACY_HYGIENE_WHITELIST,
)
from luna_modules.luna_io import (
    _compile_python_path,
    append_jsonl,
    safe_read_json,
    safe_read_text,
    safe_write_text,
    write_json_atomic,
)
from luna_modules.luna_logging import ensure_layout, now_iso
from luna_modules.luna_paths import (
    FRACTAL_DOMAIN_THRESHOLDS,
    LUNA_MODULE_REGISTRY_PATH,
    LUNA_MODULES_DIR,
    LUNA_MODULES_INIT_PATH,
    MEMORY_DIR,
    PROJECT_DIR,
    SELF_FIX_LOG_PATH,
    TARGET_FILE_DOES_NOT_EXIST,
    VERIFY_TIMEOUT_SECONDS,
)
from luna_modules.luna_routing import normalize_prompt_text

# Mutable cache shared with worker.py — the dict object is the same instance
# once worker.py does ``from luna_modules.luna_verification import VERIFICATION_CACHE``.
VERIFICATION_CACHE: Dict[Tuple[str, int, int], Dict[str, Any]] = {}


# ── Module-domain helpers ────────────────────────────────────────────────────

def _normalize_module_domain(domain: str) -> str:
    token = re.sub(r"[^a-z0-9_]+", "_", normalize_prompt_text(domain).replace(" ", "_")).strip("_")
    return f"luna_{token or 'domain'}"


def should_trigger_module_extraction(domain: str, logical_lines: int) -> bool:
    threshold = int(FRACTAL_DOMAIN_THRESHOLDS.get(str(domain).strip().lower(), 80))
    return int(logical_lines) >= threshold


def build_telemetry_module_text() -> str:
    return (
        "import sys\n"
        "from datetime import datetime\n"
        "from pathlib import Path\n"
        "from typing import Any, Callable, Deque, Optional\n\n"
        "def _stamp() -> str:\n"
        "    return datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")\n\n"
        "def ensure_telemetry_layout(*paths: Path) -> None:\n"
        "    for item in paths:\n"
        "        Path(item).parent.mkdir(parents=True, exist_ok=True)\n\n"
        "def emit_diag(\n"
        "    message: str,\n"
        "    diagnostic_prefix: str,\n"
        "    worker_log_path: Path,\n"
        "    layout_cb: Optional[Callable[[], None]] = None,\n"
        ") -> None:\n"
        "    line = f\"{diagnostic_prefix} {message}\"\n"
        "    try:\n"
        "        sys.stderr.write(line + \"\\n\")\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        if layout_cb is not None:\n"
        "            layout_cb()\n"
        "        else:\n"
        "            ensure_telemetry_layout(worker_log_path)\n"
        "        with open(worker_log_path, \"a\", encoding=\"utf-8\") as handle:\n"
        "            handle.write(f\"[{_stamp()}] {line}\\n\")\n"
        "    except Exception:\n"
        "        pass\n\n"
        "def emit_log(\n"
        "    message: str,\n"
        "    worker_log_path: Path,\n"
        "    layout_cb: Optional[Callable[[], None]] = None,\n"
        ") -> None:\n"
        "    line = f\"[{_stamp()}] {message}\"\n"
        "    print(line, flush=True)\n"
        "    try:\n"
        "        if layout_cb is not None:\n"
        "            layout_cb()\n"
        "        else:\n"
        "            ensure_telemetry_layout(worker_log_path)\n"
        "        with open(worker_log_path, \"a\", encoding=\"utf-8\") as handle:\n"
        "            handle.write(line + \"\\n\")\n"
        "    except Exception:\n"
        "        pass\n\n"
        "def emit_speak(\n"
        "    message: str,\n"
        "    mood: str,\n"
        "    autonomy_messages: Deque[str],\n"
        "    heartbeat_cb: Callable[..., None],\n"
        "    log_cb: Callable[[str], None],\n"
        ") -> None:\n"
        "    autonomy_messages.append(message)\n"
        "    heartbeat_cb(mood=mood, last_message=message)\n"
        "    log_cb(f\"[LUNA] {message}\")\n"
    )


def spawn_new_module(domain: str, module_text: str, export_names: Optional[List[str]] = None) -> Dict[str, Any]:
    ensure_layout()
    module_name = _normalize_module_domain(domain)
    module_path = LUNA_MODULES_DIR / f"{module_name}.py"
    try:
        safe_write_text(LUNA_MODULES_INIT_PATH, '"""Luna fractal modules."""\n')
        safe_write_text(module_path, module_text.rstrip() + "\n")
        compiled_ok, compile_detail = _compile_python_path(module_path)
        if not compiled_ok:
            raise RuntimeError(compile_detail)
    except Exception as exc:
        return {
            "ok": False,
            "domain": domain,
            "module_name": module_name,
            "module_path": str(module_path),
            "exports": list(export_names or []),
            "error": str(getattr(exc, "msg", exc)),
        }
    registry = safe_read_json(LUNA_MODULE_REGISTRY_PATH, default={"modules": {}})
    registry.setdefault("modules", {})[module_name] = {
        "ts": now_iso(),
        "domain": domain,
        "path": str(module_path),
        "exports": list(export_names or []),
    }
    write_json_atomic(LUNA_MODULE_REGISTRY_PATH, registry)
    return {
        "ok": True,
        "domain": domain,
        "module_name": module_name,
        "module_path": str(module_path),
        "exports": list(export_names or []),
    }


# ── Module-integrity helpers ─────────────────────────────────────────────────

def _imported_luna_modules(tree: ast.AST) -> List[str]:
    module_names: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and str(node.module).startswith("luna_modules."):
            value = str(node.module)
            if value not in module_names:
                module_names.append(value)
    return module_names


def _module_path_from_import(module_name: str) -> Path:
    return PROJECT_DIR / (module_name.replace(".", "/") + ".py")


def _module_has_worker_cycle(module_path: Path, owner_name: str) -> bool:
    text_value = safe_read_text(module_path)
    if not text_value.strip():
        return False
    owner_token = owner_name.replace(".py", "")
    # Use concatenation so this module's own text does not false-positive
    # the naive substring scan when luna_verification.py is itself checked.
    patterns = (
        f"import {owner_token}",
        f"from {owner_token} import",
        "import" " worker",
        "from" " worker import",
    )
    return any(token in text_value for token in patterns)


def _module_import_has_runtime_fallback(source_path: Path, module_name: str) -> bool:
    if str(module_name or "").strip() != "luna_modules.luna_telemetry":
        return False
    source_text = safe_read_text(source_path)
    return all(token in source_text for token in (
        "from luna_modules.luna_telemetry import",
        "def telemetry_emit_diag",
        "def telemetry_emit_log",
        "def telemetry_emit_speak",
    ))


def verify_luna_module_integrity(source_path: Path, tree: Optional[ast.AST] = None, compile_modules: bool = True) -> Dict[str, Any]:
    parsed_tree = tree
    if parsed_tree is None:
        try:
            parsed_tree = ast.parse(safe_read_text(source_path))
        except Exception as exc:
            return {"ok": False, "imports": [], "checked": [], "violations": [f"module parse failed: {exc}"]}
    module_names = _imported_luna_modules(parsed_tree)
    violations: List[str] = []
    checked: List[str] = []
    for module_name in module_names:
        module_path = _module_path_from_import(module_name)
        checked.append(str(module_path))
        if not module_path.exists():
            if _module_import_has_runtime_fallback(source_path, module_name):
                continue
            violations.append(f"missing_module :: {module_name}")
            continue
        if _module_has_worker_cycle(module_path, source_path.name):
            violations.append(f"circular_import_detected :: {module_name}")
            continue
        if compile_modules:
            compiled_ok, compile_detail = _compile_python_path(module_path)
            if not compiled_ok:
                violations.append(f"module_compile_failed :: {module_name} :: {compile_detail}")
    return {"ok": not violations, "imports": module_names, "checked": checked, "violations": violations}


# ── Hygiene gate ─────────────────────────────────────────────────────────────

def verify_code_hygiene(source_path: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "target": source_path,
        "hygiene_ok": False,
        "violations": [],
        "nested_function_limit": HYGIENE_NESTED_FUNCTION_MAX_LINES,
        "local_string_assign_limit": HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES,
        "banned_fragments": list(HYGIENE_BANNED_NAME_FRAGMENTS),
        "assign_banned_fragments": list(HYGIENE_ASSIGN_BANNED_FRAGMENTS),
        "legacy_guided_whitelist": sorted(LEGACY_HYGIENE_WHITELIST),
        "module_imports": [],
        "module_violations": [],
    }
    path = Path(source_path)
    if not path.exists():
        result["violations"].append(TARGET_FILE_DOES_NOT_EXIST)
        return result

    content = safe_read_text(path)
    try:
        tree = ast.parse(content)
    except SyntaxError as exc:
        result["violations"].append(f"hygiene parse failed: {exc}")
        return result

    visitor = HygieneVisitor(str(path))
    visitor.visit(tree)
    module_gate = verify_luna_module_integrity(path, tree, compile_modules=False)
    result["module_imports"] = list(module_gate.get("imports") or [])
    result["module_violations"] = list(module_gate.get("violations") or [])
    result["violations"] = visitor.violations + result["module_violations"]
    result["hygiene_ok"] = not result["violations"]
    return result


def _verification_has_hygiene_failure(verification: Optional[Dict[str, Any]]) -> bool:
    return bool(isinstance(verification, dict) and verification.get("hygiene_ok") is False)


def _verification_hygiene_detail(verification: Optional[Dict[str, Any]]) -> str:
    if not isinstance(verification, dict):
        return "hygiene gate failed"
    violations = list(verification.get("hygiene_violations") or verification.get("violations") or [])
    if violations:
        return "; ".join(str(item) for item in violations[:5])
    return "hygiene gate failed"


# ── Core verification pipeline ────────────────────────────────────────────────

def _blank_verification_result(target_file: str) -> Dict[str, Any]:
    return {
        "target": target_file,
        "target_exists": False,
        "ast_parse": False,
        "py_compile": False,
        "module_integrity_ok": True,
        "module_imports": [],
        "smoke_boot": None,
        "smoke_target": "",
        "hygiene_ok": True,
        "hygiene_violations": [],
        "summary": "",
        "details": [],
        "passed": False,
        "cache_hit": False,
    }


def _verification_cache_key(path: Path) -> Optional[tuple]:
    try:
        stat = path.stat()
        return (str(path), int(stat.st_mtime_ns), int(stat.st_size))
    except Exception:
        return None


def _run_smoke_boot(path: Path) -> tuple:
    """Return (passed: bool, failure_detail: str). detail is '' on success."""
    try:
        smoke = subprocess.run(
            [sys.executable, str(path), "--verify-smoke"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_DIR),
            timeout=VERIFY_TIMEOUT_SECONDS,
            env={**os.environ, "LUNA_PROJECT_DIR": str(PROJECT_DIR), "LUNA_VERIFY_MODE": "1"},
        )
        if smoke.returncode == 0:
            return True, ""
        detail = (smoke.stderr or smoke.stdout or "non-zero exit").strip()
        return False, f"smoke boot failed: {detail[:200]}"
    except Exception as exc:
        return False, f"smoke boot failed: {exc}"


def _apply_parse_compile_checks(result: Dict[str, Any], path: Path, content: str) -> None:
    parsed_tree: Optional[ast.AST] = None
    try:
        parsed_tree = ast.parse(content)
        result["ast_parse"] = True
    except SyntaxError as exc:
        result["details"].append(f"ast parse failed: {exc}")
    compiled_ok, compile_detail = _compile_python_path(path)
    if compiled_ok:
        result["py_compile"] = True
    else:
        result["details"].append(f"py_compile failed: {compile_detail}")
    module_gate = verify_luna_module_integrity(path, parsed_tree, compile_modules=True) if parsed_tree is not None else {"ok": True, "imports": [], "violations": []}
    result["module_imports"] = list(module_gate.get("imports") or [])
    result["module_integrity_ok"] = bool(module_gate.get("ok", True))
    if not result["module_integrity_ok"]:
        result["py_compile"] = False
        for item in list(module_gate.get("violations") or [])[:5]:
            result["details"].append(f"module import gate failed: {item}")


def _apply_hygiene_checks(result: Dict[str, Any], path: Path) -> None:
    hygiene = verify_code_hygiene(str(path))
    result["hygiene_ok"] = bool(hygiene.get("hygiene_ok"))
    result["hygiene_violations"] = list(hygiene.get("violations") or [])
    if not result["hygiene_ok"]:
        result["details"].append(f"hygiene gate failed: {_verification_hygiene_detail(hygiene)}")


def _apply_smoke_boot_checks(result: Dict[str, Any], path: Path) -> None:
    if path.name.lower() not in {"worker.py", "surgeapp_claude_terminal.py"}:
        return
    result["smoke_target"] = path.name
    passed, detail = _run_smoke_boot(path)
    result["smoke_boot"] = passed
    if not passed:
        result["details"].append(detail)


def verify_python_target(target_file: str) -> Dict[str, Any]:
    result = _blank_verification_result(target_file)
    path = Path(target_file)
    if not path.exists():
        result["summary"] = TARGET_FILE_DOES_NOT_EXIST
        result["details"].append(TARGET_FILE_DOES_NOT_EXIST)
        return result
    cache_key = _verification_cache_key(path)
    if cache_key and cache_key in VERIFICATION_CACHE:
        cached = dict(VERIFICATION_CACHE[cache_key])
        cached["cache_hit"] = True
        return cached
    result["target_exists"] = True
    content = safe_read_text(path)
    _apply_parse_compile_checks(result, path, content)
    _apply_hygiene_checks(result, path)
    _apply_smoke_boot_checks(result, path)
    checks = [result["target_exists"], result["ast_parse"], result["py_compile"], result["hygiene_ok"]]
    if result["smoke_boot"] is not None:
        checks.append(bool(result["smoke_boot"]))
    result["passed"] = all(checks)
    result["summary"] = "verification passed" if result["passed"] else (result["details"][0] if result["details"] else "verification failed")
    if cache_key:
        VERIFICATION_CACHE[cache_key] = dict(result)
    return result


def verification_section(verification: Dict[str, Any]) -> str:
    smoke_value = "SKIPPED" if verification.get("smoke_boot") is None else ("PASSED" if verification.get("smoke_boot") else "FAILED")
    lines = [
        "--- Verification Harness ---",
        f"target exists : {'PASSED' if verification.get('target_exists') else 'FAILED'}",
        f"AST parse     : {'PASSED' if verification.get('ast_parse') else 'FAILED'}",
        f"py_compile    : {'PASSED' if verification.get('py_compile') else 'FAILED'}",
        f"hygiene gate  : {'PASSED' if verification.get('hygiene_ok', True) else 'FAILED'}",
        f"smoke boot    : {smoke_value}",
    ]
    if verification.get("details"):
        lines.append("details       :")
        for item in verification["details"][:5]:
            lines.append(f"  - {item}")
    return "\n".join(lines)


def build_core_baseline_hashes() -> Dict[str, Any]:
    files = [PROJECT_DIR / "worker.py", PROJECT_DIR / "SurgeApp_Claude_Terminal.py"]
    hashes: Dict[str, str] = {}
    for path in files:
        if path.exists():
            hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            hashes[str(path)] = ""
    return {
        "ts": now_iso(),
        "files": hashes,
        "ok": all(bool(value) for value in hashes.values()),
    }


def freeze_core_baseline(reason: str = "") -> Dict[str, Any]:
    payload = build_core_baseline_hashes()
    payload["reason"] = reason
    core = safe_read_json(MEMORY_DIR / "luna_core_memory.json", default={})
    core["baseline_freeze"] = payload
    write_json_atomic(MEMORY_DIR / "luna_core_memory.json", core)
    return payload


def verification_ok(verification: Dict[str, Any]) -> bool:
    return bool(verification.get("passed"))


def attach_verification(report: str, verification: Dict[str, Any]) -> str:
    return report.rstrip() + "\n\n" + verification_section(verification)


# ── Repair helpers ────────────────────────────────────────────────────────────

def _restore_from_backup(target_path: Path, backup_path: Path) -> bool:
    try:
        import shutil
        shutil.copy2(str(backup_path), str(target_path))
        return True
    except Exception:
        return False


def _first_report_line(report: str) -> str:
    """Return the first meaningful line of a report string (mirrors _mission_summary_line)."""
    for line in report.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("[") and not stripped.startswith("---"):
            return stripped
    return "No summary available."


def append_self_fix_log(task_id: str, target_file: str, report: str, success: bool) -> None:
    append_jsonl(
        SELF_FIX_LOG_PATH,
        {
            "ts": now_iso(),
            "task_id": task_id,
            "target_file": target_file,
            "success": success,
            "summary": _first_report_line(report),
        },
    )
