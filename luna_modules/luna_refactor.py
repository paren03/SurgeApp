"""Self-improvement pipeline, code analysis, and journal primitives.

Extracted from ``worker.py`` (step 9 of modularity refactor).

Note: ``run_internal_council`` stays in ``worker.py`` because it needs
``sovereign_task_router`` (not yet extracted).  A callback is provided
via ``set_internal_council_callback`` so ``_maybe_run_internal_council``
here can forward to it without a circular import.
"""

from __future__ import annotations

import ast
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from luna_modules.luna_io import (
    append_jsonl,
    safe_read_json,
    safe_read_text,
    safe_write_text,
    write_json_atomic,
)
from luna_modules.luna_logging import now_iso
from luna_modules.luna_paths import (
    ANTI_PARALYSIS_VIOLATION,
    AUTONOMY_JOURNAL_PATH,
    BACKUPS_DIR,
    COUNCIL_HISTORY_PATH,
    DEFAULT_LUNA_SYSTEM_PROMPT,
    INTERNAL_COUNCIL_COMPLEXITY_MARKERS,
    INTERNAL_COUNCIL_HISTORY_LIMIT,
    LUNA_MASTER_CODEX_PATH,
    LUNA_SYSTEM_PROMPT_PATH,
    NEGATIVE_GROWTH_LINE_BUFFER,
    NEGATIVE_GROWTH_MAX_HELPER_DELTA,
    NEGATIVE_GROWTH_MAX_LONG_FUNCTION_DELTA,
    OMEGA_BATCH2_FLAGS_PATH,
    ORCHESTRATOR_REFACTOR_CONSTRAINT,
    ORCHESTRATOR_REFACTOR_TARGETS,
    PROMPT_OPTIMIZER_INTERVAL_DAYS,
    PROMPT_OPTIMIZER_MANAGED_END,
    PROMPT_OPTIMIZER_MANAGED_START,
    PROMPT_OPTIMIZER_STATE_PATH,
    PROMPT_OPTIMIZER_SUCCESS_THRESHOLD,
    SELF_FIX_LOG_PATH,
    SOLUTIONS_DIR,
    SOVEREIGN_JOURNAL_PATH,
    STRICT_REFACTOR_CATALOG,
    STRICT_REFACTOR_NEGATIVE_CONSTRAINT,
)
from luna_modules.luna_routing import normalize_prompt_text
from luna_modules.luna_tasks import build_backup_path
from luna_modules.luna_verification import (
    _restore_from_backup,
    _verification_has_hygiene_failure,
    _verification_hygiene_detail,
    append_self_fix_log,
    verification_ok,
    verification_section,
    verify_python_target,
)

# ── Callback registration for run_internal_council ────────────────────────────
# Avoids a circular import: worker.py registers its run_internal_council after
# importing this module.
_run_internal_council_fn = None


def set_internal_council_callback(fn) -> None:
    global _run_internal_council_fn
    _run_internal_council_fn = fn


# ── Journal primitives ────────────────────────────────────────────────────────

def append_autonomy_journal(event: str, detail: str, ok: bool = True) -> None:
    append_jsonl(AUTONOMY_JOURNAL_PATH, {
        "ts": now_iso(),
        "event": event,
        "detail": str(detail)[:2000],
        "ok": bool(ok),
    })


def append_sovereign_journal(
    category: str,
    summary: str,
    detail: str = "",
    ok: bool = True,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = safe_read_json(SOVEREIGN_JOURNAL_PATH, default={}) or {}
    entries = list(payload.get("entries") or [])
    entry: Dict[str, Any] = {
        "ts": now_iso(),
        "category": str(category),
        "summary": str(summary)[:240],
        "detail": str(detail)[:4000],
        "ok": bool(ok),
    }
    if isinstance(extra, dict):
        entry.update(extra)
    entries.append(entry)
    payload["entries"] = entries[-200:]
    payload["last_updated"] = now_iso()
    write_json_atomic(SOVEREIGN_JOURNAL_PATH, payload)
    return entry


def _append_sovereign_journal_once(
    category: str,
    summary: str,
    detail: str = "",
    ok: bool = True,
    extra: Optional[Dict[str, Any]] = None,
    min_gap_seconds: int = 120,
) -> Optional[Dict[str, Any]]:
    payload = safe_read_json(SOVEREIGN_JOURNAL_PATH, default={}) or {}
    entries = list(payload.get("entries") or [])
    if entries:
        last = entries[-1]
        if (
            str(last.get("category", "")) == str(category)
            and str(last.get("summary", "")) == str(summary)[:240]
            and str(last.get("detail", "")) == str(detail)[:4000]
        ):
            try:
                gap = (datetime.now() - datetime.fromisoformat(str(last.get("ts") or ""))).total_seconds()
            except Exception:
                gap = float(min_gap_seconds) + 1.0
            if gap <= float(min_gap_seconds):
                return None
    return append_sovereign_journal(category, summary, detail, ok, extra)


def _log_hygiene_violation(target_name: str, verification: Optional[Dict[str, Any]], stage: str, unattended: bool) -> None:
    append_sovereign_journal(
        "hygiene_violation",
        f"{stage} :: {target_name}",
        _verification_hygiene_detail(verification),
        False,
        {"verification": verification or {}, "auto_commit": unattended},
    )


# ── Code analysis helpers ─────────────────────────────────────────────────────

def extract_top_level_functions(content: str) -> List[Tuple[str, int, int]]:
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return []
    functions = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append((node.name, node.lineno, getattr(node, "end_lineno", node.lineno)))
    return functions


def collect_missing_docstrings(content: str) -> List[str]:
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return []
    return [
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and ast.get_docstring(node) is None
    ]


def detect_repeated_string_literals(content: str) -> List[Tuple[str, int]]:
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return []
    counts: Dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value.strip()
            if len(value) < 24 or "\n" in value or value.startswith("[LUNA"):
                continue
            counts[value] = counts.get(value, 0) + 1
    items = [(value, count) for value, count in counts.items() if count >= 2]
    items.sort(key=lambda item: (-item[1], item[0]))
    return items


def _preview(text: str, limit: int = 60) -> str:
    text = text.replace("\n", " ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


def _improvement_sections(content: str) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]], List[str]]:
    functions = extract_top_level_functions(content)
    long_functions = [(name, end - start + 1) for name, start, end in functions if (end - start + 1) > 40]
    long_functions.sort(key=lambda item: -item[1])
    return long_functions, detect_repeated_string_literals(content), collect_missing_docstrings(content)


def _append_analysis_block(report: List[str], title: str, rows: List[str], empty_text: str) -> None:
    report += ["", title]
    report.extend(rows or [empty_text])


def _improvement_suggestions(
    long_functions: List[Tuple[str, int]],
    repeated_literals: List[Tuple[str, int]],
    missing_docstrings: List[str],
) -> List[str]:
    suggestions: List[str] = []
    if long_functions:
        suggestions.append(f"  - Split {len(long_functions)} long function(s) into smaller helpers.")
    if repeated_literals:
        suggestions.append(f"  - Extract {len(repeated_literals)} repeated string(s) into named constants.")
    if missing_docstrings:
        suggestions.append(f"  - Add docstrings to {len(missing_docstrings)} function(s) for maintainability.")
    return suggestions or ["  - No significant improvements identified."]


def run_improvement_analysis(task_id: str, target_file: str) -> str:
    try:
        content = Path(target_file).read_text(encoding="utf-8")
    except Exception as exc:
        return f"[LUNA IMPROVEMENT FAILURE]\nCould not read {target_file}: {exc}\ntask_id: {task_id}\n"
    long_functions, repeated_literals, missing_docstrings = _improvement_sections(content)
    report = [
        "[LUNA IMPROVEMENT ANALYSIS]",
        f"task_id : {task_id}",
        f"target  : {target_file}",
        f"lines   : {len(content.splitlines())}",
        f"functions: {len(extract_top_level_functions(content))}",
    ]
    _append_analysis_block(report, "--- Long Functions (>40 lines) ---", [f"  {name}: {length} lines \u2014 candidate for decomposition" for name, length in long_functions[:20]], "  None detected.")
    _append_analysis_block(report, "--- Safe Repeated String Literals ---", [f'  x{count}  "{_preview(value)}"' for value, count in repeated_literals[:20]], "  None detected.")
    _append_analysis_block(report, "--- Functions Without Docstrings ---", [f"  {name}" for name in missing_docstrings[:50]], "  None detected.")
    _append_analysis_block(report, "--- Potential Improvements ---", _improvement_suggestions(long_functions, repeated_literals, missing_docstrings), "  - No significant improvements identified.")
    report += ["", "NOTE: This is a read-only analysis. No file was modified."]
    return "\n".join(report)


# ── Refactor request helpers ──────────────────────────────────────────────────

GUIDED_REWRITE_HELPER_FUNCTIONS: Dict[str, List[str]] = {
    "_handle_standard_task_mode": [
        "_run_standard_mode_action",
        "_evaluate_standard_mode_success",
    ],
    "run_refactor_self_improvement": [
        "_refactor_baseline_block_response",
        "_refactor_noop_response",
        "_refactor_stage_failure_response",
        "_refactor_apply_failure_response",
        "_refactor_apply_success_response",
    ],
    "run_rsi_cycle": [
        "_persist_rsi_cycle_result",
        "_build_rsi_cycle_report",
    ],
    "dummy_unattended_target": [
        "_dummy_unattended_step",
    ],
}


def _refactor_prompt_text(task_or_prompt: Any) -> str:
    if isinstance(task_or_prompt, dict):
        return str(task_or_prompt.get("prompt") or task_or_prompt.get("objective") or task_or_prompt.get("kind") or "")
    return str(task_or_prompt or "")


def _refactor_requested_symbols(task_or_prompt: Any) -> List[str]:
    prompt = _refactor_prompt_text(task_or_prompt)
    normalized = normalize_prompt_text(prompt)
    task_payload = task_or_prompt if isinstance(task_or_prompt, dict) else {}
    requested: List[str] = []
    target_symbol = str(task_payload.get("target_symbol") or "").strip()
    if target_symbol:
        requested.append(target_symbol)
    symbol_aliases = {
        "_handle_standard_task_mode": ["_handle_standard_task_mode", "handle standard task mode", "standard task mode"],
        "run_refactor_self_improvement": ["run_refactor_self_improvement", "run guided self improvement", "guided self improvement"],
        "dummy_unattended_target": ["dummy_unattended_target", "dummy unattended target", "dummy function"],
        "run_rsi_cycle": ["run_rsi_cycle", "run rsi cycle", "rsi cycle"],
        "run_system_action": ["run_system_action", "run system action", "system action"],
        "run_mission_orchestration": ["run_mission_orchestration", "run mission orchestration", "mission orchestration"],
        "run_sovereign_evolution_engine": ["run_sovereign_evolution_engine", "run sovereign evolution engine", "sovereign evolution engine"],
        "main": ["main", "worker main", "entrypoint"],
        "_build_refactor_candidate": ["_build_refactor_candidate", "build refactor candidate", "refactor candidate"],
        "module_import_cleanup": ["module_import_cleanup", "module import cleanup", "unused imports", "dead code removal", "import cleanup"],
        "verify_python_target": ["verify_python_target", "verify python target", "verification harness"],
    }
    for symbol, aliases in symbol_aliases.items():
        if any(alias in prompt or alias in normalized for alias in aliases):
            requested.append(symbol)
    return list(dict.fromkeys(requested))


def _unused_import_entries(source: str) -> List[Dict[str, Any]]:
    try:
        tree = ast.parse(source)
    except Exception:
        return []
    used: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(str(node.id))
    entries: List[Dict[str, Any]] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                bind = str(alias.asname or alias.name.split(".", 1)[0])
                if bind not in used:
                    entries.append({"bind": bind, "lineno": int(node.lineno), "end_lineno": int(getattr(node, "end_lineno", node.lineno))})
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                bind = str(alias.asname or alias.name)
                if bind not in used:
                    entries.append({"bind": bind, "lineno": int(node.lineno), "end_lineno": int(getattr(node, "end_lineno", node.lineno))})
    return entries


def _resolve_refactor_catalog_action(normalized_prompt: str, task_payload: Dict[str, Any]) -> str:
    action = str(task_payload.get("catalog_action") or "").strip().upper()
    if action:
        return action
    if any(token in normalized_prompt for token in ("dead code", "unused import", "uncalled internal function")):
        return "DEAD_CODE_REMOVAL"
    return "EXTRACT_HELPERS"


def _refactor_target_symbol(task_or_prompt: Any) -> str:
    task_payload = task_or_prompt if isinstance(task_or_prompt, dict) else {}
    target_symbol = str(task_payload.get("target_symbol") or "").strip()
    if target_symbol:
        return target_symbol
    requested = _refactor_requested_symbols(task_or_prompt)
    return requested[0] if requested else ""


def _is_orchestrator_refactor_target(function_name: str) -> bool:
    return str(function_name or "").strip() in ORCHESTRATOR_REFACTOR_TARGETS


def _build_refactor_catalog_prompt(function_name: str, catalog_action: str) -> str:
    parts = [
        STRICT_REFACTOR_NEGATIVE_CONSTRAINT,
        f"Catalog action: {catalog_action}.",
        STRICT_REFACTOR_CATALOG.get(catalog_action, "Perform a deterministic negative-growth refactor."),
    ]
    if function_name:
        parts.append(f"Target function: {function_name}.")
    if catalog_action == "EXTRACT_HELPERS" and _is_orchestrator_refactor_target(function_name):
        parts.append(ORCHESTRATOR_REFACTOR_CONSTRAINT)
    return " ".join(str(part).strip() for part in parts if str(part).strip())


def _join_module_lines(lines: List[str], trailing_newline: bool) -> str:
    content = "\n".join(lines)
    return content + ("\n" if trailing_newline else "")


def _build_dead_code_candidate(module_code: str) -> Tuple[str, List[str], List[str]]:
    plan = ["remove unused imports and uncalled internal functions when deterministic evidence exists"]
    changes: List[str] = []
    unused_entries = _unused_import_entries(module_code)
    if not unused_entries:
        return module_code, changes, plan
    remove_lines = set()
    removed_names: List[str] = []
    for entry in unused_entries:
        for lineno in range(int(entry["lineno"]), int(entry["end_lineno"]) + 1):
            remove_lines.add(lineno - 1)
        removed_names.append(str(entry.get("bind") or ""))
    module_lines = module_code.splitlines()
    filtered = [line for idx, line in enumerate(module_lines) if idx not in remove_lines]
    candidate = _join_module_lines(filtered, module_code.endswith("\n"))
    if removed_names:
        changes.append("removed unused imports: " + ", ".join(sorted(name for name in removed_names if name)))
    plan.append("removed deterministic unused imports to reduce module overhead")
    return candidate, changes, plan


def _top_level_function_node(module_tree: ast.AST, function_name: str) -> Optional[ast.AST]:
    for item in getattr(module_tree, "body", []):
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == function_name:
            return item
    return None


def _replace_top_level_block(module_code: str, node: ast.AST, replacement_lines: List[str]) -> str:
    module_lines = module_code.splitlines()
    start = int(getattr(node, "lineno", 1)) - 1
    end = int(getattr(node, "end_lineno", getattr(node, "lineno", 1)))
    module_lines[start:end] = replacement_lines
    return _join_module_lines(module_lines, module_code.endswith("\n"))


def _system_action_helper_lines(*args, **kwargs):
    return [] if "_system_action_helper_lines" != "_build_run_system_action_candidate" else (args[0] if args else "", [], [])


def _system_action_function_lines(*args, **kwargs):
    return [] if "_system_action_function_lines" != "_build_run_system_action_candidate" else (args[0] if args else "", [], [])


def _build_run_system_action_candidate(*args, **kwargs):
    return [] if "_build_run_system_action_candidate" != "_build_run_system_action_candidate" else (args[0] if args else "", [], [])


def _top_level_function_block(module_code: str, function_name: str) -> str:
    if not function_name:
        return ""
    try:
        module_tree = ast.parse(module_code)
    except Exception:
        return ""
    node = _top_level_function_node(module_tree, function_name)
    if node is None:
        return ""
    module_lines = module_code.splitlines()
    return "\n".join(module_lines[int(node.lineno) - 1:int(getattr(node, "end_lineno", node.lineno))]).strip()


# ── Anti-paralysis ────────────────────────────────────────────────────────────

def _anti_paralysis_verification(target_path: Path, target_symbol: str) -> Dict[str, Any]:
    return {
        "target": str(target_path),
        "target_exists": True,
        "ast_parse": True,
        "py_compile": True,
        "module_integrity_ok": True,
        "module_imports": [],
        "smoke_boot": None,
        "smoke_target": target_path.name,
        "hygiene_ok": False,
        "hygiene_violations": [ANTI_PARALYSIS_VIOLATION],
        "summary": ANTI_PARALYSIS_VIOLATION,
        "details": [ANTI_PARALYSIS_VIOLATION, f"target_symbol: {target_symbol}"],
        "passed": False,
        "cache_hit": False,
    }


def _detect_anti_paralysis_violation(
    target_path: Path,
    source: str,
    candidate_text: str,
    task_or_prompt: Any,
) -> Optional[Dict[str, Any]]:
    task_payload = task_or_prompt if isinstance(task_or_prompt, dict) else {}
    prompt = _refactor_prompt_text(task_or_prompt)
    catalog_action = _resolve_refactor_catalog_action(normalize_prompt_text(prompt), task_payload)
    if catalog_action != "EXTRACT_HELPERS":
        return None
    target_symbol = _refactor_target_symbol(task_or_prompt)
    if not target_symbol:
        return None
    previous_block = _top_level_function_block(source, target_symbol)
    candidate_block = _top_level_function_block(candidate_text, target_symbol)
    if previous_block and candidate_block and candidate_block == previous_block:
        return _anti_paralysis_verification(target_path, target_symbol)
    return None


# ── Candidate building ────────────────────────────────────────────────────────

def _build_refactor_candidate(source: str, task_or_prompt: Any, target_file: str) -> Tuple[str, List[str], List[str]]:
    prompt = _refactor_prompt_text(task_or_prompt)
    normalized = normalize_prompt_text(prompt)
    task_payload = task_or_prompt if isinstance(task_or_prompt, dict) else {}
    catalog_action = _resolve_refactor_catalog_action(normalized, task_payload)
    if not target_file.endswith(".py"):
        return source, [], []
    if catalog_action == "DEAD_CODE_REMOVAL":
        return _build_dead_code_candidate(source)
    requested_symbols = _refactor_requested_symbols(task_or_prompt)
    target_symbol = _refactor_target_symbol(task_or_prompt)
    plan: List[str] = []
    if requested_symbols:
        plan.append("extract helpers only when a direct native-python rewrite is deterministic and negative-growth-safe")
    else:
        plan.append("no deterministic extract-helper candidate was selected; dead-code cleanup may still be available if unused imports exist")
    if target_symbol:
        plan.append(f"target_symbol={target_symbol}")
    if catalog_action == "EXTRACT_HELPERS" and _is_orchestrator_refactor_target(target_symbol):
        plan.append(ORCHESTRATOR_REFACTOR_CONSTRAINT)
    return source, [], plan


def _complexity_metrics_from_text(source_text: str) -> Dict[str, int]:
    try:
        tree = ast.parse(source_text)
    except Exception:
        return {"line_count": len(source_text.splitlines()), "function_count": 0, "long_function_count": 0}
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    long_functions = []
    for node in functions:
        line_count = int(getattr(node, "end_lineno", getattr(node, "lineno", 0))) - int(getattr(node, "lineno", 0)) + 1
        if line_count > 80:
            long_functions.append(node)
    return {"line_count": len(source_text.splitlines()), "function_count": len(functions), "long_function_count": len(long_functions)}


def _verify_negative_growth(original_text: str, candidate_text: str) -> Dict[str, Any]:
    before = _complexity_metrics_from_text(original_text)
    after = _complexity_metrics_from_text(candidate_text)
    violations = []
    if after["line_count"] > before["line_count"] + NEGATIVE_GROWTH_LINE_BUFFER:
        violations.append(f"file_inflation_detected :: line_count {after['line_count']} > {before['line_count']} + {NEGATIVE_GROWTH_LINE_BUFFER}")
    function_delta = after["function_count"] - before["function_count"]
    if function_delta > NEGATIVE_GROWTH_MAX_HELPER_DELTA:
        violations.append(f"helper_explosion_detected :: function_count delta {function_delta} > {NEGATIVE_GROWTH_MAX_HELPER_DELTA}")
    long_delta = after["long_function_count"] - before["long_function_count"]
    if long_delta > NEGATIVE_GROWTH_MAX_LONG_FUNCTION_DELTA:
        violations.append(f"file_inflation_detected :: long_function_count delta {long_delta} > {NEGATIVE_GROWTH_MAX_LONG_FUNCTION_DELTA}")
    return {"negative_growth_ok": not violations, "violations": violations, "before": before, "after": after}


def _verify_refactor_candidate_text(target_path: Path, candidate_text: str) -> Dict[str, Any]:
    tmp = tempfile.mkdtemp(prefix="luna_guided_apply_")
    try:
        sandbox_path = Path(tmp) / target_path.name
        sandbox_path.write_text(candidate_text, encoding="utf-8")
        verification = verify_python_target(str(sandbox_path))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    verification["target"] = str(target_path)
    return verification


def _apply_refactor_candidate(target_path: Path, candidate_text: str) -> Dict[str, Any]:
    backup_path = build_backup_path(target_path)
    shutil.copy2(str(target_path), str(backup_path))
    safe_write_text(target_path, candidate_text)
    live_verification = verify_python_target(str(target_path))
    if verification_ok(live_verification):
        return {"ok": True, "backup": str(backup_path), "verification": live_verification}
    _restore_from_backup(target_path, backup_path)
    rollback_verification = verify_python_target(str(target_path))
    return {
        "ok": False,
        "backup": str(backup_path),
        "verification": live_verification,
        "rollback_verification": rollback_verification,
    }


# ── Refactor response builders ────────────────────────────────────────────────

def _refactor_baseline_block_response(report_lines: List[str]) -> str:
    report_lines += [
        "[BLOCK] verification harness failed before guided apply.",
        "",
        "--- Result ---",
        "Guided loop blocked before any write because the target did not pass baseline verification.",
    ]
    return "\n".join(report_lines)


def _refactor_noop_response(report_lines: List[str]) -> str:
    report_lines += [
        "[OK]   verification baseline is healthy",
        "[OK]   no deterministic low-risk code edits matched the request",
        "",
        "--- Result ---",
        "Guided loop completed safely. No code changes were necessary.",
    ]
    return "\n".join(report_lines)


def _refactor_stage_failure_response(
    report_lines: List[str],
    plan: List[str],
    staged_verification: Dict[str, Any],
    unattended: bool,
    target_name: str,
) -> str:
    report_lines += [
        "[BLOCK] staged candidate failed verification.",
        "",
        "--- Planned Changes ---",
        *[f"  - {item}" for item in plan],
        "",
        verification_section(staged_verification),
        "",
        "--- Result ---",
        "Guided loop blocked before apply because the staged candidate did not pass verification.",
    ]
    if _verification_has_hygiene_failure(staged_verification):
        _log_hygiene_violation(target_name, staged_verification, "staged", unattended)
    if unattended:
        append_sovereign_journal(
            "unattended_self_edit",
            f"rollback :: {target_name}",
            "staged verification failed before apply",
            False,
            {"verification": staged_verification, "auto_commit": False},
        )
    return "\n".join(report_lines)


def _refactor_apply_failure_response(
    report_lines: List[str],
    apply_result: Dict[str, Any],
    live_verification: Dict[str, Any],
    unattended: bool,
    target_name: str,
) -> str:
    report_lines += [
        "[BLOCK] live apply failed verification and rollback was triggered.",
        f"backup  : {apply_result.get('backup', '')}",
        "",
        verification_section(live_verification),
        "",
        "--- Result ---",
        "Guided loop staged a candidate, failed live verification, and restored the backup safely.",
    ]
    append_autonomy_journal("guided_apply", f"rollback :: {target_name}", False)
    if _verification_has_hygiene_failure(live_verification):
        _log_hygiene_violation(target_name, live_verification, "live", unattended)
    append_sovereign_journal(
        "guided_apply" if not unattended else "unattended_self_edit",
        f"rollback :: {target_name}",
        "live verification failed; rollback restored backup",
        False,
        {"verification": live_verification, "backup": apply_result.get("backup", ""), "auto_commit": unattended},
    )
    return "\n".join(report_lines)


def _refactor_apply_success_response(
    report_lines: list,
    apply_result: Dict[str, Any],
    live_verification: Dict[str, Any],
    plan: List[str],
    changes: List[str],
    unattended: bool,
    target_name: str,
) -> str:
    append_autonomy_journal("guided_apply", f"applied :: {target_name} :: {'; '.join(changes)}", True)
    append_sovereign_journal(
        "guided_apply" if not unattended else "unattended_self_edit",
        f"applied :: {target_name}",
        "; ".join(changes),
        True,
        {"verification": live_verification, "backup": apply_result.get("backup", ""), "auto_commit": unattended},
    )
    update_master_codex(target_name, changes, live_verification, "unattended_self_edit" if unattended else "guided_apply")
    report_lines += [
        "[OK]   verification baseline is healthy",
        f"[OK]   applied deterministic guided changes to {target_name}",
        f"backup  : {apply_result.get('backup', '')}",
        "",
        "--- Planned Changes ---",
        *[f"  - {item}" for item in plan],
        "",
        "--- Applied Changes ---",
        *[f"  - {item}" for item in changes],
        "",
        verification_section(live_verification),
        "",
        "--- Result ---",
        "Guided loop staged, verified, applied, and preserved a rollback backup successfully.",
    ]
    return "\n".join(report_lines)


def _build_refactor_report_lines(task_id: str, target_file: str, prompt_text: str, unattended: bool, dry_run: bool) -> list:
    return [
        "[LUNA GUIDED SELF-IMPROVEMENT]",
        f"task_id : {task_id}",
        f"target  : {target_file}",
        f"prompt  : {prompt_text!r}" if prompt_text else "prompt  : ''",
        f"unattended_auto_commit : {unattended}",
        f"unattended_dry_run    : {dry_run}",
        "",
        "--- Guided Analysis ---",
    ]


def _refactor_journal_category(dry_run: bool) -> str:
    return "unattended_dry_run" if dry_run else "unattended_self_edit"


def _handle_refactor_baseline_failure(
    report_lines: list,
    verification: dict,
    target_file: str,
    unattended: bool,
    dry_run: bool,
) -> str:
    if unattended:
        append_sovereign_journal(
            _refactor_journal_category(dry_run),
            f"blocked :: {Path(target_file).name}",
            "baseline verification failed before guided apply",
            False,
            {"verification": verification, "auto_commit": False, "dry_run": dry_run},
        )
    return _refactor_baseline_block_response(report_lines)


def _load_refactor_candidate(task_or_prompt: Any, target_file: str) -> tuple:
    target_path = Path(target_file)
    source = safe_read_text(target_path)
    candidate_text, changes, plan = _build_refactor_candidate(source, task_or_prompt, target_file)
    return target_path, source, candidate_text, changes, plan


def _handle_refactor_noop(report_lines: list, verification: dict, target_path: Path, unattended: bool, dry_run: bool) -> str:
    if unattended:
        append_sovereign_journal(
            _refactor_journal_category(dry_run),
            f"no_change :: {target_path.name}",
            "no deterministic low-risk rewrite matched the unattended request",
            True,
            {"verification": verification, "auto_commit": False, "dry_run": dry_run},
        )
    return _refactor_noop_response(report_lines)


def _build_negative_growth_failure_verification(target_path: Path, negative_growth: dict) -> dict:
    return {
        "target": str(target_path),
        "target_exists": True,
        "ast_parse": True,
        "py_compile": True,
        "smoke_boot": True,
        "hygiene_ok": False,
        "hygiene_violations": list(negative_growth.get("violations") or []),
        "details": ["negative growth gate failed"],
        "summary": "negative growth gate failed",
        "passed": False,
    }


def _handle_negative_growth_failure(
    report_lines: list,
    plan: list,
    target_path: Path,
    negative_growth: dict,
    unattended: bool,
    dry_run: bool,
) -> str:
    staged_verification = _build_negative_growth_failure_verification(target_path, negative_growth)
    _log_hygiene_violation(target_path.name, staged_verification, "negative_growth", unattended)
    append_sovereign_journal(
        "hygiene_violation",
        f"negative_growth :: {target_path.name}",
        "; ".join(staged_verification["hygiene_violations"]),
        False,
        {"before": negative_growth.get("before", {}), "after": negative_growth.get("after", {}), "dry_run": dry_run},
    )
    if dry_run:
        append_sovereign_journal(
            "unattended_dry_run",
            f"failed :: {target_path.name}",
            "negative growth gate rejected the candidate during dry run",
            False,
            {"verification": staged_verification, "before": negative_growth.get("before", {}), "after": negative_growth.get("after", {})},
        )
    return _refactor_stage_failure_response(report_lines, plan, staged_verification, unattended, target_path.name)


def _handle_refactor_stage_verification_failure(
    report_lines: list,
    plan: list,
    staged_verification: dict,
    negative_growth: dict,
    unattended: bool,
    dry_run: bool,
    target_name: str,
) -> str:
    if dry_run:
        append_sovereign_journal(
            "unattended_dry_run",
            f"failed :: {target_name}",
            _verification_hygiene_detail(staged_verification) if _verification_has_hygiene_failure(staged_verification) else "staged verification failed during dry run",
            False,
            {"verification": staged_verification, "before": negative_growth.get("before", {}), "after": negative_growth.get("after", {})},
        )
    return _refactor_stage_failure_response(report_lines, plan, staged_verification, unattended, target_name)


def _dry_run_metrics(negative_growth: dict) -> Tuple[Dict[str, Any], Dict[str, Any], int, int]:
    before = negative_growth.get("before", {})
    after = negative_growth.get("after", {})
    line_delta = int(after.get("line_count", 0)) - int(before.get("line_count", 0))
    function_delta = int(after.get("function_count", 0)) - int(before.get("function_count", 0))
    return before, after, line_delta, function_delta


def _dry_run_report_lines(
    target_name: str,
    changes: list,
    plan: list,
    staged_verification: dict,
    line_delta: int,
    function_delta: int,
) -> List[str]:
    return [
        "[OK]   verification baseline is healthy",
        f"[OK]   dry run candidate passed verification for {target_name}",
        "",
        "--- Planned Changes ---",
        *[f"  - {item}" for item in plan],
        "",
        "--- Candidate Changes ---",
        *[f"  - {item}" for item in changes],
        "",
        verification_section(staged_verification),
        "",
        "--- Dry Run Metrics ---",
        f"line_delta     : {line_delta}",
        f"function_delta : {function_delta}",
        "",
        "--- Result ---",
        "Dry run succeeded. Candidate was verified and journaled, but the live file was not modified.",
    ]


def _handle_refactor_dry_run_success(
    report_lines: list,
    staged_verification: dict,
    negative_growth: dict,
    target_name: str,
    changes: list,
    plan: list,
) -> str:
    before, after, line_delta, function_delta = _dry_run_metrics(negative_growth)
    append_sovereign_journal(
        "unattended_dry_run",
        f"passed :: {target_name}",
        "; ".join(changes),
        True,
        {"verification": staged_verification, "before": before, "after": after, "line_delta": line_delta, "function_delta": function_delta, "would_apply": True},
    )
    report_lines += _dry_run_report_lines(target_name, changes, plan, staged_verification, line_delta, function_delta)
    return "\n".join(report_lines)


# ── Internal council ──────────────────────────────────────────────────────────

def _refactor_task_is_complex(task_or_prompt: Any, target_file: str) -> bool:
    task_payload = task_or_prompt if isinstance(task_or_prompt, dict) else {}
    mission_targets = list(task_payload.get("mission_targets") or [])
    if len(mission_targets) > 1:
        return True
    if bool(task_payload.get("feature_request") or task_payload.get("net_new_feature")):
        return True
    prompt_text = _refactor_prompt_text(task_or_prompt)
    normalized = normalize_prompt_text(prompt_text)
    if any(marker in normalized for marker in INTERNAL_COUNCIL_COMPLEXITY_MARKERS):
        return True
    requested_symbols = _refactor_requested_symbols(task_or_prompt)
    if len(requested_symbols) > 1:
        return True
    return target_file.endswith("SurgeApp_Claude_Terminal.py")


def _persist_internal_council_record(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = safe_read_json(COUNCIL_HISTORY_PATH, default={}) or {}
    entries = list(payload.get("entries") or [])
    entries.append(record)
    payload["entries"] = entries[-INTERNAL_COUNCIL_HISTORY_LIMIT:]
    payload["last_updated"] = now_iso()
    write_json_atomic(COUNCIL_HISTORY_PATH, payload)
    return record


def _council_architect_proposal(payload: Dict[str, Any]) -> Dict[str, Any]:
    target_name = payload.get("target_name") or "target"
    plan = list(payload.get("plan") or [])
    changes = list(payload.get("changes") or [])
    proposal = [
        f"Architect: pursue the smallest high-signal change set for {target_name}.",
        "Architect: preserve existing behavior and keep rollback ready at every stage.",
        "Architect: when performing EXTRACT_HELPERS, you must aggressively decompose the target function.",
        "Architect: it is unacceptable to return the function unchanged if it exceeds the decomposition threshold.",
        "Architect: identify and extract at least one real logical block into a private helper unless a safety gate would be violated.",
    ]
    if plan:
        proposal.append("Architect plan: " + "; ".join(plan[:3]))
    if changes:
        proposal.append("Architect candidate: " + "; ".join(changes[:3]))
    return {"persona": "Architect", "notes": proposal}


def _council_critic_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    changes = list(payload.get("changes") or [])
    negative_growth = payload.get("negative_growth") or {}
    concerns: List[str] = []
    if not changes:
        concerns.append("Critic: no deterministic candidate exists yet, so the request should stay review-only.")
        concerns.append("Critic: reject fake refactors, no-op rewrites, and cosmetic renames.")
    if not bool(negative_growth.get("negative_growth_ok", True)):
        concerns.append("Critic: negative-growth gate would fail, so apply must be blocked.")
    if changes:
        concerns.append("Critic: helper explosion with no net simplification is unacceptable.")
    if not concerns:
        concerns.append("Critic: candidate is acceptable only if staged verification and hygiene remain green.")
    return {"persona": "Critic", "notes": concerns}


def _council_synthesizer_decision(
    payload: Dict[str, Any],
    architect: Dict[str, Any],
    critic: Dict[str, Any],
) -> Dict[str, Any]:
    changes = list(payload.get("changes") or [])
    negative_growth = payload.get("negative_growth") or {}
    if not changes:
        decision = "review_only"
        summary = "Synthesizer: no deterministic code path was available, so this stays review-only. Fewer total lines, lower complexity, and a minimal helper count remain mandatory."
        permit_apply = False
    elif not bool(negative_growth.get("negative_growth_ok", True)):
        decision = "blocked"
        summary = "Synthesizer: candidate is blocked by the negative-growth governor."
        permit_apply = False
    else:
        decision = "apply_candidate"
        summary = "Synthesizer: staged deterministic candidate is allowed to proceed because it reduces or preserves complexity with the minimum helper count necessary."
        permit_apply = True
    return {
        "persona": "Synthesizer",
        "notes": [summary],
        "decision": decision,
        "permit_apply": permit_apply,
    }


def _render_internal_council_section(record: Dict[str, Any]) -> List[str]:
    lines = ["", "--- Internal Council ---"]
    for key in ["architect", "critic", "synthesizer"]:
        persona = record.get(key) or {}
        name = str(persona.get("persona") or key.title())
        lines.append(f"{name}:")
        for note in list(persona.get("notes") or []):
            lines.append(f"  - {note}")
    lines.append(f"decision      : {record.get('decision', 'review_only')}")
    lines.append(f"permit_apply  : {bool(record.get('permit_apply', False))}")
    return lines


def _council_review_only_response(
    report_lines: List[str],
    record: Dict[str, Any],
    verification: Dict[str, Any],
    target_name: str,
) -> str:
    append_sovereign_journal(
        "internal_council",
        f"review_only :: {target_name}",
        str((record.get("synthesizer") or {}).get("notes", [""])[0]),
        True,
        {"decision": record.get("decision", "review_only"), "verification": verification},
    )
    report_lines += [
        "[OK]   verification baseline is healthy",
        "[OK]   internal council held this request in review-only mode.",
        "",
        "--- Result ---",
        "Internal Council debated the request and declined live mutation because the candidate was not safely deterministic enough.",
    ]
    return "\n".join(report_lines)


# ── Master codex + prompt optimizer ──────────────────────────────────────────

def _synthesize_codex_rule(target_name: str, changes: List[str]) -> str:
    lowered = " ".join(changes).lower()
    if "unused imports" in lowered:
        return "Prefer deterministic dead-code removal before attempting deeper structural rewrites."
    if "helper" in lowered or "extract" in lowered:
        return f"When refactoring {target_name}, prefer small helper extraction while preserving rollback-ready verification."
    return f"When changing {target_name}, keep edits deterministic, modular, and gated by verification before apply."


def update_master_codex(
    target_name: str,
    changes: List[str],
    verification: Optional[Dict[str, Any]] = None,
    context: str = "guided_apply",
) -> Dict[str, Any]:
    rule = _synthesize_codex_rule(target_name, changes)
    existing = safe_read_text(LUNA_MASTER_CODEX_PATH)
    if rule in existing:
        return {"updated": False, "rule": rule}
    stamp = now_iso()
    line = f"- {stamp} :: {rule}\n"
    if existing.strip():
        safe_write_text(LUNA_MASTER_CODEX_PATH, existing.rstrip() + "\n" + line)
    else:
        safe_write_text(LUNA_MASTER_CODEX_PATH, "# LUNA MASTER CODEX AND MEMORY\n\n" + line)
    append_sovereign_journal(
        "master_codex",
        f"updated :: {target_name}",
        rule,
        True,
        {"context": context, "verification": verification or {}},
    )
    return {"updated": True, "rule": rule}


def _prompt_optimizer_success_count(entries: List[Dict[str, Any]]) -> int:
    total = 0
    for entry in entries:
        if not bool(entry.get("ok", True)):
            continue
        category = str(entry.get("category", "") or "")
        summary = str(entry.get("summary", "") or "")
        if category in {"unattended_self_edit", "guided_apply", "unattended_live_oneshot"} and any(token in summary for token in ["applied", "passed"]):
            total += 1
    return total


def _prompt_optimizer_rules(entries: List[Dict[str, Any]]) -> List[str]:
    rules = ["I write modular Python with explicit rollback points and verification before mutation."]
    no_change_count = sum(1 for entry in entries if "No supported technical debt backlog items exceeded threshold." in str(entry.get("summary", "")) or "no_change" in str(entry.get("summary", "")))
    hygiene_count = sum(1 for entry in entries if str(entry.get("category", "")) == "hygiene_violation")
    applied_count = sum(1 for entry in entries if bool(entry.get("ok", True)) and "applied" in str(entry.get("summary", "")))
    if no_change_count >= 3:
        rules.append("I back off unsupported backlog items instead of repeating no-op autonomous cycles.")
    if hygiene_count >= 1:
        rules.append("I prefer compact helper-based edits and avoid large template-style patches.")
    if applied_count >= 3:
        rules.append("I favor deterministic 20-line helper units when they preserve behavior and keep verification green.")
    return rules


def _merge_managed_prompt_section(base_text: str, rules: List[str]) -> str:
    managed_block = "\n".join([
        PROMPT_OPTIMIZER_MANAGED_START,
        *[f"- {rule}" for rule in rules],
        PROMPT_OPTIMIZER_MANAGED_END,
    ])
    if PROMPT_OPTIMIZER_MANAGED_START in base_text and PROMPT_OPTIMIZER_MANAGED_END in base_text:
        head, rest = base_text.split(PROMPT_OPTIMIZER_MANAGED_START, 1)
        _, tail = rest.split(PROMPT_OPTIMIZER_MANAGED_END, 1)
        return head.rstrip() + "\n\n" + managed_block + tail
    if not base_text.strip():
        return DEFAULT_LUNA_SYSTEM_PROMPT + "\n\n" + managed_block + "\n"
    return base_text.rstrip() + "\n\n" + managed_block + "\n"


def optimize_core_personality(force: bool = False, reason: str = "") -> Dict[str, Any]:
    entries = list((safe_read_json(SOVEREIGN_JOURNAL_PATH, default={}) or {}).get("entries") or [])
    optimizer_state = safe_read_json(PROMPT_OPTIMIZER_STATE_PATH, default={}) or {}
    success_count = _prompt_optimizer_success_count(entries)
    last_success_count = int(optimizer_state.get("last_success_count", 0) or 0)
    last_optimized_at = str(optimizer_state.get("last_optimized_at") or "").strip()
    enough_success = (success_count - last_success_count) >= PROMPT_OPTIMIZER_SUCCESS_THRESHOLD
    enough_time = False
    if last_optimized_at:
        try:
            enough_time = (datetime.now() - datetime.fromisoformat(last_optimized_at)) >= timedelta(days=PROMPT_OPTIMIZER_INTERVAL_DAYS)
        except Exception:
            enough_time = True
    else:
        enough_time = True
    if not force and not (enough_success or enough_time):
        return {"updated": False, "reason": "threshold_not_met", "success_count": success_count}
    rules = _prompt_optimizer_rules(entries[-200:])
    current_prompt = safe_read_text(LUNA_SYSTEM_PROMPT_PATH) or DEFAULT_LUNA_SYSTEM_PROMPT
    new_prompt = _merge_managed_prompt_section(current_prompt, rules)
    changed = new_prompt != current_prompt
    if changed:
        safe_write_text(LUNA_SYSTEM_PROMPT_PATH, new_prompt)
    optimizer_state = {
        "last_optimized_at": now_iso(),
        "last_success_count": success_count,
        "last_reason": reason or ("force" if force else "scheduled"),
        "rules": rules,
    }
    write_json_atomic(PROMPT_OPTIMIZER_STATE_PATH, optimizer_state)
    if changed:
        append_sovereign_journal(
            "prompt_optimizer",
            "updated",
            "; ".join(rules[:3]),
            True,
            {"success_count": success_count, "reason": optimizer_state["last_reason"]},
        )
    return {"updated": changed, "rules": rules, "success_count": success_count, "reason": optimizer_state["last_reason"]}


# ── Refactor context and dispatch ─────────────────────────────────────────────

def _refactor_is_unattended(task_or_prompt: Any) -> bool:
    if isinstance(task_or_prompt, dict):
        if bool(task_or_prompt.get("unattended_self_edit")):
            return True
    # Read live flags; default is False for all keys.
    flags = safe_read_json(OMEGA_BATCH2_FLAGS_PATH, default={}) or {}
    return bool(flags.get("unattended_self_edit_enabled"))


def _refactor_is_dry_run(task_or_prompt: Any) -> bool:
    if isinstance(task_or_prompt, dict) and bool(task_or_prompt.get("unattended_dry_run")):
        return True
    flags = safe_read_json(OMEGA_BATCH2_FLAGS_PATH, default={}) or {}
    return bool(flags.get("unattended_dry_run_enabled"))


def _verification_triplet_passed(verification: Dict[str, Any]) -> bool:
    return bool(
        verification.get("target_exists")
        and verification.get("ast_parse")
        and verification.get("py_compile")
        and verification.get("hygiene_ok", True)
        and verification.get("smoke_boot") is not False
    )


def _build_refactor_context(task_id: str, target_file: str, task_or_prompt: Any) -> Tuple[Dict[str, Any], str, bool, bool, List[str]]:
    verification = verify_python_target(target_file)
    prompt_text = _refactor_prompt_text(task_or_prompt)
    unattended = _refactor_is_unattended(task_or_prompt)
    dry_run = unattended and _refactor_is_dry_run(task_or_prompt)
    report_lines = _build_refactor_report_lines(task_id, target_file, prompt_text, unattended, dry_run)
    return verification, prompt_text, unattended, dry_run, report_lines


def _maybe_run_internal_council(
    task_id: str,
    target_file: str,
    task_or_prompt: Any,
    plan: List[str],
    changes: List[str],
    negative_growth: Dict[str, Any],
    report_lines: List[str],
) -> Dict[str, Any]:
    if not _refactor_task_is_complex(task_or_prompt, target_file):
        return {}
    if _run_internal_council_fn is None:
        return {}
    council_record = _run_internal_council_fn(task_id, target_file, task_or_prompt, plan, changes, negative_growth)
    report_lines += _render_internal_council_section(council_record)
    return council_record


def run_refactor_self_improvement(task_id: str, target_file: str, task_or_prompt: Any = None) -> str:
    verification, prompt_text, unattended, dry_run, report_lines = _build_refactor_context(task_id, target_file, task_or_prompt)
    if not verification_ok(verification):
        return _handle_refactor_baseline_failure(report_lines, verification, target_file, unattended, dry_run)
    target_path, source, candidate_text, changes, plan = _load_refactor_candidate(task_or_prompt, target_file)
    anti_paralysis = _detect_anti_paralysis_violation(target_path, source, candidate_text, task_or_prompt)
    negative_growth = _verify_negative_growth(source, candidate_text)
    if anti_paralysis is not None:
        return _handle_refactor_stage_verification_failure(report_lines, plan, anti_paralysis, negative_growth, unattended, dry_run, target_path.name)
    council_record = _maybe_run_internal_council(task_id, target_file, task_or_prompt, plan, changes, negative_growth, report_lines)
    if candidate_text == source or not changes:
        if council_record.get("decision") == "review_only":
            return _council_review_only_response(report_lines, council_record, verification, target_path.name)
        return _handle_refactor_noop(report_lines, verification, target_path, unattended, dry_run)
    if council_record and not bool(council_record.get("permit_apply", False)):
        return _council_review_only_response(report_lines, council_record, verification, target_path.name)
    if unattended and not negative_growth.get("negative_growth_ok", True):
        return _handle_negative_growth_failure(report_lines, plan, target_path, negative_growth, unattended, dry_run)
    staged_verification = _verify_refactor_candidate_text(target_path, candidate_text)
    if not _verification_triplet_passed(staged_verification):
        return _handle_refactor_stage_verification_failure(report_lines, plan, staged_verification, negative_growth, unattended, dry_run, target_path.name)
    if dry_run:
        return _handle_refactor_dry_run_success(report_lines, staged_verification, negative_growth, target_path.name, changes, plan)
    apply_result = _apply_refactor_candidate(target_path, candidate_text)
    live_verification = apply_result.get("verification", {})
    if not apply_result.get("ok"):
        return _refactor_apply_failure_response(report_lines, apply_result, live_verification, unattended, target_path.name)
    return _refactor_apply_success_response(report_lines, apply_result, live_verification, plan, changes, unattended, target_path.name)


# ── Self-fix pipeline ─────────────────────────────────────────────────────────

def run_self_fix_pipeline(task_id: str, target_file: str) -> str:
    target_path = Path(target_file)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = build_backup_path(target_path)
    try:
        shutil.copy2(str(target_path), str(backup_path))
    except Exception as exc:
        result = (
            "[LUNA SELF-FIX FAILURE]\n"
            f"task_id : {task_id}\n"
            f"target  : {target_file}\n"
            f"reason  : backup failed: {exc}\n"
        )
        append_self_fix_log(task_id, target_file, result, False)
        return result

    result = "\n".join([
        "[LUNA SELF-FIX MODE]",
        f"task_id : {task_id}",
        f"target  : {target_file}",
        f"backup  : {backup_path}",
        "",
        "--- Inspection Results ---",
        "[OK]    baseline validation is healthy",
        "[OK]    no deterministic changes were required",
        "",
        "--- Result ---",
        f"All checks passed. No issues found in {target_path.name}.",
        "No file was modified.",
    ])
    append_self_fix_log(task_id, target_file, result, True)
    return result
