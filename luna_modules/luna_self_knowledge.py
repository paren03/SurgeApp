"""
luna_self_knowledge.py — Luna Self-Knowledge Engine

Provides precise file/function targeting so aider receives 50-200 line
excerpts instead of 4000-11000 line files, preventing timeout failures.

All functions use Python stdlib only. No print() statements.
"""

import ast
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SKIP_DIRS = {
    ".git", ".aider_venv", "__pycache__", "backups",
    "node_modules", "dist", "build",
}

INCLUDE_EXTENSIONS = {
    ".py", ".bat", ".vbs", ".ps1", ".md", ".txt",
    ".json", ".jsonl", ".yaml", ".yml",
}

HIGH_RISK_STEMS = {
    "worker", "SurgeApp_Claude_Terminal", "aider_bridge",
    "luna_guardian", "director_agent", "LaunchLuna", "luna_start",
}

PURPOSE_HINTS = {
    "worker":                    "main task orchestrator",
    "aider_bridge":              "aider subprocess manager",
    "luna_guardian":             "watchdog service",
    "director_agent":            "director / orchestration agent",
    "SurgeApp_Claude_Terminal":  "hybrid Ollama/Claude agentic terminal",
    "LaunchLuna":                "system launcher / entry point",
    "luna_start":                "system launcher / entry point",
    "luna_apprentice":           "apprentice learning agent",
    "luna_evolution_patch":      "evolution patch manager",
    "learning_engine":           "learning engine",
    "hyper_loop":                "hyper loop scheduler",
    "fractal_harness":           "fractal task harness",
    "boot_doctor":               "boot repair utility",
    "luna_paths":                "path constants module",
    "luna_logging":              "logging utilities",
    "luna_live_feed":            "live feed event emitter",
    "luna_heartbeat":            "heartbeat monitor",
    "luna_tasks":                "task queue manager",
    "luna_routing":              "command routing layer",
    "luna_memory_router":        "memory routing layer",
    "luna_self_repair_engine":   "self-repair engine",
    "luna_self_teacher":         "self-teaching / prompt learning",
    "luna_verification":         "output verification layer",
    "luna_two_pass_review":      "two-pass code review",
    "luna_code_reader":          "code reader / chunker",
    "luna_environment":          "environment detection",
    "luna_approvals":            "approval gate handler",
    "luna_autonomy_control":     "autonomy control layer",
    "luna_hygiene":              "code hygiene checker",
    "luna_refactor":             "refactor helper",
    "luna_tools":                "tool registry",
    "luna_io":                   "I/O utilities",
    "luna_architect":            "architect agent",
    "luna_goal_tracker":         "goal tracking",
    "luna_loop_detector":        "loop / cycle detector",
    "luna_conversation_log":     "conversation logger",
    "luna_missions":             "mission management",
    "luna_self_knowledge":       "self-knowledge / symbol index engine",
}

COMMAND_ROUTE_PATTERNS = [
    "/ceo", "/aider", "/selfupgrade", "continues_update", "/autonomystatus",
    "/watchdog", "luna_live_feed", "aider_jobs", "director_jobs",
    "tasks/active", "LUNA_STOP_NOW", "OLLAMA_API_BASE", "APPLY_ON_PASS", "/selfmap",
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_truncated(file_path: Path) -> str:
    """SHA-256 over first 1 MB of file content."""
    h = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(1_048_576)
            h.update(chunk)
    except Exception:
        return ""
    return h.hexdigest()


def _derive_language(suffix: str) -> str:
    mapping = {
        ".py": "python", ".bat": "batch", ".vbs": "vbscript",
        ".ps1": "powershell", ".md": "markdown", ".txt": "text",
        ".json": "json", ".jsonl": "jsonl", ".yaml": "yaml", ".yml": "yaml",
    }
    return mapping.get(suffix.lower(), "unknown")


def _derive_purpose_hint(stem: str) -> str:
    # Exact match first
    if stem in PURPOSE_HINTS:
        return PURPOSE_HINTS[stem]
    # Partial match
    for key, hint in PURPOSE_HINTS.items():
        if key in stem:
            return hint
    return ""


def _derive_risk_level(stem: str) -> str:
    return "high" if stem in HIGH_RISK_STEMS else "normal"


def _append_live_feed(project_dir: Path, event: str, msg: str) -> None:
    """Append one event line to logs/luna_live_feed.jsonl, silently ignoring errors."""
    try:
        feed_path = project_dir / "logs" / "luna_live_feed.jsonl"
        record = {
            "ts": _now_iso(),
            "event": event,
            "role": "self_knowledge",
            "source": "luna_self_knowledge",
            "msg": msg,
        }
        with open(feed_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def _load_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# build_file_index
# ---------------------------------------------------------------------------

def build_file_index(project_dir: Path) -> dict:
    """
    Walk project_dir recursively and build a metadata index of all relevant files.
    Returns dict with keys: generated_at, project_dir, file_count, files (list).
    """
    project_dir = Path(project_dir).resolve()
    files = []

    for root, dirs, filenames in os.walk(project_dir):
        # Prune skipped directories in-place
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        for fname in filenames:
            fpath = Path(root) / fname
            suffix = fpath.suffix.lower()
            if suffix not in INCLUDE_EXTENSIONS:
                continue

            try:
                stat = fpath.stat()
            except Exception:
                continue

            size_bytes = stat.st_size
            modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
            huge = size_bytes > 1_048_576  # > 1 MB

            stem = fpath.stem
            sha = _sha256_truncated(fpath) if not huge else ""

            entry = {
                "path": str(fpath),
                "relative_path": str(fpath.relative_to(project_dir)),
                "suffix": suffix,
                "size_bytes": size_bytes,
                "modified_at": modified_at,
                "sha256": sha,
                "language": _derive_language(suffix),
                "risk_level": _derive_risk_level(stem),
                "function_scope_required": size_bytes > 250_000,
                "purpose_hint": _derive_purpose_hint(stem),
                "huge": huge,
            }
            files.append(entry)

    return {
        "generated_at": _now_iso(),
        "project_dir": str(project_dir),
        "file_count": len(files),
        "files": files,
    }


# ---------------------------------------------------------------------------
# build_symbol_index
# ---------------------------------------------------------------------------

def _collect_symbols(
    tree: ast.AST,
    file_path: str,
    relative_path: str,
    source_lines: list,
    risk_level: str,
    parent_class: str = "",
) -> list:
    """Recursively extract symbols from an AST node."""
    results = []

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            sym_type = "class"
            qname = f"{parent_class}.{node.name}" if parent_class else node.name
            end_line = getattr(node, "end_lineno", node.lineno)
            doc = ast.get_docstring(node) or ""
            decorators = [
                (d.id if isinstance(d, ast.Name) else
                 (d.attr if isinstance(d, ast.Attribute) else str(d)))
                for d in node.decorator_list
            ]
            results.append({
                "file_path": file_path,
                "relative_path": relative_path,
                "qualified_name": qname,
                "symbol_type": sym_type,
                "start_line": node.lineno,
                "end_line": end_line,
                "line_count": end_line - node.lineno + 1,
                "docstring_summary": doc[:120],
                "decorators": decorators,
                "parent": parent_class,
                "risk_level": risk_level,
            })
            # Recurse into class body for methods
            results.extend(
                _collect_symbols(node, file_path, relative_path, source_lines, risk_level, parent_class=node.name)
            )

        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if parent_class:
                sym_type = "method"
                qname = f"{parent_class}.{node.name}"
            else:
                sym_type = "async_function" if isinstance(node, ast.AsyncFunctionDef) else "function"
                qname = node.name

            end_line = getattr(node, "end_lineno", node.lineno)
            doc = ast.get_docstring(node) or ""
            decorators = [
                (d.id if isinstance(d, ast.Name) else
                 (d.attr if isinstance(d, ast.Attribute) else str(d)))
                for d in node.decorator_list
            ]
            results.append({
                "file_path": file_path,
                "relative_path": relative_path,
                "qualified_name": qname,
                "symbol_type": sym_type,
                "start_line": node.lineno,
                "end_line": end_line,
                "line_count": end_line - node.lineno + 1,
                "docstring_summary": doc[:120],
                "decorators": decorators,
                "parent": parent_class,
                "risk_level": risk_level,
            })
            # Recurse for nested functions
            results.extend(
                _collect_symbols(node, file_path, relative_path, source_lines, risk_level, parent_class=parent_class)
            )

    return results


def build_symbol_index(project_dir: Path, file_index: Optional[dict] = None) -> dict:
    """
    Parse all .py files and extract class/function symbols with line ranges.
    Returns dict with keys: generated_at, symbol_count, symbols (list).
    """
    project_dir = Path(project_dir).resolve()

    if file_index is None:
        file_index = build_file_index(project_dir)

    symbols = []
    errors = []

    for entry in file_index.get("files", []):
        if entry.get("suffix") != ".py":
            continue
        fpath = Path(entry["path"])
        if entry.get("huge"):
            # Still attempt parse for large files — this is the point of the index
            pass

        try:
            source = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            errors.append({"file": entry["path"], "error": str(exc)})
            continue

        try:
            tree = ast.parse(source, filename=str(fpath))
        except SyntaxError as exc:
            errors.append({"file": entry["path"], "error": f"SyntaxError: {exc}"})
            continue
        except Exception as exc:
            errors.append({"file": entry["path"], "error": str(exc)})
            continue

        source_lines = source.splitlines()
        file_symbols = _collect_symbols(
            tree,
            file_path=entry["path"],
            relative_path=entry["relative_path"],
            source_lines=source_lines,
            risk_level=entry.get("risk_level", "normal"),
        )
        symbols.extend(file_symbols)

    return {
        "generated_at": _now_iso(),
        "symbol_count": len(symbols),
        "parse_errors": errors,
        "symbols": symbols,
    }


# ---------------------------------------------------------------------------
# build_command_route_index
# ---------------------------------------------------------------------------

def build_command_route_index(project_dir: Path, file_index: Optional[dict] = None) -> dict:
    """
    Search all .py files for known command route patterns using plain line scan.
    Returns dict with keys: generated_at, routes (list).
    """
    project_dir = Path(project_dir).resolve()

    if file_index is None:
        file_index = build_file_index(project_dir)

    routes = []

    for entry in file_index.get("files", []):
        if entry.get("suffix") != ".py":
            continue
        if entry.get("huge"):
            continue  # Skip files > 1MB for route scan to keep it fast

        fpath = Path(entry["path"])
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except Exception:
            continue

        stem = fpath.stem
        for lineno, line in enumerate(lines, start=1):
            stripped = line.strip()
            for pattern in COMMAND_ROUTE_PATTERNS:
                if pattern in line:
                    routes.append({
                        "route": pattern,
                        "file_path": entry["path"],
                        "line_number": lineno,
                        "context": stripped[:200],
                        "likely_owner_module": stem,
                    })

    return {
        "generated_at": _now_iso(),
        "route_count": len(routes),
        "routes": routes,
    }


# ---------------------------------------------------------------------------
# build_system_map
# ---------------------------------------------------------------------------

def build_system_map(project_dir: Path) -> dict:
    """
    Return a hardcoded-but-verified system map of Luna's core paths and services.
    Each entry includes an 'exists' boolean.
    """
    project_dir = Path(project_dir).resolve()

    def entry(rel: str) -> dict:
        p = project_dir / rel
        return {"path": str(p), "relative_path": rel, "exists": p.exists()}

    system_map = {
        "generated_at": _now_iso(),
        "project_dir": str(project_dir),

        "core_services": {
            "worker":    {**entry("worker.py"),                      "role": "main task orchestrator"},
            "aider_bridge": {**entry("aider_bridge.py"),             "role": "aider subprocess manager"},
            "guardian":  {**entry("luna_guardian.py"),               "role": "watchdog service"},
            "terminal":  {**entry("SurgeApp_Claude_Terminal.py"),     "role": "hybrid Ollama/Claude terminal"},
            "launcher":  {**entry("LaunchLuna.pyw"),                 "role": "system entry point"},
        },

        "queue_paths": {
            "tasks_active":        entry("tasks/active"),
            "tasks_done":          entry("tasks/done"),
            "tasks_failed":        entry("tasks/failed"),
            "aider_jobs_active":   entry("aider_jobs/active"),
            "aider_jobs_done":     entry("aider_jobs/done"),
            "aider_jobs_failed":   entry("aider_jobs/failed"),
            "solutions":           entry("solutions"),
            "director_jobs":       entry("director_jobs"),
        },

        "log_paths": {
            "luna_worker_log":       entry("logs/luna_worker.log"),
            "luna_guardian_log":     entry("logs/luna_guardian.log"),
            "luna_live_feed":        entry("logs/luna_live_feed.jsonl"),
            "luna_worker_heartbeat": entry("logs/luna_worker_heartbeat.json"),
        },

        "memory_paths": {
            "continues_update_state": entry("memory/continues_update_state.json"),
            "cu_loop_lock":           entry("memory/cu_loop.lock.json"),
            "continues_update_stop":  entry("memory/continues_update.stop"),
            "file_index":             entry("memory/file_index.json"),
            "symbol_index":           entry("memory/symbol_index.json"),
        },

        "lock_paths": {
            "cu_loop_lock":        entry("memory/cu_loop.lock.json"),
            "luna_worker_lock":    entry("logs/luna_worker.lock.json"),
            "aider_bridge_pid":    entry("logs/aider_bridge.pid"),
            "luna_guardian_lock":  entry("memory/luna_guardian.lock.json"),
        },

        "startup_files": {
            "LaunchLuna":  {**entry("LaunchLuna.pyw"), "note": "manual review required before editing"},
            "luna_start":  {**entry("luna_start.pyw"), "note": "manual review required before editing"},
        },

        "risk_files": {
            "worker":                   entry("worker.py"),
            "SurgeApp_Claude_Terminal": entry("SurgeApp_Claude_Terminal.py"),
            "aider_bridge":             entry("aider_bridge.py"),
            "luna_guardian":            entry("luna_guardian.py"),
        },

        "suggested_edit_modes": {
            "small_file":          "direct staged edit okay",
            "large_high_risk":     "function-scoped edit only",
            "startup":             "manual review required",
            "memory_log_jsonl":    "append-only unless explicitly approved",
        },
    }

    return system_map


# ---------------------------------------------------------------------------
# refresh_self_knowledge
# ---------------------------------------------------------------------------

def refresh_self_knowledge(project_dir: Path) -> dict:
    """
    Rebuild all indexes and write them to memory/.
    Returns summary dict: {ok, file_count, symbol_count, route_count, duration_seconds, generated_at}.
    """
    project_dir = Path(project_dir).resolve()
    t_start = datetime.now(timezone.utc)
    generated_at = t_start.isoformat()

    _append_live_feed(project_dir, "SELF_KNOWLEDGE_REFRESH_START",
                      f"Starting self-knowledge refresh for {project_dir}")

    try:
        # --- File index ---
        file_data = build_file_index(project_dir)
        _write_json(project_dir / "memory" / "file_index.json", file_data)
        _append_live_feed(project_dir, "SELF_KNOWLEDGE_FILE_INDEX_DONE",
                          f"File index built: {file_data['file_count']} files")

        # --- Symbol index (reuse file_data) ---
        sym_data = build_symbol_index(project_dir, file_index=file_data)
        _write_json(project_dir / "memory" / "symbol_index.json", sym_data)
        _append_live_feed(project_dir, "SELF_KNOWLEDGE_SYMBOL_INDEX_DONE",
                          f"Symbol index built: {sym_data['symbol_count']} symbols")

        # --- Command route index (reuse file_data) ---
        route_data = build_command_route_index(project_dir, file_index=file_data)
        _write_json(project_dir / "memory" / "command_route_index.json", route_data)
        _append_live_feed(project_dir, "SELF_KNOWLEDGE_ROUTE_INDEX_DONE",
                          f"Route index built: {route_data['route_count']} routes")

        # --- System map ---
        sysmap_data = build_system_map(project_dir)
        _write_json(project_dir / "memory" / "system_map.json", sysmap_data)

        # --- Summary markdown ---
        summary_md = _build_summary_md(file_data, sym_data, route_data, sysmap_data)
        summary_path = project_dir / "memory" / "luna_self_knowledge_summary.md"
        summary_path.write_text(summary_md, encoding="utf-8")

        t_end = datetime.now(timezone.utc)
        duration = (t_end - t_start).total_seconds()

        result = {
            "ok": True,
            "file_count": file_data["file_count"],
            "symbol_count": sym_data["symbol_count"],
            "route_count": route_data["route_count"],
            "duration_seconds": duration,
            "generated_at": generated_at,
        }

        _append_live_feed(project_dir, "SELF_KNOWLEDGE_REFRESH_DONE",
                          f"Refresh complete in {duration:.1f}s — "
                          f"{file_data['file_count']} files, {sym_data['symbol_count']} symbols, "
                          f"{route_data['route_count']} routes")

        return result

    except Exception as exc:
        _append_live_feed(project_dir, "SELF_KNOWLEDGE_REFRESH_FAILED",
                          f"Refresh failed: {exc}")
        return {
            "ok": False,
            "error": str(exc),
            "file_count": 0,
            "symbol_count": 0,
            "route_count": 0,
            "duration_seconds": 0.0,
            "generated_at": generated_at,
        }


# ---------------------------------------------------------------------------
# find_targets
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> list:
    """Lowercase and split on spaces, underscores, dots, slashes, dashes."""
    return [t for t in re.split(r"[\s_./\-\\]+", text.lower()) if t]


def find_targets(query: str, project_dir: Path, limit: int = 10) -> list:
    """
    Score symbols and command routes against a query, return top `limit` results.
    Auto-refreshes indexes if they don't exist.
    """
    project_dir = Path(project_dir).resolve()
    sym_path = project_dir / "memory" / "symbol_index.json"
    route_path = project_dir / "memory" / "command_route_index.json"
    file_path = project_dir / "memory" / "file_index.json"

    if not sym_path.exists() or not route_path.exists():
        refresh_self_knowledge(project_dir)

    sym_data = _load_json(sym_path) or {"symbols": []}
    route_data = _load_json(route_path) or {"routes": []}
    file_data = _load_json(file_path) or {"files": []}

    query_tokens = _tokenize(query)
    results = []

    # Score symbols
    for sym in sym_data.get("symbols", []):
        score = 0
        qn = sym.get("qualified_name", "")
        ds = sym.get("docstring_summary", "")
        sym_file = Path(sym.get("file_path", "")).name

        for tok in query_tokens:
            if tok in _tokenize(qn):
                score += 30
            if tok in _tokenize(ds):
                score += 20
        # Exact file name match
        for tok in query_tokens:
            if tok in sym_file.lower():
                score += 40
        # Exact symbol name match (the stem of qualified_name)
        sym_name = qn.split(".")[-1]
        if sym_name.lower() in [t.lower() for t in query_tokens]:
            score += 50

        if score > 0:
            results.append({
                "score": score,
                "kind": "symbol",
                "file_path": sym.get("file_path", ""),
                "symbol": qn,
                "start_line": sym.get("start_line"),
                "end_line": sym.get("end_line"),
                "reason": f"symbol match (score {score})",
            })

    # Score command routes
    for route in route_data.get("routes", []):
        score = 0
        route_str = route.get("route", "")
        ctx = route.get("context", "")
        route_file = Path(route.get("file_path", "")).name

        for tok in query_tokens:
            if tok in _tokenize(route_str):
                score += 25
            if tok in _tokenize(ctx):
                score += 15
            if tok in route_file.lower():
                score += 40

        if score > 0:
            results.append({
                "score": score,
                "kind": "route",
                "file_path": route.get("file_path", ""),
                "symbol": route_str,
                "start_line": route.get("line_number"),
                "end_line": route.get("line_number"),
                "reason": f"route match (score {score}): {ctx[:80]}",
            })

    # Score files directly
    for entry in file_data.get("files", []):
        score = 0
        fname = Path(entry.get("path", "")).name
        stem = Path(entry.get("path", "")).stem
        hint = entry.get("purpose_hint", "")

        for tok in query_tokens:
            if tok in fname.lower():
                score += 40
            if tok in _tokenize(hint):
                score += 20
            if tok == stem.lower():
                score += 50

        if score > 0:
            results.append({
                "score": score,
                "kind": "file",
                "file_path": entry.get("path", ""),
                "symbol": None,
                "start_line": 1,
                "end_line": None,
                "reason": f"file match (score {score}): {hint}",
            })

    # Sort and deduplicate by (file_path, start_line)
    results.sort(key=lambda x: x["score"], reverse=True)
    seen = set()
    deduped = []
    for r in results:
        key = (r["file_path"], r.get("start_line"), r.get("symbol"))
        if key not in seen:
            seen.add(key)
            deduped.append(r)
        if len(deduped) >= limit:
            break

    return deduped


# ---------------------------------------------------------------------------
# get_symbol_slice
# ---------------------------------------------------------------------------

def get_symbol_slice(
    file_path: str,
    symbol_name: Optional[str] = None,
    start_line: Optional[int] = None,
    end_line: Optional[int] = None,
) -> dict:
    """
    Return a slice of source code for a named symbol or explicit line range.
    Reads only the required lines — does not load the whole file for huge files.
    """
    fpath = Path(file_path)
    if not fpath.exists():
        return {"error": f"file not found: {file_path}", "file_path": file_path}

    language = _derive_language(fpath.suffix)

    # If symbol_name given, look up in symbol_index
    if symbol_name is not None:
        # Search all known project dirs (try parent of file_path going up)
        found_start = None
        found_end = None
        search_dir = fpath.parent
        for _ in range(5):  # Walk up at most 5 levels
            idx_path = search_dir / "memory" / "symbol_index.json"
            if idx_path.exists():
                sym_data = _load_json(idx_path) or {}
                for sym in sym_data.get("symbols", []):
                    # Match by qualified_name or just the last part
                    qn = sym.get("qualified_name", "")
                    stem = qn.split(".")[-1]
                    if (qn == symbol_name or stem == symbol_name) and sym.get("file_path") == str(fpath):
                        found_start = sym["start_line"]
                        found_end = sym["end_line"]
                        break
                if found_start is not None:
                    break
            parent = search_dir.parent
            if parent == search_dir:
                break
            search_dir = parent

        if found_start is None:
            # Fallback: try ast parse on the file itself
            try:
                source = fpath.read_text(encoding="utf-8", errors="replace")
                tree = ast.parse(source)
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                        if node.name == symbol_name or node.name == symbol_name.split(".")[-1]:
                            found_start = node.lineno
                            found_end = getattr(node, "end_lineno", node.lineno)
                            break
            except Exception:
                pass

        if found_start is None:
            return {
                "error": f"symbol not found: {symbol_name}",
                "file_path": file_path,
                "symbol_name": symbol_name,
            }
        start_line = found_start
        end_line = found_end

    if start_line is None:
        return {"error": "must provide symbol_name or start_line", "file_path": file_path}

    if end_line is None:
        end_line = start_line + 200  # default 200-line window

    # Read only required lines (memory efficient for huge files)
    lines_out = []
    try:
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                if lineno < start_line:
                    continue
                if lineno > end_line:
                    break
                lines_out.append(line)
    except Exception as exc:
        return {"error": str(exc), "file_path": file_path}

    source_code = "".join(lines_out)
    actual_end = start_line + len(lines_out) - 1

    return {
        "file_path": str(fpath),
        "symbol_name": symbol_name,
        "start_line": start_line,
        "end_line": actual_end,
        "line_count": len(lines_out),
        "source_code": source_code,
        "language": language,
    }


# ---------------------------------------------------------------------------
# record_patch_attempt
# ---------------------------------------------------------------------------

def record_patch_attempt(project_dir: Path, payload: dict) -> dict:
    """
    Append one patch attempt record to memory/patch_ledger.jsonl.
    Payload must include: task_id, file, symbol, prompt_family, status,
    diff_present (bool), error_reason, lesson_learned.
    """
    project_dir = Path(project_dir).resolve()
    ledger_path = project_dir / "memory" / "patch_ledger.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)

    record = dict(payload)
    record["ts"] = _now_iso()

    with open(ledger_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")

    return {"ok": True, "ledger_path": str(ledger_path)}


# ---------------------------------------------------------------------------
# summarize_self_knowledge
# ---------------------------------------------------------------------------

def _build_summary_md(
    file_data: dict,
    sym_data: dict,
    route_data: dict,
    sysmap_data: dict,
) -> str:
    lines = ["# Luna Self-Knowledge Summary", ""]
    lines.append(f"**Last refresh:** {file_data.get('generated_at', 'unknown')}")
    lines.append("")

    # Totals
    lines.append(f"## Totals")
    lines.append(f"- Files indexed: {file_data.get('file_count', 0)}")
    lines.append(f"- Symbols indexed: {sym_data.get('symbol_count', 0)}")
    lines.append(f"- Command routes found: {route_data.get('route_count', 0)}")
    lines.append(f"- Parse errors: {len(sym_data.get('parse_errors', []))}")
    lines.append("")

    # High-risk files
    high_risk = [f for f in file_data.get("files", []) if f.get("risk_level") == "high"]
    lines.append("## High-Risk Files (function-scope edits only)")
    for f in high_risk:
        size_kb = f.get("size_bytes", 0) // 1024
        lines.append(f"- `{f['relative_path']}` — {size_kb} KB — {f.get('purpose_hint', '')}")
    lines.append("")

    # Files requiring function-scope editing
    huge = [f for f in file_data.get("files", []) if f.get("function_scope_required")]
    lines.append("## Files Requiring Function-Scope Editing (> 250 KB)")
    for f in huge:
        size_kb = f.get("size_bytes", 0) // 1024
        lines.append(f"- `{f['relative_path']}` — {size_kb} KB")
    lines.append("")

    # Top 10 command routes
    lines.append("## Top 10 Command Routes")
    routes = route_data.get("routes", [])
    for i, r in enumerate(routes[:10], 1):
        lines.append(f"{i}. `{r['route']}` in `{Path(r['file_path']).name}` line {r['line_number']}")
        lines.append(f"   > {r['context'][:100]}")
    lines.append("")

    # Safe improvement suggestions (small, low-risk .py files)
    candidates = [
        f for f in file_data.get("files", [])
        if f.get("suffix") == ".py"
        and f.get("risk_level") == "normal"
        and not f.get("function_scope_required")
        and f.get("size_bytes", 0) > 0
    ]
    candidates.sort(key=lambda x: x.get("size_bytes", 0))
    lines.append("## Recommended Safe Improvement Targets (small, low-risk)")
    for f in candidates[:3]:
        size_kb = f.get("size_bytes", 0) // 1024
        hint = f.get("purpose_hint", "")
        lines.append(f"- `{f['relative_path']}` — {size_kb} KB — {hint}")
    lines.append("")

    lines.append("---")
    lines.append("*Generated by luna_self_knowledge.py*")
    return "\n".join(lines)


def summarize_self_knowledge(project_dir: Path) -> str:
    """
    Load all index files and return (and write) a markdown summary.
    """
    project_dir = Path(project_dir).resolve()

    file_data = _load_json(project_dir / "memory" / "file_index.json") or {"files": [], "file_count": 0, "generated_at": "N/A"}
    sym_data = _load_json(project_dir / "memory" / "symbol_index.json") or {"symbols": [], "symbol_count": 0, "parse_errors": []}
    route_data = _load_json(project_dir / "memory" / "command_route_index.json") or {"routes": [], "route_count": 0}
    sysmap_data = _load_json(project_dir / "memory" / "system_map.json") or {}

    md = _build_summary_md(file_data, sym_data, route_data, sysmap_data)
    summary_path = project_dir / "memory" / "luna_self_knowledge_summary.md"
    try:
        summary_path.write_text(md, encoding="utf-8")
    except Exception:
        pass
    return md


# ---------------------------------------------------------------------------
# Phase 5B: curated file self-map (read-only, stdlib only)
#
# Extends the existing file/symbol index with risk zones, module roles,
# safe / forbidden edit zones, and a small keyword search. Outputs four
# JSON files under memory/. None of these are read by runtime services
# yet; they are foundation data for later Phase 5 stages.
# ---------------------------------------------------------------------------

# Project root inferred from this module's location: luna_modules/<self>.py
_PHASE5B_PROJECT_DIR = Path(__file__).resolve().parent.parent

# Optional: PROJECT_DIR alias used by the prompt's signature defaults
PROJECT_DIR = _PHASE5B_PROJECT_DIR

# Output paths under memory/
_FILE_MAP_PATH       = _PHASE5B_PROJECT_DIR / "memory" / "luna_file_map.json"
_FUNCTION_INDEX_PATH = _PHASE5B_PROJECT_DIR / "memory" / "luna_function_index.json"
_MODULE_ROLES_PATH   = _PHASE5B_PROJECT_DIR / "memory" / "luna_module_roles.json"
_RISK_ZONES_PATH     = _PHASE5B_PROJECT_DIR / "memory" / "luna_risk_zones.json"

# Curated set of files we care about. Includes the seven core runtime files,
# the most-load-bearing luna_modules, plus all tests.
_CURATED_CORE_FILES = (
    "worker.py",
    "aider_bridge.py",
    "luna_guardian.py",
    "LaunchLuna.pyw",
    "luna_start.pyw",
    "SurgeApp_Claude_Terminal.py",
    "director_agent.py",
)
_CURATED_LUNA_MODULES = (
    "luna_modules/__init__.py",
    "luna_modules/luna_hygiene.py",
    "luna_modules/luna_paths.py",
    "luna_modules/luna_routing.py",
    "luna_modules/luna_state.py",
    "luna_modules/luna_io.py",
    "luna_modules/luna_logging.py",
    "luna_modules/luna_heartbeat.py",
    "luna_modules/luna_live_feed.py",
    "luna_modules/luna_environment.py",
    "luna_modules/luna_approvals.py",
    "luna_modules/luna_autonomy_control.py",
    "luna_modules/luna_continues_update_policy.py",
    "luna_modules/luna_aider_result_policy.py",
    "luna_modules/luna_queue_governor.py",
    "luna_modules/luna_loop_detector.py",
    "luna_modules/luna_qa_verifier.py",
    "luna_modules/luna_failure_doctor.py",
    "luna_modules/luna_verification.py",
    "luna_modules/luna_self_knowledge.py",
    "luna_modules/luna_self_teacher.py",
    "luna_modules/luna_self_repair_engine.py",
    "luna_modules/luna_refactor.py",
    "luna_modules/luna_architect.py",
    "luna_modules/luna_memory_router.py",
    "luna_modules/luna_tasks.py",
    "luna_modules/luna_two_pass_review.py",
    "luna_modules/luna_tools.py",
    "luna_modules/luna_code_reader.py",
    "luna_modules/luna_inspector_autonomy_feed.py",
    "luna_modules/luna_mission_engine.py",
    "luna_modules/luna_missions.py",
    "luna_modules/luna_goal_tracker.py",
    "luna_modules/luna_metacog.py",
    "luna_modules/luna_conversation_log.py",
    "luna_modules/luna_tool_registry.py",
    "luna_modules/luna_toolchain.py",
    "luna_modules/luna_tier_auditor.py",
    "luna_modules/luna_tier2_planner.py",
    "luna_modules/luna_tier3_memory.py",
    "luna_modules/luna_tier4_orchestrator.py",
    "luna_modules/luna_tier5_selfmodel.py",
    "luna_modules/luna_tier6_codegen.py",
    "luna_modules/luna_tier7_multiagent.py",
    "luna_modules/luna_tier8_initiative_manager.py",
    "luna_modules/luna_tier9_external_learning.py",
)

# Module role mapping. Stem (filename without extension) → role tag.
# Used by infer_module_role().
_MODULE_ROLE_BY_STEM = {
    "worker":                       "main_orchestrator",
    "aider_bridge":                 "aider_bridge",
    "luna_guardian":                "guardian",
    "SurgeApp_Claude_Terminal":     "ui_terminal",
    "LaunchLuna":                   "launcher",
    "luna_start":                   "tray",
    "director_agent":               "director",
    "luna_hygiene":                 "hygiene",
    "luna_paths":                   "routing",
    "luna_routing":                 "routing",
    "luna_state":                   "memory",
    "luna_io":                      "memory",
    "luna_logging":                 "logging",
    "luna_live_feed":               "logging",
    "luna_heartbeat":               "memory",
    "luna_environment":             "config",
    "luna_self_knowledge":          "memory",
    "luna_self_teacher":            "memory",
    "luna_self_repair_engine":      "verification",
    "luna_qa_verifier":             "verification",
    "luna_verification":            "verification",
    "luna_two_pass_review":         "verification",
    "luna_failure_doctor":          "verification",
    "luna_approvals":               "verification",
    "luna_autonomy_control":        "verification",
    "luna_queue_governor":          "verification",
    "luna_loop_detector":           "verification",
    "luna_continues_update_policy": "verification",
    "luna_aider_result_policy":     "verification",
    "luna_memory_router":           "memory",
    "luna_tasks":                   "memory",
    "luna_tools":                   "config",
    "luna_tool_registry":           "config",
    "luna_toolchain":               "config",
    "luna_code_reader":             "memory",
    "luna_refactor":                "verification",
    "luna_architect":               "verification",
    "luna_inspector_autonomy_feed": "ui_terminal",
    "luna_mission_engine":          "director",
    "luna_missions":                "director",
    "luna_goal_tracker":            "memory",
    "luna_metacog":                 "memory",
    "luna_conversation_log":        "logging",
    "luna_tier_auditor":            "verification",
}

# Hard-coded forbidden / safe edit zones for the most load-bearing files.
# zone = (start_line, end_line, label). Lines are inclusive; -1 for end_line
# means "to end of file". Lines are 1-indexed.
_HARDCODED_RISK_ZONES = {
    "worker.py": {
        "risk_level": "critical",
        "forbidden_edit_zones": [
            (1, 250, "imports_and_startup_critical_globals"),
            (10000, 10300, "continues_update_path_constants_and_state_io"),
        ],
        "safe_edit_zones": [
            # The CU compute helper is large but bounded; small additive edits
            # to logging / messages here are allowed under planned-change ledger.
            (10180, 10250, "_cu_compute_ui_status (additive only)"),
        ],
        "high_risk_zones": [
            (11318, 12200, "continues_update_loop_and_wrapper"),
        ],
    },
    "aider_bridge.py": {
        "risk_level": "high",
        "forbidden_edit_zones": [
            (1, 200, "module_constants_paths_and_globals"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [
            (200, 1700, "subprocess_orchestration_and_quarantine"),
        ],
    },
    "luna_guardian.py": {
        "risk_level": "high",
        "forbidden_edit_zones": [
            (1, 100, "module_constants_paths_locks"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [
            (100, 700, "process_detection_and_restart_budget"),
        ],
    },
    "LaunchLuna.pyw": {
        "risk_level": "high",
        "forbidden_edit_zones": [],
        "safe_edit_zones": [],
        "high_risk_zones": [
            (340, 460, "_cu_startup_gate_priority_order"),
            (520, 580, "service_startup_sequence"),
        ],
    },
    "SurgeApp_Claude_Terminal.py": {
        "risk_level": "high",
        "forbidden_edit_zones": [],
        "safe_edit_zones": [
            (788, 980, "qss_theme_constants (cosmetic, additive ok)"),
        ],
        "high_risk_zones": [
            (3700, 4100, "tick_heartbeat_and_status_rendering"),
        ],
    },
    "luna_start.pyw": {
        "risk_level": "medium",
        "forbidden_edit_zones": [],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
    "director_agent.py": {
        "risk_level": "medium",
        "forbidden_edit_zones": [],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
    "luna_modules/luna_hygiene.py": {
        "risk_level": "critical",
        "forbidden_edit_zones": [
            (1, -1, "hygiene_export_contract_load_bearing_for_worker_import"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
    "luna_modules/luna_paths.py": {
        "risk_level": "critical",
        "forbidden_edit_zones": [
            (1, -1, "path_constants_consumed_by_many_modules"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
    "luna_modules/luna_routing.py": {
        "risk_level": "critical",
        "forbidden_edit_zones": [
            (1, -1, "command_routing_dispatch_table"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
    "luna_modules/luna_state.py": {
        "risk_level": "critical",
        "forbidden_edit_zones": [
            (1, -1, "state_globals_consumed_by_orchestrator"),
        ],
        "safe_edit_zones": [],
        "high_risk_zones": [],
    },
}


def _phase5b_resolve(project_dir) -> Path:
    """Accept str or Path; return resolved Path. Defaults to project root."""
    if project_dir is None:
        return _PHASE5B_PROJECT_DIR
    return Path(project_dir).resolve()


def _phase5b_atomic_write_json(path: Path, data: dict) -> None:
    """Atomic write: tmp + rename. JSON is sort_keys=True, indent=2."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = json.dumps(data, sort_keys=True, indent=2, ensure_ascii=True)
    tmp.write_text(payload, encoding="utf-8")
    try:
        tmp.replace(path)
    except Exception:
        # On Windows, replace can race with antivirus or readers — fall back
        # to a normal write if needed; the resulting file is still valid JSON.
        try:
            path.write_text(payload, encoding="utf-8")
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass


def _phase5b_iter_curated(project_dir: Path):
    """Yield (relative_path, absolute_path) for every curated file that exists."""
    for rel in tuple(_CURATED_CORE_FILES) + tuple(_CURATED_LUNA_MODULES):
        ap = project_dir / rel
        if ap.is_file():
            yield rel, ap
    # Tests dir — include every .py test file
    tests_dir = project_dir / "tests"
    if tests_dir.is_dir():
        for ap in sorted(tests_dir.glob("test_*.py")):
            try:
                rel = str(ap.relative_to(project_dir)).replace("\\", "/")
            except Exception:
                rel = ap.name
            yield rel, ap


def infer_module_role(relative_path: str, source_text: str = "") -> str:
    """Return a coarse role tag for a relative path.

    Categories: main_orchestrator, aider_bridge, guardian, ui_terminal,
    launcher, tray, director, memory, verification, routing, hygiene,
    logging, config, tests, docs, runtime_log, unknown.
    """
    rel = (relative_path or "").replace("\\", "/")
    stem = Path(rel).stem
    # Tests
    if rel.startswith("tests/") or stem.startswith("test_"):
        return "tests"
    # Runtime data
    if rel.startswith("logs/") or rel.startswith("memory/"):
        if rel.endswith(".log") or rel.endswith(".jsonl"):
            return "runtime_log"
        return "memory"
    # Documentation
    if rel.endswith(".md") or rel.endswith(".txt"):
        return "docs"
    # Stem mapping
    if stem in _MODULE_ROLE_BY_STEM:
        return _MODULE_ROLE_BY_STEM[stem]
    # Heuristics on path
    low = rel.lower()
    if "guardian" in low:
        return "guardian"
    if "bridge" in low:
        return "aider_bridge"
    if "terminal" in low or "ui" in low:
        return "ui_terminal"
    if "router" in low or "routing" in low:
        return "routing"
    if "hygiene" in low:
        return "hygiene"
    if "verif" in low or "review" in low or "audit" in low or "approval" in low:
        return "verification"
    if "log" in low or "feed" in low:
        return "logging"
    return "unknown"


def compute_risk_zones(relative_path, functions=None) -> dict:
    """Return {risk_level, safe_edit_zones, forbidden_edit_zones, high_risk_zones}.

    Hard-coded zones for the most load-bearing files always win. Unknown
    files get inferred risk from their stem and module role.
    """
    rel = (relative_path or "").replace("\\", "/")
    if rel in _HARDCODED_RISK_ZONES:
        z = dict(_HARDCODED_RISK_ZONES[rel])
        # Convert tuples to lists for JSON-friendly output
        z["safe_edit_zones"] = [list(t) for t in z.get("safe_edit_zones", [])]
        z["forbidden_edit_zones"] = [list(t) for t in z.get("forbidden_edit_zones", [])]
        z["high_risk_zones"] = [list(t) for t in z.get("high_risk_zones", [])]
        return z

    stem = Path(rel).stem
    role = infer_module_role(rel)
    # Default risk by role
    risk = "low"
    if role == "tests":
        risk = "low"
    elif role in ("verification", "memory", "logging", "config", "routing"):
        risk = "medium"
    elif role in ("director",):
        risk = "medium"
    elif role in ("ui_terminal", "guardian", "aider_bridge", "launcher",
                  "tray", "main_orchestrator", "hygiene"):
        risk = "high"
    if stem in HIGH_RISK_STEMS:
        risk = "high"
    return {
        "risk_level": risk,
        "safe_edit_zones": [],
        "forbidden_edit_zones": [],
        "high_risk_zones": [],
    }


def _phase5b_python_symbols(source_text: str):
    """Return list of {name, kind, parent, start_line, end_line} via ast.

    On parse failure, return [] and a parse_error string.
    """
    try:
        tree = ast.parse(source_text)
    except Exception as exc:
        return [], f"parse_error: {exc.__class__.__name__}: {exc}"
    out = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            out.append({
                "name": node.name,
                "kind": "function",
                "parent": "",
                "start_line": int(getattr(node, "lineno", 0) or 0),
                "end_line": int(getattr(node, "end_lineno", 0) or 0),
            })
        elif isinstance(node, ast.ClassDef):
            out.append({
                "name": node.name,
                "kind": "class",
                "parent": "",
                "start_line": int(getattr(node, "lineno", 0) or 0),
                "end_line": int(getattr(node, "end_lineno", 0) or 0),
            })
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef) or isinstance(sub, ast.AsyncFunctionDef):
                    out.append({
                        "name": sub.name,
                        "kind": "method",
                        "parent": node.name,
                        "start_line": int(getattr(sub, "lineno", 0) or 0),
                        "end_line": int(getattr(sub, "end_lineno", 0) or 0),
                    })
    return out, ""


def build_curated_file_map(project_dir=None) -> dict:
    """Curated map of important files. See module docstring."""
    project_dir = _phase5b_resolve(project_dir)
    files = []
    parse_errors = []
    for rel, ap in _phase5b_iter_curated(project_dir):
        try:
            stat = ap.stat()
        except Exception:
            continue
        suffix = ap.suffix.lower()
        purpose = _derive_purpose_hint(ap.stem)
        role = infer_module_role(rel)
        risk = compute_risk_zones(rel)
        # Symbols summary (count only here; full list goes to function index)
        sym_count = 0
        if suffix == ".py" or suffix == ".pyw":
            try:
                txt = ap.read_text(encoding="utf-8", errors="replace")
                syms, err = _phase5b_python_symbols(txt)
                sym_count = len(syms)
                if err:
                    parse_errors.append({"path": rel, "error": err})
            except Exception as exc:
                parse_errors.append({"path": rel, "error": str(exc)[:200]})
        files.append({
            "relative_path": rel,
            "purpose_hint": purpose,
            "role": role,
            "size_bytes": int(stat.st_size),
            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "extension": suffix,
            "sha256": _sha256_truncated(ap),
            "risk_level": risk["risk_level"],
            "safe_edit_zones": risk["safe_edit_zones"],
            "forbidden_edit_zones": risk["forbidden_edit_zones"],
            "high_risk_zones": risk["high_risk_zones"],
            "symbol_count": sym_count,
        })
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "project_dir": str(project_dir),
        "file_count": len(files),
        "parse_errors": parse_errors,
        "files": sorted(files, key=lambda f: f["relative_path"]),
    }


def build_function_index(project_dir=None) -> dict:
    """Function/class index across curated python files."""
    project_dir = _phase5b_resolve(project_dir)
    rows = []
    parse_errors = []
    for rel, ap in _phase5b_iter_curated(project_dir):
        if ap.suffix.lower() not in (".py", ".pyw"):
            continue
        try:
            txt = ap.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            parse_errors.append({"path": rel, "error": str(exc)[:200]})
            continue
        syms, err = _phase5b_python_symbols(txt)
        if err:
            parse_errors.append({"path": rel, "error": err})
            continue
        risk = compute_risk_zones(rel)
        file_risk = risk["risk_level"]
        for s in syms:
            # Default each symbol's risk to its file's risk; future phases
            # may downgrade purely-additive helpers. Method risk inherits class.
            rows.append({
                "name": s["name"],
                "kind": s["kind"],
                "parent": s["parent"],
                "file": rel,
                "start_line": s["start_line"],
                "end_line": s["end_line"],
                "risk_level": file_risk,
            })
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "project_dir": str(project_dir),
        "symbol_count": len(rows),
        "parse_errors": parse_errors,
        "symbols": sorted(rows, key=lambda r: (r["file"], r["start_line"], r["name"])),
    }


def _build_module_roles(file_map: dict) -> dict:
    roles = {}
    for f in file_map.get("files", []):
        roles[f["relative_path"]] = f.get("role") or "unknown"
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "module_count": len(roles),
        "roles": dict(sorted(roles.items())),
    }


def _build_risk_zones(file_map: dict) -> dict:
    zones = {}
    for f in file_map.get("files", []):
        zones[f["relative_path"]] = {
            "risk_level": f.get("risk_level"),
            "safe_edit_zones": f.get("safe_edit_zones") or [],
            "forbidden_edit_zones": f.get("forbidden_edit_zones") or [],
            "high_risk_zones": f.get("high_risk_zones") or [],
        }
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "file_count": len(zones),
        "zones": dict(sorted(zones.items())),
    }


def refresh_curated_self_map(project_dir=None, write: bool = True) -> dict:
    """Rebuild the curated file map, function index, module roles, and risk zones.

    When write=True, writes the four JSON outputs atomically. Returns a small
    summary dict with paths, counts, and an `ok` boolean.
    """
    project_dir = _phase5b_resolve(project_dir)
    file_map = build_curated_file_map(project_dir)
    function_index = build_function_index(project_dir)
    module_roles = _build_module_roles(file_map)
    risk_zones = _build_risk_zones(file_map)
    written = []
    if write:
        for path, data in (
            (_FILE_MAP_PATH, file_map),
            (_FUNCTION_INDEX_PATH, function_index),
            (_MODULE_ROLES_PATH, module_roles),
            (_RISK_ZONES_PATH, risk_zones),
        ):
            _phase5b_atomic_write_json(path, data)
            written.append(str(path))
    return {
        "ok": True,
        "generated_at": _now_iso(),
        "project_dir": str(project_dir),
        "file_count": file_map.get("file_count", 0),
        "symbol_count": function_index.get("symbol_count", 0),
        "parse_errors": (
            list(file_map.get("parse_errors", []))
            + list(function_index.get("parse_errors", []))
        ),
        "written": written,
    }


def answer_self_map_query(query: str, project_dir=None, limit: int = 10) -> list:
    """Tiny local keyword search over the curated file map and function index.

    Splits the query on whitespace, lowercases, and scores files / symbols by
    how many tokens they contain. Returns the top `limit` matches as a list
    of small dicts. Loads existing JSON outputs if present; if missing,
    rebuilds them in-memory (does not write).
    """
    project_dir = _phase5b_resolve(project_dir)
    file_map = _load_json(_FILE_MAP_PATH)
    function_index = _load_json(_FUNCTION_INDEX_PATH)
    if not isinstance(file_map, dict) or "files" not in file_map:
        file_map = build_curated_file_map(project_dir)
    if not isinstance(function_index, dict) or "symbols" not in function_index:
        function_index = build_function_index(project_dir)

    tokens = [t for t in re.split(r"[^a-zA-Z0-9_]+", str(query or "").lower()) if t]
    if not tokens:
        return []

    def _score_text(text: str) -> int:
        low = (text or "").lower()
        return sum(1 for t in tokens if t and t in low)

    matches = []
    # File-level matches
    for f in file_map.get("files", []):
        haystack = " ".join([
            f.get("relative_path", ""),
            f.get("purpose_hint", ""),
            f.get("role", ""),
        ])
        score = _score_text(haystack)
        if score:
            matches.append({
                "kind": "file",
                "score": score,
                "path": f.get("relative_path", ""),
                "purpose": f.get("purpose_hint", ""),
                "role": f.get("role", ""),
                "risk_level": f.get("risk_level", ""),
                "line_range": [1, -1],
            })
    # Symbol-level matches
    for s in function_index.get("symbols", []):
        haystack = " ".join([
            s.get("name", ""),
            s.get("file", ""),
            s.get("parent", ""),
            s.get("kind", ""),
        ])
        score = _score_text(haystack)
        if score:
            matches.append({
                "kind": s.get("kind", "symbol"),
                "score": score,
                "name": s.get("name", ""),
                "path": s.get("file", ""),
                "parent": s.get("parent", ""),
                "line_range": [s.get("start_line", 0), s.get("end_line", 0)],
                "risk_level": s.get("risk_level", ""),
            })
    matches.sort(key=lambda m: (-m["score"], m.get("path", ""), m.get("name", "")))
    return matches[: max(0, int(limit))]
