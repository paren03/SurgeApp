"""Luna Self-Healing Playbook Engine — Phase 5E foundation.

Purpose:
    Match a failure description (free text, log snippet, verifier summary) to
    one or more curated playbooks and return ordered, operator-readable
    recommendations. **The engine never executes a command.** Every
    `safe_first_action` is shown to the operator; nothing happens automatically.

Design:
    - Pure stdlib only.
    - Seed playbooks live in memory/luna_self_healing_playbooks_seed.json
      (tracked) AND a small fallback constant inside this module so the
      engine still works if the file is missing.
    - Validation against memory/luna_self_healing_playbooks.schema.json is
      structural and cheap (the schema file is tracked but the engine does
      not pull in any JSON-Schema dependency; it checks required fields).
    - Optional integrations:
        * luna_modules.luna_memory_index.search_memory(...) for "have we
          seen this before?" — failure-tolerant.
        * luna_modules.luna_change_ledger — schema-only awareness for now.
    - CLI:
        python -m luna_modules.luna_playbook_engine --self-test
        python -m luna_modules.luna_playbook_engine --match "<text>"
        python -m luna_modules.luna_playbook_engine --match "<text>" --limit 3 --format json
        python -m luna_modules.luna_playbook_engine --match "<text>" --write-report

The CLI does not edit runtime files. With --write-report, it writes ONLY
to memory/luna_playbook_match_report.json (which is gitignored).
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1

# Resolve project root from this module's location.
_THIS_FILE = Path(__file__).resolve()
PROJECT_DIR = _THIS_FILE.parent.parent

DEFAULT_SEED_PATH = PROJECT_DIR / "memory" / "luna_self_healing_playbooks_seed.json"
DEFAULT_RUNTIME_JSONL_PATH = PROJECT_DIR / "memory" / "luna_self_healing_playbooks.jsonl"
DEFAULT_REPORT_PATH = PROJECT_DIR / "memory" / "luna_playbook_match_report.json"

ALLOWED_SEVERITIES: Tuple[str, ...] = ("info", "low", "medium", "high", "critical")

REQUIRED_FIELDS: Tuple[str, ...] = (
    "playbook_id",
    "title",
    "failure_class",
    "severity",
    "tags",
    "detection_signals",
    "safe_first_actions",
    "approval_tier_required",
    "source",
    "updated_at",
)

# Token regex used by the matcher. Matches words >=2 chars including digits
# and underscores, so identifiers like 'continues_update' stay together.
_TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9_]{1,}")


# ---------------------------------------------------------------------------
# Tiny fallback seed set (used only if the JSON seed file is missing)
# ---------------------------------------------------------------------------
# This is intentionally minimal. The full 16-playbook curated set lives in
# memory/luna_self_healing_playbooks_seed.json, which is tracked in git.
_FALLBACK_SEED: Tuple[Dict[str, Any], ...] = (
    {
        "playbook_id": "fallback_unknown_failure",
        "title": "Unknown failure (fallback)",
        "failure_class": "unknown",
        "severity": "info",
        "tags": ["fallback"],
        "detection_signals": ["unknown failure"],
        "safe_first_actions": [
            "Capture stderr + stack verbatim.",
            "Run Luna_Post_Repair_Verify.ps1 and record hard failures + warnings.",
            "Open a planned-change ledger row before any edit.",
        ],
        "unsafe_actions_to_avoid": ["Editing core files without an explicit plan."],
        "verification_commands": ["Luna_Post_Repair_Verify.ps1"],
        "rollback_or_safety_rule": "No edits without operator approval.",
        "related_files": [],
        "approval_tier_required": 0,
        "source": "phase5e_module_fallback",
        "updated_at": "2026-05-02",
    },
)


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [t.lower() for t in _TOKEN_RE.findall(str(text))]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_playbook(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Cheap structural validation. Returns (ok, errors)."""
    errors: List[str] = []
    if not isinstance(record, dict):
        return False, ["record is not a dict"]
    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")
            continue
        val = record[field]
        if field == "playbook_id":
            if not isinstance(val, str) or not re.match(r"^[a-z0-9_]+$", val):
                errors.append("playbook_id must be lowercase snake_case")
        elif field in ("title", "failure_class", "source", "updated_at"):
            if not isinstance(val, str) or not val.strip():
                errors.append(f"{field} must be a non-empty string")
        elif field == "severity":
            if val not in ALLOWED_SEVERITIES:
                errors.append(
                    f"severity must be one of {ALLOWED_SEVERITIES}, got {val!r}"
                )
        elif field in ("tags", "detection_signals", "safe_first_actions"):
            if not isinstance(val, list):
                errors.append(f"{field} must be a list")
            elif field in ("safe_first_actions",) and not val:
                errors.append("safe_first_actions must not be empty")
            elif not all(isinstance(x, str) for x in val):
                errors.append(f"{field} must contain strings only")
        elif field == "approval_tier_required":
            if not isinstance(val, int) or not (0 <= val <= 5):
                errors.append("approval_tier_required must be int in [0..5]")
    # Optional fields type checks
    for opt_list in ("likely_causes", "unsafe_actions_to_avoid",
                     "verification_commands", "related_files"):
        if opt_list in record and not isinstance(record[opt_list], list):
            errors.append(f"{opt_list} must be a list")
    if "rollback_or_safety_rule" in record and not isinstance(
            record["rollback_or_safety_rule"], str):
        errors.append("rollback_or_safety_rule must be a string")
    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_playbooks(seed_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load playbooks from the curated seed file; fall back to module constant.

    Returns {"playbooks": [...], "source": "<seed_path>|fallback",
             "validation": [{"playbook_id": "...", "ok": bool, "errors": [...]}, ...]}
    """
    target = Path(seed_path) if seed_path is not None else DEFAULT_SEED_PATH
    raw = _read_json(target)
    used_fallback = False
    if isinstance(raw, dict) and isinstance(raw.get("playbooks"), list) and raw["playbooks"]:
        playbooks = list(raw["playbooks"])
        source = str(target)
    else:
        playbooks = list(_FALLBACK_SEED)
        source = "module_fallback"
        used_fallback = True
    validation: List[Dict[str, Any]] = []
    valid_only: List[Dict[str, Any]] = []
    for p in playbooks:
        ok, errs = validate_playbook(p)
        validation.append({
            "playbook_id": p.get("playbook_id") or "(no id)",
            "ok": ok,
            "errors": errs,
        })
        if ok:
            valid_only.append(p)
    return {
        "playbooks": valid_only,
        "source": source,
        "used_fallback": used_fallback,
        "validation": validation,
    }


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _haystack_for_playbook(p: Dict[str, Any]) -> str:
    parts: List[str] = [
        str(p.get("playbook_id") or ""),
        str(p.get("title") or ""),
        str(p.get("failure_class") or ""),
        " ".join(p.get("tags") or []),
        " ".join(p.get("detection_signals") or []),
    ]
    return " ".join(parts).lower()


def score_playbook(text: str,
                   playbook: Dict[str, Any]) -> Tuple[int, Dict[str, int]]:
    """Score one playbook against `text`.

    Score components (additive):
      +3 per substring detection_signal that occurs (case-insensitive).
      +2 per tag found as a token in `text`.
      +1 per token from playbook id/title/failure_class found in `text`.

    Returns (score, breakdown).
    """
    if not text or not isinstance(playbook, dict):
        return 0, {}
    low = text.lower()
    text_tokens = set(_tokenize(text))

    breakdown: Dict[str, int] = {"signal": 0, "tag": 0, "token": 0}

    for sig in playbook.get("detection_signals") or []:
        if not isinstance(sig, str) or not sig.strip():
            continue
        if sig.lower() in low:
            breakdown["signal"] += 3

    for tag in playbook.get("tags") or []:
        if not isinstance(tag, str) or not tag.strip():
            continue
        if tag.lower() in text_tokens:
            breakdown["tag"] += 2

    seen = set()
    for chunk in (playbook.get("playbook_id"), playbook.get("title"),
                  playbook.get("failure_class")):
        for tok in _tokenize(chunk or ""):
            if tok in seen:
                continue
            seen.add(tok)
            if tok in text_tokens:
                breakdown["token"] += 1

    return breakdown["signal"] + breakdown["tag"] + breakdown["token"], breakdown


def match_playbooks(text: str,
                    seed_path: Optional[Path] = None,
                    limit: int = 5) -> List[Dict[str, Any]]:
    """Score every loaded playbook against `text` and return top-K.

    Only playbooks with score > 0 are returned. If no playbook scored,
    returns the fallback "unknown_failure" entry from the loaded set if
    present (so the operator still gets a generic safe action), else [].
    """
    loaded = load_playbooks(seed_path=seed_path)
    playbooks = loaded["playbooks"]
    scored: List[Dict[str, Any]] = []
    for p in playbooks:
        s, breakdown = score_playbook(text, p)
        if s > 0:
            scored.append({"playbook": p, "score": s, "breakdown": breakdown})
    scored.sort(key=lambda r: (-r["score"], r["playbook"].get("playbook_id", "")))
    if scored:
        return scored[: max(0, int(limit))]
    # No matches — fall back to a generic 'unknown' playbook if one exists
    for p in playbooks:
        if p.get("failure_class") == "unknown" or "fallback" in (p.get("tags") or []):
            return [{"playbook": p, "score": 0,
                     "breakdown": {"signal": 0, "tag": 0, "token": 0}}]
    return []


# ---------------------------------------------------------------------------
# Optional Phase 5D / 5C integrations — failure-tolerant
# ---------------------------------------------------------------------------

def _maybe_search_memory_index(query: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Best-effort lookup against the Phase 5D memory index. Never raises."""
    if not query:
        return []
    try:
        from luna_modules.luna_memory_index import search_memory  # type: ignore
    except Exception:
        return []
    try:
        return list(search_memory(query=query, limit=int(limit)) or [])
    except Exception:
        return []


def _maybe_change_ledger_schema_known() -> bool:
    """Return True iff the Phase 5C change-ledger module is importable."""
    try:
        import luna_modules.luna_change_ledger  # type: ignore  # noqa: F401
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def render_match_report(matches: List[Dict[str, Any]],
                        query: str = "",
                        memory_hits: Optional[List[Dict[str, Any]]] = None,
                        out_format: str = "markdown") -> str:
    """Render an operator-friendly report. out_format: 'markdown' or 'json'."""
    out_format = (out_format or "markdown").lower()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "query": str(query or ""),
        "match_count": len(matches or []),
        "matches": [],
        "memory_hits": list(memory_hits or []),
        "change_ledger_available": _maybe_change_ledger_schema_known(),
    }
    for m in matches or []:
        p = m.get("playbook") or {}
        payload["matches"].append({
            "playbook_id": p.get("playbook_id"),
            "title": p.get("title"),
            "failure_class": p.get("failure_class"),
            "severity": p.get("severity"),
            "approval_tier_required": p.get("approval_tier_required"),
            "tags": p.get("tags") or [],
            "score": m.get("score", 0),
            "breakdown": m.get("breakdown") or {},
            "safe_first_actions": p.get("safe_first_actions") or [],
            "unsafe_actions_to_avoid": p.get("unsafe_actions_to_avoid") or [],
            "verification_commands": p.get("verification_commands") or [],
            "rollback_or_safety_rule": p.get("rollback_or_safety_rule") or "",
            "related_files": p.get("related_files") or [],
        })
    if out_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    # Markdown
    lines: List[str] = []
    lines.append(f"# Luna Self-Healing Playbook Match Report")
    lines.append("")
    lines.append(f"- Generated: `{payload['generated_at']}`")
    if query:
        lines.append(f"- Query: `{query}`")
    lines.append(f"- Matches: {payload['match_count']}")
    lines.append(f"- Change-ledger available: {payload['change_ledger_available']}")
    lines.append("")
    if not matches:
        lines.append("_No playbook matched the supplied text._")
        return "\n".join(lines)
    for m in matches:
        p = m.get("playbook") or {}
        lines.append(f"## {p.get('title')}  ({p.get('playbook_id')})")
        lines.append(
            f"- failure_class: `{p.get('failure_class')}` · "
            f"severity: **{p.get('severity')}** · "
            f"approval_tier_required: **{p.get('approval_tier_required')}** · "
            f"score: **{m.get('score', 0)}** "
            f"(signal+{m.get('breakdown', {}).get('signal', 0)}, "
            f"tag+{m.get('breakdown', {}).get('tag', 0)}, "
            f"token+{m.get('breakdown', {}).get('token', 0)})"
        )
        if p.get("tags"):
            lines.append(f"- tags: {', '.join(p['tags'])}")
        if p.get("related_files"):
            lines.append(f"- related files: {', '.join(p['related_files'])}")
        lines.append("")
        lines.append("**Safe first actions (require operator approval to act):**")
        for s in p.get("safe_first_actions") or []:
            lines.append(f"  - {s}")
        if p.get("unsafe_actions_to_avoid"):
            lines.append("")
            lines.append("**Unsafe actions to avoid:**")
            for s in p["unsafe_actions_to_avoid"]:
                lines.append(f"  - {s}")
        if p.get("verification_commands"):
            lines.append("")
            lines.append("**Verification commands:**")
            for s in p["verification_commands"]:
                lines.append(f"  - `{s}`")
        if p.get("rollback_or_safety_rule"):
            lines.append("")
            lines.append(f"**Rollback / safety rule:** {p['rollback_or_safety_rule']}")
        lines.append("")
    if memory_hits:
        lines.append("---")
        lines.append("")
        lines.append("## Related memory recall (Phase 5D)")
        for h in memory_hits[:5]:
            sid = h.get("summary_id") or h.get("_index", "?")
            src = h.get("source_path") or h.get("src") or ""
            lines.append(f"- `{sid}`  source=`{src}`")
    return "\n".join(lines)


def write_report(report_text: str, path: Optional[Path] = None) -> str:
    """Write the report under memory/. Returns the resolved path."""
    target = Path(path) if path is not None else DEFAULT_REPORT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(report_text, encoding="utf-8")
    return str(target)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def self_test() -> int:
    """In-memory smoke test. Loads seed, validates, runs four spec queries.

    Exits 0 on success.
    """
    loaded = load_playbooks()
    if not loaded["playbooks"]:
        print("[FAIL] no playbooks loaded")
        return 1
    bad = [v for v in loaded["validation"] if not v["ok"]]
    if bad:
        print(f"[FAIL] {len(bad)} invalid playbooks: {bad[:3]}")
        return 2
    queries = [
        ("worker import failed ImportError hygiene", "worker_import_failure"),
        ("CU_START CU_STOP fake busy loop", "cu_fake_busy_loop"),
        ("aider context limit exceeded", "aider_context_overflow"),
        ("ollama connection refused /api/tags", "ollama_unavailable"),
    ]
    fails: List[str] = []
    for q, expected_id in queries:
        matches = match_playbooks(q, limit=3)
        if not matches:
            fails.append(f"no match for {q!r}")
            continue
        top_id = matches[0]["playbook"].get("playbook_id")
        if top_id != expected_id:
            fails.append(f"{q!r}: expected {expected_id} got {top_id}")
    summary = {
        "ok": not fails,
        "playbook_count": len(loaded["playbooks"]),
        "used_fallback": loaded["used_fallback"],
        "queries_tested": len(queries),
        "failures": fails,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not fails else 3


def _cli(argv: List[str]) -> int:
    args = list(argv or [])
    if "--self-test" in args:
        return self_test()

    if "--match" in args:
        try:
            qi = args.index("--match")
            query = args[qi + 1] if qi + 1 < len(args) else ""
        except Exception:
            query = ""
        if not query:
            print("usage: --match \"<text>\" [--limit N] [--format markdown|json] [--write-report]")
            return 4
        limit = 5
        if "--limit" in args:
            try:
                li = args.index("--limit")
                limit = int(args[li + 1])
            except Exception:
                limit = 5
        out_format = "markdown"
        if "--format" in args:
            try:
                fi = args.index("--format")
                out_format = str(args[fi + 1] or "markdown").lower()
            except Exception:
                out_format = "markdown"
        memory_hits = _maybe_search_memory_index(query, limit=3)
        matches = match_playbooks(query, limit=limit)
        report = render_match_report(matches, query=query,
                                     memory_hits=memory_hits,
                                     out_format=out_format)
        print(report)
        if "--write-report" in args:
            path = write_report(
                render_match_report(matches, query=query,
                                    memory_hits=memory_hits,
                                    out_format="json"),
                path=DEFAULT_REPORT_PATH,
            )
            print(f"\n# wrote JSON report to: {path}")
        if not matches:
            return 5
        return 0

    print("luna_playbook_engine — pass --self-test or --match \"<text>\"")
    return 0


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
