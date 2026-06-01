"""Phase 38 - Commit Safety Audit.

Classify untracked / staged paths against commit-safe vs forbidden
patterns. Operator-facing only — does NOT run git commands.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase38.commit_safety_audit.v1"


_SAFE_SOURCE_PATTERNS = (
    r"^bilingual_(voice|safety|prosody|segment)_.*\.py$",
    r"^test_phase[0-9]+_.*\.py$",
    r"^test_(dual|vocabulary|russian)_.*\.py$",
)


_SAFE_REPORT_PATTERNS = (
    r"^PHASE[0-9]+_.*\.md$",
)


_SAFE_FOLDER_PATTERNS = (
    r"^bilingual_stack/voice_adapter_phase[0-9]+/[^/]+/?$",
    r"^bilingual_stack/voice_adapter_phase[0-9]+/?$",
    r"^bilingual_stack/governance_phase[0-9]+/[^/]+/?$",
    r"^bilingual_stack/governance_phase[0-9]+/?$",
    r"^bilingual_stack/voice_adapter_phase36/"
    r"local_secret_handoff/\.gitignore$",
)


_FORBIDDEN_FILE_PATTERNS = (
    r"\.sqlite$", r"\.sqlite3$", r"\.db$",
    r"\.wav$", r"\.mp3$", r"\.ogg$", r"\.flac$", r"\.m4a$",
)


_FORBIDDEN_PATH_TOKENS = (
    "/local_secret_handoff/",
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/",
)


_SKIP_RUNTIME_PATTERNS = (
    r"^\.claude/", r"^ruvector\.db$",
    r"^lexicon/luna_vocabulary\.sqlite$",
    r"^russian_stack/russian_(lexicon|memory)\.sqlite$",
    r"^bilingual_stack/bilingual_links\.sqlite$",
    r"^corpus_sources/(backups|checkpoints|"
    r"quality_samples|synthetic_million|pilot_imports)/",
    r"^corpus_sources/phase20/ledger\.sqlite3$",
)


def get_commit_safe_patterns() -> dict[str, list[str]]:
    return {
        "safe_source": list(_SAFE_SOURCE_PATTERNS),
        "safe_report": list(_SAFE_REPORT_PATTERNS),
        "safe_folder": list(_SAFE_FOLDER_PATTERNS),
    }


def get_commit_forbidden_patterns() -> dict[str, list[str]]:
    return {
        "forbidden_file_extensions":
            list(_FORBIDDEN_FILE_PATTERNS),
        "forbidden_path_tokens":
            list(_FORBIDDEN_PATH_TOKENS),
        "skipped_runtime_patterns":
            list(_SKIP_RUNTIME_PATTERNS),
    }


def _classify_path(path: str) -> dict[str, Any]:
    p = str(path or "").replace("\\", "/")
    # Special-case the gitignore inside local_secret_handoff: safe
    # because it's exactly that one file that pins the folder
    # ignore.
    if p.endswith("bilingual_stack/voice_adapter_phase36/"
                  "local_secret_handoff/.gitignore"):
        return {"path": p, "category": "safe_gitignore",
                "reason": "phase36_local_secret_handoff_gitignore"}
    for pat in _SKIP_RUNTIME_PATTERNS:
        if re.search(pat, p):
            return {"path": p, "category": "skipped_runtime",
                    "reason": f"match:{pat}"}
    for tok in _FORBIDDEN_PATH_TOKENS:
        if tok in p:
            return {"path": p,
                    "category": "forbidden_path_token",
                    "reason": f"token:{tok}"}
    for pat in _FORBIDDEN_FILE_PATTERNS:
        if re.search(pat, p):
            return {"path": p,
                    "category": "forbidden_extension",
                    "reason": f"ext:{pat}"}
    base = p.split("/")[-1]
    for pat in _SAFE_REPORT_PATTERNS:
        if re.match(pat, base):
            return {"path": p, "category": "safe_report",
                    "reason": f"report:{pat}"}
    for pat in _SAFE_SOURCE_PATTERNS:
        if re.match(pat, base):
            return {"path": p, "category": "safe_source",
                    "reason": f"source:{pat}"}
    for pat in _SAFE_FOLDER_PATTERNS:
        if re.match(pat, p):
            return {"path": p, "category": "safe_folder",
                    "reason": f"folder:{pat}"}
    return {"path": p, "category": "unclassified",
            "reason": "no_pattern_matched"}


def classify_git_status_items(
    status_lines: Optional[list[str]] = None,
) -> dict[str, Any]:
    if not isinstance(status_lines, list):
        status_lines = []
    classifications: list[dict[str, Any]] = []
    for line in status_lines:
        # Git porcelain "XY path" — strip the two status chars
        s = line.strip()
        if not s:
            continue
        # Format: "XY path" or "?? path"
        if len(s) > 3 and s[2] == " ":
            path = s[3:]
        else:
            path = s
        classifications.append(_classify_path(path))
    return {
        "classifications": classifications,
        "count": len(classifications),
        "phase": _PHASE,
    }


def audit_commit_safety(
    status_lines: Optional[list[str]] = None,
) -> dict[str, Any]:
    res = classify_git_status_items(status_lines or [])
    cls = res.get("classifications") or []
    bins: dict[str, list[dict[str, Any]]] = {}
    for c in cls:
        bins.setdefault(c["category"], []).append(c)
    forbidden = (bins.get("forbidden_extension", []) +
                  bins.get("forbidden_path_token", []))
    return {
        "audit_id": f"caudit_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "total_items": res.get("count", 0),
        "bins": bins,
        "forbidden_count": len(forbidden),
        "ok": len(forbidden) == 0,
        "guidance": [
            "Do NOT commit anything classified as "
            "forbidden_extension or forbidden_path_token.",
            "Do NOT commit anything inside "
            "/local_secret_handoff/ except its own .gitignore.",
            "Items classified as skipped_runtime should remain "
            "untracked.",
            "safe_source / safe_report / safe_folder items are "
            "fine to commit when the operator chooses.",
            "unclassified items should be investigated by the "
            "operator before committing.",
        ],
    }


def summarize_commit_safety(audit: Any) -> dict[str, Any]:
    if not isinstance(audit, dict):
        return {"ok": False, "summary": "no_audit"}
    bins = audit.get("bins") or {}
    return {
        "ok": bool(audit.get("ok")),
        "summary": (
            f"phase38 commit-safety audit: total="
            f"{audit.get('total_items')} "
            f"forbidden={audit.get('forbidden_count')} "
            f"safe_source={len(bins.get('safe_source', []))} "
            f"safe_report={len(bins.get('safe_report', []))} "
            f"skipped_runtime="
            f"{len(bins.get('skipped_runtime', []))}"),
        "audit_id": audit.get("audit_id"),
        "phase": _PHASE,
    }


def write_commit_safety_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "classify_git_status_items",
    "get_commit_safe_patterns",
    "get_commit_forbidden_patterns",
    "audit_commit_safety",
    "summarize_commit_safety",
    "write_commit_safety_report",
]
