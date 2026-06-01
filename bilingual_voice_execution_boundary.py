"""Phase 28 - Execution Boundary Guard.

Hard refusal for any execution-related action requests against payload
objects. Word-aware matching avoids negation-key false positives.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_FORBIDDEN_ACTION_TOKENS = (
    "generate_audio", "synthesize", "speak", "tts",
    "piper", "sapi", "powershell", "subprocess", "shell",
    "os.system", "audio_path", "wav", "mp3", "voice_clone",
    "speaker_embedding", "network", "download",
)


_NEGATION_PREFIXES = ("no", "supports", "max", "accepted",
                      "is", "has", "forbidden",
                      "uses", "invokes", "produces", "writes",
                      "received", "would", "test")


_REPORTING_SUFFIXES = ("hits", "check", "result", "summary", "list",
                       "count", "info", "reasons", "report", "log",
                       "events", "id", "schema")


def _key_matches(ks: str, tok: str) -> bool:
    if ks == tok:
        return True
    parts = ks.split("_")
    if parts and parts[0] in _NEGATION_PREFIXES:
        return False
    if tok in parts:
        idx = parts.index(tok)
        # If the next part is a reporting suffix, the key is a metadata
        # field name (e.g. subprocess_hits, audio_check) — not a request.
        if idx + 1 < len(parts) and parts[idx + 1] in _REPORTING_SUFFIXES:
            return False
        return True
    if "_" in tok:
        tok_parts = tok.split("_")
        n = len(tok_parts)
        for i in range(len(parts) - n + 1):
            if parts[i:i + n] == tok_parts:
                # Reporting-suffix check applies to composite tokens too
                if i + n < len(parts) and \
                        parts[i + n] in _REPORTING_SUFFIXES:
                    return False
                return True
    if "." in tok and tok in ks:
        return True
    return False


def get_forbidden_execution_actions() -> list[str]:
    return list(_FORBIDDEN_ACTION_TOKENS)


_VALUE_NEGATION_PREFIXES = ("no_", "supports_", "forbidden_", "phase28",
                            "phase27", "phase26", "phase25",
                            "dry_run", "audio_generation", "tts_invocation",
                            "voice_cloning", "subprocess_execution",
                            "powershell_invocation", "sapi_speak",
                            "network_call", "audio_file_write")


# Keys whose VALUES are deliberate policy enumerations we should not
# scan (they list banned actions, so containing forbidden tokens is
# expected and not an actual execution attempt).
_SKIP_VALUE_SCAN_KEYS = (
    "forbidden_runtime_actions", "forbidden_actions",
    "output_policy", "permitted_actions",
    "next_allowed_actions", "supports_languages",
    "supports_streaming",
)


def scan_for_execution_intent(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _value_matches(vs: str, tok: str) -> bool:
        if vs == tok:
            return True
        # Phrases like "subprocess_execution" are policy enumeration
        # entries (not actual command requests); skip them.
        for neg in _VALUE_NEGATION_PREFIXES:
            if vs.startswith(neg):
                return False
        # Action-verb patterns: "run_subprocess", "invoke_tts" etc.
        for verb in ("run", "invoke", "call", "generate", "execute",
                     "start", "spawn", "speak", "write"):
            if vs == f"{verb}_{tok}":
                return True
            if vs.startswith(f"{verb}_") and tok in vs[len(verb) + 1:]:
                return True
        # Token appears as a whole word in the value
        words = vs.replace("-", " ").replace(".", " ").split()
        if tok in words:
            return True
        return False

    def _walk(o: Any, skip_values: bool = False) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                # Always scan keys themselves
                for tok in _FORBIDDEN_ACTION_TOKENS:
                    if _key_matches(ks, tok) and tok not in hits:
                        hits.append(tok)
                # Determine whether to scan values inside this branch
                child_skip_values = skip_values or \
                    (ks in _SKIP_VALUE_SCAN_KEYS)
                if isinstance(v, str) and not child_skip_values:
                    vs = v.lower()
                    for tok in _FORBIDDEN_ACTION_TOKENS:
                        if _value_matches(vs, tok) and tok not in hits:
                            hits.append(tok)
                _walk(v, skip_values=child_skip_values)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v, skip_values=skip_values)

    _walk(obj, skip_values=False)
    return hits


def validate_no_execution_request(obj: Any) -> dict[str, Any]:
    hits = scan_for_execution_intent(obj)
    return {"ok": not hits, "hits": hits, "phase": "phase28"}


def enforce_phase28_no_execution(obj: Any) -> dict[str, Any]:
    res = validate_no_execution_request(obj)
    return {
        "ok": res["ok"],
        "execution_blocked": True,
        "reasons": (["execution_intent_detected:" + ",".join(res["hits"])]
                    if res["hits"] else []),
        "hits": res["hits"],
        "phase": "phase28",
    }


def reject_if_audio_or_subprocess_requested(obj: Any) -> dict[str, Any]:
    hits = scan_for_execution_intent(obj)
    audio_like = [h for h in hits if h in
                  ("generate_audio", "synthesize", "speak", "tts",
                   "audio_path", "wav", "mp3", "voice_clone",
                   "speaker_embedding")]
    sub_like = [h for h in hits if h in
                ("subprocess", "shell", "os.system", "powershell",
                 "piper", "sapi")]
    return {
        "rejected": bool(audio_like or sub_like),
        "audio_hits": audio_like,
        "subprocess_hits": sub_like,
        "phase": "phase28",
    }


def build_boundary_result(obj: Any) -> dict[str, Any]:
    enforce = enforce_phase28_no_execution(obj)
    reject = reject_if_audio_or_subprocess_requested(obj)
    return {
        "ok": enforce["ok"] and not reject["rejected"],
        "execution_blocked": True,
        "reasons": enforce.get("reasons", []),
        "audio_hits": reject.get("audio_hits", []),
        "subprocess_hits": reject.get("subprocess_hits", []),
        "hits": enforce.get("hits", []),
        "phase": "phase28",
    }


def write_execution_boundary_report(
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
    "get_forbidden_execution_actions",
    "scan_for_execution_intent",
    "validate_no_execution_request",
    "enforce_phase28_no_execution",
    "reject_if_audio_or_subprocess_requested",
    "build_boundary_result",
    "write_execution_boundary_report",
]
