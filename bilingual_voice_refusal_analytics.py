"""Phase 29 - Refusal Analytics.

Categorize and aggregate adapter-call refusal / downgrade reasons.
Bounded analysis. No execution. Recommendations remain safe and never
suggest bypassing boundary rules.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase29.refusal_analytics.v1"

_HARD_CAP = 1000

_CATEGORIES = (
    "consent_missing",
    "consent_invalid",
    "consent_expired",
    "unsafe_payload",
    "unsupported_adapter",
    "unsupported_language",
    "unsupported_code_switching",
    "missing_safety_support",
    "execution_boundary",
    "audio_field_forbidden",
    "subprocess_field_forbidden",
    "network_field_forbidden",
    "voice_clone_forbidden",
    "dry_run_required",
    "unknown",
)


_ERROR_CODE_MAP = {
    "PHASE28_EXECUTION_BLOCKED": "execution_boundary",
    "PHASE29_EXECUTION_BLOCKED": "execution_boundary",
    "CONSENT_MISSING": "consent_missing",
    "CONSENT_INVALID": "consent_invalid",
    "UNSAFE_PAYLOAD": "unsafe_payload",
    "UNSUPPORTED_LANGUAGE_MODE": "unsupported_language",
    "UNSUPPORTED_CODE_SWITCHING": "unsupported_code_switching",
    "UNSUPPORTED_PROSODY": "unsupported_adapter",
    "UNSUPPORTED_PRONUNCIATION_HINTS": "unsupported_adapter",
    "ADAPTER_DRY_RUN_REQUIRED": "dry_run_required",
    "AUDIO_FIELD_FORBIDDEN": "audio_field_forbidden",
    "SUBPROCESS_FIELD_FORBIDDEN": "subprocess_field_forbidden",
    "NETWORK_FIELD_FORBIDDEN": "network_field_forbidden",
    "VOICE_CLONE_FIELD_FORBIDDEN": "voice_clone_forbidden",
    "CAPABILITY_MISMATCH": "unsupported_adapter",
    "UNKNOWN_ADAPTER": "unsupported_adapter",
    "PAYLOAD_INVALID": "unsafe_payload",
}


def classify_refusal_reason(item: Any) -> str:
    if not isinstance(item, dict):
        return "unknown"
    # Error-shape items
    code = str(item.get("code") or "").upper()
    if code in _ERROR_CODE_MAP:
        return _ERROR_CODE_MAP[code]
    # Boundary-result-shape items
    reasons = item.get("reasons") or []
    if isinstance(reasons, list):
        joined = " ".join(str(r).lower() for r in reasons)
        if "expired" in joined:
            return "consent_expired"
        if "token_invalid" in joined or "consent_invalid" in joined:
            return "consent_invalid"
        if "token_missing" in joined or "consent_missing" in joined:
            return "consent_missing"
        if "unsafe" in joined or "safety_recheck_failed" in joined:
            return "unsafe_payload"
        if "audio_field_present" in joined or "audio" in joined:
            return "audio_field_forbidden"
        if ("subprocess" in joined or "powershell" in joined
                or "sapi" in joined or "piper" in joined
                or "shell" in joined):
            return "subprocess_field_forbidden"
        if "network" in joined or "download" in joined:
            return "network_field_forbidden"
        if "voice_clone" in joined or "speaker_embedding" in joined:
            return "voice_clone_forbidden"
        if "execution_field_present" in joined or \
                "execution_intent_detected" in joined or \
                "execution_boundary" in joined:
            return "execution_boundary"
        if "dry_run" in joined:
            return "dry_run_required"
        if "capability_mismatch" in joined or \
                "unsupported" in joined:
            return "unsupported_adapter"
    return "unknown"


def aggregate_refusal_reasons(
    items: list[Any],
    limit: int = _HARD_CAP,
) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    if not isinstance(items, list):
        items = []
    counts: dict[str, int] = {c: 0 for c in _CATEGORIES}
    total = 0
    for it in items[:cap]:
        cat = classify_refusal_reason(it)
        counts[cat] = counts.get(cat, 0) + 1
        total += 1
    return {
        "total": total,
        "by_category": counts,
        "phase": _PHASE,
        "cap": _HARD_CAP,
    }


def summarize_refusal_patterns(
    refusals: list[Any],
) -> dict[str, Any]:
    agg = aggregate_refusal_reasons(refusals)
    by_cat = agg["by_category"]
    top = sorted(by_cat.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return {
        "total": agg["total"],
        "top_categories": [{"category": c, "count": n}
                           for c, n in top if n > 0],
        "phase": _PHASE,
    }


def recommend_safe_next_steps(
    refusals: list[Any],
) -> dict[str, Any]:
    agg = aggregate_refusal_reasons(refusals)
    by_cat = agg["by_category"]
    steps: list[str] = []
    if by_cat.get("consent_missing", 0):
        steps.append("Operator must issue a per-invocation dry-run "
                     "consent token before retry.")
    if by_cat.get("consent_expired", 0):
        steps.append("Refresh expired invocation tokens; do not extend "
                     "expiry indefinitely.")
    if by_cat.get("consent_invalid", 0):
        steps.append("Re-create the consent token; verify operator_id "
                     "is present and binding_hash matches the envelope.")
    if by_cat.get("unsafe_payload", 0):
        steps.append("Run Phase 25 safety redactor against the payload "
                     "before retry; preserve the safety summary as-is.")
    if by_cat.get("execution_boundary", 0):
        steps.append("Re-author the request without execution-intent "
                     "fields; Phase 29 never executes regardless.")
    if by_cat.get("audio_field_forbidden", 0):
        steps.append("Remove any audio_* / wav / mp3 fields from the "
                     "envelope before retry; audio is out of scope in "
                     "Phase 29.")
    if by_cat.get("subprocess_field_forbidden", 0):
        steps.append("Remove subprocess / shell / PowerShell / SAPI / "
                     "Piper fields; Phase 29 has no runtime path.")
    if by_cat.get("network_field_forbidden", 0):
        steps.append("Remove network / download fields; Phase 29 is "
                     "offline-only by policy.")
    if by_cat.get("voice_clone_forbidden", 0):
        steps.append("Remove voice_clone / speaker_embedding fields; "
                     "voice cloning is out of scope.")
    if by_cat.get("missing_safety_support", 0):
        steps.append("Pick an adapter whose forbidden_runtime_actions "
                     "enumerate the full safety set.")
    if by_cat.get("unsupported_language", 0):
        steps.append("Pick an adapter whose supports_languages covers "
                     "the payload's language_mode.")
    if by_cat.get("unsupported_code_switching", 0):
        steps.append("Pick a code-switch-capable adapter, OR split the "
                     "payload into single-language runs.")
    if by_cat.get("unsupported_adapter", 0):
        steps.append("Re-select adapter via Phase 27 registry; do not "
                     "bind an unknown engine.")
    if by_cat.get("dry_run_required", 0):
        steps.append("Force dry_run=True on the envelope and token "
                     "before retry; Phase 29 enforces dry-run.")
    if not steps:
        steps.append("No actionable refusals.")
    return {
        "total": agg["total"],
        "steps": steps,
        "phase": _PHASE,
        "safety_advisory": ("All Phase 29 recommendations preserve the "
                             "dry-run boundary; safety controls remain "
                             "in effect."),
    }


def write_refusal_analytics_report(
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
    "classify_refusal_reason",
    "aggregate_refusal_reasons",
    "summarize_refusal_patterns",
    "recommend_safe_next_steps",
    "write_refusal_analytics_report",
]
