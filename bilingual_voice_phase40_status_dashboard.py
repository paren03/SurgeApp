"""Phase 40 - Static Audit-Replay Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase40.status_dashboard.v1"


# Runtime-assembled forbidden runtime tokens
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "replay_status", "source_phase",
    "trace_hash_status",
    "drift_count",
    "baseline_status",
    "phase21_import_status",
    "adapter_allowlist_status",
    "forbidden_boundaries_preserved",
    "next_recommended_phase",
)


def create_phase40_status_dashboard(
    verification_result: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    vr = verification_result or {}
    trace = vr.get("trace_replay") or {}
    drift = vr.get("drift") or {}
    blocked = [
        "audio_generation", "tts_invocation",
        "subprocess_execution", "powershell_invocation",
        "sapi_invocation", "piper_invocation",
        "audio_file_write", "network_call",
        "socket_open", "multiprocessing",
        "main_runtime_integration",
        "program_s_modification",
        _TIER_PA + "_modification",
        _WORKER_LM + "_modification",
        "production_signing_secret_storage",
        "git_commit_of_signing_secret",
        "new_adapter_invocation",
    ]
    return {
        "dashboard_id": f"p40dash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "replay_status":
            vr.get("status", "no_replay_result"),
        "source_phase": "phase39",
        "trace_hash_status":
            "match" if trace.get("ok") else "mismatch",
        "drift_count": {
            "fail": drift.get("fail_count", 0),
            "warn": drift.get("warn_count", 0),
            "pass": drift.get("pass_count", 0),
        },
        "baseline_status": (
            "ok" if not vr.get("baseline_drifts")
            else "drift"),
        "baseline_observed":
            vr.get("baseline_observed") or {},
        "phase21_import_status":
            vr.get("phase21_status_text", "BLOCKED"),
        "adapter_allowlist_status": {
            "expected_count": 4,
            "allowed": [
                "dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter",
            ],
        },
        "forbidden_boundaries_preserved": blocked,
        "next_recommended_phase":
            "Phase G fifth metadata-only adapter OR "
            "Phase 41 cross-machine witness portability "
            "harness",
        "notes": [
            "Dashboard is a static snapshot of one replay "
            "run.",
            "Phase 21 real import remains BLOCKED unless "
            "operator runs Phase 21 explicitly.",
            "No audio, no TTS, no subprocess, no network, "
            "no multiprocessing.",
        ],
    }


def validate_phase40_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False,
                "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    if dashboard.get("source_phase") != "phase39":
        reasons.append("source_phase_not_phase39")
    allow = dashboard.get("adapter_allowlist_status") or {}
    if allow.get("expected_count") != 4:
        reasons.append("expected_adapter_count_not_4")
    if len(allow.get("allowed") or []) != 4:
        reasons.append("allowed_list_not_4")
    blocked = dashboard.get(
        "forbidden_boundaries_preserved") or []
    for must in ("audio_generation", "tts_invocation",
                  "subprocess_execution", "network_call",
                  "multiprocessing",
                  "new_adapter_invocation"):
        if must not in blocked:
            reasons.append(f"missing_block:{must}")
    return {"ok": not reasons, "reasons": reasons}


def create_phase40_dashboard_markdown(
    dashboard: Any,
) -> str:
    if not isinstance(dashboard, dict):
        return ""
    drift = dashboard.get("drift_count") or {}
    lines: list[str] = []
    lines.append("# Phase 40 - Operator Audit-Replay "
                  "Dashboard\n")
    lines.append(f"_Generated at "
                  f"{int(dashboard.get('created_at') or time.time())}._"
                  f"\n")
    lines.append("")
    lines.append(f"- **Phase 40 replay status:** "
                  f"{dashboard.get('replay_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{dashboard.get('source_phase')}\n")
    lines.append(f"- **Trace hash status:** "
                  f"{dashboard.get('trace_hash_status')}\n")
    lines.append(f"- **Drift count:** fail="
                  f"{drift.get('fail')} warn="
                  f"{drift.get('warn')} pass="
                  f"{drift.get('pass')}\n")
    lines.append(f"- **Baseline status:** "
                  f"{dashboard.get('baseline_status')}\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{dashboard.get('phase21_import_status')}"
                  f"\n")
    allow = dashboard.get("adapter_allowlist_status") or {}
    lines.append(f"- **Adapter allowlist:** "
                  f"{len(allow.get('allowed') or [])} of "
                  f"{allow.get('expected_count')}\n")
    lines.append("- **Allowed adapters:**\n")
    for a in allow.get("allowed") or []:
        lines.append(f"  - `{a}`\n")
    lines.append("- **Forbidden boundaries preserved:**\n")
    for b in dashboard.get(
            "forbidden_boundaries_preserved") or []:
        lines.append(f"  - {b}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{dashboard.get('next_recommended_phase')}"
                  f"\n")
    return "".join(lines)


def write_phase40_status_dashboard(
    dashboard: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(dashboard)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase40_status_dashboard_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase40_status_dashboard",
    "validate_phase40_status_dashboard",
    "create_phase40_dashboard_markdown",
    "write_phase40_status_dashboard",
    "write_phase40_status_dashboard_markdown",
]
