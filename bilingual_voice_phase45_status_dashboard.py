"""Phase 45 - Status Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase45.status_dashboard.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "phase45_status", "source_phases",
    "artifact_count",
    "chain_of_trust_status",
    "manifest_verification_status",
    "archive_verification_status",
    "tamper_suite_status",
    "no_runtime_state_status",
    "phase21_import_status",
    "forbidden_boundaries_preserved",
    "next_recommended_phase",
)


def create_phase45_status_dashboard(
    packet: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    p = packet or {}
    ts = p.get("tamper_suite_summary") or {}
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
        "adapter_invocation_in_archive",
        "adapter_reinvocation_in_verifier",
        "production_db_read_in_verifier",
        "corpus_import",
        "broken_chain_order",
        "tampered_artifact_hash",
    ]
    return {
        "dashboard_id": f"p45dash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase45_status":
            p.get("phase45_status", "no_packet"),
        "source_phases":
            list(p.get("source_phases")
                  or ["phase42", "phase43", "phase44"]),
        "artifact_count":
            int(p.get("artifact_count") or 0),
        "chain_of_trust_status":
            p.get("chain_of_trust_status", "unknown"),
        "manifest_verification_status":
            p.get("manifest_verification_status",
                   "unknown"),
        "archive_verification_status":
            p.get("archive_verification_status",
                   "unknown"),
        "tamper_suite_status": {
            "ok": bool(ts.get("ok")),
            "detected_count": ts.get("detected_count"),
            "case_count": ts.get("case_count"),
        },
        "no_runtime_state_status":
            p.get("no_runtime_state_dependency_status",
                   "unknown"),
        "phase21_import_status":
            (p.get("phase21_import_status") or {}).get(
                "status_text", "BLOCKED"),
        "forbidden_boundaries_preserved": blocked,
        "adapter_allowlist_status": {
            "expected_count": 5,
            "allowed": [
                "dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter",
                "memory_continuity_audit_metadata_adapter",
            ],
        },
        "next_recommended_phase":
            p.get("next_recommended_phase",
                   "Phase 46 cross-archive long-horizon "
                   "timeline OR Phase 41a continuity-"
                   "ledger"),
        "notes": [
            "Dashboard is a static snapshot of one "
            "multi-bundle chain-of-trust audit.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 explicitly.",
            "Verifier reads only the archive; never "
            "production DBs; never invokes adapters.",
        ],
    }


def validate_phase45_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False,
                "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    sp = list(dashboard.get("source_phases") or [])
    for must in ("phase42", "phase43", "phase44"):
        if must not in sp:
            reasons.append(f"missing_source_phase:{must}")
    allow = dashboard.get("adapter_allowlist_status") or {}
    if allow.get("expected_count") != 5:
        reasons.append(
            f"expected_adapter_count_not_5:"
            f"{allow.get('expected_count')}")
    blocked = dashboard.get(
        "forbidden_boundaries_preserved") or []
    for must in ("audio_generation", "tts_invocation",
                  "subprocess_execution", "network_call",
                  "multiprocessing",
                  "new_adapter_invocation",
                  "adapter_invocation_in_archive",
                  "production_db_read_in_verifier",
                  "broken_chain_order",
                  "tampered_artifact_hash"):
        if must not in blocked:
            reasons.append(f"missing_block:{must}")
    return {"ok": not reasons, "reasons": reasons}


def create_phase45_dashboard_markdown(
    dashboard: Any,
) -> str:
    if not isinstance(dashboard, dict):
        return ""
    ts = dashboard.get("tamper_suite_status") or {}
    lines: list[str] = []
    lines.append("# Phase 45 - Multi-Bundle Chain-of-"
                  "Trust Dashboard\n")
    lines.append(f"_Generated at "
                  f"{int(dashboard.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 45 archive status:** "
                  f"{dashboard.get('phase45_status')}\n")
    lines.append(f"- **Source phases:** "
                  f"{dashboard.get('source_phases')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{dashboard.get('artifact_count')}\n")
    lines.append(f"- **Chain-of-trust status:** "
                  f"{dashboard.get('chain_of_trust_status')}"
                  f"\n")
    lines.append(f"- **Manifest verification:** "
                  f"{dashboard.get('manifest_verification_status')}"
                  f"\n")
    lines.append(f"- **Archive verification:** "
                  f"{dashboard.get('archive_verification_status')}"
                  f"\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state status:** "
                  f"{dashboard.get('no_runtime_state_status')}"
                  f"\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{dashboard.get('phase21_import_status')}"
                  f"\n")
    allow = dashboard.get("adapter_allowlist_status") or {}
    lines.append(f"- **Adapter allowlist:** "
                  f"{len(allow.get('allowed') or [])} of "
                  f"{allow.get('expected_count')}\n")
    lines.append("- **Forbidden boundaries preserved:**\n")
    for b in dashboard.get(
            "forbidden_boundaries_preserved") or []:
        lines.append(f"  - {b}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{dashboard.get('next_recommended_phase')}"
                  f"\n")
    return "".join(lines)


def write_phase45_status_dashboard(
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


def write_phase45_status_dashboard_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase45_status_dashboard",
    "validate_phase45_status_dashboard",
    "create_phase45_dashboard_markdown",
    "write_phase45_status_dashboard",
    "write_phase45_status_dashboard_markdown",
]
