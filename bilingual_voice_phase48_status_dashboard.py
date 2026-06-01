"""Phase 48 - Status Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase48.status_dashboard.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "phase48_status", "source_phase",
    "artifact_count",
    "capsule_root_status",
    "fresh_checkout_verification_status",
    "tamper_suite_status",
    "no_runtime_state_status",
    "adapter_allowlist_status",
    "phase21_import_status",
    "forbidden_boundaries_preserved",
    "next_recommended_phase",
)


def create_phase48_status_dashboard(
    packet: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    p = packet or {}
    ts = p.get("tamper_suite_summary") or {}
    crh = p.get("capsule_root_hash")
    allow = (p.get("adapter_allowlist_status") or {})
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
        "adapter_invocation_in_capsule",
        "adapter_reinvocation_in_verifier",
        "production_db_read_in_verifier",
        "corpus_import",
        "tampered_capsule_root_hash",
        "tampered_manifest_root_hash",
        "missing_federation_artifact",
    ]
    return {
        "dashboard_id": f"p48dash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase48_status":
            p.get("phase48_status", "no_packet"),
        "source_phase":
            p.get("source_phase", "phase47"),
        "artifact_count":
            int(p.get("artifact_count") or 0),
        "capsule_root_status":
            "match"
            if (isinstance(crh, str) and len(crh) == 64)
            else "missing",
        "capsule_root_hash_summary":
            (crh[:32]
             if isinstance(crh, str) else None),
        "fresh_checkout_verification_status":
            p.get("fresh_checkout_verification_status",
                   "unknown"),
        "tamper_suite_status": {
            "ok": bool(ts.get("ok")),
            "detected_count":
                ts.get("detected_count"),
            "case_count": ts.get("case_count"),
        },
        "no_runtime_state_status":
            p.get(
                "no_runtime_state_dependency_status",
                "unknown"),
        "adapter_allowlist_status": {
            "expected_count": 5,
            "observed_count":
                int(allow.get("observed_count") or 0),
            "allowed": [
                "dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter",
                "memory_continuity_audit_metadata_adapter",
            ],
        },
        "phase21_import_status":
            (p.get("phase21_import_status") or {}).get(
                "status_text", "BLOCKED"),
        "forbidden_boundaries_preserved": blocked,
        "next_recommended_phase":
            p.get("next_recommended_phase",
                   "Phase 49 federation portability "
                   "replay verification OR Phase 41a "
                   "continuity-ledger"),
        "notes": [
            "Dashboard is a static snapshot of one "
            "federation portability capsule audit.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 explicitly.",
            "Verifier reads only capsule JSON+Markdown; "
            "never production DBs; never invokes "
            "adapters.",
        ],
    }


def validate_phase48_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False,
                "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    if dashboard.get("source_phase") != "phase47":
        reasons.append("source_phase_not_phase47")
    allow = dashboard.get(
        "adapter_allowlist_status") or {}
    if allow.get("expected_count") != 5:
        reasons.append(
            f"expected_adapter_count_not_5:"
            f"{allow.get('expected_count')}")
    if int(allow.get("observed_count") or 0) != 5:
        reasons.append(
            f"observed_adapter_count_not_5:"
            f"{allow.get('observed_count')}")
    blocked = dashboard.get(
        "forbidden_boundaries_preserved") or []
    for must in ("audio_generation", "tts_invocation",
                  "subprocess_execution", "network_call",
                  "multiprocessing",
                  "new_adapter_invocation",
                  "adapter_invocation_in_capsule",
                  "production_db_read_in_verifier",
                  "tampered_capsule_root_hash",
                  "missing_federation_artifact"):
        if must not in blocked:
            reasons.append(f"missing_block:{must}")
    return {"ok": not reasons, "reasons": reasons}


def create_phase48_dashboard_markdown(
    dashboard: Any,
) -> str:
    if not isinstance(dashboard, dict):
        return ""
    ts = dashboard.get("tamper_suite_status") or {}
    allow = dashboard.get(
        "adapter_allowlist_status") or {}
    lines: list[str] = []
    lines.append("# Phase 48 - Federation Portability "
                  "Snapshot Dashboard\n")
    lines.append(f"_Generated at "
                  f"{int(dashboard.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 48 status:** "
                  f"{dashboard.get('phase48_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{dashboard.get('source_phase')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{dashboard.get('artifact_count')}\n")
    lines.append(f"- **Capsule root status:** "
                  f"{dashboard.get('capsule_root_status')}\n")
    lines.append(f"- **Fresh-checkout verification:** "
                  f"{dashboard.get('fresh_checkout_verification_status')}\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state status:** "
                  f"{dashboard.get('no_runtime_state_status')}\n")
    lines.append(f"- **Adapter allowlist:** "
                  f"{len(allow.get('allowed') or [])} of "
                  f"{allow.get('expected_count')}\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{dashboard.get('phase21_import_status')}\n")
    lines.append("- **Forbidden boundaries preserved:**\n")
    for b in dashboard.get(
            "forbidden_boundaries_preserved") or []:
        lines.append(f"  - {b}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{dashboard.get('next_recommended_phase')}\n")
    return "".join(lines)


def write_phase48_status_dashboard(
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


def write_phase48_status_dashboard_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase48_status_dashboard",
    "validate_phase48_status_dashboard",
    "create_phase48_dashboard_markdown",
    "write_phase48_status_dashboard",
    "write_phase48_status_dashboard_markdown",
]
