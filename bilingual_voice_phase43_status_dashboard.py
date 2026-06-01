"""Phase 43 - Status Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase43.status_dashboard.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "portability_status", "source_phase",
    "artifact_count",
    "hash_verification_status",
    "fresh_checkout_verification_status",
    "excluded_artifact_status",
    "phase21_import_status",
    "forbidden_boundaries_preserved",
    "next_recommended_phase",
)


def create_phase43_status_dashboard(
    packet: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    p = packet or {}
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
        "adapter_reinvocation_on_fresh_checkout",
        "production_db_read_in_fresh_checkout",
        "corpus_import",
    ]
    return {
        "dashboard_id": f"p43dash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "portability_status":
            p.get("portability_status",
                   "no_packet_provided"),
        "source_phase": p.get("source_phase", "phase42"),
        "artifact_count":
            int(p.get("artifact_count") or 0),
        "hash_verification_status":
            p.get("hash_verification_status",
                   "unknown"),
        "fresh_checkout_verification_status":
            p.get("fresh_checkout_verification_status",
                   "unknown"),
        "excluded_artifact_status":
            "ok" if (p.get("excluded_artifacts_summary")
                       or {}).get("ok") is True
            else "drift",
        "no_runtime_db_status":
            "ok" if (p.get("no_runtime_db_status")
                       or {}).get("ok") is True
            else "drift",
        "no_audio_status":
            "ok" if (p.get("no_audio_status")
                       or {}).get("ok") is True
            else "drift",
        "no_secret_status":
            "ok" if (p.get("no_secret_status")
                       or {}).get("ok") is True
            else "drift",
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
                   "Phase 44 cross-machine bundle import "
                   "+ fresh-checkout regression"),
        "notes": [
            "Dashboard is a static snapshot of one "
            "portability run.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 explicitly.",
            "Fresh-checkout verifier reads only the "
            "bundle; never production DBs.",
        ],
    }


def validate_phase43_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False,
                "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    if dashboard.get("source_phase") != "phase42":
        reasons.append("source_phase_not_phase42")
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
                  "adapter_reinvocation_on_fresh_checkout",
                  "production_db_read_in_fresh_checkout"):
        if must not in blocked:
            reasons.append(f"missing_block:{must}")
    return {"ok": not reasons, "reasons": reasons}


def create_phase43_dashboard_markdown(
    dashboard: Any,
) -> str:
    if not isinstance(dashboard, dict):
        return ""
    lines: list[str] = []
    lines.append("# Phase 43 - Cross-Machine "
                  "Portability Dashboard\n")
    lines.append(f"_Generated at "
                  f"{int(dashboard.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 43 portability status:** "
                  f"{dashboard.get('portability_status')}"
                  f"\n")
    lines.append(f"- **Source phase:** "
                  f"{dashboard.get('source_phase')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{dashboard.get('artifact_count')}\n")
    lines.append(f"- **Hash verification status:** "
                  f"{dashboard.get('hash_verification_status')}"
                  f"\n")
    lines.append(f"- **Fresh-checkout verification:** "
                  f"{dashboard.get('fresh_checkout_verification_status')}"
                  f"\n")
    lines.append(f"- **Excluded artifact status:** "
                  f"{dashboard.get('excluded_artifact_status')}"
                  f"\n")
    lines.append(f"- **No-runtime-DB status:** "
                  f"{dashboard.get('no_runtime_db_status')}"
                  f"\n")
    lines.append(f"- **No-audio status:** "
                  f"{dashboard.get('no_audio_status')}\n")
    lines.append(f"- **No-secret-leakage status:** "
                  f"{dashboard.get('no_secret_status')}\n")
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


def write_phase43_status_dashboard(
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


def write_phase43_status_dashboard_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase43_status_dashboard",
    "validate_phase43_status_dashboard",
    "create_phase43_dashboard_markdown",
    "write_phase43_status_dashboard",
    "write_phase43_status_dashboard_markdown",
]
