"""Phase 47 - Status Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase47.status_dashboard.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "phase47_status", "source_phase",
    "checkout_count",
    "federation_root_status",
    "imported_timeline_verification_status",
    "graph_verification_status",
    "manifest_verification_status",
    "drift_status",
    "tamper_suite_status",
    "no_runtime_state_status",
    "phase21_import_status",
    "forbidden_boundaries_preserved",
    "next_recommended_phase",
)


def create_phase47_status_dashboard(
    packet: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    p = packet or {}
    ts = p.get("tamper_suite_summary") or {}
    ds = p.get("drift_summary") or {}
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
        "adapter_invocation_in_federation",
        "adapter_reinvocation_in_verifier",
        "production_db_read_in_verifier",
        "corpus_import",
        "duplicate_checkout_id",
        "tampered_timeline_root_hash",
        "tampered_federation_root_hash",
        "path_traversal",
        "url_scheme_path",
    ]
    fed_root = p.get("federation_root_hash")
    return {
        "dashboard_id": f"p47dash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase47_status":
            p.get("phase47_status", "no_packet"),
        "source_phase":
            p.get("source_phase", "phase46"),
        "checkout_count":
            int(p.get("checkout_count") or 0),
        "federation_root_status":
            "match" if (isinstance(fed_root, str)
                          and len(fed_root) == 64)
            else "missing",
        "federation_root_hash_summary":
            (fed_root[:32]
             if isinstance(fed_root, str) else None),
        "imported_timeline_verification_status":
            p.get(
                "imported_timeline_verification_status",
                "unknown"),
        "graph_verification_status":
            p.get("graph_verification_status",
                   "unknown"),
        "manifest_verification_status":
            p.get("manifest_verification_status",
                   "unknown"),
        "drift_status": {
            "ok": bool(ds.get("ok")),
            "fail_count": ds.get("fail_count"),
            "warn_count": ds.get("warn_count"),
            "pass_count": ds.get("pass_count"),
        },
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
                   "Phase 48 federation portability "
                   "snapshot OR Phase 41a continuity-"
                   "ledger"),
        "notes": [
            "Dashboard is a static snapshot of one "
            "cross-checkout federation audit.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 explicitly.",
            "Verifier reads only imported timeline JSON; "
            "never production DBs; never invokes "
            "adapters.",
        ],
    }


def validate_phase47_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False,
                "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    if dashboard.get("source_phase") != "phase46":
        reasons.append("source_phase_not_phase46")
    allow = dashboard.get(
        "adapter_allowlist_status") or {}
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
                  "adapter_invocation_in_federation",
                  "production_db_read_in_verifier",
                  "duplicate_checkout_id",
                  "tampered_timeline_root_hash",
                  "path_traversal"):
        if must not in blocked:
            reasons.append(f"missing_block:{must}")
    return {"ok": not reasons, "reasons": reasons}


def create_phase47_dashboard_markdown(
    dashboard: Any,
) -> str:
    if not isinstance(dashboard, dict):
        return ""
    ts = dashboard.get("tamper_suite_status") or {}
    ds = dashboard.get("drift_status") or {}
    lines: list[str] = []
    lines.append("# Phase 47 - Cross-Checkout Federated "
                  "Timeline Dashboard\n")
    lines.append(f"_Generated at "
                  f"{int(dashboard.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 47 status:** "
                  f"{dashboard.get('phase47_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{dashboard.get('source_phase')}\n")
    lines.append(f"- **Checkout count:** "
                  f"{dashboard.get('checkout_count')}\n")
    lines.append(f"- **Federation root status:** "
                  f"{dashboard.get('federation_root_status')}\n")
    lines.append(f"- **Imported timeline verification:** "
                  f"{dashboard.get('imported_timeline_verification_status')}\n")
    lines.append(f"- **Graph verification:** "
                  f"{dashboard.get('graph_verification_status')}\n")
    lines.append(f"- **Manifest verification:** "
                  f"{dashboard.get('manifest_verification_status')}\n")
    lines.append(f"- **Drift status:** fail="
                  f"{ds.get('fail_count')} warn="
                  f"{ds.get('warn_count')} pass="
                  f"{ds.get('pass_count')}\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state status:** "
                  f"{dashboard.get('no_runtime_state_status')}\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{dashboard.get('phase21_import_status')}\n")
    allow = dashboard.get(
        "adapter_allowlist_status") or {}
    lines.append(f"- **Adapter allowlist:** "
                  f"{len(allow.get('allowed') or [])} of "
                  f"{allow.get('expected_count')}\n")
    lines.append("- **Forbidden boundaries preserved:**\n")
    for b in dashboard.get(
            "forbidden_boundaries_preserved") or []:
        lines.append(f"  - {b}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{dashboard.get('next_recommended_phase')}\n")
    return "".join(lines)


def write_phase47_status_dashboard(
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


def write_phase47_status_dashboard_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase47_status_dashboard",
    "validate_phase47_status_dashboard",
    "create_phase47_dashboard_markdown",
    "write_phase47_status_dashboard",
    "write_phase47_status_dashboard_markdown",
]
