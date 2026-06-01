"""Phase 38 - Governance Ledger.

Machine-readable ledger of Phase 27-37 boundaries, adapters,
forbidden actions, reports, and harness results.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any


_PHASE = "phase38.governance_ledger.v1"


_PHASE_REPORTS = {
    25: "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    26: "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
    27: "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
    28: "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
    29: "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
    30: "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
    31: "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
    32: "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
    33: "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
    34: "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
    35: "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
    36: "PHASE36_KEY_HANDOFF_ENVELOPE_REPORT.md",
    37: "PHASE37_SAFETY_TRACE_ADAPTER_GOVERNANCE_REPORT.md",
}


_REPORT_HARNESS_RESULTS = {
    25: ("test_phase25_spoken_render_contract", 130),
    26: ("test_phase26_voice_memory_continuity", 140),
    27: ("test_phase27_voice_render_adapter_skeleton", 399),
    28: ("test_phase28_operator_gated_voice_adapter", 383),
    29: ("test_phase29_operator_gated_runtime_adapter_b", 386),
    30: ("test_phase30_callable_adapter_boundary", 374),
    31: ("test_phase31_multi_adapter_boundary", 379),
    32: ("test_phase32_audit_signing_and_verification", 362),
    33: ("test_phase33_three_adapter_signed_governance", 384),
    34: ("test_phase34_external_witness_verification", 378),
    35: ("test_phase35_witness_exchange_protocol", 375),
    36: ("test_phase36_key_handoff_envelope", 398),
    37: ("test_phase37_safety_trace_adapter_governance", 418),
}


_PHASE_ENTRIES: dict[int, dict[str, Any]] = {
    27: {
        "title": "Voice-Render Adapter Skeleton (dry-run only)",
        "adapters_introduced": [],
        "allowed_callable_adapters": [],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "no secrets handled",
        "evidence_policy": "n/a",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report; "
                             "delete bilingual_stack/voice_adapter/",
        "next_phase_recommendation": "Phase 28 operator-gated dry-run",
    },
    28: {
        "title": "Operator-Gated Voice Adapter (consent + audit "
                  "log + envelope, still dry-run)",
        "adapters_introduced": [],
        "allowed_callable_adapters": [],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "no secrets",
        "evidence_policy": "audit log + envelope only",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 29 per-invocation consent",
    },
    29: {
        "title": "Per-Invocation Consent + Tamper-Evident Audit "
                  "Chain (Phase B, still dry-run)",
        "adapters_introduced": [],
        "allowed_callable_adapters": [],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "no secrets",
        "evidence_policy": "hash-linked audit chain",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 30 callable boundary",
    },
    30: {
        "title": "First Callable Boundary — dummy metadata adapter "
                  "only",
        "adapters_introduced": ["dummy_metadata_adapter"],
        "allowed_callable_adapters": ["dummy_metadata_adapter"],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "operator_id hashed only",
        "evidence_policy": "invocation receipt",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 31 second metadata adapter",
    },
    31: {
        "title": "Two Metadata-Only Adapters (Phase D)",
        "adapters_introduced": ["bilingual_segment_metadata_adapter"],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "operator_id hashed only",
        "evidence_policy": "invocation + selection receipts",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 32 signing",
    },
    32: {
        "title": "Audit-Chain Signing + Evidence Bundle "
                  "(HMAC-SHA256 test-only)",
        "adapters_introduced": [],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "HMAC-SHA256 test-only keys; never "
                            "exported into reports/witness/public",
        "evidence_policy": "signed audit chain + evidence bundle",
        "witness_export_policy": "n/a",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 33 third adapter + "
                                      "signed evidence by default",
    },
    33: {
        "title": "Three Metadata-Only Adapters + Signed Evidence "
                  "Default (Phase E)",
        "adapters_introduced": ["prosody_density_metadata_adapter"],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "HMAC-SHA256 test-only; never exported",
        "evidence_policy": "signed evidence required for ok status",
        "witness_export_policy": "via Phase 34 (deferred)",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 34 external witness",
    },
    34: {
        "title": "External Witness Verification + Public "
                  "Descriptor Export",
        "adapters_introduced": [],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "public descriptor carries fingerprint "
                            "only; raw material refused",
        "evidence_policy": "witness package with signed evidence",
        "witness_export_policy": "operator-readable JSON package",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 35 local exchange",
    },
    35: {
        "title": "Local File-Based Witness Exchange Protocol",
        "adapters_introduced": [],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
            "open_socket", "multiprocessing",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "no secret-bearing artifacts; all paths "
                            "local; URLs rejected",
        "evidence_policy": "exchange contract + packet + witness "
                            "input/output + handshake record",
        "witness_export_policy": "via Phase 34 reused",
        "rollback_summary": "delete 7 modules + harness + report",
        "next_phase_recommendation": "Phase 36 out-of-band key "
                                      "handoff envelope",
    },
    36: {
        "title": "Out-of-Band Test-Key Handoff Envelope "
                  "(gitignored local_secret_handoff)",
        "adapters_introduced": [],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
            "open_socket", "multiprocessing",
            "production_secret_storage",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "test-only sealed envelopes live in "
                            "local_secret_handoff (gitignored); "
                            "consent_marker hashed; deterministic "
                            "local test wrapping (not production "
                            "encryption)",
        "evidence_policy": "envelope summary in reports; sealed "
                            "envelope only inside safe folder",
        "witness_export_policy": "public descriptor bridge "
                                  "compatible with Phase 34",
        "rollback_summary": "delete 7 modules + harness + report "
                             "+ 12 sub-folders",
        "next_phase_recommendation": "Phase 37 fourth metadata "
                                      "adapter + signed witness "
                                      "pipeline",
    },
    37: {
        "title": "Fourth Metadata-Only Callable + Signed Witness "
                  "Pipeline (Phase F)",
        "adapters_introduced":
            ["safety_redaction_trace_metadata_adapter"],
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
            "safety_redaction_trace_metadata_adapter",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
            "open_socket", "multiprocessing",
            "production_secret_storage",
        ],
        "production_db_impact": "zero",
        "secrets_policy": "test-only HMAC-SHA256; pipeline "
                            "carries only signing_metadata; raw "
                            "material refused everywhere",
        "evidence_policy": "signed evidence + witness export + "
                            "exchange required for ok status; "
                            "optional handoff requires consent "
                            "marker",
        "witness_export_policy": "Phase 34 witness package "
                                  "embedded into pipeline",
        "rollback_summary": "delete 7 modules + harness + report "
                             "+ 11 sub-folders",
        "next_phase_recommendation": "Phase 38 operator "
                                      "governance docs",
    },
}


_FULL_REGRESSION_TOTAL = 6185


def _summarize_report_path(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {"present": False, "size_bytes": 0}
    try:
        size = path.stat().st_size
    except Exception:  # noqa: BLE001
        size = 0
    head = ""
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            head = fh.read(2048)
    except Exception:  # noqa: BLE001
        pass
    m = re.search(r"^# (.+)$", head, re.MULTILINE)
    title = m.group(1).strip() if m else ""
    return {"present": True, "size_bytes": size,
            "title_line": title}


def collect_phase_reports(
    root: Path | None = None,
) -> dict[int, dict[str, Any]]:
    root = Path(root) if root else Path(__file__).resolve().parent
    out: dict[int, dict[str, Any]] = {}
    for phase, fname in _PHASE_REPORTS.items():
        out[phase] = {
            "filename": fname,
            **_summarize_report_path(root / fname),
        }
    return out


def build_boundary_guarantee_ledger(
    root: Path | None = None,
) -> dict[str, Any]:
    reports = collect_phase_reports(root)
    entries: list[dict[str, Any]] = []
    for phase in sorted(_PHASE_ENTRIES.keys()):
        e = dict(_PHASE_ENTRIES[phase])
        e["phase"] = phase
        e["report"] = reports.get(phase, {})
        if phase in _REPORT_HARNESS_RESULTS:
            name, total = _REPORT_HARNESS_RESULTS[phase]
            e["harness"] = {"name": name, "result_total": total,
                             "expected_pass": total}
        else:
            e["harness"] = {}
        entries.append(e)
    return {
        "ledger_id": f"ledger_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "latest_phase": max(_PHASE_ENTRIES.keys()),
        "full_regression_total_expected":
            _FULL_REGRESSION_TOTAL,
        "harness_count_expected":
            len([p for p in _REPORT_HARNESS_RESULTS if p >= 25])
            + 8,
        "production_invariants": {
            "english_words": 2814,
            "russian_words": 2518,
            "russian_phrases": 35,
            "bilingual_concepts": 26,
            "bilingual_entry_links": 52,
            "live_pack_manifests": 90,
        },
        "entries": entries,
        "phase21_status":
            "operator-staged real import remains BLOCKED; corpus "
            "folders empty; separate workflow from voice-adapter "
            "governance",
        "notes": [
            "All Phase 27-37 modules are standalone — not wired "
            "into Luna main runtime.",
            "No audio, no TTS, no subprocess, no PowerShell, no "
            "SAPI, no Piper, no network, no multiprocessing.",
            "All four callable adapters return metadata only.",
        ],
    }


def validate_boundary_guarantee_ledger(
    ledger: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(ledger, dict):
        return {"ok": False, "reasons": ["ledger_not_dict"]}
    for f in ("ledger_id", "created_at", "phase", "latest_phase",
              "production_invariants", "entries",
              "phase21_status"):
        if f not in ledger:
            reasons.append(f"missing_field:{f}")
    if ledger.get("latest_phase") != 37:
        reasons.append("latest_phase_not_37")
    entries = ledger.get("entries") or []
    if not isinstance(entries, list) or len(entries) != 11:
        reasons.append(f"expected_11_entries_got:{len(entries)}")
    seen_phases = {e.get("phase") for e in entries
                    if isinstance(e, dict)}
    if seen_phases != set(range(27, 38)):
        reasons.append(f"phase_range_mismatch:{seen_phases}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_boundary_guarantees(
    ledger: Any,
) -> dict[str, Any]:
    if not isinstance(ledger, dict):
        return {"ok": False, "summary": "no_ledger"}
    entries = ledger.get("entries") or []
    adapters = sorted({
        a for e in entries if isinstance(e, dict)
        for a in (e.get("allowed_callable_adapters") or [])
    })
    return {
        "ok": True,
        "summary": (
            f"phase38 ledger: phases=27-{ledger.get('latest_phase')}"
            f" allowed_adapters={len(adapters)} "
            f"full_regression="
            f"{ledger.get('full_regression_total_expected')}"),
        "ledger_id": ledger.get("ledger_id"),
        "adapter_count_distinct": len(adapters),
        "phase": _PHASE,
    }


def write_boundary_guarantee_ledger(
    ledger: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(ledger)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_governance_ledger_report(
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
    "collect_phase_reports",
    "build_boundary_guarantee_ledger",
    "validate_boundary_guarantee_ledger",
    "summarize_boundary_guarantees",
    "write_boundary_guarantee_ledger",
    "write_governance_ledger_report",
]
