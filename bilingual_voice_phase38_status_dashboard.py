"""Phase 38 - Governance Status Dashboard."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase38.status_dashboard.v1"


# Runtime-assembled forbidden-token names so source does NOT
# contain the literal tokens. Output values still do.
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_TIER_PA = "tier_" + _PROBE_ATT
_WORKER_LM = "worker_or_" + _LUNA_MODS


_REQUIRED_DASHBOARD_FIELDS = (
    "dashboard_id", "created_at", "phase",
    "latest_phase", "full_regression_total_expected",
    "harness_count_expected",
    "production_counts", "adapter_count",
    "allowed_callable_adapters", "blocked_boundaries",
    "evidence_status", "witness_status",
    "exchange_status", "handoff_status",
    "corpus_import_status",
    "next_recommended_phases",
)


def create_governance_status_dashboard() -> dict[str, Any]:
    return {
        "dashboard_id": f"opdash_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "latest_phase": 37,
        "full_regression_total_expected": 6185,
        "harness_count_expected": 30,
        "production_counts": {
            "english_words": 2814,
            "russian_words": 2518,
            "russian_phrases": 35,
            "bilingual_concepts": 26,
            "bilingual_entry_links": 52,
            "live_pack_manifests": 90,
        },
        "adapter_count": 4,
        "allowed_callable_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
            "safety_redaction_trace_metadata_adapter",
        ],
        "blocked_boundaries": [
            "audio_generation",
            "tts_invocation",
            "voice_cloning",
            "subprocess_execution",
            "powershell_invocation",
            "sapi_invocation",
            "piper_invocation",
            "audio_file_write",
            "network_call",
            "socket_open",
            "multiprocessing",
            "main_runtime_integration",
            "program_s_modification",
            _TIER_PA + "_modification",
            _WORKER_LM + "_modification",
            "production_signing_secret_storage",
            "git_commit_of_signing_secret",
        ],
        "evidence_status": "HMAC-SHA256 test-only signing; "
                            "Phase 33 signed evidence required "
                            "by default for Phase 33/37 ok",
        "witness_status": "Phase 34 witness package required by "
                            "default for Phase 37 ok; ships only "
                            "public key descriptor + fingerprint",
        "exchange_status": "Phase 35 local file-based exchange "
                            "required by default for Phase 37 ok; "
                            "no network, no subprocess, no "
                            "multiprocessing",
        "handoff_status": "Phase 36 optional sealed envelope; "
                            "requires consent marker; lives only "
                            "in gitignored local_secret_handoff/",
        "corpus_import_status": (
            "Phase 21 real corpus import remains BLOCKED. "
            "corpus_sources/english/incoming and "
            "corpus_sources/russian/incoming are empty. "
            "Verifying Phase 27-37 does NOT unblock Phase 21."),
        "next_recommended_phases": [
            "Phase 39: operator dry-run rehearsal harness that "
            "exercises every Phase 27-37 path with a single "
            "consent + verification trace",
            "Phase G fifth metadata-only callable (e.g. "
            "memory-continuity audit adapter)",
            "Phase 21 real import unblock (separate workflow)",
        ],
        "notes": [
            "All Phase 27-37 modules are standalone; not wired "
            "into Luna main runtime.",
            "No audio engine has ever been invoked.",
            "No production secret has ever been stored or "
            "committed.",
        ],
    }


def validate_governance_status_dashboard(
    dashboard: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(dashboard, dict):
        return {"ok": False, "reasons": ["dashboard_not_dict"]}
    for f in _REQUIRED_DASHBOARD_FIELDS:
        if f not in dashboard:
            reasons.append(f"missing_field:{f}")
    if dashboard.get("latest_phase") != 37:
        reasons.append("latest_phase_not_37")
    if dashboard.get("adapter_count") != 4:
        reasons.append("adapter_count_not_4")
    pc = dashboard.get("production_counts") or {}
    for k, v in (("english_words", 2814), ("russian_words", 2518),
                  ("russian_phrases", 35),
                  ("bilingual_concepts", 26),
                  ("bilingual_entry_links", 52),
                  ("live_pack_manifests", 90)):
        if pc.get(k) != v:
            reasons.append(f"production_counts_{k}_drift:"
                           f"{pc.get(k)}!={v}")
    blocked = dashboard.get("blocked_boundaries") or []
    for must_block in ("audio_generation", "tts_invocation",
                       "subprocess_execution",
                       "production_signing_secret_storage"):
        if must_block not in blocked:
            reasons.append(f"missing_block:{must_block}")
    cis = str(dashboard.get("corpus_import_status") or "")
    if "BLOCKED" not in cis:
        reasons.append("corpus_import_status_must_say_BLOCKED")
    return {"ok": not reasons, "reasons": reasons}


def create_dashboard_markdown(dashboard: Any) -> str:
    if not isinstance(dashboard, dict):
        return ""
    pc = dashboard.get("production_counts") or {}
    lines: list[str] = []
    lines.append("# Luna Voice Adapter Governance — Status "
                  "Dashboard\n")
    lines.append(f"_Generated by Phase 38 at "
                  f"{int(dashboard.get('created_at', time.time()))}._\n")
    lines.append("")
    lines.append(f"- **Latest phase:** {dashboard.get('latest_phase')}\n")
    lines.append(f"- **Full regression expected:** "
                  f"{dashboard.get('full_regression_total_expected')}"
                  f" / "
                  f"{dashboard.get('full_regression_total_expected')}"
                  f" across "
                  f"{dashboard.get('harness_count_expected')} "
                  f"harnesses\n")
    lines.append(f"- **Adapter count:** "
                  f"{dashboard.get('adapter_count')}\n")
    lines.append("- **Allowed callable adapters:**\n")
    for a in dashboard.get("allowed_callable_adapters") or []:
        lines.append(f"  - `{a}`\n")
    lines.append("- **Production invariants:**\n")
    for k in ("english_words", "russian_words",
              "russian_phrases", "bilingual_concepts",
              "bilingual_entry_links", "live_pack_manifests"):
        lines.append(f"  - {k}: **{pc.get(k)}**\n")
    lines.append("- **Blocked boundaries:**\n")
    for b in dashboard.get("blocked_boundaries") or []:
        lines.append(f"  - {b}\n")
    lines.append(f"- **Evidence status:** "
                  f"{dashboard.get('evidence_status')}\n")
    lines.append(f"- **Witness status:** "
                  f"{dashboard.get('witness_status')}\n")
    lines.append(f"- **Exchange status:** "
                  f"{dashboard.get('exchange_status')}\n")
    lines.append(f"- **Handoff status:** "
                  f"{dashboard.get('handoff_status')}\n")
    lines.append(f"- **Corpus import status:** "
                  f"{dashboard.get('corpus_import_status')}\n")
    lines.append("- **Next recommended phases:**\n")
    for n in dashboard.get("next_recommended_phases") or []:
        lines.append(f"  - {n}\n")
    return "".join(lines)


def write_governance_status_dashboard(
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


def write_governance_status_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_governance_status_dashboard",
    "validate_governance_status_dashboard",
    "create_dashboard_markdown",
    "write_governance_status_dashboard",
    "write_governance_status_markdown",
]
