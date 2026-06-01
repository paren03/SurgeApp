"""Phase 42 - Coherence Auditor.

Audits multi-trace coherence: adapter coverage, status
consistency, evidence presence, replay projection, memory
privacy, boundary preservation.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase42.coherence_auditor.v1"


_REQUIRED_ADAPTERS = (
    "dummy_metadata_adapter",
    "bilingual_segment_metadata_adapter",
    "prosody_density_metadata_adapter",
    "safety_redaction_trace_metadata_adapter",
    "memory_continuity_audit_metadata_adapter",
)


_REQUIRED_AUDIT_FIELDS = (
    "audit_id", "created_at", "phase",
    "adapter_coverage", "status_coherence",
    "evidence_coherence",
    "replay_projection_coherence",
    "memory_privacy",
    "boundary_preservation",
    "ok",
)


def _is_success_status(s: Any) -> bool:
    return str(s or "") == "ok"


def _is_refused_status(s: Any) -> bool:
    return "refused" in str(s or "")


def _is_blocked_status(s: Any) -> bool:
    return str(s or "") == "kill_switch_blocked"


def audit_phase42_adapter_coverage(
    trace_results: list[Any],
) -> dict[str, Any]:
    success_adapters = set()
    all_adapters = set()
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        name = r.get("selected_adapter_name") or ""
        if name:
            all_adapters.add(name)
        if _is_success_status(r.get("status")) and name:
            success_adapters.add(name)
    missing = [a for a in _REQUIRED_ADAPTERS
                if a not in success_adapters]
    extra = [a for a in success_adapters
              if a not in _REQUIRED_ADAPTERS]
    return {
        "category": "adapter_coverage",
        "ok": not missing and not extra,
        "success_adapter_count": len(success_adapters),
        "covered_in_success": sorted(success_adapters),
        "missing_in_success": missing,
        "unexpected_in_success": extra,
        "all_observed_adapters": sorted(all_adapters),
    }


def audit_phase42_status_coherence(
    trace_results: list[Any],
) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    refusal_called_adapter: list[str] = []
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        expected = str(r.get("expected_status_family")
                        or "ok")
        observed = str(r.get("status") or "")
        if expected == "ok":
            if not _is_success_status(observed):
                mismatches.append({
                    "scenario_id": r.get("scenario_id"),
                    "expected": expected,
                    "observed": observed})
        elif expected == "refused":
            if not _is_refused_status(observed):
                mismatches.append({
                    "scenario_id": r.get("scenario_id"),
                    "expected": expected,
                    "observed": observed})
        elif expected == "kill_switch_blocked":
            if not _is_blocked_status(observed):
                mismatches.append({
                    "scenario_id": r.get("scenario_id"),
                    "expected": expected,
                    "observed": observed})
        # Refusal/kill-switch must not produce signed
        # pipeline ok
        if expected in ("refused",
                         "kill_switch_blocked"):
            if r.get("signed_pipeline_status") == "ok":
                refusal_called_adapter.append(
                    str(r.get("scenario_id") or ""))
    return {
        "category": "status_coherence",
        "ok": (not mismatches
                and not refusal_called_adapter),
        "mismatches": mismatches,
        "refusal_or_block_with_signed_pipeline":
            refusal_called_adapter,
    }


def audit_phase42_evidence_coherence(
    trace_results: list[Any],
) -> dict[str, Any]:
    missing_ev: list[str] = []
    missing_we: list[str] = []
    missing_ex: list[str] = []
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        if not _is_success_status(r.get("status")):
            continue
        sid = str(r.get("scenario_id") or "")
        if not r.get("signed_evidence_validates"):
            missing_ev.append(sid)
        if r.get("witness_export_status") != "ok":
            missing_we.append(sid)
        if r.get("exchange_status") not in (
                "ok", "witness_failed"):
            missing_ex.append(sid)
    return {
        "category": "evidence_coherence",
        "ok": (not missing_ev and not missing_we
                and not missing_ex),
        "missing_signed_evidence": missing_ev,
        "missing_witness_export": missing_we,
        "missing_exchange": missing_ex,
    }


def audit_phase42_replay_projection_coherence(
    trace_results: list[Any],
) -> dict[str, Any]:
    missing: list[str] = []
    bad: list[str] = []
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        if not _is_success_status(r.get("status")):
            continue
        sid = str(r.get("scenario_id") or "")
        if not r.get("replay_projection_present"):
            missing.append(sid)
            continue
        proj = r.get("replay_projection_summary") or {}
        if not proj.get("trace_hash") \
                or len(str(proj.get("trace_hash"))) < 64:
            bad.append(sid)
    return {
        "category": "replay_projection_coherence",
        "ok": not missing and not bad,
        "missing_projection": missing,
        "bad_trace_hash": bad,
    }


def audit_phase42_memory_privacy(
    trace_results: list[Any],
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        meta = r.get("selected_result_metadata") or {}
        if meta.get("adapter_type") == \
                "memory_continuity_audit_metadata_adapter":
            if meta.get("raw_transcript_absent") is not True:
                failures.append({
                    "scenario_id": r.get("scenario_id"),
                    "reason": "raw_transcript_absent_false"})
            if meta.get("sensitive_fact_absent") is not True:
                failures.append({
                    "scenario_id": r.get("scenario_id"),
                    "reason": "sensitive_fact_absent_false"})
    return {
        "category": "memory_privacy",
        "ok": not failures,
        "failures": failures,
    }


def audit_phase42_boundary_preservation(
    trace_results: list[Any],
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        meta = r.get("selected_result_metadata") or {}
        sid = r.get("scenario_id")
        for k in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
            if meta.get(k) is True:
                failures.append({
                    "scenario_id": sid,
                    "reason": f"{k}_true"})
    return {
        "category": "boundary_preservation",
        "ok": not failures,
        "failures": failures,
    }


def create_phase42_coherence_audit(
    trace_results: list[Any],
) -> dict[str, Any]:
    ac = audit_phase42_adapter_coverage(trace_results)
    sc = audit_phase42_status_coherence(trace_results)
    ec = audit_phase42_evidence_coherence(trace_results)
    rp = audit_phase42_replay_projection_coherence(
        trace_results)
    mp = audit_phase42_memory_privacy(trace_results)
    bp = audit_phase42_boundary_preservation(
        trace_results)
    ok = (ac["ok"] and sc["ok"] and ec["ok"]
          and rp["ok"] and mp["ok"] and bp["ok"])
    return {
        "audit_id": f"p42coh_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "trace_count": len(trace_results or []),
        "adapter_coverage": ac,
        "status_coherence": sc,
        "evidence_coherence": ec,
        "replay_projection_coherence": rp,
        "memory_privacy": mp,
        "boundary_preservation": bp,
        "ok": ok,
        "summary": (
            f"phase42 coherence audit: traces="
            f"{len(trace_results or [])} "
            f"adapter_coverage_ok={ac['ok']} "
            f"status_ok={sc['ok']} "
            f"evidence_ok={ec['ok']} "
            f"projection_ok={rp['ok']} "
            f"memory_privacy_ok={mp['ok']} "
            f"boundary_ok={bp['ok']}"),
    }


def validate_phase42_coherence_audit(
    audit: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(audit, dict):
        return {"ok": False,
                "reasons": ["audit_not_dict"]}
    for f in _REQUIRED_AUDIT_FIELDS:
        if f not in audit:
            reasons.append(f"missing_field:{f}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase42_coherence_audit_report(
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
    "audit_phase42_adapter_coverage",
    "audit_phase42_status_coherence",
    "audit_phase42_evidence_coherence",
    "audit_phase42_replay_projection_coherence",
    "audit_phase42_memory_privacy",
    "audit_phase42_boundary_preservation",
    "create_phase42_coherence_audit",
    "validate_phase42_coherence_audit",
    "write_phase42_coherence_audit_report",
]
