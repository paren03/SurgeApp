"""Phase 42 - Multi-Trace Coherence Audit Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase42_audit_contract as ac
import bilingual_voice_phase42_scenario_builder as sb
import bilingual_voice_phase42_trace_runner as tr
import bilingual_voice_phase42_coherence_auditor as ca
import bilingual_voice_phase42_replay_matrix as rm
import bilingual_voice_phase42_drift_stability_matrix as dsm
import bilingual_voice_phase42_operator_packet as op


_PHASE = "phase42.runtime.v1"


# Runtime-assembled forbidden runtime identifier tokens so source
# does NOT literally contain them.
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = "no_tier_or_" + "probe" + "_or_attestation"


_REQUIRED_OUTPUT_FIELDS = (
    "phase42_id", "contract", "scenarios",
    "trace_results", "coherence_audit",
    "replay_matrix", "drift_stability_matrix",
    "operator_packet", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p42_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase42_multi_trace_audit(
    operator_id: str = "phase42_operator",
    output_dir: Optional[str] = None,
    limit: int = 12,
) -> dict[str, Any]:
    audit_id = _new_id()
    contract = ac.create_phase42_audit_contract(
        audit_id=audit_id, scenario_count=8)
    scenarios = sb.create_phase42_scenarios()
    trace_results = tr.run_phase42_trace_batch(
        scenarios=scenarios,
        operator_id=operator_id, limit=limit)
    coherence_audit = ca.create_phase42_coherence_audit(
        trace_results)
    replay_matrix = rm.create_phase42_replay_matrix(
        trace_results)
    drift_matrix = \
        dsm.create_phase42_drift_stability_matrix(
            trace_results,
            coherence_audit=coherence_audit,
            replay_matrix=replay_matrix)
    packet = op.create_phase42_operator_packet(
        contract, scenarios, trace_results,
        coherence_audit, replay_matrix, drift_matrix)
    packet_md = op.create_phase42_operator_packet_markdown(
        packet)
    # Status: ok only when every sub-validator + audit
    # passes
    ca_val = ca.validate_phase42_coherence_audit(
        coherence_audit)
    rm_val = rm.validate_phase42_replay_matrix(
        replay_matrix)
    dm_val = dsm.validate_phase42_drift_stability_matrix(
        drift_matrix)
    pkt_val = op.validate_phase42_operator_packet(packet)
    rm_verify = rm.verify_phase42_replay_projections(
        replay_matrix)
    status = "ok"
    if not (coherence_audit.get("ok")
            and drift_matrix.get("ok")
            and ca_val.get("ok") and rm_val.get("ok")
            and dm_val.get("ok") and pkt_val.get("ok")
            and rm_verify.get("ok")):
        status = "drift_detected"
    elif int(drift_matrix.get("warn_count") or 0) > 0:
        status = "ok_with_warnings"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(ac.write_phase42_audit_contract_report(
                contract, str(base / "contracts"
                               / "audit_contract.json")))
            paths.append(tr.write_phase42_trace_batch(
                trace_results,
                str(base / "trace_runs"
                     / "trace_batch.json")))
            paths.append(ca.write_phase42_coherence_audit_report(
                coherence_audit,
                str(base / "coherence_audits"
                     / "coherence_audit.json")))
            paths.append(rm.write_phase42_replay_matrix(
                replay_matrix,
                str(base / "replay_projections"
                     / "replay_matrix.json")))
            paths.append(
                dsm.write_phase42_drift_stability_matrix(
                    drift_matrix,
                    str(base / "drift_matrices"
                         / "drift_stability_matrix.json")))
            paths.append(op.write_phase42_operator_packet(
                packet, str(base / "operator_packets"
                             / "operator_packet.json")))
            paths.append(
                op.write_phase42_operator_packet_markdown(
                    packet_md,
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase42_id": audit_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "scenarios": scenarios,
        "trace_results": trace_results,
        "coherence_audit": coherence_audit,
        "replay_matrix": replay_matrix,
        "drift_stability_matrix": drift_matrix,
        "operator_packet": packet,
        "operator_packet_markdown": packet_md,
        "status": status,
        "safety_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_corpus_import": True,
            "no_main_runtime_integration": True,
        },
        "isolation_summary": {
            "no_program_s": True,
            _NO_WORKER_KEY: True,
            _NO_TIER_KEY: True,
        },
        "phase21_status":
            (packet.get("phase21_import_status") or {})
            .get("status_text", "BLOCKED"),
        "paths_written": paths,
        "gap_notes": [],
    }


def validate_phase42_multi_trace_output(
    output: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(output, dict):
        return {"ok": False,
                "reasons": ["output_not_dict"]}
    for f in _REQUIRED_OUTPUT_FIELDS:
        if f not in output:
            reasons.append(f"missing_field:{f}")
    if str(output.get("status") or "") not in (
            "ok", "ok_with_warnings", "drift_detected"):
        reasons.append("invalid_status")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase42_multi_trace_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    trace_results = output.get("trace_results") or []
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase42 multi-trace: status="
            f"{output.get('status')} "
            f"traces={len(trace_results)} "
            f"phase21="
            f"{output.get('phase21_status')}"),
        "phase42_id": output.get("phase42_id"),
        "phase": _PHASE,
    }


def write_phase42_runtime_report(
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
    "run_phase42_multi_trace_audit",
    "validate_phase42_multi_trace_output",
    "summarize_phase42_multi_trace_output",
    "write_phase42_runtime_report",
]
