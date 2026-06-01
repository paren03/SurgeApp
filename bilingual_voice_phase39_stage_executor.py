"""Phase 39 - Per-Scenario Stage Executor.

Runs one rehearsal scenario via the Phase 37 four-adapter
runtime and captures a stage receipt. The Phase 37 runtime
internally drives Phase 28 -> 29 -> 30/31/33/37 selection ->
32 signing -> 34 witness export -> 35 exchange -> optional 36
handoff. The executor never enables audio/TTS/subprocess/etc.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_adapter_phase37_runtime as p37rt


_PHASE = "phase39.stage_executor.v1"


_REQUIRED_RECEIPT_FIELDS = (
    "receipt_id", "created_at", "phase",
    "scenario_id", "scenario_label",
    "status",
    "selected_adapter_name",
    "signed_pipeline_status",
    "stages_present",
    "kill_switch_blocked",
)


_BANNED_FIELDS = (
    "produced_audio", "invoked_tts",
    "used_subprocess", "used_network", "wrote_files",
)


def _safe_get_chain(invocation_output: dict[str, Any]
                     ) -> list[dict[str, Any]]:
    ch = invocation_output.get("audit_chain") or []
    if not isinstance(ch, list):
        return []
    return [e for e in ch if isinstance(e, dict)]


def _detect_stages_present(
    invocation_output: dict[str, Any],
) -> dict[str, bool]:
    chain = _safe_get_chain(invocation_output)
    chain_events = {str(e.get("event_type") or e.get("type")
                         or e.get("stage") or "")
                     for e in chain}
    pipe = invocation_output.get(
        "signed_witness_pipeline") or {}
    return {
        "phase29_per_invocation_consent": bool(
            invocation_output.get("phase29_packet")),
        "phase30_callable_boundary": bool(
            invocation_output.get(
                "phase37_adapter_request")),
        "phase31_two_adapter_selection": bool(
            invocation_output.get("selection_choice")),
        "phase32_audit_chain_signing": bool(
            (pipe.get("signed_evidence_summary") or {})
            .get("algorithm")),
        "phase33_three_adapter_signed_evidence": bool(
            (pipe.get("signed_evidence_summary") or {})
            .get("evidence_validates")),
        "phase34_witness_export": (
            (pipe.get("witness_export_summary") or {})
            .get("status") == "ok"),
        "phase35_local_exchange": (
            (pipe.get("exchange_summary") or {})
            .get("status") in ("ok", "witness_failed",
                                "skipped", "")),
        "phase36_optional_handoff": (
            (pipe.get("handoff_summary") or {}).get("status")
            in ("ok", "skipped", "not_requested", "",
                None)),
        "phase37_signed_witness_pipeline": bool(pipe),
        "audit_chain_present": len(chain_events) > 0,
    }


def _check_no_runtime_leaks(
    invocation_output: dict[str, Any],
) -> dict[str, Any]:
    leaks: list[str] = []
    # Walk top-level dict; cheap, bounded
    for k in _BANNED_FIELDS:
        if invocation_output.get(k) is True:
            leaks.append(f"top:{k}")
    sel = invocation_output.get(
        "selected_adapter_result") or {}
    if isinstance(sel, dict):
        for k in _BANNED_FIELDS:
            if sel.get(k) is True:
                leaks.append(f"adapter:{k}")
    return {"ok": not leaks, "leaks": leaks}


def execute_scenario(
    scenario: dict[str, Any],
    operator_id: str = "operator_local",
) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {
            "receipt_id": f"recpt_{int(time.time())}",
            "phase": _PHASE,
            "status": "refused",
            "reason": "scenario_not_dict",
        }
    sid = str(scenario.get("scenario_id") or "")
    label = str(scenario.get("label") or "")
    invocation = p37rt.prepare_phase37_four_adapter_invocation(
        user_text=str(scenario.get("user_text") or ""),
        draft_response_text=str(
            scenario.get("draft_response_text") or ""),
        conversation_mode=str(
            scenario.get("conversation_mode")
            or "conversation"),
        user_preference=scenario.get("user_preference"),
        preferred_adapter=scenario.get("preferred_adapter"),
        operator_id=operator_id,
        approve=bool(scenario.get("approve", True)),
        kill_switch_enabled=bool(
            scenario.get("kill_switch_enabled", False)),
        sign_evidence=True,
        include_witness_export=True,
        include_exchange=True,
        include_handoff=bool(
            scenario.get("include_handoff", False)),
    )
    status = invocation.get("status") or "unknown"
    sel = invocation.get("selection_choice") or {}
    chosen = sel.get("chosen") if isinstance(sel, dict) else {}
    adapter_name = (chosen.get("adapter_name")
                    if isinstance(chosen, dict) else None)
    pipe = invocation.get("signed_witness_pipeline") or {}
    stages = _detect_stages_present(invocation)
    leak_check = _check_no_runtime_leaks(invocation)
    chain_len = len(_safe_get_chain(invocation))
    return {
        "receipt_id": f"recpt_{sid}_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "scenario_id": sid,
        "scenario_label": label,
        "status": status,
        "selected_adapter_name": adapter_name,
        "signed_pipeline_status": pipe.get("status"),
        "kill_switch_blocked": status == "kill_switch_blocked",
        "approve_requested": bool(
            scenario.get("approve", True)),
        "include_handoff_requested": bool(
            scenario.get("include_handoff", False)),
        "stages_present": stages,
        "audit_chain_length": chain_len,
        "no_runtime_leak": leak_check.get("ok") is True,
        "runtime_leak_details": leak_check.get("leaks", []),
        "expected_adapter_family":
            scenario.get("expected_adapter_family"),
        "notes": [
            "Receipt does NOT carry signing key material.",
            "Receipt does NOT carry raw operator_id.",
            "Receipt does NOT carry the spoken render "
            "payload.",
        ],
    }


def validate_scenario_receipt(
    receipt: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    for f in _REQUIRED_RECEIPT_FIELDS:
        if f not in receipt:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_FIELDS:
        if k in receipt:
            reasons.append(f"banned_field_present:{k}")
    if receipt.get("no_runtime_leak") is False:
        reasons.append("runtime_leak_detected")
    if "operator_id" in receipt and receipt.get(
            "operator_id") not in (None, ""):
        reasons.append("raw_operator_id_present")
    for secret_field in ("signing_key_material",
                          "private_key", "material_hex",
                          "sealed_payload"):
        if secret_field in receipt:
            reasons.append(f"secret_field:{secret_field}")
    return {"ok": not reasons, "reasons": reasons}


def write_scenario_receipt(
    receipt: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(receipt)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "execute_scenario",
    "validate_scenario_receipt",
    "write_scenario_receipt",
]
