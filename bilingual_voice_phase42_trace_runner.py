"""Phase 42 - Trace Runner.

Runs each Phase 42 scenario via the Phase 41 runtime and captures
a bounded result. No raw operator_id, no audio, no subprocess.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_phase41_runtime as p41rt
import bilingual_voice_phase42_scenario_builder as sb


_PHASE = "phase42.trace_runner.v1"


_REQUIRED_RESULT_FIELDS = (
    "trace_id", "created_at", "phase",
    "scenario_id", "scenario_type",
    "status", "selected_adapter_name",
    "signed_pipeline_status",
    "kill_switch_blocked",
    "replay_projection_present",
    "result_verification_ok",
    "governance_recheck_ok",
)


_BANNED_TRACE_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def _new_id() -> str:
    return f"p42tr_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase42_trace_scenario(
    scenario: dict[str, Any],
    operator_id: str = "phase42_operator",
) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {
            "trace_id": _new_id(),
            "created_at": time.time(),
            "phase": _PHASE,
            "scenario_id": "",
            "scenario_type": "",
            "status": "refused",
            "reason": "scenario_not_dict",
        }
    sval = sb.validate_phase42_scenario(scenario)
    if not sval.get("ok"):
        return {
            "trace_id": _new_id(),
            "created_at": time.time(),
            "phase": _PHASE,
            "scenario_id": scenario.get("scenario_id", ""),
            "scenario_type": scenario.get("scenario_type",
                                            ""),
            "status": "refused",
            "reason": "scenario_invalid",
            "scenario_validation": sval,
        }
    invocation = p41rt.prepare_phase41_five_adapter_invocation(
        user_text=str(scenario.get("user_text") or ""),
        draft_response_text=str(
            scenario.get("draft_response_text") or ""),
        conversation_mode=str(
            scenario.get("conversation_mode")
            or "conversation"),
        user_preference=scenario.get("user_preference"),
        preferred_adapter=scenario.get(
            "preferred_adapter"),
        voice_memory_state=scenario.get(
            "voice_memory_state"),
        operator_id=str(operator_id or ""),
        approve=bool(scenario.get("approve", True)),
        kill_switch_enabled=bool(
            scenario.get("kill_switch_enabled", False)),
        sign_evidence=True,
        include_witness_export=True,
        include_exchange=True,
        include_replay_projection=True,
    )
    status = str(invocation.get("status") or "unknown")
    sel = invocation.get("selection_choice") or {}
    chosen = sel.get("chosen") if isinstance(sel, dict) \
        else {}
    adapter_name = (chosen.get("adapter_name")
                    if isinstance(chosen, dict) else None)
    pipe = invocation.get("signed_witness_pipeline") or {}
    rv = invocation.get("result_verification") or {}
    gr = invocation.get("governance_recheck") or {}
    proj = invocation.get("replay_projection") or {}
    selected_result = invocation.get(
        "selected_adapter_result") or {}
    expected_status = str(scenario.get(
        "expected_status_family") or "ok")
    expected_adapter = scenario.get(
        "expected_adapter_family")
    return {
        "trace_id": _new_id(),
        "created_at": time.time(),
        "phase": _PHASE,
        "scenario_id": scenario.get("scenario_id", ""),
        "scenario_type": scenario.get("scenario_type", ""),
        "status": status,
        "expected_status_family": expected_status,
        "status_matches_expected":
            expected_status in status
            or status in expected_status,
        "selected_adapter_name": adapter_name,
        "expected_adapter_family": expected_adapter,
        "adapter_matches_expected":
            (expected_adapter is None
             or adapter_name == expected_adapter),
        "signed_pipeline_status": pipe.get("status"),
        "kill_switch_blocked":
            status == "kill_switch_blocked",
        "approve_requested": bool(
            scenario.get("approve", True)),
        "kill_switch_requested": bool(
            scenario.get("kill_switch_enabled", False)),
        "replay_projection_present": bool(proj),
        "result_verification_ok": bool(rv.get("ok")),
        "governance_recheck_ok": bool(gr.get("ok")),
        "signed_evidence_validates": bool(
            (pipe.get("signed_evidence_summary") or {})
            .get("evidence_validates")),
        "witness_export_status":
            (pipe.get("witness_export_summary") or {})
            .get("status"),
        "exchange_status":
            (pipe.get("exchange_summary") or {})
            .get("status"),
        "selected_result_metadata": {
            "adapter_type":
                selected_result.get("adapter_type"),
            "produced_audio":
                selected_result.get("produced_audio"),
            "invoked_tts":
                selected_result.get("invoked_tts"),
            "used_subprocess":
                selected_result.get("used_subprocess"),
            "used_network":
                selected_result.get("used_network"),
            "wrote_files":
                selected_result.get("wrote_files"),
            "raw_transcript_absent":
                selected_result.get(
                    "raw_transcript_absent"),
            "sensitive_fact_absent":
                selected_result.get(
                    "sensitive_fact_absent"),
        },
        "replay_projection_summary": {
            "projection_id": proj.get("projection_id"),
            "selected_adapter_name":
                proj.get("selected_adapter_name"),
            "trace_hash":
                (proj.get("trace_hash_summary") or {})
                .get("trace_hash"),
            "phase21_status_text":
                (proj.get("phase21_status") or {})
                .get("status_text"),
        },
        "notes": [
            "Result carries no raw operator_id.",
            "Result carries no signing material.",
            "Result carries no raw transcript or "
            "sensitive facts.",
        ],
    }


def run_phase42_trace_batch(
    scenarios: Optional[list[dict[str, Any]]] = None,
    operator_id: str = "phase42_operator",
    limit: int = 12,
) -> list[dict[str, Any]]:
    scen = list(scenarios) if scenarios else \
        sb.create_phase42_scenarios()
    cap = max(1, min(int(limit or 1), 12))
    out: list[dict[str, Any]] = []
    for s in scen[:cap]:
        out.append(run_phase42_trace_scenario(
            s, operator_id=operator_id))
    return out


def validate_phase42_trace_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    for f in _REQUIRED_RESULT_FIELDS:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_TRACE_FIELDS:
        if k in result and result.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase42_trace_batch(
    results: list[Any],
) -> dict[str, Any]:
    if not isinstance(results, list):
        return {"ok": False, "summary": "no_results"}
    total = len(results)
    ok_count = sum(1 for r in results
                    if isinstance(r, dict)
                    and r.get("status") == "ok")
    refused = sum(1 for r in results
                   if isinstance(r, dict)
                   and "refused" in str(r.get("status")
                                          or ""))
    ks = sum(1 for r in results
              if isinstance(r, dict)
              and r.get("status") == "kill_switch_blocked")
    adapter_dist: dict[str, int] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        name = r.get("selected_adapter_name") or "none"
        adapter_dist[name] = adapter_dist.get(name, 0) + 1
    return {
        "ok": True,
        "summary": (
            f"phase42 traces: total={total} ok={ok_count} "
            f"refused={refused} kill_switch_blocked={ks}"),
        "trace_count": total,
        "ok_count": ok_count,
        "refused_count": refused,
        "kill_switch_blocked_count": ks,
        "adapter_distribution": adapter_dist,
        "phase": _PHASE,
    }


def write_phase42_trace_batch(
    results: list[dict[str, Any]],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = {"phase": _PHASE,
            "written_at": time.time(),
            "trace_count": len(results),
            "traces": results}
    p.write_text(json.dumps(body, ensure_ascii=False,
                              indent=2, default=str),
                  encoding="utf-8")
    return str(p)


def write_phase42_trace_runner_report(
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
    "run_phase42_trace_scenario",
    "run_phase42_trace_batch",
    "validate_phase42_trace_result",
    "summarize_phase42_trace_batch",
    "write_phase42_trace_batch",
    "write_phase42_trace_runner_report",
]
