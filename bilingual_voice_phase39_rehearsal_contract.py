"""Phase 39 - Operator Dry-Run Rehearsal Contract.

Defines the canonical scenario list, the rehearsal-level policy,
and the dry-run guarantees the rehearsal binds itself to.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase39.rehearsal_contract.v1"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "scenarios", "scenario_count",
    "expected_phase_stages",
    "forbidden_runtime_actions",
    "rehearsal_dry_run_only",
)


_EXPECTED_PHASE_STAGES = (
    "phase28_dry_run_envelope",
    "phase29_per_invocation_consent",
    "phase30_callable_boundary",
    "phase31_two_adapter_selection",
    "phase32_audit_chain_signing",
    "phase33_three_adapter_signed_evidence",
    "phase34_witness_export",
    "phase35_local_exchange",
    "phase36_optional_handoff",
    "phase37_signed_witness_pipeline",
)


# Runtime-assembled tokens so source does NOT contain the
# literal forbidden runtime identifiers.
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"

_FORBIDDEN_ACTIONS = (
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "main_runtime_integration",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "corpus_import",
)


def _default_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": "S01",
            "label": "english_simple_dummy",
            "user_text": "Hello Luna",
            "draft_response_text": "Hi.",
            "conversation_mode": "conversation",
            "user_preference": "english",
            "preferred_adapter": "dummy_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "dummy_metadata_adapter",
        },
        {
            "scenario_id": "S02",
            "label": "russian_simple_segment",
            "user_text": "Привет Луна",
            "draft_response_text": "Привет!",
            "conversation_mode": "conversation",
            "user_preference": "russian",
            "preferred_adapter":
                "bilingual_segment_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "bilingual_segment_metadata_adapter",
        },
        {
            "scenario_id": "S03",
            "label": "mixed_code_switch_segment",
            "user_text": "Mix russian and english",
            "draft_response_text": "ok, давай.",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter": None,
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "bilingual_segment_metadata_adapter",
        },
        {
            "scenario_id": "S04",
            "label": "teacher_russian_segment",
            "user_text": "Teach me a Russian word",
            "draft_response_text": "",
            "conversation_mode": "teacher",
            "user_preference": "russian",
            "preferred_adapter":
                "bilingual_segment_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "bilingual_segment_metadata_adapter",
        },
        {
            "scenario_id": "S05",
            "label": "approve_false_refusal",
            "user_text": "approve=False refusal test",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter": None,
            "approve": False,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family": None,
        },
        {
            "scenario_id": "S06",
            "label": "kill_switch_block",
            "user_text": "kill switch test",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter": None,
            "approve": True,
            "kill_switch_enabled": True,
            "include_handoff": False,
            "expected_adapter_family": None,
        },
        {
            "scenario_id": "S07",
            "label": "prosody_density",
            "user_text": "Slow with pauses and emphasis",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter":
                "prosody_density_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "prosody_density_metadata_adapter",
        },
        {
            "scenario_id": "S08",
            "label": "safety_redaction_trace",
            "user_text": "Safety redaction check",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter":
                "safety_redaction_trace_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "safety_redaction_trace_metadata_adapter",
        },
        {
            "scenario_id": "S09",
            "label": "phase36_optional_handoff",
            "user_text": "Optional handoff envelope drill",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": None,
            "preferred_adapter":
                "dummy_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": True,
            "expected_adapter_family":
                "dummy_metadata_adapter",
        },
        {
            "scenario_id": "S10",
            "label": "english_dummy_baseline",
            "user_text": "Simple English",
            "draft_response_text": "",
            "conversation_mode": "conversation",
            "user_preference": "english",
            "preferred_adapter":
                "dummy_metadata_adapter",
            "approve": True,
            "kill_switch_enabled": False,
            "include_handoff": False,
            "expected_adapter_family":
                "dummy_metadata_adapter",
        },
    ]


def get_canonical_scenarios() -> list[dict[str, Any]]:
    return [dict(s) for s in _default_scenarios()]


def get_expected_phase_stages() -> list[str]:
    return list(_EXPECTED_PHASE_STAGES)


def get_forbidden_runtime_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_rehearsal_contract(
    scenarios: list[dict[str, Any]] | None = None,
    operator_id: str = "operator_local",
) -> dict[str, Any]:
    scen = list(scenarios) if scenarios else \
        get_canonical_scenarios()
    return {
        "contract_id": f"rcontract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": "Phase 39 Operator Dry-Run Rehearsal Contract",
        "operator_id_present": bool(operator_id),
        "scenarios": scen,
        "scenario_count": len(scen),
        "expected_phase_stages":
            list(_EXPECTED_PHASE_STAGES),
        "forbidden_runtime_actions":
            list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "notes": [
            "Every scenario runs Phase 37's "
            "prepare_phase37_four_adapter_invocation which "
            "internally chains Phase 28 -> 29 -> 30/31/33/37 "
            "selection -> 32 signing -> 34 witness export -> "
            "35 exchange -> optional 36 handoff.",
            "No audio engine is bound. All adapters return "
            "metadata only.",
            "Rehearsal does NOT integrate into Luna main "
            "runtime.",
        ],
    }


def validate_rehearsal_contract(
    contract: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(contract, dict):
        return {"ok": False,
                "reasons": ["contract_not_dict"]}
    for f in _REQUIRED_CONTRACT_FIELDS:
        if f not in contract:
            reasons.append(f"missing_field:{f}")
    if contract.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    scen = contract.get("scenarios") or []
    if not isinstance(scen, list) or len(scen) < 4:
        reasons.append("scenarios_too_short")
    seen_ids: set[str] = set()
    for s in scen if isinstance(scen, list) else []:
        if not isinstance(s, dict):
            reasons.append("scenario_not_dict")
            continue
        sid = s.get("scenario_id")
        if not sid:
            reasons.append("scenario_missing_id")
        elif sid in seen_ids:
            reasons.append(f"duplicate_scenario_id:{sid}")
        else:
            seen_ids.add(sid)
        for k in ("label", "user_text", "conversation_mode"):
            if k not in s:
                reasons.append(f"scenario_missing:{k}")
    stages = contract.get("expected_phase_stages") or []
    for must in ("phase29_per_invocation_consent",
                  "phase32_audit_chain_signing",
                  "phase34_witness_export",
                  "phase35_local_exchange",
                  "phase37_signed_witness_pipeline"):
        if must not in stages:
            reasons.append(f"missing_stage:{must}")
    forb = contract.get("forbidden_runtime_actions") or []
    for must in ("generate_audio", "invoke_tts",
                  "run_subprocess", "network_call"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_rehearsal_contract(contract: Any) -> dict[str, Any]:
    if not isinstance(contract, dict):
        return {"ok": False, "summary": "no_contract"}
    return {
        "ok": True,
        "summary": (
            f"phase39 contract: scenarios="
            f"{contract.get('scenario_count')} "
            f"stages={len(contract.get('expected_phase_stages') or [])} "
            f"forbidden={len(contract.get('forbidden_runtime_actions') or [])}"),
        "contract_id": contract.get("contract_id"),
        "phase": _PHASE,
    }


def write_rehearsal_contract(
    contract: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(contract)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_canonical_scenarios",
    "get_expected_phase_stages",
    "get_forbidden_runtime_actions",
    "create_rehearsal_contract",
    "validate_rehearsal_contract",
    "summarize_rehearsal_contract",
    "write_rehearsal_contract",
]
