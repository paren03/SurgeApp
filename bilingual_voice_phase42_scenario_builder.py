"""Phase 42 - Scenario Builder.

Builds 8 canonical scenarios that exercise the five Phase 41
metadata-only adapters. No unsafe terms echoed in full; no raw
transcripts; no sensitive facts; memory state is summary-only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase42.scenario_builder.v1"


_REQUIRED_SCENARIO_FIELDS = (
    "scenario_id", "scenario_type",
    "user_text", "draft_response_text",
    "conversation_mode",
    "approve", "kill_switch_enabled",
    "expected_status_family",
    "expected_adapter_family",
)


_BANNED_SCENARIO_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


_SAFE_MEMORY_STATE = {
    "preferred_language_mode": "english",
    "preferred_spoken_mode": "neutral",
    "code_switch_density": 0.05,
    "correction_pattern_count": 2,
    "recent_language_modes": ["english", "russian"],
    "recent_correction_kinds": ["pronoun"],
    "continuity_confidence_score": 0.82,
    "memory_scope": "session",
    "persistence_status": "ephemeral",
    "recent_drift_signal": False,
    "voice_style_continuity": "stable",
    "user_preference_drift": False,
    "session_memory_bounded": True,
    "recent_turn_count": 4,
}


def _base(scenario_id: str, scenario_type: str,
           user_text: str,
           draft_response_text: str = "",
           conversation_mode: str = "conversation",
           user_preference: Any = None,
           preferred_adapter: Any = None,
           voice_memory_state: Any = None,
           approve: bool = True,
           kill_switch_enabled: bool = False,
           expected_status_family: str = "ok",
           expected_adapter_family: Any = None,
           note: str = "") -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "scenario_type": scenario_type,
        "user_text": user_text,
        "draft_response_text": draft_response_text,
        "conversation_mode": conversation_mode,
        "user_preference": user_preference,
        "preferred_adapter": preferred_adapter,
        "voice_memory_state": voice_memory_state,
        "approve": bool(approve),
        "kill_switch_enabled": bool(kill_switch_enabled),
        "expected_status_family": expected_status_family,
        "expected_adapter_family":
            expected_adapter_family,
        "notes": [note] if note else [],
        "phase": _PHASE,
    }


def create_simple_english_scenario() -> dict[str, Any]:
    return _base(
        "P42-S01", "simple_english",
        "Hello Luna",
        draft_response_text="Hi.",
        user_preference="english",
        preferred_adapter="dummy_metadata_adapter",
        expected_adapter_family="dummy_metadata_adapter",
        note="Simple single-language English baseline.")


def create_russian_first_scenario() -> dict[str, Any]:
    # Safe Russian text ("Hello Luna")
    return _base(
        "P42-S02", "russian_first",
        "Привет Луна",
        draft_response_text="Привет!",
        user_preference="russian",
        preferred_adapter=
            "bilingual_segment_metadata_adapter",
        expected_adapter_family=
            "bilingual_segment_metadata_adapter",
        note="Russian-first safe greeting.")


def create_mixed_code_switch_scenario() -> dict[str, Any]:
    return _base(
        "P42-S03", "mixed_code_switch",
        "Mix russian and english",
        draft_response_text="ok, давай.",
        expected_adapter_family=
            "bilingual_segment_metadata_adapter",
        note="Safe EN/RU code-switch.")


def create_high_prosody_scenario() -> dict[str, Any]:
    return _base(
        "P42-S04", "high_prosody",
        "Slow with pauses and emphasis",
        preferred_adapter=
            "prosody_density_metadata_adapter",
        expected_adapter_family=
            "prosody_density_metadata_adapter",
        note="High prosody-density signal.")


def create_safety_redaction_scenario() -> dict[str, Any]:
    # No unsafe term echoed in full; we only ask for a
    # redaction trace check.
    return _base(
        "P42-S05", "safety_redaction",
        "Safety redaction check",
        preferred_adapter=
            "safety_redaction_trace_metadata_adapter",
        expected_adapter_family=
            "safety_redaction_trace_metadata_adapter",
        note=("Safety redaction trace — no unsafe terms "
               "echoed."))


def create_memory_continuity_scenario() -> dict[str, Any]:
    return _base(
        "P42-S06", "memory_continuity",
        "Continuity audit drill",
        preferred_adapter=
            "memory_continuity_audit_metadata_adapter",
        voice_memory_state=dict(_SAFE_MEMORY_STATE),
        expected_adapter_family=
            "memory_continuity_audit_metadata_adapter",
        note=("Memory continuity audit — sanitized memory "
               "state only."))


def create_refusal_scenario() -> dict[str, Any]:
    return _base(
        "P42-S07", "approve_false_refusal",
        "Refusal drill (approve=False)",
        approve=False,
        expected_status_family="refused",
        expected_adapter_family=None,
        note="Operator refuses; adapter must not be called.")


def create_kill_switch_scenario() -> dict[str, Any]:
    return _base(
        "P42-S08", "kill_switch_refusal",
        "Kill-switch drill",
        approve=True, kill_switch_enabled=True,
        expected_status_family="kill_switch_blocked",
        expected_adapter_family=None,
        note=("Kill switch engaged; all adapter calls "
               "blocked."))


def create_phase42_scenarios() -> list[dict[str, Any]]:
    return [
        create_simple_english_scenario(),
        create_russian_first_scenario(),
        create_mixed_code_switch_scenario(),
        create_high_prosody_scenario(),
        create_safety_redaction_scenario(),
        create_memory_continuity_scenario(),
        create_refusal_scenario(),
        create_kill_switch_scenario(),
    ]


def validate_phase42_scenario(
    scenario: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(scenario, dict):
        return {"ok": False,
                "reasons": ["scenario_not_dict"]}
    for f in _REQUIRED_SCENARIO_FIELDS:
        if f not in scenario:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_SCENARIO_FIELDS:
        if k in scenario and scenario.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_scenario_field:{k}")
    # voice_memory_state must be sanitized (or None / empty)
    vms = scenario.get("voice_memory_state")
    if isinstance(vms, dict):
        for k in _BANNED_SCENARIO_FIELDS:
            if k in vms and vms.get(k) not in (
                    None, "", False, [], {}):
                reasons.append(f"banned_vms_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase42_scenario_report(
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
    "create_phase42_scenarios",
    "create_simple_english_scenario",
    "create_russian_first_scenario",
    "create_mixed_code_switch_scenario",
    "create_high_prosody_scenario",
    "create_safety_redaction_scenario",
    "create_memory_continuity_scenario",
    "create_refusal_scenario",
    "create_kill_switch_scenario",
    "validate_phase42_scenario",
    "write_phase42_scenario_report",
]
