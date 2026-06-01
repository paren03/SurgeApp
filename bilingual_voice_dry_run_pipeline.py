"""Phase 27 — Voice-Render Dry-Run Pipeline.

Compose Phase 25 spoken-render contract + Phase 26 voice-memory
continuity into a dry-run render job through the Phase 27 adapter
policy. NO audio. NO subprocess. NO renderer invocation. Plan only.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_spoken_render_runtime as srr
import bilingual_voice_memory_runtime as vmr
import bilingual_voice_memory_schema as vms
import bilingual_voice_adapter_contract as vac
import bilingual_voice_adapter_policy as vap
import bilingual_voice_adapter_registry as vreg
import bilingual_voice_adapter_validation as vav


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 200


def _clamp(n: int) -> int:
    try:
        v = int(n)
    except Exception:  # noqa: BLE001
        return _DEFAULT_LIMIT
    return max(1, min(_HARD_LIMIT, v))


def _new_pipeline_id() -> str:
    return f"vdrp_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def validate_dry_run_pipeline_inputs(
    user_text: str,
    draft_response_text: str,
    conversation_mode: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(user_text, str) or not user_text.strip():
        reasons.append("missing_user_text")
    if not isinstance(draft_response_text, str):
        reasons.append("draft_response_text_not_str")
    if not isinstance(conversation_mode, str) or not conversation_mode:
        reasons.append("missing_conversation_mode")
    return {"ok": not reasons, "reasons": reasons}


def generate_spoken_payload_for_pipeline(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
) -> dict[str, Any]:
    return srr.build_spoken_render_payload(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
    )


def choose_dry_run_adapter(
    payload: dict[str, Any],
    adapter_name: Optional[str] = None,
    voice_memory_state: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    pool = vreg.get_builtin_dry_run_adapters()
    if adapter_name:
        forced = vreg.find_adapter_by_name(adapter_name)
        if forced is not None:
            pool = [forced]
    return vap.choose_adapter_for_payload(
        payload, available_adapters=pool,
        voice_memory_state=voice_memory_state)


def _payload_from_spoken_runtime(spoken: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal spoken-render payload shape that the adapter
    policy can score over, derived from the Phase 25 runtime's output."""
    payload: dict[str, Any] = {
        "language_mode": (spoken.get("voice_style_plan") or {})
            .get("chosen_spoken_mode") or "english",
        "voice_safe_text": spoken.get("voice_safe_text") or "",
        "normalized_text": spoken.get("normalized_text") or "",
        "segments": list(spoken.get("segments") or []),
        "prosody": dict(spoken.get("prosody_plan") or {}),
        "pronunciation_notes": list(
            (spoken.get("pronunciation_hints") or {})
            .get("sensitive_terms") or []),
        "code_switch_boundaries": list(
            spoken.get("code_switch_boundaries") or []),
        "safety_summary": dict(spoken.get("safety_summary") or {}),
    }
    return payload


def create_dry_run_render_job(
    payload: dict[str, Any],
    adapter_choice: dict[str, Any],
    voice_memory_state: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    descriptor = (adapter_choice or {}).get("chosen") or \
        vac.create_voice_adapter_descriptor(
            "dry_run_basic", "dry_run_renderer", capabilities={
                "supports_languages": ["en", "ru", "mixed"],
                "supports_code_switching": True,
                "supports_segments": True,
                "supports_prosody": True,
                "supports_pronunciation_hints": True,
            })
    job = vac.create_render_job(
        payload, descriptor,
        voice_memory_state=voice_memory_state,
        render_preferences={
            "prefer_dry_run": True,
            "no_runtime_execution": True,
            "policy_version": (adapter_choice or {}).get(
                "policy_version", "phase27.policy.v1"),
        })
    job["compatibility_result"] = {
        "score": (adapter_choice or {}).get("score", 0.0),
        "compatibility_reasons":
            (adapter_choice or {}).get("compatibility_reasons", []),
        "safety_reasons":
            (adapter_choice or {}).get("safety_reasons", []),
    }
    return vac.normalize_render_job(job)


def build_dry_run_voice_render_job(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    adapter_name: Optional[str] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    return run_dry_run_pipeline(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        adapter_name=adapter_name,
        limit=limit,
    )


def run_dry_run_pipeline(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    adapter_name: Optional[str] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    _clamp(limit)
    input_check = validate_dry_run_pipeline_inputs(
        user_text or "", draft_response_text or "", conversation_mode or "")
    if not input_check["ok"]:
        return {
            "pipeline_id": _new_pipeline_id(),
            "dry_run_status": "rejected_invalid_input",
            "gap_notes": input_check["reasons"],
            "language_detection": {},
            "voice_memory_summary": {},
            "spoken_payload": {},
            "adapter_choice": {},
            "render_job": {},
            "compatibility": {},
            "safety_summary": {},
            "unsupported_features": [],
            "next_required_integration_steps": [],
        }

    spoken = generate_spoken_payload_for_pipeline(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
    )
    payload = _payload_from_spoken_runtime(spoken)

    safety = payload.get("safety_summary") or {}
    if safety.get("blocked") or safety.get("unsafe"):
        return {
            "pipeline_id": _new_pipeline_id(),
            "dry_run_status": "refused_unsafe_payload",
            "language_detection": spoken.get("language_detection") or {},
            "voice_memory_summary": {},
            "spoken_payload": payload,
            "adapter_choice": {},
            "render_job": {},
            "compatibility": {},
            "safety_summary": safety,
            "unsupported_features": ["unsafe_payload"],
            "next_required_integration_steps": [],
            "gap_notes": ["payload_unsafe_refused"],
        }

    # Voice-memory pass (Phase 26)
    vm_state = vms.create_empty_voice_memory_state()
    if conversation_state and isinstance(conversation_state, dict):
        for k in ("preferred_language_mode", "preferred_spoken_mode",
                  "preferred_formality", "preferred_code_switch_density"):
            if k in conversation_state:
                vm_state[k] = conversation_state[k]
    plan = vmr.get_voice_continuity_plan(
        user_text=user_text, state=vm_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference, limit=_clamp(limit))
    vm_summary = {
        "session_id": vm_state.get("session_id"),
        "preferred_language_mode": vm_state.get("preferred_language_mode"),
        "preferred_spoken_mode": vm_state.get("preferred_spoken_mode"),
        "preferred_formality": vm_state.get("preferred_formality"),
        "preferred_code_switch_density":
            vm_state.get("preferred_code_switch_density"),
        "continuity_decision": (plan or {}).get("continuity_decision"),
    }

    choice = choose_dry_run_adapter(
        payload, adapter_name=adapter_name, voice_memory_state=vm_state)
    explain = vap.explain_adapter_choice(choice)

    if not choice.get("chosen"):
        return {
            "pipeline_id": _new_pipeline_id(),
            "dry_run_status": "no_compatible_adapter",
            "language_detection": spoken.get("language_detection") or {},
            "voice_memory_summary": vm_summary,
            "spoken_payload": payload,
            "adapter_choice": choice,
            "adapter_explanation": explain,
            "render_job": {},
            "compatibility": {},
            "safety_summary": safety,
            "unsupported_features": choice.get("compatibility_reasons", []),
            "next_required_integration_steps": [],
            "gap_notes": ["no_dry_run_adapter_matched"],
        }

    job = create_dry_run_render_job(
        payload, choice, voice_memory_state=vm_state)
    job_validation = vac.validate_render_job(job)
    boundary = vav.validate_adapter_boundary(job)

    next_steps: list[str] = []
    if choice["chosen"].get("adapter_type") == "piper_shaped":
        next_steps.append("future_phase: bind piper executable + voices "
                          "behind explicit operator consent + dry-run gate")
    if choice["chosen"].get("adapter_type") == "sapi_shaped":
        next_steps.append("future_phase: bind Windows SAPI behind "
                          "operator-gated runtime adapter, no PowerShell")
    if choice["chosen"].get("adapter_type") == "kokoro_shaped":
        next_steps.append("future_phase: evaluate kokoro license + "
                          "package availability; remain dry-run until ready")
    next_steps.append("future_phase: write runtime adapter under explicit "
                      "operator consent + per-invocation audit log")

    return {
        "pipeline_id": _new_pipeline_id(),
        "dry_run_status": "planned_dry_run" if job_validation["ok"]
            and boundary["ok"] else "validation_failed",
        "language_detection": spoken.get("language_detection") or {},
        "voice_memory_summary": vm_summary,
        "spoken_payload": payload,
        "adapter_choice": choice,
        "adapter_explanation": explain,
        "render_job": job,
        "compatibility": {
            "job_validation": job_validation,
            "boundary_validation": boundary,
        },
        "safety_summary": safety,
        "unsupported_features": choice.get("compatibility_reasons", []),
        "next_required_integration_steps": next_steps,
        "gap_notes": ([] if job_validation["ok"] and boundary["ok"]
                      else job_validation["reasons"] + boundary["reasons"]),
    }


def demo_dry_run_voice_pipeline(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna, can you tell me about the weather?",
         "It's sunny today.", "conversation", None),
        ("Привет Луна, расскажи о погоде",
         "Сегодня солнечно.", "conversation", "russian"),
        ("Hey can you mix more russian please",
         "Конечно, давай попробуем mixed mode.", "conversation", None),
        ("Teach me a new Russian word", "",
         "teacher", "russian"),
        ("Speak english only please", "",
         "conversation", "english"),
        ("Stop mixing languages", "",
         "conversation", None),
        ("Talk slower please", "",
         "conversation", None),
        ("Use professional tone", "",
         "professional", None),
        ("Practice russian with me", "",
         "teacher", "russian"),
        ("Practice english with me", "",
         "teacher", "english"),
        ("Что нового сегодня?", "",
         "conversation", None),
        ("Use bilingual mode", "",
         "conversation", None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for user_text, draft, mode, pref in scenarios:
        r = run_dry_run_pipeline(
            user_text=user_text, draft_response_text=draft,
            conversation_mode=mode, user_preference=pref)
        out.append({
            "user_text": user_text,
            "status": r.get("dry_run_status"),
            "adapter": (r.get("adapter_choice") or {}).get(
                "chosen", {}).get("adapter_name"),
            "score": (r.get("adapter_choice") or {}).get("score"),
        })
    return {"demo": out, "count": len(out)}


def write_dry_run_pipeline_report(
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
    "build_dry_run_voice_render_job",
    "validate_dry_run_pipeline_inputs",
    "generate_spoken_payload_for_pipeline",
    "choose_dry_run_adapter",
    "create_dry_run_render_job",
    "run_dry_run_pipeline",
    "demo_dry_run_voice_pipeline",
    "write_dry_run_pipeline_report",
]
