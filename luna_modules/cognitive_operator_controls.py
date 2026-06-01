"""Program B Part D — bounded operator controls.

A single import surface for the operator (or a small CLI) to:

  - engage / disengage native intelligence mode
  - flip the brain runtime kill switch
  - flip the embedding runtime kill switch
  - flip the deep adapter on/off
  - flip explainability verbosity (decision_trace_enabled)
  - read the current control posture
  - audit every operator action

Hard guarantees
---------------
- NEVER raises (each method wraps).
- All flag changes are atomic temp+rename writes to feature_flags.json.
- Every operator action appends to ``memory/cognitive/operator_action_log.json``
  with timestamp, action, reason, before/after values.
- Bounded to a small, explicit set of safe controls. No arbitrary
  filesystem or process operations.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional


PROJECT_ROOT = r"D:\SurgeApp"
COGNITIVE_DIR = os.path.join(PROJECT_ROOT, "memory", "cognitive")
FLAGS_FILE = os.path.join(COGNITIVE_DIR, "feature_flags.json")
ACTION_LOG_PATH = os.path.join(COGNITIVE_DIR, "operator_action_log.json")

MAX_ACTION_LOG = 1000

# Whitelist of flags this module is allowed to flip. Anything outside this
# list requires direct file edit.
ALLOWED_FLAGS = {
    "cognitive_brain_runtime_enabled",
    "cognitive_brain_runtime_kill_switch",
    # Main-brain GPU (llama-cpp). True = GPU inference (~3.5x), auto-falls-back
    # to gpt4all/CPU on any GPU failure. False = instant kill-switch to CPU.
    "cognitive_main_gpu_llamacpp_enabled",
    # Ack-brain on llama-cpp CPU (removes gpt4all from the hot path -> kills
    # the ~190s CUDA probe + GPU poisoning). Auto-falls-back to gpt4all.
    "cognitive_ack_llamacpp_enabled",
    # Async post-reply reasoning: kernel/drive runs fire-and-forget after the
    # reply ships (updates state for the NEXT turn). Operator-accepted
    # speed/audit tradeoff. Flip False = synchronous full reasoning.
    "cognitive_conversation_async_postreply_enabled",
    # Coalesce research-fabric usage writes (kills ~16 whole-store rewrites/turn
    # -> ~0; dominant warm-turn cost). Default True. Flip False = per-call write.
    "cognitive_research_fabric_debounce_usage_writes_enabled",
    # Voice: max reply length (chars) spoken in Serge's cloned voice (else the
    # fast voice). 0 = always clone. Default 0 (clone everything).
    "cognitive_conversation_clone_reply_max_chars",
    # Voice: acks also use the clone (default True); Russian text routes to the
    # RU clone reference + language ru (default True).
    "cognitive_conversation_clone_acks_enabled",
    "cognitive_voice_clone_language_routing_enabled",
    # Audit: amortize per-turn ledger FIFO truncation (default True).
    "cognitive_conversation_audit_lazy_truncate_enabled",
    # Voice: acks play instant pre-rendered cloned clips (default True).
    "cognitive_conversation_fixed_cloned_acks_enabled",
    # Vocabulary: ground replies in Serge's 1M+1M bilingual dictionary
    # (read-only lookup -> prompt). Default True.
    "cognitive_bilingual_vocab_lookup_enabled",
    # Main-brain GPU context window (tokens); default 4096. Auto-CPU-fallback
    # if it doesn't fit VRAM.
    "cognitive_main_gpu_n_ctx",
    "cognitive_brain_embedding_runtime_enabled",
    "cognitive_brain_embedding_runtime_kill_switch",
    "cognitive_deep_adapter_enabled",
    "cognitive_hybrid_retrieval_enabled",
    "cognitive_memory_curator_enabled",
    "cognitive_decision_trace_enabled",
    "cognitive_self_model_enabled",
    "cognitive_cockpit_enabled",
    "cognitive_scheduler_kill_switch",
    # Program C flags
    "cognitive_safe_hands_enabled",
    "cognitive_project_memory_enabled",
    "cognitive_resume_engine_enabled",
    # Program D flags
    "cognitive_voice_runtime_enabled",
    "cognitive_perception_runtime_enabled",
    # Program E flags
    "cognitive_task_complexity_estimator_enabled",
    "cognitive_backend_health_index_enabled",
    "cognitive_hardware_aware_router_enabled",
    "cognitive_deep_context_shaper_enabled",
    # Program F flags
    "cognitive_project_operator_enabled",
    # Program G flags
    "cognitive_workspace_intelligence_enabled",
    # Program H flags
    "cognitive_daily_operator_enabled",
    # Presence Layer flags
    "cognitive_presence_runtime_enabled",
    "cognitive_presence_speak_on_boot_enabled",
    # Luna Voice V3 flags
    "cognitive_luna_voice_v3_enabled",
    "cognitive_personality_shaping_enabled",
    # Luna Voice V4 flag
    "cognitive_luna_voice_v4_enabled",
    # Luna Voice V4.5 true-clone flag
    "cognitive_luna_voice_v4_5_clone_enabled",
    # Luna Conversation Runtime V1 flags
    "cognitive_conversation_runtime_enabled",
    "cognitive_conversation_premium_voice_enabled",
    # Luna Realtime Acceleration flags
    "cognitive_conversation_ack_router_enabled",
    "cognitive_phrase_library_autorender_on_boot",
    # Luna Sovereign Conversation Runtime V2 flags
    "cognitive_sovereign_conversation_runtime_enabled",
    "cognitive_sovereign_warm_on_boot_enabled",
    # Program M — Big Brain + Deep Memory flags
    "cognitive_deep_memory_enabled",
    "cognitive_memory_ranker_enabled",
    # Program N — Learned Continuity flags (each layer independently
    # rollback-able)
    "cognitive_outcome_memory_enabled",
    "cognitive_skill_traces_enabled",
    "cognitive_failure_replay_enabled",
    "cognitive_preference_learner_enabled",
    "cognitive_memory_consolidator_enabled",
    "cognitive_deep_memory_lessons_layer_enabled",
    # Program O — Bounded Workflow Operator flags
    "cognitive_workflow_operator_enabled",
    "cognitive_workflow_auto_execute_safe_steps_enabled",
    # Program P — Multimodal perception flags (each channel
    # independently rollback-able)
    "cognitive_screen_perception_enabled",
    "cognitive_document_perception_enabled",
    "cognitive_audio_perception_enabled",
    "cognitive_world_model_enabled",
    "cognitive_deep_memory_perception_layer_enabled",
    # Program Q — Executive cortex flags
    "cognitive_executive_cortex_enabled",
    "cognitive_executive_proactivity_enabled",
    "cognitive_deep_memory_executive_layer_enabled",
    # Program R — Adaptation flags
    "cognitive_success_trace_store_enabled",
    "cognitive_distillation_engine_enabled",
    "cognitive_adaptation_governor_enabled",
    "cognitive_adaptation_force_promote",
    "cognitive_adaptation_registry_enabled",
    "cognitive_runtime_use_adaptation_enabled",
    "cognitive_runtime_auto_capture_success_traces_enabled",
    "cognitive_deep_memory_adaptation_layer_enabled",
    # Program S — Realtime acceleration flags
    "cognitive_model_fabric_enabled",
    "cognitive_warm_state_policy_enabled",
    "cognitive_streaming_generation_enabled",
    "cognitive_realtime_telemetry_enabled",
    "cognitive_ack_streaming_enabled",
    "cognitive_main_streaming_enabled",
    # Program T — Sovereign knowledge engine flags
    "cognitive_knowledge_ingestion_enabled",
    "cognitive_research_memory_fabric_enabled",
    "cognitive_knowledge_trust_governor_enabled",
    "cognitive_research_synthesis_enabled",
    "cognitive_evidence_grounded_recall_enabled",
    # Program U — Sovereign simulation flags
    "cognitive_simulation_state_enabled",
    "cognitive_plan_scorer_enabled",
    "cognitive_counterfactual_engine_enabled",
    "cognitive_failure_preemption_enabled",
    "cognitive_simulation_decision_engine_enabled",
    "cognitive_runtime_use_deliberation_enabled",
    # Program V — Reflective metacognition + verifier flags
    "cognitive_reflective_state_enabled",
    "cognitive_contradiction_detector_enabled",
    "cognitive_confidence_calibrator_enabled",
    "cognitive_verifier_stack_enabled",
    "cognitive_epistemic_discipline_enabled",
    "cognitive_runtime_use_verifier_enabled",
    # Program W — Dialogue mastery flags
    "cognitive_dialogue_state_enabled",
    "cognitive_conversational_intent_enabled",
    "cognitive_relationship_continuity_enabled",
    "cognitive_tone_style_adapter_enabled",
    "cognitive_dialogue_strategy_enabled",
    "cognitive_runtime_use_dialogue_pipeline_enabled",
    "cognitive_runtime_apply_tone_rewrite_enabled",
    # Program X — Capability foundry flags
    "cognitive_capability_gap_detector_enabled",
    "cognitive_capability_spec_enabled",
    "cognitive_capability_synthesis_enabled",
    "cognitive_capability_validation_enabled",
    "cognitive_capability_self_extension_governor_enabled",
    "cognitive_capability_registry_enabled",
    "cognitive_capability_write_to_sandbox_enabled",
    # Program Y — Unified kernel flags
    "cognitive_kernel_state_bus_enabled",
    "cognitive_kernel_lifecycle_enabled",
    "cognitive_kernel_router_enabled",
    "cognitive_kernel_doctrine_enabled",
    "cognitive_unified_kernel_enabled",
    "cognitive_runtime_use_unified_kernel_enabled",
    # Program Z — Kernel drive-mode flags
    "cognitive_kernel_stage_handlers_enabled",
    "cognitive_kernel_drive_engine_enabled",
    "cognitive_kernel_budget_governor_enabled",
    "cognitive_kernel_budget_strict_enabled",
    "cognitive_runtime_use_kernel_drive_enabled",
    # NOTE: cognitive_kernel_total_turn_ms_cap is read from the
    # flags file (int) but is NOT in ALLOWED_FLAGS so set_flag
    # cannot coerce it to bool. Operators tune it by editing
    # the flags file directly or via a dedicated int-setter.
    # Program AA — Long-horizon goals
    "cognitive_goal_state_enabled",
    "cognitive_goal_progress_enabled",
    "cognitive_goal_drift_detector_enabled",
    "cognitive_goal_planner_enabled",
    "cognitive_goal_advisor_enabled",
    "cognitive_runtime_use_goals_enabled",
    # NOTE: cognitive_goal_advisor_global_cooldown_turns is an
    # int flag NOT in ALLOWED_FLAGS for the same reason as the
    # Z total-turn-cap int flag above.
    # Program BB — Self-evaluation + outcome scoring
    "cognitive_outcome_score_state_enabled",
    "cognitive_outcome_scoring_enabled",
    "cognitive_failure_attribution_enabled",
    "cognitive_goal_outcome_evaluator_enabled",
    "cognitive_self_eval_governor_enabled",
    "cognitive_runtime_use_self_eval_enabled",
    # Program BB — Program R adaptation bridge (additive only)
    "cognitive_outcome_adaptation_bridge_enabled",
    "cognitive_runtime_use_outcome_bridge_enabled",
    # Program CC — R-side bridge consumer + derived evidence
    "cognitive_bridge_derived_evidence_enabled",
    "cognitive_bridge_to_r_translator_enabled",
    "cognitive_r_extension_governor_enabled",
    "cognitive_bridge_consumer_enabled",
    "cognitive_bridge_consumer_audit_enabled",
    "cognitive_runtime_use_bridge_consumer_enabled",
    "cognitive_runtime_use_bridge_derived_evidence_enabled",
    # Program DD — Multi-turn pattern mining
    "cognitive_pattern_state_enabled",
    "cognitive_pattern_miner_enabled",
    "cognitive_recurring_failure_detector_enabled",
    "cognitive_recurring_success_detector_enabled",
    "cognitive_pattern_advisor_enabled",
    "cognitive_runtime_use_pattern_mining_enabled",
    # NOTE: cadence + thresholds are int/float flags, NOT in
    # ALLOWED_FLAGS (set_flag would coerce them to bool).
    # Program EE — Pattern advisor consumer
    "cognitive_pattern_consumer_state_enabled",
    "cognitive_pattern_q_adapter_enabled",
    "cognitive_pattern_w_adapter_enabled",
    "cognitive_pattern_cc_adapter_enabled",
    "cognitive_pattern_consumer_enabled",
    "cognitive_pattern_consumer_audit_enabled",
    "cognitive_runtime_use_pattern_consumer_enabled",
    # NOTE: adapter thresholds are float flags, NOT in ALLOWED_FLAGS.
    # Program FF — Live policy consumption (governor + 3 consumers)
    "cognitive_pattern_consumption_governor_enabled",
    "cognitive_q_pattern_consumer_enabled",
    "cognitive_w_pattern_consumer_enabled",
    "cognitive_cc_pattern_consumer_enabled",
    "cognitive_pattern_consumption_audit_enabled",
    "cognitive_runtime_use_pattern_consumption_enabled",
    # NOTE: per-turn / per-24h / cc-target-threshold are int/float
    # flags, NOT in ALLOWED_FLAGS.
    # Program GG — Meta-policy learning + threshold refinement
    "cognitive_meta_policy_evidence_enabled",
    "cognitive_meta_policy_proposer_enabled",
    "cognitive_meta_policy_proposal_state_enabled",
    "cognitive_meta_policy_apply_governor_enabled",
    "cognitive_meta_policy_audit_enabled",
    "cognitive_runtime_use_meta_policy_enabled",
    "cognitive_runtime_use_meta_policy_auto_apply_enabled",
    # NOTE: min_observation_samples / auto_apply_min_confidence /
    # per_cycle_delta_float / per_cycle_delta_int /
    # observation_window_s / auto_apply_max_per_24h_per_knob
    # are int/float flags, NOT in ALLOWED_FLAGS.
    # Program HH — Model selection + quality-tier orchestration
    "cognitive_quality_tier_registry_enabled",
    "cognitive_model_selection_context_enabled",
    "cognitive_tier_selection_governor_enabled",
    "cognitive_tier_selection_audit_enabled",
    "cognitive_runtime_use_model_selection_enabled",
    "cognitive_model_selection_paused",
    # Program II — Context compression + cross-session recall
    "cognitive_context_compression_state_enabled",
    "cognitive_context_compressor_enabled",
    "cognitive_context_bloat_governor_enabled",
    "cognitive_recall_priority_enabled",
    "cognitive_cross_session_recall_enabled",
    "cognitive_runtime_use_context_compression_enabled",
    "cognitive_context_compression_paused",
    # Program JJ — Working-memory allocation + attention budgeting
    "cognitive_working_memory_slot_registry_enabled",
    "cognitive_attention_candidate_pool_enabled",
    "cognitive_attention_budget_governor_enabled",
    "cognitive_working_memory_state_enabled",
    "cognitive_working_memory_audit_enabled",
    "cognitive_runtime_use_working_memory_enabled",
    "cognitive_working_memory_paused",
    # Program KK — Execution packing + prompt assembly discipline
    "cognitive_execution_packing_contract_enabled",
    "cognitive_execution_packer_enabled",
    "cognitive_execution_bloat_governor_enabled",
    "cognitive_execution_packed_state_enabled",
    "cognitive_execution_packing_audit_enabled",
    "cognitive_runtime_use_execution_packing_enabled",
    "cognitive_execution_packing_paused",
    # Program LL — Sovereign task decomposition + multi-step plan stitching
    "cognitive_task_plan_state_enabled",
    "cognitive_task_decomposer_enabled",
    "cognitive_plan_stitcher_enabled",
    "cognitive_plan_progress_tracker_enabled",
    "cognitive_plan_bloat_governor_enabled",
    "cognitive_runtime_use_task_planning_enabled",
    "cognitive_task_planning_paused",
    # Program MM — Sovereign step execution orchestrator + bounded recovery
    "cognitive_step_execution_state_enabled",
    "cognitive_step_dispatcher_enabled",
    "cognitive_step_recovery_governor_enabled",
    "cognitive_next_step_controller_enabled",
    "cognitive_step_execution_audit_enabled",
    "cognitive_runtime_use_step_execution_enabled",
    "cognitive_step_execution_paused",
    # Program NN — Sovereign step action table + bounded tool dispatch
    "cognitive_step_action_registry_enabled",
    "cognitive_step_action_adapters_enabled",
    "cognitive_step_action_mapper_enabled",
    "cognitive_action_dispatcher_enabled",
    "cognitive_action_dispatch_audit_enabled",
    "cognitive_runtime_use_step_action_enabled",
    "cognitive_step_action_paused",
    "cognitive_step_action_feedback_to_mm_enabled",
    # Program OO — Sovereign outcome-to-action learning + policy shaping
    "cognitive_outcome_learning_state_enabled",
    "cognitive_outcome_distiller_enabled",
    "cognitive_action_policy_shaper_enabled",
    "cognitive_policy_shaping_audit_enabled",
    "cognitive_runtime_use_policy_shaping_enabled",
    "cognitive_policy_shaping_paused",
    "cognitive_runtime_distill_each_turn_enabled",
    "cognitive_runtime_apply_overrides_to_mm_enabled",
    # Program PP — Sovereign long-horizon execution memory + consolidation
    "cognitive_execution_memory_state_enabled",
    "cognitive_strategy_consolidator_enabled",
    "cognitive_strategy_promotion_governor_enabled",
    "cognitive_execution_memory_audit_enabled",
    "cognitive_runtime_use_execution_memory_enabled",
    "cognitive_execution_memory_paused",
    "cognitive_runtime_consolidate_each_turn_enabled",
    "cognitive_runtime_promote_each_turn_enabled",
    # Live-brain takeover seam (Step 2) — flag-gated live-context wiring
    "cognitive_runtime_use_live_brain_context_enabled",
    "cognitive_live_brain_context_paused",
    # Live-brain takeover seam (Step 3) — flag-gated HH live timeout routing
    "cognitive_runtime_use_hh_live_routing_enabled",
    "cognitive_hh_live_routing_paused",
    # Unified Brain Ingress campaign — master + soft-pause per stage
    "cognitive_runtime_use_brain_ingress_router_enabled",
    "cognitive_brain_ingress_router_paused",
    "cognitive_runtime_use_event_distiller_enabled",
    "cognitive_event_distiller_paused",
    "cognitive_runtime_use_ingress_promotion_enabled",
    "cognitive_ingress_promotion_paused",
    "cognitive_runtime_use_ingress_recall_enabled",
    "cognitive_ingress_recall_paused",
}

# String-valued flags (model file names). set_flag coerces to bool, which
# would corrupt these — use set_flag_string() instead.
ALLOWED_STRING_FLAGS = {
    "cognitive_sovereign_ack_model",
    "cognitive_sovereign_main_model",
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# Process-level lock + uuid-suffixed tmp paths prevent concurrent
# writers from clobbering each other (was the root cause of the
# Program-Q-induced action log "Extra data" corruption).
import threading as _threading  # noqa: E402
import uuid as _uuid  # noqa: E402
_ACTION_LOG_LOCK = _threading.RLock()
_FLAGS_LOCK = _threading.RLock()


def _read_flags() -> Dict[str, Any]:
    try:
        if not os.path.isfile(FLAGS_FILE):
            return {}
        with open(FLAGS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _write_flags(flags: Dict[str, Any]) -> bool:
    try:
        with _FLAGS_LOCK:
            os.makedirs(COGNITIVE_DIR, exist_ok=True)
            tmp = (f"{FLAGS_FILE}.tmp."
                    f"{int(time.time() * 1000)}.{_uuid.uuid4().hex[:8]}")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(flags, fh, indent=2, default=str)
            os.replace(tmp, FLAGS_FILE)
        return True
    except Exception:  # noqa: BLE001
        return False


def _record_action(action: str, *, reason: str, before: Any = None,
                   after: Any = None, ok: bool = True,
                   detail: Optional[Dict[str, Any]] = None) -> None:
    """Append one action record. NEVER raises.

    Thread-safe: a process-level RLock plus uuid-suffixed temp paths
    prevent concurrent writers from corrupting the action log.
    """
    with _ACTION_LOG_LOCK:
        try:
            if os.path.isfile(ACTION_LOG_PATH):
                with open(ACTION_LOG_PATH, "r", encoding="utf-8") as fh:
                    try:
                        data = json.load(fh)
                    except Exception:  # noqa: BLE001
                        # Defensive recovery — if a prior writer
                        # corrupted the file, start fresh rather than
                        # losing every subsequent record forever.
                        data = {"schema_version": 1,
                                 "first_observed_at_utc": _now_iso(),
                                 "records": [],
                                 "recovered_from_corruption_at_utc":
                                    _now_iso()}
                if not isinstance(data, dict):
                    data = {"records": []}
            else:
                data = {"schema_version": 1,
                        "first_observed_at_utc": _now_iso(),
                        "records": []}
            records = list(data.get("records") or [])
            records.append({
                "at_utc": _now_iso(),
                "action": str(action),
                "reason": str(reason)[:200],
                "before": before,
                "after": after,
                "ok": bool(ok),
                "detail": detail or {},
            })
            data["records"] = records[-MAX_ACTION_LOG:]
            data["last_at_utc"] = _now_iso()
            data["count_total"] = int(data.get("count_total", 0)) + 1
            tmp = (f"{ACTION_LOG_PATH}.tmp."
                    f"{int(time.time() * 1000)}."
                    f"{_uuid.uuid4().hex[:8]}")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, default=str)
            os.replace(tmp, ACTION_LOG_PATH)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def set_flag(name: str, value: bool, *, reason: str = "operator_request") -> Dict[str, Any]:
    """Flip one whitelisted feature flag. NEVER raises.

    Returns a structured result with before/after + ok + reason.
    """
    if str(name) not in ALLOWED_FLAGS:
        _record_action("set_flag_rejected", reason=reason, ok=False,
                       detail={"name": name, "rejected_because": "not_in_whitelist"})
        return {
            "ok": False,
            "name": name,
            "rejected_because": "not_in_allowed_flags_whitelist",
            "allowed_flags": sorted(ALLOWED_FLAGS),
        }
    flags = _read_flags()
    before = flags.get(name)
    flags[name] = bool(value)
    ok = _write_flags(flags)
    _record_action(
        "set_flag",
        reason=reason,
        before=before,
        after=flags.get(name),
        ok=ok,
        detail={"name": name},
    )
    # Unified Brain Ingress (Step 6, producer #1): feed the flag-flip
    # event into the bounded ingress router. Defensive on every axis —
    # router import failure, flag check failure, route_event raise:
    # set_flag MUST still return its own result. NEVER raises.
    try:
        _flags_for_gate = flags  # already loaded above
        _router_on = bool(
            _flags_for_gate.get(
                "cognitive_runtime_use_brain_ingress_router_enabled",
                False))
        _router_paused = bool(
            _flags_for_gate.get("cognitive_brain_ingress_router_paused",
                                 False))
        if _router_on and not _router_paused:
            try:
                from luna_modules import (
                    cognitive_brain_ingress_router as _bir)
            except Exception:  # noqa: BLE001
                _bir = None
            if _bir is not None:
                try:
                    _bir.route_event(
                        source="operator_controls",
                        kind="flag_flip",
                        payload={
                            "flag": str(name),
                            "value": bool(value),
                            "before": before,
                            "after": flags.get(name),
                            "ok": bool(ok),
                            "reason": str(reason),
                        },
                    )
                except Exception:  # noqa: BLE001
                    pass
    except Exception:  # noqa: BLE001
        pass
    return {
        "ok": ok,
        "name": name,
        "before": before,
        "after": flags.get(name),
        "reason": reason,
    }


def list_promoted_facts(*, kind: Optional[str] = None,
                          limit: int = 24) -> Dict[str, Any]:
    """Operator/dashboard introspection: surface the ingress-promoted
    fact store. NEVER raises. Returns a structured dict; on error returns
    {"ok": False, "facts": [], "error": "..."}.

    Optional `kind` filters by fact_kind (e.g. "operator_action",
    "project_event"). `limit` is bounded 1..256 by the underlying store.
    """
    try:
        try:
            from luna_modules import (
                cognitive_ingress_promoted_state as _ps,
            )
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "facts": [], "fact_count": 0,
                    "error": f"import_failed:{type(exc).__name__}"}
        try:
            facts = _ps.list_facts(kind=kind, limit=limit)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "facts": [], "fact_count": 0,
                    "error": f"list_failed:{type(exc).__name__}"}
        try:
            rep = _ps.report()
        except Exception:  # noqa: BLE001
            rep = {}
        return {
            "ok": True,
            "fact_count": len(facts),
            "total_in_store": int(rep.get("fact_count") or 0),
            "hard_cap": int(rep.get("hard_cap") or 0),
            "filter_kind": kind,
            "facts": facts,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "facts": [], "fact_count": 0,
                "error": f"unexpected:{type(exc).__name__}"}


def engage_native_mode(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Wrap cognitive_native_intelligence_mode.engage_native_mode + audit."""
    try:
        from luna_modules import cognitive_native_intelligence_mode as nim
        r = nim.engage_native_mode(reason=reason)
        _record_action("engage_native_mode", reason=reason,
                       after=r.get("active_after"), ok=bool(r.get("ok")),
                       detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        _record_action("engage_native_mode", reason=reason, ok=False,
                       detail={"exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def disengage_native_mode(*, reason: str = "operator_request") -> Dict[str, Any]:
    try:
        from luna_modules import cognitive_native_intelligence_mode as nim
        r = nim.disengage_native_mode(reason=reason)
        _record_action("disengage_native_mode", reason=reason,
                       after=r.get("active_after"), ok=bool(r.get("ok")),
                       detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        _record_action("disengage_native_mode", reason=reason, ok=False,
                       detail={"exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def engage_safe_mode(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Composite safe-mode: kill-switch BOTH runtimes onto null backends.

    This forces every generation AND embedding call to the deterministic
    safety floor without disabling the audit ledger. Use when operator
    suspects model misbehavior and wants Luna to stop generating until
    diagnostics complete.
    """
    r1 = set_flag("cognitive_brain_runtime_kill_switch", True, reason=reason)
    r2 = set_flag("cognitive_brain_embedding_runtime_kill_switch", True, reason=reason)
    return {
        "ok": bool(r1.get("ok") and r2.get("ok")),
        "brain_kill_switch": r1,
        "embedding_kill_switch": r2,
        "explanation": (
            "safe mode ON: every generation call lands on null_deterministic; "
            "every embedding call lands on null_embed. Disengage with disengage_safe_mode()."
        ),
    }


def disengage_safe_mode(*, reason: str = "operator_request") -> Dict[str, Any]:
    r1 = set_flag("cognitive_brain_runtime_kill_switch", False, reason=reason)
    r2 = set_flag("cognitive_brain_embedding_runtime_kill_switch", False, reason=reason)
    return {
        "ok": bool(r1.get("ok") and r2.get("ok")),
        "brain_kill_switch": r1,
        "embedding_kill_switch": r2,
        "explanation": "safe mode OFF: backends re-enabled.",
    }


def current_control_posture() -> Dict[str, Any]:
    """Read-only summary of every control surface state. NEVER raises."""
    flags = _read_flags()
    posture: Dict[str, Any] = {
        "captured_at_utc": _now_iso(),
        "feature_flags": {
            name: flags.get(name) for name in sorted(ALLOWED_FLAGS)
        },
        "safe_mode_active": bool(
            flags.get("cognitive_brain_runtime_kill_switch")
            and flags.get("cognitive_brain_embedding_runtime_kill_switch")
        ),
        "deep_adapter_enabled": bool(
            flags.get("cognitive_deep_adapter_enabled", True)),
        "decision_trace_enabled": bool(
            flags.get("cognitive_decision_trace_enabled", True)),
    }
    try:
        from luna_modules import cognitive_native_intelligence_mode as nim
        posture["native_mode_active"] = bool(nim.is_native_mode_active())
    except Exception:  # noqa: BLE001
        posture["native_mode_active"] = "unknown"
    try:
        from luna_modules import cognitive_self_model as _sm
        posture["self_model_health"] = _sm.health_summary()
    except Exception:  # noqa: BLE001
        posture["self_model_health"] = {"overall": "unknown"}
    return posture


def recent_actions(n: int = 20) -> List[Dict[str, Any]]:
    """Return last N operator actions. NEVER raises."""
    try:
        if not os.path.isfile(ACTION_LOG_PATH):
            return []
        with open(ACTION_LOG_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        records = data.get("records") if isinstance(data, dict) else None
        if not isinstance(records, list):
            return []
        return list(records[-max(0, int(n)):])
    except Exception:  # noqa: BLE001
        return []


# ---------------------------------------------------------------------------
# Program C: approve / deny safe-hands pending actions
# ---------------------------------------------------------------------------

def list_pending_actions() -> List[Dict[str, Any]]:
    """Return safe_hands actions still in PENDING state. NEVER raises."""
    try:
        from luna_modules import cognitive_safe_hands as sh
        return sh.list_actions(state=sh.PENDING)
    except Exception:  # noqa: BLE001
        return []


def approve_action(action_id: str, *,
                   reason: str = "operator_request") -> Dict[str, Any]:
    """Approve a pending safe-hands action. NEVER raises.

    Approved actions remain in state APPROVED until something calls
    safe_hands.execute_action(action_id). Approval alone does not
    execute.
    """
    try:
        from luna_modules import cognitive_safe_hands as sh
        action = sh.get_action(action_id)
        if action is None:
            _record_action("approve_action", reason=reason, ok=False,
                           detail={"action_id": action_id, "missing": True})
            return {"ok": False, "error": "action_not_found"}
        if action.get("state") != sh.PENDING:
            _record_action("approve_action", reason=reason, ok=False,
                           detail={"action_id": action_id,
                                    "state": action.get("state")})
            return {"ok": False, "error": f"action_state={action.get('state')}"}
        sh._append_state_transition(action, sh.APPROVED, reason=reason)
        sh._save_action(action)
        _record_action("approve_action", reason=reason,
                       before=sh.PENDING, after=sh.APPROVED, ok=True,
                       detail={"action_id": action_id,
                                "name": action.get("name")})
        return {"ok": True, "action_id": action_id, "state": sh.APPROVED}
    except Exception as exc:  # noqa: BLE001
        _record_action("approve_action", reason=reason, ok=False,
                       detail={"action_id": action_id,
                                "exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def deny_action(action_id: str, *,
                reason: str = "operator_denied") -> Dict[str, Any]:
    """Deny a pending safe-hands action. NEVER raises."""
    try:
        from luna_modules import cognitive_safe_hands as sh
        action = sh.get_action(action_id)
        if action is None:
            _record_action("deny_action", reason=reason, ok=False,
                           detail={"action_id": action_id, "missing": True})
            return {"ok": False, "error": "action_not_found"}
        if action.get("state") not in (sh.PENDING, sh.APPROVED):
            _record_action("deny_action", reason=reason, ok=False,
                           detail={"action_id": action_id,
                                    "state": action.get("state")})
            return {"ok": False, "error": f"action_state={action.get('state')}"}
        prev = action.get("state")
        sh._append_state_transition(action, sh.DENIED, reason=reason)
        sh._save_action(action)
        _record_action("deny_action", reason=reason,
                       before=prev, after=sh.DENIED, ok=True,
                       detail={"action_id": action_id,
                                "name": action.get("name")})
        return {"ok": True, "action_id": action_id, "state": sh.DENIED}
    except Exception as exc:  # noqa: BLE001
        _record_action("deny_action", reason=reason, ok=False,
                       detail={"action_id": action_id,
                                "exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def propose_next_step(project_id: str) -> Dict[str, Any]:
    """Operator-readable next-step recommendation for a project.

    Wraps :func:`cognitive_resume_engine.recommend_next_step` and audits
    the operator's request. NEVER raises.
    """
    try:
        from luna_modules import cognitive_resume_engine as re
        rec = re.recommend_next_step(project_id)
        _record_action("propose_next_step", reason="operator_request",
                       ok=bool(rec.get("ok")),
                       detail={"project_id": project_id,
                                "next_step_kind": ((rec.get("next_step") or {}).get("kind"))})
        return rec
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_progression_cycle(project_id: str, *,
                           mode: str = "propose_only",
                           reason: str = "operator_request") -> Dict[str, Any]:
    """Program F: operator-invoked bounded progression cycle. NEVER raises.

    Audited via :func:`_record_action` AND in
    ``progression_cycle_audit.json``. ``mode`` is one of:
      - ``"propose_only"`` (default): cycle ends with PENDING action;
        operator must approve before execution.
      - ``"auto"``: cycle MAY auto-execute IF the step kind is
        policy-allowed AND the budget honoured.
    """
    try:
        from luna_modules import cognitive_project_operator as po
        result = po.run_cycle(project_id, mode=mode, operator_reason=reason)
        _record_action("run_progression_cycle", reason=reason,
                       ok=bool(result.get("ok")),
                       detail={"project_id": project_id,
                                "mode": mode,
                                "cycle_id": result.get("cycle_id"),
                                "mode_chosen": result.get("mode_chosen"),
                                "kind": result.get("kind"),
                                "escalated": result.get("escalated")})
        return result
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def project_operator_state(project_id: str) -> Dict[str, Any]:
    """Operator-readable project-operator view. NEVER raises."""
    try:
        from luna_modules import cognitive_project_operator as po
        return po.operator_state(project_id)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_daily_checkin(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Program H: operator-invoked bounded daily-operator cycle. NEVER raises.

    Audited via :func:`_record_action` AND in
    ``daily_operator_cycle_audit.json``. Returns the full snapshot:
    top_today, approvals_waiting, blocked_today, stale_work, suggestions,
    demoted_silent, check_in_summary.
    """
    try:
        from luna_modules import cognitive_daily_operator_engine as doe
        result = doe.run_daily_cycle(operator_reason=reason)
        snap = (result.get("snapshot") if isinstance(result, dict) else None) or {}
        _record_action("run_daily_checkin", reason=reason,
                       ok=bool(result.get("ok")),
                       detail={"cycle_id": result.get("cycle_id"),
                                "top_today_count": len(snap.get("top_today") or []),
                                "approvals_waiting_count": len(snap.get("approvals_waiting") or []),
                                "demoted_silent_count": len(snap.get("demoted_silent") or []),
                                "confidence": snap.get("confidence")})
        return result
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def today_summary() -> Dict[str, Any]:
    """Operator-readable: return the latest persisted daily snapshot.
    NEVER raises."""
    try:
        from luna_modules import cognitive_daily_operator_engine as doe
        snap = doe.get_today_summary()
        if not snap:
            return {"ok": False, "reason": "no_daily_snapshot_yet"}
        return {"ok": True, "snapshot": snap}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def acknowledge_daily_suggestion(suggestion_signature: str) -> Dict[str, Any]:
    """Operator marks they ACTED on a daily suggestion -> clears it from
    the demotion ledger so it can be surfaced again if needed. NEVER raises.
    """
    try:
        from luna_modules import cognitive_daily_operator_state as dos
        r = dos.mark_acted(suggestion_signature)
        _record_action("acknowledge_daily_suggestion", reason="operator_acted",
                       ok=bool(r.get("ok")),
                       detail={"signature": str(suggestion_signature)[:120],
                                "cleared": r.get("cleared")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Presence Layer — operator surfaces
# ---------------------------------------------------------------------------

def presence_check(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Operator-readable: synthesise Luna's current posture and return it.
    NEVER raises. Lightweight read; does not speak."""
    try:
        from luna_modules import cognitive_presence_runtime as cpr
        rep = cpr.presence_report()
        _record_action("presence_check", reason=reason, ok=True,
                       detail={"posture": (rep.get("posture") or {}).get("posture"),
                                "speak_on_boot": rep.get("speak_on_boot"),
                                "enabled": rep.get("enabled")})
        return {"ok": True, "presence": rep}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def acknowledge_luna_boot(*, reason: str = "operator_request",
                           session_id: Optional[str] = None) -> Dict[str, Any]:
    """Operator-callable single-shot boot acknowledgement. NEVER raises.

    Idempotent within a process / session_id — if already acknowledged this
    just returns the existing marker.
    """
    try:
        from luna_modules import cognitive_presence_runtime as cpr
        r = cpr.acknowledge_boot(reason=reason, session_id=session_id,
                                   caller="operator_controls.acknowledge_luna_boot")
        _record_action("acknowledge_luna_boot", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"acknowledged": r.get("acknowledged"),
                                "posture": r.get("posture"),
                                "spoke": r.get("spoke")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_speak(text: str, *, urgency: str = "normal",
                reason: str = "operator_request",
                allow_audible: bool = True) -> Dict[str, Any]:
    """Operator-callable bounded speech facade. NEVER raises.

    Goes through ``cognitive_presence_runtime.speak`` so length, rate-limit,
    audit, and voice-backend selection are all honoured.
    """
    try:
        from luna_modules import cognitive_presence_runtime as cpr
        r = cpr.speak(text, urgency=urgency, caller="operator_controls.luna_speak",
                       allow_audible=allow_audible)
        _record_action("luna_speak", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"backend": r.get("backend"),
                                "reason": r.get("reason"),
                                "text_chars": r.get("text_chars"),
                                "latency_ms": r.get("latency_ms"),
                                "allow_audible": allow_audible,
                                "urgency": urgency})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def recent_conversation_turns(n: int = 10) -> Dict[str, Any]:
    """Operator-readable: last n recorded conversation turns. NEVER raises."""
    try:
        from luna_modules import cognitive_presence_runtime as cpr
        items = cpr.recent_turns(int(n))
        return {"ok": True, "count": len(items), "turns": items}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_continuity_context(*, n_recent_turns: int = 5,
                              max_chars: int = 1200) -> Dict[str, Any]:
    """Operator-readable continuity snapshot — what Luna would treat as
    "current context" if asked to speak / respond next. NEVER raises."""
    try:
        from luna_modules import cognitive_presence_runtime as cpr
        return cpr.compose_response_context(n_recent_turns=int(n_recent_turns),
                                              max_chars=int(max_chars))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Voice V3 — phone-call-speed operator surface
# ---------------------------------------------------------------------------

def luna_speak_v3(text: str, *, intent: str = "answer",
                   reason: str = "operator_request") -> Dict[str, Any]:
    """Phone-call-speed speak. Routes through V3 coordinator: personality
    shaping + cached/warm/fresh adapter selection + latency audit.

    NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.speak_v3(text, intent=intent,
                         caller="operator_controls.luna_speak_v3")
        _record_action("luna_speak_v3", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"intent": intent,
                                "backend": r.get("backend"),
                                "elapsed_ms": r.get("elapsed_ms"),
                                "switched": r.get("switched"),
                                "audible": r.get("audible"),
                                "utt_id": r.get("utt_id")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_warm_voice_v3(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Pre-render the canonical ack cache and spawn the persistent SAPI
    child so the next speak() is at conversational speed. NEVER raises."""
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.warm_up()
        _record_action("luna_warm_voice_v3", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"elapsed_ms_total": r.get("elapsed_ms_total"),
                                "steps": [s.get("step") for s in r.get("steps", [])]})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_play_identity_demo(tag: str = "identity_demo_1",
                             *, reason: str = "operator_request"
                             ) -> Dict[str, Any]:
    """Operator-callable: play one of the six provided sample WAVs to
    audition Luna's intended voice identity. NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.play_identity_demo(tag)
        _record_action("luna_play_identity_demo", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"tag": tag,
                                "elapsed_ms": r.get("elapsed_ms")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_set_personality_mode(mode: str, *,
                                reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-callable: switch Luna's personality mode between
    "good_luna" and "bad_luna" without going through a speech command.
    NEVER raises.
    """
    try:
        from luna_modules import cognitive_personality_runtime as pers
        r = pers.set_mode(mode, reason=reason)
        _record_action("luna_set_personality_mode", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"prior_mode": r.get("prior_mode"),
                                "new_mode": r.get("new_mode")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Voice V4 — operator surface
# ---------------------------------------------------------------------------

def luna_speak_premium(text: str, *, reason: str = "operator_request"
                        ) -> Dict[str, Any]:
    """Operator-callable: speak through the V4 premium path.

    Routes through V3.speak_v3(intent="premium"), which tries the V4
    premium adapter (real-Luna sample clips when applicable, else
    profile-tuned SAPI), with fallback to V3 if premium fails.
    NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.speak_v3(text, intent="premium",
                         caller="operator_controls.luna_speak_premium")
        _record_action("luna_speak_premium", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"backend": r.get("backend"),
                                "voice_identity": r.get("voice_identity"),
                                "elapsed_ms": r.get("elapsed_ms"),
                                "audible": r.get("audible")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_audition_voice(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Operator-callable: play a real Luna sample clip (chosen from the
    prepared segments) so the operator can audition Luna's intended
    voice. NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.speak_v3("", intent="premium_audition",
                         caller="operator_controls.luna_audition_voice")
        _record_action("luna_audition_voice", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"voice_identity": r.get("voice_identity"),
                                "elapsed_ms": r.get("elapsed_ms")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_prep_voice_samples(*, force: bool = False,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: run the sample preparation pipeline. Default
    is incremental (skip already-prepared files). Pass force=True to
    rebuild everything. NEVER raises.
    """
    try:
        from luna_modules import cognitive_voice_sample_prep as prep
        p = prep.prepare_samples(force=bool(force))
        _record_action("luna_prep_voice_samples", reason=reason,
                       ok=True,
                       detail={"prepared_count": p.get("prepared_sample_count"),
                                "segment_count": p.get("segment_count"),
                                "estimated_pitch": p.get("estimated_speaker_pitch_hz"),
                                "force": force})
        return {"ok": True, "profile": p}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Voice V4.5 — true clone operator surface
# ---------------------------------------------------------------------------

def luna_speak_clone(text: str, *, reason: str = "operator_request"
                      ) -> Dict[str, Any]:
    """Speak arbitrary text in Luna's actual cloned voice via XTTS-v2.

    Latency: ~5-15 s on CPU per utterance + ~26 s cold model load
    (one-time per process). Falls back to V4 sample-clip / V3 paths if
    XTTS unavailable. NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_voice_v3 as v3
        r = v3.speak_v3(text, intent="clone",
                         caller="operator_controls.luna_speak_clone")
        _record_action("luna_speak_clone", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"backend": r.get("backend"),
                                "voice_identity": r.get("voice_identity"),
                                "elapsed_ms": r.get("elapsed_ms"),
                                "audible": r.get("audible")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_warm_clone(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Pre-load the XTTS-v2 model so the next luna_speak_clone doesn't
    pay the ~26 s cold-load cost. NEVER raises.
    """
    try:
        from luna_modules import cognitive_voice_xtts_adapter as xtts
        adapter = xtts.get_singleton()
        r = adapter.warm_up()
        _record_action("luna_warm_clone", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"elapsed_ms": r.get("elapsed_ms"),
                                "model_loaded": r.get("model_loaded")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_clone_status(*, reason: str = "operator_request") -> Dict[str, Any]:
    """Operator-readable: XTTS adapter availability + status. NEVER raises."""
    try:
        from luna_modules import cognitive_voice_xtts_adapter as xtts
        adapter = xtts.get_singleton()
        return {"ok": True,
                "is_available": adapter.is_available(),
                "details": adapter.details()}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Conversation Runtime V1 — operator surface
# ---------------------------------------------------------------------------

def luna_conversation_turn(text: str, *,
                            session_id: Optional[str] = None,
                            want_premium_voice: bool = True,
                            allow_audible: bool = True,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Handle one live conversation turn end-to-end through the canonical
    runtime. Returns the full turn record with timings + backend trace.
    NEVER raises.
    """
    try:
        from luna_modules import cognitive_conversation_runtime as cr
        r = cr.handle_turn(text,
                            session_id=session_id,
                            caller=f"operator_controls.luna_conversation_turn:{reason}",
                            want_premium_voice=bool(want_premium_voice),
                            allow_audible=bool(allow_audible))
        _record_action("luna_conversation_turn", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"turn_id": r.get("turn_id"),
                                "category": (r.get("classification") or {}).get("category"),
                                "ack_dynamic": (r.get("ack") or {}).get("dynamic"),
                                "ack_text": (r.get("ack") or {}).get("text"),
                                "ack_elapsed_ms": (r.get("ack") or {}).get("elapsed_ms"),
                                "main_brain_backend": (r.get("main_reply") or {}).get("brain_backend"),
                                "main_voice_backend": (r.get("main_reply") or {}).get("voice_backend"),
                                "total_elapsed_ms": r.get("elapsed_ms")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_conversation_report(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-readable: conversation runtime status + hot state.
    NEVER raises."""
    try:
        from luna_modules import cognitive_conversation_runtime as cr
        return {"ok": True, **cr.report()}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Realtime Acceleration — operator surface
# ---------------------------------------------------------------------------

def luna_render_phrase_library(*, force: bool = False,
                                 only_categories: Optional[list] = None,
                                 stop_after: Optional[int] = None,
                                 reason: str = "operator_request"
                                 ) -> Dict[str, Any]:
    """Operator-callable: render (or re-render) the canonical Luna phrase
    library through XTTS. Blocking; ~10-30 minutes on CPU for a full
    catalog. Use ``stop_after=N`` for a smoke run.

    NEVER raises.
    """
    try:
        from luna_modules import cognitive_voice_phrase_renderer as rdr
        r = rdr.render_catalog_blocking(
            force=bool(force),
            only_categories=only_categories,
            stop_after=stop_after,
        )
        _record_action("luna_render_phrase_library", reason=reason,
                       ok=bool(r.get("ok")),
                       detail={"rendered_total": r.get("rendered_total"),
                                "skipped_total": r.get("skipped_total"),
                                "failed_total": r.get("failed_total"),
                                "elapsed_s": r.get("elapsed_s"),
                                "force": force,
                                "only_categories": only_categories,
                                "stop_after": stop_after})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_phrase_library_status(*, reason: str = "operator_request"
                                 ) -> Dict[str, Any]:
    """Operator-readable: how much of the canonical library has been
    rendered. NEVER raises."""
    try:
        from luna_modules import cognitive_voice_phrase_renderer as rdr
        return {"ok": True, **rdr.report()}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_warm_now(*, wait: bool = True,
                    reason: str = "operator_request") -> Dict[str, Any]:
    """Operator-callable: run the boot-time warm-up cycle now. Useful for
    refreshing the Ollama model + persistent SAPI child + cached phrase
    manifest without bouncing the dashboard.

    NEVER raises.
    """
    try:
        from luna_modules import cognitive_luna_warming as warm
        r = warm.warm_all(wait=bool(wait))
        _record_action("luna_warm_now", reason=reason,
                       ok=bool(r.get("completed")),
                       detail={"ready_count": r.get("ready_count"),
                                "total": r.get("total"),
                                "ready_fraction": r.get("ready_fraction")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_warming_status(*, reason: str = "operator_request"
                         ) -> Dict[str, Any]:
    """Operator-readable: warming state. NEVER raises."""
    try:
        from luna_modules import cognitive_luna_warming as warm
        return {"ok": True, **warm.report()}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Luna Sovereign Conversation Runtime V2 — operator surface
# ---------------------------------------------------------------------------

def set_flag_string(name: str, value: str, *,
                      reason: str = "operator_request") -> Dict[str, Any]:
    """Flip one whitelisted STRING-valued feature flag (e.g. a model
    file name). Distinct from set_flag (which is for booleans).
    NEVER raises."""
    if str(name) not in ALLOWED_STRING_FLAGS:
        _record_action("set_flag_string_rejected", reason=reason, ok=False,
                       detail={"name": name, "rejected_because": "not_in_string_whitelist"})
        return {
            "ok": False,
            "name": name,
            "rejected_because": "not_in_allowed_string_flags",
            "allowed_string_flags": sorted(ALLOWED_STRING_FLAGS),
        }
    flags = _read_flags()
    before = flags.get(name)
    flags[name] = str(value)
    ok = _write_flags(flags)
    _record_action("set_flag_string", reason=reason,
                    before=before, after=str(value), ok=bool(ok),
                    detail={"name": name})
    return {"ok": bool(ok), "name": name, "before": before,
             "after": str(value), "reason": reason}


def luna_sovereign_warm_now(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: pre-load BOTH sovereign GGUFs (1.5B ack +
    7B main) into RAM and open their persistent chat_sessions so the
    next live turn is fully warm. Blocking; ~25-40 s per model on the
    first call, near-instant if the OS file cache is hot.

    NEVER raises.
    """
    started = time.time()
    detail: Dict[str, Any] = {"ack": None, "main": None}
    try:
        from luna_modules import cognitive_sovereign_ack_runtime as sa
        detail["ack"] = sa.warm_up()
    except Exception as exc:  # noqa: BLE001
        detail["ack"] = {"ok": False,
                         "error": f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import cognitive_sovereign_main_runtime as sm
        detail["main"] = sm.warm_up()
    except Exception as exc:  # noqa: BLE001
        detail["main"] = {"ok": False,
                          "error": f"{type(exc).__name__}: {exc}"}
    ok = bool((detail.get("ack") or {}).get("ok")
              and (detail.get("main") or {}).get("ok"))
    elapsed_s = round(time.time() - started, 2)
    _record_action("luna_sovereign_warm_now", reason=reason, ok=ok,
                    detail={**detail, "elapsed_s": elapsed_s})
    return {"ok": ok, "elapsed_s": elapsed_s, **detail}


def luna_sovereign_status(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: sovereign ack + main runtime state.
    NEVER raises."""
    ack_report: Dict[str, Any]
    main_report: Dict[str, Any]
    try:
        from luna_modules import cognitive_sovereign_ack_runtime as sa
        ack_report = sa.report()
    except Exception as exc:  # noqa: BLE001
        ack_report = {"available": False,
                       "error": f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import cognitive_sovereign_main_runtime as sm
        main_report = sm.report()
    except Exception as exc:  # noqa: BLE001
        main_report = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Mirror the live legacy/sovereign counters from the hot state so
    # the operator can verify Ollama-in-hot-path / cache-in-hot-path
    # have stayed at 0.
    counters: Dict[str, Any] = {}
    sov_counters: Dict[str, Any] = {}
    try:
        from luna_modules import cognitive_conversation_state as cs
        st = cs.get_state()
        counters = st.legacy_quarantine()
        sov_counters = st.sovereign_counters()
    except Exception:  # noqa: BLE001
        counters = {}
        sov_counters = {}
    return {
        "ok": True,
        "ack": ack_report,
        "main": main_report,
        "legacy_path_quarantine": counters,
        "sovereign_counters": sov_counters,
        "doctrine": [
            "no_ollama_in_live_hot_path",
            "no_canned_phrase_library_in_live_hot_path",
            "every_ack_dynamically_generated",
        ],
    }


def luna_learning_status(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program N learning state.
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("outcome_memory", "luna_modules.cognitive_outcome_memory"),
        ("skill_traces", "luna_modules.cognitive_skill_traces"),
        ("failure_replay", "luna_modules.cognitive_failure_replay"),
        ("preferences", "luna_modules.cognitive_preference_learner"),
        ("consolidations", "luna_modules.cognitive_memory_consolidator"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    return out


def luna_consolidate_now(*, window: int = 40,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: run a bounded consolidation pass now.
    NEVER raises."""
    try:
        from luna_modules import cognitive_memory_consolidator as mc
        r = mc.consolidate_now(window=int(window))
        _record_action("luna_consolidate_now", reason=reason,
                        ok=bool(r.get("ok")), detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_preferences_refresh(*, reason: str = "operator_request"
                                 ) -> Dict[str, Any]:
    """Operator-callable: re-run preference inference + persist
    snapshot. NEVER raises."""
    try:
        from luna_modules import cognitive_preference_learner as pl
        r = pl.persist_inferred_preferences()
        _record_action("luna_preferences_refresh", reason=reason,
                        ok=bool(r.get("ok")), detail={"ready":
                            r.get("snapshot", {}).get("ready")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_fabric_status(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program S fabric + warm-state
    + streaming + telemetry surfaces. Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("fabric", "luna_modules.cognitive_model_fabric"),
        ("warm_state", "luna_modules.cognitive_warm_state_policy"),
        ("streaming",
         "luna_modules.cognitive_streaming_generator"),
        ("telemetry",
         "luna_modules.cognitive_realtime_telemetry"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    return out


def luna_warm_now(*, reason: str = "operator_request"
                       ) -> Dict[str, Any]:
    """Operator-callable: apply the warm-state policy now (wakes
    cold channels per declared policy). NEVER raises."""
    try:
        from luna_modules import cognitive_warm_state_policy as wsp
        r = wsp.apply_warmup()
        _record_action("luna_warm_now", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"needs_warm_before":
                                  r.get("needs_warm_before")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_realtime_stats(*, window: int = 50,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: realtime latency stats over the last
    ``window`` turns. NEVER raises."""
    try:
        from luna_modules import cognitive_realtime_telemetry as rt
        return rt.stats(window=int(window))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_capability_foundry_status(
    *, reason: str = "operator_request"
) -> Dict[str, Any]:
    """Operator-readable: aggregated Program X capability foundry
    surfaces. Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("gap_detector",
         "luna_modules.cognitive_capability_gap_detector"),
        ("spec",
         "luna_modules.cognitive_capability_spec"),
        ("synthesis",
         "luna_modules.cognitive_capability_synthesis"),
        ("validation",
         "luna_modules.cognitive_capability_validation"),
        ("governor",
         "luna_modules."
         "cognitive_capability_self_extension_governor"),
        ("registry",
         "luna_modules.cognitive_capability_registry"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Recent registry + sandbox listing.
    try:
        from luna_modules import cognitive_capability_registry as reg
        out["recent_capabilities"] = (
            reg.list_by_status(None) or [])[:8]
    except Exception:  # noqa: BLE001
        out["recent_capabilities"] = []
    try:
        from luna_modules import cognitive_capability_synthesis as syn
        out["sandbox_files"] = syn.list_sandbox_capabilities()
    except Exception:  # noqa: BLE001
        out["sandbox_files"] = []
    return out


def luna_capability_detect_gap(
    *, observation_text: str,
    failure_signals: Optional[List[Dict[str, Any]]] = None,
    workflow_context: Optional[Dict[str, Any]] = None,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: classify an observation into a
    capability-gap record. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_capability_gap_detector as gd)
        r = gd.detect_gap(
            observation_text=observation_text,
            failure_signals=failure_signals,
            workflow_context=workflow_context,
            caller_hint="operator_request",
        )
        _record_action(
            "luna_capability_detect_gap", reason=reason,
            ok=bool(r.get("ok")),
            detail={"is_gap": r.get("is_capability_gap"),
                    "gap_type": r.get("gap_type")},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_capability_build_spec(
    *, gap_record: Dict[str, Any],
    capability_name: str,
    problem: str, scope: str,
    inputs: Optional[List[str]] = None,
    outputs: Optional[List[str]] = None,
    safety_constraints: Optional[List[str]] = None,
    dependencies: Optional[List[str]] = None,
    runtime_integration_points: Optional[List[str]] = None,
    required_tests: Optional[List[str]] = None,
    rollback_plan: Optional[str] = None,
    proof_criteria: Optional[List[str]] = None,
    value_now: Optional[str] = None,
    bounded_reason: Optional[str] = None,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: build + persist a capability spec.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_capability_spec as cs)
        from luna_modules import (
            cognitive_capability_registry as reg)
        r = cs.build_spec(
            gap_record=gap_record,
            capability_name=capability_name,
            problem=problem, scope=scope,
            inputs=inputs, outputs=outputs,
            safety_constraints=safety_constraints,
            dependencies=dependencies,
            runtime_integration_points=
                runtime_integration_points,
            required_tests=required_tests,
            rollback_plan=rollback_plan,
            proof_criteria=proof_criteria,
            value_now=value_now,
            bounded_reason=bounded_reason,
        )
        if r.get("ok") and r.get("spec"):
            cs.persist_spec(r["spec"])
            reg.upsert(
                capability_name=capability_name,
                status="specced",
                spec_id=(r["spec"] or {}).get("spec_id"),
                gap_type=(gap_record or {}).get("gap_type"),
                suggested_layer=(
                    gap_record or {}).get("suggested_layer"),
                notes=(r["spec"] or {}).get(
                    "bounded_reason"),
            )
        _record_action(
            "luna_capability_build_spec", reason=reason,
            ok=bool(r.get("ok")),
            detail={
                "spec_id":
                    (r.get("spec") or {}).get("spec_id"),
                "completeness":
                    (r.get("spec") or {}).get(
                        "completeness"),
            },
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_capability_synthesize(
    *, spec_id: Optional[str] = None,
    spec: Optional[Dict[str, Any]] = None,
    write_to_sandbox: bool = False,
    operator_pre_approved: bool = False,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: run governor + synthesizer for a spec.
    Spec can be passed directly or resolved by spec_id. The
    final write only happens when:
        write_to_sandbox is True
        AND governor outcome is approved_to_generate
        AND cognitive_capability_write_to_sandbox_enabled is True
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_capability_spec as cs)
        from luna_modules import (
            cognitive_capability_synthesis as syn)
        from luna_modules import (
            cognitive_capability_self_extension_governor
            as gov)
        from luna_modules import (
            cognitive_capability_registry as reg)
        from luna_modules import (
            cognitive_feature_flags as ff)
        target = spec
        if target is None and spec_id:
            recents = cs.recent_specs(n=50) or []
            for r in recents:
                if r.get("spec_id") == spec_id:
                    target = r
                    break
        if target is None:
            return {"ok": False,
                    "reason": "spec_not_found"}
        ge = gov.evaluate_spec(
            target,
            operator_pre_approved=operator_pre_approved)
        write_allowed = (
            ge.get("outcome") == "approved_to_generate"
            and write_to_sandbox
            and bool(ff.read_flags().get(
                "cognitive_capability_write_to_sandbox_enabled",
                True))
        )
        syn_res = syn.synthesize(
            target, write_to_sandbox=write_allowed)
        reg.upsert(
            capability_name=target.get("capability_name"),
            status=("synthesized" if write_allowed
                    else "specced"),
            spec_id=target.get("spec_id"),
            gap_type=target.get("gap_type"),
            governor_outcome=ge.get("outcome"),
            notes=("synthesized_to_sandbox" if write_allowed
                    else f"dry_run_only:{ge.get('outcome')}"),
        )
        _record_action(
            "luna_capability_synthesize", reason=reason,
            ok=bool(syn_res.get("ok")),
            detail={
                "spec_id": target.get("spec_id"),
                "governor_outcome": ge.get("outcome"),
                "wrote_to_sandbox":
                    syn_res.get("wrote_to_sandbox"),
            },
        )
        return {
            "ok": bool(syn_res.get("ok")),
            "governor": ge,
            "synthesis": syn_res,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_capability_validate(
    *, capability_name: str,
    spec_id: Optional[str] = None,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: validate a sandbox capability. NEVER
    raises."""
    try:
        from luna_modules import (
            cognitive_capability_validation as cv)
        from luna_modules import (
            cognitive_capability_registry as reg)
        r = cv.validate(
            capability_name=capability_name,
            spec_id=spec_id,
        )
        verdict = r.get("verdict")
        # Update registry status.
        status_map = {
            "passed":             "validated",
            "failed_import":      "failed",
            "failed_smoke":       "failed",
            "failed_grade":       "failed",
            "rejected_compile_only": "failed",
        }
        next_status = status_map.get(verdict, "synthesized")
        reg.upsert(
            capability_name=capability_name,
            status=next_status,
            validation_verdict=verdict,
            notes=r.get("reason"),
        )
        _record_action(
            "luna_capability_validate", reason=reason,
            ok=bool(r.get("ok")),
            detail={"capability_name": capability_name,
                    "verdict": verdict},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_capability_rollback(
    *, capability_name: str,
    reason: str = "operator_rollback",
) -> Dict[str, Any]:
    """Operator-callable: roll back a sandbox capability. Removes
    sandbox files and marks registry as ``rolled_back``."""
    try:
        from luna_modules import (
            cognitive_capability_registry as reg)
        r = reg.rollback(capability_name=capability_name,
                          reason=reason)
        _record_action(
            "luna_capability_rollback", reason=reason,
            ok=bool(r.get("ok")),
            detail={"capability_name": capability_name,
                    "removed_files": r.get("removed_files")},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_outcome_status(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-readable: aggregated BB surfaces (state /
    scoring / attribution / goal_eval / governor). Read-only.
    NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {},
        }
        for name, mod_path in (
            ("outcome_score_state",
             "luna_modules.cognitive_outcome_score_state"),
            ("outcome_scoring",
             "luna_modules.cognitive_outcome_scoring"),
            ("failure_attribution",
             "luna_modules.cognitive_failure_attribution"),
            ("goal_outcome_evaluator",
             "luna_modules."
             "cognitive_goal_outcome_evaluator"),
            ("self_eval_governor",
             "luna_modules.cognitive_self_eval_governor"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}",
                }
        _record_action("luna_outcome_status",
                        reason=reason, ok=True,
                        detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_score_turn(
    *, turn_summary: Dict[str, Any],
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: score a synthetic ``turn_summary``
    via the BB stack (scoring → attribution → goal_eval →
    governor). Does NOT persist by default. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_scoring as oc)
        from luna_modules import (
            cognitive_failure_attribution as fa)
        from luna_modules import (
            cognitive_goal_outcome_evaluator as goe)
        from luna_modules import (
            cognitive_self_eval_governor as sgov)
        ts = dict(turn_summary or {})
        score = oc.score_turn(turn_summary=ts)
        attr = fa.attribute(turn_summary=ts,
                              outcome_score=score)
        geval = goe.evaluate(turn_summary=ts)
        gov = sgov.decide(turn_summary=ts,
                            outcome_score=score,
                            goal_eval=geval,
                            attribution=attr)
        _record_action(
            "luna_score_turn", reason=reason, ok=True,
            detail={
                "outcome_label":
                    score.get("outcome_label"),
                "governor_outcome":
                    gov.get("outcome"),
            })
        return {"ok": True, "score": score,
                "attribution": attr,
                "goal_eval": geval,
                "governor": gov}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_explain_outcome(
    *, outcome_id: str,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: explain a stored outcome record.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_score_state as oss)
        r = oss.get_outcome(outcome_id)
        _record_action("luna_explain_outcome",
                        reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"outcome_id": outcome_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_outcome_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL outcome
    records. Audits the wipe. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_score_state as oss)
        r = oss.delete_all(reason=str(reason))
        _record_action(
            "luna_outcome_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_status(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: BB→R adaptation bridge report +
    recent bridge records. Read-only. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_adaptation_bridge as br)
        rep = br.report()
        recent = br.latest_recent(limit=8)
        _record_action("luna_bridge_status",
                        reason=reason, ok=True,
                        detail={"recent_count":
                                  len(recent)})
        return {"ok": True, "report": rep,
                "recent_records": recent}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_recent(
    *, state: Optional[str] = None,
    limit: int = 16,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: list recent bridge records, optionally
    filtered by state. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_adaptation_bridge as br)
        rows = br.list_records(state=state,
                                  limit=int(limit))
        _record_action("luna_bridge_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows),
                                "state": state})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_rollback(
    *, bridge_id: str,
    reason: str = "operator_rollback",
) -> Dict[str, Any]:
    """Operator-callable: reverse a prior bridge promotion.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_adaptation_bridge as br)
        r = br.rollback(bridge_id, reason=reason)
        _record_action(
            "luna_bridge_rollback",
            reason=reason, ok=bool(r.get("ok")),
            detail={"bridge_id": bridge_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL bridge
    records. Audits the wipe. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_adaptation_bridge as br)
        r = br.delete_all(reason=str(reason))
        _record_action(
            "luna_bridge_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program CC — R-side bridge consumer wrappers
def luna_bridge_consumer_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated CC consumer reports.
    Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("bridge_derived_evidence",
             "luna_modules.cognitive_bridge_derived_evidence"),
            ("bridge_to_r_translator",
             "luna_modules.cognitive_bridge_to_r_translator"),
            ("r_extension_governor",
             "luna_modules.cognitive_r_extension_governor"),
            ("bridge_consumer",
             "luna_modules.cognitive_bridge_consumer"),
            ("bridge_consumer_audit",
             "luna_modules.cognitive_bridge_consumer_audit"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        _record_action("luna_bridge_consumer_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_consumer_recent(
    *, limit: int = 8,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: most recent derived records. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_bridge_derived_evidence as bde)
        rows = bde.latest_recent(limit=int(limit))
        _record_action("luna_bridge_consumer_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_consumer_rollback(
    *, derived_id: str,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: flip a derived record to rolled_back.
    Reversible because the consumer is additive-only. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_bridge_consumer as bc)
        r = bc.rollback(str(derived_id or ""),
                          reason=str(reason or ""))
        _record_action(
            "luna_bridge_consumer_rollback",
            reason=str(reason or ""),
            ok=bool(r.get("ok")),
            detail={"derived_id": derived_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_bridge_consumer_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL derived
    records. Audits the wipe. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_bridge_derived_evidence as bde)
        r = bde.delete_all(reason=str(reason))
        _record_action(
            "luna_bridge_consumer_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program DD — Pattern mining wrappers
def luna_pattern_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated DD pattern reports.
    Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("pattern_state",
             "luna_modules.cognitive_pattern_state"),
            ("pattern_miner",
             "luna_modules.cognitive_pattern_miner"),
            ("recurring_failure_detector",
             "luna_modules."
             "cognitive_recurring_failure_detector"),
            ("recurring_success_detector",
             "luna_modules."
             "cognitive_recurring_success_detector"),
            ("pattern_advisor",
             "luna_modules.cognitive_pattern_advisor"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        _record_action("luna_pattern_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_recent(
    *, limit: int = 8,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: most recent patterns. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_state as ps)
        rows = ps.latest_recent(limit=int(limit))
        _record_action("luna_pattern_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_refresh(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: trigger a synchronous pattern mining
    pass + run both detectors. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_miner as pm)
        from luna_modules import (
            cognitive_recurring_failure_detector as rfd)
        from luna_modules import (
            cognitive_recurring_success_detector as rsd)
        mine = pm.mine_once()
        if not mine.get("ok"):
            return {"ok": False,
                    "reason":
                        f"mine_failed:"
                        f"{mine.get('reason')}"}
        candidates = mine.get("candidates_by_kind") or {}
        fail_cands = candidates.get(
            "recurring_failure") or []
        succ_cands = candidates.get(
            "recurring_success") or []
        fres = rfd.promote_candidates(fail_cands)
        sres = rsd.promote_candidates(succ_cands)
        _record_action(
            "luna_pattern_refresh", reason=reason,
            ok=True,
            detail={
                "failure_promoted":
                    fres.get("promoted"),
                "failure_refused":
                    fres.get("refused"),
                "success_promoted":
                    sres.get("promoted"),
                "success_refused":
                    sres.get("refused"),
                "scan_window_summary":
                    mine.get("scan_window_summary"),
            })
        return {
            "ok": True,
            "mine": mine,
            "failure_promotion": fres,
            "success_promotion": sres,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL pattern
    records. Audits the wipe. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_state as ps)
        r = ps.delete_all(reason=str(reason))
        _record_action(
            "luna_pattern_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program EE — Pattern consumer wrappers
def luna_pattern_consumer_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated EE consumer reports.
    Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("pattern_consumer_state",
             "luna_modules."
             "cognitive_pattern_consumer_state"),
            ("pattern_q_adapter",
             "luna_modules.cognitive_pattern_q_adapter"),
            ("pattern_w_adapter",
             "luna_modules.cognitive_pattern_w_adapter"),
            ("pattern_cc_adapter",
             "luna_modules.cognitive_pattern_cc_adapter"),
            ("pattern_consumer",
             "luna_modules.cognitive_pattern_consumer"),
            ("pattern_consumer_audit",
             "luna_modules."
             "cognitive_pattern_consumer_audit"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        _record_action("luna_pattern_consumer_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumer_recent(
    *, limit: int = 8,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: most recent hints. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_consumer_state as pcs)
        rows = pcs.latest_recent(limit=int(limit))
        _record_action("luna_pattern_consumer_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumer_rollback(
    *, hint_id: str,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: flip a hint to rolled_back. Reversible
    (adapters are additive). NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_consumer as pc)
        r = pc.rollback(str(hint_id or ""),
                          reason=str(reason or ""))
        _record_action(
            "luna_pattern_consumer_rollback",
            reason=str(reason or ""),
            ok=bool(r.get("ok")),
            detail={"hint_id": hint_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumer_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL hint
    records. Audits the wipe. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_consumer_state as pcs)
        r = pcs.delete_all(reason=str(reason))
        _record_action(
            "luna_pattern_consumer_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program FF — Live pattern consumption wrappers
def luna_pattern_consumption_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated FF live-consumption
    reports. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("pattern_consumption_governor",
             "luna_modules."
             "cognitive_pattern_consumption_governor"),
            ("q_pattern_consumer",
             "luna_modules.cognitive_q_pattern_consumer"),
            ("w_pattern_consumer",
             "luna_modules.cognitive_w_pattern_consumer"),
            ("cc_pattern_consumer",
             "luna_modules.cognitive_cc_pattern_consumer"),
            ("pattern_consumption_audit",
             "luna_modules."
             "cognitive_pattern_consumption_audit"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        _record_action("luna_pattern_consumption_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumption_recent(
    *, limit: int = 16,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: most recent FF audit rows. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_consumption_audit as pca)
        rows = pca.recent(limit=int(limit))
        _record_action("luna_pattern_consumption_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumption_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause live consumption (disable the
    runtime opt-in flag). Hint store + governor remain readable.
    Idempotent. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_runtime_use_pattern_consumption_enabled",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_pattern_consumption_pause",
            reason=str(reason or ""), ok=bool(r.get("ok")),
            detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumption_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume live consumption. Idempotent.
    NEVER raises."""
    try:
        r = set_flag(
            "cognitive_runtime_use_pattern_consumption_enabled",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_pattern_consumption_resume",
            reason=str(reason or ""), ok=bool(r.get("ok")),
            detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_pattern_consumption_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe FF governor
    state + audit ledger. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_pattern_consumption_governor as gov,
            cognitive_pattern_consumption_audit as pca)
        gov_r = gov.delete_all(reason=str(reason))
        aud_r = pca.delete_all(reason=str(reason))
        _record_action(
            "luna_pattern_consumption_delete_all",
            reason=str(reason),
            ok=bool(gov_r.get("ok") and aud_r.get("ok")),
            detail={
                "deleted_event_count":
                    gov_r.get("deleted_event_count"),
                "deleted_row_count":
                    aud_r.get("deleted_row_count")})
        return {"ok": bool(gov_r.get("ok")
                            and aud_r.get("ok")),
                "governor": gov_r,
                "audit": aud_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program GG — Meta-policy wrappers
def luna_meta_policy_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated GG meta-policy reports +
    current mutable-knob values + auto-apply posture. Read-only.
    NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("evidence",
             "luna_modules.cognitive_meta_policy_evidence"),
            ("proposer",
             "luna_modules.cognitive_meta_policy_proposer"),
            ("proposal_state",
             "luna_modules."
             "cognitive_meta_policy_proposal_state"),
            ("apply_governor",
             "luna_modules."
             "cognitive_meta_policy_apply_governor"),
            ("audit",
             "luna_modules.cognitive_meta_policy_audit"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        # Current mutable-knob values + auto-apply posture
        try:
            flags = _read_flags()
            from luna_modules.cognitive_meta_policy_proposer import (  # type: ignore  # noqa: E501
                MUTABLE_KNOB_BOUNDS as _mkb)
            out["mutable_knob_values"] = {
                k: flags.get(k) for k in _mkb.keys()
            }
            out["auto_apply_enabled"] = bool(flags.get(
                "cognitive_runtime_use_meta_policy"
                "_auto_apply_enabled", False))
        except Exception:  # noqa: BLE001
            out["mutable_knob_values"] = {}
            out["auto_apply_enabled"] = False
        _record_action("luna_meta_policy_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_recent(
    *, limit: int = 16,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: most recent GG audit rows + recent
    proposals. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_meta_policy_audit as aud,
            cognitive_meta_policy_proposal_state as pst)
        rows = aud.recent(limit=int(limit))
        props = pst.list_proposals(limit=int(limit))
        _record_action("luna_meta_policy_recent",
                        reason=reason, ok=True,
                        detail={"rows": len(rows),
                                  "props": len(props)})
        return {"ok": True,
                "recent_audit_rows": rows,
                "recent_proposals": props,
                "counts_by_event":
                    aud.counts_by_event(limit=500)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_promote(
    *, proposal_id: str,
    reason: str = "operator_promotion",
) -> Dict[str, Any]:
    """Operator-gated apply (auto=False). NEVER raises."""
    try:
        from luna_modules import (
            cognitive_meta_policy_apply_governor as apg)
        r = apg.apply_proposal(
            str(proposal_id or ""),
            auto=False,
            reason=str(reason or "")[:200])
        _record_action(
            "luna_meta_policy_promote",
            reason=str(reason or ""),
            ok=bool(r.get("ok")
                      and r.get("decision") == "apply"),
            detail={"proposal_id": proposal_id,
                      "decision": r.get("decision")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_rollback(
    *, proposal_id: str,
    reason: str = "operator_rollback",
) -> Dict[str, Any]:
    """Operator-callable: revert a previous apply by restoring
    the recorded pre_value. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_meta_policy_apply_governor as apg)
        r = apg.rollback_proposal(
            str(proposal_id or ""),
            reason=str(reason or "")[:200])
        _record_action(
            "luna_meta_policy_rollback",
            reason=str(reason or ""),
            ok=bool(r.get("ok")
                      and r.get("decision") == "rollback"),
            detail={"proposal_id": proposal_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: disable auto-apply by flipping the
    runtime opt-in flag to False. Proposal generation and
    operator-gated promotion remain available. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_runtime_use_meta_policy"
            "_auto_apply_enabled",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_meta_policy_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: re-enable auto-apply. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_runtime_use_meta_policy"
            "_auto_apply_enabled",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_meta_policy_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_meta_policy_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe proposal state
    + audit ledger. Applied knob values are PRESERVED (reverting
    an applied knob requires explicit luna_meta_policy_rollback).
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_meta_policy_proposal_state as pst,
            cognitive_meta_policy_audit as aud)
        pst_r = pst.delete_all(reason=str(reason))
        aud_r = aud.delete_all(reason=str(reason))
        _record_action(
            "luna_meta_policy_delete_all",
            reason=str(reason),
            ok=bool(pst_r.get("ok") and aud_r.get("ok")),
            detail={
                "deleted_proposal_count":
                    pst_r.get("deleted_count"),
                "deleted_audit_row_count":
                    aud_r.get("deleted_row_count")})
        return {"ok": bool(pst_r.get("ok")
                            and aud_r.get("ok")),
                "proposal_state": pst_r,
                "audit": aud_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program HH — Model-selection wrappers
def luna_model_selection_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated HH component reports +
    current tier registry posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason, "components": {}}
        for name, mod_path in (
            ("registry",
             "luna_modules."
             "cognitive_quality_tier_registry"),
            ("context",
             "luna_modules."
             "cognitive_model_selection_context"),
            ("governor",
             "luna_modules."
             "cognitive_tier_selection_governor"),
            ("audit",
             "luna_modules."
             "cognitive_tier_selection_audit"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        # Current runtime posture
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_model_selection_enabled",
            True))
        out["paused"] = bool(flags.get(
            "cognitive_model_selection_paused", False))
        _record_action("luna_model_selection_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_model_selection_recent(
    *, limit: int = 16,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent routing decisions + counts by
    decision type. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_tier_selection_audit as aud)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_decision(limit=500)
        _record_action("luna_model_selection_recent",
                        reason=reason, ok=True,
                        detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "counts_by_decision": counts}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_model_selection_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause HH selection (flips
    `cognitive_model_selection_paused` to True). The governor
    still functions but the conversation runtime will route
    around it. Idempotent. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_model_selection_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_model_selection_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_model_selection_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume HH selection. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_model_selection_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_model_selection_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_model_selection_delete_audit(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe HH audit
    ledger. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_tier_selection_audit as aud)
        r = aud.delete_all(reason=str(reason))
        _record_action(
            "luna_model_selection_delete_audit",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={"deleted_row_count":
                      r.get("deleted_row_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# Program II — Context compression + cross-session recall wrappers
def luna_context_compression_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated II component reports +
    current store/posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("state",
             "luna_modules."
             "cognitive_context_compression_state"),
            ("compressor",
             "luna_modules.cognitive_context_compressor"),
            ("governor",
             "luna_modules."
             "cognitive_context_bloat_governor"),
            ("recall_priority",
             "luna_modules.cognitive_recall_priority"),
            ("cross_session_recall",
             "luna_modules."
             "cognitive_cross_session_recall"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_context_compression"
            "_enabled", True))
        out["paused"] = bool(flags.get(
            "cognitive_context_compression_paused",
            False))
        _record_action(
            "luna_context_compression_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_context_compression_recent(
    *, limit: int = 16,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent compressed units. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_context_compression_state as st)
        rows = st.list_units(limit=int(limit))
        _record_action(
            "luna_context_compression_recent",
            reason=reason, ok=True,
            detail={"count": len(rows)})
        return {"ok": True,
                "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_context_compression_refresh(
    *, session_id: Optional[str] = None,
    summary_seed: Optional[str] = None,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: trigger ONE compress_slice pass against
    current evidence. The governor decides accept/refuse.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_context_compressor as comp)
        r = comp.compress_slice(
            session_id=session_id,
            summary_seed=summary_seed)
        _record_action(
            "luna_context_compression_refresh",
            reason=reason,
            ok=r.get("decision") == "compress",
            detail={
                "decision": r.get("decision"),
                "compression_id":
                    r.get("compression_id")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_context_compression_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause II layer (sets
    `cognitive_context_compression_paused` to True).
    NEVER raises."""
    try:
        r = set_flag(
            "cognitive_context_compression_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_context_compression_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_context_compression_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume II layer. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_context_compression_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_context_compression_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_context_compression_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL compressed
    units. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_context_compression_state as st)
        r = st.delete_all(reason=str(reason))
        _record_action(
            "luna_context_compression_delete_all",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={"deleted_count":
                      r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program JJ — Working-memory + attention-budgeting wrappers
def luna_working_memory_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated JJ component reports +
    current paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("slot_registry",
             "luna_modules."
             "cognitive_working_memory_slot_registry"),
            ("candidate_pool",
             "luna_modules."
             "cognitive_attention_candidate_pool"),
            ("budget_governor",
             "luna_modules."
             "cognitive_attention_budget_governor"),
            ("state",
             "luna_modules."
             "cognitive_working_memory_state"),
            ("audit",
             "luna_modules."
             "cognitive_working_memory_audit"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_working_memory_enabled",
            True))
        out["paused"] = bool(flags.get(
            "cognitive_working_memory_paused", False))
        _record_action(
            "luna_working_memory_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_working_memory_recent(
    *, limit: int = 24,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent JJ audit rows + counts by event +
    current active slots. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_working_memory_audit as aud,
            cognitive_working_memory_state as st)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        active = st.list_active_slots(
            limit=int(limit))
        _record_action(
            "luna_working_memory_recent",
            reason=reason, ok=True,
            detail={"row_count": len(rows),
                      "active_count": len(active)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "active_slots": active}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_working_memory_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause JJ allocation (flips
    `cognitive_working_memory_paused` to True). The state store
    + audit remain readable but the governor refuses to
    allocate. Idempotent. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_working_memory_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_working_memory_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_working_memory_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume JJ allocation. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_working_memory_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_working_memory_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_working_memory_clear_cooldowns(
    *, reason: str = "operator_clear_cooldowns",
) -> Dict[str, Any]:
    """Operator-callable: wipe all hysteresis cooldowns so
    previously-demoted (kind, source_ref) tuples become eligible
    for re-allocation. Audited. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_working_memory_state as st)
        r = st.clear_cooldowns(reason=str(reason))
        _record_action(
            "luna_working_memory_clear_cooldowns",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={"cleared_count":
                      r.get("cleared_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_working_memory_delete_audit(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe JJ audit ledger
    AND the active-state snapshot. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_working_memory_audit as aud,
            cognitive_working_memory_state as st)
        aud_r = aud.delete_all(reason=str(reason))
        st_r = st.delete_all(reason=str(reason))
        _record_action(
            "luna_working_memory_delete_audit",
            reason=str(reason),
            ok=bool(aud_r.get("ok")
                      and st_r.get("ok")),
            detail={
                "deleted_row_count":
                    aud_r.get("deleted_row_count"),
                "deleted_slot_count":
                    st_r.get("deleted_slot_count"),
                "deleted_cooldown_count":
                    st_r.get(
                        "deleted_cooldown_count")})
        return {"ok": bool(aud_r.get("ok")
                            and st_r.get("ok")),
                "audit": aud_r,
                "state": st_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program KK — Execution packing + prompt assembly wrappers
def luna_execution_packing_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated KK component reports +
    current paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("contract",
             "luna_modules."
             "cognitive_execution_packing_contract"),
            ("packer",
             "luna_modules."
             "cognitive_execution_packer"),
            ("bloat_governor",
             "luna_modules."
             "cognitive_execution_bloat_governor"),
            ("packed_state",
             "luna_modules."
             "cognitive_execution_packed_state"),
            ("audit",
             "luna_modules."
             "cognitive_execution_packing_audit"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_execution_packing"
            "_enabled", True))
        out["paused"] = bool(flags.get(
            "cognitive_execution_packing_paused",
            False))
        _record_action(
            "luna_execution_packing_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_packing_recent(
    *, limit: int = 24,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent packing-audit rows + counts by
    event + latest packed window. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_execution_packing_audit as aud,
            cognitive_execution_packed_state as st)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        window = st.latest_window()
        _record_action(
            "luna_execution_packing_recent",
            reason=reason, ok=True,
            detail={"rows": len(rows)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "latest_window": window}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_packing_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause KK packer (paused=True). The
    contract + audit remain readable but the packer refuses to
    pack. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_execution_packing_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_execution_packing_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_packing_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume KK packer. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_execution_packing_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_execution_packing_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_packing_delete_audit(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe KK audit ledger
    AND the packed-state snapshot. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_execution_packing_audit as aud,
            cognitive_execution_packed_state as st)
        aud_r = aud.delete_all(reason=str(reason))
        st_r = st.delete_all(reason=str(reason))
        _record_action(
            "luna_execution_packing_delete_audit",
            reason=str(reason),
            ok=bool(aud_r.get("ok")
                      and st_r.get("ok")),
            detail={
                "deleted_row_count":
                    aud_r.get("deleted_row_count"),
                "deleted_block_count":
                    st_r.get(
                        "deleted_block_count")})
        return {"ok": bool(aud_r.get("ok")
                            and st_r.get("ok")),
                "audit": aud_r,
                "state": st_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program LL — Sovereign task decomposition + plan stitching wrappers
def luna_task_planning_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated LL component reports +
    current paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("state",
             "luna_modules.cognitive_task_plan_state"),
            ("decomposer",
             "luna_modules.cognitive_task_decomposer"),
            ("stitcher",
             "luna_modules.cognitive_plan_stitcher"),
            ("progress_tracker",
             "luna_modules."
             "cognitive_plan_progress_tracker"),
            ("bloat_governor",
             "luna_modules."
             "cognitive_plan_bloat_governor"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_task_planning_enabled",
            True))
        out["paused"] = bool(flags.get(
            "cognitive_task_planning_paused", False))
        _record_action(
            "luna_task_planning_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_task_planning_recent(
    *, limit: int = 8,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent active-plan snapshots +
    progress tracker output. Read-only. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_task_plan_state as st,
            cognitive_plan_progress_tracker as pt)
        plans = st.list_plans(
            limit=max(1, int(limit)))
        all_snap = pt.snapshot_all_active(
            limit=max(1, int(limit)))
        _record_action(
            "luna_task_planning_recent",
            reason=reason, ok=True,
            detail={"plan_count": len(plans)})
        return {"ok": True,
                "plans": plans,
                "active_progress": all_snap}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_task_planning_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause LL task planning. State remains
    readable but the stitcher + decomposer refuse to create new
    plans / steps. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_task_planning_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_task_planning_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_task_planning_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume LL task planning. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_task_planning_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_task_planning_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_task_planning_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL LL plans +
    steps. NEVER raises. Does NOT touch upstream stores."""
    try:
        from luna_modules import (
            cognitive_task_plan_state as st)
        r = st.delete_all(reason=str(reason))
        _record_action(
            "luna_task_planning_delete_all",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={
                "deleted_plan_count":
                    r.get("deleted_plan_count"),
                "deleted_step_count":
                    r.get("deleted_step_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_task_planning_refresh(
    *, reason: str = "operator_refresh",
) -> Dict[str, Any]:
    """Operator-callable: run a retirement + sweep pass.
    Background-style cleanup; does NOT create new work.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_task_plan_state as st,
            cognitive_plan_bloat_governor as bg)
        retire_eval = bg.evaluate_retirement_pass()
        retire_ids: list = []
        if (isinstance(retire_eval, dict)
                and retire_eval.get("retire_list")):
            for entry in retire_eval["retire_list"]:
                pid = (entry.get("plan_id")
                          if isinstance(entry, dict)
                          else None)
                if pid:
                    r = st.set_plan_state(
                        plan_id=pid,
                        new_state="superseded",
                        reason="operator_refresh_retire")
                    if r.get("ok"):
                        retire_ids.append(pid)
        _record_action(
            "luna_task_planning_refresh",
            reason=str(reason),
            ok=True,
            detail={"retired_count": len(retire_ids)})
        return {"ok": True,
                "retired_plan_ids": retire_ids,
                "retire_eval": retire_eval}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program MM — Step execution orchestrator + bounded recovery wrappers
def luna_step_execution_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated MM component reports +
    current paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("state",
             "luna_modules."
             "cognitive_step_execution_state"),
            ("dispatcher",
             "luna_modules."
             "cognitive_step_dispatcher"),
            ("recovery_governor",
             "luna_modules."
             "cognitive_step_recovery_governor"),
            ("next_step_controller",
             "luna_modules."
             "cognitive_next_step_controller"),
            ("audit",
             "luna_modules."
             "cognitive_step_execution_audit"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_step_execution"
            "_enabled", True))
        out["paused"] = bool(flags.get(
            "cognitive_step_execution_paused", False))
        _record_action(
            "luna_step_execution_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_execution_recent(
    *, limit: int = 32,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent MM events + active executions +
    top refusal reasons. Read-only. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_step_execution_state as st,
            cognitive_step_execution_audit as aud)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        refusal_top = aud.top_refusal_reasons(
            limit=500)
        active = st.list_active_executions(
            limit=max(1, int(limit)))
        _record_action(
            "luna_step_execution_recent",
            reason=reason, ok=True,
            detail={"rows": len(rows)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "top_refusal_reasons": refusal_top,
                "active_executions": active}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_execution_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause MM dispatcher. Audit + state remain
    readable but the dispatcher refuses new dispatches. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_step_execution_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_step_execution_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_execution_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume MM dispatcher. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_step_execution_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_step_execution_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_execution_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL MM executions
    AND the audit ledger. NEVER raises. Does NOT touch upstream
    stores."""
    try:
        from luna_modules import (
            cognitive_step_execution_state as st,
            cognitive_step_execution_audit as aud)
        st_r = st.delete_all(reason=str(reason))
        aud_r = aud.delete_all(reason=str(reason))
        _record_action(
            "luna_step_execution_delete_all",
            reason=str(reason),
            ok=bool(st_r.get("ok")
                      and aud_r.get("ok")),
            detail={
                "deleted_execution_count":
                    st_r.get(
                        "deleted_execution_count"),
                "deleted_row_count":
                    aud_r.get("deleted_row_count")})
        return {"ok": bool(st_r.get("ok")
                            and aud_r.get("ok")),
                "state": st_r,
                "audit": aud_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_execution_refresh(
    *, reason: str = "operator_refresh",
) -> Dict[str, Any]:
    """Operator-callable: advance the MM turn counter (used by
    cooldown bookkeeping) and return a fresh status snapshot.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_step_execution_state as st)
        adv = st.advance_turn()
        rep = st.report()
        _record_action(
            "luna_step_execution_refresh",
            reason=str(reason),
            ok=True,
            detail={"current_turn":
                        adv.get("current_turn")})
        return {"ok": True,
                "turn": adv, "state_report": rep}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program NN — Step action table + bounded tool dispatch wrappers
def luna_step_action_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated NN component reports +
    paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("registry",
             "luna_modules."
             "cognitive_step_action_registry"),
            ("adapters",
             "luna_modules."
             "cognitive_step_action_adapters"),
            ("mapper",
             "luna_modules."
             "cognitive_step_action_mapper"),
            ("dispatcher",
             "luna_modules."
             "cognitive_action_dispatcher"),
            ("audit",
             "luna_modules."
             "cognitive_action_dispatch_audit"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_step_action_enabled",
            True))
        out["paused"] = bool(flags.get(
            "cognitive_step_action_paused", False))
        out["feedback_to_mm_enabled"] = bool(
            flags.get(
                "cognitive_step_action_feedback"
                "_to_mm_enabled", True))
        _record_action(
            "luna_step_action_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_action_recent(
    *, limit: int = 32,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent NN dispatch events + counts +
    top refusal reasons. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_action_dispatch_audit as aud)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        by_kind = aud.counts_by_action_kind(
            limit=500)
        refusal_top = aud.top_refusal_reasons(
            limit=500)
        _record_action(
            "luna_step_action_recent",
            reason=reason, ok=True,
            detail={"rows": len(rows)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "counts_by_action_kind": by_kind,
                "top_refusal_reasons":
                    refusal_top}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_action_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause NN dispatcher. Audit + registry
    remain readable. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_step_action_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_step_action_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_action_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume NN dispatcher. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_step_action_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_step_action_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_action_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe the NN audit
    ledger. NEVER touches upstream stores. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_action_dispatch_audit as aud)
        r = aud.delete_all(reason=str(reason))
        _record_action(
            "luna_step_action_delete_all",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={"deleted_row_count":
                        r.get("deleted_row_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_step_action_refresh(
    *, reason: str = "operator_refresh",
) -> Dict[str, Any]:
    """Operator-callable: emit a fresh status + recent snapshot.
    NEVER raises."""
    try:
        s = luna_step_action_status(reason=reason)
        r = luna_step_action_recent(
            limit=24, reason=reason)
        _record_action(
            "luna_step_action_refresh",
            reason=str(reason), ok=True,
            detail={})
        return {"ok": True, "status": s,
                "recent": r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program OO — Outcome-to-action learning + policy shaping wrappers
def luna_policy_shaping_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated OO component reports +
    paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("learning_state",
             "luna_modules."
             "cognitive_outcome_learning_state"),
            ("distiller",
             "luna_modules."
             "cognitive_outcome_distiller"),
            ("shaper",
             "luna_modules."
             "cognitive_action_policy_shaper"),
            ("audit",
             "luna_modules."
             "cognitive_policy_shaping_audit"),
            ("override_reader",
             "luna_modules."
             "cognitive_policy_override_reader"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_policy_shaping"
            "_enabled", True))
        out["paused"] = bool(flags.get(
            "cognitive_policy_shaping_paused", False))
        out["distill_each_turn_enabled"] = bool(
            flags.get(
                "cognitive_runtime_distill_each_turn"
                "_enabled", True))
        _record_action(
            "luna_policy_shaping_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_policy_shaping_recent(
    *, limit: int = 32,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent OO events + counts +
    top refusal reasons. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_policy_shaping_audit as aud,
            cognitive_action_policy_shaper as sh)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        by_target = aud.counts_by_target(limit=500)
        refusal_top = aud.top_refusal_reasons(
            limit=500)
        overrides = sh.get_overrides()
        _record_action(
            "luna_policy_shaping_recent",
            reason=reason, ok=True,
            detail={"rows": len(rows)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "counts_by_target": by_target,
                "top_refusal_reasons":
                    refusal_top,
                "overrides": overrides}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_policy_shaping_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause OO shaper. Existing overrides
    remain readable but no new nudges are applied. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_policy_shaping_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_policy_shaping_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_policy_shaping_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume OO shaper. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_policy_shaping_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_policy_shaping_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_policy_shaping_revert_all(
    *, reason: str = "operator_revert_all",
) -> Dict[str, Any]:
    """Operator-callable: clear ALL active overrides back to the
    empty state (preserves audit history). NEVER raises."""
    try:
        from luna_modules import (
            cognitive_action_policy_shaper as sh)
        r = sh.revert_all(reason=str(reason))
        _record_action(
            "luna_policy_shaping_revert_all",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_policy_shaping_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe OO learning state
    + audit AND clear all overrides. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_outcome_learning_state as ls,
            cognitive_policy_shaping_audit as aud,
            cognitive_action_policy_shaper as sh)
        ls_r = ls.delete_all(reason=str(reason))
        aud_r = aud.delete_all(reason=str(reason))
        sh_r = sh.revert_all(reason=str(reason))
        _record_action(
            "luna_policy_shaping_delete_all",
            reason=str(reason),
            ok=bool(ls_r.get("ok")
                      and aud_r.get("ok")
                      and sh_r.get("ok")),
            detail={
                "deleted_action_kind_count":
                    ls_r.get(
                        "deleted_action_kind_count"),
                "deleted_row_count":
                    aud_r.get("deleted_row_count")})
        return {"ok": bool(ls_r.get("ok")
                            and aud_r.get("ok")
                            and sh_r.get("ok")),
                "learning_state": ls_r,
                "audit": aud_r,
                "shaper_revert": sh_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


# Program PP — Long-horizon execution memory + consolidation wrappers
def luna_execution_memory_status(
    *, reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: aggregated PP component reports +
    paused posture. Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {}}
        for name, mod_path in (
            ("memory_state",
             "luna_modules."
             "cognitive_execution_memory_state"),
            ("consolidator",
             "luna_modules."
             "cognitive_strategy_consolidator"),
            ("promotion_governor",
             "luna_modules."
             "cognitive_strategy_promotion_governor"),
            ("advisor",
             "luna_modules."
             "cognitive_strategy_advisor"),
            ("audit",
             "luna_modules."
             "cognitive_execution_memory_audit"),
        ):
            try:
                m = __import__(
                    mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}"}
        flags = _read_flags()
        out["runtime_use_enabled"] = bool(flags.get(
            "cognitive_runtime_use_execution_memory"
            "_enabled", True))
        out["paused"] = bool(flags.get(
            "cognitive_execution_memory_paused", False))
        _record_action(
            "luna_execution_memory_status",
            reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_memory_recent(
    *, limit: int = 32,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: recent PP events + promoted strategies +
    top refusal reasons. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_execution_memory_audit as aud,
            cognitive_execution_memory_state as st)
        rows = aud.recent(limit=int(limit))
        counts = aud.counts_by_event(limit=500)
        by_kind = aud.counts_by_kind(limit=500)
        refusal_top = aud.top_refusal_reasons(
            limit=500)
        promoted = st.list_strategies(
            strategy_state="promoted",
            limit=max(1, int(limit)))
        _record_action(
            "luna_execution_memory_recent",
            reason=reason, ok=True,
            detail={"rows": len(rows)})
        return {"ok": True,
                "rows": rows,
                "counts_by_event": counts,
                "counts_by_kind": by_kind,
                "top_refusal_reasons": refusal_top,
                "promoted_strategies": promoted}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_memory_pause(
    *, reason: str = "operator_pause",
) -> Dict[str, Any]:
    """Operator-callable: pause PP consolidation + promotion.
    Existing strategies remain readable. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_execution_memory_paused",
            True, reason=str(reason or "")[:200])
        _record_action(
            "luna_execution_memory_pause",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_memory_resume(
    *, reason: str = "operator_resume",
) -> Dict[str, Any]:
    """Operator-callable: resume PP. NEVER raises."""
    try:
        r = set_flag(
            "cognitive_execution_memory_paused",
            False, reason=str(reason or "")[:200])
        _record_action(
            "luna_execution_memory_resume",
            reason=str(reason or ""),
            ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_memory_retire_all(
    *, reason: str = "operator_retire_all",
) -> Dict[str, Any]:
    """Operator-callable: retire all promoted/candidate strategies
    (reversible; records preserved for audit). NEVER raises."""
    try:
        from luna_modules import (
            cognitive_execution_memory_state as st)
        r = st.retire_all(reason=str(reason))
        _record_action(
            "luna_execution_memory_retire_all",
            reason=str(reason),
            ok=bool(r.get("ok")),
            detail={"retired_count":
                        r.get("retired_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_execution_memory_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe PP strategy state
    + audit. NEVER raises. Does NOT touch upstream stores."""
    try:
        from luna_modules import (
            cognitive_execution_memory_state as st,
            cognitive_execution_memory_audit as aud)
        st_r = st.delete_all(reason=str(reason))
        aud_r = aud.delete_all(reason=str(reason))
        _record_action(
            "luna_execution_memory_delete_all",
            reason=str(reason),
            ok=bool(st_r.get("ok")
                      and aud_r.get("ok")),
            detail={
                "deleted_strategy_count":
                    st_r.get(
                        "deleted_strategy_count"),
                "deleted_row_count":
                    aud_r.get("deleted_row_count")})
        return {"ok": bool(st_r.get("ok")
                            and aud_r.get("ok")),
                "state": st_r, "audit": aud_r}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error":
                    f"{type(exc).__name__}: {exc}"}


def luna_goal_create(
    *, title: str, description: str = "",
    priority: str = "normal", category: str = "general",
    operator_locked: bool = False,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: create a long-horizon goal. NEVER raises."""
    try:
        from luna_modules import cognitive_goal_state as gs
        r = gs.create_goal(
            title=str(title or ""),
            description=str(description or ""),
            priority=str(priority or "normal"),
            category=str(category or "general"),
            operator_locked=bool(operator_locked))
        _record_action(
            "luna_goal_create",
            reason=reason, ok=bool(r.get("ok")),
            detail={"goal_id": r.get("goal_id"),
                    "title_chars": len(str(title or ""))})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_goal_list(
    *, status: Optional[str] = None,
    priority: Optional[str] = None, limit: int = 50,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-readable: list goals. NEVER raises."""
    try:
        from luna_modules import cognitive_goal_state as gs
        rows = gs.list_goals(
            status=status, priority=priority,
            limit=int(limit))
        _record_action(
            "luna_goal_list",
            reason=reason, ok=True,
            detail={"count": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_goal_complete(
    *, goal_id: str,
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: mark a goal completed. NEVER raises."""
    try:
        from luna_modules import cognitive_goal_state as gs
        r = gs.set_status(
            goal_id, status="completed", reason=reason)
        _record_action(
            "luna_goal_complete",
            reason=reason, ok=bool(r.get("ok")),
            detail={"goal_id": goal_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_goal_delete_all(
    *, reason: str = "operator_privacy_wipe",
) -> Dict[str, Any]:
    """Operator-callable PRIVACY PRIMITIVE: wipe ALL goals.
    Records an auditable event. NEVER raises."""
    try:
        from luna_modules import cognitive_goal_state as gs
        r = gs.delete_all(reason=str(reason))
        _record_action(
            "luna_goal_delete_all",
            reason=str(reason), ok=bool(r.get("ok")),
            detail={"deleted_count": r.get("deleted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_goal_status(*, reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-readable: aggregated AA surfaces (state /
    progress / drift / advisor). Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {},
        }
        for name, mod_path in (
            ("goal_state",
             "luna_modules.cognitive_goal_state"),
            ("goal_progress",
             "luna_modules.cognitive_goal_progress"),
            ("goal_drift_detector",
             "luna_modules.cognitive_goal_drift_detector"),
            ("goal_advisor",
             "luna_modules.cognitive_goal_advisor"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error":
                        f"{type(exc).__name__}: {exc}",
                }
        _record_action("luna_goal_status", reason=reason,
                        ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_kernel_drive_status(*, reason: str = "operator_request"
                                  ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program Z drive-mode
    surfaces (handlers / drive_engine / budget_governor).
    Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {},
        }
        for name, mod_path in (
            ("kernel_stage_handlers",
             "luna_modules.cognitive_kernel_stage_handlers"),
            ("kernel_drive_engine",
             "luna_modules.cognitive_kernel_drive_engine"),
            ("kernel_budget_governor",
             "luna_modules.cognitive_kernel_budget_governor"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
        _record_action("luna_kernel_drive_status",
                        reason=reason, ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_kernel_drive_dry_run(
    *, user_text: str, mode: str = "default",
    main_reply_text: str = "",
    reason: str = "operator_request",
) -> Dict[str, Any]:
    """Operator-callable: invoke drive_engine.drive_turn with
    optional ``main_reply_text``. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_kernel_drive_engine as kde)
        out = kde.drive_turn(
            user_text=str(user_text or ""),
            mode=str(mode or "default"),
            recent_turns=[],
            main_reply=({"ok": True,
                          "text": str(main_reply_text),
                          "backend":
                              "sovereign_dry_run_supplied",
                          "brain_kind": "operator_supplied",
                          "latency_ms": 0}
                          if main_reply_text else None),
            caller="operator_drive_dry_run",
        )
        _record_action(
            "luna_kernel_drive_dry_run",
            reason=reason,
            ok=bool(out.get("ok")),
            detail={
                "task_class": out.get("task_class"),
                "stages_completed":
                    len(out.get("stages_completed") or []),
                "stage_history_rows":
                    len(out.get("stage_history") or []),
            },
        )
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_kernel_status(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program Y unified kernel
    surfaces (state_bus / lifecycle / router / doctrine / kernel).
    Read-only. NEVER raises."""
    try:
        out: Dict[str, Any] = {
            "ok": True, "reason": reason,
            "components": {},
        }
        for name, mod_path in (
            ("kernel_state_bus",
             "luna_modules.cognitive_kernel_state_bus"),
            ("kernel_lifecycle",
             "luna_modules.cognitive_kernel_lifecycle"),
            ("kernel_router",
             "luna_modules.cognitive_kernel_router"),
            ("kernel_doctrine",
             "luna_modules.cognitive_kernel_doctrine"),
            ("unified_kernel",
             "luna_modules.cognitive_unified_kernel"),
        ):
            try:
                m = __import__(mod_path, fromlist=["report"])
                out["components"][name] = m.report()
            except Exception as exc:  # noqa: BLE001
                out["components"][name] = {
                    "available": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
        _record_action("luna_kernel_status", reason=reason,
                        ok=True, detail={})
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_kernel_recent(*, limit: int = 8,
                          reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-readable: last ``limit`` KernelState rows from the
    state bus. Read-only. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_kernel_state_bus as ksb)
        rows = ksb.recent(limit=int(limit))
        _record_action("luna_kernel_recent", reason=reason,
                        ok=True,
                        detail={"limit": int(limit),
                                "rows_returned": len(rows)})
        return {"ok": True, "rows": rows,
                "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_kernel_dry_run(*, user_text: str,
                            mode: str = "default",
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: run ``unified_kernel.process_turn`` in
    dry-run mode (no LLM, no subsystem snapshots) and return the
    fused KernelState. Useful for verifying routing + lifecycle +
    doctrine without touching the live conversation runtime.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_unified_kernel as uk)
        out = uk.process_turn(
            user_text=str(user_text or ""),
            classification=None,
            mode=str(mode or "default"),
            caller="operator_dry_run",
        )
        _record_action(
            "luna_kernel_dry_run",
            reason=reason,
            ok=bool(out.get("ok")),
            detail={
                "task_class": out.get("task_class"),
                "cognitive_mode": out.get("cognitive_mode"),
                "stages_completed":
                    len(out.get("stages_completed") or []),
                "doctrine_violations":
                    int(out.get("doctrine", {}).get(
                        "violation_count") or 0),
            },
        )
        return out
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_dialogue_status(*, reason: str = "operator_request"
                             ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program W dialogue surfaces
    (state / intent / continuity / style / strategy). Read-only.
    NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("dialogue_state",
         "luna_modules.cognitive_dialogue_state"),
        ("conversational_intent",
         "luna_modules.cognitive_conversational_intent"),
        ("relationship_continuity",
         "luna_modules.cognitive_relationship_continuity"),
        ("tone_style_adapter",
         "luna_modules.cognitive_tone_style_adapter"),
        ("dialogue_strategy",
         "luna_modules.cognitive_dialogue_strategy"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Continuity summary at request time.
    try:
        from luna_modules import (
            cognitive_relationship_continuity as rc)
        out["continuity_summary"] = rc.summarize()
    except Exception:  # noqa: BLE001
        out["continuity_summary"] = {}
    # Recent dialogue states.
    try:
        from luna_modules import cognitive_dialogue_state as ds
        out["recent_dialogue_states"] = ds.recent_states(n=5)
    except Exception:  # noqa: BLE001
        out["recent_dialogue_states"] = []
    return out


def luna_dialogue_inspect(*, user_text: str,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: run intent + strategy classification on
    a sample user_text without persisting any dialogue state.
    Useful for inspecting what the engine would do. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    try:
        from luna_modules import (
            cognitive_conversational_intent as ci)
        out["intent"] = ci.classify(user_text=user_text)
    except Exception as exc:  # noqa: BLE001
        out["intent"] = {"ok": False,
                         "error": f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import (
            cognitive_relationship_continuity as rc)
        out["continuity"] = rc.summarize(user_text=user_text)
    except Exception as exc:  # noqa: BLE001
        out["continuity"] = {"ok": False,
                              "error":
                                  f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import cognitive_dialogue_strategy as ds
        out["strategy"] = ds.select_move(
            user_text=user_text,
            intent_classification=out.get("intent") or {},
            continuity_summary=out.get("continuity") or {},
            caller_hint="operator_inspect",
        )
    except Exception as exc:  # noqa: BLE001
        out["strategy"] = {"ok": False,
                            "error":
                                f"{type(exc).__name__}: {exc}"}
    _record_action(
        "luna_dialogue_inspect", reason=reason,
        ok=True,
        detail={"primary_purpose":
                  (out.get("intent") or {}).get(
                      "primary_purpose"),
                "chosen_move":
                  (out.get("strategy") or {}).get("move")},
    )
    return out


def luna_continuity_reset(*, reason: str = "operator_reset"
                              ) -> Dict[str, Any]:
    """Operator-callable: wipe the relationship-continuity state.
    Reversible rollback for the continuity layer. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_relationship_continuity as rc)
        r = rc.reset_continuity(reason=reason)
        _record_action(
            "luna_continuity_reset", reason=reason,
            ok=bool(r.get("ok")),
            detail={"reset": r.get("reset")},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_reflection_status(*, reason: str = "operator_request"
                               ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program V reflective surfaces
    (reflective_state / contradiction_detector /
    confidence_calibrator / verifier_stack / epistemic_discipline).
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("reflective_state",
         "luna_modules.cognitive_reflective_state"),
        ("contradiction_detector",
         "luna_modules.cognitive_contradiction_detector"),
        ("confidence_calibrator",
         "luna_modules.cognitive_confidence_calibrator"),
        ("verifier_stack",
         "luna_modules.cognitive_verifier_stack"),
        ("epistemic_discipline",
         "luna_modules.cognitive_epistemic_discipline"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import cognitive_reflective_state as rs
        out["recent_reflections"] = rs.recent_reflections(n=5)
    except Exception:  # noqa: BLE001
        out["recent_reflections"] = []
    return out


def luna_verify_candidate(*, candidate_text: str,
                              candidate_kind: str = "answer",
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: run a single verifier pass over a
    candidate text and return the full reflective record.
    NEVER raises."""
    try:
        from luna_modules import cognitive_verifier_stack as vs
        r = vs.verify(
            candidate_text=candidate_text,
            candidate_kind=candidate_kind,
            caller_hint="operator_request",
            persist=True,
        )
        _record_action(
            "luna_verify_candidate", reason=reason,
            ok=bool(r.get("ok")),
            detail={
                "reflection_id": r.get("reflection_id"),
                "outcome": r.get("outcome"),
                "confidence_label": r.get("confidence_label"),
                "revised": r.get("revised"),
            },
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_explain_reflection(*, reflection_id: Optional[str] = None,
                                reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-readable: return the most-recent finalized
    reflection OR a specific one by id. NEVER raises."""
    try:
        from luna_modules import cognitive_reflective_state as rs
        recents = rs.recent_reflections(n=20) or []
        if reflection_id:
            for r in recents:
                if r.get("reflection_id") == reflection_id:
                    return {"ok": True, "reflection": r}
            return {"ok": False,
                    "reason":
                        "reflection_id_not_found_in_recent",
                    "checked_count": len(recents)}
        for r in recents:
            if r.get("status") == "finalized":
                return {"ok": True, "reflection": r}
        return {"ok": False,
                "reason": "no_finalized_reflections_yet"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_deliberation_status(*, reason: str = "operator_request"
                                 ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program U deliberation engine
    surfaces (simulation_state / plan_scorer / counterfactual /
    preemption / decision_engine). Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("simulation_state",
         "luna_modules.cognitive_simulation_state"),
        ("plan_scorer",
         "luna_modules.cognitive_plan_scorer"),
        ("counterfactual_engine",
         "luna_modules.cognitive_counterfactual_engine"),
        ("failure_preemption",
         "luna_modules.cognitive_failure_preemption"),
        ("decision_engine",
         "luna_modules.cognitive_simulation_decision_engine"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Recent simulations summary.
    try:
        from luna_modules import cognitive_simulation_state as ss
        out["recent_simulations"] = ss.recent_simulations(n=5)
    except Exception:  # noqa: BLE001
        out["recent_simulations"] = []
    return out


def luna_compare_plans(*, decision_context: str,
                          candidate_plans: List[Dict[str, Any]],
                          scenarios:
                              Optional[List[str]] = None,
                          reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-callable: run a full deliberation cycle on the
    supplied candidate plans and return the explainable record.
    NEVER raises."""
    try:
        from luna_modules import (
            cognitive_simulation_decision_engine as de)
        r = de.deliberate(
            decision_context=decision_context,
            candidate_plans=list(candidate_plans or []),
            caller_hint="operator_request",
            scenarios=scenarios,
            persist=True,
        )
        _record_action(
            "luna_compare_plans", reason=reason,
            ok=bool(r.get("ok")),
            detail={
                "simulation_id": r.get("simulation_id"),
                "chosen_plan_id": r.get("chosen_plan_id"),
                "confidence": r.get("confidence"),
                "robustness": r.get("robustness"),
            },
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_explain_decision(*, simulation_id: Optional[str] = None,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-readable: return the most-recent finalized
    deliberation OR a specific one by id. NEVER raises."""
    try:
        from luna_modules import cognitive_simulation_state as ss
        recents = ss.recent_simulations(n=20) or []
        if simulation_id:
            for r in recents:
                if r.get("simulation_id") == simulation_id:
                    return {"ok": True, "simulation": r}
            return {"ok": False,
                    "reason": "simulation_id_not_found_in_recent",
                    "checked_count": len(recents)}
        for r in recents:
            if r.get("status") == "finalized":
                return {"ok": True, "simulation": r}
        return {"ok": False,
                "reason": "no_finalized_simulations_yet"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_knowledge_status(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program T knowledge engine
    surfaces (ingestion / fabric / governor / synthesis / recall).
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("ingestion",
         "luna_modules.cognitive_knowledge_ingestion_engine"),
        ("fabric",
         "luna_modules.cognitive_research_memory_fabric"),
        ("governor",
         "luna_modules.cognitive_knowledge_trust_governor"),
        ("synthesis",
         "luna_modules.cognitive_research_synthesis"),
        ("recall",
         "luna_modules.cognitive_evidence_grounded_recall"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    return out


def luna_ingest_source(*, text: Optional[str] = None,
                          path: Optional[str] = None,
                          source_type: Optional[str] = None,
                          trust_hint: str = "medium",
                          topic: Optional[str] = None,
                          reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-callable: ingest one source (text or file path) into
    the knowledge engine. Exactly one of ``text`` or ``path`` must
    be provided. NEVER raises."""
    if (text is None) == (path is None):
        return {"ok": False,
                "reason":
                    "must_provide_exactly_one_of_text_or_path"}
    try:
        from luna_modules import (
            cognitive_knowledge_ingestion_engine as ki)
        if path is not None:
            r = ki.ingest_file(
                path=path,
                source_type=source_type,
                trust_hint=trust_hint,
                topic=topic,
            )
        else:
            r = ki.ingest_text(
                text=text or "",
                source_type=source_type or "operator_text",
                trust_hint=trust_hint,
                topic=topic,
            )
        _record_action(
            "luna_ingest_source", reason=reason,
            ok=bool(r.get("ok")),
            detail={"ingestion_id": r.get("ingestion_id"),
                    "durable":
                        (r.get("record") or {}).get(
                            "durable_recommended"),
                    "trust_level":
                        (r.get("govern_result") or {}).get(
                            "trust_level"),
                    "topic": topic},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_synthesize_research(*, ingestion_ids: List[str],
                                 topic: str,
                                 notes: Optional[str] = None,
                                 reason: str = "operator_request"
                                 ) -> Dict[str, Any]:
    """Operator-callable: synthesize a durable research card from
    multiple ingestion records. NEVER raises."""
    try:
        from luna_modules import cognitive_research_synthesis as rs
        r = rs.synthesize_from_ingestions(
            ingestion_ids=list(ingestion_ids or []),
            topic=topic, notes=notes,
        )
        _record_action(
            "luna_synthesize_research", reason=reason,
            ok=bool(r.get("ok")),
            detail={"card_id": r.get("card_id"),
                    "claim_count": r.get("claim_count"),
                    "contradiction_count":
                        r.get("contradiction_count"),
                    "topic": topic},
        )
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_recall_evidence(*, query: str, top_k: int = 5,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: evidence-grounded recall returning explicit
    known/inferred/uncertain labels plus evidence_refs. NEVER raises."""
    try:
        from luna_modules import (
            cognitive_evidence_grounded_recall as er)
        return er.recall(query=query, top_k=int(top_k))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def luna_adaptation_status(*, reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program R adaptation surfaces.
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("trace_store", "luna_modules.cognitive_success_trace_store"),
        ("distillation", "luna_modules.cognitive_distillation_engine"),
        ("governor", "luna_modules.cognitive_adaptation_governor"),
        ("registry", "luna_modules.cognitive_adaptation_registry"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Style summary + recent governor verdicts
    try:
        from luna_modules import cognitive_success_trace_store as sts
        out["style_summary"] = sts.style_summary()
    except Exception:  # noqa: BLE001
        pass
    try:
        from luna_modules import cognitive_adaptation_governor as gov
        out["recent_governor_verdicts"] = gov.recent_verdicts(limit=5)
    except Exception:  # noqa: BLE001
        pass
    return out


def luna_adaptation_distill(*, reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-callable: run one bounded distillation pass now.
    NEVER raises."""
    try:
        from luna_modules import cognitive_distillation_engine as de
        r = de.distill_now(persist=True)
        _record_action("luna_adaptation_distill", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"promoted": r.get("promoted"),
                                 "exemplars": r.get("exemplars"),
                                 "style_cards": r.get("style_cards")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_adaptation_capture(*,
                                outcome_id: Optional[str] = None,
                                intent: str = "main",
                                reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-callable: capture an outcome by id as a success
    trace (or the most-recent eligible success when outcome_id is
    None). NEVER raises."""
    try:
        from luna_modules import cognitive_outcome_memory as om
        from luna_modules import cognitive_success_trace_store as sts
        target = None
        if outcome_id is None:
            successes = om.get_recent_outcomes(
                n=20, verdict="explicit_success",
                only_high_confidence=True) or []
            target = successes[0] if successes else None
        else:
            for o in om.get_recent_outcomes(n=200) or []:
                if o.get("outcome_id") == outcome_id:
                    target = o
                    break
        if target is None:
            return {"ok": False, "reason": "no_eligible_outcome"}
        r = sts.capture_from_outcome(target, intent=intent)
        _record_action("luna_adaptation_capture", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"outcome_id": target.get("outcome_id"),
                                 "trace_id": r.get("trace_id")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_adaptation_demote_trace(*, trace_id: str,
                                       reason: str = "operator_request"
                                       ) -> Dict[str, Any]:
    """Operator-callable: demote a single trace so it stops being
    used by the registry. NEVER raises."""
    try:
        from luna_modules import cognitive_success_trace_store as sts
        r = sts.demote_trace(trace_id, reason=reason)
        _record_action("luna_adaptation_demote_trace", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"trace_id": trace_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_adaptation_clear_all(*, reason: str = "operator_request"
                                  ) -> Dict[str, Any]:
    """Operator-callable: wipe ALL success traces. NEVER raises."""
    try:
        from luna_modules import cognitive_success_trace_store as sts
        r = sts.clear_all(reason=reason)
        _record_action("luna_adaptation_clear_all", reason=reason,
                        ok=bool(r.get("ok")), detail={})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_executive_status(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program Q executive state.
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("state", "luna_modules.cognitive_executive_state"),
        ("arbiter", "luna_modules.cognitive_executive_arbiter"),
        ("interruption", "luna_modules.cognitive_executive_interruption"),
        ("mission_control", "luna_modules.cognitive_mission_control"),
        ("proactivity", "luna_modules.cognitive_executive_proactivity"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    # Latest focus explanation
    try:
        from luna_modules import cognitive_executive_arbiter as ar
        out["last_focus_explanation"] = ar.explain_focus()
    except Exception:  # noqa: BLE001
        pass
    return out


def luna_executive_focus(*, reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: run goal arbitration NOW and pick a focus.
    NEVER raises."""
    try:
        from luna_modules import cognitive_executive_arbiter as ar
        r = ar.choose_focus(persist=True)
        _record_action("luna_executive_focus", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"chosen_goal_id":
                                  r.get("chosen_goal_id"),
                                 "score": r.get("chosen_score")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_executive_interrupt(*, operator_text: Optional[str] = None,
                                  candidate_goal_id:
                                      Optional[str] = None,
                                  force_decision:
                                      Optional[str] = None,
                                  reason: str = "operator_request"
                                  ) -> Dict[str, Any]:
    """Operator-callable: handle a candidate interruption.
    NEVER raises."""
    try:
        from luna_modules import cognitive_executive_interruption as ei
        r = ei.handle_interruption(
            operator_text=operator_text,
            candidate_goal_id=candidate_goal_id,
            force_decision=force_decision)
        _record_action("luna_executive_interrupt", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"decision": r.get("decision")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_mission_resume(*, mission_id: str,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: resume a paused mission + re-arbitrate.
    NEVER raises."""
    try:
        from luna_modules import cognitive_mission_control as mc
        r = mc.resume_mission(mission_id)
        _record_action("luna_mission_resume", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"mission_id": mission_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_executive_proactive(*, reason: str = "operator_request"
                                   ) -> Dict[str, Any]:
    """Operator-callable: generate bounded proactive suggestions now.
    NEVER raises."""
    try:
        from luna_modules import cognitive_executive_proactivity as pp
        r = pp.generate_suggestions(persist=True)
        _record_action("luna_executive_proactive", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"persisted_count":
                                  r.get("persisted_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_perception_status(*, reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-readable: aggregated Program P perception state.
    Read-only. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("screen", "luna_modules.cognitive_screen_perception"),
        ("document", "luna_modules.cognitive_document_perception"),
        ("audio", "luna_modules.cognitive_audio_perception"),
        ("world_model", "luna_modules.cognitive_world_model"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    return out


def luna_screen_capture(*, reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: take ONE on-demand screenshot + active
    window metadata. NEVER raises."""
    try:
        from luna_modules import cognitive_screen_perception as sp
        r = sp.capture_now(reason=reason)
        _record_action("luna_screen_capture", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"png_path": r.get("png_path"),
                                 "window_title":
                                    (r.get("active_window") or
                                     {}).get("title")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_screen_clear(*, reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-callable: wipe all saved screen captures. NEVER raises."""
    try:
        from luna_modules import cognitive_screen_perception as sp
        r = sp.clear_captures()
        _record_action("luna_screen_clear", reason=reason,
                        ok=bool(r.get("ok")), detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_document_perceive(*, path: str, persist: bool = False,
                                reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-callable: bounded text extraction from a document
    path. NEVER raises."""
    try:
        from luna_modules import cognitive_document_perception as dp
        r = dp.perceive(path, persist=persist)
        _record_action("luna_document_perceive", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"path": r.get("path"),
                                 "extension": r.get("extension"),
                                 "char_count": r.get("char_count"),
                                 "persist": persist})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_audio_capture(*, seconds: int = 5,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: ONE-SHOT mic capture + transcription.
    Requires ``cognitive_audio_perception_enabled=True`` (default
    OFF). NEVER raises."""
    try:
        from luna_modules import cognitive_audio_perception as ap
        r = ap.capture_and_transcribe(seconds=int(seconds),
                                         reason=reason)
        _record_action("luna_audio_capture", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"duration_s": r.get("duration_s"),
                                 "wav_path": r.get("wav_path"),
                                 "text_chars":
                                    (r.get("transcription") or
                                     {}).get("char_count")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_audio_clear(*, reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-callable: wipe all saved audio captures + transcripts.
    NEVER raises."""
    try:
        from luna_modules import cognitive_audio_perception as ap
        r = ap.clear_captures()
        _record_action("luna_audio_clear", reason=reason,
                        ok=bool(r.get("ok")), detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_world_snapshot(*, active_document_path:
                                Optional[str] = None,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-callable: fused multimodal world snapshot. NEVER raises."""
    try:
        from luna_modules import cognitive_world_model as wm
        return wm.snapshot(active_document_path=active_document_path)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error":
                f"{type(exc).__name__}: {exc}"}


def luna_workflow_status(*, workflow_id: Optional[str] = None,
                            reason: str = "operator_request"
                            ) -> Dict[str, Any]:
    """Operator-readable: aggregated workflow stack state, or one
    specific workflow if workflow_id is given. NEVER raises."""
    out: Dict[str, Any] = {"ok": True}
    for key, modname in (
        ("state", "luna_modules.cognitive_workflow_state"),
        ("planner", "luna_modules.cognitive_workflow_planner"),
        ("executor", "luna_modules.cognitive_workflow_executor"),
        ("recovery", "luna_modules.cognitive_workflow_recovery"),
    ):
        try:
            mod = __import__(modname, fromlist=["report"])
            out[key] = mod.report()
        except Exception as exc:  # noqa: BLE001
            out[key] = {"available": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    if workflow_id:
        try:
            from luna_modules import cognitive_workflow_state as ws
            from luna_modules import cognitive_workflow_recovery as wr
            wf = ws.get_workflow(workflow_id)
            out["workflow"] = wf
            out["recovery"] = wr.recovery_report(workflow_id)
            out["recommendation"] = wr.recommend_next_action(workflow_id)
        except Exception as exc:  # noqa: BLE001
            out["workflow_lookup_error"] = f"{type(exc).__name__}: {exc}"
    return out


def luna_workflow_plan(*, goal: str,
                          risk_posture: str = "low",
                          project_id: Optional[str] = None,
                          auto_save: bool = True,
                          reason: str = "operator_request"
                          ) -> Dict[str, Any]:
    """Operator-callable: decompose ``goal`` into a bounded step
    plan. By default creates a new workflow + adds the proposed
    steps; pass ``auto_save=False`` to preview without persisting.
    NEVER raises."""
    try:
        from luna_modules import cognitive_workflow_planner as wp
        r = wp.decompose_goal(goal=goal, risk_posture=risk_posture,
                                project_id=project_id,
                                auto_save=auto_save)
        _record_action("luna_workflow_plan", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"workflow_id": r.get("workflow_id"),
                                 "steps_added": r.get("steps_added"),
                                 "auto_save": auto_save})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_workflow_advance(*, workflow_id: str,
                              max_steps: int = 4,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: run up to ``max_steps`` sequential
    safe_auto / dry_run steps. Stops as soon as gating refuses.
    NEVER raises."""
    try:
        from luna_modules import cognitive_workflow_executor as we
        r = we.advance_workflow(workflow_id, max_steps=int(max_steps))
        _record_action("luna_workflow_advance", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"workflow_id": workflow_id,
                                 "iterations": r.get("iterations")})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_workflow_approve(*, workflow_id: str, approval_id: str,
                              approved: bool = True,
                              note: str = "",
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: resolve a pending workflow approval.
    NEVER raises."""
    try:
        from luna_modules import cognitive_workflow_state as ws
        r = ws.resolve_approval(workflow_id, approval_id,
                                  approved=approved, note=note)
        _record_action("luna_workflow_approve", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"workflow_id": workflow_id,
                                 "approval_id": approval_id,
                                 "approved": approved})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_workflow_abandon(*, workflow_id: str,
                              reason: str = "operator_request"
                              ) -> Dict[str, Any]:
    """Operator-callable: mark a workflow as abandoned. NEVER raises."""
    try:
        from luna_modules import cognitive_workflow_state as ws
        r = ws.set_status(workflow_id, "abandoned")
        _record_action("luna_workflow_abandon", reason=reason,
                        ok=bool(r.get("ok")),
                        detail={"workflow_id": workflow_id})
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_preferences_clear(*, reason: str = "operator_request"
                                ) -> Dict[str, Any]:
    """Operator-callable: forget the persisted preference snapshot.
    NEVER raises."""
    try:
        from luna_modules import cognitive_preference_learner as pl
        r = pl.clear_preferences()
        _record_action("luna_preferences_clear", reason=reason,
                        ok=bool(r.get("ok")), detail=r)
        return r
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_sovereign_shutdown(*, reason: str = "operator_request"
                             ) -> Dict[str, Any]:
    """Operator-callable: drop the loaded GGUFs + close their
    chat_sessions. Rollback / explicit teardown. NEVER raises."""
    detail: Dict[str, Any] = {"ack": None, "main": None}
    try:
        from luna_modules import cognitive_sovereign_ack_runtime as sa
        detail["ack"] = sa.get_singleton().shutdown()
    except Exception as exc:  # noqa: BLE001
        detail["ack"] = {"error": f"{type(exc).__name__}: {exc}"}
    try:
        from luna_modules import cognitive_sovereign_main_runtime as sm
        detail["main"] = sm.get_singleton().shutdown()
    except Exception as exc:  # noqa: BLE001
        detail["main"] = {"error": f"{type(exc).__name__}: {exc}"}
    _record_action("luna_sovereign_shutdown", reason=reason, ok=True,
                    detail=detail)
    return {"ok": True, **detail}


# ----------------------------------------------------------------------------
# Self-Improvement surface — Luna verifying / improving HER OWN brain.
# Bounded, reversible (adds self_tests\ only), kill-switchable, NEVER raises.
# ----------------------------------------------------------------------------
def luna_self_improvement_status(*, reason: str = "operator_request"
                                  ) -> Dict[str, Any]:
    """Snapshot of Luna's self-verification health: smoke-test coverage,
    report()-introspection coverage, kill-switch state, bounds. NEVER raises."""
    try:
        from luna_modules import luna_self_improvement as si
        rep = si.report()
        _record_action("luna_self_improvement_status", reason=reason,
                       ok=True, detail={"metric": rep.get("current_metric")})
        return {"ok": True, **rep}
    except Exception as exc:  # noqa: BLE001
        _record_action("luna_self_improvement_status", reason=reason,
                       ok=False, detail={"exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_self_verify(*, max_tests: int = 1000,
                      reason: str = "operator_request") -> Dict[str, Any]:
    """Run the existing self-test suite and return a live pass/fail verdict —
    'is my brain currently healthy?'. Read-only over self_tests\\. NEVER raises.
    NOTE: runs one subprocess per module; can take minutes on a full suite."""
    try:
        from luna_modules import luna_self_improvement as si
        verdict = si.verify_all(max_tests=max_tests)
        _record_action("luna_self_verify", reason=reason,
                       ok=("error" not in verdict),
                       detail={"passed": verdict.get("passed"),
                               "failed": verdict.get("failed"),
                               "pass_rate_pct": verdict.get("pass_rate_pct")})
        return {"ok": ("error" not in verdict), **verdict}
    except Exception as exc:  # noqa: BLE001
        _record_action("luna_self_verify", reason=reason, ok=False,
                       detail={"exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def luna_self_improve_now(*, max_targets: int = 12,
                           reason: str = "operator_request") -> Dict[str, Any]:
    """Run ONE bounded self-improvement cycle: generate + verify smoke tests
    for up to `max_targets` not-yet-covered modules; keep only passing ones,
    flag failures. Obeys the kill-switch. Reversible. NEVER raises."""
    try:
        from luna_modules import luna_self_improvement as si
        res = si.run_improvement_cycle(max_targets=max_targets)
        _record_action("luna_self_improve_now", reason=reason,
                       ok=bool(res.get("ok")),
                       detail={"improved": res.get("improved"),
                               "attempted": res.get("targets_attempted"),
                               "flagged": res.get("flagged_modules")})
        return res
    except Exception as exc:  # noqa: BLE001
        _record_action("luna_self_improve_now", reason=reason, ok=False,
                       detail={"exc": f"{type(exc).__name__}"})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


__all__ = [
    "ALLOWED_FLAGS", "FLAGS_FILE", "ACTION_LOG_PATH",
    "set_flag",
    "engage_native_mode", "disengage_native_mode",
    "engage_safe_mode", "disengage_safe_mode",
    "current_control_posture",
    "recent_actions",
    # Program C
    "list_pending_actions", "approve_action", "deny_action",
    "propose_next_step",
    # Program H
    "run_daily_checkin", "today_summary", "acknowledge_daily_suggestion",
    # Presence Layer + Continuity
    "presence_check", "acknowledge_luna_boot", "luna_speak",
    "recent_conversation_turns", "luna_continuity_context",
    # Program Y — Unified kernel
    "luna_kernel_status", "luna_kernel_recent",
    "luna_kernel_dry_run",
    # Program Z — Kernel drive-mode
    "luna_kernel_drive_status", "luna_kernel_drive_dry_run",
    # Program AA — Long-horizon goals
    "luna_goal_create", "luna_goal_list", "luna_goal_complete",
    "luna_goal_delete_all", "luna_goal_status",
    # Program BB — Self-evaluation + outcome scoring
    "luna_outcome_status", "luna_score_turn",
    "luna_explain_outcome", "luna_outcome_delete_all",
    # Program BB — Program R adaptation bridge
    "luna_bridge_status", "luna_bridge_recent",
    "luna_bridge_rollback", "luna_bridge_delete_all",
    # Program CC — R-side bridge consumer
    "luna_bridge_consumer_status",
    "luna_bridge_consumer_recent",
    "luna_bridge_consumer_rollback",
    "luna_bridge_consumer_delete_all",
    # Program DD — Multi-turn pattern mining
    "luna_pattern_status", "luna_pattern_recent",
    "luna_pattern_refresh", "luna_pattern_delete_all",
    # Program EE — Pattern advisor consumer
    "luna_pattern_consumer_status",
    "luna_pattern_consumer_recent",
    "luna_pattern_consumer_rollback",
    "luna_pattern_consumer_delete_all",
    # Program FF wrappers
    "luna_pattern_consumption_status",
    "luna_pattern_consumption_recent",
    "luna_pattern_consumption_pause",
    "luna_pattern_consumption_resume",
    "luna_pattern_consumption_delete_all",
    # Program GG wrappers
    "luna_meta_policy_status",
    "luna_meta_policy_recent",
    "luna_meta_policy_promote",
    "luna_meta_policy_rollback",
    "luna_meta_policy_pause",
    "luna_meta_policy_resume",
    "luna_meta_policy_delete_all",
    # Program HH wrappers
    "luna_model_selection_status",
    "luna_model_selection_recent",
    "luna_model_selection_pause",
    "luna_model_selection_resume",
    "luna_model_selection_delete_audit",
    # Program II wrappers
    "luna_context_compression_status",
    "luna_context_compression_recent",
    "luna_context_compression_refresh",
    "luna_context_compression_pause",
    "luna_context_compression_resume",
    "luna_context_compression_delete_all",
    # Program JJ wrappers
    "luna_working_memory_status",
    "luna_working_memory_recent",
    "luna_working_memory_pause",
    "luna_working_memory_resume",
    "luna_working_memory_clear_cooldowns",
    "luna_working_memory_delete_audit",
    # Program KK wrappers
    "luna_execution_packing_status",
    "luna_execution_packing_recent",
    "luna_execution_packing_pause",
    "luna_execution_packing_resume",
    "luna_execution_packing_delete_audit",
    # Program LL wrappers
    "luna_task_planning_status",
    "luna_task_planning_recent",
    "luna_task_planning_pause",
    "luna_task_planning_resume",
    "luna_task_planning_delete_all",
    "luna_task_planning_refresh",
    # Program MM wrappers
    "luna_step_execution_status",
    "luna_step_execution_recent",
    "luna_step_execution_pause",
    "luna_step_execution_resume",
    "luna_step_execution_delete_all",
    "luna_step_execution_refresh",
    # Program NN wrappers
    "luna_step_action_status",
    "luna_step_action_recent",
    "luna_step_action_pause",
    "luna_step_action_resume",
    "luna_step_action_delete_all",
    "luna_step_action_refresh",
    # Program OO wrappers
    "luna_policy_shaping_status",
    "luna_policy_shaping_recent",
    "luna_policy_shaping_pause",
    "luna_policy_shaping_resume",
    "luna_policy_shaping_revert_all",
    "luna_policy_shaping_delete_all",
    # Program PP wrappers
    "luna_execution_memory_status",
    "luna_execution_memory_recent",
    "luna_execution_memory_pause",
    "luna_execution_memory_resume",
    "luna_execution_memory_retire_all",
    "luna_execution_memory_delete_all",
    # Self-Improvement surface — Luna verifying/improving her own brain
    "luna_self_improvement_status",
    "luna_self_verify",
    "luna_self_improve_now",
    "list_promoted_facts",
]
