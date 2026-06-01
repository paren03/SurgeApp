"""Cognitive Feature Flags -- Phase 0 Foundation.

Read-only flag reader. Flags live at::

    D:\\SurgeApp\\memory\\cognitive\\feature_flags.json

The file may be absent; that's the safe default state (all OFF).
This module DOES NOT write the file -- the operator owns the file.

Rollback model
--------------
- Delete the file, OR
- Set ``"cognitive_path_enabled": false`` in the file.

Either action turns the cognitive path off completely. No restart is
required because :mod:`cognitive_core` and :mod:`cognitive_event_log`
read the flag on every call (no caching).

Phase 0 flags
-------------
- ``cognitive_path_enabled``     -- master switch. If False NO cognitive
                                    code runs.
- ``cognitive_logging_enabled``  -- if True, event log writes JSONL to
                                    disk; if False, ``emit()`` is a noop.
- ``cognitive_dry_run_only``     -- if True, executors must never take
                                    real action. (Phase 0 contract: always
                                    True. Future executor implementations
                                    will refuse to run with this False.)

Phase 1 flags
-------------
- ``cognitive_observation_enabled`` -- if True, the observation engine
                                       (luna_modules.observation_engine)
                                       collects real Luna state and
                                       persists ``memory/cognitive/
                                       latest_observation.json``. Default
                                       True because observation is
                                       read-only (no behavioral impact).
                                       Rollback by setting to False -- all
                                       integration sites become no-ops.

Phase 2 flags
-------------
- ``cognitive_interpretation_enabled`` -- if True, the interpretation
                                          engine (luna_modules.
                                          interpretation_engine) classifies
                                          observations into findings and
                                          persists ``memory/cognitive/
                                          latest_interpretation.json``.
                                          Default True because the engine
                                          is pure projection over the
                                          observation file (no external
                                          calls, no behavioral impact).
                                          Rollback by setting to False --
                                          all integration sites become
                                          no-ops.

Phase 3 flags
-------------
- ``cognitive_goal_generation_enabled`` -- if True, the goal-generation
                                           engine (luna_modules.
                                           goal_generation_engine) turns
                                           interpreted findings into
                                           Candidate Goals and persists
                                           ``memory/cognitive/
                                           latest_candidate_goals.json``.
                                           Default True because the
                                           engine never takes any action;
                                           it only proposes structured
                                           goals for later phases.
                                           Rollback: set to False.

Phase 4 flags
-------------
- ``cognitive_prioritization_enabled`` -- if True, the prioritizer
                                          (luna_modules.goal_prioritizer)
                                          ranks Candidate Goals into
                                          Prioritized Goals and persists
                                          ``memory/cognitive/
                                          latest_prioritized_goals.json``
                                          plus a bounded history index
                                          at ``goal_history_index.json``
                                          (used for novelty/age scoring).
                                          Default True because scoring
                                          is pure projection (no action).
                                          Rollback: set to False -- all
                                          integration sites become no-ops.

Phase 5 flags
-------------
- ``cognitive_planning_enabled`` -- if True, the planner
                                    (luna_modules.planning_engine) emits
                                    a bounded read-only execution plan
                                    per top-ranked prioritized goal and
                                    persists ``memory/cognitive/
                                    latest_plans.json``. Default True
                                    because every step the planner emits
                                    is dry_run + read_only + bounded;
                                    the planner NEVER executes.
                                    Rollback: set to False -- hook + endpoint
                                    become no-ops.

Phase 6 flags
-------------
- ``cognitive_execution_enabled`` -- if True, the execution coordinator
                                     (luna_modules.execution_coordinator)
                                     carries out a Plan whose every step
                                     is ``read_write_mode='read_only'``
                                     and ``dry_run=True``. Refuses to
                                     run any plan that does not pass the
                                     read-only preflight. Persists
                                     ``memory/cognitive/
                                     latest_execution_results.json``.
                                     Default True because the
                                     coordinator REFUSES non-read-only
                                     plans at preflight. Rollback: set
                                     to False -- hook + endpoint become
                                     no-ops.

Phase 7 flags
-------------
- ``cognitive_reflection_enabled`` -- if True, the reflection engine
                                      (luna_modules.reflection_engine)
                                      analyzes ExecutionResult artifacts
                                      and produces structured reflections
                                      with rule-based assessment + lesson
                                      extraction. Persists ``memory/
                                      cognitive/latest_reflection_results.
                                      json``. Default True because
                                      reflection is pure analysis -- it
                                      NEVER mutates plans or triggers
                                      execution. Rollback: set to False
                                      -- hook + endpoint become no-ops.

Phase 8 flags
-------------
- ``cognitive_memory_synthesis_enabled`` -- if True, the synthesis engine
                                            (luna_modules.memory_synthesis_engine)
                                            turns reflection lessons into
                                            structured SynthesizedMemory
                                            records and writes both a
                                            per-cycle snapshot and a
                                            bounded LRU store. Persists
                                            ``memory/cognitive/
                                            latest_synthesized_memory.json``
                                            and ``memory/cognitive/
                                            synthesized_memory_store.json``.
                                            Default True because synthesis
                                            is store-and-analyze only --
                                            it never mutates other
                                            cognitive state. Rollback: set
                                            to False.

Phase 9 flags
-------------
- ``cognitive_strategy_adaptation_enabled`` -- if True, the strategy
                                               adapter (luna_modules.
                                               strategy_adapter) converts
                                               eligible memories into
                                               bounded StrategyAdjustments
                                               and persists
                                               ``memory/cognitive/
                                               latest_strategy_adjustments.
                                               json``. Default True.
                                               Rollback: set to False.
- ``cognitive_strategy_active``             -- if True, Phase 4's
                                               prioritizer reads the
                                               latest adjustments file
                                               and applies bounded
                                               multipliers to candidate
                                               goal scores. Default True.
                                               Rollback: set to False --
                                               prioritizer ignores
                                               adjustments entirely.
                                               This is the CONSUMER side
                                               of Phase 9; keep separate
                                               from the producer flag so
                                               operator can stop
                                               application without
                                               stopping recording.

Phase 11 flags
--------------
- ``cognitive_decision_scoring_enabled`` -- if True, runtime callers
                                            (worker.py, luna_probe_health_monitor.py)
                                            delegate decision-scoring
                                            and risk-classification to
                                            :mod:`cognitive_decision_scoring`,
                                            which records every call
                                            into ``memory/cognitive/
                                            decision_scoring_audit.json``.
                                            Default True. Rollback: set
                                            to False -- callers revert to
                                            their inline-fallback formulas
                                            (behavior identical, just no
                                            audit trail).

Phase 10 flags
--------------
- ``cognitive_scheduler_enabled``     -- if True, the scheduler
                                         (luna_modules.cognitive_scheduler)
                                         gates the cognitive chain inside
                                         the existing LunaProbeHealthSweepUser
                                         scheduled-task invocation of
                                         run_one_sweep_now. Default True
                                         because the gate adds overlap
                                         protection + audit; if False,
                                         the cognitive hooks run as in
                                         Phase 9 with no scheduler gate.
- ``cognitive_scheduler_kill_switch`` -- operator panic switch. If True,
                                         the scheduler refuses to allow
                                         the cognitive chain to run at
                                         all and records skip_reason=
                                         operator_kill_switch_active.
                                         Default False. Set to True to
                                         halt all cognitive cycles
                                         without uninstalling anything.

API
---
- :func:`read_flags`        -- always returns a complete dict (defaults applied)
- :func:`flag_file_path`    -- absolute path the operator should edit
- :func:`is_path_enabled`   -- shortcut for the master switch
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict


FLAG_DIR = r"D:\SurgeApp\memory\cognitive"
FLAG_FILE = os.path.join(FLAG_DIR, "feature_flags.json")

DEFAULTS: Dict[str, Any] = {
    "schema_version": 1,
    "cognitive_path_enabled": False,
    "cognitive_logging_enabled": False,
    "cognitive_dry_run_only": True,
    # Phase 1: observation engine (read-only collector). Default ON
    # because reading disk state has no behavioral impact and the
    # integration sites are individually rollback-able.
    "cognitive_observation_enabled": True,
    # Phase 2: interpretation engine (pure projection over observations).
    # Default ON because the engine never reaches outside the observation
    # snapshot and never takes action; it just classifies signals into
    # explainable findings.
    "cognitive_interpretation_enabled": True,
    # Phase 3: goal-generation engine (pure projection over interpreted
    # findings). Default ON because the engine never takes action; it
    # only emits structured CandidateGoal records bounded by category
    # and capped per cycle.
    "cognitive_goal_generation_enabled": True,
    # Phase 4: prioritizer (explainable weighted scoring of candidate
    # goals). Default ON because scoring is pure projection.
    "cognitive_prioritization_enabled": True,
    # Phase 5: planner (bounded read-only plan templates per prioritized
    # goal). Default ON because plans are description only -- the planner
    # never executes anything.
    "cognitive_planning_enabled": True,
    # Phase 6: execution coordinator (bounded executor that runs plans
    # whose every step is read_only and dry_run=True). Refuses anything
    # else at preflight. Default ON because preflight rejects unsafe
    # plans before any step runs.
    "cognitive_execution_enabled": True,
    # Phase 7: reflection engine (rule-based analysis of execution
    # results into structured reflections + lessons). Default ON because
    # reflection is analysis-only -- never mutates state, never executes.
    "cognitive_reflection_enabled": True,
    # Phase 8: memory synthesis (turn reflection lessons into structured
    # reusable memory + bounded LRU store). Default ON because synthesis
    # only reads reflections and writes its own snapshot + store; never
    # mutates other cognitive state.
    "cognitive_memory_synthesis_enabled": True,
    # Phase 9: strategy adaptation -- producer side. Default ON to emit
    # bounded adjustments per cycle.
    "cognitive_strategy_adaptation_enabled": True,
    # Phase 9: strategy application -- consumer side. Default ON so the
    # prioritizer reads + applies the adjustments file. Operator can
    # disable application without disabling recording.
    "cognitive_strategy_active": True,
    # Phase 10: cognitive-cycle scheduler gate. Default ON (gate is added
    # at the top of run_one_sweep_now's cognitive hook section). The
    # scheduler does not create a new schedule -- it reuses the existing
    # LunaProbeHealthSweepUser scheduled task.
    "cognitive_scheduler_enabled": True,
    # Phase 10: operator kill switch. Default OFF. Set to True to halt
    # all cognitive cycles without uninstalling anything.
    "cognitive_scheduler_kill_switch": False,
    # Phase 11: cognitive-decision-scoring delegation. When True, runtime
    # callers (worker.py, luna_probe_health_monitor.py) delegate scoring
    # + risk-classification decisions to luna_modules.cognitive_decision_scoring
    # for centralized ownership + audit. Default True. When False,
    # callers use their inline fallback formulas (identical behavior).
    "cognitive_decision_scoring_enabled": True,
    # Phase 12: cognitive-meta-decision delegation. When True, the three
    # migrated worker.py callsites (determine_deployment_decision,
    # simulation_forecast, run_meta_decision selection+threshold) delegate
    # to luna_modules.cognitive_meta_decision for centralized policy +
    # audit. Default True. When False, callsites use their inline
    # fallback formulas (identical behavior, no audit trail).
    "cognitive_meta_decision_enabled": True,
    # Phase 12: cognitive brain runtime. When True, Luna's own brain
    # interface (luna_modules.cognitive_brain_runtime) is enabled and
    # callers route through it instead of calling luna_polaris_ollama_helper
    # directly. Default True. When False, the runtime returns deterministic
    # null responses and callers should use their inline fallback.
    "cognitive_brain_runtime_enabled": True,
    # Phase 12: cognitive brain runtime kill switch. Operator panic flag.
    # Default False. Set to True to force every brain runtime call onto
    # the null adapter without disabling the audit trail.
    "cognitive_brain_runtime_kill_switch": False,
    # Phase 14: cognitive embedding runtime master switch. When True,
    # callers route embeddings through luna_modules.cognitive_brain_embedding_runtime
    # which routes through gpt4all_local_embed -> ollama_embed -> null_embed.
    # Default True. When False, the runtime returns null embeddings and
    # callers should use their inline fallback.
    "cognitive_brain_embedding_runtime_enabled": True,
    # Phase 14: cognitive embedding runtime kill switch. Operator panic.
    # Default False. Set to True to force every embedding call onto the
    # null_embed adapter while still recording the audit.
    "cognitive_brain_embedding_runtime_kill_switch": False,
    # Program A: deep-reasoning adapter (qwen2.5-coder:7b-instruct).
    # Default True. When False, the gpt4all_deep_local adapter reports
    # available=False and is silently skipped by the router. Use to
    # rollback to 1.5B-only without removing the GGUF file.
    "cognitive_deep_adapter_enabled": True,
    # Program A: hybrid retrieval over synthesized memory store.
    # Default True. When False, callers fall back to legacy by-key lookup.
    "cognitive_hybrid_retrieval_enabled": True,
    # Program A: memory curator (project graph + non-reuse markers).
    # Default True. Curator never modifies the source-of-truth memory store;
    # it maintains a parallel curation index. Set False to short-circuit
    # the curator entirely.
    "cognitive_memory_curator_enabled": True,
    # Program A: decision-trace explainability surface.
    # Default True. When False, decision_trace.record() is a no-op so the
    # trace ledger does not grow.
    "cognitive_decision_trace_enabled": True,
    # Program B: self-model. When False, snapshot() returns a stub and
    # routing nudges are disabled. Default True.
    "cognitive_self_model_enabled": True,
    # Program B: cockpit render. When False, the cockpit CLI returns a
    # one-line "cockpit disabled" string. Default True.
    "cognitive_cockpit_enabled": True,
    # Program C: safe hands action layer (propose/execute/rollback bounded
    # writes with per-action checkpoints). Default True. When False, every
    # public function returns {ok: False, error: "safe_hands_disabled"}.
    "cognitive_safe_hands_enabled": True,
    # Program C: long-horizon project memory (objective/milestones/blockers/
    # unfinished work/do-not-retry). Default True.
    "cognitive_project_memory_enabled": True,
    # Program C: resume / continue intelligence (next-step recommendation +
    # stalled-project detection). Default True.
    "cognitive_resume_engine_enabled": True,
    # Program D: Luna-owned voice runtime (SAPI direct primary; legacy fallback;
    # Edge TTS optional-disabled). Default True. Set False to disable speak
    # calls -- runtime returns ok=False with reason="runtime_disabled".
    "cognitive_voice_runtime_enabled": True,
    # Program D: multimodal perception (screenshot + document + workspace).
    # Default True. Set False to disable all perception captures -- runtime
    # returns ok=False with error="runtime_disabled".
    "cognitive_perception_runtime_enabled": True,
    # Program E: task complexity estimator (prompt -> complexity_score + tier).
    # Default True. When False, returns a neutral estimate so the existing
    # capability-index router stays in charge unchanged.
    "cognitive_task_complexity_estimator_enabled": True,
    # Program E: backend health index (derived from audit ledgers).
    # Default True. When False, score_all() returns empty so the router
    # cannot downweight any backend on observed failures.
    "cognitive_backend_health_index_enabled": True,
    # Program E: hardware-aware router (combines complexity + health +
    # self-model posture). Default True. When False, brain runtime falls
    # back to the capability-index router unchanged.
    "cognitive_hardware_aware_router_enabled": True,
    # Program E: deep-context shaper (enriches deep-tier prompts with
    # retrieved memory + last decision rationale + project state).
    # Default True. When False, original prompt is forwarded unchanged.
    "cognitive_deep_context_shaper_enabled": True,
    # Program F: project operator engine (bounded progression cycle +
    # next-best-step intelligence + stall detection). Default True.
    # When False, run_cycle / recommend_next return ok=False with
    # reason="project_operator_disabled" and no cycle audit grows.
    "cognitive_project_operator_enabled": True,
    # Program G: workspace intelligence (bounded file/folder relevance
    # ranking + per-project workspace state + optional opt-in
    # app/window awareness). Default True. When False every public
    # function returns ok=False with reason="workspace_intelligence_disabled".
    "cognitive_workspace_intelligence_enabled": True,
    # Program H: daily operator engine (bounded daily triage + check-in
    # cycle + anti-nag demotion ledger). Default True. When False every
    # public function returns ok=False with reason="daily_operator_disabled"
    # and no cycle audit grows.
    "cognitive_daily_operator_enabled": True,

    # Phase / Program ?  cognitive_presence_runtime is Luna's "I am here"
    # layer: posture synthesis, bounded speech facade, single-shot boot
    # acknowledgement, conversation-turn ledger. Default True. When False
    # every public function returns ok=False with reason
    # ="presence_runtime_disabled" and no state grows. The boot greeting
    # is gated by a SECOND flag (below) so operators can have the
    # runtime ON but stay quiet by default.
    "cognitive_presence_runtime_enabled": True,
    # Audible boot greeting opt-in. Default False — Luna will not surprise
    # the operator with sound at startup unless explicitly turned on.
    "cognitive_presence_speak_on_boot_enabled": False,

    # Luna Voice V3 — phone-call-speed conversational stack.
    # When enabled, presence.speak() routes through the V3 coordinator
    # which:
    #   - detects + applies operator mode-switch commands silently
    #   - shapes text via personality (MyLuna.txt)
    #   - selects the fastest available adapter per intent
    #   - audits every utterance with timing + selection trace
    # When False, speak() falls through to the pre-V3 path (still works,
    # just slower).
    "cognitive_luna_voice_v3_enabled": True,
    # Personality shaping master flag — gates the MyLuna.txt-derived
    # pet-name sprinkling, greeting templates, mode switch detection.
    # Default True. When False the personality runtime becomes a
    # passthrough.
    "cognitive_personality_shaping_enabled": True,

    # Luna Voice V4 — premium custom voice path. When enabled, the V3
    # coordinator exposes the "premium", "premium_greet", and
    # "premium_audition" intents that route through the v4_premium
    # adapter (real Luna sample clips + voice-profile-tuned SAPI). When
    # False, premium intents fall back to the V3 path silently. V3
    # remains the default for all non-premium intents regardless.
    "cognitive_luna_voice_v4_enabled": True,
    # Luna Voice V4.5 — TRUE clone via Coqui XTTS-v2 zero-shot speaker
    # conditioning. When enabled, the V3 coordinator exposes the "clone"
    # intent which routes through the xtts_clone adapter. CPU synth is
    # 5-15 s per utterance, so this is opt-in per call. When False, the
    # clone intent falls back to v4_premium / V3 paths silently.
    "cognitive_luna_voice_v4_5_clone_enabled": True,

    # Luna Conversation Runtime V1 — owns the live chat path. When
    # enabled, /api/conversation/turn routes through the canonical
    # classifier + micro-ack + concurrent main reasoning + voice
    # routing pipeline. When False the runtime returns ok=False with
    # reason="conversation_runtime_disabled"; UI must surface the
    # disabled state honestly (no silent legacy fallback).
    "cognitive_conversation_runtime_enabled": True,
    # When False, the conversation runtime never picks the XTTS clone
    # for the main reply (V3 persistent_sapi stays primary). Useful as
    # a CPU-budget switch without disabling V4.5 entirely.
    "cognitive_conversation_premium_voice_enabled": True,

    # Luna Realtime Acceleration Pass — hybrid ack router (cache-first).
    # V2 doctrine: the hybrid router (cache → Ollama streaming → legacy
    # → deterministic) violates "no Ollama in hot path" + "no canned
    # phrase library as primary ack". Default is now **False** so the
    # live conversation path goes straight to the sovereign runtime.
    # The router code still exists for OFFLINE / non-live use and for
    # operator-controlled rollback.
    "cognitive_conversation_ack_router_enabled": False,
    # Pre-render Luna canonical phrase library via XTTS at boot if any
    # entry is missing. Default False because the first render pass is
    # CPU-heavy and operator-triggered (oc.luna_render_phrase_library).
    "cognitive_phrase_library_autorender_on_boot": False,

    # Luna SOVEREIGN Conversation Runtime V2 — dual-model dynamic local
    # path. When True (default), live chat routes to:
    #   - ack    : cognitive_sovereign_ack_runtime  (gpt4all 1B,
    #              persistent chat_session, ~400-700 ms warm)
    #   - main   : cognitive_sovereign_main_runtime (gpt4all 8B,
    #              persistent chat_session, kind-aware max_tokens)
    # When False, live chat falls back to the V1 path
    # (ack_router + brain_runtime.invoke). Doctrine rules ENFORCED at
    # this flag's "True" branch: no Ollama / no cache / no brain_runtime
    # is allowed in the live hot path; any violation is counted in
    # conversation_state.legacy_path_quarantine.
    "cognitive_sovereign_conversation_runtime_enabled": True,
    # Pre-load both sovereign GGUFs into RAM at boot via
    # ``cognitive_luna_warming.warm_all``. CPU heavy on first boot
    # (~25-40 s per model); subsequent boots reuse OS file cache.
    # Default True so the operator's first live turn after a fresh
    # Luna boot does not pay the cold-load tax.
    "cognitive_sovereign_warm_on_boot_enabled": True,

    # Program M — Big Brain + Deep Memory.
    #
    # Operator-selectable sovereign GGUF model files. Each lives in
    # ``D:\SurgeApp\local_models``. Set to ``null`` (JSON null) to fall
    # back to the runtime module's compiled-in default. Defaults are
    # Program-M canonical: the upgraded Llama-3 family pair.
    #
    # Rollback: flip these values to the legacy names below.
    "cognitive_sovereign_ack_model":
        "Llama-3.2-1B-Instruct-Q4_0.gguf",
    # Legacy ack model (rollback target): "qwen2.5-coder-1.5b-base.gguf"
    "cognitive_sovereign_main_model": "hermes3-8b-llama3.1.gguf",
    # Legacy main model (rollback target): "qwen2.5-coder-7b-instruct.gguf"

    # Program M — deep memory architecture (hierarchical layers +
    # context-pack ranker). When True (default), the conversation
    # runtime asks ``cognitive_deep_memory`` for a per-turn context
    # pack that combines working / episodic / project / semantic /
    # identity memory ranked by relevance + recency + project
    # grounding. When False, the runtime falls back to plain
    # ``recent_turns`` only (V2 behavior).
    "cognitive_deep_memory_enabled": True,
    # Program M — context-pack ranker: hybrid relevance + recency +
    # project-grounding scorer over hot + persisted memories. Default
    # True. Set False to make ``cognitive_deep_memory.assemble_pack``
    # return a recency-only pack (legacy).
    "cognitive_memory_ranker_enabled": True,

    # Program N — Learned Continuity + Self-Improving Local Intelligence.
    # Each layer is independently flag-gated so the operator can disable
    # a single learning surface without touching the others.
    #
    # - Outcome memory: per-turn ledger of {prompt, reply, verdict_inferred}.
    "cognitive_outcome_memory_enabled": True,
    # - Reusable skill traces: bounded LRU of patterns that succeeded.
    "cognitive_skill_traces_enabled": True,
    # - Failure replay: cluster failures, surface avoidance entries.
    "cognitive_failure_replay_enabled": True,
    # - Preference learner: infer style/length/mode from recent turns.
    "cognitive_preference_learner_enabled": True,
    # - Memory consolidator: distill repetitive topics into durable lessons.
    "cognitive_memory_consolidator_enabled": True,
    # - Lessons surface in deep_memory pack (a 6th layer between
    #   semantic and identity). When True the per-turn pack includes
    #   applicable skill traces + avoidance entries + consolidations.
    #   When False the pack stays at the Program M 5-layer shape.
    "cognitive_deep_memory_lessons_layer_enabled": True,

    # Program O — Bounded multi-app workflow operator.
    # MASTER flag for the whole workflow stack (state + planner +
    # executor + recovery). When False every public function returns
    # ok=False with a stable reason.
    "cognitive_workflow_operator_enabled": True,
    # When False, even safe_auto steps require human ``advance``.
    # Default True so the operator gets bounded automation by
    # default, but can flip to False for full manual operation.
    "cognitive_workflow_auto_execute_safe_steps_enabled": True,

    # Program P — Multimodal world model + screen/audio perception.
    #
    # Master per-channel flags. Each channel is independently
    # rollback-able. The defaults reflect the privacy doctrine:
    #
    # - screen perception: ON for on-demand metadata + screenshot
    #   capture (still requires an explicit ``capture_now()`` call;
    #   NEVER background-samples).
    "cognitive_screen_perception_enabled": True,
    # - document perception: ON because it only acts on an
    #   operator-supplied path.
    "cognitive_document_perception_enabled": True,
    # - audio perception: **OFF by default**. Operator must opt in
    #   explicitly to capture microphone audio.
    "cognitive_audio_perception_enabled": False,
    # - world model fuser: ON. Skips channels that are disabled or
    #   unavailable.
    "cognitive_world_model_enabled": True,
    # - perception layer in deep_memory pack: ON. Surfaces a
    #   bounded [World] block in the per-turn context pack.
    "cognitive_deep_memory_perception_layer_enabled": True,

    # Program Q — Executive cortex + long-horizon mission control.
    # MASTER flag — when False every public function in the
    # executive_state / arbiter / interruption / mission_control /
    # proactivity modules returns ok=False with a stable reason.
    "cognitive_executive_cortex_enabled": True,
    # Bounded proactivity (per-rule trigger). Default True; flip
    # to False to silence all proactive suggestion generation.
    "cognitive_executive_proactivity_enabled": True,
    # Executive layer in deep_memory pack (7th layer rendered as
    # [Executive] block — current focus, top deferred, pending
    # proactive nudge).
    "cognitive_deep_memory_executive_layer_enabled": True,

    # Program R — Local self-adaptation + distillation engine.
    # Each layer independently flag-gated for fine-grained rollback.
    "cognitive_success_trace_store_enabled": True,
    "cognitive_distillation_engine_enabled": True,
    "cognitive_adaptation_governor_enabled": True,
    # When True the governor will promote a candidate even if it
    # failed its own rules. Default False — operator must explicitly
    # opt in.
    "cognitive_adaptation_force_promote": False,
    "cognitive_adaptation_registry_enabled": True,
    # Whether the live runtime SPLICES distilled artifacts into
    # ack / main prompts. Distinct from registry-enabled — operator
    # can have the registry on (for inspection) but block runtime use.
    "cognitive_runtime_use_adaptation_enabled": True,
    # Auto-capture every explicit_success outcome into the
    # success_trace_store after each turn. Default True.
    "cognitive_runtime_auto_capture_success_traces_enabled": True,
    # Adaptation layer in deep_memory pack (rendered as [Adaptation]
    # block — top exemplars + style_card hint + preference modifier).
    "cognitive_deep_memory_adaptation_layer_enabled": True,

    # Program S — Realtime sovereign acceleration + model fabric.
    "cognitive_model_fabric_enabled": True,
    "cognitive_warm_state_policy_enabled": True,
    # Streaming generation. When True the sovereign ack/main
    # runtimes route through ``cognitive_streaming_generator`` which
    # uses gpt4all's streaming=True mode. When False the runtime
    # falls back to single non-streaming generate() calls
    # (V2-class behavior). Default True.
    "cognitive_streaming_generation_enabled": True,
    "cognitive_realtime_telemetry_enabled": True,
    # When True the ack runtime uses the streaming path. Independent
    # of the master streaming flag so the operator can selectively
    # roll back JUST the ack to non-streaming if a small model
    # misbehaves.
    "cognitive_ack_streaming_enabled": True,
    "cognitive_main_streaming_enabled": True,

    # Program T — Sovereign knowledge engine + research memory fabric.
    # All 5 default True so the engine is live. Operator can flip any
    # one False to roll back that specific layer.
    "cognitive_knowledge_ingestion_enabled": True,
    "cognitive_research_memory_fabric_enabled": True,
    "cognitive_knowledge_trust_governor_enabled": True,
    "cognitive_research_synthesis_enabled": True,
    "cognitive_evidence_grounded_recall_enabled": True,

    # Program U — Sovereign simulation + counterfactual reasoning.
    # 6 layers, each independently rollback-able. Defaults True so
    # the engine is live; flip any False to roll back that layer.
    "cognitive_simulation_state_enabled": True,
    "cognitive_plan_scorer_enabled": True,
    "cognitive_counterfactual_engine_enabled": True,
    "cognitive_failure_preemption_enabled": True,
    "cognitive_simulation_decision_engine_enabled": True,
    # Runtime opt-in: capture a deliberation snapshot per turn.
    # Default True; runtime uses a fast bounded path (no slow
    # branches) so the per-turn cost stays minimal.
    "cognitive_runtime_use_deliberation_enabled": True,

    # Program V — Reflective metacognition + verifier stack.
    # 6 layers, each independently rollback-able. Defaults True.
    "cognitive_reflective_state_enabled": True,
    "cognitive_contradiction_detector_enabled": True,
    "cognitive_confidence_calibrator_enabled": True,
    "cognitive_verifier_stack_enabled": True,
    "cognitive_epistemic_discipline_enabled": True,
    "cognitive_runtime_use_verifier_enabled": True,

    # Program W — Sovereign dialogue mastery + relationship
    # continuity engine. 7 layers, each independently
    # rollback-able.
    "cognitive_dialogue_state_enabled": True,
    "cognitive_conversational_intent_enabled": True,
    "cognitive_relationship_continuity_enabled": True,
    "cognitive_tone_style_adapter_enabled": True,
    "cognitive_dialogue_strategy_enabled": True,
    # Runtime opt-in: enable per-turn dialogue pipeline.
    "cognitive_runtime_use_dialogue_pipeline_enabled": True,
    # When True, the tone/style adapter may rewrite the brain's
    # main reply text in the live hot path. When False, the
    # pipeline still computes targets + records them, but does
    # not modify the operator-facing text.
    "cognitive_runtime_apply_tone_rewrite_enabled": True,

    # Program X — Sovereign capability foundry + self-build
    # engine. Each layer independently rollback-able.
    "cognitive_capability_gap_detector_enabled": True,
    "cognitive_capability_spec_enabled": True,
    "cognitive_capability_synthesis_enabled": True,
    "cognitive_capability_validation_enabled": True,
    "cognitive_capability_self_extension_governor_enabled": True,
    "cognitive_capability_registry_enabled": True,
    # When True, operator wrappers can request synthesis that
    # actually writes the sandbox files. When False, ALL
    # synthesis is dry-run text-only (extra safety rail).
    "cognitive_capability_write_to_sandbox_enabled": True,

    # Program CC — R-side bridge consumer + bridge-derived
    # evidence store. Each layer independently rollback-able.
    "cognitive_bridge_derived_evidence_enabled": True,
    "cognitive_bridge_to_r_translator_enabled": True,
    "cognitive_r_extension_governor_enabled": True,
    "cognitive_bridge_consumer_enabled": True,
    "cognitive_bridge_consumer_audit_enabled": True,
    # Runtime opt-ins. Default True.
    "cognitive_runtime_use_bridge_consumer_enabled": True,
    "cognitive_runtime_use_bridge_derived_evidence_enabled":
        True,

    # Program DD — Sovereign multi-turn pattern mining +
    # recurring-cause intelligence. Each layer independently
    # rollback-able.
    "cognitive_pattern_state_enabled": True,
    "cognitive_pattern_miner_enabled": True,
    "cognitive_recurring_failure_detector_enabled": True,
    "cognitive_recurring_success_detector_enabled": True,
    "cognitive_pattern_advisor_enabled": True,
    # Runtime opt-in: drive engine's reflect handler invokes
    # the pattern stack on a cadence (default every 10 turns).
    "cognitive_runtime_use_pattern_mining_enabled": True,
    # Cadence (int): patterns mine every N turns inside reflect.
    "cognitive_pattern_mining_cadence_turns": 10,
    # Detector thresholds (ints + floats, NOT in ALLOWED_FLAGS).
    "cognitive_recurring_failure_min_sample": 3,
    "cognitive_recurring_failure_min_confidence": 0.20,
    "cognitive_recurring_success_min_sample": 3,
    "cognitive_recurring_success_min_confidence": 0.20,

    # Program EE — Sovereign pattern advisor consumer.
    "cognitive_pattern_consumer_state_enabled": True,
    "cognitive_pattern_q_adapter_enabled": True,
    "cognitive_pattern_w_adapter_enabled": True,
    "cognitive_pattern_cc_adapter_enabled": True,
    "cognitive_pattern_consumer_enabled": True,
    "cognitive_pattern_consumer_audit_enabled": True,
    "cognitive_runtime_use_pattern_consumer_enabled": True,
    # Adapter thresholds (floats, NOT in ALLOWED_FLAGS).
    "cognitive_pattern_q_adapter_target_confidence_threshold":
        0.6,
    "cognitive_pattern_q_adapter_min_pattern_confidence":
        0.25,
    "cognitive_pattern_w_adapter_target_confidence_threshold":
        0.7,
    "cognitive_pattern_w_adapter_min_pattern_confidence":
        0.25,
    "cognitive_pattern_cc_adapter_min_pattern_confidence":
        0.30,

    # Program FF — Sovereign pattern-aware executive refinement
    # + live policy consumption. Master + per-consumer enables.
    "cognitive_pattern_consumption_governor_enabled": True,
    "cognitive_q_pattern_consumer_enabled": True,
    "cognitive_w_pattern_consumer_enabled": True,
    "cognitive_cc_pattern_consumer_enabled": True,
    "cognitive_pattern_consumption_audit_enabled": True,
    # Runtime opt-in (separate from individual consumer enables
    # so operator can pause LIVE consumption without disabling
    # the EE hint store or the FF reflect snapshot).
    "cognitive_runtime_use_pattern_consumption_enabled": True,
    # Bounded influence caps. ints, NOT in ALLOWED_FLAGS.
    "cognitive_pattern_consumption_max_per_turn": 1,
    "cognitive_pattern_consumption_max_per_24h": 10,
    # CC adapter does not have its own EE-side target threshold;
    # FF defines one here. float, NOT in ALLOWED_FLAGS.
    "cognitive_pattern_consumption_cc_target_confidence_threshold":
        0.7,

    # Program GG — Sovereign meta-policy learning + threshold
    # refinement. All bool flags below are in ALLOWED_FLAGS.
    "cognitive_meta_policy_evidence_enabled": True,
    "cognitive_meta_policy_proposer_enabled": True,
    "cognitive_meta_policy_proposal_state_enabled": True,
    "cognitive_meta_policy_apply_governor_enabled": True,
    "cognitive_meta_policy_audit_enabled": True,
    # Runtime opt-in for the reflect-stage GG snapshot. Distinct
    # from auto-apply (which is OFF by default below).
    "cognitive_runtime_use_meta_policy_enabled": True,
    # Auto-apply gate. DEFAULT FALSE — operator must opt in
    # explicitly. Even when True, every auto-apply is bounded
    # by confidence / sample / per-cycle / per-24h caps and
    # restricted to the closed MUTABLE_KNOBS set.
    "cognitive_runtime_use_meta_policy_auto_apply_enabled": False,
    # Int/float knobs (NOT in ALLOWED_FLAGS).
    "cognitive_meta_policy_min_observation_samples": 30,
    "cognitive_meta_policy_auto_apply_min_confidence": 0.7,
    "cognitive_meta_policy_per_cycle_delta_float": 0.05,
    "cognitive_meta_policy_per_cycle_delta_int": 1,
    "cognitive_meta_policy_observation_window_s": 86_400,
    "cognitive_meta_policy_auto_apply_max_per_24h_per_knob": 3,

    # Program HH — Sovereign model selection + quality-tier
    # orchestration. All 6 bool flags below are in ALLOWED_FLAGS.
    "cognitive_quality_tier_registry_enabled": True,
    "cognitive_model_selection_context_enabled": True,
    "cognitive_tier_selection_governor_enabled": True,
    "cognitive_tier_selection_audit_enabled": True,
    # Runtime opt-in — when False, conversation_runtime skips
    # the HH governor and uses the existing hardware_aware_router
    # path unchanged.
    "cognitive_runtime_use_model_selection_enabled": True,
    # Operator pause primitive (separate from the runtime opt-in
    # so operator can pause selection without disabling the
    # entire stack).
    "cognitive_model_selection_paused": False,

    # Program II — Sovereign context compression + cross-session
    # recall prioritization. All 7 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_context_compression_state_enabled": True,
    "cognitive_context_compressor_enabled": True,
    "cognitive_context_bloat_governor_enabled": True,
    "cognitive_recall_priority_enabled": True,
    "cognitive_cross_session_recall_enabled": True,
    "cognitive_runtime_use_context_compression_enabled": True,
    "cognitive_context_compression_paused": False,
    # Int/float knobs (NOT in ALLOWED_FLAGS — file-edit only).
    "cognitive_context_compression_max_per_session": 8,
    "cognitive_context_compression_max_per_24h": 24,
    "cognitive_context_compression_min_signal_sum": 0.6,
    "cognitive_context_compression_duplicate_jaccard": 0.78,
    "cognitive_cross_session_recall_max_units": 3,
    "cognitive_cross_session_recall_max_chars": 2400,

    # Program JJ — Sovereign working-memory allocation +
    # attention budgeting. All 7 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_working_memory_slot_registry_enabled": True,
    "cognitive_attention_candidate_pool_enabled": True,
    "cognitive_attention_budget_governor_enabled": True,
    "cognitive_working_memory_state_enabled": True,
    "cognitive_working_memory_audit_enabled": True,
    "cognitive_runtime_use_working_memory_enabled": True,
    "cognitive_working_memory_paused": False,
    # Int/float knobs (NOT in ALLOWED_FLAGS — file-edit only).
    "cognitive_working_memory_total_slot_cap": 8,
    "cognitive_working_memory_hysteresis_turns": 2,

    # Program KK — Sovereign execution packing + prompt
    # assembly discipline. All 7 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_execution_packing_contract_enabled": True,
    "cognitive_execution_packer_enabled": True,
    "cognitive_execution_bloat_governor_enabled": True,
    "cognitive_execution_packed_state_enabled": True,
    "cognitive_execution_packing_audit_enabled": True,
    "cognitive_runtime_use_execution_packing_enabled": True,
    "cognitive_execution_packing_paused": False,

    # Program Y — Sovereign Unified Cognitive Kernel + Central
    # Nervous System. Each layer independently rollback-able.
    "cognitive_kernel_state_bus_enabled": True,
    "cognitive_kernel_lifecycle_enabled": True,
    "cognitive_kernel_router_enabled": True,
    "cognitive_kernel_doctrine_enabled": True,
    "cognitive_unified_kernel_enabled": True,
    # Runtime opt-in: when True, conversation_runtime calls
    # ``cognitive_unified_kernel.process_turn`` at the end of
    # each turn to fuse all subsystem snapshots into one
    # KernelState record. When False the runtime continues
    # operating identically without kernel fusion.
    "cognitive_runtime_use_unified_kernel_enabled": True,

    # Program Z — Sovereign kernel drive-mode + lifecycle-owned
    # orchestration. Each layer independently rollback-able.
    "cognitive_kernel_stage_handlers_enabled": True,
    "cognitive_kernel_drive_engine_enabled": True,
    "cognitive_kernel_budget_governor_enabled": True,
    # When True, budget governor records over-budget stages as
    # violations on the doctrine state. When False, budget
    # over-runs are recorded but not flagged as doctrine
    # violations (advisory mode).
    "cognitive_kernel_budget_strict_enabled": True,
    # Total wall-clock cap per turn in ms (operator-tunable).
    "cognitive_kernel_total_turn_ms_cap": 90_000,
    # Runtime opt-in: when True, handle_turn delegates the
    # canonical lifecycle walk to drive_engine.drive_turn. When
    # False the legacy serial pipeline runs unchanged.
    "cognitive_runtime_use_kernel_drive_enabled": True,

    # Program AA — Sovereign long-horizon goals + cross-turn
    # memory. Each layer independently rollback-able.
    "cognitive_goal_state_enabled": True,
    "cognitive_goal_progress_enabled": True,
    "cognitive_goal_drift_detector_enabled": True,
    "cognitive_goal_planner_enabled": True,
    "cognitive_goal_advisor_enabled": True,
    # Runtime opt-in: when True, the drive engine's reflect
    # stage handler additionally invokes the AA stack (progress
    # → drift → advisor). When False the AA stack does nothing
    # in the live hot path.
    "cognitive_runtime_use_goals_enabled": True,
    # Operator-tunable global cooldown for advisor surfacing
    # (int, number of turns between consecutive surfaces).
    "cognitive_goal_advisor_global_cooldown_turns": 10,

    # Program BB — Sovereign self-evaluation + outcome
    # scoring loop. Each layer independently rollback-able.
    "cognitive_outcome_score_state_enabled": True,
    "cognitive_outcome_scoring_enabled": True,
    "cognitive_failure_attribution_enabled": True,
    "cognitive_goal_outcome_evaluator_enabled": True,
    "cognitive_self_eval_governor_enabled": True,
    # Runtime opt-in: when True, the drive engine's reflect
    # stage handler additionally invokes the BB stack
    # (scoring → attribution → goal_eval → governor →
    # outcome record).
    "cognitive_runtime_use_self_eval_enabled": True,

    # Program BB — Program R adaptation bridge (additive only).
    # Reads BB outcome records flagged for promotion and adds
    # NEW bridge records WITHOUT mutating R's existing
    # adaptation artifacts.
    "cognitive_outcome_adaptation_bridge_enabled": True,
    # Runtime opt-in: when True, the BB reflect-stage extension
    # ALSO invokes bridge_promoted_outcomes once per turn.
    "cognitive_runtime_use_outcome_bridge_enabled": True,

    # Program LL — Sovereign task decomposition + multi-step
    # plan stitching. All 7 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_task_plan_state_enabled": True,
    "cognitive_task_decomposer_enabled": True,
    "cognitive_plan_stitcher_enabled": True,
    "cognitive_plan_progress_tracker_enabled": True,
    "cognitive_plan_bloat_governor_enabled": True,
    "cognitive_runtime_use_task_planning_enabled": True,
    "cognitive_task_planning_paused": False,

    # Program MM — Sovereign step execution orchestrator +
    # bounded recovery. All 7 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_step_execution_state_enabled": True,
    "cognitive_step_dispatcher_enabled": True,
    "cognitive_step_recovery_governor_enabled": True,
    "cognitive_next_step_controller_enabled": True,
    "cognitive_step_execution_audit_enabled": True,
    "cognitive_runtime_use_step_execution_enabled": True,
    "cognitive_step_execution_paused": False,

    # Program NN — Sovereign step action table + bounded tool
    # dispatch. All 8 bool flags below are in ALLOWED_FLAGS.
    "cognitive_step_action_registry_enabled": True,
    "cognitive_step_action_adapters_enabled": True,
    "cognitive_step_action_mapper_enabled": True,
    "cognitive_action_dispatcher_enabled": True,
    "cognitive_action_dispatch_audit_enabled": True,
    "cognitive_runtime_use_step_action_enabled": True,
    "cognitive_step_action_paused": False,
    "cognitive_step_action_feedback_to_mm_enabled": True,

    # Program OO — Sovereign outcome-to-action learning +
    # execution policy shaping. All 8 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_outcome_learning_state_enabled": True,
    "cognitive_outcome_distiller_enabled": True,
    "cognitive_action_policy_shaper_enabled": True,
    "cognitive_policy_shaping_audit_enabled": True,
    "cognitive_runtime_use_policy_shaping_enabled": True,
    "cognitive_policy_shaping_paused": False,
    "cognitive_runtime_distill_each_turn_enabled": True,
    "cognitive_runtime_apply_overrides_to_mm_enabled": True,

    # Program PP — Sovereign long-horizon execution memory +
    # strategy consolidation. All 8 bool flags below are in
    # ALLOWED_FLAGS.
    "cognitive_execution_memory_state_enabled": True,
    "cognitive_strategy_consolidator_enabled": True,
    "cognitive_strategy_promotion_governor_enabled": True,
    "cognitive_execution_memory_audit_enabled": True,
    "cognitive_runtime_use_execution_memory_enabled": True,
    "cognitive_execution_memory_paused": False,
    "cognitive_runtime_consolidate_each_turn_enabled": True,
    "cognitive_runtime_promote_each_turn_enabled": True,

    # Live-brain takeover seam (Step 2) — flag-gated wiring of the
    # bounded JJ->KK->II live-context assembler into the main reply
    # path. Default OFF so behaviour is identical to the deep_memory
    # path until the operator explicitly opts in. Both flags are in
    # ALLOWED_FLAGS. OO/PP are NOT consumed on the live reply path.
    "cognitive_runtime_use_live_brain_context_enabled": False,
    "cognitive_live_brain_context_paused": False,

    # Live-brain takeover seam (Step 3) — flag-gated HH (model-selection
    # / tier governor) influence over the live main-brain timeout. HH can
    # only tune a bounded, sovereign-safe timeout; it can NEVER select a
    # non-sovereign/cloud backend. Default OFF. Both flags in ALLOWED_FLAGS.
    "cognitive_runtime_use_hh_live_routing_enabled": False,
    "cognitive_hh_live_routing_paused": False,
    # Unified Brain Ingress campaign (Step 5 wireup): all default OFF;
    # operator flips to True to activate. Kill-switches preserved via
    # the paired *_paused flags. NEVER raise from any consumer.
    "cognitive_runtime_use_brain_ingress_router_enabled": False,
    "cognitive_brain_ingress_router_paused": False,
    "cognitive_runtime_use_event_distiller_enabled": False,
    "cognitive_event_distiller_paused": False,
    "cognitive_runtime_use_ingress_promotion_enabled": False,
    "cognitive_ingress_promotion_paused": False,
    "cognitive_runtime_use_ingress_recall_enabled": False,
    "cognitive_ingress_recall_paused": False,
    # Elastic Brain (Plan 1): energy-aware model fabric. Master + kill-switch
    # default OFF so the manager lands dark; energy_mode in
    # {eco, balanced, performance}. NEVER raise from any consumer.
    "cognitive_elastic_brain_enabled": False,
    "cognitive_elastic_brain_paused": False,
    "cognitive_elastic_energy_mode": "balanced",
    # Main-brain GPU: when True, cognitive_sovereign_main_runtime loads the
    # model via llama-cpp with GPU offload (proven 3.5x on the 8B). Any GPU
    # load/gen failure auto-falls-back to gpt4all/CPU. Flip False = instant
    # kill-switch back to CPU.
    "cognitive_main_gpu_llamacpp_enabled": False,
    # Ack-brain on llama-cpp (CPU). True = the 1B ack loads via llama-cpp on
    # CPU instead of gpt4all — eliminates gpt4all's ~190s broken-CUDA probe
    # AND its GPU poisoning that made the main 8B load thrash ~29 min. Auto
    # falls back to gpt4all. Flip False = back to gpt4all.
    "cognitive_ack_llamacpp_enabled": False,
    # Fast conversation: when True, post-reply kernel/drive reasoning runs
    # fire-and-forget (updates state for the NEXT turn) instead of blocking the
    # reply. Operator-accepted speed/audit tradeoff. Flip False = synchronous
    # full reasoning (per-turn kernel audit fields present).
    "cognitive_conversation_async_postreply_enabled": False,
    # Coalesce research-fabric usage writes (last_used/usage_count): flush at
    # most once per interval instead of a whole-store rewrite per matched card
    # (~16x/turn). Default True (near-pure perf win; telemetry persists a few
    # seconds later). Flip False = exact prior per-call-write behavior.
    "cognitive_research_fabric_debounce_usage_writes_enabled": True,
    # Max main-reply length (chars) spoken in Serge's XTTS voice clone; longer
    # replies use the fast voice. 0 = always clone (operator wants the cloned
    # voice on EVERYTHING).
    "cognitive_conversation_clone_reply_max_chars": 0,
    # Acks also speak in the voice clone (intent 'clone') instead of the fast
    # voice. Default True (clone on everything); fast SAPI fallback always there
    # so acks never hang.
    "cognitive_conversation_clone_acks_enabled": True,
    # Clone path routes predominantly-Russian text to Serge's RU reference +
    # language 'ru', so Russian speaks in his cloned voice. Default True.
    "cognitive_voice_clone_language_routing_enabled": True,
    # Amortize the per-turn conversation-audit FIFO truncation: re-read the
    # ~1.6 MB ledger only every N appends instead of every turn. Default True.
    "cognitive_conversation_audit_lazy_truncate_enabled": True,
    # Acks play a pre-rendered cloned ack clip (instant + Serge's voice) instead
    # of a live synth; graceful fallback to the live path if no clips. Default
    # True. (Rendered by render_cloned_acks.py.)
    "cognitive_conversation_fixed_cloned_acks_enabled": True,
}


def flag_file_path() -> str:
    """Absolute path to the operator-owned flag file."""
    return FLAG_FILE


def read_flags() -> Dict[str, Any]:
    """Read the current flag set. Always returns a complete dict.

    Behaviour
    ---------
    - File missing      -> return :data:`DEFAULTS` unchanged (everything OFF).
    - File invalid JSON -> return DEFAULTS + ``{"_read_error": "..."}``.
    - File valid        -> merge over DEFAULTS so missing keys get safe values.

    NEVER raises.
    """
    out: Dict[str, Any] = dict(DEFAULTS)
    try:
        if not os.path.isfile(FLAG_FILE):
            return out
        with open(FLAG_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, dict):
            out["_read_error"] = "flags file is not a JSON object"
            return out
        for key in DEFAULTS:
            if key in raw:
                out[key] = raw[key]
        # Forward-compat: carry through extra keys without complaint.
        for key, value in raw.items():
            if key not in out:
                out[key] = value
    except Exception as exc:  # noqa: BLE001
        out["_read_error"] = f"{type(exc).__name__}: {exc}"
    return out


def is_path_enabled() -> bool:
    """Master switch convenience accessor."""
    return bool(read_flags().get("cognitive_path_enabled", False))


__all__ = [
    "DEFAULTS",
    "FLAG_FILE",
    "flag_file_path",
    "read_flags",
    "is_path_enabled",
]
