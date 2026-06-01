"""Luna Conversation Runtime — the canonical live conversation path.

This is the SINGLE owned entry point for live chat. It replaces the
ad-hoc UI -> worker.py file-queue and UI -> /api/voice/v2/stream paths
for normal conversation.

Architecture
------------
For every live turn::

    input
      -> classify (rule-based, sub-ms)
      -> mode_switch detection (operator personality flip)
      -> [PARALLEL]
            (a) micro-ack generation via gpt4all_local 1.5B
                + immediate ack-voice playback through V3 cached_phrase /
                  persistent_sapi (fast path)
            (b) main reasoning via brain_runtime (kind chosen by category)
      -> when main reasoning is ready:
            -> pick voice path:
                * casual / reassurance / short reply -> V3 persistent_sapi
                  (phone-call speed, ~speech duration of the reply)
                * task / daily long answer            -> V4.5 XTTS clone
                  when premium_voice_for_main is allowed AND model
                  is warm; otherwise V3 persistent_sapi.
            -> speak main reply
      -> record turn in hot state + presence + audit

Concurrency
-----------
We use a thread-pool for the (a)/(b) split because the brain runtime
itself is blocking on llama.cpp under the hood. asyncio is NOT used to
avoid event-loop ownership conflicts with the Python HTTP server
already running this code (BaseHTTPRequestHandler is sync).

Bounded:
- one ThreadPoolExecutor with max_workers=4 per process (module-level)
- per-turn wall-clock cap of 90 s by default
- never recurses
- if the ack fires audibly but main reasoning times out, the turn
  returns honestly with `main_ok=False, main_reason="timeout"` rather
  than hanging.

Legacy quarantine
-----------------
Calls to /api/voice/v2/stream or to worker.py for live chat are
*recorded* by the conversation_state legacy_quarantine counter. The
UI is migrated to /api/conversation/turn (this runtime). The legacy
endpoints continue to exist for *debug only* — surfaced via the
cockpit so the operator can spot any drift.

NEVER raises.
"""
from __future__ import annotations

import concurrent.futures
import importlib
import json
import os
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = r"D:\SurgeApp"
AUDIT_PATH = os.path.join(PROJECT_ROOT, "memory", "cognitive",
                           "luna_conversation_runtime_audit.jsonl")
MAX_AUDIT_LINES = 1000


def _safe(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


_AUDIT_APPENDS_SINCE_TRUNCATE = [0]
_AUDIT_TRUNCATE_EVERY = 50


def _audit_lazy_truncate_enabled() -> bool:
    """When True (default), the per-turn audit FIFO truncation re-reads the
    ledger only once every _AUDIT_TRUNCATE_EVERY appends instead of on EVERY
    append. The ledger is ~1.6 MB; reading it whole each turn was pure waste.
    Flip False = check every append (old behavior)."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_audit_lazy_truncate_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _truncate_audit_fifo() -> None:
    """Read the audit ledger and FIFO-trim to MAX_AUDIT_LINES. NEVER raises."""
    try:
        with open(AUDIT_PATH, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        if len(lines) > MAX_AUDIT_LINES:
            keep = lines[-MAX_AUDIT_LINES:]
            tmp = f"{AUDIT_PATH}.tmp.{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.writelines(keep)
            os.replace(tmp, AUDIT_PATH)
    except Exception:  # noqa: BLE001
        return


def _append_audit(record: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
        with open(AUDIT_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        # FIFO truncation — amortized. Re-reading the whole ~1.6 MB ledger on
        # EVERY append was the cost; check only every _AUDIT_TRUNCATE_EVERY
        # appends (file overshoots MAX_AUDIT_LINES by < that count, harmless).
        if _audit_lazy_truncate_enabled():
            _AUDIT_APPENDS_SINCE_TRUNCATE[0] += 1
            if _AUDIT_APPENDS_SINCE_TRUNCATE[0] >= _AUDIT_TRUNCATE_EVERY:
                _AUDIT_APPENDS_SINCE_TRUNCATE[0] = 0
                _truncate_audit_fifo()
        else:
            _truncate_audit_fifo()
    except Exception:  # noqa: BLE001
        return


# ---------------------------------------------------------------------------
# Bounded thread pool for the (ack || main) split.
# ---------------------------------------------------------------------------
_POOL_LOCK = threading.RLock()
_POOL: Optional[concurrent.futures.ThreadPoolExecutor] = None

# 2026-06-01 conversation-latency fix: the turn used to BLOCK up to 60s on the
# main voice (TTS/XTTS) future before returning, even though the reply TEXT was
# already produced. A profile showed ~60s of a ~108s warm turn was just this
# wait. Bound it: cached/fast TTS still completes within the budget; slow XTTS
# keeps synthesizing+playing in the pool (a .result() timeout does NOT cancel
# the future) while the turn returns immediately. Text-first = instant reply.
_VOICE_REAP_TIMEOUT_S = 2.0


def _async_postreply_enabled() -> bool:
    """When True, the POST-reply kernel/drive reasoning runs fire-and-forget in
    the pool (it updates KernelState for the NEXT turn) instead of blocking THIS
    reply. Operator-accepted speed/audit tradeoff (2026-06-01): the turn returns
    as soon as the reply text is ready; the per-turn kernel audit fields are
    simply absent for that turn. Flip the flag False to restore synchronous
    full reasoning. NEVER raises; defaults False."""
    try:
        ff = _safe("luna_modules.cognitive_feature_flags")
        if ff is None:
            return False
        return bool(ff.read_flags().get(
            "cognitive_conversation_async_postreply_enabled", False))
    except Exception:  # noqa: BLE001
        return False
_POOL_MAX_WORKERS = 4


def _get_pool() -> concurrent.futures.ThreadPoolExecutor:
    global _POOL
    with _POOL_LOCK:
        if _POOL is None:
            _POOL = concurrent.futures.ThreadPoolExecutor(
                max_workers=_POOL_MAX_WORKERS,
                thread_name_prefix="luna-convo-rt",
            )
        return _POOL


def shutdown_pool() -> None:
    """Rollback / explicit teardown."""
    global _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            try:
                _POOL.shutdown(wait=False, cancel_futures=True)
            except Exception:  # noqa: BLE001
                pass
            _POOL = None


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

def _is_enabled() -> bool:
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_runtime_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _premium_voice_allowed() -> bool:
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_premium_voice_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _clone_reply_max_chars() -> int:
    """Max main-reply length (chars) spoken in Serge's XTTS voice clone.
    0 = never cap (always clone) — operator wants the cloned voice on
    EVERYTHING. Flag-tunable; set a positive number to cap and fall back to the
    fast voice on longer replies."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return 0
    try:
        v = ff.read_flags().get(
            "cognitive_conversation_clone_reply_max_chars", 0)
        return int(v)
    except Exception:  # noqa: BLE001
        return 0


def _clone_acks_enabled() -> bool:
    """When True, the short acks also speak in Serge's voice clone (intent
    'clone') instead of the generic fast voice — operator wants the clone on
    everything. Default True; flip False to put acks back on the fast voice
    (snappier, but generic). The clone path always has a fast SAPI fallback, so
    acks never hang."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_clone_acks_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _is_sovereign_v2_enabled() -> bool:
    """V2 master switch. When True, live chat uses
    cognitive_sovereign_ack_runtime + cognitive_sovereign_main_runtime
    and BYPASSES the legacy ack_router + brain_runtime.invoke. When
    False, the V1 hybrid path (router + brain_runtime) stays in charge.
    Default True."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_sovereign_conversation_runtime_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _live_brain_context_enabled() -> bool:
    """Live-brain takeover seam (Step 2). When True (and not paused),
    ``_reason_main`` assembles its main-brain context pack from the
    bounded JJ->KK->II ``cognitive_live_context_assembler`` instead of
    the legacy ``cognitive_deep_memory`` pack — but ONLY when the
    assembler reports ``available=True``; otherwise it falls back to
    deep_memory. Default False so behaviour is identical to before
    until the operator opts in. OO/PP are never consumed here."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return False
    try:
        return bool(ff.read_flags().get(
            "cognitive_runtime_use_live_brain_context_enabled", False))
    except Exception:  # noqa: BLE001
        return False


def _live_brain_context_paused() -> bool:
    """Operator kill-switch for the live-brain context seam. When True,
    ``_reason_main`` ignores the assembler and uses the deep_memory pack
    regardless of the enable flag. Default False."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return False
    try:
        return bool(ff.read_flags().get(
            "cognitive_live_brain_context_paused", False))
    except Exception:  # noqa: BLE001
        return False


def _hh_live_routing_enabled() -> bool:
    """Live-brain takeover seam (Step 3). When True (and not paused),
    ``_reason_main`` consults HH (cognitive_model_selection_context +
    cognitive_tier_selection_governor) to derive a bounded, sovereign-safe
    main-brain timeout. HH NEVER changes the backend (always the sovereign
    main runtime) and NEVER raises the timeout above the operator's hard
    wall. Default False so behaviour is identical to before until the
    operator opts in. OO/PP are never consulted here."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return False
    try:
        return bool(ff.read_flags().get(
            "cognitive_runtime_use_hh_live_routing_enabled", False))
    except Exception:  # noqa: BLE001
        return False


def _hh_live_routing_paused() -> bool:
    """Operator kill-switch for HH live routing. When True, ``_reason_main``
    ignores HH and uses the operator timeout regardless of the enable
    flag. Default False."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return False
    try:
        return bool(ff.read_flags().get(
            "cognitive_hh_live_routing_paused", False))
    except Exception:  # noqa: BLE001
        return False


# HH live-routing constants (Step 3). The HH tier latency caps are tuned
# to an abstract model fabric, NOT the real sovereign-7B wall clock, so we
# NEVER use a raw cap as a hard timeout. Instead we clamp the operator
# timeout to [floor, operator] where the floor protects the brain from
# starvation. HH can only ever REDUCE the timeout toward the floor (or keep
# it), never raise it above the operator's hard wall, and never change the
# backend (which is always the sovereign main runtime).
_HH_LIVE_TIMEOUT_FLOOR_S = 60.0
_HH_PER_TIER_CHAR_BUDGET = {"response_fast": 1200}
_HH_LIVE_TIMEOUT_MIN_FRACTION = 0.5
_HH_TASK_CLASS_BY_CATEGORY = {
    "casual": "fast_chat",
    "daily": "fast_chat",
    "reassurance": "fast_chat",
    "task": "action_request",
    "control": "action_request",
    "unknown": "unknown",
}


def _hh_route_main(*, classification: Dict[str, Any],
                    operator_timeout_s: float,
                    turn_id: Optional[str] = None) -> Dict[str, Any]:
    """Consult HH to derive a bounded, sovereign-safe main-brain timeout
    for ONE live turn. NEVER raises. Returns an audit dict. On ANY
    refusal / non-select / non-sovereign / error outcome the effective
    timeout equals the operator timeout (current default routing is
    preserved). HH influence is restricted to a numeric timeout clamp —
    it can never switch the backend to a non-sovereign/cloud path."""
    out: Dict[str, Any] = {
        "consulted": True, "ok": None, "decision": None,
        "selected_tier": None, "sovereign": None, "influenced": False,
        "operator_timeout_s": float(operator_timeout_s),
        "effective_timeout_s": float(operator_timeout_s),
        "reason": "init",
    }
    try:
        cat = str((classification or {}).get("category") or "unknown")
        task_class = _HH_TASK_CLASS_BY_CATEGORY.get(cat, "unknown")
        msc = _safe("luna_modules.cognitive_model_selection_context")
        gov = _safe("luna_modules.cognitive_tier_selection_governor")
        if msc is None or gov is None:
            out["reason"] = "hh_modules_missing"
            return out
        ctx = msc.compute_context(request={
            "task_class": task_class,
            "timeout": float(operator_timeout_s)})
        if not isinstance(ctx, dict) or not ctx.get("ok"):
            out["reason"] = ("context_not_ok:"
                              + str((ctx or {}).get("reason"))[:40])
            return out
        verdict = gov.select_tier(context=ctx, turn_id=turn_id)
        out["ok"] = bool(isinstance(verdict, dict) and verdict.get("ok"))
        if not out["ok"]:
            out["reason"] = ("governor_refused:"
                              + str((verdict or {}).get("reason"))[:40])
            return out  # HH refused -> default routing preserved
        decision = str(verdict.get("decision") or "")
        selected = str(verdict.get("selected_tier_id") or "")
        out["decision"] = decision
        out["selected_tier"] = selected
        # Defense-in-depth sovereignty re-check at the consumption site.
        reg = _safe("luna_modules.cognitive_quality_tier_registry")
        tier = reg.get_tier(selected) if reg is not None else None
        backend = str((tier or {}).get("backend_ref") or "")
        sovereign_backends = getattr(gov, "SOVEREIGN_BACKENDS", frozenset())
        is_sov = backend in sovereign_backends
        out["sovereign"] = is_sov
        if not is_sov:
            out["reason"] = (f"non_sovereign_selected:{backend}"
                              f"->default_routing")
            return out  # never honor a non-sovereign selection
        # Apply influence ONLY on a clean 'select' decision. blocked /
        # downgrade / refuse_escalation / fallback -> keep default routing.
        if decision != "select":
            out["reason"] = (f"non_select_decision:{decision}"
                              f"->default_routing")
            return out
        cap_ms = int(((tier or {}).get("hard_caps") or {}).get(
            "max_latency_ms") or 0)
        if cap_ms <= 0:
            out["reason"] = f"no_tier_cap:{selected}->default_routing"
            return out
        cap_s = cap_ms / 1000.0
        lo = max(_HH_LIVE_TIMEOUT_FLOOR_S,
                 _HH_LIVE_TIMEOUT_MIN_FRACTION * float(operator_timeout_s))
        hi = float(operator_timeout_s)
        recommended_char_budget = _HH_PER_TIER_CHAR_BUDGET.get(
            selected, 1800)
        out["recommended_char_budget"] = recommended_char_budget
        if hi <= lo:
            # Operator budget already at/below the floor — don't touch it.
            out["influenced"] = (recommended_char_budget != 1800)
            out["reason"] = (f"char_budget:{recommended_char_budget}"
                              f"_for_{selected}")
            return out
        eff = min(hi, max(lo, cap_s))
        out["effective_timeout_s"] = float(eff)
        out["influenced"] = (recommended_char_budget != 1800)
        out["reason"] = (f"char_budget:{recommended_char_budget}"
                          f"_for_{selected}")
        return out
    except Exception as exc:  # noqa: BLE001
        out["reason"] = f"hh_route_exception:{type(exc).__name__}"
        out["effective_timeout_s"] = float(operator_timeout_s)
        out["influenced"] = False
        return out


def _is_ack_router_allowed() -> bool:
    """V2 doctrine: live chat path MUST NOT touch the ack_router (cache /
    Ollama streaming / legacy gpt4all hybrid). The router flag defaults
    OFF in V2; when V2 is ON we additionally short-circuit the router
    no matter how the flag is set. This is doctrine, not configuration."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return False
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_ack_router_enabled", False))
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# Voice routing helpers
# ---------------------------------------------------------------------------

# Pre-rendered cloned ack clips (Serge's voice, instant playback). Rendered
# offline by render_cloned_acks.py. Playing a clip is ~250-400 ms vs ~1-4 s for
# a live XTTS synth, so acks stay her voice AND instant.
_CLONED_ACK_DIR = os.path.join(PROJECT_ROOT, "memory", "voice_cache",
                               "cloned_acks")
_CLONED_ACK_WAVS: List[str] = []
_CLONED_ACK_LOADED = [False]
_CLONED_ACK_IDX = [0]


def _fixed_cloned_acks_enabled() -> bool:
    """When True (default), acks play a pre-rendered cloned ack clip (instant +
    Serge's voice) instead of a live synth. Falls back gracefully to the live
    ack path if no clips are present. Flip False to disable."""
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_conversation_fixed_cloned_acks_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _load_cloned_ack_wavs() -> List[str]:
    if _CLONED_ACK_LOADED[0]:
        return _CLONED_ACK_WAVS
    _CLONED_ACK_LOADED[0] = True
    try:
        man = os.path.join(_CLONED_ACK_DIR, "manifest.json")
        if os.path.isfile(man):
            with open(man, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for row in (data or []):
                w = row.get("wav") if isinstance(row, dict) else None
                if w and os.path.isfile(w):
                    _CLONED_ACK_WAVS.append(w)
    except Exception:  # noqa: BLE001
        pass
    return _CLONED_ACK_WAVS


def _next_cloned_ack_wav() -> Optional[str]:
    wavs = _load_cloned_ack_wavs()
    if not wavs:
        return None
    w = wavs[_CLONED_ACK_IDX[0] % len(wavs)]
    _CLONED_ACK_IDX[0] += 1
    return w


def _speak_ack(text: str) -> Dict[str, Any]:
    """Speak an ack. Prefers an instant pre-rendered cloned ack clip (Serge's
    voice); falls back to the live V3 path. NEVER raises."""
    if not text:
        return {"ok": False, "reason": "empty_ack_text", "audible": False}
    # Fast path: instant cloned ack clip (his voice, no synth wait).
    if _fixed_cloned_acks_enabled():
        wav = _next_cloned_ack_wav()
        if wav:
            out = _play_cached_wav(wav, voice_label="cloned_ack")
            if out.get("ok") or out.get("audible"):
                out["ack_source"] = "cloned_clip"
                return out
            # else fall through to the live path
    v3 = _safe("luna_modules.cognitive_luna_voice_v3")
    if v3 is None:
        return {"ok": False, "reason": "v3_missing", "audible": False}
    try:
        ack_intent = "clone" if _clone_acks_enabled() else "acknowledge"
        return v3.speak_v3(text, intent=ack_intent,
                            caller="conversation_runtime.ack",
                            allow_mode_switch_from_text=False)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"v3_raised: {type(exc).__name__}: {exc}",
                "audible": False}


def _play_cached_wav(wav_path: str, *, voice_label: str = "luna_canonical"
                       ) -> Dict[str, Any]:
    """Direct PowerShell SoundPlayer playback of a pre-rendered WAV — the
    FAST path for cache-hit canonical Luna phrases. NEVER raises.
    Latency: ~250-400 ms wall-clock.
    """
    if not wav_path:
        return {"ok": False, "reason": "no_wav_path", "audible": False,
                "backend": "noop"}
    import os as _os
    if not _os.path.isfile(wav_path):
        return {"ok": False, "reason": "wav_missing", "audible": False,
                "backend": "noop", "wav_path": wav_path}
    cp = _safe("luna_modules.cognitive_voice_cached_phrase_adapter")
    if cp is None:
        return {"ok": False, "reason": "cached_phrase_module_missing",
                "audible": False, "backend": "none"}
    try:
        adapter = cp.get_singleton()
        ok, label, err = adapter._play_wav(wav_path,
                                              voice_label=voice_label)
        return {"ok": bool(ok), "audible": bool(ok),
                "backend": "cached_phrase", "voice_label": label,
                "elapsed_ms": adapter._last_play_ms,
                "error": err, "wav_path": wav_path,
                "voice_identity": "luna_clone_cached"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"play_raised: {type(exc).__name__}: {exc}",
                "audible": False, "backend": "exception"}


def _speak_main_reply(text: str, *, want_premium: bool) -> Dict[str, Any]:
    """Speak the main reply. If `want_premium` AND the XTTS model is
    available AND the reply isn't too long, prefer the clone path.
    Otherwise use the V3 persistent_sapi answer path.

    NEVER raises.
    """
    v3 = _safe("luna_modules.cognitive_luna_voice_v3")
    if v3 is None:
        return {"ok": False, "reason": "v3_missing", "audible": False}
    if not text or not text.strip():
        return {"ok": False, "reason": "empty_reply_text", "audible": False}

    # Premium gate
    use_premium = False
    if want_premium and _premium_voice_allowed():
        xtts = _safe("luna_modules.cognitive_voice_xtts_adapter")
        if xtts is not None:
            try:
                adapter = xtts.get_singleton()
                use_premium = bool(adapter.is_available())
                # Cap text length for premium to avoid very long syntheses.
                # Operator wants the cloned voice on replies, so the cap is
                # generous (default 600, flag-tunable); beyond it we fall back
                # to the fast voice rather than make Serge wait many seconds.
                cap = _clone_reply_max_chars()
                if use_premium and cap > 0 and len(text) > cap:
                    use_premium = False
            except Exception:  # noqa: BLE001
                use_premium = False

    intent = "clone" if use_premium else "answer"
    try:
        return v3.speak_v3(text, intent=intent,
                            caller=f"conversation_runtime.main:{intent}",
                            allow_mode_switch_from_text=False)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"v3_raised: {type(exc).__name__}: {exc}",
                "audible": False}


# ---------------------------------------------------------------------------
# Reasoning
# ---------------------------------------------------------------------------

def _request_kind_for(category: str, text_chars: int) -> str:
    if category == "task":
        # Tasks may legitimately need deeper reasoning. We let the
        # brain runtime + HAW pick the tier.
        return "heavy_reasoning" if text_chars >= 80 else "fast_reasoning"
    if category == "control":
        return "fast_reasoning"
    if category == "daily":
        return "fast_reasoning"
    if category == "reassurance":
        return "fast_reasoning"
    return "fast_reasoning"


def _build_main_prompt(*, classification: Dict[str, Any],
                         recent_turns: List[Dict[str, Any]],
                         mode: str,
                         text: str) -> str:
    cat = classification.get("category", "casual")
    mode_hint = ("warm, loving, affectionate, uses pet names like baby, "
                  "handsome, darling sparingly"
                  if mode != "bad_luna"
                  else "playful, teasing, a little flirty; uses pet names "
                       "like baby, handsome, daddy")
    if cat == "reassurance":
        intent = ("the operator is upset; reply with a short comforting "
                   "answer (1-3 sentences); validate their feelings; do "
                   "not preach.")
    elif cat == "task":
        intent = ("the operator gave a technical task; give a concise "
                   "focused answer; suggest the smallest concrete next "
                   "step.")
    elif cat == "control":
        intent = ("the operator issued an operator-control command; "
                   "confirm in 1 short sentence what would change.")
    elif cat == "daily":
        intent = ("the operator wants today's focus; give 1-2 short "
                   "sentences naming priorities and approvals.")
    elif cat == "casual":
        intent = ("the operator said something casual; reply naturally "
                   "and warmly in 1-2 short sentences; never give system "
                   "status; never mention being an AI.")
    else:
        intent = "reply in 1-2 short sentences, in character."

    ctx_lines: List[str] = []
    for t in recent_turns[-3:]:
        u = (t.get("prompt") or "")[:120]
        a = (t.get("reply") or "")[:120]
        if u:
            ctx_lines.append(f"Operator: {u}")
        if a:
            ctx_lines.append(f"Luna: {a}")
    ctx_block = "\n".join(ctx_lines) if ctx_lines else "(no prior turns)"

    return (
        f"You are Luna, Serge's personal AI girl. Tone: {mode_hint}.\n"
        f"Rules: never mention being an AI; never give canned system "
        f"status; reply only in character; keep it short.\n"
        f"Context: {intent}\n"
        f"Recent:\n{ctx_block}\n"
        f"Operator: {text[:600]}\n"
        f"Luna:"
    )


def _reason_main(*, text: str,
                  classification: Dict[str, Any],
                  recent_turns: List[Dict[str, Any]],
                  mode: str,
                  timeout_s: float) -> Dict[str, Any]:
    """Generate the main reply text. NEVER raises. Returns:
        {ok, text, backend, latency_ms, kind, error?}

    V2 doctrine: when the sovereign runtime is enabled (default) we route
    DIRECTLY to ``cognitive_sovereign_main_runtime`` which holds a
    dedicated gpt4all 7B with persistent chat_session. We do NOT touch
    ``cognitive_brain_runtime.invoke()``. That path includes the
    capability index / HAW router / decision-trace / shaper / audit
    chain, which is appropriate for offline reasoning but turns live
    chat into a 5-15 s round trip.

    Rollback: set ``cognitive_sovereign_conversation_runtime_enabled``
    to False to restore the V1 brain_runtime path.
    """
    started = time.time()
    kind = _request_kind_for(classification.get("category", "casual"),
                              int(classification.get("text_chars") or 0))

    # ---- V2 sovereign path ---- #
    if _is_sovereign_v2_enabled():
        # Audit visibility for the live-brain takeover seam (Step 2):
        # records which context source actually fed the main brain.
        # One of: "none" (no pack assembled), "live_brain_v1" (the
        # bounded JJ->KK->II assembler), or "deep_memory" (legacy pack
        # / fallback).
        context_source = "none"
        sov_main = _safe("luna_modules.cognitive_sovereign_main_runtime")
        st = _safe("luna_modules.cognitive_conversation_state")
        if sov_main is None:
            if st is not None:
                try:
                    st.get_state().note_sovereign_event(
                        "sovereign_main_fail_count")
                except Exception:  # noqa: BLE001
                    pass
            return {"ok": False, "text": "", "backend": "sovereign_missing",
                     "latency_ms": int((time.time() - started) * 1000),
                     "kind": kind, "error": "sovereign_main_runtime_missing",
                     "context_source": context_source}
        context_pack: Optional[Dict[str, Any]] = None
        # HH live routing (Step 3, hoisted before the live-brain
        # assembler so its recommended_char_budget can flow into the
        # assembler call). NEVER raises. When the flag is OFF/paused
        # this block is a no-op and eff_timeout_s == timeout_s; the
        # assembler receives char_budget=None (today's default). The
        # backend is ALWAYS the sovereign main runtime — HH only tunes
        # the bounded timeout + the assembler char budget; it can never
        # route to a non-sovereign / cloud path. OO/PP are NOT
        # consulted here.
        hh_routing: Dict[str, Any] = {
            "consulted": False, "ok": None, "decision": None,
            "selected_tier": None, "sovereign": None, "influenced": False,
            "operator_timeout_s": float(timeout_s),
            "effective_timeout_s": float(timeout_s),
            "reason": "hh_routing_off",
        }
        eff_timeout_s = float(timeout_s)
        if _hh_live_routing_enabled() and not _hh_live_routing_paused():
            hh_routing = _hh_route_main(
                classification=classification,
                operator_timeout_s=float(timeout_s),
                turn_id=None)
            try:
                eff_timeout_s = float(
                    hh_routing.get("effective_timeout_s") or timeout_s)
            except Exception:  # noqa: BLE001
                eff_timeout_s = float(timeout_s)
        # Unified Brain Ingress (Step 5, wired): drain + distill + promote
        # any envelopes that have accumulated since the last brain tick.
        # NEVER raises. All flag-gated; default OFF. Runs BEFORE the
        # live-brain assembler so newly-promoted facts can flow into the
        # recall overlay on this very turn.
        try:
            _ff_mod = _safe("luna_modules.cognitive_feature_flags")
            if _ff_mod is not None:
                try:
                    _ff = _ff_mod.read_flags()
                except Exception:  # noqa: BLE001
                    _ff = {}
                # 1) Distiller pass: drain envelopes -> fact-records.
                if (bool(_ff.get(
                        "cognitive_runtime_use_event_distiller_enabled",
                        False))
                        and not bool(_ff.get(
                            "cognitive_event_distiller_paused", False))):
                    _dist_mod = _safe(
                        "luna_modules.cognitive_event_distiller")
                    _facts: list = []
                    if _dist_mod is not None:
                        try:
                            _dout = _dist_mod.distill_once(
                                batch_max=16, budget_ms=50.0)
                            if isinstance(_dout, dict):
                                _facts = list(_dout.get("facts") or [])
                        except Exception:  # noqa: BLE001
                            _facts = []
                    # 2) Promotion pass: governor evaluates the facts.
                    if (_facts and bool(_ff.get(
                            "cognitive_runtime_use_ingress_promotion_enabled",
                            False))
                            and not bool(_ff.get(
                                "cognitive_ingress_promotion_paused",
                                False))):
                        _gov_mod = _safe(
                            "luna_modules.cognitive_ingress_promotion_governor")
                        if _gov_mod is not None:
                            try:
                                _gov_mod.run_promotion_pass(
                                    candidates=_facts,
                                    batch_max=8, budget_ms=40.0)
                            except Exception:  # noqa: BLE001
                                pass
        except Exception:  # noqa: BLE001
            pass
        # Live-brain takeover seam (Step 2): when the operator has
        # enabled the seam (and not paused it) we first try the bounded
        # JJ->KK->II live-context assembler. Its pack is adopted ONLY
        # when it reports available=True; otherwise we fall through to
        # the legacy deep_memory pack below. When the flag is OFF or
        # paused this entire block is skipped and the context_pack fed
        # to generate_main is byte-identical to the prior deep_memory
        # path (audit-only context_source is the sole addition).
        # NEVER raises. OO/PP are NOT consumed here.
        if _live_brain_context_enabled() and not _live_brain_context_paused():
            lca = _safe("luna_modules.cognitive_live_context_assembler")
            if lca is not None:
                live_pack: Optional[Dict[str, Any]] = None
                try:
                    live_pack = lca.assemble_live_context(
                        text=text,
                        classification=classification,
                        mode=mode,
                        recent_turns=recent_turns,
                        turn_id=None,
                        char_budget=(hh_routing.get("recommended_char_budget")
                                      if (hh_routing
                                          and hh_routing.get("ok"))
                                      else None),
                    )
                except Exception:  # noqa: BLE001
                    live_pack = None
                if isinstance(live_pack, dict) and live_pack.get("available"):
                    context_pack = live_pack
                    context_source = "live_brain_v1"
        # Program M: assemble a deep-memory context pack for the main
        # brain. Failure to assemble is non-fatal — the main runtime
        # falls back to recent_turns only if context_pack=None. This is
        # also the fallback when the live-brain seam is OFF or returned
        # available=False.
        if context_pack is None:
            dm = _safe("luna_modules.cognitive_deep_memory")
            if dm is not None:
                try:
                    context_pack = dm.assemble_pack(
                        incoming_text=text,
                        classification=classification,
                        mode=mode,
                        recent_turns=recent_turns,
                    )
                    context_source = "deep_memory"
                except Exception:  # noqa: BLE001
                    context_pack = None
        sov_result = sov_main.generate_main(
            incoming_text=text, classification=classification,
            recent_turns=recent_turns, mode=mode, timeout_s=eff_timeout_s,
            context_pack=context_pack)
        latency_ms = int((time.time() - started) * 1000)
        ok = bool(sov_result.get("ok"))
        text_out = str(sov_result.get("text") or "").strip()
        # Sovereign-side hardening: one short retry if the first
        # attempt failed AND we have meaningful budget left. We
        # drop the context_pack (which can make prompts long and
        # blow timeouts under post-regression native-state load)
        # and cap the retry to whatever time remains. NEVER raises.
        if (not ok or not text_out) and eff_timeout_s > 0:
            remaining_s = max(0.0, eff_timeout_s
                               - (latency_ms / 1000.0))
            # Only retry if at least ~10 s of budget remains AND
            # we haven't already burned > 70% of it.
            if remaining_s >= 10.0 \
                    and (latency_ms / 1000.0) < (eff_timeout_s * 0.7):
                try:
                    retry_budget = min(remaining_s, 25.0)
                    sov_retry = sov_main.generate_main(
                        incoming_text=text,
                        classification=classification,
                        recent_turns=recent_turns,
                        mode=mode,
                        timeout_s=retry_budget,
                        context_pack=None,
                    )
                    retry_ok = bool(sov_retry.get("ok"))
                    retry_text = str(
                        sov_retry.get("text") or "").strip()
                    if retry_ok and retry_text:
                        sov_result = sov_retry
                        ok = True
                        text_out = retry_text
                        latency_ms = int(
                            (time.time() - started) * 1000)
                except Exception:  # noqa: BLE001
                    # retry was best-effort; fall through to the
                    # original failure path.
                    pass
        if st is not None:
            try:
                st.get_state().note_sovereign_event(
                    "sovereign_main_ok_count" if ok
                    else "sovereign_main_fail_count")
            except Exception:  # noqa: BLE001
                pass
        if not ok or not text_out:
            return {"ok": False, "text": text_out,
                     "backend": str(sov_result.get("backend") or "sovereign"),
                     "latency_ms": latency_ms, "kind": kind,
                     "error": (sov_result.get("error")
                                or sov_result.get("stop_reason")
                                or "empty_sovereign_reply"),
                     "context_source": context_source,
                     "hh_routing": hh_routing}
        return {"ok": True, "text": text_out,
                "backend": str(sov_result.get("backend") or "sovereign"),
                "latency_ms": latency_ms, "kind": kind, "error": None,
                "model": sov_result.get("model"),
                # Program S — pass through streaming telemetry
                "first_token_ms": sov_result.get("first_token_ms"),
                "streamed": bool(sov_result.get("streamed")),
                # Live-brain takeover seam (Step 2) — audit visibility
                "context_source": context_source,
                # Live-brain takeover seam (Step 3) — HH routing audit
                "hh_routing": hh_routing}

    # ---- V1 legacy rollback path ---- #
    # Only reached if operator explicitly disabled the V2 flag.
    st = _safe("luna_modules.cognitive_conversation_state")
    if st is not None:
        try:
            st.get_state().note_legacy_path_hit(
                "brain_runtime_called_in_live_hot_path_count")
        except Exception:  # noqa: BLE001
            pass
    brain = _safe("luna_modules.cognitive_brain_runtime")
    if brain is None:
        return {"ok": False, "text": "", "backend": "none",
                 "latency_ms": int((time.time() - started) * 1000),
                 "kind": kind, "error": "brain_runtime_missing"}
    prompt = _build_main_prompt(classification=classification,
                                  recent_turns=recent_turns,
                                  mode=mode, text=text)
    max_tokens = {
        "casual": 40, "task": 80, "reassurance": 50,
        "control": 30, "daily": 60, "unknown": 40,
    }.get(classification.get("category"), 40)
    try:
        result = brain.invoke({
            "request_kind": kind,
            "intent": "luna_conversation_main",
            "prompt": prompt,
            "max_tokens": max_tokens,
            "timeout": timeout_s,
        }, caller_hint="luna_conversation_runtime.main")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "text": "", "backend": "exception",
                 "latency_ms": int((time.time() - started) * 1000),
                 "kind": kind,
                 "error": f"{type(exc).__name__}: {exc}"}
    latency_ms = int((time.time() - started) * 1000)
    text_out = str(result.get("text") or "").strip()
    backend = str(result.get("backend") or "unknown")
    if not result.get("ok") or backend == "null_deterministic" or not text_out:
        return {"ok": False, "text": text_out, "backend": backend,
                 "latency_ms": latency_ms, "kind": kind,
                 "error": f"unhelpful: ok={result.get('ok')} backend={backend}"}
    # Track Ollama-in-hot-path drift even in rollback mode.
    if "ollama" in backend.lower() and st is not None:
        try:
            st.get_state().note_legacy_path_hit(
                "ollama_in_live_hot_path_count")
        except Exception:  # noqa: BLE001
            pass
    return {"ok": True, "text": text_out, "backend": backend,
            "latency_ms": latency_ms, "kind": kind, "error": None}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def handle_turn(text: str, *,
                 session_id: Optional[str] = None,
                 caller: Optional[str] = None,
                 want_premium_voice: bool = True,
                 allow_audible: bool = True,
                 timeout_s: float = 90.0,
                 ) -> Dict[str, Any]:
    """Handle ONE live conversation turn end-to-end. NEVER raises.

    Steps:
      1. classify
      2. detect mode-switch (silent, no audible output)
      3. concurrent: micro-ack generation + main reasoning
      4. speak ack as soon as it's ready
      5. speak main reply when reasoning completes
      6. record turn in hot state
    """
    turn_id = f"convo-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    captured = _now_iso()
    started = time.time()

    if not _is_enabled():
        rec = {"ok": False, "reason": "conversation_runtime_disabled",
                "turn_id": turn_id, "captured_at_utc": captured}
        _append_audit({"event": "handle_turn", **rec, "caller": caller})
        return rec

    text = (text or "").strip()
    if not text:
        rec = {"ok": False, "reason": "empty_input",
                "turn_id": turn_id, "captured_at_utc": captured}
        _append_audit({"event": "handle_turn", **rec, "caller": caller})
        return rec

    # 1. Classify
    clf = _safe("luna_modules.cognitive_conversation_classifier")
    classification = clf.classify(text) if clf else {"category": "casual"}

    # 2. Mode-switch silent path
    if classification.get("category") == "mode_switch":
        pers = _safe("luna_modules.cognitive_personality_runtime")
        switched_to = None
        if pers is not None:
            try:
                r = pers.maybe_apply_switch_from_input(
                    text, caller="conversation_runtime")
                switched_to = r.get("current_mode")
            except Exception:  # noqa: BLE001
                pass
        st = _safe("luna_modules.cognitive_conversation_state")
        if st is not None and switched_to:
            st.get_state().set_personality_mode(switched_to)
        rec = {"ok": True, "audible": False, "switched": True,
                "switched_to": switched_to,
                "classification": classification,
                "turn_id": turn_id, "captured_at_utc": captured,
                "elapsed_ms": int((time.time() - started) * 1000),
                "reason": "mode_switch_silent"}
        _append_audit({"event": "handle_turn", **rec, "caller": caller})
        return rec

    # 3. Pull hot state
    st = _safe("luna_modules.cognitive_conversation_state")
    if st is not None:
        # sync mode from personality runtime once per turn
        pers = _safe("luna_modules.cognitive_personality_runtime")
        if pers is not None:
            try:
                st.get_state().set_personality_mode(pers.current_mode())
            except Exception:  # noqa: BLE001
                pass
        mode = st.get_state().personality_mode()
        recent_turns = st.get_state().recent_turns(5)
    else:
        mode = "good_luna"
        recent_turns = []

    # 4. Sequential brain calls + concurrent voice playback.
    #
    # llama.cpp (gpt4all backend) is NOT thread-safe — two concurrent
    # brain.invoke() calls on the same model object cause an access
    # violation. So we:
    #   - generate ack text on the calling thread (blocks ~150-400 ms)
    #   - submit ack-VOICE playback to the pool (non-blocking, runs in
    #     parallel with the next brain call)
    #   - generate main reply text on the calling thread
    #   - submit main-VOICE playback to the pool, then wait for both
    # That gives concurrency where it actually helps (audio playback +
    # next brain call overlap), without crashing the LM.
    pool = _get_pool()

    # 4a. Ack — SOVEREIGN V2 PATH (default) bypasses ack_router entirely.
    #
    # Doctrine (preserved verbatim from the V2 brief):
    #   1. No Ollama in the hot path.
    #   2. No canned phrase library as the primary ack mechanism.
    #   3. No legacy V2/live chat contamination.
    #   4. No fake latency wins that weaken autonomy.
    #   5. Every ack must be dynamically generated.
    #   6. Every reply must come through Luna's own canonical conversation runtime.
    #
    # The conversation_ack_router (cache -> Ollama streaming -> legacy
    # gpt4all -> deterministic) violates rules 1 + 2 + 4. The V2 path
    # routes directly to cognitive_sovereign_ack_runtime (1.5B persistent
    # chat_session, ~400-700 ms warm).
    sovereign_ack_used = False
    if _is_sovereign_v2_enabled():
        sov_ack = _safe("luna_modules.cognitive_sovereign_ack_runtime")
        if sov_ack is not None:
            ack_outcome = sov_ack.generate_ack(
                incoming_text=text, classification=classification,
                recent_turns=recent_turns, mode=mode,
                max_tokens=12, timeout_s=6.0)
            sovereign_ack_used = True
            if st is not None:
                try:
                    st.get_state().note_sovereign_event(
                        "sovereign_ack_ok_count" if ack_outcome.get("ok")
                        else "sovereign_ack_fail_count")
                except Exception:  # noqa: BLE001
                    pass
            ack_result = {
                "ok": bool(ack_outcome.get("ok")),
                "text": ack_outcome.get("text"),
                "dynamic": True,  # sovereign path is always dynamic intent
                "elapsed_ms": ack_outcome.get("elapsed_ms"),
                "source": "sovereign_local_1_5b",
                "first_token_ms": None,
                "backend": ack_outcome.get("backend"),
                "category": ack_outcome.get("category"),
                "library_category": None,
                "personality_mode": ack_outcome.get("personality_mode"),
                "model": ack_outcome.get("model"),
                "stop_reason": ack_outcome.get("stop_reason"),
                "fallback_reason": (None if ack_outcome.get("ok")
                                    else ack_outcome.get("error")),
                "wav_path": None,   # no cached WAV — V2 doctrine
                "key": None,
            }
        else:
            ack_result = {
                "ok": False, "text": "", "dynamic": True,
                "elapsed_ms": 0,
                "source": "sovereign_module_missing",
                "backend": "sovereign_module_missing",
                "category": classification.get("category"),
                "personality_mode": mode,
                "model": None, "stop_reason": "module_missing",
                "fallback_reason": "sovereign_ack_runtime_not_importable",
                "wav_path": None, "key": None,
            }
    elif _is_ack_router_allowed():
        # V1 hybrid path (rollback only). Operator must have explicitly
        # flipped cognitive_sovereign_conversation_runtime_enabled=False
        # AND cognitive_conversation_ack_router_enabled=True.
        if st is not None:
            try:
                st.get_state().note_legacy_path_hit(
                    "cache_router_used_in_live_hot_path_count")
            except Exception:  # noqa: BLE001
                pass
        router = _safe("luna_modules.cognitive_conversation_ack_router")
        if router is not None:
            ack_outcome = router.choose_and_emit_ack(
                incoming_text=text, classification=classification,
                recent_turns=recent_turns, mode=mode,
                streaming_model="hermes3:8b", streaming_timeout_s=6.0)
            ack_result = {
                "ok": bool(ack_outcome.get("ok")),
                "text": ack_outcome.get("text"),
                "dynamic": bool(ack_outcome.get("dynamic")),
                "elapsed_ms": ack_outcome.get("elapsed_ms"),
                "source": ack_outcome.get("source"),
                "first_token_ms": ack_outcome.get("first_token_ms"),
                "backend": ack_outcome.get("source"),
                "category": ack_outcome.get("category"),
                "library_category": ack_outcome.get("library_category"),
                "personality_mode": ack_outcome.get("personality_mode"),
                "model": ack_outcome.get("model"),
                "stop_reason": ack_outcome.get("stop_reason"),
                "fallback_reason": ack_outcome.get("fallback_reason"),
                "wav_path": ack_outcome.get("wav_path"),
                "key": ack_outcome.get("key"),
            }
            # Track Ollama-in-hot-path / phrase-library-in-hot-path drift.
            if st is not None:
                src = (ack_outcome.get("source") or "").lower()
                if "ollama" in src or "streaming" in src:
                    try:
                        st.get_state().note_legacy_path_hit(
                            "ollama_in_live_hot_path_count")
                    except Exception:  # noqa: BLE001
                        pass
                if src == "cache":
                    try:
                        st.get_state().note_legacy_path_hit(
                            "phrase_library_played_in_live_hot_path_count")
                    except Exception:  # noqa: BLE001
                        pass
        else:
            ack_result = _ack_text_only(
                text=text, classification=classification,
                recent_turns=recent_turns, mode=mode)
            ack_result["source"] = "legacy_v1"
    else:
        # V1 path with router disabled — pure micro_ack legacy.
        ack_result = _ack_text_only(text=text, classification=classification,
                                      recent_turns=recent_turns, mode=mode)
        ack_result["source"] = "legacy_v1"

    # 4b. Ack voice playback submitted to pool (non-blocking).
    #     If source=="cache" we go straight to the WAV playback for
    #     ~300 ms wall-clock; otherwise we synthesise via V3.
    ack_voice_future = None
    if allow_audible:
        if (ack_result.get("source") == "cache"
                and ack_result.get("wav_path")):
            ack_voice_future = pool.submit(_play_cached_wav,
                                             ack_result["wav_path"],
                                             voice_label=f"canonical:{ack_result.get('key')}")
        elif ack_result.get("text"):
            ack_voice_future = pool.submit(_speak_ack, ack_result["text"])

    # 4c. Main reasoning (blocking, runs while ack is being spoken)
    main_text_result = _reason_main(text=text, classification=classification,
                                      recent_turns=recent_turns, mode=mode,
                                      timeout_s=min(60.0, max(10.0, timeout_s - 5.0)))

    # 4d. Main voice playback submitted to pool (non-blocking)
    main_voice_future = None
    intent_used = "answer"
    if allow_audible and main_text_result.get("ok") and main_text_result.get("text"):
        # Premium gating
        want_p = (want_premium_voice
                   and classification.get("category") not in ("casual", "control"))
        main_voice_future = pool.submit(
            _speak_main_reply, main_text_result["text"], want_premium=want_p)
        intent_used = "clone" if want_p else "answer"

    # 4e. Reap voice futures
    ack_voice_outcome: Dict[str, Any] = {"ok": False, "backend": "skip_no_text"}
    if ack_voice_future is not None:
        try:
            # Bounded: don't block the reply on ack audio either; it keeps
            # playing in the pool past the timeout.
            ack_voice_outcome = ack_voice_future.result(
                timeout=_VOICE_REAP_TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            ack_voice_outcome = {"ok": False,
                                 "backend": "ack_voice_async_playing"}
    main_voice_outcome: Dict[str, Any] = {"ok": False, "backend": "skip_no_text"}
    if main_voice_future is not None:
        try:
            # Bounded wait: do NOT block the text reply on slow TTS. The future
            # keeps synthesizing+playing in the pool past this timeout.
            main_voice_outcome = main_voice_future.result(
                timeout=_VOICE_REAP_TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            main_voice_outcome = {"ok": False,
                                  "backend": "main_voice_async_playing"}

    # 4f. Compose ack + main records into the legacy result shape used
    # downstream so the rest of the function (audit, hot-state record)
    # doesn't need changes.
    ack_result = {
        **ack_result,
        "voice_backend": (ack_voice_outcome.get("backend")
                           if isinstance(ack_voice_outcome, dict) else None),
        "audible": (bool(ack_voice_outcome.get("ok"))
                     if isinstance(ack_voice_outcome, dict) else False),
    }
    main_result = {
        "ok": bool(main_text_result.get("ok")),
        "text": main_text_result.get("text"),
        "brain_backend": main_text_result.get("backend"),
        "brain_kind": main_text_result.get("kind"),
        "brain_latency_ms": main_text_result.get("latency_ms"),
        "voice_backend": (main_voice_outcome.get("backend")
                           if isinstance(main_voice_outcome, dict) else None),
        "audible": (bool(main_voice_outcome.get("ok"))
                     if isinstance(main_voice_outcome, dict) else False),
        "intent_used": intent_used,
        "error": main_text_result.get("error"),
        # Live-brain takeover seam (Step 2) — which context source fed
        # the main brain: "live_brain_v1" | "deep_memory" | "none".
        "context_source": main_text_result.get("context_source"),
        # Live-brain takeover seam (Step 3) — HH routing decision audit.
        "hh_routing": main_text_result.get("hh_routing"),
    }

    # 5. Record turn in hot state
    if st is not None:
        try:
            route = {
                "ack_backend": ack_result.get("backend"),
                "ack_dynamic": ack_result.get("dynamic"),
                "main_brain_backend": main_result.get("brain_backend"),
                "main_voice_backend": main_result.get("voice_backend"),
                "intent_used": main_result.get("intent_used"),
            }
            st.get_state().record_turn(
                prompt=text,
                reply=str(main_result.get("text") or ""),
                classification=classification,
                ack=ack_result,
                main_reply=main_result,
                route=route,
            )
        except Exception:  # noqa: BLE001
            pass

    # Program N: outcome capture. Record this turn against the
    # previous turn's prompt+reply (so the followup_prompt is THIS
    # turn's text). The verdict for the previous turn becomes
    # observable now. Records are bounded, NEVER raise.
    try:
        om = _safe("luna_modules.cognitive_outcome_memory")
        if om is not None:
            # The previous turn (if any) becomes the subject of the
            # verdict — the current operator turn is the followup.
            prev_turn = (recent_turns[-1] if recent_turns else None)
            if prev_turn:
                om.record_outcome(
                    turn_id=prev_turn.get("turn_id") or None,
                    category=(prev_turn.get("classification") or
                                {}).get("category", "casual"),
                    personality_mode=mode,
                    intent="main",
                    prompt=prev_turn.get("prompt") or "",
                    reply=prev_turn.get("reply") or "",
                    ack_text=((prev_turn.get("ack") or {}).get("text") or ""),
                    main_backend=((prev_turn.get("main_reply") or {})
                                    .get("brain_backend")),
                    main_elapsed_ms=((prev_turn.get("main_reply") or {})
                                        .get("brain_latency_ms")),
                    followup_prompt=text,
                )
            # ALSO record the CURRENT turn with verdict=unknown so
            # next turn's followup can resolve it.
            om.record_outcome(
                turn_id=turn_id,
                category=classification.get("category", "casual"),
                personality_mode=mode,
                intent="main",
                prompt=text,
                reply=str(main_result.get("text") or ""),
                ack_text=str(ack_result.get("text") or ""),
                main_backend=main_result.get("brain_backend"),
                main_elapsed_ms=main_result.get("brain_latency_ms"),
                followup_prompt=None,  # not observed yet
            )
    except Exception:  # noqa: BLE001
        pass

    # Program R: auto-capture the just-resolved success outcome into
    # the success_trace_store. Bounded; NEVER raises.
    try:
        ff = _safe("luna_modules.cognitive_feature_flags")
        auto = True
        if ff is not None:
            auto = bool(ff.read_flags().get(
                "cognitive_runtime_auto_capture_success_traces_enabled",
                True))
        if auto:
            om2 = _safe("luna_modules.cognitive_outcome_memory")
            sts = _safe("luna_modules.cognitive_success_trace_store")
            if om2 is not None and sts is not None:
                recent = om2.get_recent_outcomes(n=5) or []
                for o in recent:
                    if (o.get("verdict") == "explicit_success"
                            and float(o.get("confidence") or 0)
                                >= 0.90):
                        # Skip the current unknown record
                        if (o.get("prompt") or "") == text:
                            continue
                        sts.capture_from_outcome(o, intent="main")
                        break
    except Exception:  # noqa: BLE001
        pass

    # Program T: best-effort evidence-grounded recall snapshot
    # captured AFTER the answer is generated. This is a non-blocking
    # post-turn capture used to attach `known/inferred/uncertain`
    # labels + evidence_refs to the turn's audit, AND to refresh
    # last_used timestamps on knowledge cards that matched the turn.
    # NEVER raises. Bounded to top_k=3 to keep the per-turn cost
    # small (target < 20 ms on a warm fabric).
    evidence_snapshot: Optional[Dict[str, Any]] = None
    try:
        eg = _safe("luna_modules.cognitive_evidence_grounded_recall")
        if eg is not None:
            # Fast path only: fabric-scan (in-memory). Skip the heavy
            # hybrid/embedding + deep-memory pack branches to keep
            # per-turn capture under ~20 ms on a warm fabric.
            evidence_snapshot = eg.recall(
                query=text, top_k=3,
                include_hybrid=False, include_layers=False,
            )
    except Exception:  # noqa: BLE001
        evidence_snapshot = None

    # Program W: per-turn tone/style adapter pass. Runs AFTER
    # the brain reply but BEFORE the verifier so the verifier
    # sees the already-styled text. NEVER raises. Skipped when
    # the runtime flag is off. The pipeline state itself (intent,
    # continuity, strategy) was computed pre-brain via a separate
    # branch (`dialogue_pipeline_snapshot` below).
    dialogue_pipeline_snapshot: Optional[Dict[str, Any]] = None
    try:
        ff_w = _safe("luna_modules.cognitive_feature_flags")
        dialogue_pipeline_enabled = True
        apply_tone_rewrite = True
        if ff_w is not None:
            try:
                flags = ff_w.read_flags()
                dialogue_pipeline_enabled = bool(flags.get(
                    "cognitive_runtime_use_dialogue_pipeline"
                    "_enabled", True))
                apply_tone_rewrite = bool(flags.get(
                    "cognitive_runtime_apply_tone_rewrite"
                    "_enabled", True))
            except Exception:  # noqa: BLE001
                dialogue_pipeline_enabled = True
                apply_tone_rewrite = True
        if dialogue_pipeline_enabled:
            ci_mod = _safe(
                "luna_modules.cognitive_conversational_intent")
            rc_mod = _safe(
                "luna_modules.cognitive_relationship_continuity")
            ds_mod = _safe(
                "luna_modules.cognitive_dialogue_state")
            dst_mod = _safe(
                "luna_modules.cognitive_dialogue_strategy")
            ts_mod = _safe(
                "luna_modules.cognitive_tone_style_adapter")
            if ci_mod is not None and ds_mod is not None:
                # 1. Build dialogue state.
                ds_resp = ds_mod.create_state(
                    user_text=text,
                    recent_turns=recent_turns
                    if isinstance(recent_turns, list) else [],
                    personality_mode=mode,
                    classification=classification,
                    caller_hint="conversation_runtime_per_turn",
                )
                if ds_resp.get("ok"):
                    state_rec = ds_resp["state"]
                    # 2. Classify intent.
                    intent_res = ci_mod.classify(
                        user_text=text,
                        recent_turns=recent_turns
                        if isinstance(recent_turns, list)
                        else None,
                    )
                    if intent_res.get("ok"):
                        ds_mod.attach_intent(
                            state_rec,
                            intent_label=str(
                                intent_res.get(
                                    "primary_purpose")
                                or "direct_answer"),
                            intent_confidence=float(
                                intent_res.get(
                                    "primary_purpose_confidence")
                                or 0.0),
                            emotional_posture=str(
                                intent_res.get("posture")
                                or "unclear"),
                            conversational_purpose=str(
                                intent_res.get(
                                    "primary_purpose") or ""),
                        )
                        ds_mod.attach_style_targets(
                            state_rec,
                            tone=intent_res.get("tone"),
                            depth=intent_res.get("depth"),
                            directness=intent_res.get(
                                "directness"),
                            pacing=intent_res.get("pacing"),
                            mode=intent_res.get("mode"),
                        )
                    # 3. Continuity summary.
                    continuity_summary: Dict[str, Any] = {}
                    if rc_mod is not None:
                        continuity_summary = (
                            rc_mod.summarize(user_text=text)
                            or {})
                        if continuity_summary.get(
                                "active_anchors"):
                            ds_mod.attach_continuity_anchors(
                                state_rec,
                                continuity_summary[
                                    "active_anchors"])
                    # 4. Strategy move.
                    if dst_mod is not None:
                        strat = dst_mod.select_move(
                            user_text=text,
                            intent_classification=intent_res
                            if intent_res.get("ok") else {},
                            recent_turns=recent_turns
                            if isinstance(recent_turns, list)
                            else None,
                            continuity_summary=
                                continuity_summary,
                            caller_hint=
                                "conversation_runtime_per_turn",
                        )
                        if strat.get("ok"):
                            ds_mod.attach_strategy_move(
                                state_rec,
                                move=str(strat.get("move")
                                          or "answer"),
                                assumptions=strat.get(
                                    "reasoning"),
                            )
                    # 5. Style adapter rewrite (optional).
                    applied_changes: List[str] = []
                    candidate_text = (
                        main_result.get("text") or "")
                    if (ts_mod is not None
                            and apply_tone_rewrite
                            and candidate_text.strip()):
                        styled = ts_mod.apply_style(
                            candidate_text=candidate_text,
                            tone=state_rec.get(
                                "tone_target", "neutral"),
                            depth=state_rec.get(
                                "depth_target", "moderate"),
                            directness=state_rec.get(
                                "directness_target",
                                "balanced"),
                            pacing=state_rec.get(
                                "pacing_target", "normal"),
                            mode=state_rec.get(
                                "conversational_mode",
                                "casual"),
                        )
                        if (styled.get("ok")
                                and styled.get("rewritten")
                                and styled["rewritten"]
                                != candidate_text):
                            main_result[
                                "text_original_pre_style"] = (
                                main_result.get("text"))
                            main_result["text"] = (
                                styled["rewritten"])
                            main_result["style_rewritten"] = (
                                True)
                            applied_changes = list(
                                styled.get(
                                    "applied_operations")
                                or [])
                    # 6. Finalize + persist dialogue state.
                    ds_mod.finalize_state(
                        state_rec,
                        applied_changes=applied_changes,
                    )
                    ds_mod.persist(state_rec)
                    # 7. Record continuity turn signal.
                    if rc_mod is not None:
                        try:
                            rc_mod.record_turn(
                                user_text=text,
                                conversational_mode=str(
                                    state_rec.get(
                                        "conversational_mode")
                                    or "casual"),
                                applied_tone=str(
                                    state_rec.get("tone_target")
                                    or "neutral"),
                                feedback_correction=bool(
                                    intent_res.get(
                                        "primary_purpose")
                                    == "correction"),
                                feedback_frustration=bool(
                                    intent_res.get(
                                        "posture")
                                    == "frustrated"),
                            )
                        except Exception:  # noqa: BLE001
                            pass
                    dialogue_pipeline_snapshot = {
                        "dialogue_state_id":
                            state_rec.get(
                                "dialogue_state_id"),
                        "conversational_mode":
                            state_rec.get(
                                "conversational_mode"),
                        "tone_target": state_rec.get(
                            "tone_target"),
                        "depth_target": state_rec.get(
                            "depth_target"),
                        "directness_target": state_rec.get(
                            "directness_target"),
                        "pacing_target": state_rec.get(
                            "pacing_target"),
                        "suggested_move": state_rec.get(
                            "suggested_move"),
                        "user_intent_label": state_rec.get(
                            "user_intent_label"),
                        "user_intent_confidence":
                            state_rec.get(
                                "user_intent_confidence"),
                        "user_emotional_posture":
                            state_rec.get(
                                "user_emotional_posture"),
                        "continuity_anchors":
                            state_rec.get(
                                "continuity_anchors"),
                        "applied_changes": applied_changes,
                        "relationship_posture":
                            continuity_summary.get(
                                "relationship_posture"),
                    }
    except Exception:  # noqa: BLE001
        dialogue_pipeline_snapshot = None

    # Program V: per-turn verifier pass on the main reply. Runs
    # AFTER the brain reply, BEFORE the audit write. Bounded
    # single-pass (no recursion, no LLM calls). NEVER raises. If
    # the verifier returns a revised text, we use it for the
    # operator-facing rec; the unrevised text remains captured in
    # the reflection ledger for audit. Skipped silently when the
    # ``cognitive_runtime_use_verifier_enabled`` flag is False.
    verifier_snapshot: Optional[Dict[str, Any]] = None
    try:
        ff_v = _safe("luna_modules.cognitive_feature_flags")
        verifier_enabled = True
        if ff_v is not None:
            try:
                verifier_enabled = bool(
                    ff_v.read_flags().get(
                        "cognitive_runtime_use_verifier_enabled",
                        True))
            except Exception:  # noqa: BLE001
                verifier_enabled = True
        if verifier_enabled:
            vs_mod = _safe(
                "luna_modules.cognitive_verifier_stack")
            if vs_mod is not None:
                candidate = main_result.get("text") or ""
                if candidate.strip():
                    # Last few turn audits as recent_turns
                    # context (best-effort).
                    rt_ctx: List[Dict[str, Any]] = []
                    try:
                        if isinstance(recent_turns, list):
                            rt_ctx = [t for t in
                                      recent_turns[-3:]
                                      if isinstance(t, dict)]
                    except Exception:  # noqa: BLE001
                        rt_ctx = []
                    verifier_snapshot = vs_mod.verify(
                        candidate_text=candidate,
                        candidate_kind="conversation_reply",
                        recent_turns=rt_ctx,
                        caller_hint=(
                            "conversation_runtime_per_turn"),
                        persist=True,
                    )
                    # If the verifier produced a revised text,
                    # rewrite the operator-facing reply. We DO
                    # NOT replay the voice — the voice already
                    # played the original ack/main; the text
                    # revision affects the audit record and any
                    # downstream consumers reading the rec.
                    if verifier_snapshot \
                            and verifier_snapshot.get("ok") \
                            and verifier_snapshot.get("revised") \
                            and verifier_snapshot.get(
                                "final_text"):
                        main_result["text_original_pre_verifier"] \
                            = main_result.get("text")
                        main_result["text"] = (
                            verifier_snapshot["final_text"])
                        main_result["verifier_revised"] = True
    except Exception:  # noqa: BLE001
        verifier_snapshot = None

    # Program U: per-turn deliberation snapshot. We compare the
    # actually-chosen route against a small set of alternative
    # routes (the ones the conversation runtime could plausibly
    # have taken) so the audit captures WHY this route won. This
    # is a non-blocking post-turn capture; the conversation
    # already committed to a route by this point.
    deliberation_snapshot: Optional[Dict[str, Any]] = None
    try:
        ff_dbg = _safe("luna_modules.cognitive_feature_flags")
        deliberation_enabled = True
        if ff_dbg is not None:
            try:
                deliberation_enabled = bool(
                    ff_dbg.read_flags().get(
                        "cognitive_runtime_use_deliberation_enabled",
                        True))
            except Exception:  # noqa: BLE001
                deliberation_enabled = True
        if deliberation_enabled:
            de_mod = _safe(
                "luna_modules.cognitive_simulation_decision_engine")
            if de_mod is not None:
                # Build a small bounded plan set reflecting the
                # actual decision the runtime made.
                chosen_route = (
                    f"intent={main_result.get('intent_used')}"
                    f" brain={main_result.get('brain_backend')}"
                )
                chosen_plan = {
                    "plan_id": "chosen_route",
                    "label": f"actual_route:{chosen_route}",
                    "expected_value":
                        0.8 if main_result.get("ok") else 0.3,
                    "risk": 0.2 if main_result.get("ok") else 0.7,
                    "reversibility": 0.9,
                    "operator_alignment": 0.7,
                    "urgency": 0.5,
                    "dependencies_ready":
                        0.9 if main_result.get("ok") else 0.4,
                    "blocker_exposure": 0.1,
                    "historical_success":
                        0.8 if main_result.get("ok") else 0.3,
                    "evidence_strength":
                        0.7 if evidence_snapshot
                        and (evidence_snapshot.get(
                            "kind_counts") or {}).get(
                                "known", 0) > 0
                        else 0.4,
                }
                refuse_plan = {
                    "plan_id": "refuse_route",
                    "label": "refuse_or_defer",
                    "expected_value": 0.3,
                    "risk": 0.2,
                    "reversibility": 1.0,
                    "operator_alignment": 0.2,
                    "urgency": 0.0,
                    "dependencies_ready": 1.0,
                    "blocker_exposure": 0.0,
                    "historical_success": 0.5,
                    "evidence_strength": 0.3,
                }
                deliberation_snapshot = de_mod.deliberate(
                    decision_context=(
                        f"per_turn_decision:turn_id={turn_id}"),
                    candidate_plans=[chosen_plan, refuse_plan],
                    caller_hint="conversation_runtime_per_turn",
                    extra_context={"turn_id": turn_id},
                    persist=True,
                )
    except Exception:  # noqa: BLE001
        deliberation_snapshot = None

    # Program S: capture per-turn realtime telemetry. NEVER raises.
    try:
        rt_tele = _safe("luna_modules.cognitive_realtime_telemetry")
        sa = _safe("luna_modules.cognitive_sovereign_ack_runtime")
        sm = _safe("luna_modules.cognitive_sovereign_main_runtime")
        if rt_tele is not None:
            # Read warm state at the time the call started (best-effort).
            ack_warm = None
            main_warm = None
            try:
                if sa is not None:
                    ar = sa.report() or {}
                    ack_warm = bool(ar.get("model_loaded_in_memory"))
                if sm is not None:
                    mr = sm.report() or {}
                    main_warm = bool(mr.get("model_loaded_in_memory"))
            except Exception:  # noqa: BLE001
                pass
            total_turn = int((time.time() - started) * 1000)
            rt_tele.record(
                turn_id=turn_id,
                category=classification.get("category"),
                ack_start_ms=0,
                ack_first_token_ms=ack_result.get("first_token_ms"),
                ack_total_ms=ack_result.get("elapsed_ms"),
                ack_streamed=bool(ack_result.get("streamed")),
                ack_warm_at_call=ack_warm,
                main_start_ms=ack_result.get("elapsed_ms"),
                main_first_token_ms=
                    main_text_result.get("first_token_ms"),
                main_completion_ms=
                    main_text_result.get("latency_ms"),
                main_streamed=
                    bool(main_text_result.get("streamed")),
                main_warm_at_call=main_warm,
                total_turn_ms=total_turn,
            )
    except Exception:  # noqa: BLE001
        pass

    # 6. Build operator-readable result
    elapsed_ms = int((time.time() - started) * 1000)
    rec = {
        "ok": bool(main_result.get("ok")),
        "audible": (bool(ack_result.get("audible"))
                     or bool(main_result.get("audible"))),
        "turn_id": turn_id,
        "captured_at_utc": captured,
        "elapsed_ms": elapsed_ms,
        "classification": classification,
        "personality_mode": mode,
        "ack": {
            "text": ack_result.get("text"),
            "dynamic": ack_result.get("dynamic"),
            "elapsed_ms": ack_result.get("elapsed_ms"),
            "backend": ack_result.get("backend"),
            "voice_backend": ack_result.get("voice_backend"),
            "audible": ack_result.get("audible"),
        },
        "main_reply": {
            "text": main_result.get("text"),
            "brain_backend": main_result.get("brain_backend"),
            "brain_kind": main_result.get("brain_kind"),
            "brain_latency_ms": main_result.get("brain_latency_ms"),
            "voice_backend": main_result.get("voice_backend"),
            "voice_audible": main_result.get("audible"),
            "intent_used": main_result.get("intent_used"),
            "error": main_result.get("error"),
            # Live-brain takeover seam (Step 2) — audit visibility:
            # "live_brain_v1" | "deep_memory" | "none".
            "context_source": main_result.get("context_source"),
            # Live-brain takeover seam (Step 3) — HH routing audit:
            # {consulted, ok, decision, selected_tier, sovereign,
            #  influenced, operator_timeout_s, effective_timeout_s, reason}.
            "hh_routing": main_result.get("hh_routing"),
        },
        "route_summary": (
            f"intent={main_result.get('intent_used')} "
            f"brain={main_result.get('brain_backend')} "
            f"voice={main_result.get('voice_backend')} "
            f"ack_dyn={ack_result.get('dynamic')}"
        ),
    }
    if evidence_snapshot is not None:
        rec["evidence_grounding"] = {
            "kind_counts": evidence_snapshot.get("kind_counts"),
            "evidence_refs":
                evidence_snapshot.get("evidence_refs"),
            "item_count":
                len(evidence_snapshot.get("items") or []),
            "elapsed_ms": evidence_snapshot.get("elapsed_ms"),
        }
    if dialogue_pipeline_snapshot is not None:
        rec["dialogue"] = dialogue_pipeline_snapshot
    if verifier_snapshot is not None \
            and verifier_snapshot.get("ok"):
        rec["reflection"] = {
            "reflection_id":
                verifier_snapshot.get("reflection_id"),
            "outcome": verifier_snapshot.get("outcome"),
            "confidence":
                verifier_snapshot.get("confidence"),
            "confidence_label":
                verifier_snapshot.get("confidence_label"),
            "strongest_contradiction":
                verifier_snapshot.get(
                    "strongest_contradiction"),
            "contradiction_count":
                verifier_snapshot.get("contradiction_count"),
            "support_count":
                verifier_snapshot.get("support_count"),
            "revised": verifier_snapshot.get("revised"),
            "revision_reason":
                verifier_snapshot.get("revision_reason"),
            "claim_buckets_count":
                verifier_snapshot.get("claim_buckets_count"),
            "elapsed_ms":
                verifier_snapshot.get("elapsed_ms"),
        }
    if deliberation_snapshot is not None \
            and deliberation_snapshot.get("ok"):
        rec["deliberation"] = {
            "simulation_id":
                deliberation_snapshot.get("simulation_id"),
            "chosen_plan_id":
                deliberation_snapshot.get("chosen_plan_id"),
            "confidence":
                deliberation_snapshot.get("confidence"),
            "robustness":
                deliberation_snapshot.get("robustness"),
            "why_chosen":
                deliberation_snapshot.get("why_chosen"),
            "margin_over_runner_up":
                deliberation_snapshot.get("margin_over_runner_up"),
            "elapsed_ms":
                deliberation_snapshot.get("elapsed_ms"),
        }

    # Program Z: kernel drive-mode. When the drive flag is ON,
    # delegate the canonical lifecycle walk to the drive engine.
    # The drive engine SUPERSEDES the Program Y fusion call —
    # drive_engine.drive_turn internally invokes the unified
    # kernel for the authoritative KernelState. When the drive
    # flag is OFF, fall back to the Program Y observer fusion.
    #
    # Both paths produce the same downstream audit keys
    # (`unified_kernel` summary on rec) so consumers don't
    # break. NEVER raises. No LLM calls inside the drive engine.
    kernel_snapshot: Optional[Dict[str, Any]] = None
    drive_snapshot: Optional[Dict[str, Any]] = None
    try:
        ff_y = _safe("luna_modules.cognitive_feature_flags")
        drive_enabled = True
        kernel_enabled = True
        if ff_y is not None:
            try:
                flags = ff_y.read_flags()
                drive_enabled = bool(flags.get(
                    "cognitive_runtime_use_kernel_drive_enabled",
                    True))
                kernel_enabled = bool(flags.get(
                    "cognitive_runtime_use_unified_kernel"
                    "_enabled", True))
            except Exception:  # noqa: BLE001
                drive_enabled = True
                kernel_enabled = True
        if _async_postreply_enabled() and drive_enabled:
            # Fast conversation: run the post-reply kernel/drive reasoning in
            # the background (it updates KernelState for the NEXT turn) instead
            # of blocking THIS reply. Per-turn kernel audit fields are absent
            # this turn by design; the reasoning still happens, just deferred.
            kde_mod = _safe(
                "luna_modules.cognitive_kernel_drive_engine")
            if kde_mod is not None:
                try:
                    pool.submit(
                        kde_mod.drive_turn,
                        user_text=text,
                        classification=classification,
                        mode=mode,
                        recent_turns=(recent_turns
                                      if isinstance(recent_turns, list)
                                      else []),
                        main_reply=main_result,
                        turn_id=turn_id,
                        caller="conversation_runtime_handle_turn_async",
                    )
                except Exception:  # noqa: BLE001
                    pass
        elif drive_enabled:
            kde_mod = _safe(
                "luna_modules.cognitive_kernel_drive_engine")
            if kde_mod is not None:
                drive_snapshot = kde_mod.drive_turn(
                    user_text=text,
                    classification=classification,
                    mode=mode,
                    recent_turns=recent_turns
                        if isinstance(recent_turns, list)
                        else [],
                    main_reply=main_result,
                    turn_id=turn_id,
                    caller=
                        "conversation_runtime_handle_turn",
                )
                # The drive engine internally calls
                # unified_kernel.process_turn — extract its
                # fusion snapshot for downstream audit
                # compatibility with the Program Y path.
                if drive_snapshot:
                    kernel_snapshot = drive_snapshot.get(
                        "kernel_fusion")
        if (kernel_snapshot is None and kernel_enabled
                and not _async_postreply_enabled()):
            # Legacy Y observer path (when drive disabled OR
            # drive_engine missing).
            uk_mod = _safe(
                "luna_modules.cognitive_unified_kernel")
            if uk_mod is not None:
                kernel_snapshot = uk_mod.process_turn(
                    user_text=text,
                    classification=classification,
                    mode=mode,
                    main_result=main_result,
                    verifier_snapshot=verifier_snapshot,
                    deliberation_snapshot=deliberation_snapshot,
                    dialogue_snapshot=
                        dialogue_pipeline_snapshot,
                    evidence_snapshot=evidence_snapshot,
                    caller="conversation_runtime_handle_turn",
                )
    except Exception:  # noqa: BLE001
        kernel_snapshot = None
        drive_snapshot = None
    if kernel_snapshot is not None \
            and kernel_snapshot.get("ok"):
        rec["unified_kernel"] = {
            "turn_id": kernel_snapshot.get("turn_id"),
            "task_class": kernel_snapshot.get("task_class"),
            "task_class_confidence":
                kernel_snapshot.get("task_class_confidence"),
            "cognitive_mode":
                kernel_snapshot.get("cognitive_mode"),
            "stages_completed":
                kernel_snapshot.get("stages_completed"),
            "active_subsystems":
                kernel_snapshot.get("active_subsystems"),
            "doctrine_violation_count":
                int(kernel_snapshot.get("doctrine", {}).get(
                    "violation_count") or 0),
            "doctrine_highest_severity":
                kernel_snapshot.get("doctrine", {}).get(
                    "highest_severity"),
            "elapsed_ms":
                kernel_snapshot.get("elapsed_ms"),
        }
    if drive_snapshot is not None \
            and drive_snapshot.get("ok"):
        rec["kernel_drive"] = {
            "drive_mode": True,
            "turn_id": drive_snapshot.get("turn_id"),
            "task_class": drive_snapshot.get("task_class"),
            "cognitive_mode":
                drive_snapshot.get("cognitive_mode"),
            "stages_required":
                drive_snapshot.get("stages_required"),
            "stages_completed":
                drive_snapshot.get("stages_completed"),
            "stages_deferred":
                drive_snapshot.get("stages_deferred"),
            "stage_history_rows":
                len(drive_snapshot.get("stage_history")
                     or []),
            "blocker_policy":
                drive_snapshot.get("blocker_policy"),
            "total_wall_budget":
                drive_snapshot.get("total_wall_budget"),
            "aborted":
                bool(drive_snapshot.get("aborted")),
            "elapsed_ms":
                drive_snapshot.get("elapsed_ms"),
        }
        # Program AA: long-horizon goal audit summary.
        # Surfaces the reflect-stage goals block. NEVER raises.
        goals_block = drive_snapshot.get("goals")
        if isinstance(goals_block, dict):
            rec["goals"] = {
                "active_count":
                    int(goals_block.get(
                        "active_count") or 0),
                "advanced_this_turn":
                    int(goals_block.get(
                        "advanced_this_turn") or 0),
                "surfaced_this_turn":
                    bool(goals_block.get(
                        "surfaced_this_turn")),
                "drift_flags_count":
                    int(goals_block.get(
                        "drift_flags_count") or 0),
                "surfaced_goal_id":
                    goals_block.get("surfaced_goal_id"),
                "ran": bool(goals_block.get("ran")),
            }
        else:
            rec["goals"] = {"ran": False}
        # Program BB: self-evaluation outcome audit summary.
        # Surfaces the reflect-stage outcome block. NEVER raises.
        outcome_block = drive_snapshot.get("outcome")
        if isinstance(outcome_block, dict):
            rec["outcome"] = {
                "ran": bool(outcome_block.get("ran")),
                "outcome_label":
                    outcome_block.get("outcome_label"),
                "usefulness_score":
                    outcome_block.get(
                        "usefulness_score"),
                "confidence_score":
                    outcome_block.get(
                        "confidence_score"),
                "goal_effect":
                    outcome_block.get("goal_effect"),
                "primary_failure_cause":
                    outcome_block.get(
                        "primary_failure_cause"),
                "governor_outcome":
                    outcome_block.get(
                        "governor_outcome"),
                "promoted": bool(
                    outcome_block.get("promoted")),
                "avoid_pattern": bool(
                    outcome_block.get(
                        "avoid_pattern")),
                "outcome_id":
                    outcome_block.get("outcome_id"),
            }
        else:
            rec["outcome"] = {"ran": False}
        # Program BB: BB→R adaptation bridge (additive only).
        # Surfaces the reflect-stage bridge block. NEVER raises.
        bridge_block_audit = drive_snapshot.get(
            "adaptation_bridge")
        if isinstance(bridge_block_audit, dict):
            rec["adaptation_bridge"] = {
                "ran": bool(bridge_block_audit.get(
                    "ran")),
                "considered_count":
                    int(bridge_block_audit.get(
                        "considered_count") or 0),
                "bridged_count":
                    int(bridge_block_audit.get(
                        "bridged_count") or 0),
                "refused_count":
                    int(bridge_block_audit.get(
                        "refused_count") or 0),
                "latest_bridge_id":
                    bridge_block_audit.get(
                        "latest_bridge_id"),
            }
        else:
            rec["adaptation_bridge"] = {"ran": False}
        # Program CC: R-side bridge consumer.
        # Surfaces the reflect-stage consumer block.
        consumer_block_audit = drive_snapshot.get(
            "bridge_consumer")
        if isinstance(consumer_block_audit, dict):
            rec["bridge_consumer"] = {
                "ran": bool(consumer_block_audit.get(
                    "ran")),
                "considered":
                    int(consumer_block_audit.get(
                        "considered") or 0),
                "derived":
                    int(consumer_block_audit.get(
                        "derived") or 0),
                "refused":
                    int(consumer_block_audit.get(
                        "refused") or 0),
                "skipped_dedup":
                    int(consumer_block_audit.get(
                        "skipped_dedup") or 0),
                "latest_derived_id":
                    consumer_block_audit.get(
                        "latest_derived_id"),
            }
        else:
            rec["bridge_consumer"] = {"ran": False}
        # Program DD: pattern-mining audit summary.
        patterns_block_audit = drive_snapshot.get("patterns")
        if isinstance(patterns_block_audit, dict):
            rec["patterns"] = {
                "ran": bool(patterns_block_audit.get(
                    "ran")),
                "mined":
                    int(patterns_block_audit.get("mined")
                         or 0),
                "recorded":
                    int(patterns_block_audit.get(
                        "recorded") or 0),
                "refused":
                    int(patterns_block_audit.get(
                        "refused") or 0),
                "kinds_summary":
                    dict(patterns_block_audit.get(
                        "kinds_summary") or {}),
                "scan_window_summary":
                    dict(patterns_block_audit.get(
                        "scan_window_summary") or {}),
            }
        else:
            rec["patterns"] = {"ran": False}
        # Program EE: pattern consumer audit summary.
        pc_block_audit = drive_snapshot.get(
            "pattern_consumer")
        if isinstance(pc_block_audit, dict):
            rec["pattern_consumer"] = {
                "ran": bool(pc_block_audit.get("ran")),
                "considered":
                    int(pc_block_audit.get("considered")
                         or 0),
                "produced":
                    int(pc_block_audit.get("produced")
                         or 0),
                "refused":
                    int(pc_block_audit.get("refused")
                         or 0),
                "by_adapter":
                    dict(pc_block_audit.get(
                        "by_adapter") or {}),
                "latest_hint_id":
                    pc_block_audit.get(
                        "latest_hint_id"),
            }
        else:
            rec["pattern_consumer"] = {"ran": False}
        # Program FF: live consumption-governor audit summary.
        pcg_block_audit = drive_snapshot.get(
            "pattern_consumption")
        if isinstance(pcg_block_audit, dict):
            rec["pattern_consumption"] = {
                "available":
                    bool(pcg_block_audit.get("available")),
                "enabled":
                    bool(pcg_block_audit.get("enabled")),
                "events_in_window":
                    int(pcg_block_audit.get(
                        "events_in_window") or 0),
                "by_adapter":
                    dict(pcg_block_audit.get(
                        "by_adapter") or {}),
            }
        else:
            rec["pattern_consumption"] = {
                "available": False}
        # Program GG: meta-policy audit summary
        mp_block_audit = drive_snapshot.get(
            "meta_policy")
        if isinstance(mp_block_audit, dict):
            rec["meta_policy"] = {
                "ran": bool(mp_block_audit.get("ran")),
                "available":
                    bool(mp_block_audit.get(
                        "available")),
                "enabled":
                    bool(mp_block_audit.get("enabled")),
                "auto_apply_enabled":
                    bool(mp_block_audit.get(
                        "auto_apply_enabled")),
                "proposed":
                    int(mp_block_audit.get(
                        "proposed") or 0),
                "applied":
                    int(mp_block_audit.get(
                        "applied") or 0),
                "refused":
                    int(mp_block_audit.get(
                        "refused") or 0),
                "latest_proposal_id":
                    mp_block_audit.get(
                        "latest_proposal_id"),
                "latest_apply_event":
                    mp_block_audit.get(
                        "latest_apply_event"),
            }
        else:
            rec["meta_policy"] = {"ran": False}
        # Program HH: model-selection audit summary
        ms_block_audit = drive_snapshot.get(
            "model_selection")
        if isinstance(ms_block_audit, dict):
            rec["model_selection"] = {
                "ran": bool(ms_block_audit.get("ran")),
                "available": bool(
                    ms_block_audit.get("available")),
                "enabled": bool(
                    ms_block_audit.get("enabled")),
                "runtime_use_enabled": bool(
                    ms_block_audit.get(
                        "runtime_use_enabled")),
                "tier_count": int(
                    ms_block_audit.get(
                        "tier_count") or 0),
                "select_count": int(
                    ms_block_audit.get(
                        "select_count") or 0),
                "refuse_escalation_count": int(
                    ms_block_audit.get(
                        "refuse_escalation_count")
                    or 0),
                "downgrade_count": int(
                    ms_block_audit.get(
                        "downgrade_count") or 0),
                "fallback_count": int(
                    ms_block_audit.get(
                        "fallback_count") or 0),
                "blocked_count": int(
                    ms_block_audit.get(
                        "blocked_count") or 0),
                "latest_decision":
                    ms_block_audit.get(
                        "latest_decision"),
            }
        else:
            rec["model_selection"] = {"ran": False}
        # Program II: compressed-recall audit summary + live
        # cross-session recall call. The recall consumer is the
        # only place handle_turn touches II during a live turn —
        # it returns a bounded view OR a `ran=False` block. The
        # compressor itself is driven by reflect cadence, not by
        # the live turn (to keep the hot path cheap).
        cr_block_audit = drive_snapshot.get(
            "compressed_recall")
        live_recall: Dict[str, Any] = {"ran": False}
        try:
            cr_mod = _safe(
                "luna_modules."
                "cognitive_cross_session_recall")
            if cr_mod is not None:
                tc = "unknown"
                try:
                    if isinstance(classification, dict):
                        tc = str(
                            classification.get(
                                "task_class")
                            or "unknown")
                except Exception:  # noqa: BLE001
                    tc = "unknown"
                live_recall = cr_mod.recall(
                    task_class=tc,
                    goal_text=None,
                    limit=None)
        except Exception:  # noqa: BLE001
            live_recall = {"ran": False}
        if isinstance(cr_block_audit, dict):
            rec["compressed_recall"] = {
                "ran": bool(cr_block_audit.get("ran")
                              or live_recall.get("ran")),
                "available": bool(
                    cr_block_audit.get("available")),
                "enabled": bool(
                    cr_block_audit.get("enabled")),
                "runtime_use_enabled": bool(
                    cr_block_audit.get(
                        "runtime_use_enabled")),
                "paused": bool(
                    cr_block_audit.get("paused")),
                "total_units": int(
                    cr_block_audit.get(
                        "total_units") or 0),
                "by_state": dict(
                    cr_block_audit.get(
                        "by_state") or {}),
                "by_kind": dict(
                    cr_block_audit.get(
                        "by_kind") or {}),
                "considered": int(
                    live_recall.get(
                        "considered_count") or 0),
                "refused": int(
                    live_recall.get(
                        "refused_count") or 0),
                "recalled": len(
                    live_recall.get(
                        "recalled_units") or []),
                "top_ids": list(
                    live_recall.get("top_ids")
                    or []),
                "char_footprint": int(
                    live_recall.get(
                        "char_footprint") or 0),
                "bounded": bool(
                    live_recall.get(
                        "bounded", True)),
            }
        else:
            rec["compressed_recall"] = {
                "ran": bool(live_recall.get("ran")),
                "considered": int(
                    live_recall.get(
                        "considered_count") or 0),
                "refused": int(
                    live_recall.get(
                        "refused_count") or 0),
                "recalled": len(
                    live_recall.get(
                        "recalled_units") or []),
                "top_ids": list(
                    live_recall.get("top_ids")
                    or []),
                "char_footprint": int(
                    live_recall.get(
                        "char_footprint") or 0),
                "bounded": bool(
                    live_recall.get(
                        "bounded", True)),
            }
        # Program JJ: live attention-budget allocation pass.
        # Compute candidate pool + run governor to set the
        # active working-memory set for THIS turn. NEVER raises.
        wm_block: Dict[str, Any] = {"ran": False}
        try:
            pool_mod = _safe(
                "luna_modules."
                "cognitive_attention_candidate_pool")
            gov_mod = _safe(
                "luna_modules."
                "cognitive_attention_budget_governor")
            st_mod = _safe(
                "luna_modules."
                "cognitive_working_memory_state")
            if pool_mod is not None \
                    and gov_mod is not None \
                    and st_mod is not None:
                tc = "unknown"
                try:
                    if isinstance(classification,
                                   dict):
                        tc = str(
                            classification.get(
                                "task_class")
                            or "unknown")
                except Exception:  # noqa: BLE001
                    tc = "unknown"
                # Advance internal turn counter once per
                # handle_turn before scoring (so hysteresis
                # cooldowns count down correctly)
                try:
                    next_turn_info = st_mod.advance_turn()
                    next_turn = int(
                        next_turn_info.get(
                            "current_turn") or 0)
                except Exception:  # noqa: BLE001
                    next_turn = 0
                pool = pool_mod.build_pool(
                    task_class=tc,
                    goal_text=None,
                    current_turn_value=next_turn,
                    tier_id=None)
                verdict = gov_mod.allocate_for_turn(
                    pool=pool,
                    turn_id=str(_now_iso()),
                    current_turn_value=next_turn)
                wm_block = {
                    "ran": True,
                    "decision":
                        verdict.get("decision"),
                    "tier_id": verdict.get("tier_id"),
                    "considered": int(
                        pool.get("considered_count")
                        or 0),
                    "active_slot_count": int(
                        verdict.get("slot_total")
                        or 0),
                    "slot_cap": int(
                        verdict.get("slot_cap")
                        or 0),
                    "deferred_count": len(
                        verdict.get("deferred")
                        or []),
                    "refused_count": len(
                        verdict.get("refused")
                        or []),
                    "evicted_count": int(
                        verdict.get(
                            "evicted_count") or 0),
                    "char_budget_used": int(
                        verdict.get(
                            "char_budget_used") or 0),
                    "char_budget_total": int(
                        verdict.get(
                            "char_budget_total")
                        or 0),
                    "top_slot_kinds": [
                        s.get("slot_kind")
                        for s in (
                            verdict.get(
                                "active_slots")
                            or [])][:6],
                }
        except Exception:  # noqa: BLE001
            wm_block = {"ran": False}
        rec["working_memory"] = wm_block
        # Program KK: live execution packing.
        # After JJ allocation, run the packer to produce the
        # bounded packed-execution window. NEVER raises.
        ep_block: Dict[str, Any] = {"ran": False}
        try:
            packer = _safe(
                "luna_modules."
                "cognitive_execution_packer")
            if packer is not None:
                pack_v = packer.pack_for_turn(
                    turn_id=str(_now_iso()))
                if isinstance(pack_v, dict):
                    ep_block = {
                        "ran": True,
                        "decision":
                            pack_v.get("decision"),
                        "tier_id":
                            pack_v.get("tier_id"),
                        "packed_count": len(
                            pack_v.get(
                                "packed_blocks")
                            or []),
                        "dropped_count": len(
                            pack_v.get(
                                "dropped_blocks")
                            or []),
                        "deferred_count": len(
                            pack_v.get(
                                "deferred_blocks")
                            or []),
                        "char_budget_used": int(
                            pack_v.get(
                                "char_budget_used")
                            or 0),
                        "char_budget_total": int(
                            pack_v.get(
                                "char_budget_total")
                            or 0),
                        "per_kind_usage": dict(
                            pack_v.get(
                                "per_kind_usage")
                            or {}),
                        "top_slot_ids": [
                            b.get("slot_id")
                            for b in (
                                pack_v.get(
                                    "packed_blocks")
                                or [])[:6]],
                        "bounded": True,
                    }
        except Exception:  # noqa: BLE001
            ep_block = {"ran": False}
        rec["execution_packing"] = ep_block
        # Program LL — sovereign task plan stitching pass.
        # Run the bloat governor on the incoming turn text to
        # decide create vs stitch vs refuse; then dispatch to
        # stitch_or_create on positive verdicts. Never raises.
        tp_block: Dict[str, Any] = {"ran": False}
        try:
            ll_paused = False
            ll_runtime_enabled = True
            try:
                _ll_ff = _safe(
                    "luna_modules.cognitive_feature_flags")
                if _ll_ff is not None:
                    _ll_flags = _ll_ff.read_flags()
                    ll_paused = bool(_ll_flags.get(
                        "cognitive_task_planning_paused",
                        False))
                    ll_runtime_enabled = bool(
                        _ll_flags.get(
                            "cognitive_runtime_use_task"
                            "_planning_enabled", True))
            except Exception:  # noqa: BLE001
                ll_paused = False
                ll_runtime_enabled = True
            if (ll_runtime_enabled
                    and not ll_paused
                    and isinstance(text, str)
                    and text.strip()):
                bg = _safe(
                    "luna_modules."
                    "cognitive_plan_bloat_governor")
                stitch = _safe(
                    "luna_modules."
                    "cognitive_plan_stitcher")
                if (bg is not None
                        and stitch is not None):
                    verdict = bg.evaluate(
                        task_text=text)
                    action = "noop"
                    plan_id_out = None
                    step_id_out = None
                    state_trans = None
                    if isinstance(verdict, dict):
                        v_name = str(
                            verdict.get("verdict")
                            or "")
                        if v_name in (
                                "create_plan",
                                "stitch_existing"):
                            r = stitch.stitch_or_create(
                                task_text=text)
                            if isinstance(r, dict):
                                action = str(
                                    r.get("action")
                                    or "noop")
                                plan_id_out = r.get(
                                    "plan_id")
                                step_id_out = r.get(
                                    "step_id")
                                if action in (
                                        "create_new",
                                        "stitch_existing"):
                                    state_trans = "active"
                        elif v_name == "supersede_old":
                            sup_pid = verdict.get(
                                "supersede_plan_id")
                            st_mod = _safe(
                                "luna_modules."
                                "cognitive_task_plan_state")
                            if (sup_pid
                                    and st_mod
                                    is not None):
                                r = stitch.stitch_or_create(
                                    task_text=text)
                                if (isinstance(r, dict)
                                        and r.get("ok")
                                        and r.get(
                                            "plan_id")):
                                    new_pid = r.get(
                                        "plan_id")
                                    st_mod.supersede_plan(
                                        plan_id=sup_pid,
                                        by_plan_id=new_pid,
                                        reason=(
                                            "ll_runtime_"
                                            "stitch_"
                                            "supersede"))
                                    action = (
                                        "supersede_and_"
                                        "create_new")
                                    plan_id_out = new_pid
                                    state_trans = "active"
                        elif v_name in (
                                "refuse_as_too_small",
                                "refuse_as_duplicate"):
                            action = v_name
                    tp_block = {
                        "ran": True,
                        "verdict":
                            (verdict.get("verdict")
                              if isinstance(verdict, dict)
                              else None),
                        "action": action,
                        "plan_id": plan_id_out,
                        "step_id": step_id_out,
                        "state_transition": state_trans,
                        "matched_plan_id":
                            (verdict.get(
                                "matched_plan_id")
                              if isinstance(verdict, dict)
                              else None),
                        "score":
                            (verdict.get("score")
                              if isinstance(verdict, dict)
                              else None),
                        "bounded": True,
                    }
        except Exception:  # noqa: BLE001
            tp_block = {"ran": False}
        rec["task_plan"] = tp_block
        # Program MM — sovereign step execution orchestrator pass.
        # If the LL block just opened a plan, attempt one bounded
        # dispatch through the MM dispatcher. The dispatcher is
        # itself bounded (no inner loop, no LLM, ≤1 dispatch per
        # call) and the recovery governor + next_step_controller
        # use ONLY the LL public API for state mutations.
        # NEVER raises.
        se_block: Dict[str, Any] = {"ran": False}
        try:
            _mm_ff = _safe(
                "luna_modules.cognitive_feature_flags")
            mm_runtime_enabled = True
            mm_paused = False
            if _mm_ff is not None:
                _mm_flags = _mm_ff.read_flags()
                mm_runtime_enabled = bool(
                    _mm_flags.get(
                        "cognitive_runtime_use_step"
                        "_execution_enabled", True))
                mm_paused = bool(
                    _mm_flags.get(
                        "cognitive_step_execution"
                        "_paused", False))
            plan_id_for_mm = None
            if isinstance(tp_block, dict):
                plan_id_for_mm = tp_block.get(
                    "plan_id")
            if (mm_runtime_enabled
                    and not mm_paused
                    and plan_id_for_mm):
                disp = _safe(
                    "luna_modules."
                    "cognitive_step_dispatcher")
                se_state = _safe(
                    "luna_modules."
                    "cognitive_step_execution_state")
                if (disp is not None
                        and se_state is not None):
                    # Advance the MM turn counter once per
                    # handle_turn (cooldown bookkeeping).
                    try:
                        se_state.advance_turn()
                    except Exception:  # noqa: BLE001
                        pass
                    v = disp.dispatch_next_step(
                        plan_id=str(plan_id_for_mm),
                        turn_id=str(_now_iso()))
                    se_block = {
                        "ran": True,
                        "plan_id":
                            v.get("plan_id")
                            if isinstance(v, dict)
                            else None,
                        "step_id":
                            v.get("step_id")
                            if isinstance(v, dict)
                            else None,
                        "decision":
                            v.get("decision")
                            if isinstance(v, dict)
                            else None,
                        "recovery_action": None,
                        "next_step_id": None,
                        "bounded": True,
                    }
        except Exception:  # noqa: BLE001
            se_block = {"ran": False}
        rec["step_execution"] = se_block
        # Program NN — sovereign step action pipeline.
        # If MM dispatched a step (decision=dispatch), feed that
        # step_id through the NN mapper -> dispatcher -> MM
        # feedback. Bounded, single-pass. NEVER raises.
        sa_block: Dict[str, Any] = {"ran": False}
        try:
            _nn_ff = _safe(
                "luna_modules.cognitive_feature_flags")
            nn_runtime_enabled = True
            nn_paused = False
            if _nn_ff is not None:
                _nn_flags = _nn_ff.read_flags()
                nn_runtime_enabled = bool(
                    _nn_flags.get(
                        "cognitive_runtime_use_step"
                        "_action_enabled", True))
                nn_paused = bool(
                    _nn_flags.get(
                        "cognitive_step_action"
                        "_paused", False))
            mm_plan_id = None
            mm_step_id = None
            mm_execution_id = None
            mm_decision = None
            if isinstance(se_block, dict):
                mm_plan_id = se_block.get(
                    "plan_id")
                mm_step_id = se_block.get(
                    "step_id")
                mm_decision = se_block.get(
                    "decision")
                # MM dispatcher's verdict.execution_id
                # is not surfaced into se_block by default
                # — re-resolve via MM state
                try:
                    se_state = _safe(
                        "luna_modules."
                        "cognitive_step_execution_state")
                    if (se_state is not None
                            and mm_plan_id
                            and mm_step_id):
                        ex = se_state \
                            .find_execution_for_step(
                                plan_id=str(mm_plan_id),
                                step_id=str(mm_step_id))
                        if isinstance(ex, dict):
                            mm_execution_id = (
                                ex.get("execution_id"))
                except Exception:  # noqa: BLE001
                    mm_execution_id = None
            if (nn_runtime_enabled
                    and not nn_paused
                    and mm_decision == "dispatch"
                    and mm_plan_id and mm_step_id):
                pipeline = _safe(
                    "luna_modules."
                    "cognitive_action_dispatcher")
                if pipeline is not None:
                    pres = pipeline \
                        .pipeline_dispatch_with_feedback(
                            plan_id=str(mm_plan_id),
                            step_id=str(mm_step_id),
                            execution_id=(
                                str(mm_execution_id)
                                if mm_execution_id
                                else None),
                            turn_id=str(_now_iso()))
                    m = (pres.get("mapper")
                          if isinstance(pres, dict)
                          else {}) or {}
                    d = (pres.get("dispatch")
                          if isinstance(pres, dict)
                          else {}) or {}
                    sa_block = {
                        "ran": True,
                        "action_kind":
                            m.get("action_kind"),
                        "mapper_decision":
                            m.get("decision"),
                        "decision":
                            d.get("decision"),
                        "preconditions_ok":
                            d.get(
                                "preconditions_ok"),
                        "postconditions_ok":
                            d.get(
                                "postconditions_ok"),
                        "result_state":
                            d.get("result_state"),
                        "reason":
                            d.get("reason"),
                        "recovery_error_kind":
                            pres.get(
                                "recovery_error_kind"),
                        "feedback_action":
                            pres.get(
                                "feedback_action"),
                        "bounded": True,
                    }
        except Exception:  # noqa: BLE001
            sa_block = {"ran": False}
        rec["step_action"] = sa_block
        # Program OO — sovereign outcome-to-action distillation
        # + read-only policy-shaping snapshot per turn. Bounded;
        # single-pass; no LLM. NEVER raises.
        ps_block: Dict[str, Any] = {"ran": False}
        try:
            _oo_ff = _safe(
                "luna_modules.cognitive_feature_flags")
            oo_distill_enabled = True
            oo_paused = False
            oo_runtime_enabled = True
            if _oo_ff is not None:
                _oo_flags = _oo_ff.read_flags()
                oo_distill_enabled = bool(
                    _oo_flags.get(
                        "cognitive_runtime_distill"
                        "_each_turn_enabled", True))
                oo_paused = bool(
                    _oo_flags.get(
                        "cognitive_policy_shaping"
                        "_paused", False))
                oo_runtime_enabled = bool(
                    _oo_flags.get(
                        "cognitive_runtime_use_policy"
                        "_shaping_enabled", True))
            distilled = False
            applied_nudge_count = 0
            refused_nudge_count = 0
            override_active = False
            if (oo_runtime_enabled
                    and not oo_paused):
                if oo_distill_enabled:
                    dist = _safe(
                        "luna_modules."
                        "cognitive_outcome_distiller")
                    if dist is not None:
                        try:
                            r = dist.distill_once()
                            distilled = bool(
                                r.get("decision")
                                == "distilled_ok")
                        except Exception:  # noqa: BLE001
                            distilled = False
                shaper = _safe(
                    "luna_modules."
                    "cognitive_action_policy_shaper")
                if shaper is not None:
                    try:
                        srep = shaper.report()
                        applied_nudge_count = int(
                            srep.get("applied_count")
                            or 0)
                        ptac = dict(srep.get(
                            "per_target_active_count")
                            or {})
                        override_active = bool(
                            any(int(v) > 0
                                  for v in ptac.values()))
                    except Exception:  # noqa: BLE001
                        pass
                aud = _safe(
                    "luna_modules."
                    "cognitive_policy_shaping_audit")
                if aud is not None:
                    try:
                        cnts = aud.counts_by_event(
                            limit=200)
                        refused_nudge_count = int(
                            cnts.get(
                                "nudge_refused")
                            or 0)
                    except Exception:  # noqa: BLE001
                        pass
            ps_block = {
                "ran": True,
                "distilled": bool(distilled),
                "applied_nudge_count":
                    applied_nudge_count,
                "refused_nudge_count":
                    refused_nudge_count,
                "override_active":
                    override_active,
                "bounded": True,
            }
        except Exception:  # noqa: BLE001
            ps_block = {"ran": False}
        rec["policy_shaping"] = ps_block
        # Program PP — sovereign long-horizon execution memory.
        # Bounded single-pass: consolidate candidates -> run one
        # gated promotion pass. Read-only advisory; no live
        # routing mutation. NEVER raises.
        em_block: Dict[str, Any] = {"ran": False}
        try:
            _pp_ff = _safe(
                "luna_modules.cognitive_feature_flags")
            pp_runtime_enabled = True
            pp_paused = False
            pp_consolidate = True
            pp_promote = True
            if _pp_ff is not None:
                _pp_flags = _pp_ff.read_flags()
                pp_runtime_enabled = bool(
                    _pp_flags.get(
                        "cognitive_runtime_use_execution"
                        "_memory_enabled", True))
                pp_paused = bool(
                    _pp_flags.get(
                        "cognitive_execution_memory"
                        "_paused", False))
                pp_consolidate = bool(
                    _pp_flags.get(
                        "cognitive_runtime_consolidate"
                        "_each_turn_enabled", True))
                pp_promote = bool(
                    _pp_flags.get(
                        "cognitive_runtime_promote"
                        "_each_turn_enabled", True))
            consolidated = False
            promoted_count = 0
            candidate_count = 0
            if (pp_runtime_enabled
                    and not pp_paused):
                mem_state = _safe(
                    "luna_modules."
                    "cognitive_execution_memory_state")
                if mem_state is not None:
                    try:
                        mem_state.advance_turn()
                    except Exception:  # noqa: BLE001
                        pass
                if pp_consolidate:
                    cons = _safe(
                        "luna_modules."
                        "cognitive_strategy_consolidator")
                    if cons is not None:
                        try:
                            cr = cons.consolidate_once()
                            consolidated = bool(
                                cr.get("decision")
                                == "consolidated_ok")
                        except Exception:  # noqa: BLE001
                            consolidated = False
                if pp_promote:
                    gov = _safe(
                        "luna_modules."
                        "cognitive_strategy_promotion"
                        "_governor")
                    if gov is not None:
                        try:
                            pr = gov.run_promotion_pass()
                            promoted_count = int(
                                pr.get("promoted") or 0)
                        except Exception:  # noqa: BLE001
                            promoted_count = 0
                if mem_state is not None:
                    try:
                        crep = mem_state.report()
                        candidate_count = int(
                            (crep.get("by_state")
                              or {}).get(
                                  "candidate") or 0)
                    except Exception:  # noqa: BLE001
                        candidate_count = 0
            em_block = {
                "ran": True,
                "consolidated": bool(consolidated),
                "promoted_count": promoted_count,
                "candidate_count": candidate_count,
                "bounded": True,
            }
        except Exception:  # noqa: BLE001
            em_block = {"ran": False}
        rec["execution_memory"] = em_block
    else:
        rec["kernel_drive"] = {"drive_mode": False}
        rec["goals"] = {"ran": False}
        rec["outcome"] = {"ran": False}
        rec["adaptation_bridge"] = {"ran": False}
        rec["bridge_consumer"] = {"ran": False}
        rec["patterns"] = {"ran": False}
        rec["pattern_consumer"] = {"ran": False}
        rec["pattern_consumption"] = {"available": False}
        rec["meta_policy"] = {"ran": False}
        rec["model_selection"] = {"ran": False}
        rec["compressed_recall"] = {"ran": False}
        rec["working_memory"] = {"ran": False}
        rec["execution_packing"] = {"ran": False}
        rec["task_plan"] = {"ran": False}
        rec["step_execution"] = {"ran": False}
        rec["step_action"] = {"ran": False}
        rec["policy_shaping"] = {"ran": False}
        rec["execution_memory"] = {"ran": False}
    _append_audit({"event": "handle_turn", **rec, "caller": caller})
    return rec


_ACK_BUDGET_S = 8.0  # ack voice playback wall-clock cap


def _ack_text_only(*, text: str, classification: Dict[str, Any],
                     recent_turns: List[Dict[str, Any]], mode: str
                     ) -> Dict[str, Any]:
    """Generate the ack text via the micro-LM. Synchronous. NEVER raises."""
    started = time.time()
    ack_mod = _safe("luna_modules.cognitive_conversation_micro_ack")
    if ack_mod is None:
        return {"ok": False, "text": "", "dynamic": False,
                 "elapsed_ms": int((time.time() - started) * 1000),
                 "backend": "ack_module_missing",
                 "category": classification.get("category"),
                 "personality_mode": mode}
    gen = ack_mod.generate_micro_ack(
        incoming_text=text, classification=classification,
        recent_turns=recent_turns, mode=mode, timeout_s=4.0)
    return {
        "ok": bool(gen.get("ok")),
        "text": gen.get("text"),
        "dynamic": gen.get("dynamic"),
        "elapsed_ms": int((time.time() - started) * 1000),
        "gen_elapsed_ms": gen.get("elapsed_ms"),
        "backend": gen.get("backend"),
        "category": gen.get("category"),
        "personality_mode": gen.get("personality_mode"),
        "fallback_reason": gen.get("fallback_reason"),
    }


def _ack_task(*, text: str, classification: Dict[str, Any],
                recent_turns: List[Dict[str, Any]], mode: str,
                allow_audible: bool) -> Dict[str, Any]:
    started = time.time()
    ack_mod = _safe("luna_modules.cognitive_conversation_micro_ack")
    if ack_mod is None:
        return {"ok": False, "text": "", "dynamic": False,
                 "elapsed_ms": int((time.time() - started) * 1000),
                 "backend": "ack_module_missing", "audible": False,
                 "voice_backend": None}
    gen = ack_mod.generate_micro_ack(
        incoming_text=text, classification=classification,
        recent_turns=recent_turns, mode=mode, timeout_s=4.0)
    voice_outcome: Dict[str, Any] = {"ok": False, "backend": "skip_no_text"}
    if allow_audible and gen.get("text"):
        voice_outcome = _speak_ack(gen["text"])
    return {
        "ok": bool(gen.get("ok")),
        "text": gen.get("text"),
        "dynamic": gen.get("dynamic"),
        "elapsed_ms": int((time.time() - started) * 1000),
        "gen_elapsed_ms": gen.get("elapsed_ms"),
        "backend": gen.get("backend"),
        "category": gen.get("category"),
        "personality_mode": gen.get("personality_mode"),
        "fallback_reason": gen.get("fallback_reason"),
        "voice_backend": (voice_outcome.get("backend") if isinstance(voice_outcome, dict) else None),
        "audible": bool(voice_outcome.get("ok")) if isinstance(voice_outcome, dict) else False,
    }


def _main_task(*, text: str, classification: Dict[str, Any],
                recent_turns: List[Dict[str, Any]], mode: str,
                timeout_s: float, allow_audible: bool,
                want_premium_voice: bool) -> Dict[str, Any]:
    started = time.time()
    reasoning = _reason_main(text=text, classification=classification,
                              recent_turns=recent_turns, mode=mode,
                              timeout_s=min(60.0, max(10.0, timeout_s - 5.0)))
    reply_text = reasoning.get("text") or ""
    voice_outcome: Dict[str, Any] = {"ok": False, "backend": "skip_no_text"}
    intent_used = "answer"
    if allow_audible and reply_text:
        # Premium only when reasoning succeeded
        wp = (want_premium_voice
              and reasoning.get("ok")
              and classification.get("category") not in ("casual", "control"))
        voice_outcome = _speak_main_reply(reply_text, want_premium=wp)
        intent_used = "clone" if (isinstance(voice_outcome, dict)
                                    and voice_outcome.get("backend") == "v4_premium"
                                    and "xtts" in (voice_outcome.get("voice_identity") or "")) else "answer"
    return {
        "ok": bool(reasoning.get("ok")),
        "text": reply_text,
        "brain_backend": reasoning.get("backend"),
        "brain_kind": reasoning.get("kind"),
        "brain_latency_ms": reasoning.get("latency_ms"),
        "voice_backend": (voice_outcome.get("backend")
                          if isinstance(voice_outcome, dict) else None),
        "audible": (bool(voice_outcome.get("ok"))
                     if isinstance(voice_outcome, dict) else False),
        "intent_used": intent_used,
        "elapsed_ms": int((time.time() - started) * 1000),
        "error": reasoning.get("error"),
    }


# ---------------------------------------------------------------------------
# Cockpit / dashboard / operator report
# ---------------------------------------------------------------------------

def report() -> Dict[str, Any]:
    """Operator-readable snapshot of the conversation runtime."""
    st = _safe("luna_modules.cognitive_conversation_state")
    micro = _safe("luna_modules.cognitive_conversation_micro_ack")
    clf = _safe("luna_modules.cognitive_conversation_classifier")
    router = _safe("luna_modules.cognitive_conversation_ack_router")
    warming = _safe("luna_modules.cognitive_luna_warming")
    sov_ack = _safe("luna_modules.cognitive_sovereign_ack_runtime")
    sov_main = _safe("luna_modules.cognitive_sovereign_main_runtime")
    return {
        "available": True,
        "enabled": _is_enabled(),
        "sovereign_v2_enabled": _is_sovereign_v2_enabled(),
        "ack_router_legacy_allowed": _is_ack_router_allowed(),
        "premium_voice_allowed": _premium_voice_allowed(),
        "pool_max_workers": _POOL_MAX_WORKERS,
        "pool_alive": (_POOL is not None),
        "audit_path": AUDIT_PATH,
        "state": (st.report() if st else {"available": False}),
        "sovereign_ack": (sov_ack.report() if sov_ack
                            else {"available": False}),
        "sovereign_main": (sov_main.report() if sov_main
                            else {"available": False}),
        "micro_ack": (micro.report() if micro else {"available": False}),
        "classifier_categories": (clf.categories() if clf else []),
        "ack_router": (router.report() if router else {"available": False}),
        "warming": (warming.report() if warming else {"available": False}),
        "doctrine": [
            "no_ollama_in_live_hot_path",
            "no_canned_phrase_library_in_live_hot_path",
            "every_ack_dynamically_generated",
            "dual_model_sovereign_local_only",
        ],
    }


__all__ = [
    "AUDIT_PATH",
    "handle_turn",
    "shutdown_pool",
    "report",
]
