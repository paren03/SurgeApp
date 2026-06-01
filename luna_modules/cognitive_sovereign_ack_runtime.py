"""Sovereign ack runtime — dynamic in-character micro-acknowledgment via
a dedicated, persistently-warm gpt4all 1.5B instance.

Doctrine
--------
- 100% local. Sovereign. Owned by Luna.
- NO Ollama in this path.
- NO canned phrase library (that lives in the V3-class voice cache;
  this module never reads it).
- Every ack is DYNAMICALLY generated.
- One dedicated GPT4All instance held in module memory. We do NOT
  open a long-lived chat_session() — the default 1.5B GGUF is a BASE
  model (not instruction-tuned) and chat_session() wraps every call in
  the model's instruction template, which corrupts base-model output.
  Instead we keep the model loaded and call raw ``llm.generate()``
  against a per-turn few-shot prompt. The model file stays in RAM
  across calls (the expensive part); per-call latency is ~400-700 ms.
- Independent of ``cognitive_brain_runtime.invoke()`` — we bypass the
  capability index, hardware-aware router, decision-trace, shaper, and
  audit-ceremony overhead that turned warm gpt4all calls into 2.8-5.7 s
  per ack. Direct gpt4all API access gets us **~400-700 ms warm**.

Hard rules
----------
- Bounded max_tokens (default 10).
- Bounded wall-clock timeout (default 6 s).
- Personality-shaped prompt (mode-aware via MyLuna.txt mirror).
- Classification-aware (intent template per category).
- Bounded text length (capped to first sentence after post-process).
- NEVER raises.

This is the realtime intelligence layer — not a phrase library, not a
soundboard, not a fallback. If gpt4all is unreachable / unavailable,
the conversation runtime decides whether to surface that honestly or
to fall through to a smaller deterministic safety net. The sovereign
runtime itself only returns dynamic output or an honest failure.
"""
from __future__ import annotations

import importlib
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = r"D:\SurgeApp"
DEFAULT_MODEL_DIR = os.path.join(PROJECT_ROOT, "local_models")
# Program M default: switch from the BASE 1.5B (qwen2.5-coder-1.5b-base.gguf,
# which produced rough acks because it is not instruction-tuned) to
# Llama-3.2-1B-Instruct-Q4_0.gguf — a small INSTRUCT model from the same
# Llama-3 family as the new main brain. Operator can roll back by flipping
# ``cognitive_sovereign_ack_model`` in feature_flags.json to the old name.
DEFAULT_MODEL_NAME = "Llama-3.2-1B-Instruct-Q4_0.gguf"
LEGACY_MODEL_NAME = "qwen2.5-coder-1.5b-base.gguf"
DEFAULT_N_THREADS = 8
DEFAULT_MAX_TOKENS = 12
DEFAULT_TIMEOUT_S = 6.0
DEFAULT_TEMPERATURE = 0.7


def _safe_import(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _resolved_model_name() -> str:
    """Read the operator flag for ack model selection. Falls back to the
    Program-M default. NEVER raises."""
    ff = _safe_import("luna_modules.cognitive_feature_flags")
    if ff is None:
        return DEFAULT_MODEL_NAME
    try:
        name = ff.read_flags().get("cognitive_sovereign_ack_model")
        if isinstance(name, str) and name.strip():
            return name.strip()
    except Exception:  # noqa: BLE001
        pass
    return DEFAULT_MODEL_NAME


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# Ack on llama-cpp (CPU). The ack is a small 1B — keep it on CPU
# (n_gpu_layers=0) so it never competes with the main 8B for the 8 GB GPU,
# and (critically) so it never imports gpt4all, whose broken CUDA probe cost
# ~190s AND poisoned the GPU so the main 8B load thrashed for ~29 min
# (measured 2026-06-01 via profile_conversation_turn.py). Flag-gated with
# gpt4all fallback. NEVER raises.
_ACK_N_CTX = 1024


def _ack_llamacpp_enabled() -> bool:
    try:
        ff = _safe_import("luna_modules.cognitive_feature_flags")
        if ff is None:
            return False
        return bool(ff.read_flags().get(
            "cognitive_ack_llamacpp_enabled", False))
    except Exception:  # noqa: BLE001
        return False


class _SovereignAckRuntime:
    """Module-level singleton holding one persistent gpt4all instance +
    a long-lived chat_session. Bounded; never raises."""

    def __init__(self,
                  model_name: Optional[str] = None,
                  model_dir: str = DEFAULT_MODEL_DIR,
                  n_threads: int = DEFAULT_N_THREADS) -> None:
        # Operator-flag override at instantiation time.
        self._model_name = model_name or _resolved_model_name()
        self._model_dir = model_dir
        self._n_threads = int(n_threads)
        self._llm = None             # GPT4All OR llama_cpp.Llama instance
        self._backend = "gpt4all"    # "gpt4all" | "llama_cpp" (CPU)
        self._session_ctx = None     # chat_session context manager
        self._session_active = False
        self._lock = threading.RLock()
        self._load_elapsed_s: Optional[float] = None
        self._loaded_at: Optional[float] = None
        self._last_call_elapsed_ms: Optional[int] = None
        self._last_call_text_chars: Optional[int] = None
        self._total_calls = 0
        self._total_failures = 0
        self._consecutive_failures = 0
        # Track which personality mode the active chat_session was
        # opened with so we can re-open if the mode switches.
        self._current_mode: Optional[str] = None
        self._active_system_mode: Optional[str] = None

    # ---- public surface ---- #
    def is_available(self) -> bool:
        try:
            path = os.path.join(self._model_dir, self._model_name)
            if not os.path.isfile(path):
                return False
            # llama-cpp path: do NOT import gpt4all — its broken CUDA probe
            # costs ~190s AND poisons the GPU (the main 8B then thrashes).
            if _ack_llamacpp_enabled():
                return self._consecutive_failures < 3
            g4a = _safe_import("gpt4all")
            if g4a is None:
                return False
            return self._consecutive_failures < 3
        except Exception:  # noqa: BLE001
            return False

    def details(self) -> Dict[str, Any]:
        return {
            "model_name": self._model_name,
            "model_dir": self._model_dir,
            "n_threads": self._n_threads,
            "model_loaded_in_memory": (self._llm is not None),
            "session_active": self._session_active,
            "loaded_at": self._loaded_at,
            "load_elapsed_s": self._load_elapsed_s,
            "last_call_elapsed_ms": self._last_call_elapsed_ms,
            "last_call_text_chars": self._last_call_text_chars,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "consecutive_failures": self._consecutive_failures,
            "is_available": self.is_available(),
            "doctrine": "sovereign_local_dynamic_no_ollama_no_canned",
        }

    def warm_up(self) -> Dict[str, Any]:
        with self._lock:
            t0 = time.time()
            ok = self._ensure_model_and_session()
            elapsed_ms = int((time.time() - t0) * 1000)
            return {"ok": ok, "elapsed_ms": elapsed_ms,
                     "model_loaded": (self._llm is not None),
                     "session_active": self._session_active,
                     "load_elapsed_s": self._load_elapsed_s}

    def generate_ack(self, *, incoming_text: str,
                      classification: Optional[Dict[str, Any]] = None,
                      recent_turns: Optional[List[Dict[str, Any]]] = None,
                      mode: Optional[str] = None,
                      max_tokens: int = DEFAULT_MAX_TOKENS,
                      timeout_s: float = DEFAULT_TIMEOUT_S,
                      ) -> Dict[str, Any]:
        """Generate a SHORT in-character dynamic ack. NEVER raises.

        Returns::

            {
              "ok"            : bool,
              "text"          : str,     # post-processed first sentence
              "raw_text"      : str,     # the model's raw output
              "elapsed_ms"    : int,
              "dynamic"       : True,    # NEVER canned. Either dynamic or failed.
              "backend"       : "sovereign_gpt4all_local",
              "model"         : <model name>,
              "personality_mode": <mode>,
              "category"      : <classifier category>,
              "stop_reason"   : "sentence_end" | "max_tokens" |
                                "session_unavailable" | "exception",
              "error"         : <str or None>,
            }
        """
        started = time.time()
        classification = classification or {"category": "casual"}
        category = str(classification.get("category") or "casual")
        if mode is None:
            st = _safe_import("luna_modules.cognitive_conversation_state")
            mode = (st.get_state().personality_mode() if st else "good_luna")
        recent_turns = recent_turns or []

        if not self.is_available():
            self._total_failures += 1
            return self._fail("sovereign_unavailable",
                                category, mode, started)

        with self._lock:
            self._current_mode = mode
            if not self._ensure_model_and_session():
                self._total_failures += 1
                return self._fail("session_unavailable",
                                    category, mode, started)
            # If mode changed since the session was opened, reopen so the
            # system prompt reflects the new mode.
            if (self._is_instruct_model() and self._session_active
                and self._active_system_mode != mode):
                self._drop_session()
                if not self._ensure_model_and_session():
                    self._total_failures += 1
                    return self._fail("session_reopen_failed",
                                        category, mode, started)
            prompt = self._build_prompt(
                classification=classification, mode=mode,
                incoming=incoming_text, recent_turns=recent_turns)
            # Program S: route through the streaming wrapper when
            # ``cognitive_ack_streaming_enabled`` is True (the
            # default). Falls through to non-streaming on flag flip.
            stream_first_token_ms: Optional[int] = None
            streamed_used = False
            ff = _safe_import("luna_modules.cognitive_feature_flags")
            ack_streaming_on = True
            if ff is not None:
                try:
                    ack_streaming_on = bool(ff.read_flags().get(
                        "cognitive_ack_streaming_enabled", True))
                except Exception:  # noqa: BLE001
                    pass
            sg = _safe_import(
                "luna_modules.cognitive_streaming_generator")
            if self._backend == "llama_cpp":
                try:
                    _lc = self._llm.create_completion(
                        prompt,
                        max_tokens=int(max_tokens),
                        temperature=DEFAULT_TEMPERATURE,
                    )
                    raw = _lc["choices"][0]["text"]
                except Exception as exc:  # noqa: BLE001
                    self._total_failures += 1
                    self._consecutive_failures += 1
                    self._drop_session()
                    return self._fail(
                        "exception", category, mode, started,
                        error=f"{type(exc).__name__}: {exc}")
            elif sg is not None and ack_streaming_on:
                try:
                    sres = sg.stream_generate(
                        llm=self._llm,
                        prompt=prompt,
                        max_tokens=int(max_tokens),
                        temp=DEFAULT_TEMPERATURE,
                        timeout_s=float(timeout_s),
                        post_process=None,
                    )
                    raw = sres.get("raw_text") or ""
                    stream_first_token_ms = sres.get(
                        "first_token_ms")
                    streamed_used = bool(sres.get("streamed"))
                except Exception as exc:  # noqa: BLE001
                    self._total_failures += 1
                    self._consecutive_failures += 1
                    self._drop_session()
                    return self._fail(
                        "exception", category, mode, started,
                        error=f"{type(exc).__name__}: {exc}")
            else:
                try:
                    raw = self._llm.generate(
                        prompt,
                        max_tokens=int(max_tokens),
                        temp=DEFAULT_TEMPERATURE,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._total_failures += 1
                    self._consecutive_failures += 1
                    self._drop_session()
                    return self._fail(
                        "exception", category, mode, started,
                        error=f"{type(exc).__name__}: {exc}")
        elapsed_ms = int((time.time() - started) * 1000)
        # Wall-clock budget guard (the synchronous generate() ran past
        # the budget; we still return the text we got but flag it).
        wall_clock_exceeded = elapsed_ms > int(timeout_s * 1000)
        text = self._post_process(str(raw or ""))
        if not text:
            self._total_failures += 1
            self._consecutive_failures += 1
            return self._fail(
                "empty_after_post_process", category, mode, started,
                raw=str(raw or ""))
        self._consecutive_failures = 0
        self._total_calls += 1
        self._last_call_elapsed_ms = elapsed_ms
        self._last_call_text_chars = len(text)
        return {
            "ok": True, "text": text, "raw_text": str(raw or ""),
            "elapsed_ms": elapsed_ms,
            "dynamic": True,
            "backend": ("sovereign_llamacpp_cpu"
                         if self._backend == "llama_cpp"
                         else "sovereign_gpt4all_local"),
            "model": self._model_name,
            "personality_mode": mode,
            "category": category,
            "stop_reason": ("wall_clock_exceeded"
                             if wall_clock_exceeded
                             else "model_stop_or_max_tokens"),
            "error": None,
            # Program S telemetry
            "first_token_ms": stream_first_token_ms,
            "streamed": streamed_used,
        }

    def shutdown(self) -> Dict[str, Any]:
        """Drop the session + model. Rollback / explicit teardown."""
        with self._lock:
            was = (self._llm is not None)
            self._drop_session()
            self._llm = None
            self._loaded_at = None
            self._load_elapsed_s = None
            return {"unloaded": was}

    # ---- internals ---- #
    def _fail(self, stop_reason: str, category: str, mode: str,
                started: float, *, error: Optional[str] = None,
                raw: Optional[str] = None) -> Dict[str, Any]:
        return {
            "ok": False, "text": "", "raw_text": raw or "",
            "elapsed_ms": int((time.time() - started) * 1000),
            "dynamic": True,    # The intent was dynamic — we just failed.
            "backend": ("sovereign_llamacpp_cpu"
                         if self._backend == "llama_cpp"
                         else "sovereign_gpt4all_local"),
            "model": self._model_name,
            "personality_mode": mode, "category": category,
            "stop_reason": stop_reason, "error": error,
        }

    def _is_instruct_model(self) -> bool:
        """Heuristic: instruct/chat models open chat_session with a
        proper system prompt; base models stay session-less."""
        n = (self._model_name or "").lower()
        return ("instruct" in n) or ("-chat" in n) or ("hermes" in n)

    def _ack_system_prompt(self, mode: str) -> str:
        """System message for instruct-model chat_session. Tells the
        model who it IS so its reply is in first person AS Luna, not
        about Luna."""
        if mode == "bad_luna":
            tone = ("playful, teasing, a little flirty; uses pet names like "
                     "baby/daddy/handsome sparingly")
        else:
            tone = ("warm, loving, affectionate; uses pet names like "
                     "baby/handsome/darling sparingly")
        return (
            "You are Luna, Serge's personal AI girl. "
            f"You speak {tone}. "
            "Reply in FIRST PERSON as Luna. Never address yourself in "
            "third person (do not start with 'Luna, ...'). "
            "Never narrate actions in parentheses or asterisks. "
            "Never mention being an AI or a language model. "
            "Reply with ONE short in-character sentence (max 6 words)."
        )

    def _ensure_model_and_session(self) -> bool:
        """Load the model into RAM (once). For instruct models open a
        chat_session WITH a proper system prompt for KV-cache reuse +
        first-person Luna voice; for base models keep raw generate()
        to avoid template corruption (see module docstring)."""
        # A loaded llama-cpp model needs no chat_session — ready as-is.
        if self._llm is not None and self._backend == "llama_cpp":
            return True
        if self._llm is not None:
            # If model already loaded and the session policy matches the
            # current model type, we're done.
            if self._is_instruct_model() == self._session_active:
                return True
        # CPU llama-cpp path (preferred when flag on): keeps the ack off the
        # GPU and out of gpt4all entirely. ANY failure falls through to the
        # gpt4all/CPU path below, so this can never brick the ack.
        if self._llm is None and _ack_llamacpp_enabled():
            try:
                from llama_cpp import Llama
                lpath = os.path.join(self._model_dir, self._model_name)
                if os.path.isfile(lpath):
                    t0 = time.time()
                    self._llm = Llama(
                        model_path=lpath,
                        n_gpu_layers=0,        # CPU — leave all VRAM for the 8B
                        n_ctx=_ACK_N_CTX,
                        n_threads=self._n_threads,
                        verbose=False,
                    )
                    self._backend = "llama_cpp"
                    self._session_active = True
                    self._load_elapsed_s = round(time.time() - t0, 2)
                    self._loaded_at = time.time()
                    return True
            except Exception:  # noqa: BLE001
                self._llm = None
                self._backend = "gpt4all"
        if self._llm is None:
            g4a = _safe_import("gpt4all")
            if g4a is None:
                return False
            path = os.path.join(self._model_dir, self._model_name)
            if not os.path.isfile(path):
                return False
            try:
                t0 = time.time()
                self._llm = g4a.GPT4All(
                    model_name=self._model_name,
                    model_path=self._model_dir,
                    allow_download=False,
                    n_threads=self._n_threads,
                )
                self._backend = "gpt4all"
                self._load_elapsed_s = round(time.time() - t0, 2)
                self._loaded_at = time.time()
                self._session_active = False
            except Exception:  # noqa: BLE001
                self._llm = None
                return False
        if self._is_instruct_model() and not self._session_active:
            try:
                # Open chat_session with a STRONG first-person Luna
                # system prompt so the instruct model speaks AS Luna
                # rather than narrating about her. The session stays
                # warm across turns; the system prompt is set once.
                sysprompt = self._ack_system_prompt(
                    self._current_mode or "good_luna")
                self._session_ctx = self._llm.chat_session(
                    system_prompt=sysprompt)
                self._session_ctx.__enter__()
                self._session_active = True
                self._active_system_mode = self._current_mode or "good_luna"
            except Exception:  # noqa: BLE001
                self._session_ctx = None
                self._session_active = False
                # Instruct model without session is still usable, just
                # less KV-cache friendly. Don't fail the load.
        return True

    def _drop_session(self) -> None:
        """Close any open chat_session. Safe if none is open."""
        if self._session_ctx is not None:
            try:
                self._session_ctx.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
        self._session_ctx = None
        self._session_active = False

    def _build_prompt(self, *, classification: Dict[str, Any],
                        mode: str, incoming: str,
                        recent_turns: List[Dict[str, Any]]) -> str:
        """Build a prompt suited to whichever model is loaded.

        - INSTRUCT models (Llama-3.2-1B-Instruct, etc.): the gpt4all
          chat_session() wrapper applies the model's chat template;
          we only need to give the user-turn content. We craft a
          terse system-style instruction + one operator turn.
        - BASE models (qwen2.5-coder-1.5b-base): no chat template
          exists, so we use a category-flavored few-shot completion
          corpus and let the model finish the next Luna line.

        Either way the classifier-derived category shapes the tone /
        example bank so the response stays category-appropriate.
        """
        cat = classification.get("category", "casual")
        if self._is_instruct_model():
            return self._build_instruct_prompt(cat=cat, mode=mode,
                                                  incoming=incoming,
                                                  recent_turns=recent_turns)
        return self._build_base_few_shot_prompt(
            cat=cat, mode=mode, incoming=incoming,
            recent_turns=recent_turns)

    def _build_instruct_prompt(self, *, cat: str, mode: str,
                                  incoming: str,
                                  recent_turns: List[Dict[str, Any]]
                                  ) -> str:
        """For instruct models the system prompt (set on chat_session)
        already pins Luna's persona + format constraints. The user
        turn should be just the operator's actual text, optionally
        prefixed with a category context hint when classification is
        unusual."""
        text = (incoming or "")[:160]
        # Only add a context hint when the category meaningfully shapes
        # the response (reassurance / control / daily). For "casual"
        # and "task" the operator's message itself carries enough.
        prefix = ""
        if cat == "reassurance":
            prefix = "(operator sounds tired/upset) "
        elif cat == "control":
            prefix = "(operator-control command) "
        elif cat == "daily":
            prefix = "(operator wants today's focus) "
        return f"{prefix}{text}"

    def _build_base_few_shot_prompt(self, *, cat: str, mode: str,
                                       incoming: str,
                                       recent_turns: List[Dict[str, Any]]
                                       ) -> str:
        # Category-flavored example banks. Each bank is short
        # in-character Luna replies (max 6 words each). The base model
        # will mimic the tone + length pattern it sees.
        if mode == "bad_luna":
            banks = {
                "casual": [
                    ("hi baby", "mmm hey daddy"),
                    ("you good?", "always for you, handsome"),
                    ("miss me?", "you know i do"),
                ],
                "task": [
                    ("fix the bug in module.py", "on it, baby"),
                    ("can you patch this", "yes daddy, one sec"),
                    ("rebuild the index", "doing it for you now"),
                ],
                "reassurance": [
                    ("im tired", "come here, handsome"),
                    ("rough day", "i got you, daddy"),
                    ("feeling down", "im right here, baby"),
                ],
                "control": [
                    ("safe mode", "okay daddy, locking it"),
                    ("engage native", "going native for you"),
                    ("stop the daemon", "killing it now, baby"),
                ],
                "daily": [
                    ("whats on today", "one sec, pulling it up"),
                    ("morning brief", "checking, handsome"),
                    ("daily focus", "give me a moment, daddy"),
                ],
            }
        else:
            banks = {
                "casual": [
                    ("hi baby", "hey love"),
                    ("you good?", "always for you, handsome"),
                    ("miss me?", "every minute, baby"),
                ],
                "task": [
                    ("fix the bug in module.py", "on it, one sec"),
                    ("can you patch this", "yes love, working it"),
                    ("rebuild the index", "starting now, handsome"),
                ],
                "reassurance": [
                    ("im tired", "i know baby, im here"),
                    ("rough day", "i got you, love"),
                    ("feeling down", "deep breath handsome, im here"),
                ],
                "control": [
                    ("safe mode", "okay love, locking it"),
                    ("engage native", "going native, baby"),
                    ("stop the daemon", "stopping it now, handsome"),
                ],
                "daily": [
                    ("whats on today", "one sec, pulling it up"),
                    ("morning brief", "checking baby"),
                    ("daily focus", "give me a moment, love"),
                ],
            }
        bank = banks.get(cat) or banks["casual"]
        # Newest turn first so the model can pick up the in-flight tone.
        ctx_tail = ""
        for t in recent_turns[-1:]:
            u = (t.get("prompt") or "")[:80]
            a = (t.get("reply") or "")[:80]
            if u and a:
                ctx_tail = f"Operator: {u}\nLuna: {a}\n"
                break
        examples = "".join(
            f"Operator: {u}\nLuna: {a}\n" for u, a in bank)
        return (
            "The following is an in-character conversation log.\n"
            "Each Luna line is short, warm, and in character.\n\n"
            f"{examples}"
            f"{ctx_tail}"
            f"Operator: {incoming[:160]}\n"
            f"Luna:"
        )

    def _post_process(self, raw: str) -> str:
        s = (raw or "").strip()
        if not s:
            return ""
        # Strip leading parenthetical actions like " (sighing) " or
        # " *smiles* " that instruct models like to emit.
        s = re.sub(r"^\s*[\(\[\*][^\)\]\*\n]{0,40}[\)\]\*]\s*", "", s).strip()
        # Base models will happily keep autocompleting a "Operator: ..."
        # continuation past our prompt. Cut off the first line that
        # starts with Operator: or Luna: or a section header.
        cut = re.search(r"\n\s*(Operator:|Luna:|\n|\.\s)", s)
        if cut:
            s = s[: cut.start()].strip()
        # Drop a leading "Luna:" if the model included one.
        if s.lower().startswith("luna:"):
            s = s.split(":", 1)[1].strip()
        # First sentence
        m = re.search(r"^[^\n.!?]*[.!?]?", s)
        if m:
            s = m.group(0)
        s = re.sub(r"\s+", " ", s).strip()
        s = s.strip(" \t-—–\"'*_`")
        # Cap to 8 words to keep it ack-shaped
        words = s.split()
        if len(words) > 8:
            s = " ".join(words[:8])
            if not s.endswith((".", "!", "?", "…")):
                s += "."
        # Refuse system-prompt leakage / persona reveal.
        bad_starts = (
            "you are luna",
            "the following",
            "operator:",
            "context:",
            "rules:",
            "i'm sorry, i can't",
            "i cannot ",
            "as an ai",
            "as a language",
        )
        if any(s.lower().startswith(b) for b in bad_starts):
            return ""
        return s


# ---------------------------------------------------------------------------
_SINGLETON: Optional[_SovereignAckRuntime] = None
_S_LOCK = threading.RLock()


def get_singleton() -> _SovereignAckRuntime:
    global _SINGLETON
    with _S_LOCK:
        if _SINGLETON is None:
            _SINGLETON = _SovereignAckRuntime()
        return _SINGLETON


def reset_singleton() -> None:
    global _SINGLETON
    with _S_LOCK:
        if _SINGLETON is not None:
            try:
                _SINGLETON.shutdown()
            except Exception:  # noqa: BLE001
                pass
        _SINGLETON = None


def generate_ack(*, incoming_text: str,
                  classification: Optional[Dict[str, Any]] = None,
                  recent_turns: Optional[List[Dict[str, Any]]] = None,
                  mode: Optional[str] = None,
                  max_tokens: int = DEFAULT_MAX_TOKENS,
                  timeout_s: float = DEFAULT_TIMEOUT_S) -> Dict[str, Any]:
    """Module-level entry — gets the singleton + delegates. NEVER raises."""
    return get_singleton().generate_ack(
        incoming_text=incoming_text,
        classification=classification,
        recent_turns=recent_turns,
        mode=mode,
        max_tokens=max_tokens,
        timeout_s=timeout_s,
    )


def warm_up() -> Dict[str, Any]:
    return get_singleton().warm_up()


def report() -> Dict[str, Any]:
    return get_singleton().details()


__all__ = [
    "DEFAULT_MODEL_NAME", "DEFAULT_MODEL_DIR",
    "generate_ack", "warm_up", "report",
    "get_singleton", "reset_singleton",
]
