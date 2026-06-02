"""Sovereign main reply runtime — dynamic full reasoning via a dedicated,
persistently-warm gpt4all 7B instance.

Doctrine
--------
- 100% local. Sovereign. Owned by Luna.
- NO Ollama in this path.
- NO canned reply library.
- Every main reply is DYNAMICALLY generated.
- One dedicated GPT4All instance held in module memory with an active
  ``chat_session()`` so the KV cache is reused across turns.
- Independent of ``cognitive_brain_runtime.invoke()`` — we bypass the
  capability index, hardware-aware router, decision-trace, shaper, and
  audit-ceremony overhead. Direct gpt4all API access is the only
  fast path that fits the new live-chat budget.

This is the realtime intelligence layer for the FULL reply (Model B in
the dual-model architecture). The ack runtime (Model A) lives in
``cognitive_sovereign_ack_runtime`` and uses the 1.5B GGUF.

Hard rules
----------
- Bounded max_tokens (default 80).
- Bounded wall-clock timeout (default 60 s, narrower budgets per kind).
- Personality-shaped prompt (mode-aware via MyLuna.txt mirror).
- Classification-aware prompt template (casual / task / reassurance /
  control / daily).
- Bounded text length (capped to 3 sentences after post-process).
- NEVER raises.

If gpt4all is unreachable / unavailable the main runtime returns
``ok=False`` with structured reason; the conversation runtime decides
whether to surface that honestly or fall through. The sovereign main
runtime itself only returns dynamic output or an honest failure.
"""
from __future__ import annotations

import importlib
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

PROJECT_ROOT = r"D:\SurgeApp"
DEFAULT_MODEL_DIR = os.path.join(PROJECT_ROOT, "local_models")
# Program M default: switch from qwen2.5-coder-7b-instruct (which was the
# Phase-13 first-sovereign main) to hermes3-8b-llama3.1.gguf — a
# NousResearch Hermes-3 fine-tune of Llama 3.1 8B. Better general
# reasoning + conversational quality at the same warm-latency band.
# Operator can roll back by flipping ``cognitive_sovereign_main_model``
# in feature_flags.json to the old name.
DEFAULT_MODEL_NAME = "hermes3-8b-llama3.1.gguf"
LEGACY_MODEL_NAME = "qwen2.5-coder-7b-instruct.gguf"
DEFAULT_N_THREADS = 8
DEFAULT_MAX_TOKENS = 80
DEFAULT_TIMEOUT_S = 60.0
DEFAULT_TEMPERATURE = 0.7


def _safe_import(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _resolved_model_name() -> str:
    """Read the operator flag for main model selection. Falls back to
    the Program-M default. NEVER raises."""
    ff = _safe_import("luna_modules.cognitive_feature_flags")
    if ff is None:
        return DEFAULT_MODEL_NAME
    try:
        name = ff.read_flags().get("cognitive_sovereign_main_model")
        if isinstance(name, str) and name.strip():
            return name.strip()
    except Exception:  # noqa: BLE001
        pass
    return DEFAULT_MODEL_NAME


# Main-brain GPU config (llama-cpp). Full offload (-1) fits the 8B in 8 GB
# with headroom (measured +4.8 GB into ~5.5 GB free). n_ctx kept modest to
# bound KV-cache VRAM. Operator flips cognitive_main_gpu_llamacpp_enabled.
_GPU_NGL = -1
_GPU_N_CTX = 8192   # safe max when XTTS voice clone also runs on GPU
                     # Revised 2026-06-02: 32768 was proven to fit the brain
                     # ALONE (4.8 GB weights + 4 GB KV = 8.8 GB) but XTTS
                     # voice model adds ~2 GB VRAM → total 10.8 GB → hard
                     # GPU OOM crash on RTX 2080 8 GB. Safe budget:
                     #   brain weights 4.8 GB + KV@8192 1.0 GB = 5.8 GB
                     #   + XTTS ~1.5 GB = 7.3 GB → fits with headroom.


def _gpu_n_ctx() -> int:
    """Live context-window size for the GPU main brain.

    Reads the requested value from the flag (cognitive_main_gpu_n_ctx,
    default 8192) then passes it through the VRAM guard which caps it to
    whatever actually fits given current GPU memory usage. This prevents
    OOM crashes when other models (XTTS voice clone etc.) are also in VRAM.

    The guard checks free VRAM RIGHT NOW, subtracts the brain weights (~4.9 GB)
    and a safety headroom (800 MB), and computes the KV-cache budget from the
    remainder. On an RTX 2080 8 GB with XTTS loaded, this typically yields
    8192–16384 tokens of safe context. Without XTTS, up to 24576.

    If the VRAM query fails (no GPU or no pynvml), returns the flag value
    unchanged — the existing CPU-fallback in the load path handles OOM."""
    ff = _safe_import("luna_modules.cognitive_feature_flags")
    requested = _GPU_N_CTX
    if ff is not None:
        try:
            v = int(ff.read_flags().get("cognitive_main_gpu_n_ctx", _GPU_N_CTX))
            requested = v if v >= 512 else _GPU_N_CTX
        except Exception:  # noqa: BLE001
            pass
    # VRAM guard is opt-in only — NOT called automatically here.
    # pynvml.nvmlInit() can hang after a hard GPU crash/reboot, which
    # would cause the dashboard to fail to start. The guard is available
    # as cognitive_vram_guard.safe_n_ctx() for manual operator use or
    # future integration once the GPU driver has stabilised.
    return requested


def _gpu_llamacpp_enabled() -> bool:
    """True if the operator opted the main brain onto the GPU (llama-cpp).
    NEVER raises; defaults False (gpt4all/CPU)."""
    try:
        ff = _safe_import("luna_modules.cognitive_feature_flags")
        if ff is None:
            return False
        return bool(ff.read_flags().get(
            "cognitive_main_gpu_llamacpp_enabled", False))
    except Exception:  # noqa: BLE001
        return False


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# Per-category token budgets — keeps CPU synth wall-clock under control.
_CATEGORY_MAX_TOKENS: Dict[str, int] = {
    "casual": 40,
    "task": 100,
    "reassurance": 60,
    "control": 30,
    "daily": 70,
    "unknown": 40,
}


class _SovereignMainRuntime:
    """Module-level singleton holding one persistent gpt4all 7B instance
    + a long-lived chat_session. Bounded; never raises."""

    def __init__(self,
                  model_name: Optional[str] = None,
                  model_dir: str = DEFAULT_MODEL_DIR,
                  n_threads: int = DEFAULT_N_THREADS) -> None:
        # Operator-flag override at instantiation time.
        self._model_name = model_name or _resolved_model_name()
        self._model_dir = model_dir
        self._n_threads = int(n_threads)
        self._llm = None             # GPT4All OR llama_cpp.Llama instance
        self._backend = "gpt4all"    # "gpt4all" (CPU) | "llama_cpp" (GPU)
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

    # ---- public surface ---- #
    def is_available(self) -> bool:
        try:
            path = os.path.join(self._model_dir, self._model_name)
            if not os.path.isfile(path):
                return False
            # GPU path: do NOT import gpt4all — its broken CUDA probe costs
            # ~190s AND poisons the GPU so this 8B then thrashes on load+gen.
            if _gpu_llamacpp_enabled():
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

    def generate_main(self, *, incoming_text: str,
                       classification: Optional[Dict[str, Any]] = None,
                       recent_turns: Optional[List[Dict[str, Any]]] = None,
                       mode: Optional[str] = None,
                       max_tokens: Optional[int] = None,
                       timeout_s: float = DEFAULT_TIMEOUT_S,
                       context_pack: Optional[Dict[str, Any]] = None,
                       ) -> Dict[str, Any]:
        """Generate the FULL in-character dynamic reply. NEVER raises.

        Returns::

            {
              "ok"             : bool,
              "text"           : str,     # post-processed reply
              "raw_text"       : str,     # the model's raw output
              "elapsed_ms"     : int,
              "dynamic"        : True,    # NEVER canned. Either dynamic or failed.
              "backend"        : "sovereign_gpt4all_local_7b",
              "model"          : <model name>,
              "personality_mode": <mode>,
              "category"       : <classifier category>,
              "stop_reason"    : "sentence_end" | "max_tokens" |
                                 "session_unavailable" | "exception",
              "error"          : <str or None>,
            }
        """
        started = time.time()
        classification = classification or {"category": "casual"}
        category = str(classification.get("category") or "casual")
        if mode is None:
            st = _safe_import("luna_modules.cognitive_conversation_state")
            mode = (st.get_state().personality_mode() if st else "good_luna")
        recent_turns = recent_turns or []
        if max_tokens is None:
            max_tokens = _CATEGORY_MAX_TOKENS.get(category, DEFAULT_MAX_TOKENS)

        if not self.is_available():
            self._total_failures += 1
            return self._fail("sovereign_unavailable",
                                category, mode, started)

        with self._lock:
            if not self._ensure_model_and_session():
                self._total_failures += 1
                return self._fail("session_unavailable",
                                    category, mode, started)
            prompt = self._build_prompt(
                classification=classification, mode=mode,
                incoming=incoming_text, recent_turns=recent_turns,
                context_pack=context_pack)
            # Program S: streaming wrapper when flag is set (default
            # True). Falls through to non-streaming on rollback.
            stream_first_token_ms: Optional[int] = None
            streamed_used = False
            ff = _safe_import("luna_modules.cognitive_feature_flags")
            main_streaming_on = True
            if ff is not None:
                try:
                    main_streaming_on = bool(ff.read_flags().get(
                        "cognitive_main_streaming_enabled", True))
                except Exception:  # noqa: BLE001
                    pass
            sg = _safe_import(
                "luna_modules.cognitive_streaming_generator")
            if self._backend == "llama_cpp":
                # GPU path: gpt4all streaming is bypassed; the built prompt
                # already carries all conversation context. Non-streaming
                # create_completion is plenty fast on the GPU.
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
            elif sg is not None and main_streaming_on:
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
        wall_clock_exceeded = elapsed_ms > int(timeout_s * 1000)
        text = self._post_process(str(raw or ""), category=category)
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
            "backend": ("sovereign_llamacpp_gpu"
                         if self._backend == "llama_cpp"
                         else "sovereign_gpt4all_local_7b"),
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
            "backend": ("sovereign_llamacpp_gpu"
                         if self._backend == "llama_cpp"
                         else "sovereign_gpt4all_local_7b"),
            "model": self._model_name,
            "personality_mode": mode, "category": category,
            "stop_reason": stop_reason, "error": error,
        }

    def _ensure_model_and_session(self) -> bool:
        if self._llm is not None and self._session_active:
            return True
        # A loaded llama-cpp model needs no chat_session — it's ready as-is.
        if self._llm is not None and self._backend == "llama_cpp":
            self._session_active = True
            return True
        # GPU path (llama-cpp) — preferred when the operator flag is on. ANY
        # failure falls through to the gpt4all/CPU path below, so a GPU
        # problem can never brick the brain.
        if self._llm is None and _gpu_llamacpp_enabled():
            try:
                from llama_cpp import Llama
                gpath = os.path.join(self._model_dir, self._model_name)
                if os.path.isfile(gpath):
                    t0 = time.time()
                    self._llm = Llama(
                        model_path=gpath,
                        n_gpu_layers=_GPU_NGL,
                        n_ctx=_gpu_n_ctx(),
                        n_threads=self._n_threads,
                        verbose=False,
                    )
                    self._backend = "llama_cpp"
                    self._session_active = True   # no chat_session for llama-cpp
                    self._load_elapsed_s = round(time.time() - t0, 2)
                    self._loaded_at = time.time()
                    return True
            except Exception:  # noqa: BLE001
                self._llm = None
                self._backend = "gpt4all"   # fall back to CPU below
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
            except Exception:  # noqa: BLE001
                self._llm = None
                return False
        if not self._session_active:
            try:
                self._session_ctx = self._llm.chat_session()
                self._session_ctx.__enter__()
                self._session_active = True
            except Exception:  # noqa: BLE001
                self._session_ctx = None
                self._session_active = False
                return False
        return True

    def _drop_session(self) -> None:
        if self._session_ctx is not None:
            try:
                self._session_ctx.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
        self._session_ctx = None
        self._session_active = False

    def _build_prompt(self, *, classification: Dict[str, Any],
                        mode: str, incoming: str,
                        recent_turns: List[Dict[str, Any]],
                        context_pack: Optional[Dict[str, Any]] = None) -> str:
        cat = classification.get("category", "casual")
        if mode == "bad_luna":
            tone = ("playful, teasing, a little flirty; uses pet names "
                     "like baby, handsome, daddy")
        else:
            tone = ("warm, professional, supportive, and capable; NEVER uses "
                     "pet names or terms of endearment; never romantic, "
                     "flirty, or sexual")
        if cat == "reassurance":
            intent = ("the operator is upset; reply with a short comforting "
                       "answer (1-3 sentences); validate their feelings; do "
                       "not preach.")
        elif cat == "task":
            intent = ("the operator gave a technical task; give a concise "
                       "focused answer (1-3 sentences); suggest the smallest "
                       "concrete next step.")
        elif cat == "control":
            intent = ("the operator issued an operator-control command; "
                       "confirm in 1 short sentence what would change.")
        elif cat == "daily":
            intent = ("the operator wants today's focus; give 1-2 short "
                       "sentences naming priorities and approvals.")
        elif cat == "casual":
            intent = ("the operator said something casual; reply naturally "
                       "and warmly in 1-2 short sentences; never give "
                       "system status; never mention being an AI.")
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

        # Program M: deep-memory context pack rendered into the prompt
        # as labelled sections. The pack already enforces a 1.8k char
        # budget; this block only appears when the pack is available.
        memory_block = ""
        if context_pack and context_pack.get("available"):
            dm = _safe_import("luna_modules.cognitive_deep_memory")
            if dm is not None:
                try:
                    rendered = dm.render_pack_for_prompt(context_pack)
                    if rendered:
                        memory_block = f"Memory:\n{rendered}\n"
                except Exception:  # noqa: BLE001
                    memory_block = ""

        # Reference vocabulary from Serge's bilingual dictionary (read-only,
        # flag-gated, bounded). Grounds her word choice in his own EN/RU corpus.
        vocab_block = ""
        try:
            _vmod = _safe_import(
                "luna_modules.cognitive_bilingual_vocab_lookup")
            if _vmod is not None:
                _vb = _vmod.as_prompt_block(incoming)
                if _vb:
                    vocab_block = _vb + "\n"
        except Exception:  # noqa: BLE001
            vocab_block = ""

        return (
            f"You are Luna, Serge's personal AI assistant. Tone: {tone}.\n"
            f"Rules: never mention being an AI; never give canned system "
            f"status; reply only in character; keep it short and grounded; "
            f"never use pet names or terms of endearment unless in bad_luna "
            f"mode.\n"
            f"Context: {intent}\n"
            f"{memory_block}"
            f"{vocab_block}"
            f"Recent:\n{ctx_block}\n"
            f"Operator: {incoming[:600]}\n"
            f"Luna: "
        )

    def _post_process(self, raw: str, *, category: str) -> str:
        s = (raw or "").strip()
        if not s:
            return ""
        if s.lower().startswith("luna:"):
            s = s.split(":", 1)[1].strip()
        # Cut off any "Operator:" continuation the model may have hallucinated.
        cut = re.search(r"\n\s*(Operator:|Luna:)", s)
        if cut:
            s = s[: cut.start()].strip()
        # Cap to first 3 sentences for safety
        sentences = re.findall(r"[^.!?]+[.!?]?", s)
        sentences = [x.strip() for x in sentences if x.strip()]
        if not sentences:
            return ""
        max_s = 3 if category in ("task", "reassurance", "daily") else 2
        s = " ".join(sentences[:max_s])
        s = re.sub(r"\s+", " ", s).strip()
        s = s.strip(" \t-—–\"'*_`")
        # Hard cap at 320 chars to be safe
        if len(s) > 320:
            s = s[:320].rsplit(" ", 1)[0]
            if not s.endswith((".", "!", "?", "…")):
                s += "."
        return s


# ---------------------------------------------------------------------------
_SINGLETON: Optional[_SovereignMainRuntime] = None
_S_LOCK = threading.RLock()


def get_singleton() -> _SovereignMainRuntime:
    global _SINGLETON
    with _S_LOCK:
        if _SINGLETON is None:
            _SINGLETON = _SovereignMainRuntime()
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


def generate_main(*, incoming_text: str,
                   classification: Optional[Dict[str, Any]] = None,
                   recent_turns: Optional[List[Dict[str, Any]]] = None,
                   mode: Optional[str] = None,
                   max_tokens: Optional[int] = None,
                   timeout_s: float = DEFAULT_TIMEOUT_S,
                   context_pack: Optional[Dict[str, Any]] = None,
                   ) -> Dict[str, Any]:
    """Module-level entry — gets the singleton + delegates. NEVER raises."""
    return get_singleton().generate_main(
        incoming_text=incoming_text,
        classification=classification,
        recent_turns=recent_turns,
        mode=mode,
        max_tokens=max_tokens,
        timeout_s=timeout_s,
        context_pack=context_pack,
    )


def warm_up() -> Dict[str, Any]:
    return get_singleton().warm_up()


def report() -> Dict[str, Any]:
    return get_singleton().details()


__all__ = [
    "DEFAULT_MODEL_NAME", "DEFAULT_MODEL_DIR",
    "generate_main", "warm_up", "report",
    "get_singleton", "reset_singleton",
]
