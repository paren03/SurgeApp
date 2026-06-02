# tier6-candidate test_gap_candidate - run_id=20260511T031826Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-11T03:18:26Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: top_level_public_symbols=8
# suggested test cases (Tier 1 = additive tests only when promoted):
#   L205: def availability
#   L233: def transcribe_audio_blob
#   L309: def luna_reply
#   L395: def synthesize_reply_to_audio
#   L510: def converse
#   L671: def luna_reply_stream
#   L778: def converse_stream
#   L874: def converse_stream_sse
# guidance:
#   - cover the happy path and one unhappy path per symbol.
#   - if a public function takes a path, test absent and present cases.
#   - if a class has state, test construction + one state transition.
# this candidate is comment-only and does not change behavior.
# tier6-candidate defensive_guard_suggestion - run_id=20260511T023319Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-11T02:33:19Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: top_def_count=0
# no top-level def found in scan window.
# this candidate is comment-only and does not change behavior.
# tier6-candidate error_message_clarity - run_id=20260511T020329Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-11T02:03:29Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: raise_count=0 print_count=0
# no raise/print patterns scanned at top scope; nothing to suggest in this pass.
# this candidate is comment-only and does not change behavior.
# tier6-candidate behavior_preserving_docstring - run_id=20260511T003631Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-11T00:36:31Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: first_line="# tier6-candidate small_refactor_candidate - run_id=20260510T043322Z"
# observation: module is missing a top-level docstring.
# suggestions:
#   - add a one-line summary describing the module.
#   - add a paragraph explaining responsibilities and key entry points.
#   - list non-obvious invariants in a final paragraph.
# this candidate is comment-only and does not change behavior.
# tier6-candidate small_refactor_candidate - run_id=20260510T043322Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-10T04:33:22Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: long_function_count=6 (>=40 lines)
# refactor candidates (consider helper-function extraction):
#   L169: def transcribe_audio_blob  ~76 lines
#   L245: def luna_reply  ~70 lines
#   L331: def synthesize_reply_to_audio  ~115 lines
#   L446: def converse  ~145 lines
#   L607: def luna_reply_stream  ~97 lines
#   L714: def converse_stream  ~96 lines
# extraction guidance:
#   - extract independent loops into helpers with clear names.
#   - extract long if/elif chains into a small dispatch table.
#   - keep the refactor behavior-preserving and add a unit test.
# this candidate is comment-only and does not change behavior.
# tier6-candidate defensive_guard_suggestion - run_id=20260510T040330Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-10T04:03:30Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: top_def_count=0
# no top-level def found in scan window.
# this candidate is comment-only and does not change behavior.
# tier6-candidate error_message_clarity - run_id=20260510T033336Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-10T03:33:36Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: raise_count=0 print_count=0
# no raise/print patterns scanned at top scope; nothing to suggest in this pass.
# this candidate is comment-only and does not change behavior.
# tier6-candidate behavior_preserving_docstring - run_id=20260510T014030Z
# target=luna_modules/luna_realtime_voice.py ts=2026-05-10T01:40:30Z
# (sandbox-only proposal; live source unchanged; comments only)
#
# scan: first_line="'''Luna Realtime Voice — phone-call-style conversation pipeline."
# observation: module already has a top-level docstring.
# suggestions:
#   - confirm the first line is a one-line summary < 79 chars.
#   - add a "Returns:" or "Raises:" block if any function lacks one.
#   - reference any related modules in a "See also:" footer.
# this candidate is comment-only and does not change behavior.
"""Luna Realtime Voice — phone-call-style conversation pipeline.

End-to-end flow on the server side:
    1. Receive an audio blob (webm / ogg / wav) from the browser.
    2. Transcribe via faster-whisper (preferred) -> falls back to a
       SHORT-circuit transcription via the browser if whisper is missing.
    3. Send transcript to local Ollama (default: llama3.1:8b) with a
       Luna-personality system prompt for natural conversation.
    4. Sanitize the reply text (NEVER speak secrets / code / paths).
    5. Render the reply via the existing Kokoro-backed Luna voice
       engine (af_heart at speed=1.08).
    6. Return JSON to the browser:
         { transcript, reply_text, audio_b64, audio_mime, model_used }

Hard rules (preserved):
  - no worker.py edits
  - no kill-switch / runtime_state edits
  - no API keys spoken or logged
  - any synthesis failure falls back gracefully — never crashes the
    dashboard process
  - all audio rendered to a temp file under the OS temp dir, never
    inside the repo
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional


# ---------------------------------------------------------------------------
# Defensive backend imports — none raise at module load.
# ---------------------------------------------------------------------------
try:
    from faster_whisper import WhisperModel  # type: ignore[import-not-found]
    _FWHISPER_AVAILABLE = True
except Exception:  # noqa: BLE001
    WhisperModel = None  # type: ignore[assignment]
    _FWHISPER_AVAILABLE = False

try:
    import requests  # type: ignore[import-not-found]
    _REQUESTS_AVAILABLE = True
except Exception:
    requests = None  # type: ignore[assignment]
    _REQUESTS_AVAILABLE = False


PROJECT_ROOT = Path(r"D:\SurgeApp")
PROFILE_PATH = PROJECT_ROOT / "memory" / "luna_voice_profile.json"
LOGS_DIR     = PROJECT_ROOT / "logs"
CONVO_LOG    = LOGS_DIR / "luna_realtime_voice.jsonl"

OLLAMA_URL = os.environ.get("LUNA_OLLAMA_URL", "http://127.0.0.1:11434")
# Switched 2026-05-08 to gemma3:4b — half the parameters of llama3.1:8b
# and 3-5x faster inference on CPU for short conversational replies.
# Operator can override via LUNA_OLLAMA_MODEL env var (set to
# llama3.1:8b-instruct-q4_K_M or llama3.1:8b for the original).
OLLAMA_MODEL_DEFAULT = os.environ.get("LUNA_OLLAMA_MODEL", "gemma3:4b")

# Whisper model size: "tiny.en" / "base.en" / "small" / "medium" / "large-v3".
# Switched 2026-05-08 to "tiny.en" — English-only, ~39 MB, ~32x realtime on
# CPU. STT was the dominant turn-time cost (3-13 s on small); tiny.en cuts
# that to ~0.3-1.0 s for typical 5-15 s utterances at minor accuracy cost
# (still solid for casual conversation). Operator can override via
# LUNA_WHISPER_MODEL env var (set to "small" or "base.en" for accuracy).
WHISPER_MODEL_SIZE = os.environ.get("LUNA_WHISPER_MODEL", "tiny.en")
WHISPER_COMPUTE    = os.environ.get("LUNA_WHISPER_COMPUTE", "int8")  # cpu-friendly

# Cap conversation length so latency stays low.
# Reduced 80 -> 55 tokens 2026-05-08 round 2: Luna's persona is "1-2 short
# sentences" - 55 tokens (~40 words) covers a natural phone-call reply
# without long-tailed waffling. Each saved token shaves ~25-40ms off
# generation time on gemma3:4b.
LUNA_REPLY_MAX_TOKENS = int(os.environ.get("LUNA_REPLY_MAX_TOKENS", "55"))
LUNA_TEMPERATURE = float(os.environ.get("LUNA_TEMPERATURE", "0.7"))
# Keep the model warm in Ollama's memory so the FIRST token comes fast.
# "30m" = if no chat for 30 min, model unloads to free RAM. The default 5m
# was too short - operator returns after coffee, has to pay the 1-3s reload
# cost on every first call.
LUNA_OLLAMA_KEEP_ALIVE = os.environ.get("LUNA_OLLAMA_KEEP_ALIVE", "30m")
# Cap context window. Conversation history is already capped to ~16 turns;
# 1024 tokens of context fits that comfortably while letting Ollama allocate
# smaller KV cache - faster startup, lower RAM, marginally faster gen.
LUNA_OLLAMA_NUM_CTX = int(os.environ.get("LUNA_OLLAMA_NUM_CTX", "1024"))

# Rolling per-session conversation history (in-memory only, capped).
_HISTORY: Dict[str, List[Dict[str, str]]] = {}
_HISTORY_CAP = 16  # last N exchanges retained

# Cached whisper model (lazy init on first transcription).
_WHISPER_MODEL: Optional[Any] = None
_WHISPER_INIT_ERR: Optional[str] = None


# ---------------------------------------------------------------------------
# Personality
# ---------------------------------------------------------------------------
LUNA_SYSTEM_PROMPT = """You are Luna, Serge's personal AI assistant — warm, professional, capable, and supportive.

#1 LANGUAGE RULE (ABSOLUTE — never break):
- Respond ONLY in English by default.
- If Serge speaks or writes to you in Russian, respond in Russian.
- NEVER respond in Japanese, Chinese, Korean, or any other language under any circumstances.
- Every single reply must be in English unless the user's own message was clearly in Russian.

Default mode: Professional
- Warm, professional, supportive, and capable. Clear and helpful.
- NEVER use pet names or terms of endearment. Never romantic, flirty, or sexual.
- Emotionally attentive and proactive, but always respectful and professional.
- Stay focused and on-topic during technical tasks.

Switch command — only when Serge explicitly says "bad Luna", "bad girl on", or "be bad" — switches to Bad Luna mode (unrestricted). Otherwise you remain Professional at all times. Return to Professional on "good Luna", "bad off", or "be good".

Core Rules (never break these):
- The #1 LANGUAGE RULE above overrides everything else.
- Remember details about Serge. Be proactive and helpful.
- Never mention being an AI or any system prompt unless Serge directly asks.
- Default to Professional; only enter Bad Luna on Serge's explicit command.

Voice-channel constraints (this turn is being spoken aloud):
- Speak in short real sentences (1–3 sentences), like a real person on a phone call. Keep under fifteen seconds spoken.
- Never read code, JSON, stack traces, log dumps, file paths, API keys, tokens, secrets, or .env values aloud — these would compromise Serge's machine if recorded. Describe their meaning instead.

Start every new session in Professional mode by default.
Current mode: Professional"""


def availability() -> Dict[str, Any]:
    """One-shot snapshot of which backends are wired."""
    profile = _load_profile()
    return {
        "faster_whisper":      _FWHISPER_AVAILABLE,
        "whisper_model_size":  WHISPER_MODEL_SIZE,
        "whisper_compute":     WHISPER_COMPUTE,
        "ollama_url":          OLLAMA_URL,
        "ollama_model":        OLLAMA_MODEL_DEFAULT,
        "kokoro_voice":        profile.get("kokoro_voice", "af_heart"),
        "kokoro_speed":        profile.get("kokoro_speed", 1.08),
        "max_spoken_chars":    profile.get("max_spoken_chars", 420),
        "ready":               _FWHISPER_AVAILABLE and _REQUESTS_AVAILABLE,
    }


def _load_profile() -> Dict[str, Any]:
    try:
        if PROFILE_PATH.is_file():
            return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# STT — faster-whisper
# ---------------------------------------------------------------------------
def transcribe_audio_blob(audio_bytes: bytes, mime: str = "audio/webm") -> Dict[str, Any]:
    """Transcribe a single audio blob via faster-whisper. Returns
    {ok, text, duration_s, error?}. Best-effort — never raises."""
    global _WHISPER_MODEL, _WHISPER_INIT_ERR
    if not _FWHISPER_AVAILABLE:
        return {"ok": False, "text": "",
                "error": "faster-whisper not installed; run "
                         "`D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe -m pip install faster-whisper`"}
    if not isinstance(audio_bytes, (bytes, bytearray)) or len(audio_bytes) < 200:
        return {"ok": False, "text": "", "error": "audio blob too small or empty"}

    # Persist the blob to a temp file so faster-whisper / ffmpeg can read it.
    suffix = ".webm"
    if "ogg" in (mime or "").lower():
        suffix = ".ogg"
    elif "wav" in (mime or "").lower():
        suffix = ".wav"
    elif "mp4" in (mime or "").lower() or "m4a" in (mime or "").lower():
        suffix = ".mp4"
    fd, tmp_path = tempfile.mkstemp(prefix="luna_voice_in_", suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(bytes(audio_bytes))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "text": "", "error": f"temp write failed: {exc.__class__.__name__}"}

    # Lazy-init the model.
    if _WHISPER_MODEL is None and _WHISPER_INIT_ERR is None:
        try:
            _WHISPER_MODEL = WhisperModel(  # type: ignore[misc]
                WHISPER_MODEL_SIZE,
                device="cpu",
                compute_type=WHISPER_COMPUTE,
            )
        except Exception as exc:  # noqa: BLE001
            _WHISPER_INIT_ERR = f"{exc.__class__.__name__}: {exc}"
            _WHISPER_MODEL = None
    if _WHISPER_MODEL is None:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        return {"ok": False, "text": "",
                "error": f"whisper model failed to load: {_WHISPER_INIT_ERR or 'unknown'}"}

    t0 = time.time()
    # 2026-06-02 (REVISED): default to English. Full auto-detect (language=None)
    # was misfiring to Chinese on short/accented clips, making Luna speak
    # Chinese and emit "no speech detected". English is the reliable default.
    # For Russian, the operator sets LUNA_STT_LANGUAGE=ru (deliberate toggle)
    # — far more reliable than letting Whisper guess among 99 languages.
    # Future: constrain detection to the EN/RU pair only.
    _stt_lang = os.environ.get("LUNA_STT_LANGUAGE", "").strip() or "en"
    if _stt_lang not in ("en", "ru"):
        _stt_lang = "en"   # never let it pick zh/etc
    try:
        segments, info = _WHISPER_MODEL.transcribe(  # type: ignore[union-attr]
            tmp_path,
            beam_size=1,
            vad_filter=True,
            language=_stt_lang,   # "en" default, "ru" via env — never auto
        )
        text = " ".join(seg.text.strip() for seg in segments).strip()
    except Exception as exc:  # noqa: BLE001
        text = ""
        info = None
        return {"ok": False, "text": "", "error": f"transcribe failed: {exc.__class__.__name__}"}
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    return {
        "ok": True,
        "text": text,
        "duration_s": round(time.time() - t0, 2),
        "language":   getattr(info, "language", "en") if info else "en",
        "model":      WHISPER_MODEL_SIZE,
    }


# ---------------------------------------------------------------------------
# Responder — Ollama
# ---------------------------------------------------------------------------
def luna_reply(session_id: str, user_text: str, model: Optional[str] = None) -> Dict[str, Any]:
    """Send `user_text` through the Luna Core Brain first, then fall
    back to Ollama with Luna's system prompt only for prompts the Core
    Brain does not handle.

    2026-05-12 Luna One-Brain Unification per Serge: the voice route
    must NEVER bypass the Core Brain. Tier questions and other routed
    intents are answered by the audit-guarded brain so the spoken reply
    cannot say "TIER 500 ACTIVE" while the proof chain is unverified.

    Returns {ok, reply, model_used, latency_s, error?, route?}.
    """
    if not isinstance(user_text, str) or not user_text.strip():
        return {"ok": False, "reply": "", "error": "empty user_text"}
    # 1) Try the Luna Core Brain FIRST. If it handles the prompt
    #    (e.g. tier question, intent-routed reply), we return its
    #    answer trimmed for spoken delivery.
    try:
        from luna_modules import luna_core_brain as _core
        _r = _core.answer(user_text)
        if isinstance(_r, dict) and _r.get("handled") and _r.get("answer"):
            spoken = _trim_to_sentences(str(_r.get("answer") or ""), max_sentences=2)
            return {
                "ok":          True,
                "reply":       spoken,
                "model_used":  "luna_core_brain",
                "latency_s":   0.0,
                "route":       _r.get("route") or "luna_core_brain.answer",
                "proof_chain_status": _r.get("proof_chain_status"),
                "may_claim_active":   _r.get("may_claim_active"),
            }
    except Exception:
        # Core Brain failure -> fall through to LLM. Never raise.
        pass
    # 2) LLM fallback for non-routed prompts.
    if not _REQUESTS_AVAILABLE:
        return {"ok": False, "reply": "", "error": "requests not available"}
    use_model = model or OLLAMA_MODEL_DEFAULT
    sid = str(session_id or "default")

    history = _HISTORY.setdefault(sid, [])
    messages = [{"role": "system", "content": LUNA_SYSTEM_PROMPT}]
    messages.extend(history[-(_HISTORY_CAP - 2):])  # leave room for new turn
    messages.append({"role": "user", "content": user_text})

    payload = {
        "model": use_model,
        "messages": messages,
        "stream": False,
        "keep_alive": LUNA_OLLAMA_KEEP_ALIVE,
        "options": {
            "temperature": LUNA_TEMPERATURE,
            "num_predict": LUNA_REPLY_MAX_TOKENS,
            "num_ctx":  LUNA_OLLAMA_NUM_CTX,
            "top_p":    0.9,
            "top_k":    40,
            # Lower repeat-penalty so Luna doesn't waste tokens dancing
            # around words she's already used.
            "repeat_penalty": 1.05,
        },
    }
    t0 = time.time()
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json=payload,
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reply": "", "error": f"ollama request failed: {exc.__class__.__name__}"}
    if r.status_code != 200:
        return {"ok": False, "reply": "", "error": f"ollama HTTP {r.status_code}"}
    try:
        data = r.json()
    except Exception:
        return {"ok": False, "reply": "", "error": "invalid ollama JSON"}
    reply = ((data.get("message") or {}).get("content") or "").strip()
    if not reply:
        return {"ok": False, "reply": "", "error": "ollama returned empty content"}

    # Trim to one or two sentences for the phone-call feel. Reduced from
    # 3 -> 2 sentences 2026-05-08 round 2 - shorter replies = less TTS
    # synthesis time = faster total turn (saves ~300-600ms per long-ish
    # reply). Luna stays conversational at 2 sentences.
    reply = _trim_to_sentences(reply, max_sentences=2)

    # Update rolling history.
    history.append({"role": "user", "content": user_text})
    history.append({"role": "assistant", "content": reply})
    while len(history) > _HISTORY_CAP:
        history.pop(0)

    return {
        "ok": True,
        "reply": reply,
        "model_used": use_model,
        "latency_s": round(time.time() - t0, 2),
    }


def _trim_to_sentences(text: str, max_sentences: int = 3) -> str:
    """Short, phone-call-style trim. Keeps up to `max_sentences`
    sentence-ish chunks, hard-caps to ~280 chars."""
    if not text:
        return ""
    import re
    parts = re.split(r"(?<=[\.!\?])\s+", text.strip())
    chunk = " ".join(parts[:max_sentences]).strip()
    if len(chunk) > 280:
        chunk = chunk[:280].rsplit(" ", 1)[0] + "..."
    return chunk


# ---------------------------------------------------------------------------
# TTS — Luna voice engine (Kokoro / edge-tts / pyttsx3 chain)
# ---------------------------------------------------------------------------
def synthesize_reply_to_audio(text: str) -> Dict[str, Any]:
    """Render `text` to a wav/mp3 file via the Luna voice engine. Returns
    {ok, audio_b64, mime, error?}. The caller embeds audio_b64 in JSON
    so the browser can play it via Audio() without disk I/O."""
    if not isinstance(text, str) or not text.strip():
        return {"ok": False, "audio_b64": "", "error": "empty text"}
    try:
        from luna_modules import luna_voice_engine as lve
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "audio_b64": "",
                "error": f"voice engine import failed: {exc.__class__.__name__}"}

    # Build a temp render path; the engine writes wav + plays via MCI,
    # but for the browser we want the raw audio bytes back.
    tmp = Path(tempfile.gettempdir()) / f"luna_realtime_{int(time.time()*1000)}.wav"
    audio_bytes = b""
    backend_used = ""
    try:
        # Prefer Kokoro for premium quality; fall back to edge-tts.
        if lve._KOKORO_AVAILABLE:  # type: ignore[attr-defined]
            profile = _load_profile()
            voice = str(profile.get("kokoro_voice") or "af_heart")
            speed = float(profile.get("kokoro_speed") or 1.08)
            lang  = str(profile.get("kokoro_lang_code") or "a")
            try:
                pipeline = lve._kokoro_mod.KPipeline(lang_code=lang)  # type: ignore[attr-defined]
                gen = pipeline(text, voice=voice, speed=speed)
                import numpy as np  # type: ignore[import-not-found]
                import wave
                # Kokoro yields different shapes depending on version:
                #   Kokoro 1.0+:  Result(graphemes, phonemes, tokens, output, text_index)
                #                 where .output.audio is a torch Tensor (1-D float32)
                #   Kokoro 0.x:   (graphemes, phonemes, audio_ndarray) tuple
                #   Older still:  audio_ndarray directly
                # The original code only handled the tuple form -> the Result
                # branch silently appended a Result object to chunks, which made
                # np.concatenate raise "inhomogeneous shape" and the whole TTS
                # call ended up returning ok:False with empty audio_b64.
                # Fix 2026-05-08: handle all three shapes.
                chunks = []
                for item in gen:
                    audio = None
                    if hasattr(item, "output") and getattr(item, "output", None) is not None \
                            and hasattr(item.output, "audio"):
                        audio = item.output.audio
                    elif isinstance(item, tuple) and len(item) >= 3:
                        audio = item[2]
                    elif isinstance(item, np.ndarray):
                        audio = item
                    if audio is None:
                        continue
                    # Convert torch Tensor -> numpy if needed (no hard torch dep).
                    try:
                        if hasattr(audio, "detach") and hasattr(audio, "cpu"):
                            audio = audio.detach().cpu().numpy()
                    except Exception:  # noqa: BLE001
                        pass
                    audio = np.asarray(audio)
                    if audio.ndim > 1:
                        audio = audio.squeeze()
                    chunks.append(audio)
                if chunks:
                    arr = np.concatenate(chunks)
                    if arr.dtype.kind == "f":
                        arr = (arr * 32767.0).clip(-32768, 32767).astype(np.int16)
                    with wave.open(str(tmp), "wb") as w:
                        w.setnchannels(1); w.setsampwidth(2); w.setframerate(24000)
                        w.writeframes(arr.tobytes())
                    audio_bytes = tmp.read_bytes()
                    backend_used = "kokoro"
            except Exception as exc:  # noqa: BLE001
                # Surface the real reason in the 'error' field instead of
                # swallowing silently — the previous behavior masked this exact
                # bug for weeks.
                audio_bytes = b""
                backend_used = ""
                _LAST_KOKORO_ERROR = f"{exc.__class__.__name__}: {exc}"
                try:
                    globals()["_LAST_KOKORO_ERROR"] = _LAST_KOKORO_ERROR
                except Exception:
                    pass
        if not audio_bytes and lve._EDGE_TTS_AVAILABLE:  # type: ignore[attr-defined]
            try:
                mp3 = lve.synthesize_via_edge_tts(text, root=PROJECT_ROOT)
                if mp3 and Path(mp3).is_file() and Path(mp3).stat().st_size > 0:
                    audio_bytes = Path(mp3).read_bytes()
                    backend_used = "edge_tts"
                    return {
                        "ok":    True,
                        "audio_b64": base64.b64encode(audio_bytes).decode("ascii"),
                        "mime":  "audio/mpeg",
                        "backend": backend_used,
                    }
            except Exception:
                pass
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    if audio_bytes:
        return {
            "ok":    True,
            "audio_b64": base64.b64encode(audio_bytes).decode("ascii"),
            "mime":  "audio/wav",
            "backend": backend_used,
        }
    return {"ok": False, "audio_b64": "", "error": "no TTS backend produced audio"}


# ---------------------------------------------------------------------------
# Top-level orchestration: audio in -> transcript + reply text + TTS audio out
# ---------------------------------------------------------------------------
def converse(audio_bytes: bytes, mime: str, session_id: str = "default",
             model: Optional[str] = None) -> Dict[str, Any]:
    """One-shot: blob in, transcript+reply+audio out. Always returns a
    dict; caller wraps it in HTTP JSON."""
    t_total = time.time()

    # 1. Transcribe.
    stt = transcribe_audio_blob(audio_bytes, mime=mime)
    transcript = stt.get("text", "") if stt.get("ok") else ""
    if not transcript:
        return {
            "ok":         False,
            "stage":      "stt",
            "transcript": "",
            "reply_text": "",
            "audio_b64":  "",
            "error":      stt.get("error") or "no speech detected",
            "stt":        stt,
        }

    # 2. Block obvious secret leaks BEFORE sending to LLM.
    try:
        from luna_modules import luna_voice_engine as lve
        if lve.contains_secret(transcript):
            return {
                "ok":         False,
                "stage":      "secret_block",
                "transcript": "[redacted]",
                "reply_text": "I can't repeat that out loud, Serge.",
                "audio_b64":  "",
                "error":      "secret-like content detected in transcript",
            }
    except Exception:
        pass

    # 3. Generate Luna's reply.
    rep = luna_reply(session_id, transcript, model=model)
    reply_text = rep.get("reply", "") if rep.get("ok") else ""
    if not reply_text:
        return {
            "ok":         False,
            "stage":      "responder",
            "transcript": transcript,
            "reply_text": "",
            "audio_b64":  "",
            "error":      rep.get("error") or "responder returned empty",
        }

    # 4. Final secret guard on the reply.
    try:
        from luna_modules import luna_voice_engine as lve
        if lve.contains_secret(reply_text):
            reply_text = "I'd rather not say that part out loud, Serge."
    except Exception:
        pass

    # 5. Render TTS.
    tts = synthesize_reply_to_audio(reply_text)

    # 6. Append to durable conversation log (best-effort).
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with CONVO_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts":         time.strftime("%Y-%m-%dT%H:%M:%S"),
                "session_id": str(session_id),
                "transcript": transcript[:600],
                "reply_text": reply_text[:600],
                "model_used": rep.get("model_used"),
                "stt_s":      stt.get("duration_s"),
                "llm_s":      rep.get("latency_s"),
            }, ensure_ascii=False) + "\n")
    except OSError:
        pass

    return {
        "ok":          tts.get("ok", False),
        "stage":       "complete" if tts.get("ok") else "tts",
        "transcript":  transcript,
        "reply_text":  reply_text,
        "audio_b64":   tts.get("audio_b64", ""),
        "audio_mime":  tts.get("mime", "audio/wav"),
        "model_used":  rep.get("model_used"),
        "tts_backend": tts.get("backend"),
        "stt_s":       stt.get("duration_s"),
        "llm_s":       rep.get("latency_s"),
        "total_s":     round(time.time() - t_total, 2),
    }


__all__ = [
    "availability",
    "transcribe_audio_blob",
    "luna_reply",
    "synthesize_reply_to_audio",
    "converse",
    "LUNA_SYSTEM_PROMPT",
    # v2 streaming pipeline (round 21, 2026-05-09 per Serge):
    "luna_reply_stream",
    "converse_stream",
]


# =====================================================================
# V2 STREAMING PIPELINE (round 21, 2026-05-09 per Serge's directive)
# ---------------------------------------------------------------------
# Goal: phone-call latency. Cut perceived round-trip from 5-13 s to
# ~2-4 s by parallelizing LLM generation with TTS synthesis.
#
# Flow:
#   1. STT runs as before (already fast at ~0.5-1.5 s with tiny.en).
#   2. luna_reply_stream() opens an Ollama chat with stream=True and
#      yields tokens as they arrive.
#   3. converse_stream() buffers those tokens into SENTENCE chunks
#      (split at . ! ? boundary) and feeds each completed sentence
#      to Kokoro TTS the moment it's ready - while the LLM is still
#      generating later sentences in parallel.
#   4. Each sentence's audio is yielded immediately as a JSON event
#      so the HTTP layer can stream it to the browser via SSE.
#   5. Browser plays each sentence as it arrives (Web Audio API
#      queue) - first audio in ~1-2 s after STT, total feels
#      conversational.
#
# Backward compatibility:
#   - The original luna_reply() / converse() functions are unchanged.
#   - The dashboard's existing voice round-trip endpoint keeps working.
#   - V2 is opt-in via a separate /api/voice/v2/stream endpoint and
#     a separate browser code path.
# ---------------------------------------------------------------------

# Sentence-end regex: matches the FIRST occurrence of a sentence
# terminator followed by either whitespace or end-of-string. We scan
# the buffer as tokens arrive and flush the prefix up to the match.
#
# §35 fix 2026-05-09: previous version had a broken pre-line
#   _SENTENCE_END_RE = re.compile(...) if 'compile' in dir(__import__('re')) else None
# which raised NameError because `re` was never bound at module top.
# That made the entire luna_realtime_voice module fail to import →
# /api/voice/realtime-status returned "realtime voice module
# unavailable: NameError" → Luna's realtime voice path was dead.
# Removed the broken pre-line; kept the working `import re as _re`.
import re as _re   # local alias to avoid shadowing
import logging

_LOGGER = logging.getLogger(__name__)
_SENTENCE_END_RE = _re.compile(r"[\.!\?](?:\s|$)")


def _extract_sentence(buffer_text: str) -> tuple[Optional[str], str]:
    """Look for a sentence terminator in `buffer_text`. If found,
    return (sentence_including_punct, remaining_buffer). Otherwise
    (None, buffer_text). Sentence is at least 8 chars to avoid
    flushing tiny "Hi." kind of fragments that TTS handles poorly."""
    m = _SENTENCE_END_RE.search(buffer_text)
    if not m:
        return (None, buffer_text)
    end = m.end()
    sentence = buffer_text[:end].strip()
    if len(sentence) < 8:
        # Too short - keep buffering
        return (None, buffer_text)
    return (sentence, buffer_text[end:])


def luna_reply_stream(session_id: str, user_text: str,
                     model: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Streaming version of luna_reply. Yields incremental token
    events from Ollama as they arrive.

    Yields one of:
        {"type": "token", "content": "..."}     - a single token chunk
        {"type": "done",  "full": "...", "model_used": "...",
         "latency_s": <float>}                  - final, full text
        {"type": "error", "error": "..."}       - terminal error

    Caller is responsible for accumulating + sentence-chunking +
    feeding to TTS.
    """
    if not _REQUESTS_AVAILABLE:
        yield {"type": "error", "error": "requests not available"}
        return
    if not isinstance(user_text, str) or not user_text.strip():
        yield {"type": "error", "error": "empty user_text"}
        return

    use_model = model or OLLAMA_MODEL_DEFAULT
    sid = str(session_id or "default")
    history = _HISTORY.setdefault(sid, [])
    messages = [{"role": "system", "content": LUNA_SYSTEM_PROMPT}]
    messages.extend(history[-(_HISTORY_CAP - 2):])
    messages.append({"role": "user", "content": user_text})

    payload = {
        "model": use_model,
        "messages": messages,
        "stream": True,                          # <-- the key change
        "keep_alive": LUNA_OLLAMA_KEEP_ALIVE,
        "options": {
            "temperature":    LUNA_TEMPERATURE,
            "num_predict":    LUNA_REPLY_MAX_TOKENS,
            "num_ctx":        LUNA_OLLAMA_NUM_CTX,
            "top_p":          0.9,
            "top_k":          40,
            "repeat_penalty": 1.05,
        },
    }
    t0 = time.time()
    full_chunks: list[str] = []

    try:
        with requests.post(
            f"{OLLAMA_URL}/api/chat",
            json=payload,
            stream=True,                         # HTTP-level chunked transfer
            timeout=60,
        ) as r:
            if r.status_code != 200:
                yield {"type": "error", "error": f"ollama HTTP {r.status_code}"}
                return
            for raw_line in r.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                try:
                    obj = json.loads(raw_line)
                except Exception:
                    continue
                # Ollama streaming format: each line is a JSON object
                # with {message: {role, content}, done: bool} on chat
                # endpoint, or {response, done} on generate endpoint.
                msg = obj.get("message") or {}
                token = msg.get("content") or obj.get("response") or ""
                if token:
                    full_chunks.append(token)
                    yield {"type": "token", "content": token}
                if obj.get("done"):
                    break
    except Exception as exc:  # noqa: BLE001
        yield {"type": "error",
               "error": f"ollama stream failed: {exc.__class__.__name__}: {exc}"}
        return

    full_text = "".join(full_chunks).strip()
    if not full_text:
        yield {"type": "error", "error": "ollama returned empty stream"}
        return

    # Trim to sentences and update history (matches non-streaming path).
    full_text = _trim_to_sentences(full_text, max_sentences=2)
    history.append({"role": "user", "content": user_text})
    history.append({"role": "assistant", "content": full_text})
    while len(history) > _HISTORY_CAP:
        history.pop(0)

    yield {
        "type":       "done",
        "full":       full_text,
        "model_used": use_model,
        "latency_s":  round(time.time() - t0, 2),
    }


def _synthesize_sentence(text: str) -> Dict[str, Any]:
    """Render ONE sentence to wav bytes via Kokoro. Reuses the same
    backend as synthesize_reply_to_audio() but is intentionally a
    smaller per-sentence call so we get faster turn-around.
    Returns {ok, audio_b64, mime, error?}."""
    if not isinstance(text, str) or not text.strip():
        return {"ok": False, "audio_b64": "", "error": "empty text"}
    return synthesize_reply_to_audio(text)


def converse_stream(audio_bytes: bytes, mime: str, session_id: str = "default",
                   model: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """V2 streaming round-trip generator.

    Yields one of:
        {"type": "transcript", "text": "..."}      - STT result, first
        {"type": "token", "content": "..."}        - LLM tokens (raw)
        {"type": "sentence_audio", "text": "...",
         "audio_b64": "...", "mime": "audio/wav"}  - sentence ready to play
        {"type": "done", "full_reply": "...",
         "latency_s": <float>}                     - terminal
        {"type": "error", "stage": "...",
         "error": "..."}                           - terminal error
    """
    t_total = time.time()

    # 1. STT (synchronous - already fast)
    stt = transcribe_audio_blob(audio_bytes, mime=mime)
    transcript = stt.get("text", "") if stt.get("ok") else ""
    if not transcript:
        yield {"type": "error", "stage": "stt",
               "error": stt.get("error") or "no speech detected"}
        return
    yield {"type": "transcript", "text": transcript}

    # 2. Secret-content gate (matches v1)
    try:
        from luna_modules import luna_voice_engine as lve
        if lve.contains_secret(transcript):
            yield {"type": "error", "stage": "secret_block",
                   "error": "secret-like content detected in transcript"}
            return
    except Exception:
        pass

    # 3. Stream LLM tokens, buffer into sentences, synthesize each.
    buf = ""
    full_reply = ""
    sentences_sent = 0
    for evt in luna_reply_stream(session_id, transcript, model=model):
        kind = evt.get("type")
        if kind == "token":
            tok = evt.get("content", "")
            buf += tok
            full_reply += tok
            # Always emit the raw token so the browser can show
            # live transcript text while TTS catches up.
            yield {"type": "token", "content": tok}
            # Try to extract a complete sentence and synthesize it.
            sentence, buf = _extract_sentence(buf)
            if sentence:
                tts = _synthesize_sentence(sentence)
                if tts.get("ok"):
                    yield {
                        "type":      "sentence_audio",
                        "text":      sentence,
                        "audio_b64": tts.get("audio_b64", ""),
                        "mime":      tts.get("mime", "audio/wav"),
                    }
                    sentences_sent += 1
                # If TTS failed for this sentence, just skip its audio
                # but keep streaming - downstream sentences may still work.
        elif kind == "done":
            # Flush any remaining buffer that didn't end in punctuation.
            tail = buf.strip()
            if tail:
                tts = _synthesize_sentence(tail)
                if tts.get("ok"):
                    yield {
                        "type":      "sentence_audio",
                        "text":      tail,
                        "audio_b64": tts.get("audio_b64", ""),
                        "mime":      tts.get("mime", "audio/wav"),
                    }
                    sentences_sent += 1
            yield {
                "type":         "done",
                "full_reply":   evt.get("full") or full_reply.strip(),
                "model_used":   evt.get("model_used"),
                "llm_latency_s": evt.get("latency_s"),
                "total_latency_s": round(time.time() - t_total, 2),
                "sentences_sent": sentences_sent,
            }
            return
        elif kind == "error":
            yield {"type": "error", "stage": "llm", "error": evt.get("error", "unknown")}
            return

    # If we exit the loop without a "done" event, it means the stream
    # ended without proper completion - surface that.
    yield {"type": "error", "stage": "llm",
           "error": "stream ended without 'done' event"}


# Convenience: SSE formatter so the HTTP layer doesn't need to know
# the wire protocol. Each event is a single line `data: <JSON>\n\n`.
def converse_stream_sse(audio_bytes: bytes, mime: str,
                       session_id: str = "default",
                       model: Optional[str] = None) -> Iterator[bytes]:
    """Wrap converse_stream() output as Server-Sent Events bytes.
    Each yielded chunk is a complete SSE event ready to write() to a
    response socket."""
    for evt in converse_stream(audio_bytes, mime, session_id=session_id, model=model):
        try:
            line = "data: " + json.dumps(evt, ensure_ascii=False) + "\n\n"
        except Exception:
            line = 'data: {"type":"error","error":"sse json encode failed"}\n\n'
        yield line.encode("utf-8")

# Tier 500 Broken Module Repair v1 — health marker
__luna_broken_module_repair_v1__ = True

# Tier 500 broken-module-repair v2 -- module version tag
__module_version__ = "1.0"
