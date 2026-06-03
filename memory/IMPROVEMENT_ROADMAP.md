# Luna Improvement Roadmap — 2026-06-03

Research-backed improvements across the voice + tooling stack. Sources at bottom.

---

## ✅ DONE THIS SESSION

### 1. Offline LLM fallback — Luna never goes mute
**Problem:** `luna_claude_bridge.py` caught `APIConnectionError` but just returned
an error → Luna went silent whenever the cloud/internet/key was down.

**Fix (shipped):**
- New `luna_modules/luna_offline_fallback.py` — streams from local Ollama
  (`llama3.1:8b-instruct-q4_K_M`, auto-picks best installed model) over stdlib
  urllib, no new deps. EN/RU, one-sentence spoken style.
- Wired into all 3 bridge exception handlers (`APIConnectionError`,
  `AuthenticationError`, generic). Cloud fails → local model answers seamlessly.
- Verified: answered "What is OSHA?" cleanly via llama3.1 offline.

### 2. openWakeWord installed (building block)
- `pip install openwakeword` + models downloaded.
- Bundled wake words now available: **hey_jarvis_v0.1**, alexa, hey_mycroft,
  plus **silero_vad** (high-precision VAD) + melspectrogram/embedding models.

---

## 🟡 READY TO APPLY (building blocks installed, not yet wired)

### 3. Lower-latency wake word — swap Whisper-tiny → openWakeWord
**Current:** `luna_voice_listener.py` runs Whisper-tiny on every utterance to
detect "Luna" (heavier, ~slower, more CPU).
**Upgrade:** openWakeWord's `hey_jarvis_v0.1` runs continuously at <4% CPU,
more accurate than Porcupine on far-field speech, 15-20 models per RPi3 core.
**Note:** No pretrained "Luna" model exists — either use "Hey Jarvis", or train
a custom Luna model (openWakeWord trains tiny heads on synthetic speech, no manual
recording). NOT auto-applied because `luna_voice_listener.py` is actively edited by
the autonomous loop — apply when the loop is quiescent to avoid conflict.

### 4. Barge-in / interruption (the #1 Jarvis UX gap)
**Current:** Luna mutes the mic while speaking → you CAN'T interrupt her.
**Upgrade:** True barge-in = detect user speech while she talks → stop TTS
within ~200ms → switch to listening, keep context.
**Building block ready:** `silero_vad` (downloaded with openWakeWord) is the
real-time VAD this needs. Design (from research):
  - Run silero_vad on the mic stream EVEN while Luna speaks (don't fully mute)
  - AND-logic: VAD probability AND volume must both exceed threshold (reject echo)
  - Hysteresis: separate start/stop thresholds; require ~150ms sustained speech
  - On trigger: hard-stop the `_LunaEQStream` playback, drain TTS queue, listen
**Risk:** touches the active voice loop — apply carefully / with the loop paused.

### 5. Kokoro fast-TTS (already installed)
- `kokoro` is installed (82M, 96× realtime, <0.3s, CPU-capable).
- NO voice cloning → can't replace Luna's XTTS clone voice.
- Use case: a "fast mode" for non-personal output (reading long docs aloud) or
  a fallback if the GPU is busy. Keep XTTS as the default for her real voice.

---

## 🔵 RESEARCH NOTES (for later)

- **Local LLM models for the 2080 (8GB):** Qwen3-7B = best multilingual (strong
  Russian), Llama3.3-8B = best all-round, Mistral-Small-3 = fastest tok/s. All at
  Q4_K_M. Could `ollama pull qwen3` for a stronger EN/RU offline brain than llama3.1.
- **Top open TTS 2026:** Chatterbox-Turbo beat ElevenLabs 65% blind; Fish-Speech S2
  leads open benchmarks. XTTS-v2 still the cloning gold standard (what Luna uses).
- **Barge-in stack:** Silero VAD + voice isolation + AND-logic + hysteresis +
  pre-buffer. No single lib does it all — wire the pieces.

## Sources
- openWakeWord: github.com/dscripka/openWakeWord
- TTS comparison: promptquorum.com/power-local-llm/local-tts-voice-cloning-piper-coqui-xtts
- Barge-in: orga-ai.com/blog/blog-barge-in-voice-agents-guide · notch.cx/post/turn-detection-in-voice-ai
- Local LLM 8GB: sitepoint.com/best-local-llm-models-2026
