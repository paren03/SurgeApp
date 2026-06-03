"""
luna_voice_listener.py — Always-on microphone listener for Luna Jarvis mode

Pipeline:
  Mic → WebRTC VAD (ultra-low CPU) → Whisper tiny (wake word check)
  → confirmed "hey luna" / "luna" → Whisper small (full transcription)
  → returns text to luna_jarvis.py

CPU usage when idle: < 1% (VAD only, no Whisper running)
Activation latency: ~0.4s after you stop speaking (RTX 2080)
"""

import os
import sounddevice as sd
import numpy as np
import webrtcvad
import collections
import queue
import threading
import time
import logging
from pathlib import Path

logger = logging.getLogger("luna.voice_listener")

# ── Config ────────────────────────────────────────────────────────────────────
SAMPLE_RATE       = 16000   # Whisper needs 16kHz
FRAME_DURATION_MS = 30      # WebRTC VAD frame size (10, 20, or 30 ms)
FRAME_SIZE        = int(SAMPLE_RATE * FRAME_DURATION_MS / 1000)  # 480 samples
VAD_AGGRESSIVENESS = 2      # 0-3 (3 = most aggressive filtering)

WAKE_WORDS        = ["luna", "hey luna", "luna.", "hey luna.",
                     "луна", "привет луна", "эй луна"]   # + Russian forms
PRE_SPEECH_FRAMES = 10      # frames to keep before speech starts (300ms)
MAX_SPEECH_SEC    = 8       # stop recording after this many seconds (was 15)
SILENCE_FRAMES    = 22      # frames of silence = end of speech (~660ms, was 900)
MAX_SPEECH_FRAMES = int(MAX_SPEECH_SEC * 1000 / FRAME_DURATION_MS)  # hard cap
QUEUE_MAX_FRAMES  = 120     # ~3.6s of audio — bound the backlog so it can't spiral

WHISPER_TINY_MODEL  = "tiny"   # wake word detection — fastest
WHISPER_SMALL_MODEL = "base"   # full transcription — more accurate


def _whisper_device():
    """
    Pick the fastest available Whisper backend.

    GPU (cuda/float16) is ~5x faster than CPU but needs cuDNN — which the
    torch cu128 build ships inside torch/lib. We add that dir to the DLL search
    path so ctranslate2 (faster-whisper's backend) can find it. Falls back to
    CPU/int8 if CUDA or cuDNN isn't available.
    """
    try:
        import torch, pathlib
        if torch.cuda.is_available():
            lib = pathlib.Path(torch.__file__).parent / "lib"
            if lib.exists():
                os.add_dll_directory(str(lib))
            return "cuda", "float16"
    except Exception as e:
        logger.warning(f"GPU Whisper unavailable ({e}); using CPU")
    return "cpu", "int8"


class VoiceListener:
    """Always-on microphone listener with wake word detection."""

    def __init__(self):
        self.vad = webrtcvad.Vad(VAD_AGGRESSIVENESS)
        self._audio_queue = queue.Queue(maxsize=QUEUE_MAX_FRAMES)
        self._muted = False     # True while Luna is speaking → don't hear herself
        self._running = False
        self._whisper_tiny = None
        self._whisper_small = None
        self._on_wake_callback = None
        self._on_transcription_callback = None
        self._stream = None

    # ── Model loading ─────────────────────────────────────────────────────────

    def load_models(self):
        """Load Whisper models. Call once at startup. Prefers GPU (~5x faster)."""
        from faster_whisper import WhisperModel
        device, ct = _whisper_device()
        logger.info(f"Loading Whisper tiny+base on {device} ({ct})...")
        try:
            self._whisper_tiny = WhisperModel(WHISPER_TINY_MODEL, device=device, compute_type=ct)
            self._whisper_small = WhisperModel(WHISPER_SMALL_MODEL, device=device, compute_type=ct)
        except Exception as e:
            logger.warning(f"Whisper {device} load failed ({e}); falling back to CPU")
            self._whisper_tiny = WhisperModel(WHISPER_TINY_MODEL, device="cpu", compute_type="int8")
            self._whisper_small = WhisperModel(WHISPER_SMALL_MODEL, device="cpu", compute_type="int8")

        # Warm up the kernels so the FIRST real command isn't slow (GPU cold start ~5s).
        try:
            dummy = np.zeros(SAMPLE_RATE, dtype=np.float32)
            list(self._whisper_tiny.transcribe(dummy, language="en", beam_size=1)[0])
            list(self._whisper_small.transcribe(dummy, language="en", beam_size=1)[0])
        except Exception:
            pass
        logger.info("Whisper models loaded + warmed.")

    # ── Audio capture ─────────────────────────────────────────────────────────

    def _audio_callback(self, indata, frames, time_info, status):
        """Called by sounddevice on each audio frame."""
        # Drop frames while muted (Luna is speaking) or stopped — this is what
        # stops her hearing herself and stops the transcribe backlog spiral.
        if not self._running or self._muted:
            return
        audio = (indata[:, 0] * 32768).astype(np.int16).tobytes()
        try:
            self._audio_queue.put_nowait(audio)
        except queue.Full:
            pass  # backlog full → drop oldest-by-skipping; never block/spiral

    def start_stream(self):
        """Open the microphone stream."""
        self._stream = sd.InputStream(
            samplerate=SAMPLE_RATE,
            channels=1,
            dtype="float32",
            blocksize=FRAME_SIZE,
            callback=self._audio_callback,
        )
        self._stream.start()
        logger.info(f"Microphone open — listening for {WAKE_WORDS}")

    def stop_stream(self):
        if self._stream:
            self._stream.stop()
            self._stream.close()

    # ── Mute control (so Luna doesn't transcribe her own voice) ───────────────

    def mute(self):
        """Stop capturing audio (call while Luna is speaking)."""
        self._muted = True

    def unmute(self):
        """Resume capturing — discard anything that arrived while muted."""
        self._drain_queue()
        self._muted = False

    def _drain_queue(self):
        """Throw away all buffered audio frames (clears echo + backlog)."""
        try:
            while True:
                self._audio_queue.get_nowait()
        except queue.Empty:
            pass

    # ── Wake word detection loop ──────────────────────────────────────────────

    def _collect_speech(self) -> bytes:
        """
        Collect audio frames until speech ends.
        Returns raw PCM bytes of the full utterance (wake word included).
        """
        ring_buffer = collections.deque(maxlen=PRE_SPEECH_FRAMES)
        voiced_frames = []
        in_speech = False
        silence_count = 0

        deadline = time.time() + MAX_SPEECH_SEC

        while time.time() < deadline:
            try:
                frame = self._audio_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            is_speech = self.vad.is_speech(frame, SAMPLE_RATE)

            if not in_speech:
                ring_buffer.append(frame)
                if is_speech:
                    in_speech = True
                    voiced_frames.extend(ring_buffer)
                    ring_buffer.clear()
            else:
                voiced_frames.append(frame)
                if len(voiced_frames) >= MAX_SPEECH_FRAMES:
                    break   # hard cap — never build a giant buffer
                if not is_speech:
                    silence_count += 1
                    if silence_count >= SILENCE_FRAMES:
                        break
                else:
                    silence_count = 0

        return b"".join(voiced_frames)

    def _audio_to_float(self, pcm_bytes: bytes) -> np.ndarray:
        """Convert raw PCM int16 bytes to float32 numpy array."""
        audio_np = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
        return audio_np / 32768.0

    def _transcribe(self, audio_np: np.ndarray, model_size: str,
                    language: "str | None" = "en") -> str:
        """
        Run Whisper transcription on audio.

        language="en" for the fast wake-word check (her name detects reliably).
        language=None on the command pass = auto-detect, so Serge can speak
        Russian or English and she transcribes whichever he used.
        """
        model = self._whisper_tiny if model_size == "tiny" else self._whisper_small
        if language is None:
            # Restrict to Serge's two languages. Plain auto-detect was guessing
            # Hindi/etc. on short accented clips and producing garbage. Force EN
            # AND RU, then keep whichever transcription is more confident.
            best_text, best_score = "", -1e9
            for lang in ("en", "ru"):
                segs, _ = model.transcribe(audio_np, language=lang, beam_size=1)
                segs = list(segs)
                if not segs:
                    continue
                score = sum(s.avg_logprob for s in segs) / len(segs)
                if score > best_score:
                    best_text = " ".join(s.text for s in segs).strip()
                    best_score = score
            return best_text.lower()
        segments, _ = model.transcribe(audio_np, language=language, beam_size=1)
        return " ".join(seg.text for seg in segments).strip().lower()

    def _check_wake_word(self, text: str) -> bool:
        """Return True if any wake word is in the transcribed text."""
        t = text.lower().strip()
        return any(w in t for w in WAKE_WORDS)

    def _extract_command(self, text: str) -> str:
        """Strip wake word prefix and return the actual command."""
        t = text.lower().strip()
        for w in sorted(WAKE_WORDS, key=len, reverse=True):
            if t.startswith(w):
                t = t[len(w):].strip().lstrip(",").strip()
                break
        return t

    # ── Main listen loop ──────────────────────────────────────────────────────

    def listen_loop(
        self,
        on_wake=None,
        on_transcription=None,
    ):
        """
        Main blocking loop. Runs forever until stop() is called.

        on_wake(text): called when wake word detected
        on_transcription(command_text): called with the user's command
        """
        self._running = True
        self._on_wake_callback = on_wake
        self._on_transcription_callback = on_transcription

        logger.info("Luna Jarvis listening... say 'Hey Luna' to activate.")

        while self._running:
            # Always collecting audio frames silently
            pcm = self._collect_speech()

            if len(pcm) < SAMPLE_RATE * 2:  # less than 0.1s — skip noise
                continue

            audio_np = self._audio_to_float(pcm)

            # Fast tiny-model check: is this a wake word? (en/ru so Russian "Луна" wakes her)
            tiny_text = self._transcribe(audio_np, "tiny", language=None)
            logger.debug(f"[tiny] heard: '{tiny_text}'")

            if not self._check_wake_word(tiny_text):
                continue  # not a wake word — ignore and keep listening

            logger.info(f"Wake word detected: '{tiny_text}'")
            if on_wake:
                on_wake(tiny_text)

            # High-accuracy transcription of the command — auto-detect language
            # so Serge can speak English OR Russian and she follows along.
            full_text = self._transcribe(audio_np, "small", language=None)
            command = self._extract_command(full_text)
            logger.info(f"Command: '{command}'")

            if command and on_transcription:
                on_transcription(command)

    def stop(self):
        self._running = False


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")
    listener = VoiceListener()
    listener.load_models()
    listener.start_stream()

    def on_wake(text):
        print(f"\n[WAKE] Heard: {text}")

    def on_command(cmd):
        print(f"[COMMAND] '{cmd}'")

    try:
        listener.listen_loop(on_wake=on_wake, on_transcription=on_command)
    except KeyboardInterrupt:
        listener.stop()
        listener.stop_stream()
        print("Stopped.")
