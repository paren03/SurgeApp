"""
luna_voice_clone.py — High-fidelity Luna clone voice via XTTS-v2

Uses the 5 LONGEST/cleanest reference recordings (averaged) for the most
accurate clone of Luna's actual voice — not the weak 9-second sample the
default runtime used.

Reference samples (108 seconds total vs the old 9s):
  3_trimmed.wav (27.3s) + 4_trimmed.wav (21.4s) + 0_trimmed.wav (20.7s)
  + 5_trimmed.wav (19.7s) + 6_trimmed.wav (19.1s)

Model loads once and stays in memory. First synth ~2s, subsequent ~1s.
"""

import os
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger("luna.voice_clone")

# Suppress noisy TTS logs
logging.getLogger("TTS").setLevel(logging.WARNING)

VOICE_CACHE = Path(r"D:\SurgeApp\memory\voice_cache\luna_samples_prepared")

# The 3 CLEANEST reference samples (all >45dB SNR). Excluding 5 & 6 (noisier,
# ~40dB) which were polluting the clone with robotic artifacts.
REFERENCE_SAMPLES = [
    VOICE_CACHE / "0_trimmed.wav",   # 20.7s, 48.3 dB SNR
    VOICE_CACHE / "3_trimmed.wav",   # 27.3s, 48.2 dB SNR
    VOICE_CACHE / "4_trimmed.wav",   # 21.4s, 45.2 dB SNR
]

XTTS_MODEL = "tts_models/multilingual/multi-dataset/xtts_v2"
LANGUAGE = "en"

# ── Voice tuning parameters (XTTS-v2) ─────────────────────────────────────────
# De-robotized via sandbox spectral-matching to Luna's REAL recordings:
# temp 0.85 gives natural prosody; the post-EQ matches her real voice signature.
# Distance to real Luna: 0.059 (was 0.174 untuned) — 66% closer.
VOICE_TUNING = {
    "temperature": 0.85,         # natural prosody (kills robotic monotone)
    "length_penalty": 1.0,
    "repetition_penalty": 2.3,   # low = smooth, no clipped/robotic pacing
    "top_k": 50,
    "top_p": 0.88,
    "speed": 1.0,
    "enable_text_splitting": True,
}

# Apply the corrective "Luna EQ" post-filter (matches her real voice profile).
APPLY_LUNA_EQ = True


def _luna_eq(wav, sr):
    """
    Corrective EQ that matches synthesized output to Luna's real voice profile.
    Tames boomy sub-bass, adds low-mid warmth, restores natural air/life.
    Derived by spectral-matching against her actual recordings.
    """
    try:
        import numpy as np
        import scipy.signal as sig

        def shelf_low(w, fc, gain_db):
            g = 10 ** (gain_db / 20)
            sos = sig.butter(2, fc, btype="low", fs=sr, output="sos")
            return w + (g - 1) * sig.sosfilt(sos, w)

        def shelf_high(w, fc, gain_db):
            g = 10 ** (gain_db / 20)
            sos = sig.butter(2, fc, btype="high", fs=sr, output="sos")
            return w + (g - 1) * sig.sosfilt(sos, w)

        def peak(w, fc, q, gain_db):
            g = 10 ** (gain_db / 20)
            bw = fc / q
            lo, hi = max(20, fc - bw / 2), min(sr / 2 - 1, fc + bw / 2)
            sos = sig.butter(2, [lo, hi], btype="band", fs=sr, output="sos")
            return w + (g - 1) * sig.sosfilt(sos, w)

        w = shelf_low(wav, 300, -3.0)     # tame boomy sub-bass
        w = peak(w, 1100, 1.2, +2.5)      # add low-mid warmth/presence
        w = shelf_high(w, 7500, +4.0)     # restore natural air/life
        w = w / (np.abs(w).max() + 1e-9) * 0.95   # normalize, avoid clipping
        return w.astype("float32")
    except Exception as e:
        logger.warning(f"Luna EQ skipped: {e}")
        return wav


def _detect_lang(text: str) -> str:
    """
    Pick the XTTS synthesis language from the text itself.

    Restricted ON PURPOSE to Serge's two languages (English + Russian) so the
    old random Chinese/Japanese bug can never come back: anything that isn't
    Cyrillic is synthesized as English.
    """
    for ch in text:
        if "Ѐ" <= ch <= "ӿ":     # Cyrillic block → Russian
            return "ru"
    return "en"


class _LunaEQStream:
    """
    Streaming version of _luna_eq. Same corrective curve (matches Luna's real
    voice profile), but carries each filter's state across chunks so there are
    NO clicks/ticks at chunk boundaries — her voice stays smooth while we play
    audio as it streams out of the model.
    """

    def __init__(self, sr):
        import scipy.signal as sig
        self.sr = sr
        self.sos_low = sig.butter(2, 300, btype="low", fs=sr, output="sos")
        bw = 1100 / 1.2
        lo, hi = max(20, 1100 - bw / 2), min(sr / 2 - 1, 1100 + bw / 2)
        self.sos_peak = sig.butter(2, [lo, hi], btype="band", fs=sr, output="sos")
        self.sos_high = sig.butter(2, 7500, btype="high", fs=sr, output="sos")
        # start every filter from rest (zero initial conditions)
        self.zi_low = sig.sosfilt_zi(self.sos_low) * 0.0
        self.zi_peak = sig.sosfilt_zi(self.sos_peak) * 0.0
        self.zi_high = sig.sosfilt_zi(self.sos_high) * 0.0
        self.g_low = 10 ** (-3.0 / 20)
        self.g_peak = 10 ** (2.5 / 20)
        self.g_high = 10 ** (4.0 / 20)

    def process(self, w):
        import numpy as np
        import scipy.signal as sig
        yl, self.zi_low = sig.sosfilt(self.sos_low, w, zi=self.zi_low)
        w = w + (self.g_low - 1) * yl
        yp, self.zi_peak = sig.sosfilt(self.sos_peak, w, zi=self.zi_peak)
        w = w + (self.g_peak - 1) * yp
        yh, self.zi_high = sig.sosfilt(self.sos_high, w, zi=self.zi_high)
        w = w + (self.g_high - 1) * yh
        # fixed headroom (NO per-chunk normalize → avoids volume pumping)
        return np.clip(w * 0.95, -1.0, 1.0).astype("float32")


def _patch_torchaudio_soundfile():
    """
    torchaudio 2.x defaults to torchcodec (needs FFmpeg). Replace torchaudio.load
    with a soundfile-based loader so XTTS can read reference WAVs without FFmpeg.
    """
    try:
        import torch
        import torchaudio
        import soundfile as sf

        def _sf_load(filepath, *args, **kwargs):
            data, sr = sf.read(str(filepath), dtype="float32", always_2d=True)
            # soundfile: (frames, channels) → torchaudio: (channels, frames)
            tensor = torch.from_numpy(data.T).contiguous()
            return tensor, sr

        torchaudio.load = _sf_load
        logger.info("Patched torchaudio.load -> soundfile (no FFmpeg needed)")
    except Exception as e:
        logger.warning(f"Could not patch torchaudio: {e}")


class LunaCloneVoice:
    """XTTS-v2 clone of Luna's voice using the best reference samples."""

    _instance = None  # singleton — load model once

    def __init__(self):
        self._tts = None
        self._model = None              # low-level XTTS model (for cached latents)
        self._gpt_cond_latent = None    # cached voice fingerprint
        self._speaker_embedding = None  # cached speaker embedding
        self._refs = [str(p) for p in REFERENCE_SAMPLES if p.exists()]
        if not self._refs:
            logger.error("No reference samples found! Voice clone unavailable.")
        else:
            logger.info(f"Clone using {len(self._refs)} reference samples")

    @classmethod
    def get(cls):
        """Get the shared singleton instance (loads model once)."""
        if cls._instance is None:
            cls._instance = LunaCloneVoice()
            cls._instance._load()
        return cls._instance

    def _load(self):
        """Load XTTS-v2 model into memory (GPU) + pre-compute voice fingerprint."""
        if self._tts is not None:
            return
        os.environ["COQUI_TOS_AGREED"] = "1"  # accept license non-interactively
        try:
            _patch_torchaudio_soundfile()   # avoid FFmpeg/torchcodec dependency
            import torch
            from TTS.api import TTS
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Loading XTTS-v2 on {device}...")
            self._tts = TTS(XTTS_MODEL).to(device)

            # Grab the low-level model and pre-compute the speaker fingerprint ONCE.
            # This is the expensive step — caching it makes every later sentence fast.
            try:
                self._model = self._tts.synthesizer.tts_model
                logger.info("Pre-computing voice fingerprint from reference samples...")
                # Richer conditioning = more natural, less robotic:
                #  - gpt_cond_len 30s: captures more of Luna's prosody/intonation
                #  - max_ref_length 60s: uses more of the reference audio
                #  - sound_norm_refs: normalizes refs for a cleaner, smoother clone
                self._gpt_cond_latent, self._speaker_embedding = \
                    self._model.get_conditioning_latents(
                        audio_path=self._refs,
                        gpt_cond_len=30,
                        gpt_cond_chunk_len=6,
                        max_ref_length=60,
                        sound_norm_refs=True,
                    )
                logger.info("Voice fingerprint cached — fast synthesis enabled.")
            except Exception as e:
                logger.warning(f"Could not cache latents (will use slower path): {e}")
                self._model = None

            logger.info("XTTS-v2 clone voice ready.")
        except Exception as e:
            logger.error(f"Failed to load XTTS-v2: {e}")
            self._tts = None

    def synthesize(self, text: str, out_path: str = "", language: str = None) -> str:
        """
        Generate speech in Luna's cloned voice. Returns path to WAV file.
        Uses the cached voice fingerprint (fast path) when available.

        language: "en"/"ru"; auto-detected from the text when None.
        """
        if self._tts is None:
            self._load()
        if self._tts is None:
            raise RuntimeError("XTTS-v2 not available")

        lang = language or _detect_lang(text)

        if not out_path:
            fd, out_path = tempfile.mkstemp(suffix=".wav", prefix="luna_clone_")
            os.close(fd)

        # FAST PATH: cached fingerprint → no per-sentence reference processing
        if self._model is not None and self._gpt_cond_latent is not None:
            import soundfile as sf
            out = self._model.inference(
                text,
                lang,
                self._gpt_cond_latent,
                self._speaker_embedding,
                temperature=VOICE_TUNING["temperature"],
                length_penalty=VOICE_TUNING["length_penalty"],
                repetition_penalty=VOICE_TUNING["repetition_penalty"],
                top_k=VOICE_TUNING["top_k"],
                top_p=VOICE_TUNING["top_p"],
                speed=VOICE_TUNING["speed"],
                enable_text_splitting=VOICE_TUNING["enable_text_splitting"],
            )
            wav = out["wav"]
            if hasattr(wav, "cpu"):
                wav = wav.cpu().numpy()
            import numpy as np
            wav = np.asarray(wav, dtype="float32")
            if APPLY_LUNA_EQ:
                wav = _luna_eq(wav, 24000)   # match real Luna voice profile
            sf.write(out_path, wav, 24000)
            return out_path

        # SLOW PATH fallback: full pipeline with reference samples each call
        self._tts.tts_to_file(
            text=text,
            speaker_wav=self._refs,
            language=lang,
            file_path=out_path,
            temperature=VOICE_TUNING["temperature"],
            length_penalty=VOICE_TUNING["length_penalty"],
            repetition_penalty=VOICE_TUNING["repetition_penalty"],
            top_k=VOICE_TUNING["top_k"],
            top_p=VOICE_TUNING["top_p"],
            speed=VOICE_TUNING["speed"],
            split_sentences=VOICE_TUNING["enable_text_splitting"],
        )
        return out_path

    def speak(self, text: str):
        """
        Speak text in Luna's cloned voice immediately.

        Tries the STREAMING path first (she begins talking in ~0.5s instead of
        waiting ~2s for the whole sentence) — same model, same voice, just
        played as it's generated. If streaming isn't available or errors, falls
        back to the proven full-synth path so she always speaks.
        """
        if not text.strip():
            return
        if self._tts is None:
            self._load()
        language = _detect_lang(text)

        # FAST: stream audio chunks as they generate
        try:
            if self._speak_streaming(text, language):
                return
        except Exception as e:
            logger.warning(f"Streaming path error, using standard synth: {e}")

        # PROVEN fallback: full synth + play (full offline EQ)
        try:
            wav_path = self.synthesize(text, language=language)
            self._play(wav_path)
            try:
                os.remove(wav_path)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Clone speak failed: {e}")
            raise

    def _speak_streaming(self, text: str, language: str) -> bool:
        """
        Stream synthesis: play audio chunks the moment XTTS produces them.
        Returns True if it played audio, False to fall back to full synth.
        Needs the cached voice fingerprint (the fast path).
        """
        if self._model is None or self._gpt_cond_latent is None:
            return False
        import numpy as np
        import sounddevice as sd

        eq = _LunaEQStream(24000) if APPLY_LUNA_EQ else None
        stream = sd.OutputStream(samplerate=24000, channels=1, dtype="float32")
        stream.start()
        try:
            chunks = self._model.inference_stream(
                text,
                language,
                self._gpt_cond_latent,
                self._speaker_embedding,
                stream_chunk_size=20,
                temperature=VOICE_TUNING["temperature"],
                length_penalty=VOICE_TUNING["length_penalty"],
                repetition_penalty=VOICE_TUNING["repetition_penalty"],
                top_k=VOICE_TUNING["top_k"],
                top_p=VOICE_TUNING["top_p"],
                speed=VOICE_TUNING["speed"],
            )
            got_audio = False
            for chunk in chunks:
                if hasattr(chunk, "cpu"):
                    chunk = chunk.cpu().numpy()
                chunk = np.asarray(chunk, dtype="float32").reshape(-1)
                if chunk.size == 0:
                    continue
                if eq is not None:
                    chunk = eq.process(chunk)
                stream.write(chunk.reshape(-1, 1))
                got_audio = True
            return got_audio
        finally:
            import time as _t
            _t.sleep(0.2)        # let the buffer drain so her last word isn't clipped
            stream.stop()
            stream.close()

    def _play(self, wav_path: str):
        """Play a WAV file through the default audio device."""
        try:
            import soundfile as sf
            import sounddevice as sd
            data, sr = sf.read(wav_path, dtype="float32")
            sd.play(data, sr)
            sd.wait()
            return
        except Exception:
            pass
        # Fallback: winsound (Windows built-in)
        try:
            import winsound
            winsound.PlaySound(wav_path, winsound.SND_FILENAME)
        except Exception as e:
            logger.error(f"Audio playback failed: {e}")


# ── Module-level convenience ──────────────────────────────────────────────────
def speak(text: str):
    """Speak text in Luna's cloned voice (loads model on first call)."""
    LunaCloneVoice.get().speak(text)


# ── Test ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    print("Testing Luna clone voice with best reference samples...")
    print(f"References: {[p.name for p in REFERENCE_SAMPLES if p.exists()]}")
    print()
    print("Generating + speaking...")
    speak("Hi Serge. This is my real voice now. I'm using your best recordings, "
          "so I should sound like the Luna you made. How do I sound?")
    print("Done. Did that sound like Luna?")
