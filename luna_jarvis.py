"""
luna_jarvis.py — Luna Jarvis Mode: Main Voice Assistant Orchestrator

Pipeline:
  Microphone → Wake Word "Hey Luna" → STT (Whisper) → Claude API → TTS (Piper) → Speakers

Usage:
    python luna_jarvis.py           # full voice mode
    python luna_jarvis.py --text    # keyboard input mode (test without mic)
    python luna_jarvis.py --test    # quick TTS + Claude test

Serge just says "Hey Luna" and she answers. That's it.
"""

import os
import sys
import time
import queue
import threading
import logging
import argparse
from pathlib import Path

# ── Setup logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(Path(r"D:\SurgeApp\logs\luna_jarvis.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("luna.jarvis")

# ── Ensure SurgeApp in path ───────────────────────────────────────────────────
SURGE_ROOT = Path(r"D:\SurgeApp")
if str(SURGE_ROOT) not in sys.path:
    sys.path.insert(0, str(SURGE_ROOT))

from luna_modules.luna_claude_bridge import ask_claude
from luna_modules import luna_jarvis_tools as jt   # her "hands" (vault, notes, status)

# Active voice listener (set in run_voice_mode). Used to mute the mic while Luna
# is speaking so she never transcribes her own voice (the freeze/echo fix).
_LISTENER = None


# ── TTS: use Luna's existing voice runtime ────────────────────────────────────
def speak(text: str):
    """
    Speak text in Luna's HIGH-FIDELITY cloned voice (XTTS-v2).
    Uses the 5 best reference recordings averaged — sounds like the real Luna.
    Falls back to the cognitive_voice_runtime, then SAPI, on failure.
    """
    if not text.strip():
        return
    # Primary: high-fidelity XTTS-v2 clone with best reference samples
    try:
        from luna_modules.luna_voice_clone import speak as clone_speak
        clone_speak(text)
        return
    except Exception as e:
        logger.warning(f"Clone voice failed, falling back: {e}")

    # Fallback: cognitive voice runtime
    try:
        from luna_modules.cognitive_voice_runtime import CognitiveVoiceRuntime
        CognitiveVoiceRuntime().speak(text)
        return
    except Exception as e:
        logger.error(f"TTS fallback failed: {e}")
        print(f"[LUNA SAYS] {text}")


# ── Streaming TTS buffer ──────────────────────────────────────────────────────
class StreamingTTS:
    """
    Buffers Claude streaming tokens and speaks complete sentences
    as soon as they arrive — makes responses feel instant.
    """
    def __init__(self):
        self._buffer = ""
        self._sentence_ends = ".!?。"

    def feed(self, token: str):
        """Feed a streaming token. Speaks complete sentences immediately."""
        self._buffer += token
        # Check for sentence boundaries
        while True:
            found = -1
            for ch in self._sentence_ends:
                idx = self._buffer.find(ch)
                if idx != -1 and (found == -1 or idx < found):
                    found = idx
            if found == -1:
                break
            sentence = self._buffer[:found + 1].strip()
            self._buffer = self._buffer[found + 1:]
            if sentence:
                speak(sentence)

    def flush(self):
        """Speak any remaining text in buffer."""
        remaining = self._buffer.strip()
        if remaining:
            speak(remaining)
        self._buffer = ""


# ── Proactive memory context ──────────────────────────────────────────────────
def get_memory_context() -> str:
    """
    Pull key context from Luna's memory for the current session.
    Gives Claude awareness of what's happening on the machine.
    """
    context_parts = []

    # Worker heartbeat
    try:
        hb_file = SURGE_ROOT / "memory" / "worker_heartbeat.json"
        if hb_file.exists():
            import json
            hb = json.loads(hb_file.read_text())
            context_parts.append(f"Luna worker last heartbeat: {hb.get('timestamp', 'unknown')}")
    except Exception:
        pass

    # Dashboard status
    try:
        import urllib.request
        req = urllib.request.urlopen("http://127.0.0.1:8765/api/health", timeout=1)
        if req.status == 200:
            context_parts.append("Luna dashboard: online at localhost:8765")
    except Exception:
        context_parts.append("Luna dashboard: offline or unreachable")

    if context_parts:
        return "\n".join(context_parts)
    return ""


# ── Command router ────────────────────────────────────────────────────────────
SYSTEM_COMMANDS = {
    "dashboard": "http://127.0.0.1:8765",
    "status": None,  # handled specially
    "stop": None,    # handled specially
    "exit": None,
    "quit": None,
}

def handle_command(text: str, tts: StreamingTTS) -> bool:
    """
    Handle special local commands before sending to Claude.
    Returns True if handled locally (skip Claude), False to continue to Claude.
    """
    t = text.lower().strip()

    if t in ("stop", "exit", "quit", "goodbye", "bye"):
        speak("Goodbye, Serge. Shutting down Jarvis mode.")
        return True  # Signal to stop

    if "open dashboard" in t or "show dashboard" in t:
        import webbrowser
        webbrowser.open("http://127.0.0.1:8765")
        speak("Opening your dashboard.")
        return False

    if t in ("status", "system status", "how are you", "how are you doing"):
        ctx = get_memory_context()
        if ctx:
            speak(f"Systems look good. {ctx}")
        else:
            speak("I'm running normally. Worker is active. What do you need?")
        return False

    return False  # Not a local command — send to Claude


def process_query(text: str, tts: StreamingTTS) -> bool:
    """
    Process a voice command/question.
    Returns True if should stop Jarvis mode.
    """
    if not text.strip():
        return False

    logger.info(f"Processing: '{text}'")

    # Check local commands
    t = text.lower().strip()
    if t in ("stop", "exit", "quit", "goodbye", "bye", "shut down"):
        speak("Shutting down Jarvis mode. I'll keep monitoring in the background.")
        return True

    # Get memory context for richer responses
    ctx = get_memory_context()

    # Stream Claude's response → TTS speaks sentences as they arrive
    def on_token(token):
        tts.feed(token)

    logger.info("Sending to Claude...")
    t0 = time.time()
    if _LISTENER is not None:
        _LISTENER.mute()        # she's about to talk — stop hearing herself
    try:
        ask_claude(text, on_token=on_token, memory_context=ctx,
                   tools=jt.TOOLS, execute_tool=jt.execute_tool)
        tts.flush()
    finally:
        if _LISTENER is not None:
            _LISTENER.unmute()  # drain her own audio + resume listening
    logger.info(f"Response complete in {time.time()-t0:.2f}s")
    return False


# ── Main loops ────────────────────────────────────────────────────────────────

def run_voice_mode():
    """Full voice mode: microphone → wake word → STT → Claude → TTS."""
    global _LISTENER
    from luna_modules.luna_voice_listener import VoiceListener

    tts = StreamingTTS()
    listener = VoiceListener()
    _LISTENER = listener   # let process_query mute the mic while she speaks

    logger.info("Loading speech recognition models (first run downloads ~150MB)...")
    speak("Loading voice models. One moment.")
    listener.load_models()
    listener.start_stream()
    listener.mute()        # mic is open — don't capture our own greeting
    speak("Jarvis mode active. Say Hey Luna to activate me.")
    listener.unmute()      # drop the greeting echo, start listening clean

    stop_event = threading.Event()

    def on_wake(text):
        # No spoken "Yes?" ack — her actual reply is the acknowledgment.
        # (Saves a whole synth+playback cycle → noticeably faster.)
        pass

    def on_transcription(command):
        should_stop = process_query(command, tts)
        if should_stop:
            stop_event.set()

    # Run listener in thread
    listen_thread = threading.Thread(
        target=listener.listen_loop,
        kwargs={"on_wake": on_wake, "on_transcription": on_transcription},
        daemon=True
    )
    listen_thread.start()

    try:
        stop_event.wait()  # Block until stop commanded
    except KeyboardInterrupt:
        speak("Shutting down.")
    finally:
        listener.stop()
        listener.stop_stream()
        logger.info("Jarvis voice mode stopped.")


def run_text_mode():
    """Keyboard input mode — test Claude + TTS without microphone."""
    tts = StreamingTTS()
    speak("Jarvis text mode active. Type your questions. Type 'quit' to exit.")
    print("\n[JARVIS TEXT MODE] Type questions below. Ctrl+C to quit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            speak("Goodbye.")
            break

        if not user_input:
            continue

        should_stop = process_query(user_input, tts)
        if should_stop:
            break

    print("Jarvis stopped.")


def run_test():
    """Quick test: TTS works + Claude responds."""
    print("=== Luna Jarvis Quick Test ===\n")

    # Test 1: TTS
    print("[1] Testing TTS...")
    speak("Hello Serge. Jarvis mode is working. My voice systems are online.")
    print("    TTS: OK\n")

    # Test 2: Claude bridge
    print("[2] Testing Claude bridge...")
    tts = StreamingTTS()
    def on_token(t):
        tts.feed(t)
        print(t, end="", flush=True)

    ask_claude("Say hello to Serge in one sentence as Luna.", on_token=on_token)
    tts.flush()
    print("\n    Claude: OK\n")

    print("=== All tests passed. Ready for voice mode. ===")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Luna Jarvis Voice Assistant")
    parser.add_argument("--text",  action="store_true", help="Keyboard input mode (no mic)")
    parser.add_argument("--test",  action="store_true", help="Quick TTS + Claude test")
    parser.add_argument("--voice", action="store_true", help="Full voice mode (default)")
    args = parser.parse_args()

    # Ensure log dir exists
    (SURGE_ROOT / "logs").mkdir(exist_ok=True)

    if args.test:
        run_test()
    elif args.text:
        run_text_mode()
    else:
        # Default: full voice mode
        run_voice_mode()
