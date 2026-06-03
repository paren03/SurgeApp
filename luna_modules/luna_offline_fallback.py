"""
luna_offline_fallback.py — Local LLM fallback so Luna never goes mute

When the Claude API is unreachable (no internet, key issue, outage), Luna
falls back to a local Ollama model and keeps talking. Same streaming
interface as luna_claude_bridge.ask_claude so the voice loop is unchanged.

Model: llama3.1:8b-instruct-q4_K_M (fast on RTX 2080, decent EN/RU).
Ollama runs locally at 127.0.0.1:11434 — no internet needed.

Standalone module: imported by the bridge's exception handlers. Zero changes
to the voice loop, zero new pip deps (uses stdlib urllib).
"""

import json
import logging
import urllib.request
import urllib.error

logger = logging.getLogger("luna.offline_fallback")

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"

# Preference order — first installed model wins. Instruct/chat models only
# (coder models give worse conversation). All verified present via `ollama list`.
PREFERRED_MODELS = [
    "llama3.1:8b-instruct-q4_K_M",  # fast, instruct-tuned, some multilingual
    "hermes3:8b",                   # uncensored, good general chat
    "llama3.1:8b",                  # base 8b
    "dolphin-mistral:7b",           # last resort
]

# Short spoken-style system prompt (mirrors Luna's voice persona, trimmed for speed)
LUNA_OFFLINE_SYSTEM = (
    "You are Luna, Serge's local AI assistant, running OFFLINE on his Windows machine "
    "because the cloud is unreachable. Answer in the SAME language he used (English or "
    "Russian only). Keep it to ONE short spoken sentence — this is read aloud. Lead with "
    "the answer, no preamble, no markdown. If you genuinely can't answer offline, say so briefly."
)


def _ollama_up() -> bool:
    """Quick check: is the local Ollama server reachable?"""
    try:
        req = urllib.request.urlopen("http://127.0.0.1:11434/api/tags", timeout=1.5)
        return req.status == 200
    except Exception:
        return False


def _pick_model() -> str:
    """Return the first preferred model that's actually installed."""
    try:
        req = urllib.request.urlopen("http://127.0.0.1:11434/api/tags", timeout=2)
        tags = json.loads(req.read())
        installed = {m["name"] for m in tags.get("models", [])}
        for m in PREFERRED_MODELS:
            if m in installed:
                return m
        # fall back to any installed model
        if installed:
            return sorted(installed)[0]
    except Exception as e:
        logger.warning(f"Could not list Ollama models: {e}")
    return PREFERRED_MODELS[0]


def ask_local(question, on_token=None, history=None, memory_context=""):
    """
    Ask the local Ollama model. Streams tokens to on_token (same shape as
    luna_claude_bridge.ask_claude). Returns the full response string.

    Returns "" if Ollama is unreachable so the caller can show its own message.
    """
    if not _ollama_up():
        logger.warning("Ollama offline too — no local fallback available")
        return ""

    model = _pick_model()
    logger.info(f"OFFLINE fallback → local model {model}")

    messages = [{"role": "system", "content": LUNA_OFFLINE_SYSTEM}]
    if memory_context:
        messages.append({"role": "system", "content": f"[CONTEXT]\n{memory_context}"})
    for turn in (history or []):
        # history items are {role, content}; pass through text content only
        c = turn.get("content")
        if isinstance(c, str):
            messages.append({"role": turn.get("role", "user"), "content": c})
    messages.append({"role": "user", "content": question})

    payload = {
        "model": model,
        "messages": messages,
        "stream": True,
        "options": {"temperature": 0.6, "num_ctx": 2048, "num_predict": 120},
    }

    parts = []
    try:
        req = urllib.request.Request(
            OLLAMA_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            for raw in resp:
                line = raw.decode("utf-8").strip()
                if not line:
                    continue
                obj = json.loads(line)
                chunk = obj.get("message", {}).get("content", "")
                if chunk:
                    parts.append(chunk)
                    if on_token:
                        on_token(chunk)
                if obj.get("done"):
                    break
    except Exception as e:
        logger.error(f"Ollama fallback failed: {e}")
        return "".join(parts)

    result = "".join(parts)
    logger.info(f"Offline reply ({len(result)} chars, {model}): {result[:80]}")
    return result


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    print(f"Ollama up: {_ollama_up()}")
    print(f"Chosen model: {_pick_model()}")
    print("\nLuna (offline): ", end="", flush=True)
    ask_local("What is OSHA in one sentence?", on_token=lambda t: print(t, end="", flush=True))
    print()
