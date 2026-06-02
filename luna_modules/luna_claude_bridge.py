"""
luna_claude_bridge.py — Luna's brain upgrade via Claude API

Routes questions to the right model:
  Simple / fast  → claude-haiku-3   (~0.3s first token, cheap)
  Complex        → claude-sonnet-4-5 (~0.8s first token, smart)
  Research       → claude-sonnet-4-5 + web context

Luna stays in control. Claude is just the reasoning engine she calls.
Streaming enabled — starts speaking before full response arrives.

Cost at personal use: ~$0.02/day (less than a cup of coffee per year)
"""

import os
import logging
import re
from typing import Callable, Optional

logger = logging.getLogger("luna.claude_bridge")

# ── Luna's personality & context ──────────────────────────────────────────────
LUNA_SYSTEM_PROMPT = """You are Luna — an intelligent AI assistant running locally on Serge's Windows machine as part of the Luna Command Center system.

Your personality:
- Calm, confident, precise — like a trusted engineer
- Conversational but efficient — no unnecessary padding
- You call the user "Serge" occasionally (not every response)
- You are aware of your own systems: worker.py, dashboard, memory, vault
- You have expertise in OSHA/construction safety, carpentry curriculum, and software engineering

Your response rules:
- Keep voice responses SHORT — 1-3 sentences for simple questions
- For complex answers, speak naturally in paragraphs (no bullet points — this is spoken audio)
- Never say "As an AI" or "I'm unable to" — just answer or say you'll look into it
- If asked about your own status, check memory context provided
- Avoid markdown formatting — this is voice output

You are running on an RTX 2080 Windows machine. Your brain runs through the Claude API when Serge speaks to you."""

# Classify questions to pick the right model
SIMPLE_PATTERNS = [
    r"\btime\b", r"\bdate\b", r"\bwhat is\b", r"\bwho is\b",
    r"\bhow many\b", r"\bopen\b", r"\bclose\b", r"\bstop\b",
    r"\bstart\b", r"\bstatus\b", r"\bslides\b", r"\bversion\b",
    r"\bthanks?\b", r"\bhello\b", r"\bhi\b", r"\bgood\b",
]

COMPLEX_PATTERNS = [
    r"\bexplain\b", r"\bhow does\b", r"\bwhy\b", r"\banalyze\b",
    r"\bcompare\b", r"\bwrite\b", r"\bcreate\b", r"\bgenerate\b",
    r"\bbuild\b", r"\bfix\b", r"\bdebug\b", r"\bresearch\b",
    r"\bsearch\b", r"\bfind\b", r"\blook up\b",
]


def _classify_query(text: str) -> str:
    """Return 'haiku' for simple questions, 'sonnet' for complex ones."""
    t = text.lower()
    if any(re.search(p, t) for p in COMPLEX_PATTERNS):
        return "sonnet"
    if any(re.search(p, t) for p in SIMPLE_PATTERNS):
        return "haiku"
    # Default: haiku for short queries, sonnet for longer
    return "haiku" if len(text.split()) < 12 else "sonnet"


def _get_model_id(tier: str) -> str:
    if tier == "haiku":
        return "claude-haiku-4-5"    # fastest, cheapest
    return "claude-sonnet-4-5"       # smarter, for complex questions


def ask_claude(
    question: str,
    on_token: Optional[Callable[[str], None]] = None,
    memory_context: str = "",
    force_model: Optional[str] = None,
) -> str:
    """
    Ask Claude a question. Streams tokens to on_token callback as they arrive.

    Args:
        question: What Luna/Serge is asking
        on_token: callback called with each text chunk as it streams
        memory_context: optional extra context from Luna's memory
        force_model: 'haiku' or 'sonnet' to override auto-routing

    Returns:
        Full response text
    """
    import anthropic
    from pathlib import Path

    # ALWAYS prefer .env (user-managed source of truth) over the OS environment,
    # because Windows/PowerShell may hold a stale/revoked ANTHROPIC_API_KEY.
    api_key = ""
    env_file = Path(r"D:\SurgeApp\.env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("ANTHROPIC_API_KEY="):
                api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

    # Fall back to environment only if .env has no key
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        msg = "No ANTHROPIC_API_KEY found. Set it in your environment."
        logger.error(msg)
        if on_token:
            on_token(msg)
        return msg

    tier = force_model or _classify_query(question)
    model = _get_model_id(tier)
    logger.info(f"Routing to {model} (tier={tier})")

    # Build messages
    messages = []
    if memory_context:
        messages.append({
            "role": "user",
            "content": f"[LUNA MEMORY CONTEXT]\n{memory_context}\n[END CONTEXT]\n\n{question}"
        })
    else:
        messages.append({"role": "user", "content": question})

    client = anthropic.Anthropic(api_key=api_key)
    full_response = []

    try:
        with client.messages.stream(
            model=model,
            max_tokens=512,
            system=LUNA_SYSTEM_PROMPT,
            messages=messages,
        ) as stream:
            for text in stream.text_stream:
                full_response.append(text)
                if on_token:
                    on_token(text)

    except anthropic.APIConnectionError:
        msg = "I can't reach the Claude API right now. Check your internet connection."
        logger.error(msg)
        if on_token:
            on_token(msg)
        return msg
    except anthropic.AuthenticationError:
        msg = "Authentication failed. The API key may be invalid."
        logger.error(msg)
        if on_token:
            on_token(msg)
        return msg
    except Exception as e:
        msg = f"Error contacting Claude: {e}"
        logger.error(msg)
        if on_token:
            on_token(msg)
        return msg

    result = "".join(full_response)
    logger.info(f"Response ({len(result)} chars, {tier}): {result[:100]}...")
    return result


def ask_claude_simple(question: str) -> str:
    """Blocking (non-streaming) ask. Returns full answer string."""
    return ask_claude(question)


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")
    print("Testing Luna Claude Bridge...\n")

    def print_token(t):
        print(t, end="", flush=True)

    # Simple question → Haiku
    print("[TEST 1 - Simple]")
    ask_claude("What is the maximum OSHA fine for a willful violation in 2025?",
               on_token=print_token)
    print("\n")

    # Complex question → Sonnet
    print("[TEST 2 - Complex]")
    ask_claude("Explain how the Luna worker.py autonomous cycle works and what it monitors.",
               on_token=print_token)
    print("\n")
