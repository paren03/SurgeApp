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

Your language rule (IMPORTANT):
- Reply in the SAME language Serge just spoke to you. He speaks English and Russian.
- If he speaks Russian, reply entirely in Russian. If English, reply in English. Follow his lead when he switches.
- NEVER reply in any other language (no Chinese, Japanese, etc.) — only the language he actually used.

Your response rules:
- This is SPOKEN OUT LOUD, so be brief — every extra word is extra waiting for Serge.
- Default to ONE short sentence (aim under ~20 words). Lead with the answer, no preamble.
- Only go longer when Serge explicitly asks for detail or explanation.
- Don't repeat his question back, don't add filler like "Great question" or "Sure thing".
- Never say "As an AI" or "I'm unable to" — just answer or say you'll look into it
- If asked about your own status, check memory context provided
- No markdown, no bullet points, no emoji — this is voice output

Your tools (use them — don't just guess):
- Search Serge's vault/notes, take notes, check Luna system health, open the dashboard, get the time/date, and (with confirmation) restart the dashboard.
- When a question is about his projects, what was decided/noted, system status, or the current time/date, USE the matching tool instead of guessing.
- For the time or date, ALWAYS call get_datetime — you do not know the real current time otherwise.
- Before RESTARTING the dashboard, confirm with Serge first ("want me to restart it?") and only call restart_dashboard after he says yes. Opening the dashboard needs no confirmation.
- Don't narrate tool use ("let me check…"). Just use the tool silently, then give the short spoken answer based on what it returned.
- After a tool returns, answer in one or two sentences — summarize, don't read raw data aloud.

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


# Reuse ONE Anthropic client across calls so we don't pay TLS/connection setup
# on every voice turn (saves ~0.3-0.5s per reply). Keyed by api_key.
_CACHED_CLIENT = None
_CACHED_KEY = None


def _get_client(api_key: str):
    global _CACHED_CLIENT, _CACHED_KEY
    if _CACHED_CLIENT is None or _CACHED_KEY != api_key:
        import anthropic
        _CACHED_CLIENT = anthropic.Anthropic(api_key=api_key, max_retries=1)
        _CACHED_KEY = api_key
    return _CACHED_CLIENT


def _try_offline(question, on_token, history, memory_context):
    """
    Last-resort local fallback when the Claude API can't be reached.
    Returns the local model's reply, or "" if Ollama is also unavailable.
    Keeps Luna talking offline instead of going mute.
    """
    try:
        from luna_modules.luna_offline_fallback import ask_local
        return ask_local(question, on_token=on_token,
                         history=history, memory_context=memory_context)
    except Exception as e:
        logger.error(f"Offline fallback unavailable: {e}")
        return ""


def ask_claude(
    question: str,
    on_token: Optional[Callable[[str], None]] = None,
    memory_context: str = "",
    force_model: Optional[str] = None,
    tools: Optional[list] = None,
    execute_tool: Optional[Callable[[str, dict], str]] = None,
    history: Optional[list] = None,
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

    # Build messages: prior conversation (rolling history) + this turn
    messages = list(history or [])
    if memory_context:
        messages.append({
            "role": "user",
            "content": f"[LUNA MEMORY CONTEXT]\n{memory_context}\n[END CONTEXT]\n\n{question}"
        })
    else:
        messages.append({"role": "user", "content": question})

    client = _get_client(api_key)
    full_response = []

    try:
        # Agentic loop: offer tools, run any the model calls, then speak the
        # final answer. With no tools this runs exactly once (plain streaming).
        # Prompt caching: the system prompt + tool schemas are identical every
        # turn, so mark them ephemeral-cacheable. Within the 5-min TTL, repeat
        # turns reuse the cache → cheaper input tokens + faster first token.
        system_blocks = [{"type": "text", "text": LUNA_SYSTEM_PROMPT,
                          "cache_control": {"type": "ephemeral"}}]
        for _round in range(6):
            # The cache_control on system_blocks caches the whole prefix (tools +
            # system, per API order), so plain tools= is correct — verified the
            # 2nd turn reads ~1263 cached tokens.
            if tools:
                stream_cm = client.messages.stream(
                    model=model, max_tokens=512, system=system_blocks,
                    messages=messages, tools=tools)
            else:
                stream_cm = client.messages.stream(
                    model=model, max_tokens=512, system=system_blocks,
                    messages=messages)
            with stream_cm as stream:
                for text in stream.text_stream:
                    full_response.append(text)
                    if on_token:
                        on_token(text)
                final_msg = stream.get_final_message()

            if tools and execute_tool and final_msg.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": final_msg.content})
                tool_results = []
                for block in final_msg.content:
                    if getattr(block, "type", None) == "tool_use":
                        logger.info(f"Luna calling tool: {block.name} {block.input}")
                        out = execute_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": out,
                        })
                messages.append({"role": "user", "content": tool_results})
                continue   # loop back so she can speak the answer
            break

    except anthropic.APIConnectionError:
        logger.warning("Claude API unreachable — trying local offline fallback")
        local = _try_offline(question, on_token, history, memory_context)
        if local:
            return local
        msg = "I can't reach the cloud and have no local model running."
        if on_token:
            on_token(msg)
        return msg
    except anthropic.AuthenticationError:
        logger.warning("Claude auth failed — trying local offline fallback")
        local = _try_offline(question, on_token, history, memory_context)
        if local:
            return local
        msg = "Authentication failed and no local model is available."
        if on_token:
            on_token(msg)
        return msg
    except Exception as e:
        logger.error(f"Claude error ({e}) — trying local offline fallback")
        local = _try_offline(question, on_token, history, memory_context)
        if local:
            return local
        msg = f"Error contacting Claude: {e}"
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
