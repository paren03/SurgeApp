"""
luna_jarvis_tools.py — Luna's "hands": tools her Claude brain can call by voice.

Each tool is (1) a schema dict offered to Claude and (2) a local executor that
runs and returns a short text result she speaks back. Everything here is
lightweight (file reads, one HTTP call, nvidia-smi) — no GPU, no heavy deps —
so it cannot crash the machine. Side-effecting actions are deliberately limited
to safe, reversible ones; anything destructive is intentionally NOT exposed.
"""

import json
import logging
import subprocess
import datetime
import urllib.request
import webbrowser
from pathlib import Path

logger = logging.getLogger("luna.jarvis_tools")

VAULT_ROOT   = Path(r"C:\Users\paren\Documents\Obsidian Vault")
SESSIONS_DIR = VAULT_ROOT / "20 Sessions"
SURGE_ROOT   = Path(r"D:\SurgeApp")

# Optional extra docs root (e.g. OSHA / carpentry curriculum). Set later once we
# know where Serge keeps them; None = search the vault only.
MATERIALS_ROOT = None

_NO_WINDOW = 0x08000000  # CREATE_NO_WINDOW — never flash a console (popup-safe)


# ── Tool schemas offered to Claude ────────────────────────────────────────────
TOOLS = [
    {
        "name": "search_vault",
        "description": (
            "Search Serge's Obsidian vault — his personal notes, project files, and "
            "dated session logs — plus his reference materials. Use this whenever he "
            "asks what was decided, noted, planned, or worked on, or anything about "
            "his projects (Luna, the dashboard, the bilingual stack, curriculum, "
            "OSHA/safety, carpentry). Returns matching snippets with their source."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to look for"}
            },
            "required": ["query"],
        },
    },
    {
        "name": "add_vault_note",
        "description": (
            "Append a note or to-do to today's session log in Serge's vault. Use "
            "when he says to note, remember, jot down, log, or add something."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "note": {"type": "string", "description": "The note text to save"}
            },
            "required": ["note"],
        },
    },
    {
        "name": "luna_status",
        "description": (
            "Check the Luna system's live health: worker heartbeat, dashboard, and "
            "GPU usage. Use when Serge asks if things are running, up, or healthy."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "luna_action",
        "description": (
            "Perform a safe Luna action. Supported: 'open_dashboard' (open the "
            "dashboard in the browser). Use when he asks to open/show the dashboard."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["open_dashboard"]}
            },
            "required": ["action"],
        },
    },
]


# ── Executors ─────────────────────────────────────────────────────────────────

def _iter_doc_files():
    """Yield all markdown/text docs to search (vault + optional materials)."""
    roots = [VAULT_ROOT]
    if MATERIALS_ROOT:
        roots.append(Path(MATERIALS_ROOT))
    for root in roots:
        if not root.exists():
            continue
        for pat in ("*.md", "*.txt"):
            for p in root.rglob(pat):
                yield p


def _search_vault(query: str, max_hits: int = 3) -> str:
    try:
        terms = [t for t in query.lower().split() if len(t) > 2] or [query.lower().strip()]
        hits = []
        for doc in _iter_doc_files():
            try:
                text = doc.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            low = text.lower()
            score = sum(low.count(t) for t in terms)
            if not score:
                continue
            lines = text.splitlines()
            snippet = ""
            for i, ln in enumerate(lines):
                if any(t in ln.lower() for t in terms):
                    snippet = " ".join(lines[max(0, i - 1):i + 2]).strip()
                    break
            hits.append((score, doc.name, snippet[:300]))
        if not hits:
            return f"I couldn't find anything in your notes about '{query}'."
        hits.sort(reverse=True)
        return "Found in your notes:\n" + "\n".join(
            f"[{name}] {snip}" for _, name, snip in hits[:max_hits]
        )
    except Exception as e:
        return f"Vault search failed: {e}"


def _add_vault_note(note: str) -> str:
    try:
        if not note.strip():
            return "There was no note text to save."
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        f = SESSIONS_DIR / f"{today}.md"
        text = f.read_text(encoding="utf-8") if f.exists() else f"# {today} — Session log\n"
        if "## Voice notes" not in text:
            text += "\n## Voice notes\n"
        stamp = datetime.datetime.now().strftime("%H:%M")
        text += f"- ({stamp}) {note.strip()}\n"
        f.write_text(text, encoding="utf-8")
        return f"Saved to today's session log: {note.strip()}"
    except Exception as e:
        return f"Couldn't save the note: {e}"


def _luna_status() -> str:
    parts = []
    try:
        hb = json.loads((SURGE_ROOT / "memory" / "worker_heartbeat.json").read_text())
        parts.append(f"worker last heartbeat {hb.get('timestamp', 'unknown')}")
    except Exception:
        parts.append("worker heartbeat not found")
    try:
        r = urllib.request.urlopen("http://127.0.0.1:8765/api/health", timeout=1)
        parts.append("dashboard online" if r.status == 200 else f"dashboard HTTP {r.status}")
    except Exception:
        parts.append("dashboard offline")
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.used,memory.total,utilization.gpu",
             "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5, creationflags=_NO_WINDOW,
        )
        if out.returncode == 0 and out.stdout.strip():
            parts.append("GPU " + out.stdout.strip().splitlines()[0])
    except Exception:
        parts.append("GPU usage unknown")
    return "; ".join(parts)


def _luna_action(action: str) -> str:
    if action == "open_dashboard":
        try:
            webbrowser.open("http://127.0.0.1:8765")
            return "Opened the dashboard in your browser."
        except Exception as e:
            return f"Couldn't open the dashboard: {e}"
    return f"That action ('{action}') isn't available."


_DISPATCH = {
    "search_vault":   lambda inp: _search_vault(inp.get("query", "")),
    "add_vault_note": lambda inp: _add_vault_note(inp.get("note", "")),
    "luna_status":    lambda inp: _luna_status(),
    "luna_action":    lambda inp: _luna_action(inp.get("action", "")),
}


def execute_tool(name: str, tool_input: dict) -> str:
    """Run a tool by name. ALWAYS returns a string, never raises."""
    try:
        fn = _DISPATCH.get(name)
        if not fn:
            return f"Unknown tool: {name}"
        result = str(fn(tool_input or {}))
        logger.info(f"tool {name}({tool_input}) -> {result[:80]}")
        return result
    except Exception as e:
        logger.error(f"tool {name} failed: {e}")
        return f"Tool {name} error: {e}"


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("search_vault:", _search_vault("dashboard warden")[:200])
    print("luna_status :", _luna_status())
    print("add_note    :", _add_vault_note("tool self-test note"))
