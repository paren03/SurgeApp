# MCP Servers — Setup & Status

> Installed 2026-06-03. All at **user scope** (`C:\Users\paren\.claude.json` → `mcpServers`),
> so they're available in every project, not just SurgeApp.

## Installed Servers

| Server | Type | Status | What it gives Claude |
|---|---|---|---|
| **sequential-thinking** | stdio (npx) | ✓ Connected | Structured multi-step reasoning for complex builds/debugging |
| **memory** | stdio (npx) | ✓ Connected | Persistent knowledge graph across sessions → `D:\SurgeApp\memory\luna_knowledge_graph.json` |
| **playwright** | stdio (npx) | ✓ Connected | Live browser automation — research, scraping, dashboard testing, screenshot-compare. Chromium installed. |
| **github** | stdio (token) | ✓ Connected | Repo management — issues, PRs, push, for `github.com/paren03/SurgeApp`. Uses the `gh` CLI token (no manual OAuth needed). |
| fal-ai | http | ✓ Connected | (pre-existing) AI image generation |

## GitHub — Fully Wired (no action needed)

GitHub MCP now connects automatically using your `gh` CLI token
(`GITHUB_PERSONAL_ACCESS_TOKEN` in the config). The original remote
Copilot-OAuth endpoint was swapped for the token-based stdio server
because it couldn't be activated without a manual `/mcp` login.
If the `gh` token ever expires, re-run `gh auth login`, then refresh the
token in the config with `gh auth token`.

## Important: Restart to Use Them

MCP servers load when Claude Code **starts**. The 3 connected servers
(sequential-thinking, memory, playwright) will be callable in your **next**
Claude Code session. Current session won't see them until restart.

## How to Manage

```bash
claude mcp list                    # health-check all servers
claude mcp get sequential-thinking # details on one
claude mcp remove <name> -s user   # remove one
```

## Config Reference (what was added)

```jsonc
// C:\Users\paren\.claude.json → "mcpServers"
{
  "sequential-thinking": { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"] },
  "memory":              { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-memory"],
                           "env": { "MEMORY_FILE_PATH": "D:/SurgeApp/memory/luna_knowledge_graph.json" } },
  "playwright":          { "command": "npx", "args": ["-y", "@playwright/mcp@latest"] },
  "github":              { "type": "http", "url": "https://api.githubcopilot.com/mcp/" }
}
```

## Also Installed — PPT Master Skill ✓

- **PPT Master** (`hugohe3/ppt-master`) — INSTALLED 2026-06-03
  - Location: `C:\Users\paren\.claude\skills\ppt-master\` (registered Claude skill)
  - Source clone: `C:\n8n-run\pptx-workflow\ppt-master\`
  - Wired keys: OpenAI (gpt-image-2) + Pexels in skill `.env`
  - What it does: source doc (PDF/DOCX/URL/MD) → editable PPTX via SVG pipeline, follows your .pptx template, native shapes + speaker notes
  - Triggers on: "create PPT", "make presentation", "ppt-master"
  - Output: `exports/<name>_<timestamp>.pptx`

## Not Installed (with reasons)

- **Presenton** (`presenton/presenton`) — SUPERSEDED. Needs Docker (you removed it, task #6). PPT Master + skywork-pptx now cover the same job (AI→editable PPTX following templates) with no Docker. Tasks #192/#194 redundant unless you specifically want Presenton's web UI.
- **ElevenLabs MCP** — premium TTS backup. Skipped — your free XTTS clone works well.
