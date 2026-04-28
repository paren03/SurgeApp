# File: D:\SurgeApp\luna_apprentice.py
# Purpose: "Apprenticeship Loop" for Luna
# - Watches Aider + repo changes
# - Distills short lessons into memory\Journal.txt + memory\instructor_lessons.jsonl
# - Captures diffs into solutions\instructor_<ts>.diff (optional)
# - Designed to be safe: READS code, does NOT apply edits.
#
# Run (foreground):  python D:\SurgeApp\luna_apprentice.py
# Run (background):  Start_Luna_Apprentice.bat
#
# Defaults assume your repo lives at D:\SurgeApp (override with LUNA_PROJECT_DIR env var).

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# -------------------------------
# Paths / config
# -------------------------------
DEFAULT_PROJECT_DIR = r"D:\SurgeApp"
PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", DEFAULT_PROJECT_DIR))

LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"
SOLUTIONS_DIR = PROJECT_DIR / "solutions"

AIDER_HISTORY = PROJECT_DIR / ".aider.chat.history.md"  # created by aider when run inside repo
STATE_PATH = LOGS_DIR / "apprentice_state.json"
LESSONS_JSONL = MEMORY_DIR / "instructor_lessons.jsonl"
JOURNAL_TXT = MEMORY_DIR / "Journal.txt"

# Ollama
OLLAMA_API_BASE = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("LUNA_INSTRUCTOR_MODEL", "llama3.1:8b-instruct-q4_K_M")

# Loop
POLL_SECONDS = float(os.environ.get("LUNA_APPRENTICE_POLL", "2.0"))

# Limits
MAX_EXCERPT_CHARS = 10_000
MAX_JOURNAL_APPEND_CHARS = 2_000

# Hide flashing console windows when spawning console-subsystem children
# (e.g. git.exe) from a pythonw parent that has no console of its own.
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_dirs() -> None:
    for d in (LOGS_DIR, MEMORY_DIR, SOLUTIONS_DIR):
        d.mkdir(parents=True, exist_ok=True)


def _safe_read_json(path: Path, default: Any = None) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "null")
    except Exception:
        return default
    return default


def _safe_write_json(path: Path, obj: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _append_journal(text: str) -> None:
    try:
        JOURNAL_TXT.parent.mkdir(parents=True, exist_ok=True)
        with JOURNAL_TXT.open("a", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_tail_text(path: Path, max_bytes: int = 200_000) -> str:
    """Read up to max_bytes from end of file."""
    try:
        if not path.exists():
            return ""
        size = path.stat().st_size
        with path.open("rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
            data = f.read()
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


# -------------------------------
# Ollama client (no external deps)
# -------------------------------
def _ollama_chat(messages: List[Dict[str, str]], model: str = OLLAMA_MODEL, timeout: float = 20.0) -> Optional[str]:
    """Calls Ollama /api/chat with stream=false. Returns assistant content or None."""
    payload = {"model": model, "messages": messages, "stream": False}
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url=f"{OLLAMA_API_BASE}/api/chat",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        obj = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
        msg = obj.get("message") or {}
        content = msg.get("content")
        return content.strip() if isinstance(content, str) else None
    except (URLError, HTTPError, TimeoutError, ConnectionError, ValueError):
        return None


# -------------------------------
# Git helpers
# -------------------------------
def _is_git_repo() -> bool:
    return (PROJECT_DIR / ".git").exists()


def _git(cmd: List[str], timeout: float = 10.0) -> Tuple[int, str, str]:
    try:
        p = subprocess.run(
            ["git", "-C", str(PROJECT_DIR)] + cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            creationflags=_NO_WINDOW,
        )
        return int(p.returncode), p.stdout or "", p.stderr or ""
    except Exception as e:
        return 1, "", str(e)


def _git_status_porcelain() -> str:
    if not _is_git_repo():
        return ""
    rc, out, _ = _git(["status", "--porcelain"], timeout=8.0)
    return out.strip() if rc == 0 else ""


def _git_diff() -> str:
    if not _is_git_repo():
        return ""
    rc, out, _ = _git(["diff"], timeout=12.0)
    return out.strip() if rc == 0 else ""


# -------------------------------
# Lesson distillation
# -------------------------------
LESSON_PROMPT = """You are Luna's Instructor Recorder.
You will receive an excerpt from Aider/chat history and/or git status/diff context.
Write a compact lesson Luna can store and reuse.

Rules:
- Output JSON with keys: topic, keypoints (array), commands (array), files (array), tests (array), cautions (array).
- Keep it short. keypoints max 6 items.
- No long prose.
"""


def _distill_lesson(excerpt: str, git_status: str, diff_preview: str) -> Dict[str, Any]:
    excerpt = (excerpt or "").strip()
    git_status = (git_status or "").strip()
    diff_preview = (diff_preview or "").strip()

    if len(excerpt) > MAX_EXCERPT_CHARS:
        excerpt = excerpt[-MAX_EXCERPT_CHARS:]
    if len(diff_preview) > MAX_EXCERPT_CHARS:
        diff_preview = diff_preview[:MAX_EXCERPT_CHARS]

    user_blob = (
        "AIDER_EXCERPT:\n" + excerpt +
        "\n\nGIT_STATUS:\n" + git_status +
        "\n\nDIFF_PREVIEW:\n" + diff_preview
    ).strip()

    out = _ollama_chat([
        {"role": "system", "content": LESSON_PROMPT},
        {"role": "user", "content": user_blob},
    ])

    if out:
        try:
            obj = json.loads(out)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return {
                "topic": "Instructor summary (raw)",
                "keypoints": [out[:400]],
                "commands": [],
                "files": [],
                "tests": [],
                "cautions": ["Model returned non-JSON; stored as raw summary."],
            }

    # Fallback (no Ollama)
    files: List[str] = []
    for ln in git_status.splitlines():
        parts = ln.split()
        if parts:
            files.append(parts[-1])
    return {
        "topic": "Apprentice capture (no Ollama)",
        "keypoints": [
            "Ollama not reachable; stored minimal lesson.",
            "Review diff in solutions/ if present.",
        ],
        "commands": [],
        "files": files[:12],
        "tests": [],
        "cautions": ["Start Ollama server: ollama serve"],
    }


def _format_journal_snippet(lesson: Dict[str, Any]) -> str:
    topic = str(lesson.get("topic") or "Lesson").strip()
    keypoints = lesson.get("keypoints") if isinstance(lesson.get("keypoints"), list) else []
    cautions = lesson.get("cautions") if isinstance(lesson.get("cautions"), list) else []
    cmds = lesson.get("commands") if isinstance(lesson.get("commands"), list) else []
    files = lesson.get("files") if isinstance(lesson.get("files"), list) else []
    tests = lesson.get("tests") if isinstance(lesson.get("tests"), list) else []

    lines: List[str] = []
    lines.append(f"\n\n=== Instructor Lesson ({_now_iso()}) ===")
    lines.append(f"Topic: {topic}")
    if keypoints:
        lines.append("Keypoints:")
        for kp in keypoints[:6]:
            lines.append(f"- {str(kp)[:220]}")
    if files:
        lines.append("Files: " + ", ".join(str(f)[:120] for f in files[:10]))
    if cmds:
        lines.append("Commands:")
        for c in cmds[:6]:
            lines.append(f"- {str(c)[:220]}")
    if tests:
        lines.append("Tests:")
        for t in tests[:6]:
            lines.append(f"- {str(t)[:220]}")
    if cautions:
        lines.append("Cautions:")
        for c in cautions[:6]:
            lines.append(f"- {str(c)[:220]}")

    blob = "\n".join(lines)
    if len(blob) > MAX_JOURNAL_APPEND_CHARS:
        blob = blob[:MAX_JOURNAL_APPEND_CHARS] + "\n…"
    return blob


def main() -> int:
    _ensure_dirs()
    state = _safe_read_json(STATE_PATH, default={}) or {}
    last_aider_hash = str(state.get("aider_tail_hash") or "")
    last_git_hash = str(state.get("git_diff_hash") or "")

    print(f"[apprentice] project={PROJECT_DIR}")
    print(f"[apprentice] ollama={OLLAMA_API_BASE} model={OLLAMA_MODEL}")
    print(f"[apprentice] poll={POLL_SECONDS}s")

    while True:
        try:
            aider_tail = _read_tail_text(AIDER_HISTORY, max_bytes=200_000)
            aider_hash = _sha256_bytes(aider_tail.encode("utf-8", errors="ignore")) if aider_tail else ""

            git_status = _git_status_porcelain()
            diff = _git_diff()
            diff_hash = _sha256_bytes(diff.encode("utf-8", errors="ignore")) if diff else ""

            changed = False
            if aider_hash and aider_hash != last_aider_hash:
                changed = True
            if diff_hash and diff_hash != last_git_hash:
                changed = True

            if changed:
                diff_preview = ""
                if diff:
                    lines = diff.splitlines()
                    diff_preview = "\n".join(lines[:200])
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    try:
                        (SOLUTIONS_DIR / f"instructor_{ts}.diff").write_text(diff, encoding="utf-8")
                    except Exception:
                        pass

                lesson = _distill_lesson(aider_tail, git_status, diff_preview)
                record = {
                    "ts": _now_iso(),
                    "source": "luna_apprentice",
                    "ollama_api_base": OLLAMA_API_BASE,
                    "model": OLLAMA_MODEL,
                    "git_status": git_status,
                    "lesson": lesson,
                }
                _append_jsonl(LESSONS_JSONL, record)
                _append_journal(_format_journal_snippet(lesson))

                last_aider_hash = aider_hash or last_aider_hash
                last_git_hash = diff_hash or last_git_hash
                _safe_write_json(STATE_PATH, {
                    "ts": _now_iso(),
                    "aider_tail_hash": last_aider_hash,
                    "git_diff_hash": last_git_hash,
                })

            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            print("\n[apprentice] stopped")
            return 0
        except Exception:
            time.sleep(POLL_SECONDS)
            continue


if __name__ == "__main__":
    raise SystemExit(main())
