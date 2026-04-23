"""Pure I/O helpers for the Luna worker.

Extracted verbatim from ``worker.py`` (step 2 of modularity refactor).
"""

from __future__ import annotations

import json
import os
import py_compile
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

# On Windows, os.replace() can fail with PermissionError (WinError 5) when
# antivirus software or another process briefly holds the destination file.
# _ATOMIC_REPLACE_RETRIES controls how many times we retry before giving up.
_ATOMIC_REPLACE_RETRIES = 3 if sys.platform == "win32" else 1
_ATOMIC_REPLACE_DELAY = 0.05  # seconds between retries

from luna_modules.luna_logging import _diag, ensure_layout
from luna_modules.luna_paths import LUNA_MASTER_CODEX_PATH


def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
    except Exception:
        return ""


def _atomic_replace(temp_path: Path, dest_path: Path) -> None:
    """Replace dest_path with temp_path atomically, with retry on Windows lock contention.

    Always removes temp_path on failure so orphaned .tmp files cannot accumulate.
    """
    last_exc: Exception = OSError("unknown")
    for attempt in range(_ATOMIC_REPLACE_RETRIES):
        try:
            os.replace(str(temp_path), str(dest_path))
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt < _ATOMIC_REPLACE_RETRIES - 1:
                time.sleep(_ATOMIC_REPLACE_DELAY)
        except Exception as exc:
            last_exc = exc
            break
    # Cleanup orphaned temp file before propagating
    try:
        temp_path.unlink(missing_ok=True)
    except Exception:
        pass
    raise last_exc


def safe_write_text(path: Path, text: str) -> None:
    temp_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        temp_path.write_text(text, encoding="utf-8")
        _atomic_replace(temp_path, path)
    except Exception as exc:
        _diag(f"safe_write_text failed for {path}: {exc}")


def _compile_python_path(path: Path) -> Tuple[bool, str]:
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix="luna_compile_")
        pyc_path = Path(temp_dir) / f"{path.stem}.pyc"
        py_compile.compile(str(path), cfile=str(pyc_path), doraise=True)
        return True, ""
    except Exception as exc:
        return False, str(getattr(exc, "msg", exc))
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


def safe_read_json(path: Path, default=None):
    if default is None:
        default = {}
    try:
        if path.exists():
            raw = path.read_text(encoding="utf-8", errors="ignore")
            if not raw.strip():
                return default
            return json.loads(raw)
    except Exception as exc:
        _diag(f"safe_read_json failed for {path}: {exc}")
    return default


def write_json_atomic(path: Path, data: Any) -> None:
    temp_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        temp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        _atomic_replace(temp_path, path)
    except Exception as exc:
        _diag(f"write_json_atomic failed for {path}: {exc}")


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass


def append_codex_note(title: str, body: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    note = f"\n\n## {stamp} — {title}\n{body.strip()}\n"
    try:
        ensure_layout()
        with open(LUNA_MASTER_CODEX_PATH, "a", encoding="utf-8") as handle:
            handle.write(note)
    except Exception as exc:
        _diag(f"append_codex_note failed: {exc}")
