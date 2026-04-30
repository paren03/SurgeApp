"""Append-only memory routing for Luna autonomy summaries."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def route_autonomy_summary(project_dir: str | Path, summary: Dict[str, Any]) -> Dict[str, Any]:
    """Write a durable learning summary without deleting or rewriting queues."""
    root = Path(project_dir)
    memory_dir = root / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "ts": _now_iso(),
        "kind": "autonomy_summary",
        "attempted": summary.get("attempted", "not recorded"),
        "changed": summary.get("changed", "not recorded"),
        "failed": summary.get("failed", "not recorded"),
        "no_diff": summary.get("no_diff", "not recorded"),
        "learned": summary.get("learned", "not recorded"),
        "next": summary.get("next", "not recorded"),
        "risky_files": summary.get("risky_files", []),
        "prompts_worked": summary.get("prompts_worked", 0),
        "prompts_failed": summary.get("prompts_failed", 0),
    }
    jsonl_path = memory_dir / "nightly_updates.jsonl"
    md_path = memory_dir / "nightly_updates.md"
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    with md_path.open("a", encoding="utf-8") as handle:
        handle.write(
            "\n"
            f"## Autonomy Memory Router - {record['ts']}\n"
            f"- attempted: {record.get('attempted', 'not recorded')}\n"
            f"- changed: {record.get('changed', 'not recorded')}\n"
            f"- failed: {record.get('failed', 'not recorded')}\n"
            f"- no_diff: {record.get('no_diff', 'not recorded')}\n"
            f"- learned: {record.get('learned', 'not recorded')}\n"
            f"- next: {record.get('next', 'not recorded')}\n"
        )
    return {"ok": True, "jsonl_path": str(jsonl_path), "md_path": str(md_path), "record": record}
