"""Mission pipeline — create, update, and track Luna missions.

Fixes vs original script:
- write_json_atomic() requires a pathlib.Path, not a str.
  os.path.join() returns a str and would raise AttributeError on
  path.parent / path.with_name() calls inside write_json_atomic.
- MISSIONS_DIR added to luna_paths.py (was not previously defined).
- update_mission_status() now actually reads, mutates, and persists
  the mission JSON rather than being a no-op comment.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from luna_modules.luna_io import safe_read_json, write_json_atomic
from luna_modules.luna_logging import _diag, ensure_layout, log
from luna_modules.luna_paths import MISSIONS_DIR


def _ensure_missions_dir() -> None:
    MISSIONS_DIR.mkdir(parents=True, exist_ok=True)


class LunaMission:
    """Represents a single autonomous mission with structured metadata."""

    def __init__(self, title: str, objective: str) -> None:
        self.mission_id: str = f"MSN-{uuid.uuid4().hex[:6].upper()}"
        self.title: str = title
        self.objective: str = objective
        self.tasks: List[Dict[str, Any]] = []
        self.status: str = "INITIALIZING"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "title": self.title,
            "objective": self.objective,
            "tasks": self.tasks,
            "status": self.status,
        }


def _mission_path(mission_id: str) -> Path:
    """Return the Path to a mission's JSON file."""
    return MISSIONS_DIR / f"{mission_id}.json"


def start_new_mission(title: str, objective: str) -> str:
    """Create a new mission, persist it, and return its mission_id."""
    _ensure_missions_dir()
    mission = LunaMission(title, objective)
    log(f"[MISSION] Starting: {title} ({mission.mission_id})")
    write_json_atomic(_mission_path(mission.mission_id), mission.to_dict())
    return mission.mission_id


def update_mission_status(mission_id: str, status: str) -> bool:
    """Load the mission JSON, update its status, and persist it.

    Returns True on success, False if the mission file is not found.
    """
    path = _mission_path(mission_id)
    data = safe_read_json(path, default={})
    if not data:
        _diag(f"update_mission_status: mission {mission_id} not found at {path}")
        return False
    data["status"] = status
    write_json_atomic(path, data)
    log(f"[MISSION] {mission_id} status -> {status}")
    return True


def load_mission(mission_id: str) -> Optional[Dict[str, Any]]:
    """Return the mission dict or None if not found."""
    data = safe_read_json(_mission_path(mission_id), default={})
    return data if data else None


def list_missions() -> List[Dict[str, Any]]:
    """Return all persisted missions sorted by mission_id."""
    if not MISSIONS_DIR.exists():
        return []
    missions = []
    for path in sorted(MISSIONS_DIR.glob("MSN-*.json")):
        data = safe_read_json(path, default={})
        if data:
            missions.append(data)
    return missions
