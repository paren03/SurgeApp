"""Metacognition and evolution integrity gate.

luna_logging.log() accepts only a message string; severity is
communicated via message prefix, not a ``level`` keyword argument.
"""

from __future__ import annotations

from typing import Any, Dict

from luna_modules.luna_logging import log
from luna_modules.luna_state import CORE_STATE

_HEARTBEAT_FAILURE_THRESHOLD = 5


def evaluate_evolution_integrity() -> bool:
    """Return False if heartbeat instability exceeds the threshold."""
    if CORE_STATE.heartbeat_failure_count > _HEARTBEAT_FAILURE_THRESHOLD:
        log(
            f"[CRITICAL] Heartbeat instability detected in Metacognition "
            f"(failure_count={CORE_STATE.heartbeat_failure_count} "
            f"> threshold={_HEARTBEAT_FAILURE_THRESHOLD})."
        )
        return False
    return True


def can_proceed_with_evolution() -> bool:
    """Level-7 gatekeeper: allow evolution only when state is healthy."""
    if CORE_STATE.stop_requested:
        log("[METACOG] Evolution gated: stop_requested is True.")
        return False
    return evaluate_evolution_integrity()


def run_metacognitive_reflection(context: str = "") -> Dict[str, Any]:
    """Placeholder for metacognitive reflection cycle."""
    log(f"[METACOG] Metacognitive reflection triggered: {context}")
    return {"ok": True}


def recursive_belief_revision() -> Dict[str, Any]:
    """Placeholder for recursive belief revision."""
    return {"ok": True, "revisions": 0}


def run_self_audit() -> Dict[str, Any]:
    """Placeholder for metacog-layer self-audit."""
    return {"ok": True, "integrity": 1.0}


def build_evolution_gate_report() -> Dict[str, Any]:
    """Return a snapshot of the current gate state."""
    return {
        "status": "OPEN" if can_proceed_with_evolution() else "BLOCKED",
        "stop_requested": CORE_STATE.stop_requested,
        "heartbeat_failure_count": CORE_STATE.heartbeat_failure_count,
        "threshold": _HEARTBEAT_FAILURE_THRESHOLD,
    }


def persist_evolution_state() -> None:
    """Placeholder: persist metacog state to disk (future step)."""
    pass
