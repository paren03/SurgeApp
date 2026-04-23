"""Metacognition and evolution integrity checks.

Uses CORE_STATE to gate autonomous evolution steps with live health
checks. ``log`` from luna_logging does not accept a ``level`` kwarg;
severity is conveyed in the message prefix instead.
"""

from __future__ import annotations

from luna_modules.luna_logging import log
from luna_modules.luna_state import CORE_STATE

_HEARTBEAT_FAILURE_THRESHOLD = 5


def evaluate_evolution_integrity() -> bool:
    """Check CORE_STATE for signs of instability before evolution steps."""
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
