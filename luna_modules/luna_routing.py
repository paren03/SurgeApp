"""Command classifiers and worker-mode resolution.

Extracted from ``worker.py`` (step 8 of modularity refactor).
The metacognition/level5/RSI classifier functions (``_is_metacognition_command``,
``_is_level5_command``, ``_is_rsi_command``) remain in ``worker.py`` and move
with their respective domain modules in later steps.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from luna_modules.luna_paths import (
    GUIDED_IMPROVEMENT_TRIGGERS,
    IMPROVEMENT_MODE_TRIGGERS,
    MCP_ADOPTION_TRIGGERS,
    MODE_ALIASES,
    SELF_FIX_TRIGGERS,
    SUPPORTED_TASK_TYPES,
)


def normalize_prompt_text(text: str) -> str:
    lowered = (text or "").strip().lower()
    lowered = lowered.replace("_", " ").replace("-", " ")
    return " ".join(lowered.split())


def prompt_has_any(prompt: str, triggers) -> bool:
    normalized = normalize_prompt_text(prompt)
    return any(normalize_prompt_text(trigger) in normalized for trigger in triggers)


def normalize_task_type(task_type: str) -> str:
    normalized = normalize_prompt_text(task_type).replace(" ", "_")
    return normalized if normalized in SUPPORTED_TASK_TYPES else "chat"


def normalize_worker_mode(mode: str) -> str:
    normalized = normalize_prompt_text(mode).replace(" ", "_")
    return MODE_ALIASES.get(normalized, "")


def is_self_fix_command(prompt: str) -> bool:
    return prompt_has_any(prompt, SELF_FIX_TRIGGERS)


def is_refactor_improvement_command(prompt: str) -> bool:
    return prompt_has_any(prompt, GUIDED_IMPROVEMENT_TRIGGERS)


def is_improvement_command(prompt: str) -> bool:
    return prompt_has_any(prompt, IMPROVEMENT_MODE_TRIGGERS)


def is_mcp_adoption_command(prompt: str) -> bool:
    normalized = f" {normalize_prompt_text(prompt)} "
    if "model context protocol" in normalized:
        return True
    return any(trigger in normalized for trigger in MCP_ADOPTION_TRIGGERS)


def is_mission_command(prompt: str) -> bool:
    normalized = normalize_prompt_text(prompt)
    return normalized.startswith("mission:") or normalized.startswith("mission ")


def is_quit_command(prompt: str) -> bool:
    return normalize_prompt_text(prompt) in {"quit", "quit worker", "stop worker", "shutdown worker", "exit worker"}


def resolve_declared_payload_mode(task: Dict[str, Any]) -> str:
    return normalize_worker_mode(task.get("worker_mode") or task.get("mode") or "")


def task_has_mission_payload(task: Dict[str, Any]) -> bool:
    mission_targets = task.get("mission_targets")
    mission_steps = task.get("mission_steps")
    mission_id = str(task.get("mission_id") or "").strip()
    mission_type = str(task.get("mission_type") or "").strip()
    verification_required = task.get("verification_required")
    return bool(
        (isinstance(mission_targets, list) and len(mission_targets) >= 1)
        or (isinstance(mission_steps, list) and len(mission_steps) >= 1)
        or mission_id
        or mission_type.startswith("safe_")
        or verification_required is True
    )


def resolve_worker_mode(task: Dict[str, Any]) -> Tuple[str, str, str]:
    user_input = (task.get("prompt") or "").strip()
    task_type = normalize_task_type(task.get("task_type", ""))
    declared_mode = resolve_declared_payload_mode(task)
    mission_payload = task_has_mission_payload(task)
    if task_type == "approval_response":
        return "approval-response", task_type, declared_mode or "approval_response"
    if task_type == "meta_decision" or declared_mode == "meta_decision" or normalize_prompt_text(user_input) == "decide next":
        return "meta-decision", task_type or "meta_decision", declared_mode or "meta_decision"
    if task_type == "acquisition_request" or declared_mode == "acquisition_request" or "install app from github" in normalize_prompt_text(user_input):
        return "acquisition-request", task_type or "acquisition_request", declared_mode or "acquisition_request"
    if task_type == "self_upgrade_pipeline" or declared_mode == "self_upgrade_pipeline" or normalize_prompt_text(user_input) in {"self upgrade now", "run self upgrade", "upgrade self now"}:
        return "self-upgrade", task_type or "self_upgrade_pipeline", declared_mode or "self_upgrade_pipeline"
    if task_type == "upgrade_proposal" or declared_mode == "upgrade_proposal" or normalize_prompt_text(user_input).startswith(("upgrade:", "proposal:")):
        return "upgrade-proposal", task_type or "upgrade_proposal", declared_mode or "upgrade_proposal"
    if task_type == "mcp_adoption" or declared_mode == "mcp_adoption" or is_mcp_adoption_command(user_input):
        return "mcp-adoption", task_type or "mcp_adoption", declared_mode or "mcp_adoption"
    if task_type == "system_action":
        return "system-action", task_type, declared_mode or "system_action"
    if task_type == "mission_orchestration" or declared_mode == "mission_kernel" or mission_payload or is_mission_command(user_input):
        return "mission-kernel", task_type or "mission_orchestration", declared_mode or "mission_kernel"
    if task_type == "code_fix" or declared_mode == "self_fix" or is_self_fix_command(user_input):
        return "self-fix", task_type or "code_fix", declared_mode or "self_fix"
    if task_type == "guided_improvement" or declared_mode == "guided_loop" or is_refactor_improvement_command(user_input):
        return "guided-loop", task_type or "guided_improvement", declared_mode or "guided_loop"
    if task_type == "improvement_analysis" or declared_mode == "improvement" or is_improvement_command(user_input):
        return "improvement", task_type or "improvement_analysis", declared_mode or "improvement"
    if is_quit_command(user_input):
        return "quit", task_type or "system_action", declared_mode or "system_action"
    if task_type == "chat" or declared_mode in {"chat", "chat_response"}:
        return "chat-response", task_type or "chat", declared_mode or "chat_response"
    return "blocked", task_type or "chat", declared_mode or "direct_target"


def classify_extended_prompt_route(prompt: str) -> str:
    normalized = normalize_prompt_text(prompt)
    if normalized.startswith("plan goal:") or normalized.startswith("decompose goal:") or normalized.startswith("complex goal:"):
        return "planning_request"
    if normalized.startswith("run tool pipeline:") or normalized.startswith("pipeline:") or "tool pipeline" in normalized:
        return "tool_pipeline_request"
    if normalized in {"review drift", "check drift", "run self audit", "self audit now"}:
        return "drift_review"
    if is_mcp_adoption_command(prompt):
        return "mcp_adoption"
    return "standard"
