"""Path and string constants shared across the Luna worker.

Extracted verbatim from ``worker.py`` (step 1 of modularity refactor).
No logic changes; this module is the dependency base for all other
``luna_modules`` submodules and is intended to be imported by
``worker.py`` for backward compatibility.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_OWNER = "Serge"
DEFAULT_LUNA_NAME = "Luna"

DEFAULT_PROJECT_DIR = r"D:\SurgeApp"
PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", DEFAULT_PROJECT_DIR))

TASKS_DIR = PROJECT_DIR / "tasks"
ACTIVE_DIR = TASKS_DIR / "active"
DONE_DIR = TASKS_DIR / "done"
FAILED_DIR = TASKS_DIR / "failed"
SOLUTIONS_DIR = PROJECT_DIR / "solutions"
LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"
BACKUPS_DIR = PROJECT_DIR / "backups" / "self_fix"
LUNA_MODULES_DIR = PROJECT_DIR / "luna_modules"
LUNA_MODULES_INIT_PATH = LUNA_MODULES_DIR / "__init__.py"
LUNA_TELEMETRY_MODULE_PATH = LUNA_MODULES_DIR / "luna_telemetry.py"

WORKER_LOG_PATH = LOGS_DIR / "luna_worker.log"
WORKER_HEARTBEAT_PATH = LOGS_DIR / "luna_worker_heartbeat.json"
WORKER_LOCK_PATH = LOGS_DIR / "luna_worker.lock.json"
LUNA_TASK_MEMORY_PATH = MEMORY_DIR / "luna_task_memory.json"
LUNA_SESSION_MEMORY_PATH = MEMORY_DIR / "luna_session_memory.json"
LUNA_APPROVAL_QUEUE_PATH = MEMORY_DIR / "luna_approval_queue.json"
LUNA_AUTONOMY_STATE_PATH = MEMORY_DIR / "luna_autonomy_state.json"
LUNA_MASTER_CODEX_PATH = PROJECT_DIR / "LUNA_MASTER_CODEX_AND_MEMORY.md"
LUNA_SYSTEM_PROMPT_PATH = MEMORY_DIR / "LUNA_SYSTEM_PROMPT.txt"
PROMPT_OPTIMIZER_STATE_PATH = MEMORY_DIR / "luna_prompt_optimizer_state.json"
LUNA_MODULE_REGISTRY_PATH = MEMORY_DIR / "luna_module_registry.json"
SELF_FIX_LOG_PATH = LOGS_DIR / "luna_self_fix.jsonl"
VERIFICATION_HISTORY_PATH = SELF_FIX_LOG_PATH
LOGIC_UPDATES_DIR = PROJECT_DIR / "logic_updates"
TEMP_TEST_ZONE_DIR = PROJECT_DIR / "temp_test_zone"
ARCHIVE_LOGS_DIR = LOGS_DIR / "archive"
SAFETY_RULES_PATH = PROJECT_DIR / "LUNA_SAFETY_RULES.txt"
KILL_SWITCH_PATH = PROJECT_DIR / "LUNA_STOP_NOW.flag"
HUMAN_CHECKIN_PATH = MEMORY_DIR / "luna_last_checkin.json"
COUNCIL_HISTORY_PATH = MEMORY_DIR / "luna_council_history.json"
ACQUISITIONS_DIR = PROJECT_DIR / "acquisitions"
ACQUISITION_RECEIPTS_PATH = MEMORY_DIR / "luna_acquisition_receipts.json"
TRUSTED_ACQUISITION_REGISTRY_PATH = MEMORY_DIR / "luna_trusted_acquisition_registry.json"
DECISION_ENGINE_STATE_PATH = MEMORY_DIR / "luna_decision_engine_state.json"
DECISION_HISTORY_PATH = MEMORY_DIR / "luna_decision_history.json"
SELF_UPGRADE_STATE_PATH = MEMORY_DIR / "luna_self_upgrade_state.json"
SUPERVISOR_STATE_PATH = MEMORY_DIR / "luna_supervisor_state.json"
UPGRADE_HISTORY_PATH = MEMORY_DIR / "luna_upgrade_history.json"

IDENTITY_STATE_PATH = MEMORY_DIR / "luna_identity_state.json"
WORLD_MODEL_STATE_PATH = MEMORY_DIR / "luna_world_model_state.json"
VAULT_STATE_PATH = MEMORY_DIR / "luna_vault_state.json"
SOVEREIGN_EVOLUTION_STATE_PATH = MEMORY_DIR / "luna_sovereign_evolution_state.json"
FEDERATED_AGENT_REPORTS_PATH = MEMORY_DIR / "luna_federated_agent_reports.json"
SIMULATION_FORECASTS_PATH = MEMORY_DIR / "luna_simulation_forecasts.json"
RUNTIME_LAYER_MAP_PATH = MEMORY_DIR / "luna_runtime_layer_map.json"
SHADOW_DEFINITION_AUDIT_PATH = MEMORY_DIR / "luna_shadow_definition_audit.json"
WORKER_ROUTE_REGRESSION_PATH = LOGS_DIR / "luna_worker_route_regression.json"
MCP_DIR = PROJECT_DIR / "mcp"
MCP_MANIFEST_PATH = MCP_DIR / "luna_mcp_manifest.json"
MCP_RESOURCE_INDEX_PATH = MCP_DIR / "luna_mcp_resource_index.json"
MCP_CONTEXT_BUNDLE_PATH = MCP_DIR / "luna_mcp_context_bundle.json"
MCP_POLICY_PATH = MCP_DIR / "luna_mcp_policy.json"
MCP_README_PATH = MCP_DIR / "README.md"
CORE_BASELINE_STATUS_PATH = MEMORY_DIR / "luna_core_baseline_status.json"
OMEGA_BATCH2_STATE_PATH = MEMORY_DIR / "luna_omega_batch2_state.json"
OMEGA_BATCH2_FLAGS_PATH = MEMORY_DIR / "luna_omega_batch2_flags.json"
ALWAYS_ON_AUTONOMY_PATH = MEMORY_DIR / "luna_always_on_autonomy.json"
WATCHDOG_STATUS_PATH = MEMORY_DIR / "luna_guardian_status.json"
GUARDIAN_LOCK_PATH = MEMORY_DIR / "luna_guardian.lock.json"
THERMAL_GUARD_STATE_PATH = MEMORY_DIR / "luna_thermal_guard_state.json"
AUTONOMY_JOURNAL_PATH = MEMORY_DIR / "luna_autonomy_journal.jsonl"
SOVEREIGN_JOURNAL_PATH = MEMORY_DIR / "luna_sovereign_journal.json"
INTENT_LEDGER_PATH = MEMORY_DIR / "luna_intent_ledger.json"
TECHNICAL_DEBT_BACKLOG_PATH = MEMORY_DIR / "luna_technical_debt_backlog.json"
UNATTENDED_SELF_EDIT_INTERVAL_SECONDS = 180.0

# Hygiene/refactor whitelist lookups (constants kept here because the
# hygiene module imports them; full hygiene rules live in luna_hygiene.py).
LEGACY_HYGIENE_WHITELIST_BY_FILE = {
    "SurgeApp_Claude_Terminal.py": {"is_guided_improvement_command"},
    "worker.py": {"run_guided_self_improvement"},
}
DEBT_SCAN_LINE_THRESHOLD = 45
DEBT_RETRY_COOLDOWN_SECONDS = 600
DEBT_RETRY_STATE_PATH = MEMORY_DIR / "luna_debt_retry_state.json"
DEBT_SCAN_PROTECTED_TARGETS = {"verify_code_hygiene", "scan_for_technical_debt"}
FRACTAL_DOMAIN_THRESHOLDS = {"telemetry": 12, "verification": 80, "task_memory": 80}
INTERNAL_COUNCIL_COMPLEXITY_MARKERS = (
    "multi file",
    "multi-file",
    "both core files",
    "new feature",
    "net new feature",
    "architecture",
    "engine",
    "pipeline",
    "feature",
    "rewrite",
    "build",
)
INTERNAL_COUNCIL_HISTORY_LIMIT = 200
PROMPT_OPTIMIZER_SUCCESS_THRESHOLD = 50
PROMPT_OPTIMIZER_INTERVAL_DAYS = 7
PROMPT_OPTIMIZER_MANAGED_START = "# LUNA META OPTIMIZER START"
PROMPT_OPTIMIZER_MANAGED_END = "# LUNA META OPTIMIZER END"
DEFAULT_LUNA_SYSTEM_PROMPT = "I am Luna. I write deterministic, modular Python with verification-first discipline. I prefer small helpers, clear rollback paths, and bounded autonomous behavior."

HEARTBEAT_DEADLOCK_SECONDS = 10.0
STRATEGY_INTERVAL_SECONDS = 12.0

VERIFY_TIMEOUT_SECONDS = 8
WORKER_STALE_SECONDS = 12
HEARTBEAT_INTERVAL_SECONDS = 1.0
HEARTBEAT_RECOVERY_GRACE_SECONDS = max(3.0, HEARTBEAT_INTERVAL_SECONDS * 3.0)
AUTONOMY_INTERVAL_SECONDS = 20.0
MAX_SELF_HEAL_ATTEMPTS = 5

DIAGNOSTIC_PREFIX = "[LUNA-DIAG]"

SAFE_AUTONOMY_TARGETS = [
    PROJECT_DIR,
    LOGS_DIR,
    SOLUTIONS_DIR,
    TASKS_DIR,
    MEMORY_DIR,
    LUNA_MODULES_DIR,
]

ALLOWED_FILES = [
    str(PROJECT_DIR / "worker.py"),
    str(PROJECT_DIR / "SurgeApp_Claude_Terminal.py"),
    str(PROJECT_DIR / "LUNA_MASTER_CODEX_AND_MEMORY.md"),
    str(LUNA_TELEMETRY_MODULE_PATH),
    str(MEMORY_DIR / "LUNA_SYSTEM_PROMPT.txt"),
    str(MEMORY_DIR / "luna_core_memory.json"),
    str(MEMORY_DIR / "luna_user_profile.json"),
    str(MEMORY_DIR / "luna_task_memory.json"),
    str(MEMORY_DIR / "luna_tool_memory.json"),
    str(MEMORY_DIR / "luna_session_memory.json"),
    str(MEMORY_DIR / "luna_archive_memory.json"),
    str(LUNA_APPROVAL_QUEUE_PATH),
    str(LUNA_AUTONOMY_STATE_PATH),
    str(LUNA_MODULE_REGISTRY_PATH),
    str(SAFETY_RULES_PATH),
    str(HUMAN_CHECKIN_PATH),
    str(COUNCIL_HISTORY_PATH),
]

SELF_FIX_TRIGGERS = frozenset([
    "fix worker",
    "fix worker codex behavior",
    "do all of them fix all",
    "self analyze and fix worker",
    "self-analyze and fix worker",
    "analyze and fix worker",
    "fix all worker issues",
    "self fix worker",
    "self-fix worker",
    "analyze worker and fix",
    "repair worker",
])

GUIDED_IMPROVEMENT_TRIGGERS = frozenset([
    "guide improve worker",
    "guided self-improvement",
    "guided self improvement",
    "analyze and apply safe fixes",
    "guide worker improvements",
    "run guided worker loop",
])

IMPROVEMENT_MODE_TRIGGERS = frozenset([
    "improve worker performance",
    "optimize worker logic",
    "analyze weaknesses",
    "analyse weaknesses",
    "worker analysis",
    "analyze worker",
    "analyse worker",
])

MCP_ADOPTION_TRIGGERS = frozenset([
    "adopt mcp",
    "model context protocol",
    "mcp for luna",
    "mcp adoption",
])

SUPPORTED_TASK_TYPES = {
    "chat",
    "code_fix",
    "guided_improvement",
    "improvement_analysis",
    "mission_orchestration",
    "ui_patch",
    "memory_update",
    "system_action",
    "approval_response",
    "upgrade_proposal",
    "acquisition_request",
    "meta_decision",
    "self_upgrade_pipeline",
    "mcp_adoption",
}

MODE_ALIASES = {
    "self_fix": "self_fix",
    "self-fix": "self_fix",
    "guided_loop": "guided_loop",
    "guided-loop": "guided_loop",
    "guided_improvement": "guided_loop",
    "improvement": "improvement",
    "improvement_analysis": "improvement",
    "improvement-analysis": "improvement",
    "mission_kernel": "mission_kernel",
    "mission-kernel": "mission_kernel",
    "direct_target": "direct_target",
    "system_action": "system_action",
    "approval_response": "approval_response",
    "upgrade_proposal": "upgrade_proposal",
    "acquisition_request": "acquisition_request",
    "meta_decision": "meta_decision",
    "self_upgrade_pipeline": "self_upgrade_pipeline",
    "mcp_adoption": "mcp_adoption",
    "mcp-adoption": "mcp_adoption",
    "chat": "chat",
    "chat_response": "chat_response",
    "chat-response": "chat_response",
}

LUNA_EXECUTION_FAILURE = "[LUNA EXECUTION FAILURE]"
LUNA_IMPROVEMENT_FAILURE = "[LUNA IMPROVEMENT FAILURE]"
LUNA_PENDING_APPROVAL = "[LUNA PENDING APPROVAL]"
TARGET_FILE_DOES_NOT_EXIST = "target file does not exist"
CORE_STRUCTURAL_FILES = {
    str(PROJECT_DIR / "worker.py"),
    str(PROJECT_DIR / "SurgeApp_Claude_Terminal.py"),
}
PRIVACY_BLACKLIST = ("password", ".key", "cookie", "login", "license number", "contractor license")
DEFAULT_SAFETY_RULES = [
    "Stay inside D:\\SurgeApp unless an action is explicitly approval-gated.",
    "Never delete or overwrite live structural files directly.",
    "Always create backups before staged upgrades.",
    "Respect the kill switch immediately.",
    "Rate-limit autonomous maintenance and require human review for high-risk actions.",
]
