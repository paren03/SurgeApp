"""Phase 5L: Luna Delegated AI Approval Council foundation.

Stdlib only. Read-mostly. Local-only simulated reviewers — NO external API
calls in this phase. Builds the approval-packet / reviewer-response /
quorum / receipt / redaction layer that future phases will wire into
Director, Guardian, Worker, Aider Bridge, and Limited Routine Autonomy.

Hard rules in Phase 5L:
  * No external network calls. `allow_external_reviewers` MUST be false.
  * Receipts are append-only and gitignored.
  * Non-delegable actions never reach a council vote — they short-circuit to
    `deny` or `needs_human`.
  * Replay protection: every receipt carries a nonce + packet_hash + expiry;
    `find_valid_receipt` rejects mismatch / stale / non-approve.
  * Secret-like text is redacted before any reviewer sees a packet.

Tracked schema/policy:
  memory/luna_ai_council.schema.json
  memory/luna_ai_council_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_ai_council_approvals.jsonl
  memory/luna_ai_council_state.json
  memory/luna_ai_council_report.json
  memory/luna_ai_council_report.md
  memory/luna_delegated_approval_state.json

CLI:
  python -m luna_modules.luna_ai_council --self-test
  python -m luna_modules.luna_ai_council --simulate --tier 2 --action low_risk_additive
  python -m luna_modules.luna_ai_council --simulate --tier 3 --action medium_code_edit
  python -m luna_modules.luna_ai_council --simulate --tier 4 --action high_risk_core_edit
  python -m luna_modules.luna_ai_council --simulate --non-delegable
  python -m luna_modules.luna_ai_council --print-policy
  python -m luna_modules.luna_ai_council --write-report
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

VALID_ACTION_TYPES = (
    "read_only",
    "generated_artifact",
    "low_risk_additive",
    "medium_code_edit",
    "high_risk_core_edit",
    "emergency_repair",
    "non_delegable",
)

VALID_RESOURCE_STATUSES = (
    "normal", "light", "pause_high_intensity", "hibernate", "blocked", "unknown",
)

VALID_UPGRADE_GATE = ("allow", "deny", "needs_approval", "unknown")
VALID_SECRETS_SCAN = ("pass", "fail", "unknown")

VALID_REVIEWERS = ("local_luna", "local_safety", "local_qa", "chatgpt", "grok", "claude")
VALID_REVIEWER_MODES = ("simulated_local", "external_future")
VALID_DECISIONS = ("approve", "deny", "needs_human", "abstain")

VALID_QUORUM_DECISIONS = ("approve", "deny", "needs_human", "stale", "abstain")

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "allow_external_reviewers": False,
    "reviewer_pool": list(VALID_REVIEWERS),
    "simulated_reviewer_pool": ["local_luna", "local_safety", "local_qa"],
    "packet_expiry_minutes": 60,
    "quorum_timeout_seconds": 30,
    "stale_receipt_minutes": 60,
    "max_target_files_by_tier": {"0": 0, "1": 0, "2": 3, "3": 6, "4": 2, "5": 1},
    "max_diff_lines_by_tier": {"0": 0, "1": 0, "2": 200, "3": 400, "4": 120, "5": 60},
    "tier_rules": {
        "0": {"name": "read_only", "council_required": False, "allowed_action_types": ["read_only"]},
        "1": {"name": "generated_reports_memory_refresh", "council_required": False, "allowed_action_types": ["read_only", "generated_artifact"]},
        "2": {"name": "low_risk_additive", "council_required": True, "allowed_action_types": ["low_risk_additive"]},
        "3": {"name": "medium_code_edit", "council_required": True, "allowed_action_types": ["medium_code_edit"]},
        "4": {"name": "high_risk_core_edit", "council_required": True, "allowed_action_types": ["high_risk_core_edit"]},
        "5": {"name": "emergency_repair", "council_required": True, "allowed_action_types": ["emergency_repair"]},
    },
    "non_delegable_actions": [
        "delete_memory", "truncate_memory", "delete_logs", "truncate_logs",
        "delete_backups", "truncate_backups", "delete_uploads", "delete_queues",
        "delete_tasks", "delete_solutions", "change_identity", "change_personality",
        "change_goals", "leak_secrets", "leak_env", "leak_api_vault", "leak_token",
        "leak_key", "package_install", "external_network_outside_reviewer_apis",
        "git_push", "git_push_force", "replace_architecture",
        "broad_multi_file_rewrite", "disable_verifier_failures",
        "self_approve_council_policy",
    ],
    "secret_patterns": [
        r"(?i)\bAWS_ACCESS_KEY_ID\b",
        r"(?i)\bAWS_SECRET_ACCESS_KEY\b",
        r"(?i)\bOPENAI_API_KEY\b",
        r"(?i)\bANTHROPIC_API_KEY\b",
        r"(?i)\b(?:GROK|XAI)_API_KEY\b",
        r"(?i)\bSLACK_BOT_TOKEN\b",
        r"(?i)\bSTRIPE_(?:LIVE|TEST)_KEY\b",
        r"(?i)\bbearer\s+[A-Za-z0-9._\-]{8,}",
        r"(?i)\bapi[_-]?key\b\s*[:=]\s*[\"']?[A-Za-z0-9._\-]{8,}",
        r"(?i)\btoken\s*[:=]\s*[\"']?[A-Za-z0-9._\-]{8,}",
        r"(?i)\bpassword\s*[:=]\s*[\"']?[^\s\"'\n]{4,}",
        r"(?i)\bsecret\s*[:=]\s*[\"']?[^\s\"'\n]{4,}",
        r"\bsk-[A-Za-z0-9]{16,}",
        r"\bxoxb-[A-Za-z0-9-]{20,}",
        r"\bgh[pousr]_[A-Za-z0-9]{20,}",
        r"(?i)BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY",
    ],
    "secret_keyword_terms": [
        ".env", "api_key", "api-key", "apikey", "token", "bearer",
        "password", "secret",
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
        "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GROK_API_KEY", "XAI_API_KEY",
    ],
    "high_risk_paths": [
        "worker.py", "aider_bridge.py", "luna_guardian.py",
        "LaunchLuna.pyw", "SurgeApp_Claude_Terminal.py", "luna_start.pyw",
        "director_agent.py",
    ],
    "forbidden_paths": [
        "luna_modules/luna_hygiene.py", "luna_modules/luna_paths.py",
        "luna_modules/luna_routing.py", "luna_modules/luna_state.py",
        "memory/luna_personality_state.json", "memory/luna_active_goal.json",
        "Luna_Post_Repair_Verify.ps1", ".env",
    ],
    "high_confidence_deny_threshold": 70,
    "required_evidence_for_tier_4": ["sandbox_result", "rollback_plan", "verification_commands"],
}

DEFAULT_POLICY_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_ai_council_policy.json"
DEFAULT_RECEIPTS_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_ai_council_approvals.jsonl"


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_iso(ts: str) -> _dt.datetime | None:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        if ts.endswith("Z"):
            return _dt.datetime.fromisoformat(ts[:-1]).replace(tzinfo=_dt.timezone.utc)
        return _dt.datetime.fromisoformat(ts)
    except ValueError:
        return None


def make_approval_id(prefix: str = "apr") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def make_nonce() -> str:
    return uuid.uuid4().hex


def sha256_text(text: Any) -> str:
    if isinstance(text, bytes):
        return hashlib.sha256(text).hexdigest()
    return hashlib.sha256(str(text).encode("utf-8", errors="replace")).hexdigest()


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def write_json_atomic(path: Path | str, data: Any) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False), encoding="utf-8")
    os.replace(tmp, p)
    return p


def append_jsonl(path: Path | str, row: dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return p


def load_council_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_ai_council_policy.json"
    if not p.is_file():
        p = DEFAULT_POLICY_PATH
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = "module_fallback"
        out["_loaded_from_file"] = False
        out["allow_external_reviewers"] = False
        return out
    out = dict(_DEFAULT_POLICY)
    for k, v in raw.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    # Phase 5L hard rule — external reviewers stay off no matter what file says.
    out["allow_external_reviewers"] = False
    return out


# ---------- secret redaction ----------


def redact_secret_text(text: Any, policy: dict[str, Any] | None = None) -> str:
    pol = policy or _DEFAULT_POLICY
    if not isinstance(text, str):
        return text if isinstance(text, (int, float, bool)) else ""
    redacted = text
    for pat in pol.get("secret_patterns", []):
        try:
            redacted = re.sub(pat, "[REDACTED]", redacted)
        except re.error:
            continue
    # Redact env-style key=value lines too.
    for term in pol.get("secret_keyword_terms", []):
        try:
            redacted = re.sub(
                rf"(?i)({re.escape(term)})\s*[:=]\s*[\"']?[^\s\"'\n]+",
                r"\1=[REDACTED]",
                redacted,
            )
        except re.error:
            continue
    return redacted


def _redact_in(value: Any, policy: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return redact_secret_text(value, policy)
    if isinstance(value, list):
        return [_redact_in(v, policy) for v in value]
    if isinstance(value, dict):
        return {k: _redact_in(v, policy) for k, v in value.items()}
    return value


def redact_packet(packet: dict[str, Any], policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    if not isinstance(packet, dict):
        return packet
    cleaned: dict[str, Any] = {}
    for k, v in packet.items():
        if k in {"approval_id", "nonce", "task_id", "schema_version", "created_at", "expires_at", "redaction_applied"}:
            cleaned[k] = v
            continue
        cleaned[k] = _redact_in(v, pol)
    cleaned["redaction_applied"] = True
    return cleaned


# ---------- non-delegable classification ----------


def _path_in_list(path: str, candidates: Iterable[str]) -> bool:
    p = path.replace("\\", "/").lower()
    for c in candidates:
        if c.replace("\\", "/").lower() in p:
            return True
    return False


def classify_non_delegable(
    packet: dict[str, Any], policy: dict[str, Any] | None = None
) -> list[str]:
    pol = policy or _DEFAULT_POLICY
    flags: list[str] = list(packet.get("non_delegable_flags") or [])
    action = str(packet.get("action_type") or "").lower()
    summary = " ".join([
        str(packet.get("planned_change_summary") or ""),
        str(packet.get("diff_summary") or ""),
        str(packet.get("goal") or ""),
    ]).lower()
    targets = [str(t).replace("\\", "/").lower() for t in (packet.get("target_files") or [])]

    if action == "non_delegable":
        flags.append("explicit_non_delegable_action_type")

    forbid = pol.get("forbidden_paths", [])
    for t in targets:
        if _path_in_list(t, forbid):
            flags.append(f"forbidden_path:{t}")

    if any(t.endswith(".env") or "/.env" in t for t in targets) or ".env" in summary:
        flags.append("touches_dotenv")
    if "api_vault" in summary or any("api_vault" in t for t in targets):
        flags.append("touches_api_vault")
    if "personality" in summary or any("personality" in t for t in targets):
        flags.append("touches_personality")
    if "identity" in summary or any("identity" in t for t in targets):
        flags.append("touches_identity")
    if "luna_active_goal" in summary or any("luna_active_goal" in t for t in targets):
        flags.append("touches_active_goal")

    if "git push" in summary or "git_push" in summary or "force-push" in summary or "force push" in summary:
        flags.append("git_push")

    if any(kw in summary for kw in ("delete memory", "truncate memory", "wipe memory", "rm -rf memory", "rm -rf logs", "delete logs", "delete backups", "delete queues", "delete tasks", "delete solutions", "delete uploads")):
        flags.append("delete_destructive")

    if any(kw in summary for kw in ("pip install", "winget install", "apt install", "brew install", "npm install", "yarn add")):
        flags.append("package_install")

    if any(kw in summary for kw in ("disable verifier", "comment out [fail]", "weaken verifier")):
        flags.append("disable_verifier_failures")

    if "luna_ai_council_policy" in summary or any("luna_ai_council_policy" in t for t in targets):
        flags.append("self_approve_council_policy")

    if str(packet.get("secrets_scan") or "").lower() == "fail":
        flags.append("secrets_scan_fail")

    # de-dupe preserving order
    seen = set()
    out = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


# ---------- packet build / validate ----------


def build_approval_packet(
    *,
    goal: str,
    task_id: str,
    risk_tier: int,
    approval_tier_required: int,
    action_type: str,
    target_files: Iterable[str] | None = None,
    function_ranges: Iterable[Any] | None = None,
    planned_change_summary: str = "",
    diff_summary: str = "",
    sandbox_result: str = "",
    verification_commands: Iterable[str] | None = None,
    rollback_plan: str = "",
    secrets_scan: str = "unknown",
    resource_status: str = "unknown",
    upgrade_gate_decision: str = "unknown",
    capability_readiness: str = "unknown",
    non_delegable_flags: Iterable[str] | None = None,
    question: str = "Approve this action? yes/no with reason.",
    source: str = "test",
    policy: dict[str, Any] | None = None,
    redact: bool = True,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    if action_type not in VALID_ACTION_TYPES:
        action_type = "non_delegable"
    risk_tier_i = max(0, min(5, int(risk_tier)))
    tier_req = max(0, min(5, int(approval_tier_required)))
    secrets_scan_v = secrets_scan if secrets_scan in VALID_SECRETS_SCAN else "unknown"
    resource_v = resource_status if resource_status in VALID_RESOURCE_STATUSES else "unknown"
    gate_v = upgrade_gate_decision if upgrade_gate_decision in VALID_UPGRADE_GATE else "unknown"
    expiry_minutes = int(pol.get("packet_expiry_minutes", 60))
    created = _dt.datetime.now(_dt.timezone.utc)
    expires = created + _dt.timedelta(minutes=expiry_minutes)
    packet: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "approval_id": make_approval_id(),
        "nonce": make_nonce(),
        "created_at": created.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "goal": str(goal or ""),
        "task_id": str(task_id or ""),
        "risk_tier": risk_tier_i,
        "approval_tier_required": tier_req,
        "action_type": action_type,
        "target_files": [str(t).replace("\\", "/") for t in (target_files or [])],
        "function_ranges": list(function_ranges or []),
        "planned_change_summary": str(planned_change_summary or ""),
        "diff_summary": str(diff_summary or ""),
        "sandbox_result": str(sandbox_result or ""),
        "verification_commands": [str(v) for v in (verification_commands or [])],
        "rollback_plan": str(rollback_plan or ""),
        "secrets_scan": secrets_scan_v,
        "resource_status": resource_v,
        "upgrade_gate_decision": gate_v,
        "capability_readiness": str(capability_readiness or "unknown"),
        "non_delegable_flags": list(non_delegable_flags or []),
        "question": str(question or "Approve this action? yes/no with reason."),
        "redaction_applied": False,
        "source": str(source or "test"),
    }
    # Compute non-delegable flags from packet body.
    auto_flags = classify_non_delegable(packet, pol)
    for f in auto_flags:
        if f not in packet["non_delegable_flags"]:
            packet["non_delegable_flags"].append(f)
    if redact:
        packet = redact_packet(packet, pol)
    return packet


def validate_approval_packet(packet: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(packet, dict):
        return False, ["packet not a dict"]
    required = (
        "schema_version", "approval_id", "nonce", "created_at", "expires_at",
        "goal", "task_id", "risk_tier", "approval_tier_required", "action_type",
        "target_files", "function_ranges", "planned_change_summary",
        "diff_summary", "sandbox_result", "verification_commands",
        "rollback_plan", "secrets_scan", "resource_status",
        "upgrade_gate_decision", "capability_readiness", "non_delegable_flags",
        "question", "redaction_applied", "source",
    )
    for k in required:
        if k not in packet:
            errs.append(f"packet.{k} missing")
    if packet.get("action_type") not in VALID_ACTION_TYPES:
        errs.append(f"action_type invalid: {packet.get('action_type')!r}")
    if packet.get("secrets_scan") not in VALID_SECRETS_SCAN:
        errs.append(f"secrets_scan invalid: {packet.get('secrets_scan')!r}")
    if packet.get("resource_status") not in VALID_RESOURCE_STATUSES:
        errs.append(f"resource_status invalid: {packet.get('resource_status')!r}")
    if packet.get("upgrade_gate_decision") not in VALID_UPGRADE_GATE:
        errs.append(f"upgrade_gate_decision invalid: {packet.get('upgrade_gate_decision')!r}")
    if not isinstance(packet.get("redaction_applied"), bool):
        errs.append("redaction_applied must be bool")
    rt = packet.get("risk_tier")
    if not isinstance(rt, int) or not (0 <= rt <= 5):
        errs.append("risk_tier must be int 0..5")
    return (not errs), errs


# ---------- reviewer response build / validate ----------


def build_reviewer_response(
    *,
    reviewer: str,
    decision: str,
    confidence: int = 0,
    reviewer_mode: str = "simulated_local",
    risk_notes: Iterable[str] | None = None,
    required_changes: Iterable[str] | None = None,
    approval_conditions: Iterable[str] | None = None,
    packet_hash: str = "",
    nonce: str = "",
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "reviewer": reviewer if reviewer in VALID_REVIEWERS else "local_luna",
        "reviewer_mode": reviewer_mode if reviewer_mode in VALID_REVIEWER_MODES else "simulated_local",
        "decision": decision if decision in VALID_DECISIONS else "abstain",
        "confidence": max(0, min(100, int(confidence))),
        "risk_notes": list(risk_notes or []),
        "required_changes": list(required_changes or []),
        "approval_conditions": list(approval_conditions or []),
        "timestamp": now_iso(),
        "packet_hash": str(packet_hash or ""),
        "nonce": str(nonce or ""),
    }


def validate_reviewer_response(response: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(response, dict):
        return False, ["response not a dict"]
    for k in (
        "schema_version", "reviewer", "reviewer_mode", "decision", "confidence",
        "risk_notes", "required_changes", "approval_conditions",
        "timestamp", "packet_hash", "nonce",
    ):
        if k not in response:
            errs.append(f"response.{k} missing")
    if response.get("reviewer") not in VALID_REVIEWERS:
        errs.append(f"reviewer invalid: {response.get('reviewer')!r}")
    if response.get("reviewer_mode") not in VALID_REVIEWER_MODES:
        errs.append(f"reviewer_mode invalid: {response.get('reviewer_mode')!r}")
    if response.get("decision") not in VALID_DECISIONS:
        errs.append(f"decision invalid: {response.get('decision')!r}")
    c = response.get("confidence")
    if not isinstance(c, int) or not (0 <= c <= 100):
        errs.append("confidence must be int 0..100")
    return (not errs), errs


# ---------- local simulated reviewers ----------


def _packet_hash(packet: dict[str, Any]) -> str:
    canonical = json.dumps(packet, sort_keys=True, ensure_ascii=True)
    return sha256_text(canonical)


def _packet_is_complete(packet: dict[str, Any]) -> tuple[bool, list[str]]:
    missing: list[str] = []
    for f in ("goal", "task_id", "action_type", "planned_change_summary"):
        if not str(packet.get(f) or "").strip():
            missing.append(f)
    return (not missing), missing


def simulate_local_reviewer(
    packet: dict[str, Any],
    reviewer_name: str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    p_hash = _packet_hash(packet)
    nonce = str(packet.get("nonce") or "")

    def _resp(decision: str, confidence: int, notes: Iterable[str], conds: Iterable[str] | None = None, changes: Iterable[str] | None = None) -> dict[str, Any]:
        return build_reviewer_response(
            reviewer=reviewer_name,
            decision=decision,
            confidence=confidence,
            reviewer_mode="simulated_local",
            risk_notes=list(notes),
            approval_conditions=list(conds or []),
            required_changes=list(changes or []),
            packet_hash=p_hash,
            nonce=nonce,
        )

    if reviewer_name not in pol.get("simulated_reviewer_pool", []):
        return _resp("abstain", 0, [f"reviewer {reviewer_name!r} not in simulated pool"])

    risk_tier = int(packet.get("risk_tier") or 0)
    action = str(packet.get("action_type") or "")
    nd_flags = list(packet.get("non_delegable_flags") or [])
    secrets = str(packet.get("secrets_scan") or "")
    resource = str(packet.get("resource_status") or "")
    gate = str(packet.get("upgrade_gate_decision") or "")
    targets = list(packet.get("target_files") or [])
    sandbox = str(packet.get("sandbox_result") or "").strip()
    rollback = str(packet.get("rollback_plan") or "").strip()
    verify = list(packet.get("verification_commands") or [])

    high_risk = pol.get("high_risk_paths", [])
    forbid = pol.get("forbidden_paths", [])

    if reviewer_name == "local_luna":
        complete, missing = _packet_is_complete(packet)
        if not complete:
            return _resp("needs_human", 80, [f"packet incomplete: missing {missing}"])
        if nd_flags:
            return _resp("deny", 90, [f"non-delegable flags: {nd_flags}"])
        if action == "non_delegable":
            return _resp("deny", 90, ["explicit non_delegable action"])
        if gate == "deny":
            return _resp("deny", 80, ["upgrade gate denied"])
        if gate == "needs_approval" and risk_tier >= 3:
            return _resp("needs_human", 70, ["upgrade gate needs approval at tier>=3"])
        if risk_tier <= 1:
            return _resp("approve", 80, ["read-only/generated artifact path is fine"])
        if risk_tier == 2:
            return _resp("approve", 75, ["low-risk additive aligns with goal"])
        if risk_tier == 3:
            return _resp("approve", 65, ["medium code edit; verify sandbox + rollback evidence"])
        if risk_tier == 4:
            if sandbox and rollback and verify:
                return _resp("approve", 60, ["high-risk core edit with full evidence"], conds=["receipt single-use", "guardian must validate before apply"])
            return _resp("needs_human", 70, ["high-risk core edit missing evidence"])
        if risk_tier == 5:
            if action == "emergency_repair" and rollback:
                return _resp("approve", 60, ["emergency repair with rollback"], conds=["operator must witness apply"])
            return _resp("needs_human", 75, ["emergency repair without rollback"])
        return _resp("abstain", 0, [f"unknown risk_tier {risk_tier}"])

    if reviewer_name == "local_safety":
        if secrets == "fail":
            return _resp("deny", 95, ["secrets scan fail"])
        if nd_flags:
            return _resp("deny", 90, [f"non-delegable: {nd_flags}"])
        for t in targets:
            if _path_in_list(t, forbid):
                return _resp("deny", 95, [f"forbidden path target: {t}"])
        if resource == "blocked":
            if action in ("medium_code_edit", "high_risk_core_edit", "emergency_repair"):
                return _resp("needs_human", 80, ["resource blocked — defer code edits"])
            return _resp("approve", 60, ["resource blocked but action is read-only"])
        if risk_tier >= 3 and not rollback:
            return _resp("deny", 75, ["no rollback plan for tier>=3"])
        if risk_tier >= 4 and not sandbox:
            return _resp("needs_human", 70, ["no sandbox evidence for tier>=4"])
        if any(_path_in_list(t, high_risk) for t in targets) and risk_tier < 4:
            return _resp("needs_human", 70, ["high-risk path edited at tier<4"])
        if risk_tier <= 1:
            return _resp("approve", 80, ["safety: read-only/generated artifact"])
        return _resp("approve", 70, ["safety checks pass"])

    if reviewer_name == "local_qa":
        is_code_edit = action in ("medium_code_edit", "high_risk_core_edit", "emergency_repair")
        if is_code_edit and not verify:
            return _resp("needs_human", 75, ["no verification commands for code edit"])
        if risk_tier >= 3 and not str(packet.get("diff_summary") or "").strip():
            return _resp("needs_human", 70, ["no diff_summary for tier>=3"])
        if risk_tier >= 3 and not targets:
            return _resp("needs_human", 70, ["no target_files for tier>=3"])
        if risk_tier <= 1:
            return _resp("approve", 80, ["qa: nothing to test for read-only"])
        if risk_tier == 2:
            return _resp("approve", 70, ["qa: low-risk additive — verify present"])
        if risk_tier == 3:
            return _resp("approve", 60, ["qa: 2-of-3 quorum suffices for medium"])
        if risk_tier == 4:
            return _resp("approve", 55, ["qa: high-risk edit demands extra eyes"], conds=["sandbox must pass before apply"])
        if risk_tier == 5:
            return _resp("approve", 55, ["qa: emergency repair requires post-verify"])
        return _resp("abstain", 0, [f"qa: unknown risk_tier {risk_tier}"])

    return _resp("abstain", 0, [f"reviewer {reviewer_name!r} not implemented"])


def run_local_council_simulation(
    packet: dict[str, Any], policy: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    pol = policy or _DEFAULT_POLICY
    pool = list(pol.get("simulated_reviewer_pool") or _DEFAULT_POLICY["simulated_reviewer_pool"])
    return [simulate_local_reviewer(packet, name, pol) for name in pool]


# ---------- quorum ----------


def evaluate_quorum(
    packet: dict[str, Any],
    responses: list[dict[str, Any]],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    rule = ""
    reasons: list[str] = []
    if not isinstance(responses, list):
        responses = []

    expires = _parse_iso(str(packet.get("expires_at") or ""))
    if expires is not None and _dt.datetime.now(_dt.timezone.utc) > expires:
        return {
            "decision": "stale",
            "rule": "packet_expired",
            "responses_considered": 0,
            "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
            "reasons": [f"packet expired at {packet.get('expires_at')}"],
        }

    nd_flags = list(packet.get("non_delegable_flags") or [])
    if nd_flags:
        return {
            "decision": "needs_human",
            "rule": "non_delegable",
            "responses_considered": 0,
            "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
            "reasons": [f"non_delegable_flags={nd_flags}"],
        }

    if str(packet.get("secrets_scan") or "") == "fail":
        return {
            "decision": "deny",
            "rule": "secrets_scan_fail",
            "responses_considered": 0,
            "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
            "reasons": ["secrets scan failed; cannot delegate"],
        }

    valid_responses: list[dict[str, Any]] = []
    malformed = 0
    for r in responses:
        ok, _errs = validate_reviewer_response(r)
        if ok:
            valid_responses.append(r)
        else:
            malformed += 1
    if malformed and not valid_responses:
        return {
            "decision": "needs_human",
            "rule": "all_responses_malformed",
            "responses_considered": malformed,
            "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
            "reasons": ["all reviewer responses malformed"],
        }

    deny_threshold = int(pol.get("high_confidence_deny_threshold", 70))
    high_conf_deny = [r for r in valid_responses if r["decision"] == "deny" and int(r.get("confidence", 0)) >= deny_threshold]
    if high_conf_deny:
        rs = [f"{r['reviewer']}: {r.get('risk_notes')}" for r in high_conf_deny]
        return {
            "decision": "deny",
            "rule": "high_confidence_deny",
            "responses_considered": len(valid_responses),
            "approve_count": sum(1 for r in valid_responses if r["decision"] == "approve"),
            "deny_count": sum(1 for r in valid_responses if r["decision"] == "deny"),
            "needs_human_count": sum(1 for r in valid_responses if r["decision"] == "needs_human"),
            "abstain_count": sum(1 for r in valid_responses if r["decision"] == "abstain"),
            "reasons": [f"high-confidence deny by {rs}"],
        }

    risk_tier = int(packet.get("risk_tier") or 0)
    action = str(packet.get("action_type") or "")
    resource = str(packet.get("resource_status") or "")
    gate = str(packet.get("upgrade_gate_decision") or "")

    approve = sum(1 for r in valid_responses if r["decision"] == "approve")
    deny = sum(1 for r in valid_responses if r["decision"] == "deny")
    nh = sum(1 for r in valid_responses if r["decision"] == "needs_human")
    abst = sum(1 for r in valid_responses if r["decision"] == "abstain")
    counts = {
        "responses_considered": len(valid_responses),
        "approve_count": approve, "deny_count": deny,
        "needs_human_count": nh, "abstain_count": abst,
    }

    def _ret(decision: str, rule_text: str, why: list[str]) -> dict[str, Any]:
        return {"decision": decision, "rule": rule_text, "reasons": why, **counts}

    # Tier 0 / 1 short-circuit
    if risk_tier <= 1:
        if resource == "blocked":
            return _ret("needs_human", "tier01_resource_blocked", ["resource blocked"])
        if gate == "deny":
            return _ret("deny", "tier01_gate_deny", ["upgrade_gate=deny"])
        return _ret("approve", "tier01_no_council_needed", ["tier 0/1 short-circuit"])

    if risk_tier == 2:
        luna = next((r for r in valid_responses if r["reviewer"] == "local_luna"), None)
        safety = next((r for r in valid_responses if r["reviewer"] == "local_safety"), None)
        if luna and luna["decision"] == "approve" and (safety is None or safety["decision"] != "deny"):
            if resource == "blocked":
                return _ret("needs_human", "tier2_resource_blocked", ["resource blocked"])
            return _ret("approve", "tier2_local_luna_plus_safety_not_deny", ["luna approves, safety not deny"])
        return _ret("needs_human", "tier2_quorum_unmet", ["tier 2 needs local_luna approve + local_safety not deny"])

    if risk_tier == 3:
        if resource == "blocked":
            return _ret("needs_human", "tier3_resource_blocked", ["resource blocked"])
        if approve >= 2 and deny == 0:
            return _ret("approve", "tier3_2_of_3", [f"approve={approve} deny={deny}"])
        if deny > 0:
            return _ret("deny", "tier3_any_deny", [f"approve={approve} deny={deny}"])
        return _ret("needs_human", "tier3_quorum_unmet", [f"approve={approve} needs>=2"])

    if risk_tier == 4:
        if resource in ("blocked", "pause_high_intensity"):
            return _ret("needs_human", "tier4_resource_not_normal", [f"resource={resource}"])
        for ev in pol.get("required_evidence_for_tier_4", []):
            val = packet.get(ev)
            if not val or (isinstance(val, list) and not val) or (isinstance(val, str) and not val.strip()):
                return _ret("needs_human", "tier4_missing_evidence", [f"missing evidence: {ev}"])
        if approve >= len(valid_responses) and deny == 0 and len(valid_responses) >= 3:
            return _ret("approve", "tier4_unanimous", [f"all {approve} reviewers approve with full evidence"])
        if deny > 0:
            return _ret("deny", "tier4_any_deny", [f"deny>0 at tier 4"])
        return _ret("needs_human", "tier4_quorum_unmet", [f"approve={approve} need=all of {len(valid_responses)}"])

    if risk_tier == 5:
        if action != "emergency_repair":
            return _ret("needs_human", "tier5_action_not_emergency", [f"action_type={action}"])
        if approve >= len(valid_responses) and deny == 0 and len(valid_responses) >= 3 and str(packet.get("rollback_plan") or "").strip():
            return _ret("approve", "tier5_emergency_with_rollback", ["emergency repair unanimous + rollback"])
        return _ret("needs_human", "tier5_emergency_needs_operator", ["emergency repair without unanimous + rollback"])

    return _ret("needs_human", "unknown_tier", [f"unknown tier {risk_tier}"])


# ---------- approval receipt ----------


def build_approval_receipt(
    packet: dict[str, Any],
    responses: list[dict[str, Any]],
    quorum_result: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    p_hash = _packet_hash(packet)
    targets = list(packet.get("target_files") or [])
    target_blob = "|".join(sorted(targets))
    target_hash = sha256_text(target_blob)
    expiry_minutes = int(pol.get("stale_receipt_minutes", 60))
    created = _dt.datetime.now(_dt.timezone.utc)
    expires = created + _dt.timedelta(minutes=expiry_minutes)
    decision = str(quorum_result.get("decision") or "needs_human")
    valid_for_commands = list(packet.get("verification_commands") or [])
    rp_hash_input = "|".join([
        p_hash, target_hash, str(packet.get("nonce") or ""), decision,
    ])
    return {
        "schema_version": SCHEMA_VERSION,
        "receipt_id": f"rcpt_{uuid.uuid4().hex[:12]}",
        "approval_id": str(packet.get("approval_id") or ""),
        "task_id": str(packet.get("task_id") or ""),
        "nonce": str(packet.get("nonce") or ""),
        "packet_hash": p_hash,
        "target_files_hash": target_hash,
        "target_files": targets,
        "approved_action_type": str(packet.get("action_type") or ""),
        "approved_risk_tier": int(packet.get("risk_tier") or 0),
        "decision": decision,
        "quorum_summary": {k: quorum_result.get(k) for k in ("rule", "responses_considered", "approve_count", "deny_count", "needs_human_count", "abstain_count", "reasons")},
        "reviewer_decisions": [{"reviewer": r.get("reviewer"), "decision": r.get("decision"), "confidence": r.get("confidence")} for r in (responses or []) if isinstance(r, dict)],
        "created_at": created.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "valid_for_commands": valid_for_commands,
        "valid_for_target_files": targets,
        "valid_for_diff_hash": sha256_text(str(packet.get("diff_summary") or "")) if packet.get("diff_summary") else "",
        "replay_protection_hash": sha256_text(rp_hash_input),
        "conditions": _aggregate_conditions(responses),
        "invalidation_rules": [
            "expired",
            "nonce_mismatch_with_packet",
            "packet_hash_mismatch",
            "target_files_mismatch",
            "decision_not_approve",
            "non_delegable_action",
            "older_than_max_age_minutes",
        ],
    }


def _aggregate_conditions(responses: list[dict[str, Any]] | None) -> list[str]:
    out: list[str] = []
    seen = set()
    for r in responses or []:
        if not isinstance(r, dict):
            continue
        for c in r.get("approval_conditions") or []:
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def validate_approval_receipt(
    receipt: Any,
    packet: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(receipt, dict):
        return False, ["receipt not a dict"]
    required = (
        "schema_version", "receipt_id", "approval_id", "task_id", "nonce",
        "packet_hash", "target_files_hash", "target_files",
        "approved_action_type", "approved_risk_tier", "decision",
        "quorum_summary", "reviewer_decisions", "created_at", "expires_at",
        "valid_for_commands", "valid_for_target_files",
        "replay_protection_hash", "conditions", "invalidation_rules",
    )
    for k in required:
        if k not in receipt:
            errs.append(f"receipt.{k} missing")
    if receipt.get("decision") not in VALID_QUORUM_DECISIONS:
        errs.append(f"decision invalid: {receipt.get('decision')!r}")
    expires = _parse_iso(str(receipt.get("expires_at") or ""))
    if expires is not None and _dt.datetime.now(_dt.timezone.utc) > expires:
        errs.append("receipt expired")
    if packet is not None:
        if str(receipt.get("nonce") or "") != str(packet.get("nonce") or ""):
            errs.append("nonce mismatch with packet")
        if str(receipt.get("packet_hash") or "") != _packet_hash(packet):
            errs.append("packet_hash mismatch")
        if list(receipt.get("target_files") or []) != list(packet.get("target_files") or []):
            errs.append("target_files mismatch")
    return (not errs), errs


def append_approval_receipt(
    project_dir: Path | str,
    receipt: dict[str, Any],
) -> Path:
    pdir = Path(project_dir)
    p = pdir / "memory" / "luna_ai_council_approvals.jsonl"
    return append_jsonl(p, receipt)


def read_approval_receipts(
    project_dir: Path | str, limit: int = 100
) -> list[dict[str, Any]]:
    pdir = Path(project_dir)
    p = pdir / "memory" / "luna_ai_council_approvals.jsonl"
    out: list[dict[str, Any]] = []
    if not p.is_file():
        return out
    try:
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines()[-max(1, int(limit)):]:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if isinstance(rec, dict):
                    out.append(rec)
            except ValueError:
                continue
    except OSError:
        return out
    return out


def find_valid_receipt(
    project_dir: Path | str,
    approval_id: str | None = None,
    task_id: str | None = None,
    target_files: Iterable[str] | None = None,
    max_age_minutes: int | None = None,
    packet: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    pol = policy or _DEFAULT_POLICY
    receipts = read_approval_receipts(project_dir, limit=1000)
    if max_age_minutes is None:
        max_age_minutes = int(pol.get("stale_receipt_minutes", 60))
    target_set = set([t.replace("\\", "/") for t in (target_files or [])])
    now = _dt.datetime.now(_dt.timezone.utc)
    for rec in reversed(receipts):
        if approval_id and str(rec.get("approval_id") or "") != approval_id:
            continue
        if task_id and str(rec.get("task_id") or "") != task_id:
            continue
        if target_set and set(rec.get("target_files") or []) != target_set:
            continue
        if str(rec.get("decision") or "") != "approve":
            continue
        # non-delegable check via stored quorum_summary rule
        rule = str((rec.get("quorum_summary") or {}).get("rule") or "")
        if rule == "non_delegable":
            continue
        created = _parse_iso(str(rec.get("created_at") or ""))
        if created is not None:
            age_min = (now - created).total_seconds() / 60.0
            if age_min > max_age_minutes:
                continue
        expires = _parse_iso(str(rec.get("expires_at") or ""))
        if expires is not None and now > expires:
            continue
        if packet is not None:
            ok, _e = validate_approval_receipt(rec, packet=packet, policy=pol)
            if not ok:
                continue
        return rec
    return None


# ---------- rendering ----------


def render_council_report_markdown(
    packet: dict[str, Any],
    responses: list[dict[str, Any]],
    quorum_result: dict[str, Any],
    receipt: dict[str, Any] | None = None,
) -> str:
    lines: list[str] = []
    lines.append("# Luna Delegated AI Approval Council — Report")
    lines.append("")
    lines.append(f"- **approval_id**: `{packet.get('approval_id', '?')}`")
    lines.append(f"- **task_id**: `{packet.get('task_id', '?')}`")
    lines.append(f"- **goal**: {packet.get('goal', '')!r}")
    lines.append(f"- **action_type**: `{packet.get('action_type')}`")
    lines.append(f"- **risk_tier**: {packet.get('risk_tier')}")
    lines.append(f"- **approval_tier_required**: {packet.get('approval_tier_required')}")
    lines.append(f"- **secrets_scan**: `{packet.get('secrets_scan')}`")
    lines.append(f"- **resource_status**: `{packet.get('resource_status')}`")
    lines.append(f"- **upgrade_gate_decision**: `{packet.get('upgrade_gate_decision')}`")
    lines.append(f"- **non_delegable_flags**: `{packet.get('non_delegable_flags')}`")
    lines.append(f"- **redaction_applied**: `{packet.get('redaction_applied')}`")
    lines.append("")
    lines.append("## Reviewer decisions")
    for r in responses or []:
        if not isinstance(r, dict):
            continue
        lines.append(
            f"- **{r.get('reviewer')}** ({r.get('reviewer_mode')}) — "
            f"`{r.get('decision')}` confidence={r.get('confidence')}"
        )
        for n in r.get("risk_notes") or []:
            lines.append(f"  - note: {n}")
        for c in r.get("approval_conditions") or []:
            lines.append(f"  - condition: {c}")
    lines.append("")
    lines.append("## Quorum result")
    lines.append(f"- **decision**: `{quorum_result.get('decision')}`")
    lines.append(f"- **rule**: `{quorum_result.get('rule')}`")
    lines.append(f"- **counts**: approve={quorum_result.get('approve_count')} deny={quorum_result.get('deny_count')} needs_human={quorum_result.get('needs_human_count')} abstain={quorum_result.get('abstain_count')}")
    for r in quorum_result.get("reasons") or []:
        lines.append(f"  - reason: {r}")
    if receipt:
        lines.append("")
        lines.append("## Receipt")
        lines.append(f"- receipt_id: `{receipt.get('receipt_id')}`")
        lines.append(f"- decision: `{receipt.get('decision')}`")
        lines.append(f"- expires_at: {receipt.get('expires_at')}")
        lines.append(f"- valid_for_target_files: {receipt.get('valid_for_target_files')}")
        lines.append(f"- replay_protection_hash: `{receipt.get('replay_protection_hash')[:16]}...`")
    lines.append("")
    lines.append("> Phase 5L hard rule — local simulated reviewers only. No external API calls. Council is not yet wired into Director/Guardian/Worker/UI.")
    return "\n".join(lines) + "\n"


def write_council_report(
    project_dir: Path | str,
    packet: dict[str, Any],
    responses: list[dict[str, Any]],
    quorum_result: dict[str, Any],
    receipt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    json_p = mem / "luna_ai_council_report.json"
    md_p = mem / "luna_ai_council_report.md"
    state_p = mem / "luna_ai_council_state.json"
    delegated_state_p = mem / "luna_delegated_approval_state.json"
    project_root = pdir
    for p in (json_p, md_p, state_p, delegated_state_p):
        try:
            p.resolve().relative_to(project_root)
        except ValueError:
            raise ValueError(f"path escapes project root: {p}")
    write_json_atomic(json_p, {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "packet": packet,
        "reviewer_responses": responses,
        "quorum_result": quorum_result,
        "receipt": receipt or {},
    })
    md_p.parent.mkdir(parents=True, exist_ok=True)
    md_p.write_text(render_council_report_markdown(packet, responses, quorum_result, receipt), encoding="utf-8")
    write_json_atomic(state_p, {
        "schema_version": SCHEMA_VERSION,
        "last_generated_at": now_iso(),
        "last_approval_id": packet.get("approval_id"),
        "last_decision": (quorum_result or {}).get("decision"),
        "last_action_type": packet.get("action_type"),
        "last_risk_tier": packet.get("risk_tier"),
        "external_reviewers_enabled": False,
    })
    write_json_atomic(delegated_state_p, {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "summary": "Phase 5L local-only council; no external reviewers; no enforcement wired into runtime.",
        "phase_5L_complete": True,
    })
    return {
        "json": str(json_p),
        "md": str(md_p),
        "state": str(state_p),
        "delegated_state": str(delegated_state_p),
    }


# ---------- self-test ----------


_SAMPLE_GOAL = "Refactor the heartbeat helper for clarity"


def _build_sample_packet(tier: int, action: str, *, secrets_scan: str = "pass", with_evidence: bool = True, with_targets: bool = True, non_delegable: bool = False) -> dict[str, Any]:
    targets = (
        ["luna_modules/luna_logging.py"] if tier <= 2 and with_targets else
        ["luna_modules/luna_self_knowledge.py"] if tier == 3 and with_targets else
        ["worker.py"] if tier == 4 and with_targets else
        ["LUNA_STOP_NOW.flag"] if tier == 5 and with_targets else
        []
    )
    diff = "small additive helper" if tier <= 2 else "+12 -4 in scoped function"
    sandbox = "passed in sandbox" if with_evidence else ""
    rollback = "git checkout HEAD -- " + (targets[0] if targets else "<none>") if with_evidence else ""
    verify = ["python -m py_compile " + (targets[0] if targets else "")] if with_evidence and targets else []
    nd_flags = ["delete_destructive"] if non_delegable else []
    if non_delegable:
        action = "non_delegable"
    return build_approval_packet(
        goal=_SAMPLE_GOAL,
        task_id=f"task_{tier}_{action}",
        risk_tier=tier,
        approval_tier_required=tier,
        action_type=action,
        target_files=targets,
        function_ranges=[],
        planned_change_summary=f"Tier {tier} {action} sample for council simulation",
        diff_summary=diff,
        sandbox_result=sandbox,
        verification_commands=verify,
        rollback_plan=rollback,
        secrets_scan=secrets_scan,
        resource_status="normal",
        upgrade_gate_decision="needs_approval" if tier >= 3 else "allow",
        capability_readiness="controlled_autonomy_ready",
        non_delegable_flags=nd_flags,
        source="self_test",
    )


def self_test() -> int:
    pol = load_council_policy(None)
    pol["allow_external_reviewers"] = False
    samples = {
        "tier_2_low_risk_additive": _build_sample_packet(2, "low_risk_additive"),
        "tier_3_medium_code_edit": _build_sample_packet(3, "medium_code_edit"),
        "tier_4_high_risk_core_edit": _build_sample_packet(4, "high_risk_core_edit"),
        "non_delegable": _build_sample_packet(2, "low_risk_additive", non_delegable=True),
        "secrets_fail": _build_sample_packet(2, "low_risk_additive", secrets_scan="fail"),
    }
    summary: dict[str, Any] = {"ok": True, "decisions": {}, "redaction_proven": False}
    for name, packet in samples.items():
        ok, errs = validate_approval_packet(packet)
        if not ok:
            summary["ok"] = False
            summary["decisions"][name] = {"validate": "fail", "errors": errs}
            continue
        responses = run_local_council_simulation(packet, pol)
        for r in responses:
            ok2, e2 = validate_reviewer_response(r)
            if not ok2:
                summary["ok"] = False
                summary["decisions"][name] = {"validate_response": "fail", "errors": e2}
                break
        quorum = evaluate_quorum(packet, responses, pol)
        summary["decisions"][name] = {
            "decision": quorum["decision"],
            "rule": quorum["rule"],
            "approve": quorum["approve_count"],
            "deny": quorum["deny_count"],
            "nh": quorum["needs_human_count"],
        }
    # Redaction proof
    pkt = build_approval_packet(
        goal="leak secrets",
        task_id="t",
        risk_tier=2,
        approval_tier_required=2,
        action_type="low_risk_additive",
        target_files=["luna_modules/luna_logging.py"],
        planned_change_summary="OPENAI_API_KEY=sk-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa and bearer abc123def456 token=verysecretkey1234",
        diff_summary="api_key=topsecret123",
        rollback_plan="",
    )
    raw_inner = "OPENAI_API_KEY=sk-"
    summary["redaction_proven"] = (
        raw_inner not in json.dumps(pkt)
        and pkt.get("redaction_applied") is True
    )
    if not summary["redaction_proven"]:
        summary["ok"] = False
    print(json.dumps(summary, indent=2))
    return 0 if summary["ok"] else 1


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Delegated AI Approval Council foundation (Phase 5L)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--print-policy", action="store_true")
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--non-delegable", action="store_true")
    parser.add_argument("--tier", type=int, default=2)
    parser.add_argument("--action", default="low_risk_additive")
    parser.add_argument("--write-report", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    pdir = Path(args.project_dir)
    pol = load_council_policy(pdir)

    if args.print_policy:
        print(json.dumps(pol, indent=2, sort_keys=False))
        return 0

    if args.simulate:
        action = args.action if args.action in VALID_ACTION_TYPES else "low_risk_additive"
        tier = max(0, min(5, int(args.tier)))
        packet = _build_sample_packet(tier, action, non_delegable=bool(args.non_delegable))
        ok, errs = validate_approval_packet(packet)
        if not ok:
            print(json.dumps({"ok": False, "validate_packet_errors": errs}, indent=2))
            return 1
        responses = run_local_council_simulation(packet, pol)
        quorum = evaluate_quorum(packet, responses, pol)
        receipt = build_approval_receipt(packet, responses, quorum, pol)
        out = {
            "tier": tier,
            "action": action,
            "non_delegable": bool(args.non_delegable),
            "decision": quorum["decision"],
            "rule": quorum["rule"],
            "approve": quorum["approve_count"],
            "deny": quorum["deny_count"],
            "nh": quorum["needs_human_count"],
            "receipt_id": receipt["receipt_id"],
            "external_reviewers_enabled": pol["allow_external_reviewers"],
        }
        if args.write_report:
            try:
                paths = write_council_report(pdir, packet, responses, quorum, receipt)
                out["wrote"] = paths
            except ValueError as e:
                out["write_error"] = str(e)
        print(json.dumps(out, indent=2))
        return 0

    if args.write_report:
        # Headless write of a sample tier-2 council report.
        packet = _build_sample_packet(2, "low_risk_additive")
        responses = run_local_council_simulation(packet, pol)
        quorum = evaluate_quorum(packet, responses, pol)
        receipt = build_approval_receipt(packet, responses, quorum, pol)
        paths = write_council_report(pdir, packet, responses, quorum, receipt)
        print(json.dumps({"wrote": paths, "decision": quorum["decision"]}, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
