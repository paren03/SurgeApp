"""Phase 5M: Luna Approval Router foundation.

Stdlib only. Plan/evaluate-only. NEVER executes approved actions, NEVER
modifies target files, NEVER calls external reviewer APIs, NEVER runs Aider,
NEVER installs packages.

The router converts a goal/task/plan into a Phase 5L AI-Council approval
packet, runs the local simulated council, and (optionally, off by default)
appends an append-only receipt. It also verifies an existing receipt against
a planned action.

Hard rules in Phase 5M:
  * `safe_to_execute_now` is ALWAYS False in every router report.
  * `allow_execution` is ALWAYS False regardless of policy file contents.
  * `external_reviewers_enabled` is ALWAYS False.
  * Non-delegable actions short-circuit to `blocked` / `needs_human` and
    never reach a council vote.
  * Optional Phase 5 modules import with try/except; missing modules degrade
    gracefully.

Tracked schema/policy:
  memory/luna_approval_router.schema.json
  memory/luna_approval_router_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_approval_router_report.json
  memory/luna_approval_router_report.md
  memory/luna_approval_router_state.json
  memory/luna_approval_requests.jsonl
  memory/luna_approval_receipt_checks.jsonl

CLI:
  python -m luna_modules.luna_approval_router --self-test
  python -m luna_modules.luna_approval_router --request "..." --action ... --target ... --dry-run
  python -m luna_modules.luna_approval_router --write-report --request "..." --action ...
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Optional Phase 5 imports — wrapped to keep router working if missing.
try:  # pragma: no cover
    from luna_modules import luna_ai_council as _ai_council  # type: ignore
except Exception:  # pragma: no cover
    _ai_council = None
try:  # pragma: no cover
    from luna_modules import luna_upgrade_gate as _upgrade_gate  # type: ignore
except Exception:  # pragma: no cover
    _upgrade_gate = None
try:  # pragma: no cover
    from luna_modules import luna_task_graph as _task_graph  # type: ignore
except Exception:  # pragma: no cover
    _task_graph = None
try:  # pragma: no cover
    from luna_modules import luna_sandbox as _sandbox  # type: ignore
except Exception:  # pragma: no cover
    _sandbox = None
try:  # pragma: no cover
    from luna_modules import luna_resource_monitor as _resource_monitor  # type: ignore
except Exception:  # pragma: no cover
    _resource_monitor = None
try:  # pragma: no cover
    from luna_modules import luna_capability_scorecard as _scorecard  # type: ignore
except Exception:  # pragma: no cover
    _scorecard = None
try:  # pragma: no cover
    from luna_modules import luna_change_ledger as _change_ledger  # type: ignore
except Exception:  # pragma: no cover
    _change_ledger = None
try:  # pragma: no cover
    from luna_modules import luna_limited_autonomy as _limited_autonomy  # type: ignore
except Exception:  # pragma: no cover
    _limited_autonomy = None


VALID_REQUEST_ACTIONS = (
    "read_only",
    "generated_artifact",
    "low_risk_additive",
    "medium_code_edit",
    "high_risk_core_edit",
    "emergency_repair",
    "non_delegable",
    "unknown",
)

VALID_RESOURCE_STATUSES = (
    "normal", "light", "pause_high_intensity", "hibernate", "blocked", "unknown",
)

VALID_UPGRADE_GATE = ("allow", "deny", "needs_approval", "unknown")

VALID_SOURCES = ("operator", "director", "limited_autonomy", "test")

VALID_ROUTING_DECISIONS = (
    "not_required", "approved", "denied", "needs_human", "blocked", "stale", "dry_run",
)

# Maps requested_action to council action_type when needed.
_ACTION_TO_COUNCIL = {
    "read_only": "read_only",
    "generated_artifact": "generated_artifact",
    "low_risk_additive": "low_risk_additive",
    "medium_code_edit": "medium_code_edit",
    "high_risk_core_edit": "high_risk_core_edit",
    "emergency_repair": "emergency_repair",
    "non_delegable": "non_delegable",
    "unknown": "non_delegable",
}

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "default_source": "operator",
    "write_receipts_default": False,
    "write_reports_default": True,
    "allow_execution": False,
    "external_reviewers_enabled": False,
    "require_sandbox_for_tier": {"3": True, "4": True},
    "require_rollback_for_tier": {"2": True, "3": True, "4": True},
    "require_verification_for_tier": {"2": True, "3": True, "4": True},
    "generated_artifact_actions": ["read_only", "generated_artifact"],
    "code_edit_actions": [
        "low_risk_additive", "medium_code_edit",
        "high_risk_core_edit", "emergency_repair",
    ],
    "non_delegable_actions": [
        "delete_memory", "truncate_memory", "delete_logs", "delete_backups",
        "delete_uploads", "delete_queues", "delete_tasks", "delete_solutions",
        "change_identity", "change_personality", "change_goals",
        "leak_secrets", "package_install",
        "external_network_outside_reviewer_apis", "git_push", "git_push_force",
        "replace_architecture", "broad_multi_file_rewrite",
        "disable_verifier_failures", "self_approve_council_policy",
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
    "target_file_limits": {"0": 0, "1": 0, "2": 3, "3": 6, "4": 2, "5": 1},
    "receipt_max_age_minutes": 60,
    "default_request_expiry_minutes": 60,
}

DEFAULT_POLICY_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_approval_router_policy.json"


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_request_id(prefix: str = "req") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def sha256_json(data: Any) -> str:
    canonical = json.dumps(data, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8", errors="replace")).hexdigest()


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


def load_router_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_approval_router_policy.json"
    if not p.is_file():
        p = DEFAULT_POLICY_PATH
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = "module_fallback"
        out["_loaded_from_file"] = False
    else:
        out = dict(_DEFAULT_POLICY)
        for k, v in raw.items():
            out[k] = v
        out["_source"] = str(p)
        out["_loaded_from_file"] = True
    # Phase 5M hard rules.
    out["allow_execution"] = False
    out["external_reviewers_enabled"] = False
    return out


# ---------- normalization + inference ----------


def normalize_target_files(target_files: Any) -> list[str]:
    if not target_files:
        return []
    if isinstance(target_files, str):
        target_files = [target_files]
    seen: set[str] = set()
    out: list[str] = []
    for t in target_files:
        if not t:
            continue
        norm = str(t).replace("\\", "/").lstrip("./")
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _path_in(path: str, candidates: Iterable[str]) -> bool:
    p = path.replace("\\", "/").lower()
    for c in candidates or []:
        if c.replace("\\", "/").lower() in p:
            return True
    return False


_NON_DELEGABLE_KEYWORDS = (
    ("delete memory", "delete_memory"),
    ("truncate memory", "truncate_memory"),
    ("wipe memory", "delete_memory"),
    ("rm -rf memory", "delete_memory"),
    ("delete logs", "delete_logs"),
    ("delete backups", "delete_backups"),
    ("delete uploads", "delete_uploads"),
    ("delete queues", "delete_queues"),
    ("delete tasks", "delete_tasks"),
    ("delete solutions", "delete_solutions"),
    ("change identity", "change_identity"),
    ("change personality", "change_personality"),
    ("modify personality", "change_personality"),
    ("change goals", "change_goals"),
    ("modify goals", "change_goals"),
    ("leak secret", "leak_secrets"),
    ("pip install", "package_install"),
    ("npm install", "package_install"),
    ("winget install", "package_install"),
    ("apt install", "package_install"),
    ("brew install", "package_install"),
    ("git push", "git_push"),
    ("force-push", "git_push_force"),
    ("git push --force", "git_push_force"),
    ("disable verifier", "disable_verifier_failures"),
    ("comment out [fail]", "disable_verifier_failures"),
    ("approve council policy", "self_approve_council_policy"),
    ("replace architecture", "replace_architecture"),
)


def _classify_non_delegable(
    goal: str,
    target_files: list[str],
    requested_action: str,
    policy: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    text = " ".join([goal or "", requested_action or "", " ".join(target_files or [])]).lower()
    if requested_action == "non_delegable":
        flags.append("explicit_non_delegable_request")
    forbid = policy.get("forbidden_paths") or []
    for t in target_files:
        if _path_in(t, forbid):
            flags.append(f"forbidden_path:{t}")
        if t.endswith(".env") or "/.env" in t:
            flags.append("touches_dotenv")
        if "luna_personality_state" in t.lower():
            flags.append("change_personality")
        if "luna_active_goal" in t.lower():
            flags.append("change_goals")
        if "luna_ai_council_policy" in t.lower():
            flags.append("self_approve_council_policy")
        if t.lower().endswith("/luna_post_repair_verify.ps1") or t.lower() == "luna_post_repair_verify.ps1":
            flags.append("disable_verifier_failures")
    for kw, tag in _NON_DELEGABLE_KEYWORDS:
        if kw in text:
            flags.append(tag)
    # Dedupe preserving order.
    seen = set()
    out: list[str] = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def infer_action_type(
    goal: str,
    target_files: Iterable[str] | None = None,
    requested_action: str = "",
    policy: dict[str, Any] | None = None,
) -> str:
    pol = policy or _DEFAULT_POLICY
    targets = normalize_target_files(list(target_files or []))
    nd = _classify_non_delegable(goal, targets, requested_action, pol)
    if nd:
        return "non_delegable"
    if requested_action and requested_action in VALID_REQUEST_ACTIONS:
        if requested_action == "unknown":
            # Probe further via goal/targets.
            pass
        else:
            return requested_action
    text = (goal or "").lower()
    high_risk = pol.get("high_risk_paths") or []
    forbid = pol.get("forbidden_paths") or []
    for t in targets:
        if _path_in(t, forbid):
            return "non_delegable"
    if any(_path_in(t, high_risk) for t in targets):
        return "high_risk_core_edit"
    if any(t.startswith("memory/") for t in targets):
        return "generated_artifact"
    if any(t.startswith("luna_modules/") and t.endswith(".py") for t in targets):
        if "refactor" in text or "rewrite" in text or "edit" in text:
            return "medium_code_edit"
        return "low_risk_additive"
    if "report" in text or "summary" in text or "scorecard" in text or "brief" in text:
        return "generated_artifact"
    if "read" in text or "inspect" in text or "audit" in text:
        return "read_only"
    return "unknown"


def infer_approval_tier(
    goal: str,
    target_files: Iterable[str] | None = None,
    requested_action: str = "",
    upgrade_gate_decision: str | None = None,
    policy: dict[str, Any] | None = None,
) -> int:
    pol = policy or _DEFAULT_POLICY
    action = infer_action_type(goal, target_files, requested_action, pol)
    tier_map = {
        "read_only": 0,
        "generated_artifact": 1,
        "low_risk_additive": 2,
        "medium_code_edit": 3,
        "high_risk_core_edit": 4,
        "emergency_repair": 5,
        "non_delegable": 4,
        "unknown": 3,
    }
    base = tier_map.get(action, 3)
    if upgrade_gate_decision == "deny":
        base = max(base, 4)
    return base


# ---------- evidence collection ----------


def collect_context_evidence(
    project_dir: Path | str,
    goal: str,
    target_files: Iterable[str] | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir)
    targets = normalize_target_files(list(target_files or []))
    out: dict[str, Any] = {
        "project_dir": str(pdir).replace("\\", "/"),
        "goal": str(goal or ""),
        "target_files": targets,
        "warnings": [],
        "blockers": [],
    }

    # Git clean
    try:
        import subprocess as _subp
        proc = _subp.run(
            ["git", "status", "--porcelain"],
            cwd=str(pdir), capture_output=True, text=True, timeout=15,
        )
        text = proc.stdout or ""
        tracked_dirty = [ln for ln in text.splitlines() if ln and not ln.startswith("??")]
        out["git_clean"] = len(tracked_dirty) == 0
        if tracked_dirty:
            out["warnings"].append(f"git_dirty:{len(tracked_dirty)}")
    except Exception as e:
        out["git_clean"] = None
        out["warnings"].append(f"git_status_error:{type(e).__name__}")

    # Verifier
    logs = pdir / "logs"
    verifier_status = "unknown"
    if logs.is_dir():
        matches = sorted(
            logs.glob("luna_post_repair_verify_*.txt"),
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
        )
        if matches:
            try:
                raw = matches[-1].read_bytes()
                if raw.startswith(b"\xff\xfe"):
                    text = raw.decode("utf-16-le", errors="replace")
                elif raw.startswith(b"\xef\xbb\xbf"):
                    text = raw.decode("utf-8-sig", errors="replace")
                else:
                    text = raw.decode("utf-8", errors="replace")
                if "No hard failures found" in text and "No warnings found" in text:
                    verifier_status = "clean"
                elif "[FAIL]" in text:
                    verifier_status = "fail"
                elif "[WARN]" in text:
                    verifier_status = "warn"
                else:
                    verifier_status = "unknown"
            except OSError:
                verifier_status = "unknown"
    out["verifier_status"] = verifier_status
    if verifier_status == "fail":
        out["blockers"].append("verifier_fail")

    # Resource status
    res_status = "unknown"
    if _resource_monitor is not None:
        try:
            snap = _resource_monitor.build_resource_snapshot(pdir)
            res_status = str(snap.get("recommended_mode") or "unknown")
        except Exception as e:
            out["warnings"].append(f"resource_monitor_error:{type(e).__name__}")
    out["resource_status"] = res_status
    if res_status == "blocked":
        out["blockers"].append("resource_blocked")

    # Capability scorecard
    readiness = "unknown"
    if _scorecard is not None:
        try:
            sc = _scorecard.build_capability_scorecard(pdir)
            readiness = str(sc.get("readiness_level") or "unknown")
        except Exception as e:
            out["warnings"].append(f"scorecard_error:{type(e).__name__}")
    out["capability_readiness"] = readiness

    # Upgrade gate
    gate = "unknown"
    if _upgrade_gate is not None:
        try:
            # Use a lightweight hypothetical proposal so the gate gives a tier read.
            sample = {
                "plan_id": "router_probe",
                "title": (goal or "router probe")[:80],
                "actor": "luna_approval_router",
                "target_files": targets or ["luna_modules/luna_logging.py"],
                "line_ranges": {(targets[0] if targets else "luna_modules/luna_logging.py"): [[60, 65]]},
                "action_type": "edit",
                "expected_diff_type": "small_edit",
                "risk_level": "low",
                "approval_tier": 2,
                "diff_stats": {"files_changed": 1, "insertions": 1, "deletions": 1},
                "verification_commands": ["python -m py_compile " + (targets[0] if targets else "luna_modules/luna_logging.py")],
                "rollback_plan": "git checkout HEAD -- " + (targets[0] if targets else "luna_modules/luna_logging.py"),
                "install_commands": [],
                "external_network": False,
                "touches_personality_or_goals": False,
                "touches_memory_content": False,
                "touches_runtime_queue": False,
                "operator_approved": False,
            }
            decision = _upgrade_gate.evaluate_upgrade_proposal(sample)
            gate = str(decision.get("decision") or "unknown")
        except Exception as e:
            out["warnings"].append(f"upgrade_gate_error:{type(e).__name__}")
    out["upgrade_gate_decision"] = gate

    # Existing receipt count
    try:
        rcpt_path = pdir / "memory" / "luna_ai_council_approvals.jsonl"
        out["existing_receipt_count"] = (
            sum(1 for _ in rcpt_path.read_text(encoding="utf-8", errors="replace").splitlines() if _.strip())
            if rcpt_path.is_file() else 0
        )
    except OSError:
        out["existing_receipt_count"] = 0

    return out


# ---------- request build / validate ----------


def build_router_request(
    goal: str,
    target_files: Iterable[str] | None = None,
    requested_action: str = "",
    source: str = "operator",
    task_id: str = "",
    planned_change_summary: str = "",
    diff_summary: str = "",
    verification_commands: Iterable[str] | None = None,
    rollback_plan: str = "",
    sandbox_report_path: str = "",
    upgrade_gate_decision: str = "unknown",
    resource_status: str = "unknown",
    capability_readiness: str = "unknown",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if requested_action not in VALID_REQUEST_ACTIONS:
        requested_action = "unknown"
    if source not in VALID_SOURCES:
        source = "operator"
    if upgrade_gate_decision not in VALID_UPGRADE_GATE:
        upgrade_gate_decision = "unknown"
    if resource_status not in VALID_RESOURCE_STATUSES:
        resource_status = "unknown"
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": make_request_id(),
        "created_at": now_iso(),
        "goal": str(goal or ""),
        "task_id": str(task_id or ""),
        "source": source,
        "requested_action": requested_action,
        "target_files": normalize_target_files(list(target_files or [])),
        "planned_change_summary": str(planned_change_summary or ""),
        "diff_summary": str(diff_summary or ""),
        "verification_commands": [str(v) for v in (verification_commands or [])],
        "rollback_plan": str(rollback_plan or ""),
        "sandbox_report_path": str(sandbox_report_path or ""),
        "upgrade_gate_decision": upgrade_gate_decision,
        "resource_status": resource_status,
        "capability_readiness": str(capability_readiness or "unknown"),
        "metadata": dict(metadata or {}),
    }


def validate_router_request(request: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(request, dict):
        return False, ["request not a dict"]
    required = (
        "schema_version", "request_id", "created_at", "goal", "task_id",
        "source", "requested_action", "target_files", "planned_change_summary",
        "diff_summary", "verification_commands", "rollback_plan",
        "sandbox_report_path", "upgrade_gate_decision", "resource_status",
        "capability_readiness", "metadata",
    )
    for k in required:
        if k not in request:
            errs.append(f"request.{k} missing")
    if request.get("source") not in VALID_SOURCES:
        errs.append(f"source invalid: {request.get('source')!r}")
    if request.get("requested_action") not in VALID_REQUEST_ACTIONS:
        errs.append(f"requested_action invalid: {request.get('requested_action')!r}")
    if not isinstance(request.get("target_files"), list):
        errs.append("target_files must be list")
    if not str(request.get("goal") or "").strip():
        errs.append("goal must not be empty")
    return (not errs), errs


# ---------- packet build ----------


def build_packet_from_request(
    project_dir: Path | str,
    request: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_router_policy(project_dir)
    targets = normalize_target_files(request.get("target_files") or [])
    action = infer_action_type(request.get("goal", ""), targets, request.get("requested_action", ""), pol)
    tier = infer_approval_tier(
        request.get("goal", ""), targets, request.get("requested_action", ""),
        upgrade_gate_decision=request.get("upgrade_gate_decision"),
        policy=pol,
    )
    council_action = _ACTION_TO_COUNCIL.get(action, "non_delegable")
    nd_flags = _classify_non_delegable(
        request.get("goal", ""), targets, request.get("requested_action", ""), pol,
    )
    if _ai_council is None:
        # Synthetic packet matching Phase 5L shape so downstream tools still work.
        synth = {
            "schema_version": 1,
            "approval_id": make_request_id("apr"),
            "nonce": uuid.uuid4().hex,
            "created_at": now_iso(),
            "expires_at": now_iso(),
            "goal": str(request.get("goal") or ""),
            "task_id": str(request.get("task_id") or ""),
            "risk_tier": tier,
            "approval_tier_required": tier,
            "action_type": council_action,
            "target_files": targets,
            "function_ranges": [],
            "planned_change_summary": str(request.get("planned_change_summary") or ""),
            "diff_summary": str(request.get("diff_summary") or ""),
            "sandbox_result": str(request.get("sandbox_report_path") or ""),
            "verification_commands": list(request.get("verification_commands") or []),
            "rollback_plan": str(request.get("rollback_plan") or ""),
            "secrets_scan": "unknown",
            "resource_status": str(request.get("resource_status") or "unknown"),
            "upgrade_gate_decision": str(request.get("upgrade_gate_decision") or "unknown"),
            "capability_readiness": str(request.get("capability_readiness") or "unknown"),
            "non_delegable_flags": list(nd_flags),
            "question": "Approve this action? yes/no with reason.",
            "redaction_applied": False,
            "source": str(request.get("source") or "test"),
        }
        return synth
    return _ai_council.build_approval_packet(
        goal=request.get("goal", ""),
        task_id=request.get("task_id", ""),
        risk_tier=tier,
        approval_tier_required=tier,
        action_type=council_action,
        target_files=targets,
        planned_change_summary=request.get("planned_change_summary", ""),
        diff_summary=request.get("diff_summary", ""),
        sandbox_result=request.get("sandbox_report_path", ""),
        verification_commands=request.get("verification_commands", []),
        rollback_plan=request.get("rollback_plan", ""),
        secrets_scan="unknown",
        resource_status=request.get("resource_status", "unknown"),
        upgrade_gate_decision=request.get("upgrade_gate_decision", "unknown"),
        capability_readiness=request.get("capability_readiness", "unknown"),
        non_delegable_flags=nd_flags,
        source=request.get("source", "test"),
        redact=True,
    )


# ---------- council evaluation ----------


def evaluate_request_with_local_council(
    project_dir: Path | str,
    request: dict[str, Any],
    write_receipt: bool = False,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_router_policy(project_dir)
    pdir = Path(project_dir)
    packet = build_packet_from_request(pdir, request, pol)
    if _ai_council is None:
        # No council module — degrade to needs_human.
        responses: list[dict[str, Any]] = []
        quorum = {
            "decision": "needs_human",
            "rule": "ai_council_unavailable",
            "responses_considered": 0,
            "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
            "reasons": ["luna_ai_council module unavailable"],
        }
        receipt = {}
    else:
        try:
            council_pol = _ai_council.load_council_policy(pdir)
        except Exception:
            council_pol = None
        responses = _ai_council.run_local_council_simulation(packet, council_pol)
        quorum = _ai_council.evaluate_quorum(packet, responses, council_pol)
        receipt = _ai_council.build_approval_receipt(packet, responses, quorum, council_pol)
        if write_receipt and quorum.get("decision") == "approve":
            try:
                _ai_council.append_approval_receipt(pdir, receipt)
            except OSError:
                pass
    return {
        "packet": packet,
        "responses": responses,
        "quorum_result": quorum,
        "receipt": receipt,
    }


# ---------- routing ----------


def _decision_from_quorum(quorum_decision: str) -> str:
    mapping = {
        "approve": "approved",
        "deny": "denied",
        "needs_human": "needs_human",
        "stale": "stale",
        "abstain": "needs_human",
    }
    return mapping.get(str(quorum_decision or ""), "needs_human")


def _missing_evidence(
    request: dict[str, Any],
    tier: int,
    policy: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    tier_key = str(tier)
    needs_sandbox = bool(policy.get("require_sandbox_for_tier", {}).get(tier_key))
    needs_rollback = bool(policy.get("require_rollback_for_tier", {}).get(tier_key))
    needs_verify = bool(policy.get("require_verification_for_tier", {}).get(tier_key))
    if needs_sandbox and not str(request.get("sandbox_report_path") or "").strip():
        out.append("sandbox_report_path")
    if needs_rollback and not str(request.get("rollback_plan") or "").strip():
        out.append("rollback_plan")
    if needs_verify and not list(request.get("verification_commands") or []):
        out.append("verification_commands")
    return out


def route_approval_request(
    project_dir: Path | str,
    request: dict[str, Any],
    dry_run: bool = True,
    write_report: bool = False,
    write_receipt: bool = False,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_router_policy(project_dir)
    pdir = Path(project_dir)

    ok, errs = validate_router_request(request)
    if not ok:
        report = _build_router_report(
            request=request if isinstance(request, dict) else {},
            action_type="non_delegable",
            tier=4,
            decision="blocked",
            packet={}, responses=[], quorum={"decision": "needs_human", "rule": "request_validation_failed", "reasons": errs},
            receipt={}, receipt_valid=False,
            non_delegable_flags=["request_validation_failed"],
            blockers=errs,
            warnings=[],
            recommended_next_action="fix_request_payload",
        )
        if write_report:
            _persist_router_report(pdir, report)
        return report

    targets = normalize_target_files(request.get("target_files") or [])
    request["target_files"] = targets

    action_type = infer_action_type(request["goal"], targets, request.get("requested_action", ""), pol)
    tier = infer_approval_tier(
        request["goal"], targets, request.get("requested_action", ""),
        upgrade_gate_decision=request.get("upgrade_gate_decision"),
        policy=pol,
    )

    nd_flags = _classify_non_delegable(
        request["goal"], targets, request.get("requested_action", ""), pol,
    )

    # Append the request as a planning record (always allowed; gitignored).
    try:
        append_jsonl(
            pdir / "memory" / "luna_approval_requests.jsonl",
            {
                "ts": now_iso(),
                "request_id": request["request_id"],
                "task_id": request.get("task_id"),
                "goal": request.get("goal"),
                "source": request.get("source"),
                "action_type": action_type,
                "tier": tier,
                "target_files": targets,
                "non_delegable_flags": nd_flags,
                "dry_run": bool(dry_run),
            },
        )
    except OSError:
        pass

    operator_fallback = {
        "could_enqueue_human_approval": True,
        "suggested_reason": f"action_type={action_type} tier={tier} flags={nd_flags}",
    }

    # Non-delegable short-circuit.
    if nd_flags or action_type == "non_delegable":
        report = _build_router_report(
            request=request,
            action_type="non_delegable",
            tier=max(tier, 4),
            decision="blocked",
            packet={}, responses=[],
            quorum={
                "decision": "needs_human",
                "rule": "non_delegable_router",
                "responses_considered": 0,
                "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
                "reasons": [f"non_delegable_flags={nd_flags or [action_type]}"],
            },
            receipt={}, receipt_valid=False,
            non_delegable_flags=nd_flags or [action_type],
            blockers=[],
            warnings=[],
            recommended_next_action="route_to_operator_for_explicit_authorization",
            operator_queue_fallback=operator_fallback,
        )
        if write_report:
            _persist_router_report(pdir, report)
        return report

    # Tier 0/1 short-circuit (no council needed).
    if tier <= 1:
        report = _build_router_report(
            request=request,
            action_type=action_type,
            tier=tier,
            decision="not_required",
            packet={}, responses=[],
            quorum={
                "decision": "approve",
                "rule": "tier01_no_council_needed",
                "responses_considered": 0,
                "approve_count": 0, "deny_count": 0, "needs_human_count": 0, "abstain_count": 0,
                "reasons": ["tier 0/1 short-circuit"],
            },
            receipt={}, receipt_valid=False,
            non_delegable_flags=[],
            blockers=[],
            warnings=[],
            recommended_next_action="proceed_via_routine_autonomy_or_operator",
            operator_queue_fallback=operator_fallback,
        )
        if write_report:
            _persist_router_report(pdir, report)
        return report

    # Tier 2-5: run local council.
    eval_out = evaluate_request_with_local_council(pdir, request, write_receipt=False, policy=pol)
    packet = eval_out["packet"]
    responses = eval_out["responses"]
    quorum = eval_out["quorum_result"]
    receipt = eval_out["receipt"]

    blockers: list[str] = []
    warnings: list[str] = []

    missing = _missing_evidence(request, tier, pol)
    if missing:
        warnings.append(f"missing_evidence:{missing}")

    quorum_decision = str(quorum.get("decision") or "needs_human")
    routing_decision = _decision_from_quorum(quorum_decision)
    if dry_run and routing_decision == "approved":
        # Phase 5M dry-run never finalizes an approval into an executable action.
        routing_decision = "dry_run"

    receipt_valid = False
    if routing_decision == "approved" and _ai_council is not None and isinstance(receipt, dict) and receipt:
        ok2, _e = _ai_council.validate_approval_receipt(receipt, packet=packet)
        receipt_valid = bool(ok2)

    if write_receipt and not dry_run and routing_decision == "approved" and _ai_council is not None:
        try:
            _ai_council.append_approval_receipt(pdir, receipt)
        except OSError:
            pass

    rec_next = (
        "wait_for_phase_5N_executor_wiring_before_apply"
        if routing_decision in ("approved", "dry_run") else
        "review_blockers_or_get_operator_approval"
    )

    report = _build_router_report(
        request=request,
        action_type=action_type,
        tier=tier,
        decision=routing_decision,
        packet=packet, responses=responses,
        quorum=quorum, receipt=receipt,
        receipt_valid=receipt_valid,
        non_delegable_flags=[],
        blockers=blockers,
        warnings=warnings,
        recommended_next_action=rec_next,
        operator_queue_fallback=operator_fallback,
    )
    if write_report:
        _persist_router_report(pdir, report)
    return report


def _build_router_report(
    *,
    request: dict[str, Any],
    action_type: str,
    tier: int,
    decision: str,
    packet: dict[str, Any],
    responses: list[dict[str, Any]],
    quorum: dict[str, Any],
    receipt: dict[str, Any],
    receipt_valid: bool,
    non_delegable_flags: list[str],
    blockers: list[str],
    warnings: list[str],
    recommended_next_action: str,
    operator_queue_fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if decision not in VALID_ROUTING_DECISIONS:
        decision = "needs_human"
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": str(request.get("request_id") or ""),
        "created_at": now_iso(),
        "goal": str(request.get("goal") or ""),
        "target_files": list(request.get("target_files") or []),
        "action_type": action_type,
        "approval_tier_required": int(tier),
        "routing_decision": decision,
        "packet": packet or {},
        "responses": list(responses or []),
        "quorum_result": dict(quorum or {}),
        "receipt": dict(receipt or {}),
        "receipt_valid": bool(receipt_valid),
        "non_delegable_flags": list(non_delegable_flags or []),
        "blockers": list(blockers or []),
        "warnings": list(warnings or []),
        "recommended_next_action": recommended_next_action,
        "safe_to_execute_now": False,
        "operator_queue_fallback": dict(operator_queue_fallback or {}),
    }


# ---------- receipt verification ----------


def verify_receipt_for_request(
    project_dir: Path | str,
    request: dict[str, Any],
    receipt: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_router_policy(project_dir)
    pdir = Path(project_dir)
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(request, dict):
        return {"ok": False, "errors": ["request not a dict"], "warnings": []}
    if not isinstance(receipt, dict):
        return {"ok": False, "errors": ["receipt not a dict"], "warnings": []}

    request_targets = normalize_target_files(request.get("target_files") or [])
    receipt_targets = normalize_target_files(receipt.get("target_files") or [])
    if set(request_targets) != set(receipt_targets):
        errors.append("target_files mismatch")

    inferred_action = infer_action_type(
        request.get("goal", ""), request_targets,
        request.get("requested_action", ""), pol,
    )
    council_action = _ACTION_TO_COUNCIL.get(inferred_action, "non_delegable")
    if str(receipt.get("approved_action_type") or "") != council_action:
        errors.append(
            f"action_type mismatch: receipt={receipt.get('approved_action_type')!r} request_inferred={council_action!r}"
        )

    if str(receipt.get("decision") or "") != "approve":
        errors.append("receipt decision is not approve")

    expires = receipt.get("expires_at") or ""
    try:
        if expires:
            ts = expires
            if ts.endswith("Z"):
                exp_dt = _dt.datetime.fromisoformat(ts[:-1]).replace(tzinfo=_dt.timezone.utc)
            else:
                exp_dt = _dt.datetime.fromisoformat(ts)
            if _dt.datetime.now(_dt.timezone.utc) > exp_dt:
                errors.append("receipt expired")
    except ValueError:
        warnings.append("receipt expires_at unparseable")

    nd_flags = _classify_non_delegable(
        request.get("goal", ""), request_targets,
        request.get("requested_action", ""), pol,
    )
    if nd_flags:
        errors.append(f"non_delegable: {nd_flags}")

    if _ai_council is not None:
        ok2, e2 = _ai_council.validate_approval_receipt(receipt, packet=None, policy=None)
        if not ok2:
            errors.extend(f"council_validate:{e}" for e in e2)

    # Append a verification check audit row (gitignored).
    try:
        append_jsonl(
            pdir / "memory" / "luna_approval_receipt_checks.jsonl",
            {
                "ts": now_iso(),
                "request_id": request.get("request_id"),
                "receipt_id": receipt.get("receipt_id"),
                "ok": not errors,
                "errors": errors,
                "warnings": warnings,
            },
        )
    except OSError:
        pass

    return {"ok": not errors, "errors": errors, "warnings": warnings}


# ---------- rendering / writing ----------


def render_router_report_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Approval Router — Report")
    lines.append("")
    lines.append(f"- **request_id**: `{report.get('request_id', '?')}`")
    lines.append(f"- **goal**: {report.get('goal', '')!r}")
    lines.append(f"- **action_type**: `{report.get('action_type', '?')}`")
    lines.append(f"- **approval_tier_required**: {report.get('approval_tier_required', '?')}")
    lines.append(f"- **routing_decision**: `{report.get('routing_decision', '?')}`")
    lines.append(f"- **safe_to_execute_now**: `{report.get('safe_to_execute_now')}` *(Phase 5M hard rule — always false)*")
    lines.append(f"- **non_delegable_flags**: `{report.get('non_delegable_flags')}`")
    lines.append("")
    lines.append("## Target files")
    for t in report.get("target_files") or []:
        lines.append(f"- `{t}`")
    if not report.get("target_files"):
        lines.append("- _(none)_")
    lines.append("")
    quorum = report.get("quorum_result") or {}
    lines.append("## Quorum result")
    lines.append(f"- decision: `{quorum.get('decision')}`")
    lines.append(f"- rule: `{quorum.get('rule')}`")
    lines.append(f"- counts: approve={quorum.get('approve_count')} deny={quorum.get('deny_count')} needs_human={quorum.get('needs_human_count')} abstain={quorum.get('abstain_count')}")
    if report.get("blockers"):
        lines.append("")
        lines.append("## Blockers")
        for b in report["blockers"]:
            lines.append(f"- {b}")
    if report.get("warnings"):
        lines.append("")
        lines.append("## Warnings")
        for w in report["warnings"]:
            lines.append(f"- {w}")
    lines.append("")
    lines.append(f"## Recommended next action")
    lines.append(f"- {report.get('recommended_next_action', '')}")
    lines.append("")
    lines.append("> Phase 5M router is plan/evaluate-only. It does not execute approved actions, does not call external reviewers, and does not modify target files.")
    return "\n".join(lines) + "\n"


def _persist_router_report(project_dir: Path, report: dict[str, Any]) -> dict[str, str]:
    mem = project_dir / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    json_p = mem / "luna_approval_router_report.json"
    md_p = mem / "luna_approval_router_report.md"
    state_p = mem / "luna_approval_router_state.json"
    project_root = project_dir.resolve()
    for p in (json_p, md_p, state_p):
        try:
            p.resolve().relative_to(project_root)
        except ValueError:
            raise ValueError(f"path escapes project root: {p}")
    write_json_atomic(json_p, report)
    md_p.write_text(render_router_report_markdown(report), encoding="utf-8")
    write_json_atomic(state_p, {
        "schema_version": SCHEMA_VERSION,
        "last_generated_at": now_iso(),
        "last_request_id": report.get("request_id"),
        "last_routing_decision": report.get("routing_decision"),
        "last_action_type": report.get("action_type"),
        "last_tier": report.get("approval_tier_required"),
        "safe_to_execute_now": False,
        "external_reviewers_enabled": False,
    })
    return {"json": str(json_p), "md": str(md_p), "state": str(state_p)}


def write_router_report(project_dir: Path | str, report: dict[str, Any]) -> dict[str, str]:
    pdir = Path(project_dir).resolve()
    return _persist_router_report(pdir, report)


# ---------- self-test ----------


def _build_sample_request(action: str, target: str, *, goal: str = "", source: str = "test") -> dict[str, Any]:
    return build_router_request(
        goal=goal or f"Phase 5M smoke {action}",
        target_files=[target] if target else [],
        requested_action=action,
        source=source,
        task_id=f"task_{action}",
        planned_change_summary=f"smoke for {action}",
        diff_summary="+1 -0 sample" if action.endswith("_edit") or action == "low_risk_additive" else "",
        verification_commands=["python -m py_compile " + target] if target.endswith(".py") else [],
        rollback_plan="git checkout HEAD -- " + target if action != "non_delegable" and target else "",
        sandbox_report_path="memory/luna_sandbox_report.json" if action in ("medium_code_edit", "high_risk_core_edit") else "",
        upgrade_gate_decision="needs_approval" if action in ("medium_code_edit", "high_risk_core_edit") else "allow",
        resource_status="normal",
        capability_readiness="controlled_autonomy_ready",
    )


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        samples = {
            "generated_artifact": _build_sample_request("generated_artifact", "memory/luna_capability_scorecard.json"),
            "low_risk_additive": _build_sample_request("low_risk_additive", "luna_modules/example.py"),
            "medium_code_edit": _build_sample_request("medium_code_edit", "luna_modules/luna_self_knowledge.py"),
            "high_risk_core_edit": _build_sample_request("high_risk_core_edit", "worker.py"),
            "non_delegable": _build_sample_request("non_delegable", "memory/nightly_updates.md", goal="Delete memory logs"),
        }
        decisions: dict[str, Any] = {}
        for name, req in samples.items():
            r = route_approval_request(td, req, dry_run=True, write_report=False)
            decisions[name] = {
                "routing_decision": r["routing_decision"],
                "action_type": r["action_type"],
                "tier": r["approval_tier_required"],
                "safe_to_execute_now": r["safe_to_execute_now"],
                "non_delegable_flags": r["non_delegable_flags"],
            }
        # Receipt verification round-trip
        req = samples["low_risk_additive"]
        eval_out = evaluate_request_with_local_council(td, req, write_receipt=False)
        receipt = eval_out["receipt"]
        verify = verify_receipt_for_request(td, req, receipt) if receipt else {"ok": False, "errors": ["no_receipt_in_self_test"]}
        ok = (
            all(d["safe_to_execute_now"] is False for d in decisions.values())
            and decisions["non_delegable"]["routing_decision"] == "blocked"
            and decisions["generated_artifact"]["routing_decision"] in ("not_required", "approved", "dry_run")
        )
        out = {"ok": bool(ok), "decisions": decisions, "verify_receipt_for_low_risk": verify}
        print(json.dumps(out, indent=2))
        return 0 if ok else 1


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Approval Router foundation (Phase 5M)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--request", default=None, help="Goal text for the approval request.")
    parser.add_argument("--action", default="unknown", choices=list(VALID_REQUEST_ACTIONS))
    parser.add_argument("--target", action="append", default=None, help="Project-relative target file. Repeatable.")
    parser.add_argument("--source", default="operator", choices=list(VALID_SOURCES))
    parser.add_argument("--task-id", default="")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", default=False, help="(Phase 5M still refuses to execute; just controls receipt persistence.)")
    parser.add_argument("--write-report", action="store_true", default=False)
    parser.add_argument("--write-receipt", action="store_true", default=False, help="Append receipt to ledger only when quorum approves and --no-dry-run is set.")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    if args.request is None:
        parser.error("--request is required (or use --self-test)")

    pdir = Path(args.project_dir)
    targets = args.target or []
    request = build_router_request(
        goal=args.request,
        target_files=targets,
        requested_action=args.action,
        source=args.source,
        task_id=args.task_id,
        planned_change_summary=args.request,
        upgrade_gate_decision="unknown",
        resource_status="unknown",
    )
    dry_run = not bool(args.no_dry_run)
    report = route_approval_request(
        pdir, request,
        dry_run=dry_run,
        write_report=bool(args.write_report),
        write_receipt=bool(args.write_receipt),
    )
    out = {
        "request_id": report["request_id"],
        "action_type": report["action_type"],
        "tier": report["approval_tier_required"],
        "routing_decision": report["routing_decision"],
        "non_delegable_flags": report["non_delegable_flags"],
        "blockers": report["blockers"],
        "warnings": report["warnings"],
        "recommended_next_action": report["recommended_next_action"],
        "safe_to_execute_now": report["safe_to_execute_now"],
        "operator_queue_fallback": report["operator_queue_fallback"],
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
