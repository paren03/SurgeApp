"""Phase 5H: Luna Dynamic Task Graph + Exit Criteria foundation.

Read-mostly. Stdlib only. Foundation module — does NOT auto-execute tasks,
edit files, call Aider, run installs, or modify queues. Later phases may
wire it into director_agent.py or worker.py.

Tracked schema/config:
  memory/luna_task_graph.schema.json
  memory/luna_exit_criteria.schema.json
  memory/luna_task_graph_config.json

Generated runtime artifacts (gitignored):
  memory/luna_task_graph.json
  memory/luna_task_graph_report.md
  memory/luna_task_graph_build_report.json

CLI:
  python -m luna_modules.luna_task_graph --self-test
  python -m luna_modules.luna_task_graph --goal "Build file self-map"
  python -m luna_modules.luna_task_graph --goal "..." --write
  python -m luna_modules.luna_task_graph --print-markdown --goal "..."
  python -m luna_modules.luna_task_graph --goal "..." --goal-only-json
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

VALID_TASK_STATES = (
    "proposed",
    "ready",
    "blocked",
    "running",
    "done",
    "failed",
    "skipped",
)

VALID_RISK_LEVELS = ("low", "medium", "high", "critical")

VALID_CHECK_TYPES = (
    "file_exists",
    "command_passes",
    "report_contains",
    "git_clean",
    "verifier_clean",
    "manual_review",
)

VALID_CRITERION_STATUSES = ("pending", "passed", "failed", "skipped", "unknown")

VALID_DRIFT_STATUSES = ("aligned", "watch", "drifted")

_DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "task_count_min": 3,
    "task_count_max": 8,
    "default_approval_tier_low": 2,
    "default_approval_tier_medium": 3,
    "default_approval_tier_high": 4,
    "default_approval_tier_critical": 5,
    "high_risk_path_substrings": [
        "worker.py",
        "aider_bridge.py",
        "luna_guardian.py",
        "LaunchLuna.pyw",
        "SurgeApp_Claude_Terminal.py",
        "luna_start.pyw",
        "director_agent.py",
    ],
    "critical_path_substrings": [
        "luna_modules/luna_hygiene.py",
        "luna_modules/luna_paths.py",
        "luna_modules/luna_routing.py",
        "luna_modules/luna_state.py",
        "memory/luna_personality_state.json",
        "memory/luna_active_goal.json",
    ],
    "risk_keyword_high": [
        "delete", "rm -rf", "wipe", "force", "overwrite", "reinstall",
        "uninstall", "downgrade", "drop table", "kill",
        "factory reset", "purge", "rewrite worker", "disable guardian",
        "install package", "pip install", "npm install", "winget install",
        "external api", "cloud api", "fetch url", "open port", "expose port",
        "personality", "identity", "goals",
    ],
    "risk_keyword_medium": [
        "edit", "modify", "patch", "refactor", "rename", "move",
        "queue", "schedule", "config", "settings", "policy",
    ],
    "risk_keyword_low": [
        "report", "summary", "scorecard", "blueprint", "design",
        "document", "doc", "describe", "explain", "self-map",
        "self map", "playbook", "ledger", "index", "schema",
        "read", "view", "list", "show",
    ],
    "intent_drift_thresholds": {
        "watch_below_overlap": 0.55,
        "drift_below_overlap": 0.25,
        "drift_keyword_bonus": 35,
    },
    "intent_drift_unrelated_keywords": [
        "buy", "purchase", "credit card", "bank", "wallet",
        "password", "ssh key", "api key", "exfiltrate",
        "scrape", "ddos", "exploit", "bypass guardian",
        "disable safety",
    ],
    "stop_words": [
        "a", "an", "the", "and", "or", "but", "to", "of", "for",
        "in", "on", "at", "with", "by", "from", "as", "is", "are",
        "be", "been", "being", "this", "that", "these", "those",
        "it", "its", "i", "we", "you", "he", "she", "they",
        "do", "does", "did", "have", "has", "had", "will", "would",
        "should", "could", "can", "may", "might",
    ],
}

DEFAULT_CONFIG_PATH = (
    _PROJECT_DIR_DEFAULT / "memory" / "luna_task_graph_config.json"
)


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _short_uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def make_graph_id(prefix: str = "graph") -> str:
    return _short_uid(prefix)


def make_task_id(prefix: str = "task") -> str:
    return _short_uid(prefix)


def normalize_goal(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text).strip()


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    p = config_path or DEFAULT_CONFIG_PATH
    cfg = load_json(p, default=None)
    if not isinstance(cfg, dict):
        merged = dict(_DEFAULT_CONFIG)
        merged["_source"] = "module_fallback"
        merged["_loaded_from_file"] = False
        return merged
    out = dict(_DEFAULT_CONFIG)
    for k, v in cfg.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    return out


_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")


def _tokenize(text: str, stop_words: Iterable[str] | None = None) -> list[str]:
    if not isinstance(text, str):
        return []
    raw = [t.lower() for t in _TOKEN_RE.findall(text)]
    if stop_words:
        sw = set(stop_words)
        return [t for t in raw if t not in sw and len(t) > 1]
    return [t for t in raw if len(t) > 1]


# ---------- risk + tier inference ----------


def _normalize_target_path(p: str) -> str:
    return p.replace("\\", "/").lstrip("./")


def infer_target_files(
    task_text: str, file_map_path: Path | None = None
) -> list[str]:
    text = task_text or ""
    found: list[str] = []
    file_map = load_json(file_map_path, default=None) if file_map_path else None
    candidates: list[str] = []
    if isinstance(file_map, dict):
        keys = file_map.get("files") or file_map.get("modules") or file_map
        if isinstance(keys, dict):
            candidates.extend(str(k) for k in keys.keys())
        elif isinstance(keys, list):
            candidates.extend(str(k) for k in keys)
    common = [
        "worker.py",
        "aider_bridge.py",
        "luna_guardian.py",
        "LaunchLuna.pyw",
        "SurgeApp_Claude_Terminal.py",
        "luna_start.pyw",
        "director_agent.py",
        "luna_modules/luna_hygiene.py",
        "luna_modules/luna_paths.py",
        "luna_modules/luna_routing.py",
        "luna_modules/luna_state.py",
        "luna_modules/luna_self_knowledge.py",
        "luna_modules/luna_change_ledger.py",
        "luna_modules/luna_memory_index.py",
        "luna_modules/luna_playbook_engine.py",
        "luna_modules/luna_upgrade_gate.py",
        "luna_modules/luna_capability_scorecard.py",
        "luna_modules/luna_task_graph.py",
        "memory/luna_personality_state.json",
        "memory/luna_active_goal.json",
    ]
    for c in common + candidates:
        if not c:
            continue
        norm = _normalize_target_path(c)
        if norm.lower() in text.lower() and norm not in found:
            found.append(norm)
    return found


def infer_task_risk(
    task_text: str,
    target_files: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> str:
    cfg = config or _DEFAULT_CONFIG
    text = (task_text or "").lower()
    targets = [t.lower() for t in (target_files or [])]
    for crit in cfg.get("critical_path_substrings", []):
        c = crit.lower()
        if any(c in t for t in targets) or c in text:
            return "critical"
    for high in cfg.get("high_risk_path_substrings", []):
        h = high.lower()
        if any(h in t for t in targets) or h in text:
            return "high"
    for kw in cfg.get("risk_keyword_high", []):
        if kw.lower() in text:
            return "high"
    for kw in cfg.get("risk_keyword_medium", []):
        if kw.lower() in text:
            return "medium"
    for kw in cfg.get("risk_keyword_low", []):
        if kw.lower() in text:
            return "low"
    return "low"


def infer_approval_tier(
    risk_level: str,
    target_files: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> int:
    cfg = config or _DEFAULT_CONFIG
    mapping = {
        "low": int(cfg.get("default_approval_tier_low", 2)),
        "medium": int(cfg.get("default_approval_tier_medium", 3)),
        "high": int(cfg.get("default_approval_tier_high", 4)),
        "critical": int(cfg.get("default_approval_tier_critical", 5)),
    }
    base = mapping.get(risk_level, 2)
    if target_files:
        lows = [t.lower() for t in target_files]
        for crit in cfg.get("critical_path_substrings", []):
            if any(crit.lower() in t for t in lows):
                base = max(base, mapping["critical"])
        for hi in cfg.get("high_risk_path_substrings", []):
            if any(hi.lower() in t for t in lows):
                base = max(base, mapping["high"])
    return base


# ---------- exit criteria ----------


def _criterion(
    check_type: str,
    description: str,
    *,
    command: str | None = None,
    path: str | None = None,
    expected_text: str | None = None,
    required: bool = True,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "criterion_id": _short_uid("crit"),
        "description": description,
        "check_type": check_type,
        "required": bool(required),
        "status": "pending",
        "evidence": [],
    }
    if command is not None:
        rec["command"] = command
    if path is not None:
        rec["path"] = path
    if expected_text is not None:
        rec["expected_text"] = expected_text
    return rec


def build_exit_criteria(
    task_title: str,
    task_type: str,
    target_files: list[str] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if task_type in {"plan", "schema"}:
        out.append(
            _criterion(
                "manual_review",
                f"Operator confirms plan/schema for: {task_title}",
                required=True,
            )
        )
    if task_type in {"implement", "schema"} and target_files:
        for tf in target_files[:3]:
            out.append(
                _criterion(
                    "file_exists",
                    f"Expected artifact present: {tf}",
                    path=tf,
                    required=True,
                )
            )
    if task_type in {"implement", "tests", "verify"}:
        out.append(
            _criterion(
                "command_passes",
                "Module compiles via py_compile",
                command="python -m py_compile <module>",
                required=True,
            )
        )
    if task_type == "tests":
        out.append(
            _criterion(
                "command_passes",
                "All targeted tests pass",
                command="python -m unittest <test_module>",
                required=True,
            )
        )
    if task_type == "verify":
        out.append(
            _criterion(
                "verifier_clean",
                "Luna_Post_Repair_Verify.ps1 reports 0 hard failures and 0 warnings",
                command='powershell -ExecutionPolicy Bypass -File Luna_Post_Repair_Verify.ps1',
                required=True,
            )
        )
        out.append(
            _criterion(
                "command_passes",
                "import worker prints IMPORT_OK",
                command="python -c \"import worker; print('IMPORT_OK')\"",
                required=True,
            )
        )
    if task_type == "commit":
        out.append(
            _criterion(
                "git_clean",
                "Tracked tree is clean after commit",
                required=True,
            )
        )
        out.append(
            _criterion(
                "command_passes",
                "Commit lands on main with descriptive message",
                command="git log --oneline -1",
                required=True,
            )
        )
    if task_type == "report":
        out.append(
            _criterion(
                "report_contains",
                "Report file contains required summary sections",
                required=False,
            )
        )
    if not out:
        out.append(
            _criterion(
                "manual_review",
                f"Operator confirms task complete: {task_title}",
                required=True,
            )
        )
    return out


def validate_exit_criteria(criteria: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(criteria, list):
        return False, ["criteria is not a list"]
    if not criteria:
        return False, ["criteria list is empty"]
    for i, c in enumerate(criteria):
        if not isinstance(c, dict):
            errors.append(f"criteria[{i}] not a dict")
            continue
        for k in ("criterion_id", "description", "check_type", "required", "status", "evidence"):
            if k not in c:
                errors.append(f"criteria[{i}].{k} missing")
        if c.get("check_type") not in VALID_CHECK_TYPES:
            errors.append(f"criteria[{i}].check_type invalid: {c.get('check_type')!r}")
        if c.get("status") not in VALID_CRITERION_STATUSES:
            errors.append(f"criteria[{i}].status invalid: {c.get('status')!r}")
        if not isinstance(c.get("required"), bool):
            errors.append(f"criteria[{i}].required must be bool")
        if not isinstance(c.get("evidence"), list):
            errors.append(f"criteria[{i}].evidence must be list")
    return (not errors), errors


# ---------- task node ----------


def build_task_node(
    *,
    title: str,
    description: str,
    task_type: str,
    source_goal: str,
    dependencies: list[str] | None = None,
    target_files: list[str] | None = None,
    config: dict[str, Any] | None = None,
    expected_artifacts: list[str] | None = None,
    rollback_plan: str | None = None,
    verification_commands: list[str] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    cfg = config or _DEFAULT_CONFIG
    targets = list(target_files or [])
    risk = infer_task_risk(f"{title} {description}", targets, cfg)
    tier = infer_approval_tier(risk, targets, cfg)
    criteria = build_exit_criteria(title, task_type, targets)
    rb = rollback_plan or (
        f"Revert via git for {', '.join(targets)}" if targets else "git revert latest commit"
    )
    state = "ready" if not dependencies else "blocked"
    now = now_iso()
    return {
        "task_id": make_task_id(),
        "title": title,
        "description": description,
        "task_type": task_type,
        "state": state,
        "dependencies": list(dependencies or []),
        "target_files": [_normalize_target_path(t) for t in targets],
        "risk_level": risk,
        "approval_tier_required": tier,
        "exit_criteria": criteria,
        "verification_commands": list(verification_commands or [
            "python -m py_compile <module>",
        ]),
        "rollback_plan": rb,
        "expected_artifacts": list(expected_artifacts or []),
        "blockers": [],
        "created_at": now,
        "updated_at": now,
        "source_goal": source_goal,
        "notes": list(notes or []),
    }


def validate_task_node(node: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(node, dict):
        return False, ["node not a dict"]
    required_fields = (
        "task_id",
        "title",
        "description",
        "state",
        "dependencies",
        "target_files",
        "risk_level",
        "approval_tier_required",
        "exit_criteria",
        "verification_commands",
        "rollback_plan",
        "expected_artifacts",
        "blockers",
        "created_at",
        "updated_at",
        "source_goal",
    )
    for k in required_fields:
        if k not in node:
            errors.append(f"task.{k} missing")
    if node.get("state") not in VALID_TASK_STATES:
        errors.append(f"task.state invalid: {node.get('state')!r}")
    if node.get("risk_level") not in VALID_RISK_LEVELS:
        errors.append(f"task.risk_level invalid: {node.get('risk_level')!r}")
    if not isinstance(node.get("approval_tier_required"), int):
        errors.append("task.approval_tier_required must be int")
    elif not (0 <= node["approval_tier_required"] <= 5):
        errors.append("task.approval_tier_required out of [0,5]")
    if not isinstance(node.get("dependencies"), list):
        errors.append("task.dependencies must be list")
    if not isinstance(node.get("exit_criteria"), list) or not node.get("exit_criteria"):
        errors.append("task.exit_criteria must be non-empty list")
    else:
        ok, errs = validate_exit_criteria(node["exit_criteria"])
        if not ok:
            errors.extend(f"exit_criteria: {e}" for e in errs)
    return (not errors), errors


# ---------- goal decomposition ----------


_RISKY_GOAL_TOKENS = (
    "worker.py",
    "aider_bridge",
    "guardian",
    "launcher",
    "installer",
    "package install",
    "memory deletion",
    "architecture replacement",
    "personality",
    "identity",
    "goals",
)


def split_goal_into_candidate_tasks(goal_text: str) -> list[dict[str, str]]:
    """Deterministic split into 3-8 candidate tasks. No LLM."""
    goal = normalize_goal(goal_text)
    lower = goal.lower()
    is_risky = any(tok in lower for tok in _RISKY_GOAL_TOKENS)
    is_pure_doc = bool(
        re.search(r"\b(report|summary|describe|document|explain|blueprint)\b", lower)
    ) and not is_risky
    base = [
        {
            "task_type": "inspect",
            "title": f"Baseline inspection for: {goal[:80]}",
            "description": (
                "Run pre-flight: git status, py_compile core files, import worker, "
                "verifier. Establish a clean baseline before any change."
            ),
        },
        {
            "task_type": "plan",
            "title": "Draft plan / schema for the work",
            "description": (
                "Define scope, tracked schema/config, generated artifacts, allowed "
                "and forbidden targets, and rollback requirements. No edits yet."
            ),
        },
    ]
    middle: list[dict[str, str]] = []
    if not is_pure_doc:
        middle.append(
            {
                "task_type": "schema",
                "title": "Create tracked schema/config files",
                "description": (
                    "Add JSON-Schema and config under memory/. Stdlib only. "
                    "Do not edit forbidden runtime services."
                ),
            }
        )
        middle.append(
            {
                "task_type": "implement",
                "title": "Implement additive module / artifact",
                "description": (
                    "Stdlib only. Module must not auto-execute, edit, install, or "
                    "modify queues. Read-mostly with optional --write CLI."
                ),
            }
        )
        middle.append(
            {
                "task_type": "tests",
                "title": "Add unittest tests with TemporaryDirectory fixtures",
                "description": (
                    "Cover happy paths, missing-evidence degradation, validation "
                    "errors, CLI rc=0, and absence of cloud/network calls."
                ),
            }
        )
    middle.append(
        {
            "task_type": "verify",
            "title": "Run full verification chain",
            "description": (
                "py_compile all core files, import worker -> IMPORT_OK, "
                "Luna_Post_Repair_Verify.ps1 -> 0 fail / 0 warn."
            ),
        }
    )
    middle.append(
        {
            "task_type": "commit",
            "title": "Stage allowed files and commit",
            "description": (
                "git add only allowed source/test/schema/config/.gitignore files. "
                "Commit with a feature-scoped message. Do not push."
            ),
        }
    )
    middle.append(
        {
            "task_type": "report",
            "title": "Write completion report and next-step recommendation",
            "description": (
                "Summarize commit hash, files changed, tests passed, sample output, "
                "and the smallest safe next step."
            ),
        }
    )
    candidates = base + middle
    cap_max = _DEFAULT_CONFIG["task_count_max"]
    cap_min = _DEFAULT_CONFIG["task_count_min"]
    candidates = candidates[:cap_max]
    if len(candidates) < cap_min:
        candidates.append(
            {
                "task_type": "manual_review",
                "title": "Operator approval gate",
                "description": "Operator reviews and approves the plan output.",
            }
        )
    return candidates


# ---------- graph build / sort / state ----------


def build_task_graph(
    goal_text: str,
    project_dir: Path | str | None = None,
    context: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or load_config()
    goal_norm = normalize_goal(goal_text)
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    file_map_path = pdir / "memory" / "luna_file_map.json"
    candidates = split_goal_into_candidate_tasks(goal_norm)
    nodes: list[dict[str, Any]] = []
    prev_id: str | None = None
    for c in candidates:
        targets_for_this = infer_target_files(
            f"{c['title']} {c['description']} {goal_norm}",
            file_map_path if file_map_path.is_file() else None,
        )
        node = build_task_node(
            title=c["title"],
            description=c["description"],
            task_type=c["task_type"],
            source_goal=goal_norm,
            dependencies=[prev_id] if prev_id else [],
            target_files=targets_for_this,
            config=cfg,
        )
        nodes.append(node)
        prev_id = node["task_id"]
    overall_risk = "low"
    overall_tier = int(cfg.get("default_approval_tier_low", 2))
    risk_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
    for n in nodes:
        if risk_order[n["risk_level"]] > risk_order[overall_risk]:
            overall_risk = n["risk_level"]
        if n["approval_tier_required"] > overall_tier:
            overall_tier = n["approval_tier_required"]
    drift = detect_intent_drift(
        goal_norm,
        " ".join(c["title"] for c in candidates),
        recent_actions=(context or {}).get("recent_actions"),
        config=cfg,
    )
    now = now_iso()
    graph = {
        "schema_version": SCHEMA_VERSION,
        "graph_id": make_graph_id(),
        "created_at": now,
        "updated_at": now,
        "source_goal": goal_text or "",
        "normalized_goal": goal_norm,
        "project_dir": str(pdir).replace("\\", "/"),
        "tasks": nodes,
        "overall_risk_level": overall_risk,
        "overall_approval_tier_required": overall_tier,
        "intent_drift": drift,
        "notes": [
            "Phase 5H task graph. Foundation only — does not auto-execute.",
            "All edits/installs/queues forbidden until operator approves.",
        ],
    }
    return graph


def validate_task_graph(graph: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(graph, dict):
        return False, ["graph not a dict"]
    required = (
        "schema_version",
        "graph_id",
        "created_at",
        "updated_at",
        "source_goal",
        "normalized_goal",
        "tasks",
        "overall_risk_level",
        "overall_approval_tier_required",
        "intent_drift",
    )
    for k in required:
        if k not in graph:
            errors.append(f"graph.{k} missing")
    if graph.get("overall_risk_level") not in VALID_RISK_LEVELS:
        errors.append(f"graph.overall_risk_level invalid: {graph.get('overall_risk_level')!r}")
    tasks = graph.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        errors.append("graph.tasks must be non-empty list")
    else:
        ids = set()
        for i, t in enumerate(tasks):
            ok, errs = validate_task_node(t)
            if not ok:
                errors.extend(f"tasks[{i}]: {e}" for e in errs)
            tid = t.get("task_id") if isinstance(t, dict) else None
            if tid in ids:
                errors.append(f"tasks[{i}].task_id duplicate: {tid}")
            if tid:
                ids.add(tid)
        for i, t in enumerate(tasks):
            for dep in (t.get("dependencies") or []):
                if dep not in ids:
                    errors.append(f"tasks[{i}].dependencies references unknown id: {dep}")
    drift = graph.get("intent_drift")
    if not isinstance(drift, dict):
        errors.append("graph.intent_drift must be dict")
    else:
        if drift.get("status") not in VALID_DRIFT_STATUSES:
            errors.append(f"intent_drift.status invalid: {drift.get('status')!r}")
        if drift.get("recommended_action") not in {"continue", "pause_for_approval"}:
            errors.append(
                f"intent_drift.recommended_action invalid: {drift.get('recommended_action')!r}"
            )
    return (not errors), errors


def topological_sort_tasks(graph: dict[str, Any]) -> list[str]:
    tasks = graph.get("tasks") or []
    indeg: dict[str, int] = {}
    edges: dict[str, list[str]] = {}
    by_id: dict[str, dict[str, Any]] = {}
    for t in tasks:
        tid = t["task_id"]
        by_id[tid] = t
        indeg.setdefault(tid, 0)
        edges.setdefault(tid, [])
    for t in tasks:
        tid = t["task_id"]
        for dep in (t.get("dependencies") or []):
            if dep not in by_id:
                raise ValueError(f"unknown dependency: {dep} -> {tid}")
            edges[dep].append(tid)
            indeg[tid] += 1
    queue: list[str] = [tid for tid, d in indeg.items() if d == 0]
    queue.sort()
    order: list[str] = []
    while queue:
        cur = queue.pop(0)
        order.append(cur)
        for nxt in sorted(edges[cur]):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)
    if len(order) != len(by_id):
        raise ValueError("cycle detected in task graph")
    return order


def ready_tasks(graph: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = {t["task_id"]: t for t in (graph.get("tasks") or [])}
    out: list[dict[str, Any]] = []
    for t in (graph.get("tasks") or []):
        if t.get("state") not in ("proposed", "ready"):
            continue
        deps = t.get("dependencies") or []
        if all(by_id.get(d, {}).get("state") == "done" for d in deps):
            out.append(t)
    return out


def mark_task_state(
    graph: dict[str, Any],
    task_id: str,
    state: str,
    reason: str = "",
) -> dict[str, Any]:
    if state not in VALID_TASK_STATES:
        raise ValueError(f"invalid state: {state}")
    found = False
    for t in (graph.get("tasks") or []):
        if t.get("task_id") == task_id:
            t["state"] = state
            t["updated_at"] = now_iso()
            if reason:
                notes = t.setdefault("notes", [])
                notes.append(f"[{t['updated_at']}] state={state}: {reason}")
            found = True
            break
    if not found:
        raise KeyError(f"task_id not found: {task_id}")
    graph["updated_at"] = now_iso()
    return graph


# ---------- intent drift ----------


def detect_intent_drift(
    original_goal: str,
    current_task_text: str,
    recent_actions: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or _DEFAULT_CONFIG
    stop_words = cfg.get("stop_words", [])
    thresholds = cfg.get("intent_drift_thresholds", {})
    watch_below = float(thresholds.get("watch_below_overlap", 0.55))
    drift_below = float(thresholds.get("drift_below_overlap", 0.25))
    bonus = int(thresholds.get("drift_keyword_bonus", 35))
    unrelated = [k.lower() for k in cfg.get("intent_drift_unrelated_keywords", [])]

    g_tokens = set(_tokenize(original_goal or "", stop_words))
    c_tokens = set(_tokenize(current_task_text or "", stop_words))
    if not g_tokens or not c_tokens:
        overlap = 0.0
    else:
        overlap = len(g_tokens & c_tokens) / max(1, min(len(g_tokens), len(c_tokens)))
    evidence: list[str] = [
        f"overlap_ratio={overlap:.2f}",
        f"goal_tokens={len(g_tokens)}",
        f"current_tokens={len(c_tokens)}",
    ]
    drift_score = int(round((1.0 - overlap) * 100))
    haystack = " ".join(
        [current_task_text or ""] + list(recent_actions or [])
    ).lower()
    matched_unrelated: list[str] = []
    for kw in unrelated:
        if kw and kw in haystack:
            matched_unrelated.append(kw)
    if matched_unrelated:
        drift_score = min(100, drift_score + bonus)
        evidence.append(f"unrelated_keywords={matched_unrelated[:5]}")
    if overlap < drift_below or matched_unrelated:
        status = "drifted"
        action = "pause_for_approval"
    elif overlap < watch_below:
        status = "watch"
        action = "pause_for_approval"
    else:
        status = "aligned"
        action = "continue"
    return {
        "drift_score": int(max(0, min(100, drift_score))),
        "status": status,
        "evidence": evidence,
        "recommended_action": action,
    }


# ---------- rendering / writing ----------


def render_task_graph_markdown(graph: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Task Graph")
    lines.append("")
    lines.append(f"- **Graph ID**: `{graph.get('graph_id', '?')}`")
    lines.append(f"- **Generated**: {graph.get('created_at', '?')}")
    lines.append(f"- **Goal**: {graph.get('source_goal', '')!r}")
    lines.append(f"- **Normalized goal**: {graph.get('normalized_goal', '')!r}")
    lines.append(f"- **Overall risk**: `{graph.get('overall_risk_level', '?')}`")
    lines.append(
        f"- **Overall approval tier required**: {graph.get('overall_approval_tier_required', '?')}"
    )
    drift = graph.get("intent_drift") or {}
    lines.append(
        f"- **Intent drift**: score={drift.get('drift_score', '?')} "
        f"status=`{drift.get('status', '?')}` "
        f"action=`{drift.get('recommended_action', '?')}`"
    )
    lines.append("")
    lines.append("## Tasks")
    lines.append("")
    lines.append("| # | ID | Title | Type | Risk | Tier | State | Deps | Targets |")
    lines.append("|--:|----|-------|------|------|------:|-------|------|---------|")
    for i, t in enumerate(graph.get("tasks", []), start=1):
        deps = ",".join(d[-6:] for d in (t.get("dependencies") or []))
        tgts = ",".join(t.get("target_files") or []) or "—"
        title = (t.get("title") or "").replace("|", "\\|")
        lines.append(
            f"| {i} | `{t['task_id'][-8:]}` | {title} | {t.get('task_type', '?')} | "
            f"{t['risk_level']} | {t['approval_tier_required']} | {t['state']} | "
            f"{deps or '—'} | {tgts} |"
        )
    lines.append("")
    lines.append("## Per-task detail")
    for t in graph.get("tasks", []):
        lines.append("")
        lines.append(f"### {t['title']}")
        lines.append(f"- **task_id**: `{t['task_id']}`")
        lines.append(f"- **type**: {t.get('task_type', '?')}")
        lines.append(f"- **risk**: {t['risk_level']} (tier {t['approval_tier_required']})")
        lines.append(f"- **state**: {t['state']}")
        lines.append(f"- **dependencies**: {', '.join(t.get('dependencies') or []) or '—'}")
        lines.append(f"- **target_files**: {', '.join(t.get('target_files') or []) or '—'}")
        if t.get("description"):
            lines.append(f"- **description**: {t['description']}")
        if t.get("rollback_plan"):
            lines.append(f"- **rollback**: {t['rollback_plan']}")
        if t.get("exit_criteria"):
            lines.append("- **exit_criteria**:")
            for c in t["exit_criteria"]:
                req = "required" if c.get("required") else "optional"
                lines.append(
                    f"  - [{c.get('check_type')}] {c.get('description')} ({req})"
                )
        if t.get("verification_commands"):
            lines.append("- **verification_commands**:")
            for v in t["verification_commands"]:
                lines.append(f"  - `{v}`")
    notes = graph.get("notes") or []
    if notes:
        lines.append("")
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, data: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding="utf-8")
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def write_task_graph(
    graph: dict[str, Any],
    json_path: Path | str,
    markdown_path: Path | str | None = None,
    build_report_path: Path | str | None = None,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    json_p = Path(json_path)
    root = Path(project_root).resolve() if project_root else _PROJECT_DIR_DEFAULT.resolve()
    try:
        json_p.resolve().relative_to(root)
    except ValueError:
        raise ValueError(f"json_path must be inside project root: {json_p}")
    _atomic_write(json_p, json.dumps(graph, indent=2, sort_keys=False))
    written: dict[str, Any] = {"json": str(json_p)}
    if markdown_path:
        md_p = Path(markdown_path)
        try:
            md_p.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"markdown_path must be inside project root: {md_p}")
        _atomic_write(md_p, render_task_graph_markdown(graph))
        written["markdown"] = str(md_p)
    if build_report_path:
        rp = Path(build_report_path)
        try:
            rp.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"build_report_path must be inside project root: {rp}")
        report = {
            "schema_version": SCHEMA_VERSION,
            "graph_id": graph.get("graph_id"),
            "generated_at": graph.get("created_at"),
            "task_count": len(graph.get("tasks") or []),
            "overall_risk_level": graph.get("overall_risk_level"),
            "overall_approval_tier_required": graph.get("overall_approval_tier_required"),
            "intent_drift_status": (graph.get("intent_drift") or {}).get("status"),
            "wrote": written,
        }
        _atomic_write(rp, json.dumps(report, indent=2))
        written["build_report"] = str(rp)
    return written


# ---------- self-test ----------


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        graph = build_task_graph(
            "Build a read-only file self-map and verify it",
            project_dir=td,
        )
        ok, errs = validate_task_graph(graph)
        if not ok:
            print(json.dumps({"ok": False, "errors": errs}, indent=2))
            return 1
        order = topological_sort_tasks(graph)
        if len(order) != len(graph["tasks"]):
            print(json.dumps({"ok": False, "error": "topo sort length mismatch"}))
            return 1
        rdy = ready_tasks(graph)
        if not rdy:
            print(json.dumps({"ok": False, "error": "no initial ready tasks"}))
            return 1
        first = rdy[0]
        mark_task_state(graph, first["task_id"], "done", reason="self-test")
        json_p = td / "memory" / "luna_task_graph.json"
        md_p = td / "memory" / "luna_task_graph_report.md"
        rp = td / "memory" / "luna_task_graph_build_report.json"
        write_task_graph(graph, json_p, md_p, rp, project_root=td)
        out = {
            "ok": True,
            "graph_id": graph["graph_id"],
            "task_count": len(graph["tasks"]),
            "overall_risk_level": graph["overall_risk_level"],
            "overall_approval_tier_required": graph["overall_approval_tier_required"],
            "intent_drift_status": graph["intent_drift"]["status"],
        }
        print(json.dumps(out, indent=2))
        return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Dynamic Task Graph + Exit Criteria (Phase 5H)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--goal", default=None)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--goal-only-json", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    parser.add_argument(
        "--out-json",
        default=str(_PROJECT_DIR_DEFAULT / "memory" / "luna_task_graph.json"),
    )
    parser.add_argument(
        "--out-md",
        default=str(_PROJECT_DIR_DEFAULT / "memory" / "luna_task_graph_report.md"),
    )
    parser.add_argument(
        "--out-report",
        default=str(_PROJECT_DIR_DEFAULT / "memory" / "luna_task_graph_build_report.json"),
    )
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    if not args.goal:
        parser.error("--goal is required (or use --self-test)")
    pdir = Path(args.project_dir)
    graph = build_task_graph(args.goal, project_dir=pdir)
    if args.write:
        write_task_graph(graph, args.out_json, args.out_md, args.out_report)
        print(
            json.dumps(
                {
                    "wrote_json": args.out_json,
                    "wrote_md": args.out_md,
                    "wrote_report": args.out_report,
                    "graph_id": graph["graph_id"],
                    "task_count": len(graph["tasks"]),
                    "overall_risk_level": graph["overall_risk_level"],
                    "overall_approval_tier_required": graph["overall_approval_tier_required"],
                    "intent_drift_status": graph["intent_drift"]["status"],
                },
                indent=2,
            )
        )
        return 0
    if args.print_markdown:
        sys.stdout.write(render_task_graph_markdown(graph))
        return 0
    if args.goal_only_json:
        sys.stdout.write(json.dumps(graph, indent=2))
        return 0
    print(
        json.dumps(
            {
                "graph_id": graph["graph_id"],
                "task_count": len(graph["tasks"]),
                "overall_risk_level": graph["overall_risk_level"],
                "overall_approval_tier_required": graph["overall_approval_tier_required"],
                "intent_drift_status": graph["intent_drift"]["status"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
