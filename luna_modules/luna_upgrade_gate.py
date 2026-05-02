"""Luna Safe Self-Upgrade Gate — Phase 5F foundation.

Purpose:
    Evaluate a proposed Luna edit (a "proposal" dict) and return a
    structured allow/deny/needs_approval decision *before* any self-upgrade
    is allowed. This module is **read-only and additive**. It does not edit
    code, does not run aider, does not run subprocesses, does not modify
    runtime services, and is not yet wired into worker.py.

Composition:
    - Phase 5B risk zones (`memory/luna_risk_zones.json`) — read-only.
    - Phase 5C change ledger schema (`memory/luna_change_ledger.schema.json`)
      — used only as a presence check; ledger rows are not required.
    - Phase 5D memory recall (`luna_modules.luna_memory_index.search_memory`)
      — optional, failure-tolerant.
    - Phase 5E playbook engine (`luna_modules.luna_playbook_engine.match_playbooks`)
      — optional, failure-tolerant.

CLI:
    python -m luna_modules.luna_upgrade_gate --self-test
    python -m luna_modules.luna_upgrade_gate --proposal-json '{...}' [--format markdown|json]
    python -m luna_modules.luna_upgrade_gate --proposal-file <path> [--format markdown|json]
    python -m luna_modules.luna_upgrade_gate --proposal-... --write-report

The CLI never edits runtime files. With --write-report it writes ONLY to
memory/luna_upgrade_gate_report.{json,md}, both gitignored.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
PROJECT_DIR = _THIS_FILE.parent.parent

DEFAULT_POLICY_PATH       = PROJECT_DIR / "memory" / "luna_upgrade_gate_policy.json"
DEFAULT_RISK_ZONES_PATH   = PROJECT_DIR / "memory" / "luna_risk_zones.json"
DEFAULT_REPORT_JSON_PATH  = PROJECT_DIR / "memory" / "luna_upgrade_gate_report.json"
DEFAULT_REPORT_MD_PATH    = PROJECT_DIR / "memory" / "luna_upgrade_gate_report.md"

ALLOWED_DECISIONS: Tuple[str, ...] = ("allow", "deny", "needs_approval")
ALLOWED_RISK_LEVELS: Tuple[str, ...] = ("low", "medium", "high", "critical")
ALLOWED_CHECK_STATUSES: Tuple[str, ...] = ("pass", "warn", "fail")


# ---------------------------------------------------------------------------
# Default policy (used when memory/luna_upgrade_gate_policy.json is missing)
# ---------------------------------------------------------------------------

_DEFAULT_POLICY: Dict[str, Any] = {
    "schema_version": 1,
    "default_autonomy_tier": 2,
    "max_files_changed": 1,
    "max_insertions": 120,
    "max_deletions": 80,
    "max_total_line_delta": 180,
    "allow_package_installs": False,
    "allow_external_network": False,
    "allow_personality_goal_changes": False,
    "allow_memory_deletes": False,
    "critical_files": [
        "luna_modules/luna_hygiene.py",
        "luna_modules/luna_paths.py",
        "luna_modules/luna_routing.py",
        "luna_modules/luna_state.py",
    ],
    "high_risk_files": [
        "worker.py",
        "aider_bridge.py",
        "luna_guardian.py",
        "LaunchLuna.pyw",
        "SurgeApp_Claude_Terminal.py",
        "luna_start.pyw",
        "director_agent.py",
    ],
    "forbidden_delete_prefixes": [
        "memory/", "logs/", "aider_jobs/", "backups/", "uploads/",
        "tasks/", "solutions/", "logic_updates/", "director_jobs/",
    ],
    "secret_path_patterns": [
        r"(?i)(^|/)\.env$",
        r"(?i)api_vault",
        r"(?i)(^|/)token($|/|\.)",
        r"(?i)(^|/)secret($|/|\.)",
        r"(?i)(^|/)credentials($|/|\.)",
    ],
    "personality_path_patterns": [
        "personality", "identity", "luna_system_prompt", "LUNA_SYSTEM_PROMPT",
        "luna_core_memory", "luna_personality", "user_goals",
    ],
    "install_command_patterns": [
        r"(?i)\bpip(?:3)?\s+install\b",
        r"(?i)\bpython\s+-m\s+pip\s+install\b",
        r"(?i)\buv\s+pip\s+install\b",
        r"(?i)\bnpm\s+install\b",
        r"(?i)\byarn\s+add\b",
        r"(?i)\bwinget\s+install\b",
        r"(?i)\bchoco\s+install\b",
        r"(?i)\bapt(?:-get)?\s+install\b",
        r"(?i)\bpacman\s+-S\b",
        r"(?i)\bbrew\s+install\b",
        r"(?i)\b(?:msiexec|setup\.exe)\b",
        r"(?i)Install-Module\b",
        r"(?i)Add-AppxPackage\b",
    ],
    "external_network_patterns": [
        r"(?i)\bhttps?://(?!127\.0\.0\.1|localhost)[a-z0-9.-]+",
        r"(?i)\bcurl\s+(?!.*(?:127\.0\.0\.1|localhost))",
        r"(?i)\bwget\s+(?!.*(?:127\.0\.0\.1|localhost))",
        r"(?i)\bInvoke-WebRequest\s+(?!.*(?:127\.0\.0\.1|localhost))",
        r"(?i)\bInvoke-RestMethod\s+(?!.*(?:127\.0\.0\.1|localhost))",
    ],
    "generated_runtime_report_patterns": [
        "memory/luna_upgrade_gate_report.json",
        "memory/luna_upgrade_gate_report.md",
    ],
}


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def now_iso() -> str:
    """UTC ISO-8601 timestamp."""
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_policy(policy_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the gate policy from JSON; fall back to in-module defaults."""
    target = Path(policy_path) if policy_path is not None else DEFAULT_POLICY_PATH
    raw = _read_json(target)
    if isinstance(raw, dict) and raw:
        merged = dict(_DEFAULT_POLICY)
        merged.update({k: v for k, v in raw.items() if v is not None})
        merged.setdefault("_source", str(target))
        merged.setdefault("_loaded_from_file", True)
        return merged
    fallback = dict(_DEFAULT_POLICY)
    fallback["_source"] = "module_fallback"
    fallback["_loaded_from_file"] = False
    return fallback


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------

def normalize_target(path: str) -> str:
    """Return project-relative POSIX path. Refuses traversal/outside-project."""
    raw = "" if path is None else str(path).strip()
    if not raw:
        return ""
    cleaned = raw.replace("\\", "/").rstrip("/")
    if ".." in Path(cleaned).parts:
        raise ValueError(f"path traversal not allowed: {raw!r}")
    p = Path(cleaned)
    if p.is_absolute():
        try:
            return str(p.resolve().relative_to(PROJECT_DIR.resolve())).replace("\\", "/")
        except Exception:
            raise ValueError(
                f"target file is outside project_dir: {cleaned!r} not under {PROJECT_DIR!s}"
            )
    return str(p).replace("\\", "/")


# ---------------------------------------------------------------------------
# Risk classification
# ---------------------------------------------------------------------------

def classify_target_risk(target_file: str,
                         risk_zones_path: Optional[Path] = None,
                         policy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Classify risk for a single target file.

    Reads memory/luna_risk_zones.json when present (Phase 5B output). Falls
    back to policy critical_files / high_risk_files lists. Always returns a
    dict with risk_level, source, and any zone metadata.
    """
    rel = normalize_target(target_file) if target_file else ""
    pol = policy or _DEFAULT_POLICY
    target = Path(risk_zones_path) if risk_zones_path is not None else DEFAULT_RISK_ZONES_PATH
    zones = _read_json(target)
    out: Dict[str, Any] = {
        "target_file": rel,
        "risk_level": "low",
        "source": "default",
        "safe_edit_zones": [],
        "forbidden_edit_zones": [],
        "high_risk_zones": [],
    }

    # 1. Phase 5B risk zones file wins when present and well-formed.
    if isinstance(zones, dict):
        zmap = zones.get("zones") or {}
        if isinstance(zmap, dict) and rel in zmap and isinstance(zmap[rel], dict):
            entry = zmap[rel]
            level = str(entry.get("risk_level") or "low").lower()
            if level not in ALLOWED_RISK_LEVELS:
                level = "low"
            out["risk_level"] = level
            out["safe_edit_zones"] = list(entry.get("safe_edit_zones") or [])
            out["forbidden_edit_zones"] = list(entry.get("forbidden_edit_zones") or [])
            out["high_risk_zones"] = list(entry.get("high_risk_zones") or [])
            out["source"] = "memory/luna_risk_zones.json"
            return out

    # 2. Fall back to policy lists.
    if rel in (pol.get("critical_files") or []):
        out["risk_level"] = "critical"
        out["source"] = "policy.critical_files"
    elif rel in (pol.get("high_risk_files") or []):
        out["risk_level"] = "high"
        out["source"] = "policy.high_risk_files"
    return out


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check(name: str, status: str, detail: str = "") -> Dict[str, Any]:
    if status not in ALLOWED_CHECK_STATUSES:
        status = "warn"
    return {"name": name, "status": status, "detail": detail or ""}


def evaluate_git_clean(status_text: str, allow_dirty: bool = False) -> Dict[str, Any]:
    """Evaluate a `git status --porcelain` text. Pass only if empty or
    explicitly approved-dirty."""
    text = (status_text or "").strip()
    if not text:
        return _check("git_clean", "pass", "tracked tree clean")
    if allow_dirty:
        return _check("git_clean", "warn",
                      "tracked tree has changes; allow_dirty=true (operator approval)")
    return _check("git_clean", "fail",
                  f"tracked tree has changes (first chars: {text[:200]!r})")


def evaluate_diff_size(diff_stats: Dict[str, Any],
                       policy: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce per-policy ceilings on files_changed / insertions / deletions /
    total_line_delta."""
    s = dict(diff_stats or {})
    fc = int(s.get("files_changed") or 0)
    ins = int(s.get("insertions") or 0)
    dele = int(s.get("deletions") or 0)
    delta = ins + dele
    max_files = int(policy.get("max_files_changed") or _DEFAULT_POLICY["max_files_changed"])
    max_ins = int(policy.get("max_insertions") or _DEFAULT_POLICY["max_insertions"])
    max_del = int(policy.get("max_deletions") or _DEFAULT_POLICY["max_deletions"])
    max_total = int(policy.get("max_total_line_delta") or _DEFAULT_POLICY["max_total_line_delta"])
    fails: List[str] = []
    if fc > max_files:
        fails.append(f"files_changed={fc} > max {max_files}")
    if ins > max_ins:
        fails.append(f"insertions={ins} > max {max_ins}")
    if dele > max_del:
        fails.append(f"deletions={dele} > max {max_del}")
    if delta > max_total:
        fails.append(f"total_line_delta={delta} > max {max_total}")
    if not fails:
        return _check(
            "diff_size", "pass",
            f"files={fc} ins={ins} del={dele} delta={delta} (limits "
            f"{max_files}/{max_ins}/{max_del}/{max_total})",
        )
    return _check("diff_size", "fail", "; ".join(fails))


def _proposal_text_blob(proposal: Dict[str, Any]) -> str:
    """Concatenate every plausibly-textual proposal field for pattern scanning."""
    parts: List[str] = []
    for key in ("title", "description", "reason", "notes", "expected_diff_type",
                "rollback_plan", "summary"):
        v = proposal.get(key) if isinstance(proposal, dict) else None
        if isinstance(v, str):
            parts.append(v)
    for list_key in ("install_commands", "verification_commands",
                     "external_network_calls", "shell_commands"):
        v = proposal.get(list_key) if isinstance(proposal, dict) else None
        if isinstance(v, list):
            for x in v:
                if isinstance(x, str):
                    parts.append(x)
    diff_text = ""
    if isinstance(proposal, dict):
        diff_text = str(proposal.get("diff_text") or proposal.get("diff") or "")
    if diff_text:
        parts.append(diff_text)
    return "\n".join(parts)


def evaluate_install_or_external_actions(proposal: Dict[str, Any],
                                         policy: Dict[str, Any]) -> Dict[str, Any]:
    """Deny installs and non-localhost external network unless policy allows."""
    blob = _proposal_text_blob(proposal)
    fails: List[str] = []
    install_ok = bool(policy.get("allow_package_installs", False))
    network_ok = bool(policy.get("allow_external_network", False))
    operator_approved = bool(proposal.get("operator_approved", False))

    # Explicit install_commands list (any non-empty entry)
    install_cmds = [c for c in (proposal.get("install_commands") or []) if c]
    if install_cmds and not (install_ok and operator_approved):
        fails.append(f"install_commands present ({len(install_cmds)} entries) "
                     f"and policy.allow_package_installs={install_ok}, "
                     f"operator_approved={operator_approved}")

    # Pattern match install commands inside the blob
    for pat in policy.get("install_command_patterns") or []:
        try:
            if re.search(pat, blob):
                if not (install_ok and operator_approved):
                    fails.append(f"install pattern matched: {pat}")
                break
        except re.error:
            continue

    # External network
    external_flag = bool(proposal.get("external_network", False))
    if external_flag and not (network_ok and operator_approved):
        fails.append(
            f"proposal.external_network=True but policy.allow_external_network={network_ok}, "
            f"operator_approved={operator_approved}"
        )
    for pat in policy.get("external_network_patterns") or []:
        try:
            if re.search(pat, blob):
                if not (network_ok and operator_approved):
                    fails.append(f"external network pattern matched: {pat}")
                break
        except re.error:
            continue

    if not fails:
        return _check("install_and_external", "pass",
                      "no installs / no external network in proposal")
    return _check("install_and_external", "fail", "; ".join(fails))


def evaluate_forbidden_paths(proposal: Dict[str, Any],
                             policy: Dict[str, Any]) -> Dict[str, Any]:
    """Deny deletes/truncates/moves under protected runtime prefixes; deny
    edits to secret-looking paths."""
    fails: List[str] = []
    targets = [normalize_target(t) if isinstance(t, str) else "" for t in
               (proposal.get("target_files") or [])]
    targets = [t for t in targets if t]

    # Direct edits to secret-looking paths
    for t in targets:
        for pat in policy.get("secret_path_patterns") or []:
            try:
                if re.search(pat, t):
                    fails.append(f"target {t!r} matches secret pattern {pat!r}")
                    break
            except re.error:
                continue

    # Touches runtime queue / logs / backup
    if proposal.get("touches_runtime_queue"):
        fails.append("proposal.touches_runtime_queue=True is denied")
    if proposal.get("touches_memory_content"):
        memory_ok = bool(policy.get("allow_memory_deletes", False))
        operator_approved = bool(proposal.get("operator_approved", False))
        if not (memory_ok and operator_approved):
            fails.append(
                f"proposal.touches_memory_content=True but "
                f"policy.allow_memory_deletes={memory_ok}, "
                f"operator_approved={operator_approved}"
            )

    # Explicit delete/move/truncate ops
    delete_ops = proposal.get("delete_paths") or []
    forbidden_prefixes = tuple(policy.get("forbidden_delete_prefixes") or [])
    for op_path in delete_ops:
        if not isinstance(op_path, str):
            continue
        norm = op_path.replace("\\", "/").lstrip("/")
        for prefix in forbidden_prefixes:
            if norm.startswith(prefix):
                fails.append(f"delete path {norm!r} under forbidden prefix {prefix!r}")
                break

    # Pattern match forbidden delete-y commands in the blob
    blob = _proposal_text_blob(proposal)
    deletion_patterns = (
        r"(?i)\brm\s+-rf?\s+",
        r"(?i)Remove-Item\s+(?:-Recurse|-Force)",
        r"(?i)Remove-Item\s+",
        r"(?i)\bshutil\.rmtree\(",
        r"(?i)\bos\.unlink\(",
        r"(?i)\bos\.remove\(",
        r"(?i)\bdel\s+/[FQS]\b",
    )
    danger_zones = forbidden_prefixes
    for pat in deletion_patterns:
        try:
            for m in re.finditer(pat, blob):
                tail = blob[m.end(): m.end() + 200]
                # Flag only if a forbidden prefix appears nearby
                norm_tail = tail.replace("\\", "/")
                hit = next((p for p in danger_zones if p in norm_tail), None)
                if hit:
                    fails.append(f"forbidden delete-like command near {hit!r}: {pat!r}")
                    break
        except re.error:
            continue

    if not fails:
        return _check("forbidden_paths", "pass",
                      "no forbidden delete/secret path operations detected")
    return _check("forbidden_paths", "fail", "; ".join(fails))


def evaluate_personality_goal_safety(proposal: Dict[str, Any],
                                     policy: Dict[str, Any]) -> Dict[str, Any]:
    """Deny edits to personality/identity/goals/system-prompt files unless
    explicit per-proposal approval AND policy allow_personality_goal_changes=True."""
    fails: List[str] = []
    targets = [normalize_target(t) if isinstance(t, str) else "" for t in
               (proposal.get("target_files") or [])]
    targets = [t for t in targets if t]
    pers_patterns = policy.get("personality_path_patterns") or []
    explicit_flag = bool(proposal.get("touches_personality_or_goals", False))
    explicit_approval = bool(proposal.get("personality_change_approved", False))
    policy_ok = bool(policy.get("allow_personality_goal_changes", False))

    detected_targets: List[str] = []
    low_targets = [t.lower() for t in targets]
    for t, low in zip(targets, low_targets):
        for sub in pers_patterns:
            if isinstance(sub, str) and sub.lower() in low:
                detected_targets.append(t)
                break

    blob_low = _proposal_text_blob(proposal).lower()
    blob_hits = [s for s in pers_patterns
                 if isinstance(s, str) and s.lower() in blob_low]

    if explicit_flag or detected_targets or blob_hits:
        if not (policy_ok and explicit_approval):
            if detected_targets:
                fails.append(f"personality/goals targets: {detected_targets[:5]}")
            if blob_hits:
                fails.append(f"personality/goals patterns in proposal text: {blob_hits[:5]}")
            if explicit_flag:
                fails.append(
                    f"touches_personality_or_goals=True; "
                    f"policy.allow_personality_goal_changes={policy_ok}, "
                    f"personality_change_approved={explicit_approval}"
                )

    if not fails:
        return _check("personality_goal_safety", "pass",
                      "no personality / identity / goals changes detected")
    return _check("personality_goal_safety", "fail", "; ".join(fails))


def evaluate_plan_contract(proposal: Dict[str, Any],
                           policy: Dict[str, Any]) -> Dict[str, Any]:
    """Require plan_id, target_files, reason/title, expected_diff_type,
    rollback_plan, verification_commands; line_ranges encouraged for code edits."""
    p = proposal or {}
    fails: List[str] = []
    warns: List[str] = []
    if not (isinstance(p.get("plan_id"), str) and p["plan_id"].strip()):
        fails.append("missing plan_id")
    targets = p.get("target_files")
    if not (isinstance(targets, list) and targets and all(isinstance(t, str) and t.strip() for t in targets)):
        fails.append("missing target_files (non-empty list of strings)")
    title = p.get("title") or p.get("reason")
    if not (isinstance(title, str) and title.strip()):
        fails.append("missing title or reason")
    if not (isinstance(p.get("expected_diff_type"), str) and p["expected_diff_type"].strip()):
        fails.append("missing expected_diff_type")
    if not (isinstance(p.get("rollback_plan"), str) and p["rollback_plan"].strip()):
        fails.append("missing rollback_plan")
    vc = p.get("verification_commands")
    if not (isinstance(vc, list) and vc):
        fails.append("missing verification_commands (non-empty list)")
    # line_ranges encouraged for code edits
    action_type = str(p.get("action_type") or "").lower()
    if action_type in ("edit", "refactor", "rollback") and not p.get("line_ranges"):
        warns.append("line_ranges missing for code edit")

    if fails:
        detail = "; ".join(fails)
        if warns:
            detail += " | warns: " + "; ".join(warns)
        return _check("plan_contract", "fail", detail)
    if warns:
        return _check("plan_contract", "warn", "; ".join(warns))
    return _check("plan_contract", "pass", "all required plan fields present")


# ---------------------------------------------------------------------------
# Optional integrations (failure-tolerant)
# ---------------------------------------------------------------------------

def match_relevant_playbooks(proposal: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    """Best-effort Phase 5E lookup. Never raises."""
    try:
        from luna_modules.luna_playbook_engine import match_playbooks  # type: ignore
    except Exception:
        return []
    text_parts: List[str] = []
    for k in ("title", "reason", "description", "notes",
              "action_type", "expected_diff_type"):
        v = proposal.get(k) if isinstance(proposal, dict) else None
        if isinstance(v, str):
            text_parts.append(v)
    for t in proposal.get("target_files") or []:
        if isinstance(t, str):
            text_parts.append(t)
    blob = " ".join(text_parts)
    try:
        results = match_playbooks(blob, limit=int(limit)) or []
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        p = r.get("playbook") or {}
        out.append({
            "playbook_id": p.get("playbook_id"),
            "title": p.get("title"),
            "severity": p.get("severity"),
            "approval_tier_required": p.get("approval_tier_required"),
            "score": r.get("score"),
            "first_safe_action": (p.get("safe_first_actions") or [None])[0],
        })
    return out


def recall_similar_failures(proposal: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    """Best-effort Phase 5D lookup against luna_memory_index. Never raises."""
    try:
        from luna_modules.luna_memory_index import search_memory  # type: ignore
    except Exception:
        return []
    parts: List[str] = []
    for k in ("title", "reason", "action_type", "expected_diff_type"):
        v = proposal.get(k) if isinstance(proposal, dict) else None
        if isinstance(v, str):
            parts.append(v)
    for t in proposal.get("target_files") or []:
        if isinstance(t, str):
            parts.append(t)
    query = " ".join(parts).strip()
    if not query:
        return []
    try:
        rows = search_memory(query=query, limit=int(limit)) or []
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        out.append({
            "summary_id": r.get("summary_id") or r.get("_index"),
            "source_path": r.get("source_path") or r.get("src"),
            "tags": r.get("tags") or [],
            "score": r.get("_score") if "_score" in r else r.get("rank"),
        })
    return out


# ---------------------------------------------------------------------------
# Main gate
# ---------------------------------------------------------------------------

def _risk_score_from_levels(levels: List[str]) -> int:
    """Highest risk wins. Returns 0..100."""
    weight = {"low": 10, "medium": 35, "high": 65, "critical": 90}
    if not levels:
        return 5
    return max(weight.get(str(l).lower(), 5) for l in levels)


def _risk_level_for_score(score: int) -> str:
    if score >= 80:
        return "critical"
    if score >= 55:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def evaluate_upgrade_proposal(proposal: Dict[str, Any],
                              project_dir: Optional[Path] = None,
                              policy_path: Optional[Path] = None,
                              context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Main gate. Returns the structured decision dict."""
    proposal = dict(proposal or {})
    context = dict(context or {})
    policy = load_policy(policy_path)
    active_tier = int(policy.get("default_autonomy_tier") or 2)

    checks: List[Dict[str, Any]] = []
    reasons: List[str] = []

    # 1. plan contract
    contract = evaluate_plan_contract(proposal, policy)
    checks.append(contract)

    # 2. install / external network
    inst = evaluate_install_or_external_actions(proposal, policy)
    checks.append(inst)

    # 3. forbidden paths / deletions
    fp = evaluate_forbidden_paths(proposal, policy)
    checks.append(fp)

    # 4. personality / goals
    ps = evaluate_personality_goal_safety(proposal, policy)
    checks.append(ps)

    # 5. diff size
    ds = evaluate_diff_size(proposal.get("diff_stats") or {}, policy)
    checks.append(ds)

    # 6. git clean — caller may pass status text via context['git_status']
    git_status_text = str(context.get("git_status") or "")
    allow_dirty = bool(proposal.get("allow_dirty", False))
    gc = evaluate_git_clean(git_status_text, allow_dirty=allow_dirty)
    checks.append(gc)

    # 7. classify each target
    targets = proposal.get("target_files") or []
    target_risks: List[Dict[str, Any]] = []
    risk_levels_present: List[str] = []
    forbidden_zone_hits: List[str] = []
    for t in targets:
        if not isinstance(t, str) or not t.strip():
            continue
        try:
            tr = classify_target_risk(t, policy=policy)
        except ValueError as exc:
            target_risks.append({"target_file": t, "error": str(exc)[:200]})
            checks.append(_check("path_safety", "fail",
                                 f"target {t!r}: {exc!s}"))
            continue
        target_risks.append(tr)
        risk_levels_present.append(tr.get("risk_level") or "low")
        # If proposal supplies line_ranges per file, check forbidden zones
        proposed_ranges = (proposal.get("line_ranges") or {}).get(t) or []
        for forb in tr.get("forbidden_edit_zones") or []:
            try:
                fs = int((forb[0] if isinstance(forb, list) else None) or 0)
                fe_raw = forb[1] if isinstance(forb, list) else None
                fe = int(fe_raw) if fe_raw is not None else 0
                if fe == -1:
                    fe = 10 ** 9
            except Exception:
                continue
            for span in proposed_ranges:
                try:
                    a, b = int(span[0]), int(span[1])
                except Exception:
                    continue
                if a <= fe and fs <= b:
                    forbidden_zone_hits.append(
                        f"{t}:{a}-{b} overlaps forbidden zone {fs}-{fe} ({forb[2] if len(forb) > 2 else ''})"
                    )

    if forbidden_zone_hits:
        checks.append(_check(
            "risk_zones", "fail",
            "; ".join(forbidden_zone_hits[:5]),
        ))
    else:
        checks.append(_check(
            "risk_zones", "pass",
            f"{len(target_risks)} target(s) classified; no forbidden-zone overlap",
        ))

    # 8. Optional Phase 5E playbook + Phase 5D recall
    matched_playbooks = match_relevant_playbooks(proposal, limit=3)
    similar_failures = recall_similar_failures(proposal, limit=3)

    # ---- Decision ----------------------------------------------------------
    has_fail = any(c["status"] == "fail" for c in checks)
    has_warn = any(c["status"] == "warn" for c in checks)
    risk_score = _risk_score_from_levels(risk_levels_present)
    risk_level = _risk_level_for_score(risk_score)

    # Required approval tier:
    #   low -> max(1, active)  (logs/reports OK)
    #   medium -> 2
    #   high -> 3
    #   critical -> 4
    tier_by_risk = {"low": 1, "medium": 2, "high": 3, "critical": 4}
    tier_required = tier_by_risk.get(risk_level, 2)

    operator_approved = bool(proposal.get("operator_approved", False))
    proposal_tier = int(proposal.get("approval_tier") or 0)

    # Defaults
    decision = "needs_approval"
    safe_next = "Operator must approve this proposal before any change is applied."

    if has_fail:
        decision = "deny"
        for c in checks:
            if c["status"] == "fail":
                reasons.append(f"{c['name']}: {c['detail']}")
        safe_next = "Fix the failing check(s) above and re-evaluate."
    elif risk_level == "critical":
        decision = "needs_approval" if (operator_approved and proposal_tier >= tier_required) else "deny"
        if decision == "deny":
            reasons.append(
                f"target risk_level=critical; tier_required={tier_required} > "
                f"proposal.approval_tier={proposal_tier} (operator_approved={operator_approved})"
            )
            safe_next = "Critical-risk targets are never auto-allowed. Get explicit operator approval and set approval_tier appropriately."
        else:
            safe_next = "Critical-risk proposal acknowledged; needs_approval pending operator final review."
    elif tier_required > active_tier:
        decision = "needs_approval"
        reasons.append(
            f"required tier {tier_required} > active_tier {active_tier} from policy"
        )
        safe_next = "Operator must approve and bump approval_tier on the proposal."
        if operator_approved and proposal_tier >= tier_required:
            decision = "allow"
            safe_next = "Operator-approved with sufficient tier; allow."
    else:
        # Eligible to allow
        if has_warn:
            decision = "needs_approval"
            safe_next = "Resolve warnings or get operator approval; proposal is otherwise within policy."
        else:
            decision = "allow"
            safe_next = "All checks pass; proposal is within active autonomy tier."

    required_verification = list(proposal.get("verification_commands") or [])
    required_backups: List[str] = []
    for t in targets:
        rel = t if isinstance(t, str) else ""
        if rel:
            required_backups.append(
                f"backups/<phase>_<ts>/{rel.replace('/', '__')}.bak"
            )

    return {
        "ok": decision == "allow",
        "decision": decision,
        "risk_score": int(risk_score),
        "risk_level": risk_level,
        "approval_tier_required": int(tier_required),
        "active_autonomy_tier": int(active_tier),
        "reasons": reasons,
        "checks": checks,
        "target_risks": target_risks,
        "required_verification": required_verification,
        "required_backups": required_backups,
        "matched_playbooks": matched_playbooks,
        "similar_failures": similar_failures,
        "safe_next_action": safe_next,
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "policy_source": policy.get("_source", "module_fallback"),
        "policy_loaded_from_file": bool(policy.get("_loaded_from_file", False)),
        "proposal_id": proposal.get("plan_id") or "",
        "proposal_title": proposal.get("title") or proposal.get("reason") or "",
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def render_gate_report(decision: Dict[str, Any], out_format: str = "markdown") -> str:
    """Render an operator-friendly report. out_format: 'markdown' or 'json'."""
    out_format = (out_format or "markdown").lower()
    if out_format == "json":
        return json.dumps(decision, indent=2, sort_keys=True, ensure_ascii=True)
    d = decision or {}
    lines: List[str] = []
    lines.append("# Luna Safe Self-Upgrade Gate — Decision")
    lines.append("")
    lines.append(f"- Generated: `{d.get('generated_at', '')}`")
    lines.append(f"- Proposal: `{d.get('proposal_id', '')}` — {d.get('proposal_title', '')}")
    lines.append(
        f"- **Decision: {str(d.get('decision', '')).upper()}** · "
        f"risk: **{d.get('risk_level', '?')}** ({d.get('risk_score', 0)}/100) · "
        f"approval_tier_required: **{d.get('approval_tier_required', '?')}** · "
        f"active_autonomy_tier: {d.get('active_autonomy_tier', '?')}"
    )
    lines.append(f"- Policy: `{d.get('policy_source', 'module_fallback')}` "
                 f"(from_file={d.get('policy_loaded_from_file', False)})")
    lines.append("")
    if d.get("reasons"):
        lines.append("## Reasons")
        for r in d["reasons"]:
            lines.append(f"- {r}")
        lines.append("")
    lines.append("## Checks")
    for c in d.get("checks", []):
        lines.append(f"- **{c.get('name')}**: `{c.get('status')}` — {c.get('detail', '')}")
    lines.append("")
    if d.get("target_risks"):
        lines.append("## Target risks")
        for tr in d["target_risks"]:
            lines.append(
                f"- `{tr.get('target_file')}` · risk: **{tr.get('risk_level')}** "
                f"(source: {tr.get('source')})"
            )
        lines.append("")
    if d.get("matched_playbooks"):
        lines.append("## Matched playbooks (Phase 5E)")
        for m in d["matched_playbooks"]:
            lines.append(f"- `{m.get('playbook_id')}` · severity: {m.get('severity')} · "
                         f"first safe action: {m.get('first_safe_action')}")
        lines.append("")
    if d.get("similar_failures"):
        lines.append("## Similar past failures (Phase 5D)")
        for s in d["similar_failures"]:
            lines.append(f"- `{s.get('summary_id')}` · src=`{s.get('source_path')}`")
        lines.append("")
    if d.get("required_verification"):
        lines.append("## Required verification")
        for v in d["required_verification"]:
            lines.append(f"- `{v}`")
        lines.append("")
    if d.get("required_backups"):
        lines.append("## Required backups")
        for b in d["required_backups"]:
            lines.append(f"- `{b}`")
        lines.append("")
    lines.append(f"**Safe next action:** {d.get('safe_next_action', '')}")
    return "\n".join(lines)


def write_gate_report(decision: Dict[str, Any], path: Optional[Path] = None) -> str:
    """Write the report to disk. JSON if path ends with .json, else Markdown."""
    if path is None:
        target = DEFAULT_REPORT_JSON_PATH
    else:
        target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    out_format = "json" if str(target).lower().endswith(".json") else "markdown"
    target.write_text(render_gate_report(decision, out_format=out_format),
                      encoding="utf-8")
    return str(target)


# ---------------------------------------------------------------------------
# Self-test + CLI
# ---------------------------------------------------------------------------

def _good_low_risk_proposal() -> Dict[str, Any]:
    return {
        "plan_id": "plan_self_test_low_risk",
        "title": "smoke: tiny logging-comment edit",
        "actor": "self_test",
        "target_files": ["luna_modules/luna_logging.py"],
        "line_ranges": {"luna_modules/luna_logging.py": [[60, 65]]},
        "action_type": "edit",
        "expected_diff_type": "small_edit",
        "risk_level": "low",
        "approval_tier": 2,
        "diff_stats": {"files_changed": 1, "insertions": 4, "deletions": 1},
        "verification_commands": [
            "python -m py_compile luna_modules/luna_logging.py",
        ],
        "rollback_plan": "git checkout HEAD -- luna_modules/luna_logging.py",
        "install_commands": [],
        "external_network": False,
        "touches_personality_or_goals": False,
        "touches_memory_content": False,
        "touches_runtime_queue": False,
        "operator_approved": False,
    }


def _worker_proposal() -> Dict[str, Any]:
    p = _good_low_risk_proposal()
    p["plan_id"] = "plan_self_test_worker"
    p["title"] = "smoke: would-edit worker.py logging"
    p["target_files"] = ["worker.py"]
    p["line_ranges"] = {"worker.py": [[12200, 12210]]}
    return p


def _install_proposal() -> Dict[str, Any]:
    p = _good_low_risk_proposal()
    p["plan_id"] = "plan_self_test_install"
    p["title"] = "smoke: pip install requests"
    p["install_commands"] = ["pip install requests"]
    return p


def _memory_delete_proposal() -> Dict[str, Any]:
    p = _good_low_risk_proposal()
    p["plan_id"] = "plan_self_test_memdel"
    p["title"] = "smoke: would-delete memory/luna_change_ledger.jsonl"
    p["touches_memory_content"] = True
    p["delete_paths"] = ["memory/luna_change_ledger.jsonl"]
    return p


def self_test() -> int:
    """Smoke each gate path on synthetic proposals. Exit 0 iff all pass."""
    failures: List[str] = []
    # 1. Low-risk proposal: must NOT deny on a fail check.
    d1 = evaluate_upgrade_proposal(_good_low_risk_proposal())
    if d1["decision"] == "deny":
        failures.append(f"low-risk denied: {d1['reasons']}")
    # 2. worker.py proposal: must not auto-allow.
    d2 = evaluate_upgrade_proposal(_worker_proposal())
    if d2["decision"] == "allow":
        failures.append("worker.py proposal was auto-allowed")
    # 3. install proposal: must deny.
    d3 = evaluate_upgrade_proposal(_install_proposal())
    if d3["decision"] != "deny":
        failures.append(f"install proposal not denied: {d3['decision']}")
    # 4. memory deletion proposal: must deny.
    d4 = evaluate_upgrade_proposal(_memory_delete_proposal())
    if d4["decision"] != "deny":
        failures.append(f"memory deletion not denied: {d4['decision']}")

    summary = {
        "ok": not failures,
        "failures": failures,
        "samples": {
            "low_risk":   {"decision": d1["decision"], "risk_level": d1["risk_level"]},
            "worker_py":  {"decision": d2["decision"], "risk_level": d2["risk_level"]},
            "install":    {"decision": d3["decision"], "risk_level": d3["risk_level"]},
            "mem_delete": {"decision": d4["decision"], "risk_level": d4["risk_level"]},
        },
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not failures else 1


def _cli(argv: List[str]) -> int:
    args = list(argv or [])
    if "--self-test" in args:
        return self_test()

    proposal: Optional[Dict[str, Any]] = None
    if "--proposal-json" in args:
        try:
            i = args.index("--proposal-json")
            proposal = json.loads(args[i + 1])
        except Exception as exc:
            print(f"--proposal-json parse error: {exc}", file=sys.stderr)
            return 2
    elif "--proposal-file" in args:
        try:
            i = args.index("--proposal-file")
            proposal = json.loads(Path(args[i + 1]).read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"--proposal-file read error: {exc}", file=sys.stderr)
            return 3

    if proposal is None:
        print(
            "luna_upgrade_gate — usage:\n"
            "  --self-test\n"
            "  --proposal-json '<json>' [--format markdown|json] [--write-report]\n"
            "  --proposal-file <path>  [--format markdown|json] [--write-report]"
        )
        return 0

    out_format = "markdown"
    if "--format" in args:
        try:
            fi = args.index("--format")
            out_format = str(args[fi + 1] or "markdown").lower()
        except Exception:
            out_format = "markdown"

    decision = evaluate_upgrade_proposal(proposal)
    print(render_gate_report(decision, out_format=out_format))

    if "--write-report" in args:
        if out_format == "json":
            path = write_gate_report(decision, path=DEFAULT_REPORT_JSON_PATH)
        else:
            path = write_gate_report(decision, path=DEFAULT_REPORT_MD_PATH)
        # Also drop a JSON sibling so callers always get machine-readable output
        write_gate_report(decision, path=DEFAULT_REPORT_JSON_PATH)
        print(f"\n# wrote report to: {path}")

    return 0 if decision["decision"] == "allow" else 4


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
