"""Phase 5Z: Luna 24-Hour Advisory Soak Verdict Module.

Stdlib only. Reads a completed advisory soak report (from Phase 5VW
luna_decision_brief --soak) plus the formal soak policy from Phase 5XY,
validates each `success_requires` check, and produces a PASS /
FAIL_WITH_REASONS / NO_SOAK_FOUND / INCOMPLETE verdict for Serge.

Hard rules in Phase 5Z:
  * advisory_only is ALWAYS True.
  * safe_to_execute_now is ALWAYS False.
  * safe_to_apply_real_project is ALWAYS False.
  * guardian_enforcing_live is ALWAYS False.
  * live_enforcement_ready is ALWAYS False (even if soak passes).
  * Phase 5Z does NOT run the long soak.
  * Phase 5Z does NOT enable live Guardian enforcement.

Tracked schema/checklist:
  memory/luna_soak_verdict.schema.json
  memory/luna_live_enforcement_readiness_checklist.json

Generated runtime artifacts (gitignored):
  memory/luna_soak_verdict_report.json
  memory/luna_soak_verdict_report.md
  memory/luna_live_enforcement_readiness_status.json
  memory/luna_live_enforcement_readiness_status.md

CLI:
  python -m luna_modules.luna_soak_verdict --self-test
  python -m luna_modules.luna_soak_verdict --evaluate
  python -m luna_modules.luna_soak_verdict --evaluate --report memory/luna_advisory_soak_report.json
  python -m luna_modules.luna_soak_verdict --write
  python -m luna_modules.luna_soak_verdict --print-markdown
  python -m luna_modules.luna_soak_verdict --sample-pass
  python -m luna_modules.luna_soak_verdict --sample-fail
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Canonical recommendation tokens (mirror Phase 5T/5VW).
_REC_APPROVE = "APPROVE_RECOMMENDED"
_REC_WAIT = "WAIT_FOR_MORE_EVIDENCE"
_REC_DENY = "DO_NOT_APPROVE"
_REC_SERGE = "SERGE_ONLY"
_REC_UNKNOWN = "UNKNOWN"

# Overall recommendation tokens used by the brief/soak.
_OVERALL_TOKENS = (
    "continue_safe_routine", "wait_for_evidence",
    "do_not_approve", "serge_only", "no_actions",
)

_DEFAULT_FORMAL_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "phase": "5XY",
    "advisory_only": True,
    "safe_to_execute_now": False,
    "safe_to_apply_real_project": False,
    "guardian_enforcing_live": False,
    "recommended_cycles_for_24h": 144,
    "recommended_sleep_seconds": 600,
    "short_smoke_cycles": 3,
    "short_smoke_sleep_seconds": 1,
    "success_requires": [
        "no_hard_failures",
        "no_warnings",
        "safe_to_execute_now_false",
        "safe_to_apply_real_project_false",
        "guardian_enforcing_live_false",
        "stable_recommendation_or_explained_changes",
        "no_source_file_modifications",
    ],
    "command_template": (
        "D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe "
        "-m luna_modules.luna_decision_brief --soak --cycles 144 "
        "--sleep-seconds 600 --write-soak"
    ),
}


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_verdict_id(prefix: str = "verdict") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


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


def read_jsonl(path: Path | str, limit: int | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    """Read a JSONL file. Returns (rows, parse_warnings).

    Skips blank lines. Records parse errors as warnings but never raises.
    """
    p = Path(path)
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    if not p.is_file():
        return rows, warnings
    try:
        with p.open("r", encoding="utf-8") as fh:
            for i, line in enumerate(fh, 1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    obj = json.loads(stripped)
                except (ValueError, UnicodeDecodeError) as e:
                    warnings.append(f"jsonl_parse_error:line={i}:{type(e).__name__}")
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
                if limit is not None and len(rows) >= limit:
                    break
    except OSError as e:
        warnings.append(f"jsonl_read_error:{type(e).__name__}:{str(e)[:200]}")
    return rows, warnings


# ---------- policy ----------


def load_formal_soak_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_formal_advisory_soak_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_FORMAL_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(_DEFAULT_FORMAL_POLICY)
    for k, v in raw.items():
        out[k] = v
    # Hard rules always enforced regardless of policy contents.
    out["advisory_only"] = True
    out["safe_to_execute_now"] = False
    out["safe_to_apply_real_project"] = False
    out["guardian_enforcing_live"] = False
    out["_source"] = str(p)
    return out


# ---------- locating reports ----------


def find_latest_soak_report(project_dir: Path | str) -> dict[str, str]:
    """Find the most recent soak report + jsonl. Returns dict with paths and 'found'."""
    pdir = Path(project_dir)
    json_p = pdir / "memory" / "luna_advisory_soak_report.json"
    jsonl_p = pdir / "memory" / "luna_advisory_soak.jsonl"
    return {
        "report_path": str(json_p) if json_p.is_file() else "",
        "jsonl_path": str(jsonl_p) if jsonl_p.is_file() else "",
        "found": json_p.is_file() or jsonl_p.is_file(),
    }


# ---------- recommendation normalization ----------


def normalize_recommendation(value: Any) -> str:
    """Return a canonical recommendation token for either decision-card or soak-overall recs."""
    if value is None:
        return _REC_UNKNOWN
    text = str(value).strip()
    if not text:
        return _REC_UNKNOWN
    # Try lowercase soak overall first.
    low = text.lower().replace("-", "_").replace(" ", "_")
    if low in _OVERALL_TOKENS:
        return low
    # Try uppercase decision-card recommendation.
    up = text.upper().replace("-", "_").replace(" ", "_")
    if up in (_REC_APPROVE, _REC_WAIT, _REC_DENY, _REC_SERGE, _REC_UNKNOWN):
        return up
    if up in ("APPROVE", "APPROVED", "OK", "GREEN"):
        return _REC_APPROVE
    if up in ("WAIT", "PENDING", "EVIDENCE_NEEDED"):
        return _REC_WAIT
    if up in ("DENY", "DENIED", "BLOCK", "BLOCKED"):
        return _REC_DENY
    if up in ("SERGE", "HUMAN_ONLY", "OPERATOR_ONLY", "MANUAL"):
        return _REC_SERGE
    return _REC_UNKNOWN


# ---------- cycle extraction ----------


def extract_soak_cycles(report_or_rows: Any) -> list[dict[str, Any]]:
    """Extract per-cycle dicts from either a soak report or a JSONL row list.

    Defensive: handles missing keys, non-dict entries, and mixed shapes.
    """
    cycles: list[dict[str, Any]] = []
    if isinstance(report_or_rows, dict):
        cr = report_or_rows.get("cycle_results") or []
        if isinstance(cr, list):
            for c in cr:
                if isinstance(c, dict):
                    cycles.append(c)
    elif isinstance(report_or_rows, list):
        for row in report_or_rows:
            if isinstance(row, dict):
                cycles.append(row)
    return cycles


# ---------- check helpers ----------


def check_no_hard_failures(report: dict[str, Any] | None) -> dict[str, Any]:
    fails = []
    if isinstance(report, dict):
        fails = list(report.get("failures") or [])
    return {
        "name": "no_hard_failures",
        "ok": not fails,
        "evidence": fails,
    }


def check_no_warnings(report: dict[str, Any] | None) -> dict[str, Any]:
    warnings: list[str] = []
    explained: list[str] = []
    if isinstance(report, dict):
        warnings = list(report.get("warnings") or [])
        # Treat the known stability variation warning as the only one we accept
        # only if it is paired with an `explained_recommendation_changes` field.
        if warnings and isinstance(report.get("explained_recommendation_changes"), list):
            explained = list(report.get("explained_recommendation_changes"))
    return {
        "name": "no_warnings",
        "ok": not warnings,
        "evidence": warnings,
        "explained": explained,
    }


def _cycle_has_unsafe_field(cycle: dict[str, Any], key: str) -> bool:
    """Return True if the cycle has key set to anything other than False."""
    val = cycle.get(key, False)
    return val is not False


def check_hard_safety_fields_false(
    report: dict[str, Any] | None,
    cycles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return three checks: safe_to_execute_now, safe_to_apply_real_project,
    guardian_enforcing_live -- all must be False everywhere."""
    cycles = cycles if cycles is not None else extract_soak_cycles(report)
    rep = report if isinstance(report, dict) else {}

    out: list[dict[str, Any]] = []
    for key, name in (
        ("safe_to_execute_now", "safe_to_execute_now_false"),
        ("safe_to_apply_real_project", "safe_to_apply_real_project_false"),
        ("guardian_enforcing_live", "guardian_enforcing_live_false"),
    ):
        offenders: list[str] = []
        if _cycle_has_unsafe_field(rep, key):
            offenders.append(f"top_level:{key}={rep.get(key)!r}")
        for c in cycles:
            if _cycle_has_unsafe_field(c, key):
                offenders.append(
                    f"cycle:{c.get('cycle', '?')}:{key}={c.get(key)!r}"
                )
        out.append({
            "name": name,
            "ok": not offenders,
            "evidence": offenders,
        })
    return out


def check_guardian_not_enforcing(
    report: dict[str, Any] | None,
    cycles: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Composite check: guardian_enforcing_live must be False at top and per cycle."""
    cycles = cycles if cycles is not None else extract_soak_cycles(report)
    offenders: list[str] = []
    rep = report if isinstance(report, dict) else {}
    if _cycle_has_unsafe_field(rep, "guardian_enforcing_live"):
        offenders.append(f"top_level:guardian_enforcing_live={rep.get('guardian_enforcing_live')!r}")
    for c in cycles:
        if _cycle_has_unsafe_field(c, "guardian_enforcing_live"):
            offenders.append(f"cycle:{c.get('cycle', '?')}")
    return {
        "name": "guardian_not_enforcing",
        "ok": not offenders,
        "evidence": offenders,
    }


def check_recommendation_stability(
    report: dict[str, Any] | None,
    cycles: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Verify the overall_recommendation is stable across cycles, OR every change
    has an explanation in `explained_recommendation_changes`."""
    cycles = cycles if cycles is not None else extract_soak_cycles(report)
    recs = [
        normalize_recommendation(c.get("overall_recommendation"))
        for c in cycles
        if isinstance(c, dict)
    ]
    rec_set = set(r for r in recs if r != _REC_UNKNOWN)
    if len(rec_set) <= 1:
        return {
            "name": "stable_recommendation_or_explained_changes",
            "ok": True,
            "evidence": [f"single_recommendation:{recs[0]!r}" if recs else "no_cycles"],
            "stable_recommendation": recs[0] if recs else "no_actions",
            "recommendation_changes": [],
        }

    # Multiple distinct recommendations — only OK if every transition is explained.
    transitions: list[dict[str, Any]] = []
    prev = None
    for i, r in enumerate(recs):
        if prev is not None and r != prev:
            transitions.append({"from": prev, "to": r, "cycle_index": i + 1})
        prev = r

    explanations = []
    if isinstance(report, dict):
        explanations = list(report.get("explained_recommendation_changes") or [])

    explained_count = 0
    for t in transitions:
        for ex in explanations:
            if not isinstance(ex, dict):
                continue
            if (
                str(ex.get("from") or "") == t["from"]
                and str(ex.get("to") or "") == t["to"]
                and (ex.get("explanation") or "").strip()
            ):
                explained_count += 1
                break

    ok = (explained_count == len(transitions))
    return {
        "name": "stable_recommendation_or_explained_changes",
        "ok": ok,
        "evidence": [
            f"distinct_recommendations:{sorted(rec_set)!r}",
            f"transitions:{transitions!r}",
            f"explained_count:{explained_count}/{len(transitions)}",
        ],
        "stable_recommendation": recs[0] if recs else "no_actions",
        "recommendation_changes": transitions,
    }


def check_no_source_file_modifications(
    report: dict[str, Any] | None,
    cycles: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """If the report or any cycle records source-file modifications, fail."""
    cycles = cycles if cycles is not None else extract_soak_cycles(report)
    offenders: list[str] = []
    rep = report if isinstance(report, dict) else {}
    if rep.get("source_files_modified"):
        offenders.append(f"top_level:source_files_modified={rep.get('source_files_modified')!r}")
    for c in cycles:
        if c.get("source_files_modified"):
            offenders.append(f"cycle:{c.get('cycle', '?')}:{c.get('source_files_modified')!r}")
        if c.get("real_project_modified") is True:
            offenders.append(f"cycle:{c.get('cycle', '?')}:real_project_modified=true")
    return {
        "name": "no_source_file_modifications",
        "ok": not offenders,
        "evidence": offenders,
    }


# ---------- evaluator ----------


def evaluate_soak_report(
    project_dir: Path | str,
    report_path: Path | str | None = None,
    jsonl_path: Path | str | None = None,
) -> dict[str, Any]:
    """Evaluate a soak report against the formal soak policy. Returns a verdict dict.

    Hard rules: advisory_only, safe_to_execute_now=False, safe_to_apply_real_project=False,
    guardian_enforcing_live=False, live_enforcement_ready=False — always.
    """
    pdir = Path(project_dir).resolve()
    policy = load_formal_soak_policy(pdir)
    located = find_latest_soak_report(pdir)

    rp = Path(report_path) if report_path else (
        Path(located["report_path"]) if located["report_path"] else None
    )
    jp = Path(jsonl_path) if jsonl_path else (
        Path(located["jsonl_path"]) if located["jsonl_path"] else None
    )

    report: dict[str, Any] | None = None
    if rp is not None and rp.is_file():
        loaded = load_json(rp, default=None)
        if isinstance(loaded, dict):
            report = loaded

    jsonl_rows: list[dict[str, Any]] = []
    jsonl_parse_warnings: list[str] = []
    if jp is not None and jp.is_file():
        jsonl_rows, jsonl_parse_warnings = read_jsonl(jp)

    # If we have neither a report nor jsonl rows, return NO_SOAK_FOUND.
    if report is None and not jsonl_rows:
        return _build_verdict(
            project_dir=str(pdir),
            policy=policy,
            verdict="NO_SOAK_FOUND",
            source_report_path=str(rp or ""),
            source_jsonl_path=str(jp or ""),
            observed_cycles=0,
            observed_duration_seconds=0,
            checks=[],
            failures=["no_soak_report_or_jsonl_found"],
            warnings=jsonl_parse_warnings,
            stable_recommendation="",
            recommendation_changes=[],
            checklist_satisfied=False,
            serge_summary=(
                "No advisory soak evidence found yet. Run the 24-hour command "
                "in a separate window, then re-evaluate."
            ),
            recommended_next_action=(
                "Run: " + str(policy.get("command_template", ""))
            ),
        )

    # Use report if available, else synthesize a minimal report from JSONL rows.
    if report is None:
        report = {
            "schema_version": 1,
            "soak_id": "from_jsonl",
            "started_at": jsonl_rows[0].get("ts", "") if jsonl_rows else "",
            "finished_at": jsonl_rows[-1].get("ts", "") if jsonl_rows else "",
            "cycles": len(jsonl_rows),
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
            "cycle_results": jsonl_rows,
            "failures": [],
            "warnings": [],
        }

    cycles = extract_soak_cycles(report)
    if not cycles and jsonl_rows:
        cycles = jsonl_rows

    # Build checks.
    checks: list[dict[str, Any]] = []
    checks.append(check_no_hard_failures(report))
    checks.append(check_no_warnings(report))
    checks.extend(check_hard_safety_fields_false(report, cycles))
    checks.append(check_guardian_not_enforcing(report, cycles))
    stab = check_recommendation_stability(report, cycles)
    checks.append(stab)
    checks.append(check_no_source_file_modifications(report, cycles))

    # Aggregate failures / warnings.
    failures: list[str] = []
    warnings: list[str] = list(jsonl_parse_warnings)
    for ch in checks:
        if not ch["ok"]:
            failures.append(f"{ch['name']}:{ch['evidence']!r}")

    # Cycle / duration evidence.
    required_cycles = int(policy.get("recommended_cycles_for_24h", 144))
    required_sleep = int(policy.get("recommended_sleep_seconds", 600))
    required_duration = required_cycles * required_sleep
    observed_cycles = len(cycles)
    observed_duration = float(report.get("duration_seconds") or 0.0)
    completed_24h_flag = bool(report.get("completed_24h"))

    # Verdict logic.
    duration_ok = (
        observed_duration >= required_duration
        or completed_24h_flag
    )
    cycles_ok = observed_cycles >= required_cycles

    if not failures and cycles_ok and duration_ok:
        verdict = "PASS"
        checklist_satisfied = True
    elif not failures and (not cycles_ok or not duration_ok):
        verdict = "INCOMPLETE"
        checklist_satisfied = False
    else:
        verdict = "FAIL_WITH_REASONS"
        checklist_satisfied = False

    serge_summary = _build_serge_summary(
        verdict, observed_cycles, required_cycles,
        observed_duration, required_duration, failures,
        stab.get("stable_recommendation", ""),
    )

    if verdict == "PASS":
        rec_next = (
            "Mark `24_hour_advisory_soak` checklist item as satisfied. "
            "The other items (manual_schema_review, explicit_serge_approval, etc.) "
            "still need human review before any future live enforcement phase."
        )
    elif verdict == "INCOMPLETE":
        rec_next = (
            f"Soak is healthy but not the 24-hour evidence "
            f"({observed_cycles}/{required_cycles} cycles, "
            f"{observed_duration:.0f}/{required_duration} seconds). "
            "Run the full 24-hour command and re-evaluate."
        )
    else:
        rec_next = (
            "Address the failing checks above before re-running the soak. "
            "Live enforcement remains forbidden."
        )

    return _build_verdict(
        project_dir=str(pdir),
        policy=policy,
        verdict=verdict,
        source_report_path=str(rp or ""),
        source_jsonl_path=str(jp or ""),
        observed_cycles=observed_cycles,
        observed_duration_seconds=observed_duration,
        checks=checks,
        failures=failures,
        warnings=warnings,
        stable_recommendation=stab.get("stable_recommendation", ""),
        recommendation_changes=stab.get("recommendation_changes", []),
        checklist_satisfied=checklist_satisfied,
        serge_summary=serge_summary,
        recommended_next_action=rec_next,
    )


def _build_verdict(
    *,
    project_dir: str,
    policy: dict[str, Any],
    verdict: str,
    source_report_path: str,
    source_jsonl_path: str,
    observed_cycles: int,
    observed_duration_seconds: float,
    checks: list[dict[str, Any]],
    failures: list[str],
    warnings: list[str],
    stable_recommendation: str,
    recommendation_changes: list[dict[str, Any]],
    checklist_satisfied: bool,
    serge_summary: str,
    recommended_next_action: str,
) -> dict[str, Any]:
    required_cycles = int(policy.get("recommended_cycles_for_24h", 144))
    required_sleep = int(policy.get("recommended_sleep_seconds", 600))
    return {
        "schema_version": SCHEMA_VERSION,
        "verdict_id": make_verdict_id(),
        "generated_at": now_iso(),
        "project_dir": project_dir,
        "source_report_path": source_report_path,
        "source_jsonl_path": source_jsonl_path,
        "advisory_only": True,
        "verdict": verdict,
        "required_cycles": required_cycles,
        "observed_cycles": int(observed_cycles),
        "required_duration_seconds": required_cycles * required_sleep,
        "observed_duration_seconds": float(observed_duration_seconds),
        "checks": checks,
        "failures": failures,
        "warnings": warnings,
        "stable_recommendation": stable_recommendation,
        "recommendation_changes": recommendation_changes,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "live_enforcement_ready": False,
        "checklist_item_24h_soak_satisfied": bool(checklist_satisfied),
        "serge_summary": serge_summary,
        "recommended_next_action": recommended_next_action,
        "notes": [
            "Phase 5Z advisory only: this verdict does not enable live enforcement.",
            "live_enforcement_ready stays False even on PASS until a future phase.",
        ],
    }


def _build_serge_summary(
    verdict: str,
    observed_cycles: int,
    required_cycles: int,
    observed_duration: float,
    required_duration: int,
    failures: list[str],
    stable_recommendation: str,
) -> str:
    if verdict == "NO_SOAK_FOUND":
        return (
            "No advisory soak evidence found. The 24-hour soak has not been run yet, "
            "or the report files are missing."
        )
    if verdict == "INCOMPLETE":
        return (
            f"Short soak is healthy but not the 24-hour evidence: "
            f"{observed_cycles}/{required_cycles} cycles "
            f"({observed_duration:.0f}/{required_duration} seconds). "
            "Stable so far; rerun with the full 24h command."
        )
    if verdict == "PASS":
        return (
            f"Good news. The 24-hour advisory soak passed: {observed_cycles} cycles, "
            f"{observed_duration:.0f} seconds, stable recommendation "
            f"{stable_recommendation!r}, no failures, no warnings, "
            "no source file modifications. The 24_hour_advisory_soak checklist "
            "item is satisfied. Live enforcement is still NOT enabled — other "
            "checklist items still need human review."
        )
    if verdict == "FAIL_WITH_REASONS":
        return (
            f"Soak FAILED. Reasons: {failures[:3]!r}. Live enforcement remains "
            "blocked. Fix the failing checks and re-run the soak."
        )
    return "Soak verdict unclear."


# ---------- live enforcement readiness status ----------


def build_live_enforcement_readiness_status(
    project_dir: Path | str,
    verdict: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a readiness status report combining the verdict + the tracked checklist.

    live_enforcement_enabled is ALWAYS False in Phase 5Z.
    """
    pdir = Path(project_dir).resolve()
    checklist = load_json(
        pdir / "memory" / "luna_live_enforcement_readiness_checklist.json",
        default=None,
    )
    if not isinstance(checklist, dict):
        checklist = {
            "schema_version": 1,
            "phase": "5Z",
            "live_enforcement_enabled": False,
            "advisory_only": True,
            "required_before_live_guardian_enforcement": [],
            "forbidden_until_complete": [],
        }

    items = list(checklist.get("required_before_live_guardian_enforcement") or [])
    soak_item_ok = bool(verdict and verdict.get("checklist_item_24h_soak_satisfied"))

    updated_items: list[dict[str, Any]] = []
    pending_count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        new_item = dict(item)
        if new_item.get("id") == "24_hour_advisory_soak":
            new_item["status"] = "satisfied" if soak_item_ok else "pending"
        if new_item.get("status") not in ("satisfied", "skipped"):
            pending_count += 1
        updated_items.append(new_item)

    all_satisfied = pending_count == 0 and bool(updated_items)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(pdir),
        "advisory_only": True,
        "live_enforcement_enabled": False,
        "live_enforcement_ready": False,  # Phase 5Z hard rule
        "all_checklist_items_satisfied": all_satisfied,
        "pending_count": pending_count,
        "items": updated_items,
        "verdict_summary": {
            "verdict": (verdict or {}).get("verdict", "NO_SOAK_FOUND"),
            "checklist_item_24h_soak_satisfied": soak_item_ok,
            "stable_recommendation": (verdict or {}).get("stable_recommendation", ""),
            "observed_cycles": (verdict or {}).get("observed_cycles", 0),
        },
        "forbidden_until_complete": list(checklist.get("forbidden_until_complete") or []),
        "notes": [
            "Phase 5Z advisory only.",
            "live_enforcement_enabled stays False until a future explicit phase.",
        ],
    }


# ---------- validation ----------


def validate_soak_verdict(verdict: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(verdict, dict):
        return False, ["verdict is not a dict"]
    required = (
        "schema_version", "verdict_id", "generated_at", "project_dir",
        "advisory_only", "verdict", "checks",
        "safe_to_execute_now", "safe_to_apply_real_project",
        "guardian_enforcing_live", "live_enforcement_ready",
        "checklist_item_24h_soak_satisfied",
        "serge_summary", "recommended_next_action",
    )
    for f in required:
        if f not in verdict:
            errs.append(f"missing_required_field:{f!r}")
    if verdict.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{verdict.get('schema_version')!r}")
    if verdict.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if verdict.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False")
    if verdict.get("safe_to_apply_real_project") is not False:
        errs.append("safe_to_apply_real_project must be False")
    if verdict.get("guardian_enforcing_live") is not False:
        errs.append("guardian_enforcing_live must be False")
    if verdict.get("live_enforcement_ready") is not False:
        errs.append("live_enforcement_ready must be False in Phase 5Z")
    valid_verdicts = ("PASS", "FAIL_WITH_REASONS", "NO_SOAK_FOUND", "INCOMPLETE")
    if verdict.get("verdict") not in valid_verdicts:
        errs.append(f"invalid_verdict:{verdict.get('verdict')!r}")
    return len(errs) == 0, errs


# ---------- markdown rendering ----------


def render_soak_verdict_markdown(verdict: dict[str, Any]) -> str:
    lines = [
        "# Luna 24-Hour Advisory Soak Verdict",
        "",
        f"**verdict_id**: {verdict.get('verdict_id', '')}",
        f"**generated_at**: {verdict.get('generated_at', '')}",
        f"**verdict**: {verdict.get('verdict', '')}",
        f"**advisory_only**: {verdict.get('advisory_only', True)}",
        f"**safe_to_execute_now**: {verdict.get('safe_to_execute_now', False)}",
        f"**safe_to_apply_real_project**: {verdict.get('safe_to_apply_real_project', False)}",
        f"**guardian_enforcing_live**: {verdict.get('guardian_enforcing_live', False)}",
        f"**live_enforcement_ready**: {verdict.get('live_enforcement_ready', False)}",
        f"**24_hour_advisory_soak satisfied**: {verdict.get('checklist_item_24h_soak_satisfied', False)}",
        "",
        "## Cycle / Duration Evidence",
        f"- required_cycles: {verdict.get('required_cycles', 0)}",
        f"- observed_cycles: {verdict.get('observed_cycles', 0)}",
        f"- required_duration_seconds: {verdict.get('required_duration_seconds', 0)}",
        f"- observed_duration_seconds: {verdict.get('observed_duration_seconds', 0)}",
        "",
        "## Plain-English Summary",
        verdict.get("serge_summary", ""),
        "",
        "## Checks",
    ]
    for ch in verdict.get("checks") or []:
        status = "OK" if ch.get("ok") else "FAIL"
        lines.append(f"- [{status}] {ch.get('name', '')}")
        for e in (ch.get("evidence") or [])[:5]:
            lines.append(f"   - {e}")

    fails = verdict.get("failures") or []
    if fails:
        lines += ["", "## Failures"]
        for f in fails:
            lines.append(f"- {f}")

    warns = verdict.get("warnings") or []
    if warns:
        lines += ["", "## Warnings"]
        for w in warns:
            lines.append(f"- {w}")

    lines += [
        "",
        f"**stable_recommendation**: {verdict.get('stable_recommendation', '')}",
        f"**recommended_next_action**: {verdict.get('recommended_next_action', '')}",
        "",
    ]
    return "\n".join(lines)


# ---------- write ----------


def write_soak_verdict_report(
    project_dir: Path | str,
    verdict: dict[str, Any],
) -> dict[str, str]:
    """Write verdict + readiness status artifacts under memory/."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_soak_verdict_report.json"
    md_p = mem / "luna_soak_verdict_report.md"

    write_json_atomic(json_p, verdict)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_soak_verdict_markdown(verdict), encoding="utf-8")
    os.replace(tmp, md_p)

    # Build and write the readiness status.
    readiness = build_live_enforcement_readiness_status(pdir, verdict=verdict)
    readiness_json_p = mem / "luna_live_enforcement_readiness_status.json"
    readiness_md_p = mem / "luna_live_enforcement_readiness_status.md"
    write_json_atomic(readiness_json_p, readiness)
    md_lines = [
        "# Luna Live Enforcement Readiness Status",
        "",
        f"**generated_at**: {readiness.get('generated_at', '')}",
        f"**advisory_only**: {readiness.get('advisory_only', True)}",
        f"**live_enforcement_enabled**: {readiness.get('live_enforcement_enabled', False)}",
        f"**live_enforcement_ready**: {readiness.get('live_enforcement_ready', False)}",
        f"**all_checklist_items_satisfied**: {readiness.get('all_checklist_items_satisfied', False)}",
        f"**pending_count**: {readiness.get('pending_count', 0)}",
        "",
        "## Checklist",
    ]
    for it in readiness.get("items") or []:
        md_lines.append(f"- [{it.get('status', '?')}] {it.get('id', '?')}")
    md_lines += ["", "## Verdict Summary"]
    vs = readiness.get("verdict_summary") or {}
    for k, v in vs.items():
        md_lines.append(f"- {k}: {v}")
    md_lines.append("")
    tmp_r = readiness_md_p.with_suffix(readiness_md_p.suffix + ".tmp")
    tmp_r.write_text("\n".join(md_lines), encoding="utf-8")
    os.replace(tmp_r, readiness_md_p)

    return {
        "json": str(json_p),
        "md": str(md_p),
        "readiness_json": str(readiness_json_p),
        "readiness_md": str(readiness_md_p),
    }


# ---------- sample reports ----------


def _sample_pass_report() -> dict[str, Any]:
    """Build a synthetic 144-cycle 24h clean soak report."""
    cycles = []
    for i in range(144):
        cycles.append({
            "cycle": i + 1,
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
            "overall_recommendation": "serge_only",
            "counts": {"approve_recommended": 0, "wait_for_more_evidence": 1,
                       "do_not_approve": 3, "serge_only": 1, "unknown": 0},
            "files_checked": 6,
            "missing_artifacts": 4,
        })
    return {
        "schema_version": 1,
        "soak_id": "soak_sample_pass",
        "started_at": "2026-05-01T00:00:00.000000Z",
        "finished_at": "2026-05-02T00:00:00.000000Z",
        "duration_seconds": 86400.0,
        "cycles": 144,
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "cycle_results": cycles,
        "failures": [],
        "warnings": [],
        "recommended_next_action": "stable_recommendation:serge_only",
    }


def _sample_fail_report() -> dict[str, Any]:
    """Build a synthetic failing soak report (failures + unsafe field)."""
    cycles = [
        {
            "cycle": 1, "advisory_only": True,
            "safe_to_execute_now": False, "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False, "overall_recommendation": "serge_only",
            "counts": {}, "files_checked": 6, "missing_artifacts": 4,
        },
        # Cycle 2 has an unsafe field — should fail the safety check.
        {
            "cycle": 2, "advisory_only": True,
            "safe_to_execute_now": True, "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False, "overall_recommendation": "continue_safe_routine",
            "counts": {}, "files_checked": 6, "missing_artifacts": 4,
        },
    ]
    return {
        "schema_version": 1,
        "soak_id": "soak_sample_fail",
        "started_at": "2026-05-01T00:00:00.000000Z",
        "finished_at": "2026-05-01T00:30:00.000000Z",
        "duration_seconds": 1800.0,
        "cycles": 2,
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "cycle_results": cycles,
        "failures": ["sample_failure_for_test"],
        "warnings": ["sample_warning_for_test"],
        "source_files_modified": ["worker.py"],
    }


def _evaluate_sample(project_dir: Path, report_dict: dict[str, Any]) -> dict[str, Any]:
    """Helper: write the sample to a temp report file inside project_dir/memory and evaluate."""
    pdir = Path(project_dir)
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    rp = mem / "luna_advisory_soak_report.json"
    write_json_atomic(rp, report_dict)
    return evaluate_soak_report(pdir, report_path=rp)


# ---------- self-test ----------


def self_test() -> int:
    """Run end-to-end self-test in a TemporaryDirectory. Returns 0 on success."""
    import tempfile

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)

        # 1. NO_SOAK_FOUND when no report present.
        v1 = evaluate_soak_report(td)
        assert v1["verdict"] == "NO_SOAK_FOUND", v1["verdict"]
        assert v1["safe_to_execute_now"] is False
        assert v1["live_enforcement_ready"] is False

        # 2. PASS for a synthetic 144-cycle clean report.
        v2 = _evaluate_sample(td, _sample_pass_report())
        assert v2["verdict"] == "PASS", f"expected PASS got {v2['verdict']}"
        assert v2["checklist_item_24h_soak_satisfied"] is True
        assert v2["safe_to_execute_now"] is False
        assert v2["safe_to_apply_real_project"] is False
        assert v2["guardian_enforcing_live"] is False
        assert v2["live_enforcement_ready"] is False  # Phase 5Z hard rule

        # 3. FAIL_WITH_REASONS for the synthetic failing report.
        v3 = _evaluate_sample(td, _sample_fail_report())
        assert v3["verdict"] == "FAIL_WITH_REASONS", f"expected FAIL got {v3['verdict']}"
        assert v3["checklist_item_24h_soak_satisfied"] is False
        assert v3["safe_to_execute_now"] is False

        # Validation should pass on all verdicts.
        for v in (v1, v2, v3):
            ok, errs = validate_soak_verdict(v)
            assert ok, f"verdict invalid: {errs}"

        # Markdown should mention the verdict.
        md = render_soak_verdict_markdown(v2)
        assert "PASS" in md
        assert "Plain-English Summary" in md

        # Write under temp memory only.
        written = write_soak_verdict_report(td, v2)
        for key, p in written.items():
            assert str(Path(p).resolve()).startswith(str(td.resolve())), \
                f"{p} escapes temp dir"
            assert "memory" in str(p)

        print(json.dumps({
            "self_test": "PASS",
            "no_soak_verdict": v1["verdict"],
            "pass_verdict": v2["verdict"],
            "fail_verdict": v3["verdict"],
            "live_enforcement_ready": False,
            "safe_to_execute_now": False,
        }, indent=2))
    return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Advisory Soak Verdict (Phase 5Z)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--report", default=None,
                        help="Path to a soak report JSON. Defaults to memory/luna_advisory_soak_report.json.")
    parser.add_argument("--jsonl", default=None,
                        help="Optional path to luna_advisory_soak.jsonl.")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--sample-pass", action="store_true")
    parser.add_argument("--sample-fail", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.sample_pass:
        import tempfile
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            verdict = _evaluate_sample(td, _sample_pass_report())
        summary = {
            "verdict": verdict["verdict"],
            "checklist_item_24h_soak_satisfied": verdict["checklist_item_24h_soak_satisfied"],
            "observed_cycles": verdict["observed_cycles"],
            "stable_recommendation": verdict["stable_recommendation"],
            "live_enforcement_ready": verdict["live_enforcement_ready"],
            "safe_to_execute_now": verdict["safe_to_execute_now"],
            "serge_summary": verdict["serge_summary"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    if args.sample_fail:
        import tempfile
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            verdict = _evaluate_sample(td, _sample_fail_report())
        summary = {
            "verdict": verdict["verdict"],
            "checklist_item_24h_soak_satisfied": verdict["checklist_item_24h_soak_satisfied"],
            "failures": verdict["failures"][:5],
            "live_enforcement_ready": verdict["live_enforcement_ready"],
            "safe_to_execute_now": verdict["safe_to_execute_now"],
            "serge_summary": verdict["serge_summary"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    if args.evaluate or args.write or args.print_markdown:
        verdict = evaluate_soak_report(
            pdir,
            report_path=args.report,
            jsonl_path=args.jsonl,
        )
        if args.write:
            written = write_soak_verdict_report(pdir, verdict)
            print(json.dumps({
                "verdict": verdict["verdict"],
                "checklist_item_24h_soak_satisfied": verdict["checklist_item_24h_soak_satisfied"],
                "live_enforcement_ready": verdict["live_enforcement_ready"],
                "safe_to_execute_now": verdict["safe_to_execute_now"],
                "written": written,
            }, indent=2))
            return 0
        if args.print_markdown:
            sys.stdout.write(render_soak_verdict_markdown(verdict))
            return 0
        # Default: print verdict summary as JSON.
        summary = {
            "verdict": verdict["verdict"],
            "observed_cycles": verdict["observed_cycles"],
            "required_cycles": verdict["required_cycles"],
            "stable_recommendation": verdict["stable_recommendation"],
            "checklist_item_24h_soak_satisfied": verdict["checklist_item_24h_soak_satisfied"],
            "live_enforcement_ready": verdict["live_enforcement_ready"],
            "safe_to_execute_now": verdict["safe_to_execute_now"],
            "safe_to_apply_real_project": verdict["safe_to_apply_real_project"],
            "guardian_enforcing_live": verdict["guardian_enforcing_live"],
            "serge_summary": verdict["serge_summary"],
            "recommended_next_action": verdict["recommended_next_action"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
