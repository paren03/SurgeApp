"""Phase 5VW: Luna Morning Decision Brief + Advisory Soak Harness.

Stdlib only. Reads existing advisory artifacts (router/enforcer/readiness/
routine/executor/scorecard/resource), aggregates Serge decision-card
recommendations, and produces a single plain-English morning brief that
Serge can read instead of inspecting source code. Includes a bounded
advisory-soak loop that proves the digest is stable before any future
live Guardian enforcement.

Hard rules in Phase 5VW:
  * advisory_only is ALWAYS True.
  * safe_to_execute_now is ALWAYS False.
  * safe_to_apply_real_project is ALWAYS False.
  * guardian_enforcing_live is ALWAYS False.
  * No execution. No service starts/stops. No source/target writes.

Tracked schema/policy:
  memory/luna_decision_brief.schema.json
  memory/luna_decision_brief_policy.json
  memory/luna_advisory_soak_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_morning_decision_brief.json
  memory/luna_morning_decision_brief.md
  memory/luna_decision_card_digest.json
  memory/luna_decision_card_digest.md
  memory/luna_advisory_soak_report.json
  memory/luna_advisory_soak_report.md
  memory/luna_advisory_soak.jsonl
  memory/luna_next_safe_action.json

CLI:
  python -m luna_modules.luna_decision_brief --self-test
  python -m luna_modules.luna_decision_brief --build
  python -m luna_modules.luna_decision_brief --write
  python -m luna_modules.luna_decision_brief --print-markdown
  python -m luna_modules.luna_decision_brief --soak --cycles 3 --sleep-seconds 1
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Recognized normalized recommendations.
_REC_APPROVE = "APPROVE_RECOMMENDED"
_REC_WAIT = "WAIT_FOR_MORE_EVIDENCE"
_REC_DENY = "DO_NOT_APPROVE"
_REC_SERGE = "SERGE_ONLY"
_REC_UNKNOWN = "UNKNOWN"

_VALID_RECS = (_REC_APPROVE, _REC_WAIT, _REC_DENY, _REC_SERGE, _REC_UNKNOWN)

_REC_TO_BUCKET = {
    _REC_APPROVE: "approve_recommended",
    _REC_WAIT: "wait_for_more_evidence",
    _REC_DENY: "do_not_approve",
    _REC_SERGE: "serge_only",
    _REC_UNKNOWN: "unknown",
}

_DEFAULT_SOURCES = (
    "memory/luna_approval_router_report.json",
    "memory/luna_council_enforcer_report.json",
    "memory/luna_guardian_readiness_report.json",
    "memory/luna_guardian_enforcement_readiness.json",
    "memory/luna_limited_autonomy_report.json",
    "memory/luna_routine_approval_report.json",
    "memory/luna_decision_card_preview.json",
    "memory/luna_deterministic_executor_report.json",
    "memory/luna_capability_scorecard.json",
    "memory/luna_resource_status.json",
)

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "phase": "5VW",
    "advisory_only": True,
    "safe_to_execute_now": False,
    "safe_to_apply_real_project": False,
    "guardian_enforcing_live": False,
    "source_artifacts": list(_DEFAULT_SOURCES),
    "max_top_items": 10,
    "never_enable_execution": True,
}

_DEFAULT_SOAK_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "phase": "5VW",
    "advisory_only": True,
    "safe_to_execute_now": False,
    "safe_to_apply_real_project": False,
    "guardian_enforcing_live": False,
    "default_cycles": 3,
    "max_cycles": 20,
    "default_sleep_seconds": 1,
    "max_sleep_seconds": 60,
    "never_enable_execution": True,
}


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_brief_id(prefix: str = "brief") -> str:
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


def append_jsonl(path: Path | str, row: dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return p


# ---------- policy ----------


def load_decision_brief_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_decision_brief_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(_DEFAULT_POLICY)
    for k, v in raw.items():
        out[k] = v
    out["advisory_only"] = True
    out["safe_to_execute_now"] = False
    out["safe_to_apply_real_project"] = False
    out["guardian_enforcing_live"] = False
    out["never_enable_execution"] = True
    out["_source"] = str(p)
    return out


def _load_soak_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_advisory_soak_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_SOAK_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(_DEFAULT_SOAK_POLICY)
    for k, v in raw.items():
        out[k] = v
    out["advisory_only"] = True
    out["safe_to_execute_now"] = False
    out["safe_to_apply_real_project"] = False
    out["guardian_enforcing_live"] = False
    out["never_enable_execution"] = True
    return out


# ---------- artifact reading ----------


def read_optional_artifact(
    project_dir: Path | str,
    relative_path: str,
) -> dict[str, Any]:
    """Read an artifact relative to project_dir. Always returns a dict."""
    pdir = Path(project_dir)
    p = pdir / relative_path
    data = load_json(p, default=None)
    return {
        "source": relative_path,
        "path": str(p),
        "found": isinstance(data, dict),
        "data": data if isinstance(data, dict) else {},
    }


# ---------- decision-card extraction ----------


def normalize_recommendation(value: Any) -> str:
    """Map a raw recommendation string to one of the known recommendation buckets."""
    if value is None:
        return _REC_UNKNOWN
    text = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    if text in _VALID_RECS:
        return text
    # Tolerate variants.
    if text in ("APPROVE", "APPROVED", "OK", "GREEN"):
        return _REC_APPROVE
    if text in ("WAIT", "PENDING", "EVIDENCE_NEEDED", "MORE_EVIDENCE"):
        return _REC_WAIT
    if text in ("DENY", "DENIED", "BLOCK", "BLOCKED", "REJECT", "REJECTED"):
        return _REC_DENY
    if text in ("SERGE", "HUMAN_ONLY", "OPERATOR_ONLY", "MANUAL"):
        return _REC_SERGE
    if text in ("UNAVAILABLE", "MISSING", ""):
        return _REC_UNKNOWN
    return _REC_UNKNOWN


def _make_card_record(
    recommendation: str,
    *,
    source: str,
    goal: str = "",
    action_type: str = "",
    risk_tier: int = 0,
    plain_english: str = "",
    card_id: str = "",
) -> dict[str, Any]:
    return {
        "recommendation": normalize_recommendation(recommendation),
        "source": str(source),
        "goal": str(goal),
        "action_type": str(action_type),
        "risk_tier": int(risk_tier or 0),
        "plain_english": str(plain_english),
        "card_id": str(card_id),
    }


def extract_decision_cards(obj: Any, source: str = "") -> list[dict[str, Any]]:
    """Extract decision cards from a router/enforcer/readiness/routing report.

    Walks the top-level dict for known card shapes and returns a list of
    normalized records. Defensive: handles missing keys, lists, and nested
    structures (e.g. enforcer build_guardian_approval_status with action lists).
    """
    out: list[dict[str, Any]] = []
    if not isinstance(obj, dict):
        return out

    # 1. Direct decision_card on the report itself (router-shape).
    direct_card = obj.get("decision_card")
    if isinstance(direct_card, dict):
        rec = direct_card.get("recommendation") or obj.get("decision_card_recommendation")
        if rec:
            out.append(_make_card_record(
                rec,
                source=source,
                goal=str(direct_card.get("goal") or obj.get("goal") or ""),
                action_type=str(direct_card.get("action_type") or obj.get("action_type") or ""),
                risk_tier=int(direct_card.get("risk_tier") or obj.get("approval_tier_required") or 0),
                plain_english=str(
                    direct_card.get("plain_english_final_recommendation")
                    or obj.get("serge_plain_english_summary")
                    or obj.get("plain_english_decision")
                    or ""
                ),
                card_id=str(direct_card.get("card_id") or ""),
            ))
    elif obj.get("decision_card_recommendation"):
        # Card was unavailable but a recommendation string is present.
        out.append(_make_card_record(
            obj.get("decision_card_recommendation"),
            source=source,
            goal=str(obj.get("goal") or ""),
            action_type=str(obj.get("action_type") or ""),
            risk_tier=int(obj.get("approval_tier_required") or obj.get("risk_tier") or 0),
            plain_english=str(
                obj.get("serge_plain_english_summary")
                or obj.get("plain_english_decision")
                or ""
            ),
        ))

    # 2. Nested action lists (enforcer / readiness / routing).
    for list_key in ("actions", "cycle_results", "routing_results"):
        items = obj.get(list_key)
        if isinstance(items, list):
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                child_source = f"{source}#{list_key}[{i}]" if source else f"{list_key}[{i}]"
                out.extend(extract_decision_cards(item, source=child_source))

    return out


# ---------- aggregation ----------


def aggregate_decision_cards(cards: list[dict[str, Any]]) -> dict[str, int]:
    """Aggregate decision-card recommendations into bucket counts."""
    counts = {
        "approve_recommended": 0,
        "wait_for_more_evidence": 0,
        "do_not_approve": 0,
        "serge_only": 0,
        "unknown": 0,
    }
    for c in cards or []:
        rec = normalize_recommendation((c or {}).get("recommendation"))
        bucket = _REC_TO_BUCKET.get(rec, "unknown")
        counts[bucket] += 1
    return counts


# ---------- digest ----------


def build_decision_digest(
    project_dir: Path | str,
    source_paths: list[str] | None = None,
) -> dict[str, Any]:
    """Read all advisory artifacts and build a decision-card digest."""
    pdir = Path(project_dir)
    policy = load_decision_brief_policy(pdir)
    sources = list(source_paths or policy.get("source_artifacts") or _DEFAULT_SOURCES)

    files_checked: list[str] = []
    missing: list[str] = []
    all_cards: list[dict[str, Any]] = []

    for rel in sources:
        artifact = read_optional_artifact(pdir, rel)
        if artifact["found"]:
            files_checked.append(rel)
            cards = extract_decision_cards(artifact["data"], source=rel)
            all_cards.extend(cards)
        else:
            missing.append(rel)

    counts = aggregate_decision_cards(all_cards)

    # Top items: prioritize SERGE_ONLY > DO_NOT_APPROVE > WAIT > APPROVE.
    priority_order = {
        _REC_SERGE: 0,
        _REC_DENY: 1,
        _REC_WAIT: 2,
        _REC_APPROVE: 3,
        _REC_UNKNOWN: 4,
    }
    sorted_cards = sorted(
        all_cards,
        key=lambda c: (priority_order.get(c["recommendation"], 9), -int(c.get("risk_tier") or 0)),
    )
    max_top = int(policy.get("max_top_items", 10))
    top_items = sorted_cards[:max_top]

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "counts": counts,
        "top_items": top_items,
        "files_checked": files_checked,
        "missing_artifacts": missing,
        "total_cards": len(all_cards),
    }


def validate_decision_digest(digest: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(digest, dict):
        return False, ["digest is not a dict"]
    for f in ("schema_version", "generated_at", "advisory_only",
              "safe_to_execute_now", "counts", "top_items", "files_checked"):
        if f not in digest:
            errs.append(f"missing_required_field:{f!r}")
    if digest.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{digest.get('schema_version')!r}")
    if digest.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if digest.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False")
    counts = digest.get("counts") or {}
    for bucket in ("approve_recommended", "wait_for_more_evidence",
                   "do_not_approve", "serge_only", "unknown"):
        if bucket not in counts:
            errs.append(f"counts.{bucket}_missing")
    return len(errs) == 0, errs


# ---------- next safe action ----------


def classify_next_safe_action(
    digest: dict[str, Any],
    project_dir: Path | str | None = None,
) -> tuple[str, str]:
    """Classify the next safe action from a digest.

    Returns (overall_recommendation, plain_english_next_action).
    """
    if not isinstance(digest, dict):
        return "no_actions", "No digest available — defer to Serge."
    counts = digest.get("counts") or {}
    serge = int(counts.get("serge_only") or 0)
    deny = int(counts.get("do_not_approve") or 0)
    wait = int(counts.get("wait_for_more_evidence") or 0)
    approve = int(counts.get("approve_recommended") or 0)
    total = serge + deny + wait + approve + int(counts.get("unknown") or 0)

    if total == 0:
        return (
            "no_actions",
            "No pending decision cards across the advisory chain. "
            "Continue normal routine work; no action required.",
        )

    if serge > 0:
        return (
            "serge_only",
            f"{serge} action(s) require Serge personally. "
            "Do not approve via delegated hierarchy. Surface in morning brief.",
        )
    if deny > 0:
        return (
            "do_not_approve",
            f"{deny} action(s) failed safety signals. "
            "Fix the failing checks (verifier, sandbox, secrets, rollback) before re-evaluating.",
        )
    if wait > 0:
        return (
            "wait_for_evidence",
            f"{wait} action(s) need more evidence (sandbox/verifier/receipt). "
            "Gather missing signals and re-run the digest.",
        )
    if approve > 0:
        return (
            "continue_safe_routine",
            f"{approve} action(s) are safe to approve via standing policy. "
            "safe_to_execute_now is still False until live wiring is enabled.",
        )
    return (
        "no_actions",
        "Only unknown decision cards present — defer to Serge.",
    )


# ---------- morning brief ----------


def build_morning_decision_brief(
    project_dir: Path | str,
    digest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Serge-readable morning decision brief.

    Hard rules: advisory_only=True, safe_to_execute_now=False,
    safe_to_apply_real_project=False, guardian_enforcing_live=False.
    """
    pdir = Path(project_dir).resolve()
    if digest is None:
        digest = build_decision_digest(pdir)

    overall, next_action = classify_next_safe_action(digest, pdir)
    counts = digest.get("counts") or {
        "approve_recommended": 0, "wait_for_more_evidence": 0,
        "do_not_approve": 0, "serge_only": 0, "unknown": 0,
    }

    serge_summary = _build_serge_summary(overall, counts, digest.get("total_cards") or 0)

    what_not = [
        "Do not approve non-delegable or destructive actions via the hierarchy.",
        "Do not enable safe_to_execute_now anywhere.",
        "Do not edit Luna_Post_Repair_Verify.ps1 or weaken safety gates.",
        "Do not let Aider blindly edit Luna source.",
        "Do not bypass sandbox/verifier/rollback gates.",
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "brief_id": make_brief_id(),
        "generated_at": now_iso(),
        "project_dir": str(pdir),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "overall_recommendation": overall,
        "counts": counts,
        "top_items": list(digest.get("top_items") or []),
        "serge_summary": serge_summary,
        "next_safe_action": next_action,
        "what_luna_should_not_do": what_not,
        "files_checked": list(digest.get("files_checked") or []),
        "missing_artifacts": list(digest.get("missing_artifacts") or []),
        "notes": [
            "Phase 5VW advisory only. No execution wired.",
            "safe_to_execute_now is False; safe_to_apply_real_project is False.",
            "Guardian enforcement remains advisory only.",
        ],
    }


def _build_serge_summary(overall: str, counts: dict[str, int], total: int) -> str:
    if overall == "no_actions":
        return (
            f"Good morning. No pending decision cards across the advisory chain "
            f"({total} card(s) found). Routine work can continue safely."
        )
    if overall == "continue_safe_routine":
        return (
            f"Good morning. {counts.get('approve_recommended', 0)} action(s) are "
            f"safe to approve via your standing policy. "
            f"{counts.get('wait_for_more_evidence', 0)} need evidence. No SERGE_ONLY "
            f"or DO_NOT_APPROVE items today. safe_to_execute_now is still False — "
            "live wiring requires a future explicit phase."
        )
    if overall == "wait_for_evidence":
        return (
            f"Good morning. {counts.get('wait_for_more_evidence', 0)} action(s) are "
            "waiting for evidence (sandbox/verifier/receipt). The hierarchy is "
            "holding back as designed. No SERGE_ONLY items."
        )
    if overall == "do_not_approve":
        return (
            f"Good morning. {counts.get('do_not_approve', 0)} action(s) failed "
            "safety signals. Do not approve until the failures are fixed and "
            "re-verified. No live execution occurred."
        )
    if overall == "serge_only":
        return (
            f"Good morning. {counts.get('serge_only', 0)} action(s) need your "
            "personal decision (non-delegable or destructive). The hierarchy is "
            "holding them for you. Nothing has been executed."
        )
    return (
        "Good morning. The decision-card stream is unclear. Defer to Serge "
        "review of the morning brief."
    )


def validate_morning_brief(brief: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(brief, dict):
        return False, ["brief is not a dict"]
    required = (
        "schema_version", "brief_id", "generated_at", "project_dir",
        "advisory_only", "safe_to_execute_now", "safe_to_apply_real_project",
        "guardian_enforcing_live", "overall_recommendation", "counts",
        "top_items", "serge_summary", "next_safe_action",
    )
    for f in required:
        if f not in brief:
            errs.append(f"missing_required_field:{f!r}")
    if brief.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{brief.get('schema_version')!r}")
    if brief.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if brief.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False")
    if brief.get("safe_to_apply_real_project") is not False:
        errs.append("safe_to_apply_real_project must be False")
    if brief.get("guardian_enforcing_live") is not False:
        errs.append("guardian_enforcing_live must be False")
    valid_recs = (
        "continue_safe_routine", "wait_for_evidence",
        "do_not_approve", "serge_only", "no_actions",
    )
    if brief.get("overall_recommendation") not in valid_recs:
        errs.append(f"invalid_overall_recommendation:{brief.get('overall_recommendation')!r}")
    return len(errs) == 0, errs


# ---------- markdown rendering ----------


def render_morning_brief_markdown(brief: dict[str, Any]) -> str:
    counts = brief.get("counts") or {}
    lines = [
        "# Luna Morning Decision Brief",
        "",
        f"**brief_id**: {brief.get('brief_id', '')}",
        f"**generated_at**: {brief.get('generated_at', '')}",
        f"**advisory_only**: {brief.get('advisory_only', True)}",
        f"**safe_to_execute_now**: {brief.get('safe_to_execute_now', False)}",
        f"**safe_to_apply_real_project**: {brief.get('safe_to_apply_real_project', False)}",
        f"**guardian_enforcing_live**: {brief.get('guardian_enforcing_live', False)}",
        "",
        f"## Overall Recommendation: {brief.get('overall_recommendation', '')}",
        "",
        "## Plain-English Summary",
        brief.get("serge_summary", ""),
        "",
        "## Decision-Card Counts",
        f"- approve_recommended: {counts.get('approve_recommended', 0)}",
        f"- wait_for_more_evidence: {counts.get('wait_for_more_evidence', 0)}",
        f"- do_not_approve: {counts.get('do_not_approve', 0)}",
        f"- serge_only: {counts.get('serge_only', 0)}",
        f"- unknown: {counts.get('unknown', 0)}",
        "",
        "## Next Safe Action",
        brief.get("next_safe_action", ""),
    ]
    items = brief.get("top_items") or []
    if items:
        lines += ["", "## Top Items"]
        for i, item in enumerate(items, 1):
            rec = item.get("recommendation", "")
            src = item.get("source", "")
            tier = item.get("risk_tier", 0)
            goal = (item.get("goal") or "")[:80]
            lines.append(f"{i}. [{rec}] tier={tier} source=`{src}` goal=`{goal}`")
            pe = item.get("plain_english") or ""
            if pe:
                lines.append(f"   - {pe[:200]}")

    notdo = brief.get("what_luna_should_not_do") or []
    if notdo:
        lines += ["", "## What Luna Should NOT Do"]
        for x in notdo:
            lines.append(f"- {x}")

    fc = brief.get("files_checked") or []
    if fc:
        lines += ["", "## Files Checked"]
        for f in fc:
            lines.append(f"- {f}")

    miss = brief.get("missing_artifacts") or []
    if miss:
        lines += ["", "## Missing Artifacts (advisory)"]
        for f in miss:
            lines.append(f"- {f}")

    notes = brief.get("notes") or []
    if notes:
        lines += ["", "## Notes"]
        for n in notes:
            lines.append(f"- {n}")
    lines.append("")
    return "\n".join(lines)


# ---------- write artifacts ----------


def write_morning_brief(
    project_dir: Path | str,
    brief: dict[str, Any],
) -> dict[str, str]:
    """Write the morning brief + digest + next-safe-action artifacts under memory/."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_morning_decision_brief.json"
    md_p = mem / "luna_morning_decision_brief.md"
    digest_json = mem / "luna_decision_card_digest.json"
    digest_md = mem / "luna_decision_card_digest.md"
    next_action_p = mem / "luna_next_safe_action.json"

    write_json_atomic(json_p, brief)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_morning_brief_markdown(brief), encoding="utf-8")
    os.replace(tmp, md_p)

    # Digest is the brief minus serge_summary/next_safe_action/notes.
    digest_view = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": brief.get("generated_at"),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "counts": brief.get("counts", {}),
        "top_items": brief.get("top_items", []),
        "files_checked": brief.get("files_checked", []),
        "missing_artifacts": brief.get("missing_artifacts", []),
    }
    write_json_atomic(digest_json, digest_view)
    tmp_dm = digest_md.with_suffix(digest_md.suffix + ".tmp")
    tmp_dm.write_text(render_morning_brief_markdown(brief), encoding="utf-8")
    os.replace(tmp_dm, digest_md)

    write_json_atomic(next_action_p, {
        "schema_version": SCHEMA_VERSION,
        "generated_at": brief.get("generated_at"),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "overall_recommendation": brief.get("overall_recommendation"),
        "next_safe_action": brief.get("next_safe_action"),
    })

    return {
        "json": str(json_p),
        "md": str(md_p),
        "digest_json": str(digest_json),
        "digest_md": str(digest_md),
        "next_action": str(next_action_p),
    }


# ---------- advisory soak ----------


def build_soak_cycle(project_dir: Path | str, cycle_index: int) -> dict[str, Any]:
    """Build one soak cycle result from a fresh digest. No execution."""
    pdir = Path(project_dir)
    digest = build_decision_digest(pdir)
    brief = build_morning_decision_brief(pdir, digest=digest)
    return {
        "cycle": int(cycle_index),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "overall_recommendation": brief.get("overall_recommendation"),
        "counts": brief.get("counts", {}),
        "files_checked": len(brief.get("files_checked") or []),
        "missing_artifacts": len(brief.get("missing_artifacts") or []),
    }


def run_advisory_soak(
    project_dir: Path | str,
    cycles: int = 3,
    sleep_seconds: float = 1.0,
    write: bool = False,
) -> dict[str, Any]:
    """Run a bounded advisory soak. Never executes anything."""
    pdir = Path(project_dir)
    pol = _load_soak_policy(pdir)

    cycles = max(1, min(int(pol.get("max_cycles", 20)), int(cycles)))
    sleep_seconds = max(0.0, min(float(pol.get("max_sleep_seconds", 60)), float(sleep_seconds)))

    started = now_iso()
    started_t = time.monotonic()
    soak_id = make_brief_id("soak")
    cycle_results: list[dict[str, Any]] = []
    failures: list[str] = []
    warnings: list[str] = []

    for i in range(cycles):
        try:
            res = build_soak_cycle(pdir, i + 1)
            cycle_results.append(res)
        except Exception as e:
            failures.append(f"cycle_{i+1}:{type(e).__name__}:{str(e)[:200]}")
        if i < cycles - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    finished = now_iso()
    duration = time.monotonic() - started_t

    # Derive a stable recommendation: most common bucket across cycles.
    recs = [str(c.get("overall_recommendation") or "no_actions") for c in cycle_results]
    if recs:
        rec_set = set(recs)
        if len(rec_set) == 1:
            rec_next = f"stable_recommendation:{recs[0]}"
        else:
            rec_next = f"varying_recommendations:{sorted(rec_set)!r}"
            warnings.append("recommendation_varied_across_cycles")
    else:
        rec_next = "no_cycles_completed"

    report = {
        "schema_version": SCHEMA_VERSION,
        "soak_id": soak_id,
        "started_at": started,
        "finished_at": finished,
        "duration_seconds": round(duration, 3),
        "cycles": len(cycle_results),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "guardian_enforcing_live": False,
        "cycle_results": cycle_results,
        "failures": failures,
        "warnings": warnings,
        "recommended_next_action": rec_next,
    }

    if write:
        write_soak_report(pdir, report)
    return report


def validate_soak_report(report: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(report, dict):
        return False, ["report is not a dict"]
    required = (
        "schema_version", "soak_id", "started_at", "finished_at", "cycles",
        "advisory_only", "safe_to_execute_now", "safe_to_apply_real_project",
        "guardian_enforcing_live", "cycle_results",
    )
    for f in required:
        if f not in report:
            errs.append(f"missing_required_field:{f!r}")
    if report.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{report.get('schema_version')!r}")
    if report.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if report.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False")
    if report.get("safe_to_apply_real_project") is not False:
        errs.append("safe_to_apply_real_project must be False")
    if report.get("guardian_enforcing_live") is not False:
        errs.append("guardian_enforcing_live must be False")
    return len(errs) == 0, errs


def render_soak_report_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Luna Advisory Soak Report",
        "",
        f"**soak_id**: {report.get('soak_id', '')}",
        f"**started_at**: {report.get('started_at', '')}",
        f"**finished_at**: {report.get('finished_at', '')}",
        f"**cycles**: {report.get('cycles', 0)}",
        f"**advisory_only**: {report.get('advisory_only', True)}",
        f"**safe_to_execute_now**: {report.get('safe_to_execute_now', False)}",
        f"**safe_to_apply_real_project**: {report.get('safe_to_apply_real_project', False)}",
        f"**guardian_enforcing_live**: {report.get('guardian_enforcing_live', False)}",
        "",
        "## Cycle Results",
    ]
    for c in report.get("cycle_results") or []:
        rec = c.get("overall_recommendation", "")
        counts = c.get("counts") or {}
        total = sum(int(counts.get(k, 0)) for k in counts)
        lines.append(
            f"- cycle {c.get('cycle')}: rec={rec}, total_cards={total}, "
            f"files_checked={c.get('files_checked', 0)}"
        )

    fails = report.get("failures") or []
    if fails:
        lines += ["", "## Failures"]
        for f in fails:
            lines.append(f"- {f}")

    warns = report.get("warnings") or []
    if warns:
        lines += ["", "## Warnings"]
        for w in warns:
            lines.append(f"- {w}")

    lines += [
        "",
        f"**recommended_next_action**: {report.get('recommended_next_action', '')}",
        "",
    ]
    return "\n".join(lines)


def write_soak_report(
    project_dir: Path | str,
    report: dict[str, Any],
) -> dict[str, str]:
    """Write soak report artifacts under memory/."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    json_p = mem / "luna_advisory_soak_report.json"
    md_p = mem / "luna_advisory_soak_report.md"
    soak_jsonl = mem / "luna_advisory_soak.jsonl"

    write_json_atomic(json_p, report)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_soak_report_markdown(report), encoding="utf-8")
    os.replace(tmp, md_p)

    append_jsonl(soak_jsonl, {
        "ts": now_iso(),
        "soak_id": report.get("soak_id"),
        "cycles": report.get("cycles", 0),
        "advisory_only": True,
        "safe_to_execute_now": False,
        "guardian_enforcing_live": False,
        "recommended_next_action": report.get("recommended_next_action"),
    })

    return {"json": str(json_p), "md": str(md_p), "jsonl": str(soak_jsonl)}


# ---------- self-test ----------


def self_test() -> int:
    """Run end-to-end self-test in a TemporaryDirectory. Returns 0 on success."""
    import tempfile

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)

        # Seed a synthetic router report with a green decision card.
        green_router = {
            "schema_version": 1,
            "request_id": "req_x",
            "goal": "Refresh Luna scorecard memory (test)",
            "action_type": "generated_artifact",
            "approval_tier_required": 1,
            "routing_decision": "not_required",
            "safe_to_execute_now": False,
            "decision_card": {
                "schema_version": 1,
                "card_id": "card_test_green",
                "recommendation": "APPROVE_RECOMMENDED",
                "risk_level": "low",
                "goal_alignment": "aligned",
                "safe_to_execute_now": False,
                "plain_english_final_recommendation":
                    "Safe to approve direction; advisory only.",
            },
            "decision_card_recommendation": "APPROVE_RECOMMENDED",
            "serge_plain_english_summary": "Safe to approve direction; advisory only.",
        }
        write_json_atomic(
            td / "memory" / "luna_approval_router_report.json",
            green_router,
        )

        # Seed a synthetic guardian readiness report with mixed cards.
        readiness = {
            "schema_version": 1,
            "advisory_only": True,
            "guardian_enforcing_live": False,
            "ready_for_live_guardian_enforcement": False,
            "actions": [
                {
                    "action_id": "a1", "action_type": "process_kill",
                    "risk_tier": 5, "safe_to_execute_now": False,
                    "decision_card_recommendation": "SERGE_ONLY",
                    "decision_card": {
                        "card_id": "c_serge",
                        "recommendation": "SERGE_ONLY",
                        "plain_english_final_recommendation": "Non-delegable.",
                        "safe_to_execute_now": False,
                    },
                },
                {
                    "action_id": "a2", "action_type": "medium_code_edit",
                    "risk_tier": 3, "safe_to_execute_now": False,
                    "decision_card_recommendation": "WAIT_FOR_MORE_EVIDENCE",
                },
            ],
            "decision_card_summary": {
                "approve_recommended": 0,
                "wait_for_more_evidence": 1,
                "do_not_approve": 0,
                "serge_only": 1,
                "unavailable": 0,
            },
        }
        write_json_atomic(
            td / "memory" / "luna_guardian_readiness_report.json",
            readiness,
        )

        # Build digest + brief.
        digest = build_decision_digest(td)
        ok, errs = validate_decision_digest(digest)
        assert ok, f"digest invalid: {errs}"
        assert digest["counts"]["serge_only"] >= 1, "expected at least 1 SERGE_ONLY"
        assert digest["counts"]["approve_recommended"] >= 1, "expected at least 1 APPROVE"
        assert digest["safe_to_execute_now"] is False

        brief = build_morning_decision_brief(td, digest=digest)
        ok, errs = validate_morning_brief(brief)
        assert ok, f"brief invalid: {errs}"
        assert brief["safe_to_execute_now"] is False
        assert brief["safe_to_apply_real_project"] is False
        assert brief["guardian_enforcing_live"] is False
        # SERGE_ONLY beats APPROVE in the priority order.
        assert brief["overall_recommendation"] == "serge_only", \
            f"expected serge_only, got {brief['overall_recommendation']!r}"

        # Write the brief and verify paths stay under temp.
        written = write_morning_brief(td, brief)
        for key, p in written.items():
            assert str(Path(p).resolve()).startswith(str(td.resolve())), \
                f"{p} escapes temp dir"
            assert "memory" in str(p)

        # Render markdown.
        md = render_morning_brief_markdown(brief)
        assert "Overall Recommendation" in md
        assert "False" in md  # safe_to_execute_now: False

        # Run a mini soak.
        soak = run_advisory_soak(td, cycles=2, sleep_seconds=0)
        ok, errs = validate_soak_report(soak)
        assert ok, f"soak invalid: {errs}"
        assert soak["cycles"] == 2
        assert soak["safe_to_execute_now"] is False
        assert soak["guardian_enforcing_live"] is False

        print(json.dumps({
            "self_test": "PASS",
            "overall_recommendation": brief["overall_recommendation"],
            "counts": brief["counts"],
            "soak_cycles": soak["cycles"],
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
        }, indent=2))
    return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Decision Brief + Advisory Soak (Phase 5VW)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--soak", action="store_true")
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--write-soak", action="store_true",
                        help="Also write the soak report under memory/.")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.build:
        brief = build_morning_decision_brief(pdir)
        summary = {
            "advisory_only": brief["advisory_only"],
            "safe_to_execute_now": brief["safe_to_execute_now"],
            "safe_to_apply_real_project": brief["safe_to_apply_real_project"],
            "guardian_enforcing_live": brief["guardian_enforcing_live"],
            "overall_recommendation": brief["overall_recommendation"],
            "counts": brief["counts"],
            "files_checked": brief["files_checked"],
            "next_safe_action": brief["next_safe_action"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    if args.write:
        brief = build_morning_decision_brief(pdir)
        written = write_morning_brief(pdir, brief)
        print(json.dumps({
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
            "overall_recommendation": brief["overall_recommendation"],
            "counts": brief["counts"],
            "written": written,
        }, indent=2))
        return 0

    if args.print_markdown:
        # Prefer the latest written brief; otherwise build fresh.
        latest = load_json(
            pdir / "memory" / "luna_morning_decision_brief.json",
            default=None,
        )
        if not isinstance(latest, dict):
            latest = build_morning_decision_brief(pdir)
        sys.stdout.write(render_morning_brief_markdown(latest))
        return 0

    if args.soak:
        report = run_advisory_soak(
            pdir,
            cycles=args.cycles,
            sleep_seconds=args.sleep_seconds,
            write=bool(args.write_soak),
        )
        summary = {
            "soak_id": report["soak_id"],
            "cycles": report["cycles"],
            "advisory_only": report["advisory_only"],
            "safe_to_execute_now": report["safe_to_execute_now"],
            "safe_to_apply_real_project": report["safe_to_apply_real_project"],
            "guardian_enforcing_live": report["guardian_enforcing_live"],
            "recommended_next_action": report["recommended_next_action"],
            "failures": report["failures"],
            "warnings": report["warnings"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
