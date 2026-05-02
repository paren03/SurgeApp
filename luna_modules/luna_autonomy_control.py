"""Autonomy Control v1 quality gate.

Phase 1 goal: stop no-diff / repeated-failure floods without deleting queues,
memory, logs, backups, or staged edits.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


NO_DIFF_MARKERS = (
    "diff empty",
    "no changes",
    "no diff",
    "nothing to apply",
    "already up to date",
    "already implemented",
)

FAILURE_MARKERS = (
    "failed",
    "traceback",
    "exception",
    "timeout",
    "verification failed",
    "rollback",
)

DEFAULT_CYCLE_BUDGETS = {
    "max_failed_per_cycle": 5,
    "max_noop_per_cycle": 5,
    "max_jobs_per_cycle": 12,
    "max_same_file_jobs_per_cycle": 3,
    "max_same_prompt_family_per_cycle": 2,
}

DONE_REASONS = {
    "real_diff",
    "analysis_only",
    "already_compliant_with_evidence",
}


@dataclass(frozen=True)
class AutonomyPaths:
    project_dir: Path

    @property
    def memory_dir(self) -> Path:
        return self.project_dir / "memory"

    @property
    def logs_dir(self) -> Path:
        return self.project_dir / "logs"

    @property
    def kill_switch(self) -> Path:
        return self.project_dir / "LUNA_STOP_NOW.flag"

    @property
    def quarantine_dir(self) -> Path:
        return self.project_dir / "quarantine" / "autonomy_control"

    @property
    def report_path(self) -> Path:
        return self.memory_dir / "luna_autonomy_quality_gate.json"

    @property
    def director_backlog_path(self) -> Path:
        return self.memory_dir / "director_autonomy_control_backlog.md"

    @property
    def continues_update_stop(self) -> Path:
        return self.memory_dir / "continues_update.stop"

    @property
    def nightly_md(self) -> Path:
        return self.memory_dir / "nightly_updates.md"

    @property
    def nightly_jsonl(self) -> Path:
        return self.memory_dir / "nightly_updates.jsonl"

    @property
    def live_feed(self) -> Path:
        return self.logs_dir / "luna_live_feed.jsonl"


from datetime import datetime

def get_current_time_iso() -> str:
    """Return current time in ISO format with seconds precision."""
    return datetime.now().isoformat(timespec="seconds")


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _safe_read_json(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(_safe_read_text(path) or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _contains_any(text: str, markers: Iterable[str]) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in markers)


def classify_autonomy_artifact(path: Path) -> Dict[str, Any]:
    """Classify one queue/result artifact without mutating it."""
    text = _safe_read_text(path)
    payload = _safe_read_json(path) if path.suffix.lower() == ".json" else {}
    blob = "\n".join([text, json.dumps(payload, ensure_ascii=True)])
    no_diff = is_no_diff(blob)
    failed = is_failure(blob)

    status = str(payload.get("status") or payload.get("final_status") or "").lower()
    if "failed" in status or "timeout" in status:
        failed = True
    if status in {"no_diff", "empty_diff", "no_changes"}:
        no_diff = True

    recommended_action, reason = determine_action(no_diff, failed)

    return {
        "path": str(path),
        "name": path.name,
        "kind": "json" if path.suffix.lower() == ".json" else "text",
        "status": status,
        "no_diff": no_diff,
        "failed": failed,
        "recommended_action": recommended_action,
        "reason": reason,
    }

def is_no_diff(blob: str) -> bool:
    return _contains_any(blob, NO_DIFF_MARKERS)

def is_failure(blob: str) -> bool:
    return _contains_any(blob, FAILURE_MARKERS)

from typing import Tuple

def determine_action(no_diff: bool, failed: bool) -> Tuple[str, str]:
    recommended_action = "allow"
    reason = "artifact does not match no-diff or failure markers"
    if no_diff:
        recommended_action = "quarantine"
        reason = "no_diff_detected"
    elif failed:
        recommended_action = "quarantine"
        reason = "failure_detected"

    return recommended_action, reason


def evaluate_done_policy(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return strict DONE/NOOP/FAILED policy for one completed job row."""
    diff_exists = bool(job.get("diff_exists") or job.get("diff_path"))
    analysis_only = bool(job.get("analysis_only") is True)
    evidence = str(job.get("compliance_evidence") or job.get("evidence") or "").strip()
    already_compliant = bool(job.get("already_compliant") is True and evidence)
    failed = str(job.get("status") or "").lower() in {"failed", "error", "timeout"} or bool(job.get("failure_reason"))

    if failed:
        return {
            "status": "failed",
            "done_allowed": False,
            "reason": str(job.get("failure_reason") or "failure_detected"),
            "counts_as_successful_upgrade": False,
        }
    if diff_exists:
        return {
            "status": "done",
            "done_allowed": True,
            "reason": "real_diff",
            "counts_as_successful_upgrade": True,
        }
    if analysis_only:
        return {
            "status": "done",
            "done_allowed": True,
            "reason": "analysis_only",
            "counts_as_successful_upgrade": False,
        }
    if already_compliant:
        return {
            "status": "done",
            "done_allowed": True,
            "reason": "already_compliant_with_evidence",
            "counts_as_successful_upgrade": False,
        }
    return {
        "status": "noop",
        "done_allowed": False,
        "reason": "no_diff",
        "noop_reason": "no_diff",
        "counts_as_successful_upgrade": False,
    }


def prompt_family(prompt: str) -> str:
    """Small deterministic family key to catch repeated prompt loops."""
    lowered = " ".join(str(prompt or "").lower().replace("-", " ").replace("_", " ").split())
    if not lowered:
        return "empty"
    low_value_markers = (
        ("docstring", "one_line_docstring"),
        ("type hint", "type_hints"),
        ("format", "formatting"),
        ("whitespace", "formatting"),
        ("comment", "comment_cleanup"),
    )
    for marker, family in low_value_markers:
        if marker in lowered:
            return family
    return " ".join(lowered.split()[:5])


def evaluate_cycle_budget(jobs: List[Dict[str, Any]], budgets: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Evaluate per-cycle limits without mutating queues."""
    limits = dict(DEFAULT_CYCLE_BUDGETS)
    if budgets:
        limits.update({key: int(value) for key, value in budgets.items() if key in limits})

    failed_count = 0
    noop_count = 0
    target_files: List[str] = []
    families: List[str] = []
    job_results: List[Dict[str, Any]] = []
    for job in jobs:
        result = evaluate_done_policy(job)
        job_results.append({**job, "quality_gate": result})
        if result["status"] == "failed":
            failed_count += 1
        if result["status"] == "noop":
            noop_count += 1
        target = str(job.get("target_file") or "").strip()
        if target:
            target_files.append(target.lower())
        families.append(prompt_family(str(job.get("prompt") or job.get("instructions") or "")))

    file_counts = Counter(target_files)
    family_counts = Counter(families)
    violations: List[Dict[str, Any]] = []
    checks = [
        ("max_jobs_per_cycle", len(jobs)),
        ("max_failed_per_cycle", failed_count),
        ("max_noop_per_cycle", noop_count),
    ]
    for name, observed in checks:
        if observed > limits[name]:
            violations.append({"budget": name, "observed": observed, "limit": limits[name]})
    for target, observed in file_counts.items():
        if observed > limits["max_same_file_jobs_per_cycle"]:
            violations.append({
                "budget": "max_same_file_jobs_per_cycle",
                "target_file": target,
                "observed": observed,
                "limit": limits["max_same_file_jobs_per_cycle"],
            })
    for family, observed in family_counts.items():
        if observed > limits["max_same_prompt_family_per_cycle"]:
            violations.append({
                "budget": "max_same_prompt_family_per_cycle",
                "prompt_family": family,
                "observed": observed,
                "limit": limits["max_same_prompt_family_per_cycle"],
            })

    return {
        "ts": _now_iso(),
        "budgets": limits,
        "job_count": len(jobs),
        "failed_count": failed_count,
        "noop_count": noop_count,
        "violations": violations,
        "exceeded": bool(violations),
        "actions": ["pause_continues_update", "emit_inspector_event", "write_nightly_summary", "stop_creating_new_jobs"] if violations else [],
        "jobs": job_results,
    }


def record_budget_violation(project_dir: str | Path, budget_report: Dict[str, Any]) -> Dict[str, Any]:
    """Pause continues-update and write Inspector/nightly summaries when limits are exceeded."""
    paths = AutonomyPaths(Path(project_dir))
    if not budget_report.get("exceeded"):
        return {"ok": True, "action": "none", "reason": "budget_not_exceeded"}

    paths.continues_update_stop.parent.mkdir(parents=True, exist_ok=True)
    paths.continues_update_stop.write_text(_now_iso(), encoding="utf-8")

    event = {
        "ts": _now_iso(),
        "event": "AUTONOMY_BUDGET_EXCEEDED",
        "role": "autonomy_control",
        "violations": budget_report.get("violations", []),
        "action": "paused_continues_update",
    }
    _append_jsonl(paths.live_feed, event)
    _append_jsonl(paths.nightly_jsonl, event)
    paths.nightly_md.parent.mkdir(parents=True, exist_ok=True)
    with paths.nightly_md.open("a", encoding="utf-8") as handle:
        handle.write(
            "\n\n## Autonomy budget exceeded - "
            + event["ts"]
            + "\n"
            + "- action: paused continues_update\n"
            + "- violations: "
            + json.dumps(event["violations"], ensure_ascii=True)
            + "\n"
        )
    return {"ok": True, "action": "paused_continues_update", "event": event}


def scan_autonomy_quality(project_dir: str | Path) -> Dict[str, Any]:
    """Return a non-mutating quality-gate report for Inspector display."""
    paths = AutonomyPaths(Path(project_dir))
    search_roots = [
        paths.project_dir / "aider_jobs" / "failed",
        paths.project_dir / "aider_jobs" / "done",
        paths.project_dir / "tasks" / "failed",
        paths.project_dir / "solutions",
    ]
    artifacts: List[Dict[str, Any]] = []
    for root in search_roots:
        if not root.exists():
            continue
        for item in sorted(root.glob("*")):
            if item.is_file() and item.suffix.lower() in {".json", ".txt", ".md"}:
                artifacts.append(classify_autonomy_artifact(item))

    quarantine_candidates = [
        row for row in artifacts if row.get("recommended_action") == "quarantine"
    ]
    report = {
        "ts": _now_iso(),
        "phase": "phase_1_quality_gate",
        "kill_switch_present": paths.kill_switch.exists(),
        "artifact_count": len(artifacts),
        "quarantine_candidate_count": len(quarantine_candidates),
        "quarantine_candidates": quarantine_candidates[:100],
        "summary": (
            f"{len(quarantine_candidates)} quarantine candidate(s) found"
            if quarantine_candidates
            else "no autonomy flood candidates found"
        ),
        "policy": {
            "delete": "never",
            "bad_or_stuck_items": "quarantine",
            "kill_switch": str(paths.kill_switch),
        },
    }
    _write_json(paths.report_path, report)
    return report


def quarantine_autonomy_artifact(project_dir: str | Path, artifact_path: str | Path, reason: str) -> Dict[str, Any]:
    """Move one bad/stuck artifact into quarantine, preserving it."""
    paths = AutonomyPaths(Path(project_dir))
    source = Path(artifact_path)
    if paths.kill_switch.exists():
        return {
            "ok": False,
            "blocked": True,
            "reason": "kill_switch_present",
            "kill_switch": str(paths.kill_switch),
            "source": str(source),
        }
    try:
        resolved_project = paths.project_dir.resolve()
        resolved_source = source.resolve()
        if not str(resolved_source).lower().startswith(str(resolved_project).lower()):
            return {"ok": False, "reason": "source_outside_project", "source": str(source)}
    except Exception as exc:
        return {"ok": False, "reason": f"path_resolution_failed: {exc}", "source": str(source)}
    if not source.exists() or not source.is_file():
        return {"ok": False, "reason": "source_missing", "source": str(source)}

    rel = source.resolve().relative_to(paths.project_dir.resolve())
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination = paths.quarantine_dir / stamp / rel
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))
    manifest = {
        "ts": _now_iso(),
        "source": str(source),
        "destination": str(destination),
        "reason": reason,
        "policy": "quarantine-not-delete",
    }
    _write_json(destination.with_suffix(destination.suffix + ".quarantine.json"), manifest)
    return {"ok": True, **manifest}


def build_autonomy_control_summary(project_dir: str | Path) -> str:
    """Human-readable v1 summary for Inspector/control surfaces."""
    report = scan_autonomy_quality(project_dir)
    lines = [
        "[LUNA AUTONOMY CONTROL V1]",
        f"phase: {report['phase']}",
        f"kill_switch_present: {report['kill_switch_present']}",
        f"artifacts_scanned: {report['artifact_count']}",
        f"quarantine_candidates: {report['quarantine_candidate_count']}",
        f"summary: {report['summary']}",
        "policy: never delete; quarantine bad/stuck items",
    ]
    for row in report["quarantine_candidates"][:8]:
        lines.append(f"- {row['reason']}: {row['path']}")
    return "\n".join(lines)
