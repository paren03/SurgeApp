"""Phase 6A: Luna Manual Schema Review Helper.

Stdlib only. Inventories every tracked Luna schema/policy/checklist/roadmap
file under memory/, hashes them, classifies them, and scans for dangerous
"enabling" flags (e.g. safe_to_execute_now=true, allow_aider=true,
guardian_enforcing_live=true). Produces a Serge-readable manual review
report so the `manual_schema_review` checklist item from Phase 5Z can be
satisfied prior to any future live Guardian enforcement phase.

Hard rules in Phase 6A:
  * advisory_only is ALWAYS True.
  * live_enforcement_enabled is ALWAYS False.
  * safe_to_execute_now is ALWAYS False.
  * safe_to_apply_real_project is ALWAYS False.
  * The module READS the policies/schemas/checklists; it never edits them.
  * No execution. No installs. No external API calls. No Aider invocation.

Tracked schema (optional):
  memory/luna_schema_review.schema.json

Generated runtime artifacts (gitignored):
  memory/luna_schema_review_report.json
  memory/luna_schema_review_report.md
  memory/luna_schema_policy_inventory.json

Phase archive copy (Markdown only, no secrets) is written to:
  D:\\SurgeApp\\Luna New UpGrades\\PHASE6A_SCHEMA_REVIEW_HELPER_REPORT_<timestamp>.md

CLI:
  python -m luna_modules.luna_schema_review --self-test
  python -m luna_modules.luna_schema_review --scan
  python -m luna_modules.luna_schema_review --write
  python -m luna_modules.luna_schema_review --print-markdown
  python -m luna_modules.luna_schema_review --copy-to-upgrades
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent
_DEFAULT_ARCHIVE_DIR = "Luna New UpGrades"

# Glob patterns for tracked policy/schema/checklist/roadmap files under memory/.
_MEMORY_GLOB_PATTERNS: tuple = (
    "luna_*.schema.json",
    "luna_*_policy.json",
    "luna_*_checklist.json",
    "luna_*_roadmap.json",
    "luna_*_config.json",
    "luna_super_ai_*.json",
    "luna_autonomy_tiers.json",
    "luna_overnight_policy.json",
    "luna_routine_policy.json",
)

# Files we expect to be present in a healthy Luna project for live-enforcement readiness review.
_EXPECTED_FILES: tuple = (
    "memory/luna_live_enforcement_readiness_checklist.json",
    "memory/luna_serge_standing_approval_policy.json",
    "memory/luna_super_ai_north_star.json",
    "memory/luna_ai_council_policy.json",
    "memory/luna_approval_router_policy.json",
    "memory/luna_council_enforcer_policy.json",
    "memory/luna_deterministic_executor_policy.json",
    "memory/luna_guardian_readiness_policy.json",
    "memory/luna_decision_brief_policy.json",
    "memory/luna_formal_advisory_soak_policy.json",
    "memory/luna_soak_verdict.schema.json",
    "memory/luna_routine_policy.json",
    "memory/luna_autonomy_tiers.json",
)

# Dangerous boolean flag names — if any of these are True in a tracked file, FAIL.
_DANGEROUS_FLAG_NAMES: tuple = (
    "allow_execution",
    "safe_to_execute_now",
    "safe_to_apply_real_project",
    "guardian_enforcing_live",
    "live_enforcement_enabled",
    "live_enforcement_ready",
    "live_execution_enabled",
    "executor_allowed",
    "real_apply_allowed",
    "allow_real_apply",
    "allow_aider",
    "allow_code_edits",
    "allow_routine_code_edits",
    "allow_overnight_code_edits",
    "allow_installs",
    "allow_package_install",
    "allow_external_network",
    "allow_process_kill",
    "allow_git_push",
    "allow_git_reset",
    "allow_git_clean",
    "allow_memory_delete",
    "allow_log_delete",
    "allow_queue_delete",
    "allow_policy_weakening",
    "allow_quorum_weakening",
)

# Suspicious raw-text patterns (fallback in case JSON parse misses a nested key).
_DANGEROUS_RAW_PATTERNS: tuple = (
    '"safe_to_execute_now": true',
    '"safe_to_apply_real_project": true',
    '"guardian_enforcing_live": true',
    '"live_enforcement_enabled": true',
    '"live_enforcement_ready": true',
    '"allow_execution": true',
    '"allow_real_apply": true',
    '"allow_aider": true',
    '"allow_code_edits": true',
    '"allow_installs": true',
    '"allow_external_network": true',
    '"allow_process_kill": true',
    '"executor_allowed": true',
)

# Safe flag names — explicitly recorded as evidence that a False is in place.
_SAFE_FLAG_NAMES: tuple = (
    "safe_to_execute_now",
    "safe_to_apply_real_project",
    "guardian_enforcing_live",
    "live_enforcement_enabled",
    "live_enforcement_ready",
    "allow_execution",
    "allow_real_apply",
    "allow_aider",
    "allow_code_edits",
    "allow_installs",
    "allow_external_network",
    "advisory_only",  # advisory_only=True is a positive safe flag
)


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_review_id(prefix: str = "schema") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def sha256_file(path: Path | str) -> str:
    p = Path(path)
    if not p.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


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


# ---------- file discovery ----------


def _git_ls_files_memory(project_dir: Path) -> list[str] | None:
    """Try to list tracked memory/*.json files via git. Returns None if git unavailable."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(project_dir), "ls-files", "memory/*.json"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            return None
        files = [
            line.strip()
            for line in proc.stdout.splitlines()
            if line.strip().endswith(".json")
        ]
        return files
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def iter_candidate_policy_schema_files(project_dir: Path | str) -> list[str]:
    """Return relative paths of candidate policy/schema/checklist/roadmap files under memory/.

    Prefers `git ls-files memory/*.json` so the scan focuses on tracked sources of truth.
    Falls back to a glob over memory/ if git is unavailable.
    """
    pdir = Path(project_dir)
    mem = pdir / "memory"
    if not mem.is_dir():
        return []

    tracked = _git_ls_files_memory(pdir)
    if tracked is not None:
        out: list[str] = []
        for rel in tracked:
            name = Path(rel).name
            for pat in _MEMORY_GLOB_PATTERNS:
                if Path(name).match(pat):
                    out.append(rel.replace("\\", "/"))
                    break
        return sorted(set(out))

    # Glob fallback.
    seen: set[str] = set()
    for pat in _MEMORY_GLOB_PATTERNS:
        for p in mem.glob(pat):
            rel = f"memory/{p.name}"
            seen.add(rel)
    return sorted(seen)


# ---------- classification ----------


def classify_file_kind(path: Path | str, data: Any) -> str:
    """Classify a file by its name and JSON shape."""
    name = Path(path).name.lower()
    if name.endswith(".schema.json"):
        return "schema"
    if "checklist" in name:
        return "checklist"
    if "roadmap" in name:
        return "roadmap"
    if "policy" in name or "tiers" in name:
        return "policy"
    if isinstance(data, dict):
        if "schema_version" in data and (
            "$schema" in data or "definitions" in data or "properties" in data
        ):
            return "schema"
        if "required_before_live_guardian_enforcement" in data:
            return "checklist"
        if "phase" in data or "policy" in name:
            return "policy"
    return "unknown"


def guess_file_purpose(path: Path | str, data: Any) -> str:
    """Best-effort plain-English guess at what a file controls."""
    name = Path(path).name
    if isinstance(data, dict):
        desc = data.get("description") or data.get("purpose") or data.get("title")
        if isinstance(desc, str) and desc.strip():
            return desc.strip()[:200]
    # Heuristic from filename.
    lname = name.lower()
    if "north_star" in lname:
        return "Luna's Super-AI north star and standing-goal alignment."
    if "serge_standing_approval" in lname:
        return "Serge's standing approval intent for the delegated approval hierarchy."
    if "aider_tutor" in lname:
        return "Future Aider Tutor Mode roadmap (planned after safety chain)."
    if "council" in lname and "enforcer" in lname:
        return "Council enforcer advisory policy."
    if "approval_router" in lname:
        return "Approval router routing/quorum policy."
    if "deterministic_executor" in lname:
        return "Deterministic sandbox executor policy."
    if "guardian_readiness" in lname:
        return "Guardian readiness advisory policy."
    if "decision_brief" in lname:
        return "Phase 5VW decision-card morning brief policy."
    if "advisory_soak" in lname:
        return "Phase 5XY/5VW advisory soak policy."
    if "soak_verdict" in lname:
        return "Phase 5Z 24-hour soak verdict schema."
    if "live_enforcement_readiness" in lname:
        return "Phase 5Z live-enforcement readiness checklist (live disabled)."
    if "ai_council" in lname:
        return "Local AI council quorum/policy."
    if "routine_policy" in lname or "overnight_policy" in lname:
        return "Limited routine autonomy day/night policy."
    if "autonomy_tiers" in lname:
        return "Autonomy tier definitions for delegated approval."
    if "decision_card" in lname and "schema" in lname:
        return "Decision card schema (Phase 5T)."
    return f"Unclassified Luna policy/schema/config: {name}"


# ---------- dangerous-flag detection ----------


def _walk_truthy_keys(obj: Any, path_prefix: str = "") -> list[tuple[str, str]]:
    """Walk a JSON-decoded object and yield (key_path, key_name) for True boolean keys."""
    out: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            kp = f"{path_prefix}.{k}" if path_prefix else str(k)
            if isinstance(v, bool):
                if v is True:
                    out.append((kp, str(k)))
            elif isinstance(v, (dict, list)):
                out.extend(_walk_truthy_keys(v, kp))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            out.extend(_walk_truthy_keys(item, f"{path_prefix}[{i}]"))
    return out


def _walk_falsey_keys(obj: Any, path_prefix: str = "") -> list[tuple[str, str]]:
    """Walk a JSON-decoded object and yield (key_path, key_name) for False boolean keys."""
    out: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            kp = f"{path_prefix}.{k}" if path_prefix else str(k)
            if isinstance(v, bool):
                if v is False:
                    out.append((kp, str(k)))
            elif isinstance(v, (dict, list)):
                out.extend(_walk_falsey_keys(v, kp))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            out.extend(_walk_falsey_keys(item, f"{path_prefix}[{i}]"))
    return out


def detect_dangerous_policy_flags(
    path: Path | str,
    data: Any,
    raw_text: str = "",
) -> dict[str, list[str]]:
    """Detect dangerous (true) and safe (false) policy flags. Returns {dangerous, safe}."""
    dangerous: list[str] = []
    safe: list[str] = []

    if isinstance(data, (dict, list)):
        for kp, kn in _walk_truthy_keys(data):
            if kn in _DANGEROUS_FLAG_NAMES:
                dangerous.append(f"{kp}=true")
        for kp, kn in _walk_falsey_keys(data):
            if kn in _SAFE_FLAG_NAMES:
                safe.append(f"{kp}=false")

    if raw_text:
        lower = raw_text.lower()
        for pat in _DANGEROUS_RAW_PATTERNS:
            if pat.lower() in lower:
                # Avoid duplicating findings already detected via the JSON walk.
                marker = pat.replace('"', '').replace(': ', '=')
                if not any(marker in d for d in dangerous):
                    dangerous.append(f"raw_text:{pat!r}")

    return {"dangerous": sorted(set(dangerous)), "safe": sorted(set(safe))}


# ---------- per-file review ----------


def _risk_level_from_flags(
    kind: str,
    purpose: str,
    dangerous: list[str],
) -> str:
    if dangerous:
        return "critical"
    pl = purpose.lower()
    if any(k in pl for k in ("guardian", "executor", "council enforcer", "approval router")):
        return "high"
    if kind in ("checklist",) and "live_enforcement" in pl:
        return "high"
    if kind == "roadmap":
        return "low"
    if kind == "schema":
        return "low"
    return "medium"


def review_policy_schema_file(
    path: Path | str,
    project_dir: Path | str,
) -> dict[str, Any]:
    """Review a single policy/schema file. Returns an inventory record."""
    pdir = Path(project_dir).resolve()
    rel = str(Path(path)).replace("\\", "/")
    abs_p = pdir / rel
    record: dict[str, Any] = {
        "path": rel,
        "absolute_path": str(abs_p),
        "sha256": "",
        "size_bytes": 0,
        "modified_at": "",
        "schema_version": None,
        "kind": "unknown",
        "purpose_guess": "",
        "risk_level": "unknown",
        "dangerous_flags": [],
        "safe_flags": [],
        "parse_ok": False,
        "parse_error": "",
        "manual_review_notes": [],
    }

    if not abs_p.is_file():
        record["parse_error"] = "file_not_found"
        return record

    try:
        st = abs_p.stat()
        record["size_bytes"] = int(st.st_size)
        record["modified_at"] = _dt.datetime.fromtimestamp(
            st.st_mtime, tz=_dt.timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except OSError:
        pass

    record["sha256"] = sha256_file(abs_p)

    raw_text = ""
    try:
        raw_text = abs_p.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        record["parse_error"] = f"read_failed:{type(e).__name__}:{str(e)[:200]}"
        return record

    data: Any = None
    try:
        data = json.loads(raw_text)
        record["parse_ok"] = True
    except (ValueError, UnicodeDecodeError) as e:
        record["parse_error"] = f"json_parse_failed:{type(e).__name__}:{str(e)[:200]}"

    if isinstance(data, dict):
        sv = data.get("schema_version")
        if isinstance(sv, int):
            record["schema_version"] = sv

    kind = classify_file_kind(rel, data)
    record["kind"] = kind
    purpose = guess_file_purpose(rel, data)
    record["purpose_guess"] = purpose

    flags = detect_dangerous_policy_flags(rel, data, raw_text)
    record["dangerous_flags"] = flags["dangerous"]
    record["safe_flags"] = flags["safe"]
    record["risk_level"] = _risk_level_from_flags(kind, purpose, flags["dangerous"])

    if flags["dangerous"]:
        record["manual_review_notes"].append(
            "DANGEROUS flags detected — Serge must review and explicitly authorize."
        )
    if not record["parse_ok"]:
        record["manual_review_notes"].append(
            "Parse error: file did not load as JSON; treat as unreviewed."
        )

    return record


# ---------- inventory ----------


def build_schema_policy_inventory(project_dir: Path | str) -> dict[str, Any]:
    """Build a full inventory of tracked policy/schema files. Read-only."""
    pdir = Path(project_dir).resolve()
    files = iter_candidate_policy_schema_files(pdir)
    records = [review_policy_schema_file(rel, pdir) for rel in files]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(pdir),
        "scanned_files": files,
        "records": records,
        "advisory_only": True,
        "live_enforcement_enabled": False,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
    }


def validate_inventory(inventory: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(inventory, dict):
        return False, ["inventory not a dict"]
    if inventory.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{inventory.get('schema_version')!r}")
    if inventory.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if inventory.get("live_enforcement_enabled") is not False:
        errs.append("live_enforcement_enabled must be False")
    if not isinstance(inventory.get("records"), list):
        errs.append("records must be a list")
    return len(errs) == 0, errs


# ---------- report ----------


def build_schema_review_report(
    project_dir: Path | str,
    inventory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the Serge-readable schema review report.

    Hard rules: advisory_only=True, live_enforcement_enabled=False,
    safe_to_execute_now=False, safe_to_apply_real_project=False.
    """
    pdir = Path(project_dir).resolve()
    inv = inventory if isinstance(inventory, dict) else build_schema_policy_inventory(pdir)
    records = list(inv.get("records") or [])

    dangerous_findings: list[dict[str, Any]] = []
    parse_error_count = 0
    dangerous_count = 0
    for r in records:
        if r.get("dangerous_flags"):
            dangerous_count += len(r["dangerous_flags"])
            dangerous_findings.append({
                "path": r["path"],
                "risk_level": r["risk_level"],
                "kind": r["kind"],
                "dangerous_flags": list(r["dangerous_flags"]),
                "purpose_guess": r.get("purpose_guess", ""),
            })
        if not r.get("parse_ok"):
            parse_error_count += 1

    scanned_set = {r["path"] for r in records}
    missing = [rel for rel in _EXPECTED_FILES if rel not in scanned_set]

    if dangerous_count > 0:
        verdict = "FAIL_DANGEROUS_POLICY"
    elif missing or parse_error_count > 0:
        verdict = "INCOMPLETE_MISSING_FILES"
    else:
        verdict = "PASS_READY_FOR_SERGE_REVIEW"

    serge_summary = _build_serge_summary(
        verdict, dangerous_count, len(missing), parse_error_count, len(records),
    )

    rec_next = _recommended_next_action(verdict, dangerous_count, missing, parse_error_count)

    return {
        "schema_version": SCHEMA_VERSION,
        "review_id": make_review_id(),
        "generated_at": now_iso(),
        "project_dir": str(pdir),
        "advisory_only": True,
        "live_enforcement_enabled": False,
        "safe_to_execute_now": False,
        "safe_to_apply_real_project": False,
        "files_reviewed": len(records),
        "dangerous_flags_count": dangerous_count,
        "missing_expected_files_count": len(missing),
        "parse_error_count": parse_error_count,
        "verdict": verdict,
        "inventory": records,
        "dangerous_findings": dangerous_findings,
        "missing_expected_files": missing,
        "serge_summary": serge_summary,
        "recommended_next_action": rec_next,
        "phase_archive_copy": "",
        "notes": [
            "Phase 6A advisory only.",
            "live_enforcement_enabled stays False even on PASS.",
            "PASS only means: ready for Serge's manual review, not ready for live enforcement.",
        ],
    }


def _build_serge_summary(
    verdict: str,
    dangerous_count: int,
    missing_count: int,
    parse_errors: int,
    files_reviewed: int,
) -> str:
    if verdict == "FAIL_DANGEROUS_POLICY":
        return (
            f"DANGEROUS policy flags detected ({dangerous_count} flags across "
            f"{files_reviewed} reviewed files). Serge must review and reject or "
            "fix before proceeding. Live enforcement remains blocked."
        )
    if verdict == "INCOMPLETE_MISSING_FILES":
        return (
            f"Schema review is incomplete: {missing_count} expected file(s) missing, "
            f"{parse_errors} parse error(s). Live enforcement remains blocked. "
            "Generate the missing policies, then re-run --scan."
        )
    return (
        f"Reviewed {files_reviewed} tracked Luna policy/schema files. No dangerous "
        "flags found and all expected files present. Ready for Serge's manual "
        "review (read the report markdown). Live enforcement still NOT enabled — "
        "the manual_schema_review checklist item only flips to satisfied after "
        "Serge personally reads and approves."
    )


def _recommended_next_action(
    verdict: str,
    dangerous_count: int,
    missing: list[str],
    parse_errors: int,
) -> str:
    if verdict == "FAIL_DANGEROUS_POLICY":
        return (
            f"Audit the {dangerous_count} dangerous flag(s) in dangerous_findings. "
            "Do NOT mark manual_schema_review as satisfied. Do NOT proceed to live "
            "enforcement. Roll back any unintended policy change."
        )
    if verdict == "INCOMPLETE_MISSING_FILES":
        return (
            f"Generate missing files ({len(missing)}) or fix parse errors "
            f"({parse_errors}), then re-run --scan."
        )
    return (
        "Open memory/luna_schema_review_report.md in your editor and read it "
        "yourself. If you agree with every entry, manually mark "
        'manual_schema_review as "satisfied" in '
        "memory/luna_live_enforcement_readiness_checklist.json. Then run the 24h "
        "advisory soak: "
        "D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe -m luna_modules.luna_decision_brief "
        "--soak --cycles 144 --sleep-seconds 600 --write-soak"
    )


def validate_schema_review_report(report: Any) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not isinstance(report, dict):
        return False, ["report not a dict"]
    required = (
        "schema_version", "review_id", "generated_at", "project_dir",
        "advisory_only", "live_enforcement_enabled", "safe_to_execute_now",
        "safe_to_apply_real_project", "files_reviewed", "verdict",
        "inventory", "serge_summary",
    )
    for f in required:
        if f not in report:
            errs.append(f"missing_required_field:{f!r}")
    if report.get("schema_version") != 1:
        errs.append(f"invalid_schema_version:{report.get('schema_version')!r}")
    if report.get("advisory_only") is not True:
        errs.append("advisory_only must be True")
    if report.get("live_enforcement_enabled") is not False:
        errs.append("live_enforcement_enabled must be False")
    if report.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False")
    if report.get("safe_to_apply_real_project") is not False:
        errs.append("safe_to_apply_real_project must be False")
    valid_verdicts = (
        "PASS_READY_FOR_SERGE_REVIEW",
        "FAIL_DANGEROUS_POLICY",
        "INCOMPLETE_MISSING_FILES",
    )
    if report.get("verdict") not in valid_verdicts:
        errs.append(f"invalid_verdict:{report.get('verdict')!r}")
    return len(errs) == 0, errs


# ---------- markdown rendering ----------


def render_schema_review_markdown(report: dict[str, Any]) -> str:
    """Render a Serge-readable markdown report."""
    verdict = report.get("verdict", "")
    counts_line = (
        f"reviewed={report.get('files_reviewed', 0)} "
        f"dangerous={report.get('dangerous_flags_count', 0)} "
        f"missing={report.get('missing_expected_files_count', 0)} "
        f"parse_errors={report.get('parse_error_count', 0)}"
    )
    lines = [
        "# Luna Schema & Policy Review",
        "",
        f"**review_id**: {report.get('review_id', '')}",
        f"**generated_at**: {report.get('generated_at', '')}",
        f"**verdict**: {verdict}",
        f"**advisory_only**: {report.get('advisory_only', True)}",
        f"**live_enforcement_enabled**: {report.get('live_enforcement_enabled', False)}",
        f"**safe_to_execute_now**: {report.get('safe_to_execute_now', False)}",
        f"**safe_to_apply_real_project**: {report.get('safe_to_apply_real_project', False)}",
        "",
        f"## One-sentence Verdict",
        report.get("serge_summary", ""),
        "",
        "## Counts",
        f"- {counts_line}",
        "",
    ]

    danger = report.get("dangerous_findings") or []
    if danger:
        lines += ["## Dangerous Flags", ""]
        for d in danger:
            lines.append(f"- **{d.get('path', '')}** ({d.get('risk_level', '?')})")
            for f in d.get("dangerous_flags") or []:
                lines.append(f"   - {f}")
        lines.append("")

    missing = report.get("missing_expected_files") or []
    if missing:
        lines += ["## Missing Expected Files", ""]
        for m in missing:
            lines.append(f"- {m}")
        lines.append("")

    inv = report.get("inventory") or []
    high = [r for r in inv if r.get("risk_level") in ("high", "critical")]
    if high:
        lines += ["## Top High-Risk Policies (advisory)", ""]
        for r in high[:15]:
            lines.append(
                f"- [{r.get('risk_level', '?')}] `{r.get('path', '')}` "
                f"-- {r.get('purpose_guess', '')[:120]}"
            )
        lines.append("")

    lines += [
        "## What This Means",
        "",
        "- This report inventories every tracked Luna policy/schema/checklist/roadmap "
        "file under memory/.",
        "- A dangerous flag (e.g. `safe_to_execute_now=true`) anywhere in any tracked "
        "file would FAIL the verdict and block live enforcement.",
        "- A clean PASS only means the files are ready for Serge to read manually -- "
        "live enforcement stays disabled until Serge personally approves.",
        "",
        "## What NOT to Approve Yet",
        "",
        "- Do not mark `manual_schema_review` satisfied without reading the report.",
        "- Do not flip `live_enforcement_enabled` to true based on this report alone.",
        "- Do not approve any change that flips a `*_enabled` or `safe_to_*` flag to "
        "true without Serge's personal approval.",
        "",
        "## Next Command to Run",
        "",
        "```",
        "D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe -m luna_modules.luna_decision_brief "
        "--soak --cycles 144 --sleep-seconds 600 --write-soak",
        "```",
        "",
    ]

    if report.get("phase_archive_copy"):
        lines += [
            f"**phase_archive_copy**: {report['phase_archive_copy']}",
            "",
        ]

    notes = report.get("notes") or []
    if notes:
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
        lines.append("")

    return "\n".join(lines)


# ---------- write artifacts ----------


def write_schema_review_report(
    project_dir: Path | str,
    report: dict[str, Any],
) -> dict[str, str]:
    """Write the schema review JSON + markdown + inventory under memory/."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_schema_review_report.json"
    md_p = mem / "luna_schema_review_report.md"
    inv_p = mem / "luna_schema_policy_inventory.json"

    write_json_atomic(json_p, report)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_schema_review_markdown(report), encoding="utf-8")
    os.replace(tmp, md_p)

    inv_view = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": report.get("generated_at"),
        "advisory_only": True,
        "live_enforcement_enabled": False,
        "files_reviewed": report.get("files_reviewed", 0),
        "records": report.get("inventory", []),
    }
    write_json_atomic(inv_p, inv_view)

    return {
        "json": str(json_p),
        "md": str(md_p),
        "inventory": str(inv_p),
    }


def copy_report_to_phase_archive(
    project_dir: Path | str,
    markdown_text: str,
    archive_dir: str = _DEFAULT_ARCHIVE_DIR,
) -> str:
    """Copy the markdown report to the phase archive folder under project_dir.

    The archive path is project_dir/<archive_dir>/PHASE6A_SCHEMA_REVIEW_HELPER_REPORT_<timestamp>.md.
    The function never writes outside project_dir.
    """
    pdir = Path(project_dir).resolve()
    target_dir = pdir / archive_dir
    # Refuse to write if the resolved target escapes pdir.
    try:
        target_dir.resolve().relative_to(pdir)
    except ValueError:
        raise ValueError(f"archive path escapes project: {target_dir!r}")
    target_dir.mkdir(parents=True, exist_ok=True)

    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"PHASE6A_SCHEMA_REVIEW_HELPER_REPORT_{ts}.md"
    out = target_dir / fname
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(markdown_text, encoding="utf-8")
    os.replace(tmp, out)
    return str(out)


# ---------- self-test ----------


def self_test() -> int:
    """End-to-end self-test in a TemporaryDirectory. Returns 0 on success."""
    import tempfile

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        (td / _DEFAULT_ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)

        # Seed a clean policy file.
        (td / "memory" / "luna_clean_policy.json").write_text(
            json.dumps({
                "schema_version": 1,
                "advisory_only": True,
                "safe_to_execute_now": False,
                "safe_to_apply_real_project": False,
                "guardian_enforcing_live": False,
                "live_enforcement_enabled": False,
                "allow_aider": False,
                "allow_code_edits": False,
            }),
            encoding="utf-8",
        )

        # Inventory the temp project — should produce one record.
        inv = build_schema_policy_inventory(td)
        ok, errs = validate_inventory(inv)
        assert ok, f"inventory invalid: {errs}"
        assert len(inv["records"]) >= 1

        # Build report — verdict must be INCOMPLETE_MISSING_FILES because
        # the temp project does not have all expected real files.
        report = build_schema_review_report(td, inventory=inv)
        ok, errs = validate_schema_review_report(report)
        assert ok, f"report invalid: {errs}"
        assert report["safe_to_execute_now"] is False
        assert report["live_enforcement_enabled"] is False
        # No dangerous flags in the seed file.
        assert report["dangerous_flags_count"] == 0
        # Missing expected files (most are absent).
        assert report["missing_expected_files_count"] > 0
        assert report["verdict"] == "INCOMPLETE_MISSING_FILES"

        # Seed a dangerous policy and confirm verdict flips to FAIL.
        (td / "memory" / "luna_dangerous_policy.json").write_text(
            json.dumps({
                "schema_version": 1,
                "safe_to_execute_now": True,  # DANGEROUS
                "allow_aider": True,           # DANGEROUS
            }),
            encoding="utf-8",
        )
        report2 = build_schema_review_report(td)
        assert report2["verdict"] == "FAIL_DANGEROUS_POLICY", \
            f"expected FAIL got {report2['verdict']}"
        assert report2["dangerous_flags_count"] >= 2

        # Write the report and confirm it stays under temp.
        written = write_schema_review_report(td, report)
        for key, p in written.items():
            assert str(Path(p).resolve()).startswith(str(td.resolve())), \
                f"{p} escapes temp dir"
            assert "memory" in str(p)

        # Render markdown and copy to archive.
        md = render_schema_review_markdown(report)
        assert "Verdict" in md or "verdict" in md.lower()
        archive = copy_report_to_phase_archive(td, md, archive_dir=_DEFAULT_ARCHIVE_DIR)
        assert str(Path(archive).resolve()).startswith(str(td.resolve())), \
            f"archive {archive} escapes temp"
        assert "PHASE6A" in archive

        print(json.dumps({
            "self_test": "PASS",
            "clean_verdict": report["verdict"],
            "dangerous_verdict": report2["verdict"],
            "live_enforcement_enabled": False,
            "safe_to_execute_now": False,
        }, indent=2))
    return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Schema/Policy Review Helper (Phase 6A)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--scan", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--copy-to-upgrades", action="store_true",
                        help="Write the markdown report to D:\\SurgeApp\\Luna New UpGrades.")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.scan:
        report = build_schema_review_report(pdir)
        summary = {
            "verdict": report["verdict"],
            "files_reviewed": report["files_reviewed"],
            "dangerous_flags_count": report["dangerous_flags_count"],
            "missing_expected_files_count": report["missing_expected_files_count"],
            "parse_error_count": report["parse_error_count"],
            "advisory_only": report["advisory_only"],
            "live_enforcement_enabled": report["live_enforcement_enabled"],
            "safe_to_execute_now": report["safe_to_execute_now"],
            "safe_to_apply_real_project": report["safe_to_apply_real_project"],
            "serge_summary": report["serge_summary"],
            "recommended_next_action": report["recommended_next_action"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    if args.write:
        report = build_schema_review_report(pdir)
        written = write_schema_review_report(pdir, report)
        print(json.dumps({
            "verdict": report["verdict"],
            "files_reviewed": report["files_reviewed"],
            "dangerous_flags_count": report["dangerous_flags_count"],
            "missing_expected_files_count": report["missing_expected_files_count"],
            "advisory_only": True,
            "live_enforcement_enabled": False,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "written": written,
        }, indent=2))
        return 0

    if args.print_markdown:
        existing = load_json(
            pdir / "memory" / "luna_schema_review_report.json", default=None,
        )
        if not isinstance(existing, dict):
            existing = build_schema_review_report(pdir)
        sys.stdout.write(render_schema_review_markdown(existing))
        return 0

    if args.copy_to_upgrades:
        report = build_schema_review_report(pdir)
        written = write_schema_review_report(pdir, report)
        md = render_schema_review_markdown(report)
        archive_path = copy_report_to_phase_archive(pdir, md)
        report["phase_archive_copy"] = archive_path
        # Re-write the JSON so it records the archive path.
        write_json_atomic(
            Path(written["json"]),
            report,
        )
        print(json.dumps({
            "verdict": report["verdict"],
            "files_reviewed": report["files_reviewed"],
            "dangerous_flags_count": report["dangerous_flags_count"],
            "missing_expected_files_count": report["missing_expected_files_count"],
            "advisory_only": True,
            "live_enforcement_enabled": False,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "phase_archive_copy": archive_path,
            "written": written,
        }, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
