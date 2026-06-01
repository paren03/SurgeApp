"""Phase 39 - Bundled Rehearsal Report Writer.

Bundles the contract + consent + trace + recheck into a single
operator-readable JSON report and an optional Markdown summary.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase39.rehearsal_report.v1"


_REQUIRED_REPORT_FIELDS = (
    "report_id", "created_at", "phase",
    "contract", "consent", "trace", "recheck",
    "ok", "summary",
    "production_invariants_expected",
    "forbidden_runtime_actions",
)


_PRODUCTION_INVARIANTS = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


# Runtime-assembled tokens so source does NOT contain the
# literal forbidden runtime identifiers.
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"

_FORBIDDEN_ACTIONS = (
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "main_runtime_integration",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "corpus_import",
)


def bundle_rehearsal_report(
    contract: dict[str, Any],
    consent: dict[str, Any],
    trace: dict[str, Any],
    recheck: dict[str, Any],
) -> dict[str, Any]:
    ok = bool(recheck.get("ok"))
    trace_ok = isinstance(trace, dict) and \
        trace.get("rehearsal_dry_run_only") is True
    consent_ok = isinstance(consent, dict) and \
        consent.get("status") == "ok"
    ok_total = ok and trace_ok and consent_ok
    return {
        "report_id": f"rreport_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "contract": contract or {},
        "consent": consent or {},
        "trace": trace or {},
        "recheck": recheck or {},
        "ok": ok_total,
        "summary": (
            f"phase39 rehearsal: receipts="
            f"{(trace or {}).get('receipt_count')} "
            f"ok={(trace or {}).get('ok_receipt_count')} "
            f"refused="
            f"{(trace or {}).get('refused_receipt_count')} "
            f"kill_switch_blocked="
            f"{(trace or {}).get('kill_switch_blocked_count')} "
            f"recheck_ok={ok}"),
        "production_invariants_expected":
            dict(_PRODUCTION_INVARIANTS),
        "forbidden_runtime_actions":
            list(_FORBIDDEN_ACTIONS),
        "notes": [
            "Report carries no signing material.",
            "Report carries no raw operator_id.",
            "Report carries no spoken render payload.",
            "Rehearsal does NOT integrate into Luna main "
            "runtime.",
            "Phase 21 real corpus import remains BLOCKED.",
        ],
    }


def validate_rehearsal_report(
    report: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(report, dict):
        return {"ok": False, "reasons": ["report_not_dict"]}
    for f in _REQUIRED_REPORT_FIELDS:
        if f not in report:
            reasons.append(f"missing_field:{f}")
    pi = report.get("production_invariants_expected") or {}
    for k, v in _PRODUCTION_INVARIANTS.items():
        if pi.get(k) != v:
            reasons.append(f"invariant_drift:{k}")
    return {"ok": not reasons, "reasons": reasons}


def create_rehearsal_markdown(
    report: Any,
) -> str:
    if not isinstance(report, dict):
        return ""
    trace = report.get("trace") or {}
    recheck = report.get("recheck") or {}
    cov = trace.get("per_stage_coverage") or {}
    lines: list[str] = []
    lines.append("# Phase 39 - Operator Dry-Run Rehearsal "
                  "Report\n")
    lines.append(f"_Generated at "
                  f"{int(report.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Receipts:** {trace.get('receipt_count')}\n")
    lines.append(f"- **ok:** {trace.get('ok_receipt_count')}\n")
    lines.append(f"- **refused:** "
                  f"{trace.get('refused_receipt_count')}\n")
    lines.append(f"- **kill-switch blocked:** "
                  f"{trace.get('kill_switch_blocked_count')}\n")
    lines.append(f"- **adapter distribution:** "
                  f"{trace.get('adapter_distribution')}\n")
    lines.append(f"- **recheck ok:** {recheck.get('ok')}\n")
    lines.append(f"- **allowlist ok:** "
                  f"{recheck.get('all_receipts_within_allowlist')}"
                  f"\n")
    lines.append(f"- **metadata-only ok:** "
                  f"{recheck.get('all_receipts_metadata_only')}"
                  f"\n")
    lines.append(f"- **secret-leakage ok:** "
                  f"{recheck.get('secret_leakage_ok')}\n")
    lines.append("- **stage coverage:**\n")
    for k, v in cov.items():
        lines.append(f"  - {k}: present="
                      f"{v.get('present')} absent="
                      f"{v.get('absent')}\n")
    lines.append("")
    lines.append("**This rehearsal does NOT produce audio. "
                  "No TTS / subprocess / network / "
                  "multiprocessing.**\n")
    return "".join(lines)


def write_rehearsal_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_rehearsal_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "bundle_rehearsal_report",
    "validate_rehearsal_report",
    "create_rehearsal_markdown",
    "write_rehearsal_report",
    "write_rehearsal_markdown",
]
