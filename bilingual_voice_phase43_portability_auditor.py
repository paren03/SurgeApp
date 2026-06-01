"""Phase 43 - Portability Auditor.

Read-only audit of portability + exclusions over the bundle.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase43.portability_auditor.v1"


_RUNTIME_DB_TOKENS = (".sqlite", ".sqlite3", ".db")
_AUDIO_TOKENS = (".wav", ".mp3", ".ogg", ".flac",
                  ".m4a", ".aac", ".opus")
_EXCLUDED_PATH_TOKENS = (
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/", "/local_secret_handoff/",
    "/.claude/",
    "/corpus_sources/english/incoming/",
    "/corpus_sources/russian/incoming/",
)


_SECRET_FIELD_TOKENS = (
    "signing_key_material", "private_key",
    "material_hex", "sealed_payload",
    "raw_secret", "secret_material",
    "key_material_hex",
)


_COMMAND_FIELD_TOKENS = ("command", "command_line",
                          "shell_command",
                          "subprocess_args")


def _iter_paths(bundle: Any):
    if not isinstance(bundle, dict):
        return
    for e in bundle.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        yield e


def audit_phase43_no_runtime_db_artifacts(
    bundle: Any,
) -> dict[str, Any]:
    hits: list[str] = []
    for e in _iter_paths(bundle):
        ap = str(e.get("relative_path") or "").replace("\\", "/").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if ap.endswith(tok):
                hits.append(str(e.get("artifact_key")
                                 or e.get("absolute_path")))
                break
    return {"category": "no_runtime_db_artifacts",
            "ok": not hits,
            "severity": "fail" if hits else "pass",
            "hits": hits}


def audit_phase43_no_audio_artifacts(
    bundle: Any,
) -> dict[str, Any]:
    hits: list[str] = []
    for e in _iter_paths(bundle):
        ap = str(e.get("relative_path") or "").replace("\\", "/").lower()
        for tok in _AUDIO_TOKENS:
            if ap.endswith(tok):
                hits.append(str(e.get("artifact_key")
                                 or e.get("absolute_path")))
                break
    return {"category": "no_audio_artifacts",
            "ok": not hits,
            "severity": "fail" if hits else "pass",
            "hits": hits}


def audit_phase43_excluded_artifacts(
    bundle: Any,
) -> dict[str, Any]:
    hits: list[str] = []
    for e in _iter_paths(bundle):
        ap = str(e.get("relative_path")
                  or "").replace("\\", "/").lower()
        for tok in _EXCLUDED_PATH_TOKENS:
            if tok in ap:
                hits.append(str(e.get("artifact_key")
                                 or e.get("absolute_path")))
                break
    return {"category": "excluded_artifacts",
            "ok": not hits,
            "severity": "fail" if hits else "pass",
            "hits": hits}


def audit_phase43_no_secret_leakage(
    bundle: Any,
) -> dict[str, Any]:
    hits: list[str] = []
    for e in _iter_paths(bundle):
        inline = e.get("inline_content")
        body_str = ""
        if isinstance(inline, (dict, list)):
            try:
                body_str = json.dumps(inline,
                                        ensure_ascii=False,
                                        default=str)
            except Exception:  # noqa: BLE001
                body_str = ""
        elif isinstance(inline, str):
            body_str = inline
        if not body_str:
            continue
        for tok in _SECRET_FIELD_TOKENS:
            if f'"{tok}"' in body_str:
                hits.append(
                    f"{e.get('artifact_key')}:{tok}")
                break
    return {"category": "no_secret_leakage",
            "ok": not hits,
            "severity": "fail" if hits else "pass",
            "hits": hits}


def audit_phase43_no_adapter_invocation_claims(
    bundle: Any,
) -> dict[str, Any]:
    """Bundle's own boundary_summary must claim
    `no_adapter_reinvocation_in_bundle=True`."""
    if not isinstance(bundle, dict):
        return {"category":
                "no_adapter_invocation_claims",
                "ok": False,
                "severity": "fail",
                "reason": "bundle_not_dict"}
    bs = bundle.get("boundary_summary") or {}
    ok = bool(bs.get("no_adapter_reinvocation_in_bundle"))
    return {"category":
            "no_adapter_invocation_claims",
            "ok": ok,
            "severity": "pass" if ok else "fail",
            "boundary_summary": bs}


def audit_phase43_no_command_fields(
    bundle: Any,
) -> dict[str, Any]:
    hits: list[str] = []
    for e in _iter_paths(bundle):
        inline = e.get("inline_content")
        body_str = ""
        if isinstance(inline, (dict, list)):
            try:
                body_str = json.dumps(inline,
                                        ensure_ascii=False,
                                        default=str)
            except Exception:  # noqa: BLE001
                body_str = ""
        if not body_str:
            continue
        for tok in _COMMAND_FIELD_TOKENS:
            if f'"{tok}"' in body_str:
                hits.append(
                    f"{e.get('artifact_key')}:{tok}")
                break
    return {"category": "no_command_fields",
            "ok": not hits,
            "severity": "fail" if hits else "pass",
            "hits": hits}


def audit_phase43_phase21_metadata(
    bundle: Any,
) -> dict[str, Any]:
    txt = str((bundle or {}).get(
        "phase21_status_text") or "")
    drifted = txt == "STAGED_AWAITING_OPERATOR"
    if txt not in ("BLOCKED", "STAGED_AWAITING_OPERATOR"):
        return {"category": "phase21_metadata",
                "ok": False,
                "severity": "fail",
                "reason":
                    f"phase21_status_unexpected:{txt}"}
    return {"category": "phase21_metadata",
            "ok": True,
            "severity": "warn" if drifted else "pass",
            "phase21_status_text": txt}


def audit_phase43_bundle_portability(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"audit_id":
                f"p43audit_{int(time.time())}",
                "ok": False,
                "reasons": ["bundle_not_dict"],
                "phase": _PHASE}
    checks = [
        audit_phase43_no_runtime_db_artifacts(bundle),
        audit_phase43_no_audio_artifacts(bundle),
        audit_phase43_excluded_artifacts(bundle),
        audit_phase43_no_secret_leakage(bundle),
        audit_phase43_no_adapter_invocation_claims(
            bundle),
        audit_phase43_no_command_fields(bundle),
        audit_phase43_phase21_metadata(bundle),
    ]
    fail = sum(1 for c in checks
                if c.get("severity") == "fail")
    warn = sum(1 for c in checks
                if c.get("severity") == "warn")
    passc = sum(1 for c in checks
                 if c.get("severity") == "pass")
    return {
        "audit_id": f"p43audit_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "checks": checks,
        "fail_count": fail,
        "warn_count": warn,
        "pass_count": passc,
        "ok": fail == 0,
        "summary": (
            f"phase43 portability audit: fail={fail} "
            f"warn={warn} pass={passc}"),
    }


def summarize_phase43_portability_audit(
    audit: Any,
) -> dict[str, Any]:
    if not isinstance(audit, dict):
        return {"ok": False, "summary": "no_audit"}
    return {
        "ok": bool(audit.get("ok")),
        "summary": audit.get("summary"),
        "audit_id": audit.get("audit_id"),
        "phase": _PHASE,
    }


def write_phase43_portability_audit_report(
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


__all__ = [
    "audit_phase43_bundle_portability",
    "audit_phase43_excluded_artifacts",
    "audit_phase43_no_secret_leakage",
    "audit_phase43_no_audio_artifacts",
    "audit_phase43_no_runtime_db_artifacts",
    "audit_phase43_no_adapter_invocation_claims",
    "audit_phase43_no_command_fields",
    "audit_phase43_phase21_metadata",
    "summarize_phase43_portability_audit",
    "write_phase43_portability_audit_report",
]
