"""Phase 37 - Governance Recheck."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_callable_adapter_interface as p30i
import bilingual_voice_phase31_adapter_interface as p31i
import bilingual_voice_phase33_adapter_interface as p33i
import bilingual_voice_phase37_adapter_interface as p37i
import bilingual_voice_phase36_secret_boundary as sb


_PHASE = "phase37.governance_recheck.v1"


_AUDIO_TOKENS = tuple(a + b for a, b in (
    ("py", "ttsx3"), ("gt", "ts"), ("ed", "ge_tts"),
    ("pi", "per."), ("co", "qui"), ("whi", "sper"),
    ("pya", "udio"), ("sou", "nddevice"), ("py", "dub"),
    ("sou", "ndfile"), ("com", "types"), ("win", "32com"),
))

_EXEC_TOKENS = tuple(a + b for a, b in (
    ("subproc", "ess.run"), ("subproc", "ess.Popen"),
    ("subproc", "ess.call"), ("os.sy", "stem("),
    ("she", "ll=True"), ("os.po", "pen"),
    ("ctype", "s.windll"), ("powe", "rshell "),
    ("powe", "rshell.exe"),
))


_BOUNDED_BYTES = 256 * 1024


def _read_bounded(path: str) -> str:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    try:
        if p.stat().st_size > _BOUNDED_BYTES:
            return ""
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        return ""


def verify_phase37_phase30_strictness() -> dict[str, Any]:
    allowed = list(p30i.ALLOWED_ADAPTER_TYPES)
    return {
        "ok": allowed == ["dummy_metadata_adapter"],
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase37_phase31_boundary() -> dict[str, Any]:
    allowed = list(p31i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase37_phase33_boundary() -> dict[str, Any]:
    allowed = list(p33i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase37_four_adapter_boundary() -> dict[str, Any]:
    allowed = list(p37i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase37_phase36_secret_boundary() -> dict[str, Any]:
    pol = sb.get_phase36_secret_boundary_policy()
    return {
        "ok": isinstance(pol, dict) and pol.get(
            "safe_secret_path_token") == "local_secret_handoff",
        "policy_version": pol.get("version"),
        "phase": _PHASE,
    }


def verify_phase37_allowed_adapters_only(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(records, list):
        return {"ok": False, "reasons": ["records_not_list"]}
    allowed = set(p37i.ALLOWED_ADAPTER_TYPES)
    bad: list[str] = []
    for r in records[:200]:
        if not isinstance(r, dict):
            continue
        for k in ("adapter_name", "selected_adapter_name",
                  "adapter_type"):
            v = str(r.get(k) or "")
            if v and v not in allowed:
                bad.append(f"{k}:{v}")
    return {"ok": not bad, "bad": bad, "phase": _PHASE}


def verify_phase37_metadata_only_results(
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(results, list):
        return {"ok": False, "reasons": ["results_not_list"]}
    bad: list[str] = []
    for r in results[:200]:
        if not isinstance(r, dict):
            continue
        for k in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
            if r.get(k) is True:
                bad.append(f"{r.get('result_id') or 'unknown'}:{k}")
    return {"ok": not bad, "bad": bad, "phase": _PHASE}


def verify_phase37_signed_evidence_required(
    invocation_output: Any,
) -> dict[str, Any]:
    if not isinstance(invocation_output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    status = str(invocation_output.get("status") or "")
    pipe = invocation_output.get("signed_witness_pipeline") or {}
    has_signed_ev = bool(
        (pipe.get("signed_evidence_summary") or {})
        .get("evidence_validates"))
    if status == "ok" and not has_signed_ev:
        return {"ok": False,
                "reasons": ["signed_evidence_required_for_ok"],
                "phase": _PHASE}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def verify_phase37_witness_export_required(
    pipeline_output: Any,
) -> dict[str, Any]:
    if not isinstance(pipeline_output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    we = pipeline_output.get("witness_export_summary") or {}
    if pipeline_output.get("status") == "ok" and \
            we.get("status") != "ok":
        return {"ok": False,
                "reasons": ["witness_export_required_for_ok"],
                "phase": _PHASE}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def verify_phase37_exchange_required(
    pipeline_output: Any,
) -> dict[str, Any]:
    if not isinstance(pipeline_output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    ex = pipeline_output.get("exchange_summary") or {}
    if pipeline_output.get("status") == "ok" and \
            ex.get("status") not in ("ok", "witness_failed"):
        return {"ok": False,
                "reasons": ["exchange_required_for_ok"],
                "phase": _PHASE}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def verify_phase37_no_secret_leakage(
    records: list[Any],
) -> dict[str, Any]:
    if not isinstance(records, list):
        return {"ok": False, "reasons": ["records_not_list"]}
    leaks: list[str] = []
    for r in records[:200]:
        hits = sb.scan_object_for_secret_fields(r)
        for h in hits:
            if h not in leaks:
                leaks.append(h)
    return {"ok": not leaks, "leaks": leaks, "phase": _PHASE}


def verify_phase37_no_audio_boundary(
    records: list[Any],
) -> dict[str, Any]:
    if not isinstance(records, list):
        return {"ok": False, "reasons": ["records_not_list"]}
    hits: list[dict[str, Any]] = []
    for raw in records[:50]:
        if not isinstance(raw, str):
            continue
        src = _read_bounded(raw)
        if not src:
            continue
        for tok in _AUDIO_TOKENS:
            if tok in src:
                hits.append({"path": raw, "token": tok})
    return {"ok": not hits, "hits": hits, "phase": _PHASE}


def verify_phase37_no_execution_boundary(
    records: list[Any],
) -> dict[str, Any]:
    if not isinstance(records, list):
        return {"ok": False, "reasons": ["records_not_list"]}
    hits: list[dict[str, Any]] = []
    for raw in records[:50]:
        if not isinstance(raw, str):
            continue
        src = _read_bounded(raw)
        if not src:
            continue
        for tok in _EXEC_TOKENS:
            if tok in src:
                hits.append({"path": raw, "token": tok})
    return {"ok": not hits, "hits": hits, "phase": _PHASE}


def write_phase37_governance_recheck_report(
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
    "verify_phase37_phase30_strictness",
    "verify_phase37_phase31_boundary",
    "verify_phase37_phase33_boundary",
    "verify_phase37_four_adapter_boundary",
    "verify_phase37_phase36_secret_boundary",
    "verify_phase37_allowed_adapters_only",
    "verify_phase37_metadata_only_results",
    "verify_phase37_signed_evidence_required",
    "verify_phase37_witness_export_required",
    "verify_phase37_exchange_required",
    "verify_phase37_no_secret_leakage",
    "verify_phase37_no_audio_boundary",
    "verify_phase37_no_execution_boundary",
    "write_phase37_governance_recheck_report",
]
