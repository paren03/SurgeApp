"""Phase 33 - Governance Recheck.

Re-verifies Phase 30 strictness, Phase 31 two-adapter boundary, and
Phase 33 three-adapter boundary; checks that signed evidence is present
for successful Phase 33 calls.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_callable_adapter_interface as p30i
import bilingual_voice_phase31_adapter_interface as p31i
import bilingual_voice_phase33_adapter_interface as p33i


_PHASE = "phase33.governance_recheck.v1"


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


def verify_phase33_phase30_strictness() -> dict[str, Any]:
    allowed = list(p30i.ALLOWED_ADAPTER_TYPES)
    return {
        "ok": allowed == ["dummy_metadata_adapter"],
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase33_phase31_boundary() -> dict[str, Any]:
    allowed = list(p31i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase33_three_adapter_boundary() -> dict[str, Any]:
    allowed = list(p33i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase33_allowed_adapters_only(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(records, list):
        return {"ok": False, "reasons": ["records_not_list"]}
    allowed = set(p33i.ALLOWED_ADAPTER_TYPES)
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


def verify_phase33_metadata_only_results(
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(results, list):
        return {"ok": False, "reasons": ["results_not_list"]}
    bad: list[str] = []
    for r in results[:200]:
        if not isinstance(r, dict):
            continue
        for k in ("produced_audio", "invoked_tts", "used_subprocess",
                  "used_network", "wrote_files"):
            if r.get(k) is True:
                bad.append(f"{r.get('result_id') or 'unknown'}:{k}")
    return {"ok": not bad, "bad": bad, "phase": _PHASE}


def verify_phase33_signed_evidence_required(
    invocation_output: Any,
) -> dict[str, Any]:
    if not isinstance(invocation_output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    status = str(invocation_output.get("status") or "")
    has_signed = bool(invocation_output.get("signed_evidence"))
    if status == "ok" and not has_signed:
        return {"ok": False,
                "reasons": ["signed_evidence_required_for_ok"],
                "phase": _PHASE}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def verify_phase33_no_audio_boundary(
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


def verify_phase33_no_execution_boundary(
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


def write_phase33_governance_recheck_report(
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
    "verify_phase33_phase30_strictness",
    "verify_phase33_phase31_boundary",
    "verify_phase33_three_adapter_boundary",
    "verify_phase33_allowed_adapters_only",
    "verify_phase33_metadata_only_results",
    "verify_phase33_signed_evidence_required",
    "verify_phase33_no_audio_boundary",
    "verify_phase33_no_execution_boundary",
    "write_phase33_governance_recheck_report",
]
