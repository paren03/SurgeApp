"""Phase 47 - Federation Drift Detector (read-only)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase47.drift_detector.v1"


_EXPECTED_BASELINES = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


def _packages_from(imported: list[Any]
                     ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in imported or []:
        if isinstance(i, dict):
            p = i.get("package")
            if isinstance(p, dict):
                out.append(p)
    return out


def detect_phase47_checkout_count_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "checkout_count_drift",
                "drifted": True,
                "severity": "fail",
                "reason": "records_not_dict"}
    g_count = int((records.get("graph") or {}).get(
        "checkout_count") or 0)
    m_count = int((records.get("manifest") or {}).get(
        "checkout_count") or 0)
    i_count = len(_packages_from(
        records.get("imported_timelines") or []))
    drifted = not (g_count == m_count == i_count
                    and g_count >= 2)
    return {
        "category": "checkout_count_drift",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "graph_count": g_count,
        "manifest_count": m_count,
        "imported_count": i_count,
    }


def detect_phase47_timeline_root_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "timeline_root_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    graph = records.get("graph") or {}
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    expected = {p.get("checkout_id"):
                str(p.get("timeline_root_hash")
                     or "")
                for p in pkgs}
    observed = dict(
        graph.get("timeline_root_hashes") or {})
    drifted = expected != observed
    return {
        "category": "timeline_root_drift",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "expected": expected,
        "observed": observed,
    }


def detect_phase47_adapter_allowlist_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "adapter_allowlist_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    drifts: list[str] = []
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    for p in pkgs:
        n = int(p.get("adapter_allowlist_count") or 0)
        if n != 5:
            drifts.append(
                f"{p.get('checkout_id')}:{n}")
    return {
        "category": "adapter_allowlist_drift",
        "drifted": bool(drifts),
        "severity": "fail" if drifts else "pass",
        "drifts": drifts,
    }


def detect_phase47_baseline_claim_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "baseline_claim_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    drifts: list[str] = []
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    for p in pkgs:
        baseline = p.get(
            "production_baseline_expected") or {}
        for k, v in _EXPECTED_BASELINES.items():
            if baseline.get(k) != v:
                drifts.append(
                    f"{p.get('checkout_id')}:{k}:"
                    f"{baseline.get(k)}!={v}")
    return {
        "category": "baseline_claim_drift",
        "drifted": bool(drifts),
        "severity": "fail" if drifts else "pass",
        "drifts": drifts,
    }


def detect_phase47_phase21_status_drift(
    records: dict[str, Any],
    root: Optional[Path] = None,
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "phase21_status_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    drifts_fail: list[str] = []
    drifts_warn: list[str] = []
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    for p in pkgs:
        p21 = str(p.get("phase21_status_text") or "")
        cid = p.get("checkout_id")
        if p21 == "BLOCKED":
            continue
        if p21 == "STAGED_AWAITING_OPERATOR":
            drifts_warn.append(f"{cid}:STAGED")
            continue
        drifts_fail.append(f"{cid}:{p21}")
    # Read-only check of incoming folders for warn
    r = root or Path(__file__).resolve().parent
    en_inc = (r / "corpus_sources" / "english"
                / "incoming")
    ru_inc = (r / "corpus_sources" / "russian"
                / "incoming")
    en_files: list[str] = []
    ru_files: list[str] = []
    try:
        if en_inc.exists() and en_inc.is_dir():
            for x in en_inc.iterdir():
                if x.is_file():
                    en_files.append(x.name)
                if len(en_files) > 100:
                    break
        if ru_inc.exists() and ru_inc.is_dir():
            for x in ru_inc.iterdir():
                if x.is_file():
                    ru_files.append(x.name)
                if len(ru_files) > 100:
                    break
    except Exception:  # noqa: BLE001
        pass
    staged_locally = bool(en_files or ru_files)
    if staged_locally:
        drifts_warn.append(
            "local_incoming_staged_no_import")
    drifted = bool(drifts_fail) or bool(drifts_warn)
    severity = "fail" if drifts_fail else (
        "warn" if drifts_warn else "pass")
    return {
        "category": "phase21_status_drift",
        "drifted": drifted,
        "severity": severity,
        "drifts_fail": drifts_fail,
        "drifts_warn": drifts_warn,
        "english_incoming_filenames": en_files,
        "russian_incoming_filenames": ru_files,
    }


def detect_phase47_boundary_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category": "boundary_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    drifts: list[str] = []
    graph = records.get("graph") or {}
    bs = graph.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts",
              "no_subprocess", "no_network",
              "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_in_federation",
              "no_production_db_read_in_federation"):
        if bs.get(k) is not True:
            drifts.append(f"graph:{k}")
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    for p in pkgs:
        cid = p.get("checkout_id")
        pbs = p.get("boundary_summary") or {}
        if pbs.get(
                "no_adapter_invocation_in_timeline"
                ) is not True:
            drifts.append(f"{cid}:adapter_invocation")
        if pbs.get(
                "no_production_db_read_in_timeline"
                ) is not True:
            drifts.append(f"{cid}:db_read")
    return {
        "category": "boundary_drift",
        "drifted": bool(drifts),
        "severity": "fail" if drifts else "pass",
        "drifts": drifts,
    }


def detect_phase47_secret_audio_command_drift(
    records: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(records, dict):
        return {"category":
                "secret_audio_command_drift",
                "drifted": True, "severity": "fail",
                "reason": "records_not_dict"}
    hits: list[str] = []
    banned = (
        "raw_transcript", "sensitive_facts",
        "signing_key_material", "private_key",
        "material_hex", "sealed_payload",
        "audio_bytes", "audio_path", "audio_file",
        "command", "command_line",
    )
    pkgs = _packages_from(
        records.get("imported_timelines") or [])
    for p in pkgs:
        for k in banned:
            if k in p and p.get(k) not in (
                    None, "", False, [], {}):
                hits.append(
                    f"{p.get('checkout_id')}:{k}")
    graph = records.get("graph") or {}
    for k in banned:
        if k in graph and graph.get(k) not in (
                None, "", False, [], {}):
            hits.append(f"graph:{k}")
    manifest = records.get("manifest") or {}
    for k in banned:
        if k in manifest and manifest.get(k) not in (
                None, "", False, [], {}):
            hits.append(f"manifest:{k}")
    return {
        "category": "secret_audio_command_drift",
        "drifted": bool(hits),
        "severity": "fail" if hits else "pass",
        "hits": hits,
    }


def detect_phase47_federation_drift(
    imported_timelines: Any,
    graph: Any,
    manifest: Any,
) -> dict[str, Any]:
    records = {
        "imported_timelines": imported_timelines or [],
        "graph": graph or {},
        "manifest": manifest or {},
    }
    checks = [
        detect_phase47_checkout_count_drift(records),
        detect_phase47_timeline_root_drift(records),
        detect_phase47_adapter_allowlist_drift(records),
        detect_phase47_baseline_claim_drift(records),
        detect_phase47_phase21_status_drift(records),
        detect_phase47_boundary_drift(records),
        detect_phase47_secret_audio_command_drift(
            records),
    ]
    fail = sum(1 for c in checks
                if c.get("severity") == "fail")
    warn = sum(1 for c in checks
                if c.get("severity") == "warn")
    passc = sum(1 for c in checks
                 if c.get("severity") == "pass")
    return {
        "drift_id": f"p47drift_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "checks": checks,
        "fail_count": fail,
        "warn_count": warn,
        "pass_count": passc,
        "ok": fail == 0,
        "summary": (
            f"phase47 drift: fail={fail} warn={warn} "
            f"pass={passc}"),
    }


def summarize_phase47_drift(
    drift: Any,
) -> dict[str, Any]:
    if not isinstance(drift, dict):
        return {"ok": False, "summary": "no_drift"}
    return {
        "ok": bool(drift.get("ok")),
        "summary": drift.get("summary"),
        "drift_id": drift.get("drift_id"),
        "phase": _PHASE,
    }


def write_phase47_drift_report(
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
    "detect_phase47_federation_drift",
    "detect_phase47_checkout_count_drift",
    "detect_phase47_timeline_root_drift",
    "detect_phase47_adapter_allowlist_drift",
    "detect_phase47_baseline_claim_drift",
    "detect_phase47_phase21_status_drift",
    "detect_phase47_boundary_drift",
    "detect_phase47_secret_audio_command_drift",
    "summarize_phase47_drift",
    "write_phase47_drift_report",
]
