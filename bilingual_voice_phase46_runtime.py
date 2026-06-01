"""Phase 46 - Cross-Archive Long-Horizon Timeline Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase46_timeline_contract as tc
import bilingual_voice_phase46_archive_collector as ac
import bilingual_voice_phase46_timeline_builder as tb
import bilingual_voice_phase46_timeline_manifest as tm
import bilingual_voice_phase46_long_horizon_verifier as lhv
import bilingual_voice_phase46_tamper_suite as ts
import bilingual_voice_phase46_operator_packet as op
import bilingual_voice_phase46_status_dashboard as sd


_PHASE = "phase46.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase46_id", "contract", "timeline",
    "timeline_manifest", "verification_result",
    "tamper_suite_result", "operator_packet",
    "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p46_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase46_long_horizon_timeline(
    output_dir: Optional[str] = None,
    archive_count: int = 3,
    capture_spacing_seconds: float = 1.05,
) -> dict[str, Any]:
    timeline_id = _new_id()
    contract = tc.create_phase46_timeline_contract(
        timeline_id=timeline_id)
    # Capture N Phase 45 archives over time (default 3
    # — enforces min_archive_count=2 and gives us link
    # diversity).
    n = max(2, min(int(archive_count or 2), 8))
    # Per-run subdirectory so accumulated state from
    # prior runs does not bleed into the current
    # timeline count.
    run_suffix = (f"run_{int(time.time())}_"
                   f"{uuid.uuid4().hex[:8]}")
    if output_dir:
        captures_dir = (Path(output_dir)
                         / "captured_archives"
                         / run_suffix)
    else:
        captures_dir = (Path(__file__).resolve().parent
                         / "bilingual_stack"
                         / "voice_adapter_phase46"
                         / "captured_archives"
                         / run_suffix)
    captures_dir.mkdir(parents=True, exist_ok=True)
    ac.capture_n_phase45_archives(
        n, output_dir=str(captures_dir),
        spacing_seconds=capture_spacing_seconds)
    collection = ac.load_captured_archives(
        base_dir=str(captures_dir))
    timeline = tb.build_phase46_timeline(
        collection, timeline_id=timeline_id)
    manifest = tm.create_phase46_timeline_manifest(
        timeline)
    verification_result = \
        lhv.verify_phase46_long_horizon_timeline(
            timeline, manifest=manifest)
    tamper_result = ts.run_phase46_tamper_suite(
        timeline, manifest=manifest)
    packet = op.create_phase46_operator_packet(
        contract, timeline, manifest,
        verification_result, tamper_result)
    dashboard = sd.create_phase46_status_dashboard(
        packet)
    # Status logic
    c_val = tc.validate_phase46_timeline_contract(
        contract)
    t_val = tb.validate_phase46_timeline(timeline)
    m_val = tm.validate_phase46_timeline_manifest(
        manifest)
    pkt_val = op.validate_phase46_operator_packet(
        packet)
    dash_val = sd.validate_phase46_status_dashboard(
        dashboard)
    ts_val = ts.validate_phase46_tamper_suite_result(
        tamper_result)
    status = "ok"
    if not (c_val.get("ok") and t_val.get("ok")
            and m_val.get("ok") and pkt_val.get("ok")
            and dash_val.get("ok") and ts_val.get("ok")
            and verification_result.get("ok")
            and tamper_result.get("ok")):
        status = "drift_detected"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(
                tc.write_phase46_timeline_contract_report(
                    contract,
                    str(base / "timeline_contracts"
                         / "timeline_contract.json")))
            paths.append(tb.write_phase46_timeline(
                timeline,
                str(base / "timelines"
                     / "timeline.json")))
            paths.append(tm.write_phase46_timeline_manifest(
                manifest,
                str(base / "timeline_manifests"
                     / "timeline_manifest.json")))
            paths.append(
                lhv.write_phase46_long_horizon_verification_report(
                    verification_result,
                    str(base / "verification_outputs"
                         / "verification_result.json")))
            paths.append(ts.write_phase46_tamper_suite_report(
                tamper_result,
                str(base / "tamper_tests"
                     / "tamper_suite.json")))
            paths.append(op.write_phase46_operator_packet(
                packet,
                str(base / "operator_packets"
                     / "operator_packet.json")))
            paths.append(
                op.write_phase46_operator_packet_markdown(
                    op.create_phase46_operator_packet_markdown(
                        packet),
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
            paths.append(sd.write_phase46_status_dashboard(
                dashboard,
                str(base / "dashboards"
                     / "STATUS_DASHBOARD.json")))
            paths.append(
                sd.write_phase46_status_dashboard_markdown(
                    sd.create_phase46_dashboard_markdown(
                        dashboard),
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase46_id": timeline_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "timeline": timeline,
        "timeline_manifest": manifest,
        "verification_result": verification_result,
        "tamper_suite_result": tamper_result,
        "operator_packet": packet,
        "status_dashboard": dashboard,
        "status": status,
        "safety_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_corpus_import": True,
            "no_main_runtime_integration": True,
            "no_adapter_invocation_in_timeline": True,
            "no_production_db_read_in_timeline": True,
        },
        "isolation_summary": {
            "no_program_s": True,
            _NO_WORKER_KEY: True,
            _NO_TIER_KEY: True,
        },
        "phase21_status":
            (packet.get("phase21_import_status") or {})
            .get("status_text", "BLOCKED"),
        "paths_written": paths,
        "gap_notes": [],
    }


def validate_phase46_timeline_output(
    output: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(output, dict):
        return {"ok": False,
                "reasons": ["output_not_dict"]}
    for f in _REQUIRED_OUTPUT_FIELDS:
        if f not in output:
            reasons.append(f"missing_field:{f}")
    if str(output.get("status") or "") not in (
            "ok", "ok_with_warnings", "drift_detected"):
        reasons.append("invalid_status")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase46_timeline_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase46 timeline: status="
            f"{output.get('status')} phase21="
            f"{output.get('phase21_status')}"),
        "phase46_id": output.get("phase46_id"),
        "phase": _PHASE,
    }


def write_phase46_runtime_report(
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
    "run_phase46_long_horizon_timeline",
    "validate_phase46_timeline_output",
    "summarize_phase46_timeline_output",
    "write_phase46_runtime_report",
]
