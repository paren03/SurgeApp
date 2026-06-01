"""Phase 39 - Rehearsal Runtime Orchestrator.

Single entry point that runs the full operator dry-run rehearsal:
contract -> umbrella consent -> per-scenario execution -> trace
assembly -> governance recheck -> bundled report. No daemons. No
threads. No subprocess. No audio. Bounded scenario count.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase39_rehearsal_contract as rc
import bilingual_voice_phase39_consent_orchestrator as co
import bilingual_voice_phase39_stage_executor as se
import bilingual_voice_phase39_trace_assembler as ta
import bilingual_voice_phase39_governance_recheck as gr
import bilingual_voice_phase39_rehearsal_report as rr


_PHASE = "phase39.rehearsal_runtime.v1"


def run_phase39_rehearsal(
    operator_id: str = "operator_local",
    scenarios: Optional[list[dict[str, Any]]] = None,
    output_dir: Optional[str] = None,
    write_per_scenario_receipts: bool = True,
) -> dict[str, Any]:
    contract = rc.create_rehearsal_contract(
        scenarios=scenarios, operator_id=operator_id)
    consent = co.create_umbrella_consent(
        operator_id=operator_id,
        scenario_count=contract.get("scenario_count") or 0)
    binding = co.bind_consent_to_contract(consent, contract)
    if not binding.get("ok"):
        return {
            "phase": _PHASE,
            "status": "refused",
            "reason": "consent_binding_failed",
            "binding": binding,
        }
    receipts: list[dict[str, Any]] = []
    paths_written: list[str] = []
    base = Path(output_dir) if output_dir else None
    for scen in contract.get("scenarios") or []:
        receipt = se.execute_scenario(
            scen, operator_id=operator_id)
        receipts.append(receipt)
        if base and write_per_scenario_receipts:
            sid = scen.get("scenario_id") or "unknown"
            out = (base / "stages"
                        / f"receipt_{sid}.json")
            try:
                paths_written.append(
                    se.write_scenario_receipt(
                        receipt, str(out)))
            except Exception:  # noqa: BLE001
                pass
    trace = ta.assemble_rehearsal_trace(
        contract, consent, receipts)
    recheck = gr.recheck_rehearsal_trace(trace)
    report = rr.bundle_rehearsal_report(
        contract, consent, trace, recheck)
    if base:
        try:
            paths_written.append(rc.write_rehearsal_contract(
                contract, str(base / "contracts"
                               / "rehearsal_contract.json")))
            paths_written.append(co.write_umbrella_consent(
                consent, str(base / "consents"
                              / "umbrella_consent.json")))
            paths_written.append(ta.write_rehearsal_trace(
                trace, str(base / "traces"
                            / "rehearsal_trace.json")))
            paths_written.append(gr.write_rehearsal_recheck(
                recheck, str(base / "recheck"
                              / "rehearsal_recheck.json")))
            paths_written.append(rr.write_rehearsal_report(
                report, str(base / "reports"
                             / "rehearsal_report.json")))
            paths_written.append(rr.write_rehearsal_markdown(
                rr.create_rehearsal_markdown(report),
                str(base / "reports"
                     / "rehearsal_report.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase": _PHASE,
        "status": "ok",
        "started_at": time.time(),
        "contract_id": contract.get("contract_id"),
        "consent_id": consent.get("consent_id"),
        "trace_id": trace.get("trace_id"),
        "recheck_id": recheck.get("recheck_id"),
        "report_id": report.get("report_id"),
        "binding": binding,
        "contract": contract,
        "consent": consent,
        "trace": trace,
        "recheck": recheck,
        "report": report,
        "paths_written": paths_written,
    }


def validate_phase39_runtime_output(
    output: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    if output.get("phase") != _PHASE:
        reasons.append(f"phase_mismatch:{output.get('phase')}")
    if output.get("status") != "ok":
        reasons.append(f"status_not_ok:{output.get('status')}")
    for f in ("contract", "consent", "trace", "recheck",
              "report", "binding"):
        if f not in output:
            reasons.append(f"missing_field:{f}")
    if output.get("status") == "ok":
        recheck = output.get("recheck") or {}
        if recheck.get("ok") is not True:
            reasons.append("recheck_not_ok")
    return {"ok": not reasons, "reasons": reasons}


def write_phase39_runtime_report(
    output: dict[str, Any],
    output_path: str,
) -> str:
    import json
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(output)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "run_phase39_rehearsal",
    "validate_phase39_runtime_output",
    "write_phase39_runtime_report",
]
