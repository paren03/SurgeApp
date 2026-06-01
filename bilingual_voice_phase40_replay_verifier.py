"""Phase 40 - Replay Verifier.

Combines contract + loader + trace replay + drift detection into
a single verification result.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase40_replay_contract as rc
import bilingual_voice_phase40_replay_loader as rl
import bilingual_voice_phase40_trace_replayer as tr
import bilingual_voice_phase40_drift_detector as dd


_PHASE = "phase40.replay_verifier.v1"


_REQUIRED_RESULT_FIELDS = (
    "verification_id", "created_at", "phase",
    "contract", "artifacts_summary",
    "trace_replay", "drift",
    "status",
    "baseline_observed",
    "phase21_status_text",
    "rehearsal_dry_run_only",
)


def create_phase40_verification_result(
    contract: dict[str, Any],
    artifacts: dict[str, Any],
    trace_replay: dict[str, Any],
    drift: dict[str, Any],
) -> dict[str, Any]:
    art_sum = rl.summarize_loaded_replay_artifacts(artifacts)
    bld = next((d for d in (drift.get("results") or [])
                if d.get("category") == "baseline_drift"), {})
    p21 = next((d for d in (drift.get("results") or [])
                if d.get("category") == "phase21_status_drift"),
                {})
    fail = int(drift.get("fail_count") or 0)
    warn = int(drift.get("warn_count") or 0)
    trace_ok = bool(trace_replay.get("ok"))
    art_ok = bool(artifacts.get("ok"))
    status = "ok"
    if not art_ok or not trace_ok or fail > 0:
        status = "drift_detected"
    if warn > 0 and status == "ok":
        status = "ok_with_warnings"
    return {
        "verification_id": f"vfy_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "contract": contract or {},
        "artifacts_summary": art_sum,
        "trace_replay": trace_replay or {},
        "drift": drift or {},
        "status": status,
        "fail_count": fail,
        "warn_count": warn,
        "baseline_observed": bld.get("observed") or {},
        "baseline_drifts": bld.get("drifts") or [],
        "phase21_status_text":
            p21.get("phase21_status_text", "BLOCKED"),
        "phase21_drifted":
            p21.get("drifted") is True,
        "rehearsal_dry_run_only": True,
        "notes": [
            "Verification is read-only.",
            "No adapters were invoked.",
            "No subprocess / network / audio activity.",
        ],
    }


def verify_phase40_replay_from_artifacts(
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    contract = rc.create_phase40_replay_contract(
        replay_id=f"replay_{int(time.time())}")
    trace_replay = tr.replay_phase39_trace(artifacts)
    drift = dd.detect_phase40_drift(artifacts)
    return create_phase40_verification_result(
        contract, artifacts, trace_replay, drift)


def verify_phase40_replay(
    base_dir: Optional[str] = None,
) -> dict[str, Any]:
    artifacts = rl.load_phase39_replay_artifacts(
        base_dir=base_dir)
    return verify_phase40_replay_from_artifacts(artifacts)


def validate_phase40_verification_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    for f in _REQUIRED_RESULT_FIELDS:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    status = str(result.get("status") or "")
    if status not in ("ok", "ok_with_warnings",
                       "drift_detected"):
        reasons.append(f"invalid_status:{status}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase40_verification_result(
    result: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "summary": "no_result"}
    return {
        "ok": str(result.get("status") or "") in
            ("ok", "ok_with_warnings"),
        "summary": (
            f"phase40 verify: status="
            f"{result.get('status')} "
            f"fail={result.get('fail_count')} "
            f"warn={result.get('warn_count')} "
            f"trace_ok="
            f"{(result.get('trace_replay') or {}).get('ok')}"),
        "verification_id": result.get("verification_id"),
        "phase": _PHASE,
    }


def write_phase40_verification_result(
    result: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(result)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "verify_phase40_replay",
    "verify_phase40_replay_from_artifacts",
    "create_phase40_verification_result",
    "validate_phase40_verification_result",
    "summarize_phase40_verification_result",
    "write_phase40_verification_result",
]
