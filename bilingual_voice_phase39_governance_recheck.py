"""Phase 39 - Rehearsal Governance Recheck.

Re-applies Phase 30/31/33/37 allowlist + boundary checks to the
assembled rehearsal trace. Confirms no scenario receipt names a
disallowed adapter and no scenario produced audio / TTS / etc.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase37_adapter_interface as p37i
import bilingual_voice_phase37_governance_recheck as p37gov


_PHASE = "phase39.governance_recheck.v1"


_REQUIRED_RECHECK_FIELDS = (
    "recheck_id", "created_at", "phase",
    "trace_id", "adapter_allowlist",
    "all_receipts_metadata_only",
    "all_receipts_within_allowlist",
    "secret_leakage_ok",
    "summary",
)


_BANNED_RECEIPT_FIELDS = (
    "produced_audio", "invoked_tts",
    "used_subprocess", "used_network", "wrote_files",
)


def _adapter_allowed(name: Any) -> bool:
    return name in p37i.ALLOWED_ADAPTER_TYPES or \
        name in (None, "", "none")


def recheck_rehearsal_trace(
    trace: Any,
) -> dict[str, Any]:
    if not isinstance(trace, dict):
        return {
            "recheck_id": f"rrchk_{int(time.time())}",
            "created_at": time.time(),
            "phase": _PHASE,
            "ok": False,
            "reasons": ["trace_not_dict"],
        }
    receipts = trace.get("receipts") or []
    if not isinstance(receipts, list):
        receipts = []
    bad_adapter: list[str] = []
    bad_runtime: list[str] = []
    for r in receipts:
        if not isinstance(r, dict):
            continue
        name = r.get("selected_adapter_name")
        if not _adapter_allowed(name):
            bad_adapter.append(
                f"{r.get('scenario_id')}:{name}")
        for k in _BANNED_RECEIPT_FIELDS:
            if r.get(k) is True:
                bad_runtime.append(
                    f"{r.get('scenario_id')}:{k}")
        if r.get("no_runtime_leak") is False:
            for x in r.get("runtime_leak_details") or []:
                bad_runtime.append(
                    f"{r.get('scenario_id')}:{x}")
    # Delegate to Phase 37 governance recheck for the
    # canonical metadata-only check
    p37_metadata_only = p37gov.verify_phase37_metadata_only_results(
        receipts)
    p37_secret_leak = p37gov.verify_phase37_no_secret_leakage(
        receipts)
    # Allowlist verification via Phase 37 helper
    p37_allowed = p37gov.verify_phase37_allowed_adapters_only([
        {"adapter_name": r.get("selected_adapter_name")}
        for r in receipts
        if isinstance(r, dict)
        and r.get("selected_adapter_name")
    ])
    ok = (not bad_adapter and not bad_runtime
          and p37_metadata_only.get("ok") is True
          and p37_secret_leak.get("ok") is True
          and p37_allowed.get("ok") is True)
    return {
        "recheck_id": f"rrchk_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "trace_id": trace.get("trace_id", ""),
        "adapter_allowlist": list(
            p37i.ALLOWED_ADAPTER_TYPES),
        "all_receipts_within_allowlist":
            not bad_adapter and p37_allowed.get("ok") is True,
        "all_receipts_metadata_only":
            not bad_runtime
            and p37_metadata_only.get("ok") is True,
        "secret_leakage_ok":
            p37_secret_leak.get("ok") is True,
        "bad_adapter_entries": bad_adapter,
        "bad_runtime_entries": bad_runtime,
        "phase37_metadata_only_detail": p37_metadata_only,
        "phase37_secret_leakage_detail": p37_secret_leak,
        "phase37_allowed_adapters_detail": p37_allowed,
        "summary": (
            f"phase39 recheck: receipts={len(receipts)} "
            f"allowlist_ok={not bad_adapter} "
            f"metadata_only_ok={not bad_runtime} "
            f"secret_ok={p37_secret_leak.get('ok')}"),
        "ok": ok,
        "reasons": [],
    }


def validate_rehearsal_recheck(
    recheck: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(recheck, dict):
        return {"ok": False, "reasons": ["recheck_not_dict"]}
    for f in _REQUIRED_RECHECK_FIELDS:
        if f not in recheck:
            reasons.append(f"missing_field:{f}")
    allowlist = recheck.get("adapter_allowlist") or []
    if len(allowlist) != 4:
        reasons.append(
            f"adapter_allowlist_not_four:{len(allowlist)}")
    return {"ok": not reasons, "reasons": reasons}


def write_rehearsal_recheck(
    recheck: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(recheck)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "recheck_rehearsal_trace",
    "validate_rehearsal_recheck",
    "write_rehearsal_recheck",
]
