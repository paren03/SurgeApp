"""Phase 36 - Handoff Verifier.

Verifies Phase 32/33 signed evidence using a local handoff envelope.
Unsealed key material stays in memory only. Verification result never
contains secret material.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase36_key_handoff_envelope as henv
import bilingual_voice_phase36_secret_boundary as sb
import bilingual_voice_audit_chain_signer as acs
import bilingual_voice_phase34_offline_verifier as p34v
import bilingual_voice_phase35_exporter_packet as p35ep
import bilingual_voice_phase35_witness_input as p35wi
import bilingual_voice_phase35_witness_verifier as p35wv


_PHASE = "phase36.handoff_verifier.v1"


def _strip_secret_from_result(result: Any) -> dict[str, Any]:
    """Ensure verification result carries no secret material."""
    if not isinstance(result, dict):
        return {}
    hits = sb.scan_object_for_secret_fields(result)
    if not hits:
        return result
    # Walk & remove
    def _strip(o: Any) -> Any:
        if isinstance(o, dict):
            return {k: _strip(v) for k, v in o.items()
                    if str(k).lower() not in
                    sb._SECRET_FIELDS}  # noqa: SLF001
        if isinstance(o, list):
            return [_strip(v) for v in o]
        return o

    return _strip(result)


def create_handoff_verification_result(
    checks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(checks, dict):
        checks = {}
    passed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok")]
    failed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok") is False]
    return {
        "created_at": time.time(),
        "status": "pass" if not failed else "fail",
        "checks": checks,
        "checks_passed": passed,
        "checks_failed": failed,
        "checks_warned": [],
        "phase": _PHASE,
    }


def verify_with_handoff_envelope(
    signed_evidence: Any,
    sealed_envelope: Any,
    consent_marker: str = "",
) -> dict[str, Any]:
    unseal = henv.unseal_key_handoff_envelope(
        sealed_envelope, consent_marker)
    if not unseal.get("ok"):
        return create_handoff_verification_result({
            "unseal": {"ok": False,
                        "reasons": unseal.get("reasons", [])},
        })
    key = unseal["key_descriptor"]
    if not isinstance(signed_evidence, dict):
        return create_handoff_verification_result({
            "unseal": {"ok": True, "reasons": []},
            "signed_evidence_structural":
                {"ok": False,
                 "reasons": ["signed_evidence_not_dict"]},
        })
    chain = signed_evidence.get("signed_audit_chain") or []
    if not isinstance(chain, list) or not chain:
        return create_handoff_verification_result({
            "unseal": {"ok": True, "reasons": []},
            "signed_evidence_structural":
                {"ok": False,
                 "reasons": ["signed_audit_chain_missing"]},
        })
    chain_check = acs.verify_signed_audit_chain(chain, key)
    res = create_handoff_verification_result({
        "unseal": {"ok": True, "reasons": []},
        "signed_evidence_chain": {
            "ok": bool(chain_check.get("ok")),
            "reasons": chain_check.get("reasons", []),
        },
    })
    # Defensive: secret stripped from result before returning
    return _strip_secret_from_result(res)


def verify_witness_package_with_handoff(
    package: Any,
    sealed_envelope: Any,
    consent_marker: str = "",
) -> dict[str, Any]:
    unseal = henv.unseal_key_handoff_envelope(
        sealed_envelope, consent_marker)
    if not unseal.get("ok"):
        return create_handoff_verification_result({
            "unseal": {"ok": False,
                        "reasons": unseal.get("reasons", [])},
        })
    key = unseal["key_descriptor"]
    res = p34v.verify_witness_package(package, key)
    checks = res.get("checks") if isinstance(res, dict) else {}
    final = create_handoff_verification_result(checks or {})
    final["status"] = res.get("status") if isinstance(res, dict) \
        else "fail"
    final["checks_passed"] = res.get("checks_passed", []) \
        if isinstance(res, dict) else []
    final["checks_failed"] = res.get("checks_failed", []) \
        if isinstance(res, dict) else []
    return _strip_secret_from_result(final)


def verify_exchange_packet_with_handoff(
    exporter_packet: Any,
    sealed_envelope: Any,
    consent_marker: str = "",
) -> dict[str, Any]:
    unseal = henv.unseal_key_handoff_envelope(
        sealed_envelope, consent_marker)
    if not unseal.get("ok"):
        return create_handoff_verification_result({
            "unseal": {"ok": False,
                        "reasons": unseal.get("reasons", [])},
        })
    key = unseal["key_descriptor"]
    pkt_val = p35ep.validate_exporter_packet(exporter_packet)
    if not pkt_val["ok"]:
        return create_handoff_verification_result({
            "unseal": {"ok": True, "reasons": []},
            "exporter_packet_validation":
                {"ok": False,
                 "reasons": pkt_val["reasons"]},
        })
    win = p35wi.create_witness_input(exporter_packet)
    out = p35wv.verify_witness_input(win, key)
    final = create_handoff_verification_result(
        out.get("checks") or {})
    final["status"] = out.get("status") or final.get("status")
    final["checks_passed"] = out.get("checks_passed", [])
    final["checks_failed"] = out.get("checks_failed", [])
    return _strip_secret_from_result(final)


def write_handoff_verification_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    if not isinstance(report, dict):
        raise ValueError("report_not_dict")
    hits = sb.scan_object_for_secret_fields(report)
    if hits:
        raise ValueError(
            "refuse_to_write_report_with_secret_fields:" +
            ",".join(hits))
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "verify_with_handoff_envelope",
    "verify_witness_package_with_handoff",
    "verify_exchange_packet_with_handoff",
    "create_handoff_verification_result",
    "write_handoff_verification_report",
]
