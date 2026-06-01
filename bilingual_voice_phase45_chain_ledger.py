"""Phase 45 - Chain-of-Trust Ledger.

Builds and verifies a ledger linking Phase 42 -> 43 -> 44
artifacts in strict order. All checks are read-only over the
archive's inline content.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase45.chain_ledger.v1"


_REQUIRED_LEDGER_FIELDS = (
    "ledger_id", "created_at", "phase",
    "archive_id", "ordered_phases",
    "chain_links",
    "phase42_summary", "phase43_summary",
    "phase44_summary",
    "baseline_invariant_summary",
    "phase21_status_summary",
    "boundary_summary",
    "chain_root_hash",
)


_REQUIRED_CHAIN_LINKS = (
    "phase42_to_phase43_bundle",
    "phase43_to_phase44_import",
    "phase44_import_to_roundtrip_receipt",
    "phase44_tamper_suite_to_operator_packet",
    "phase44_operator_packet_to_dashboard",
)


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _find(archive: dict[str, Any],
          key: str) -> dict[str, Any]:
    for e in archive.get("artifacts") or []:
        if isinstance(e, dict) and e.get(
                "artifact_key") == key:
            return e
    return {}


def _inline(archive: dict[str, Any],
            key: str) -> Any:
    return _find(archive, key).get("inline_content")


def _sha(archive: dict[str, Any], key: str) -> str:
    return str(_find(archive, key).get("sha256") or "")


def _phase42_summary(
    archive: dict[str, Any],
) -> dict[str, Any]:
    op = _inline(archive, "phase42_operator_packet") \
        or {}
    coh = _inline(archive, "phase42_coherence_audit") \
        or {}
    rmat = _inline(archive, "phase42_replay_matrix") or {}
    return {
        "operator_packet_audit_status":
            op.get("audit_status")
            if isinstance(op, dict) else None,
        "coherence_audit_ok":
            bool((coh or {}).get("ok"))
            if isinstance(coh, dict) else False,
        "replay_matrix_compatibility":
            (rmat or {}).get("compatibility_status")
            if isinstance(rmat, dict) else None,
        "operator_packet_sha":
            _sha(archive, "phase42_operator_packet"),
        "coherence_audit_sha":
            _sha(archive, "phase42_coherence_audit"),
        "replay_matrix_sha":
            _sha(archive, "phase42_replay_matrix"),
    }


def _phase43_summary(
    archive: dict[str, Any],
) -> dict[str, Any]:
    bundle = _inline(archive,
                       "phase43_portable_bundle") or {}
    manifest = _inline(archive,
                         "phase43_bundle_manifest") or {}
    fresh = _inline(archive,
                     "phase43_fresh_checkout_result") \
        or {}
    pop = _inline(archive, "phase43_operator_packet") \
        or {}
    return {
        "bundle_source_phase":
            (bundle or {}).get("source_phase")
            if isinstance(bundle, dict) else None,
        "manifest_source_phase":
            (manifest or {}).get("source_phase")
            if isinstance(manifest, dict) else None,
        "fresh_checkout_ok":
            bool((fresh or {}).get("ok"))
            if isinstance(fresh, dict) else False,
        "operator_packet_portability_status":
            (pop or {}).get("portability_status")
            if isinstance(pop, dict) else None,
        "portable_bundle_sha":
            _sha(archive, "phase43_portable_bundle"),
        "bundle_manifest_sha":
            _sha(archive, "phase43_bundle_manifest"),
        "operator_packet_sha":
            _sha(archive, "phase43_operator_packet"),
    }


def _phase44_summary(
    archive: dict[str, Any],
) -> dict[str, Any]:
    imp = _inline(archive, "phase44_imported_bundle") \
        or {}
    immf = _inline(archive, "phase44_import_manifest") \
        or {}
    fresh = _inline(archive,
                     "phase44_fresh_import_result") or {}
    tamper = _inline(archive, "phase44_tamper_suite") \
        or {}
    receipt = _inline(archive,
                       "phase44_roundtrip_receipt") or {}
    pop = _inline(archive, "phase44_operator_packet") \
        or {}
    return {
        "imported_count":
            int((imp or {}).get("imported_count") or 0)
            if isinstance(imp, dict) else 0,
        "import_manifest_source_bundle_id":
            (immf or {}).get("source_bundle_id")
            if isinstance(immf, dict) else None,
        "fresh_import_ok":
            bool((fresh or {}).get("ok"))
            if isinstance(fresh, dict) else False,
        "tamper_suite_ok":
            bool((tamper or {}).get("ok"))
            if isinstance(tamper, dict) else False,
        "tamper_detected_count":
            int((tamper or {}).get("detected_count")
                 or 0)
            if isinstance(tamper, dict) else 0,
        "roundtrip_status":
            (receipt or {}).get("import_status")
            if isinstance(receipt, dict) else None,
        "operator_packet_status":
            (pop or {}).get("phase44_status")
            if isinstance(pop, dict) else None,
        "imported_bundle_sha":
            _sha(archive, "phase44_imported_bundle"),
        "import_manifest_sha":
            _sha(archive, "phase44_import_manifest"),
        "tamper_suite_sha":
            _sha(archive, "phase44_tamper_suite"),
        "roundtrip_receipt_sha":
            _sha(archive, "phase44_roundtrip_receipt"),
        "operator_packet_sha":
            _sha(archive, "phase44_operator_packet"),
    }


def _baseline_summary(
    archive: dict[str, Any],
) -> dict[str, Any]:
    expected = (archive or {}).get(
        "production_baseline_expected") or {}
    p42op = _inline(archive,
                      "phase42_operator_packet") or {}
    p42_invariants = (p42op or {}).get(
        "production_baseline_summary") if isinstance(
        p42op, dict) else None
    return {
        "expected": dict(expected),
        "phase42_invariants":
            p42_invariants if isinstance(
                p42_invariants, dict) else None,
    }


def _phase21_summary(
    archive: dict[str, Any],
) -> dict[str, Any]:
    return {
        "archive": str(archive.get(
            "phase21_status_text") or "BLOCKED"),
        "phase43_bundle_phase21":
            (_inline(archive,
                       "phase43_portable_bundle") or {})
            .get("phase21_status_text")
            if isinstance(_inline(archive,
                                    "phase43_portable_bundle"),
                          dict) else None,
        "phase44_imported_phase21":
            (_inline(archive,
                       "phase44_imported_bundle") or {})
            .get("phase21_status_text")
            if isinstance(_inline(archive,
                                    "phase44_imported_bundle"),
                          dict) else None,
    }


def _build_chain_links(
    archive: dict[str, Any],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    # phase42 -> phase43 bundle: the Phase 43 bundle's
    # inline content lists Phase 42 artifacts at the
    # entry level. Cross-link by ensuring Phase 42
    # operator_packet sha is present in the archive AND
    # the Phase 43 bundle's source_phase claim is
    # phase42-compatible.
    p43b = _inline(archive, "phase43_portable_bundle") \
        or {}
    p43_source = p43b.get("source_phase") \
        if isinstance(p43b, dict) else None
    out["phase42_to_phase43_bundle"] = {
        "from": "phase42_operator_packet",
        "to": "phase43_portable_bundle",
        "from_sha": _sha(archive,
                          "phase42_operator_packet"),
        "to_sha": _sha(archive,
                        "phase43_portable_bundle"),
        "phase43_source_phase_claim": p43_source,
        "ok": (str(p43_source or "") == "phase42"
                and bool(_sha(archive,
                               "phase42_operator_packet"))
                and bool(_sha(archive,
                               "phase43_portable_bundle"))),
    }
    # phase43 -> phase44 import: import_manifest's
    # source_bundle_id should match the phase43 portable
    # bundle's bundle_id.
    p44m = _inline(archive, "phase44_import_manifest") \
        or {}
    p44_src_id = p44m.get("source_bundle_id") \
        if isinstance(p44m, dict) else None
    p43_id = p43b.get("bundle_id") \
        if isinstance(p43b, dict) else None
    out["phase43_to_phase44_import"] = {
        "from": "phase43_portable_bundle",
        "to": "phase44_import_manifest",
        "from_sha": _sha(archive,
                          "phase43_portable_bundle"),
        "to_sha": _sha(archive,
                        "phase44_import_manifest"),
        "phase43_bundle_id": p43_id,
        "phase44_source_bundle_id": p44_src_id,
        "ok": (bool(p43_id) and bool(p44_src_id)
                and p43_id == p44_src_id),
    }
    # phase44 import -> roundtrip receipt: receipt's
    # source_bundle_hash_summary.source_bundle_id should
    # match imported manifest's source_bundle_id.
    p44r = _inline(archive,
                     "phase44_roundtrip_receipt") or {}
    src_summary = (p44r or {}).get(
        "source_bundle_hash_summary") if isinstance(
        p44r, dict) else {}
    rcpt_source_bundle_id = (src_summary or {}).get(
        "source_bundle_id")
    out["phase44_import_to_roundtrip_receipt"] = {
        "from": "phase44_import_manifest",
        "to": "phase44_roundtrip_receipt",
        "from_sha":
            _sha(archive, "phase44_import_manifest"),
        "to_sha":
            _sha(archive, "phase44_roundtrip_receipt"),
        "import_manifest_source_bundle_id": p44_src_id,
        "receipt_source_bundle_id":
            rcpt_source_bundle_id,
        "ok": (bool(p44_src_id)
                and bool(rcpt_source_bundle_id)
                and p44_src_id == rcpt_source_bundle_id),
    }
    # phase44 tamper -> operator packet
    p44ts = _inline(archive, "phase44_tamper_suite") \
        or {}
    p44op = _inline(archive,
                      "phase44_operator_packet") or {}
    p44op_tamper = (p44op or {}).get(
        "tamper_suite_summary") if isinstance(
        p44op, dict) else {}
    out["phase44_tamper_suite_to_operator_packet"] = {
        "from": "phase44_tamper_suite",
        "to": "phase44_operator_packet",
        "from_sha":
            _sha(archive, "phase44_tamper_suite"),
        "to_sha":
            _sha(archive, "phase44_operator_packet"),
        "tamper_suite_detected":
            int((p44ts or {}).get("detected_count") or 0)
            if isinstance(p44ts, dict) else 0,
        "operator_packet_tamper_detected":
            int((p44op_tamper or {}).get(
                "detected_count") or 0)
            if isinstance(p44op_tamper, dict) else 0,
        "ok": (isinstance(p44ts, dict)
                and isinstance(p44op_tamper, dict)
                and (p44ts.get("detected_count")
                     == p44op_tamper.get(
                         "detected_count"))),
    }
    # phase44 operator packet -> dashboard
    p44dj = _inline(archive,
                      "phase44_status_dashboard_json") \
        or {}
    pkt_status = (p44op or {}).get("phase44_status") \
        if isinstance(p44op, dict) else None
    dash_status = (p44dj or {}).get("phase44_status") \
        if isinstance(p44dj, dict) else None
    out["phase44_operator_packet_to_dashboard"] = {
        "from": "phase44_operator_packet",
        "to": "phase44_status_dashboard_json",
        "from_sha":
            _sha(archive, "phase44_operator_packet"),
        "to_sha":
            _sha(archive,
                  "phase44_status_dashboard_json"),
        "operator_packet_status": pkt_status,
        "dashboard_status": dash_status,
        "ok": (bool(pkt_status) and bool(dash_status)
                and pkt_status == dash_status),
    }
    return out


def create_phase45_chain_ledger(
    archive: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ledger_id": "",
                "phase": _PHASE,
                "status": "refused",
                "reason": "archive_not_dict"}
    chain = _build_chain_links(archive)
    chain_root = _stable_hash(chain)
    return {
        "ledger_id":
            f"p45chain_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "archive_id": archive.get("archive_id", ""),
        "ordered_phases": ["phase42", "phase43",
                            "phase44"],
        "chain_links": chain,
        "phase42_summary": _phase42_summary(archive),
        "phase43_summary": _phase43_summary(archive),
        "phase44_summary": _phase44_summary(archive),
        "baseline_invariant_summary":
            _baseline_summary(archive),
        "phase21_status_summary":
            _phase21_summary(archive),
        "boundary_summary":
            dict(archive.get("boundary_summary") or {}),
        "chain_root_hash": chain_root,
        "notes": [
            "Ledger order is fixed: phase42 -> phase43 "
            "-> phase44.",
            "Each link cross-checks SHA + semantic id "
            "(bundle_id, source_bundle_id, etc.).",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase45_chain_ledger(
    ledger: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(ledger, dict):
        return {"ok": False,
                "reasons": ["ledger_not_dict"]}
    for f in _REQUIRED_LEDGER_FIELDS:
        if f not in ledger:
            reasons.append(f"missing_field:{f}")
    order = ledger.get("ordered_phases") or []
    if order != ["phase42", "phase43", "phase44"]:
        reasons.append(f"ordered_phases_wrong:{order}")
    chain = ledger.get("chain_links") or {}
    for must in _REQUIRED_CHAIN_LINKS:
        if must not in chain:
            reasons.append(f"missing_chain_link:{must}")
    expected = ledger.get("chain_root_hash") or ""
    rec = _stable_hash(chain)
    if expected != rec:
        reasons.append("chain_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase45_chain_links(
    ledger: Any,
    archive: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(ledger, dict):
        return {"ok": False,
                "reasons": ["ledger_not_dict"]}
    reasons: list[str] = []
    chain = ledger.get("chain_links") or {}
    for k, link in chain.items():
        if not isinstance(link, dict):
            reasons.append(f"link_not_dict:{k}")
            continue
        if link.get("ok") is not True:
            reasons.append(f"link_not_ok:{k}")
    # If archive supplied, also confirm chain integrity
    # holds against re-derived links.
    if isinstance(archive, dict):
        rederived = _build_chain_links(archive)
        if rederived != chain:
            reasons.append("chain_links_disagree_with_archive")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase45_chain_breaks(
    ledger: Any,
    archive: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    res = verify_phase45_chain_links(ledger,
                                       archive=archive)
    return {
        "broken": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def summarize_phase45_chain_ledger(
    ledger: Any,
) -> dict[str, Any]:
    if not isinstance(ledger, dict):
        return {"ok": False, "summary": "no_ledger"}
    chain = ledger.get("chain_links") or {}
    ok_count = sum(1 for v in chain.values()
                    if isinstance(v, dict)
                    and v.get("ok") is True)
    return {
        "ok": ok_count == len(_REQUIRED_CHAIN_LINKS),
        "summary": (
            f"phase45 chain: ok_links={ok_count}/"
            f"{len(_REQUIRED_CHAIN_LINKS)} "
            f"phase21="
            f"{(ledger.get('phase21_status_summary') or {}).get('archive')}"),
        "ledger_id": ledger.get("ledger_id"),
        "phase": _PHASE,
    }


def write_phase45_chain_ledger(
    ledger: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(ledger)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase45_chain_ledger_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase45_chain_ledger(report, output_path)


__all__ = [
    "create_phase45_chain_ledger",
    "validate_phase45_chain_ledger",
    "verify_phase45_chain_links",
    "detect_phase45_chain_breaks",
    "summarize_phase45_chain_ledger",
    "write_phase45_chain_ledger",
    "write_phase45_chain_ledger_report",
]
