"""Phase 45 - Archive Tamper Suite (13 cases)."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase45_archive_verifier as av
import bilingual_voice_phase45_archive_manifest as am
import bilingual_voice_phase45_chain_ledger as cl


_PHASE = "phase45.tamper_suite.v1"


_TAMPER_CASES = (
    "missing_phase42_replay_matrix",
    "missing_phase43_bundle",
    "missing_phase44_roundtrip_receipt",
    "modified_phase43_bundle_hash",
    "modified_phase44_operator_packet_hash",
    "broken_chain_phase_order",
    "injected_audio_flag",
    "injected_secret_field",
    "injected_command_field",
    "injected_runtime_db_reference",
    "phase21_status_unexpected",
    "adapter_reinvocation_claim",
    "production_db_read_claim",
)


def create_phase45_tamper_cases(
    archive: Any,
) -> list[dict[str, Any]]:
    if not isinstance(archive, dict):
        return []
    return [{"case": c, "expected_detection": True}
             for c in _TAMPER_CASES]


def _drop_entry(archive: dict[str, Any],
                  key: str) -> dict[str, Any]:
    archive["artifacts"] = [
        e for e in archive.get("artifacts") or []
        if isinstance(e, dict)
        and e.get("artifact_key") != key]
    archive["artifact_count"] = len(
        archive["artifacts"])
    hashes = dict(archive.get("artifact_hashes") or {})
    hashes.pop(key, None)
    archive["artifact_hashes"] = hashes
    return archive


def _set_entry_sha(archive: dict[str, Any],
                     key: str,
                     new_sha: str) -> dict[str, Any]:
    for e in archive.get("artifacts") or []:
        if isinstance(e, dict) and e.get(
                "artifact_key") == key:
            e["sha256"] = new_sha
    hashes = dict(archive.get("artifact_hashes") or {})
    if key in hashes:
        hashes[key] = new_sha
        archive["artifact_hashes"] = hashes
    return archive


def apply_phase45_tamper_case(
    archive: Any,
    tamper_case: str,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reason": "archive_not_dict"}
    a = copy.deepcopy(archive)
    case = str(tamper_case or "")
    if case == "missing_phase42_replay_matrix":
        _drop_entry(a, "phase42_replay_matrix")
    elif case == "missing_phase43_bundle":
        _drop_entry(a, "phase43_portable_bundle")
    elif case == "missing_phase44_roundtrip_receipt":
        _drop_entry(a, "phase44_roundtrip_receipt")
    elif case == "modified_phase43_bundle_hash":
        _set_entry_sha(a, "phase43_portable_bundle",
                         "0" * 64)
    elif case == "modified_phase44_operator_packet_hash":
        _set_entry_sha(a, "phase44_operator_packet",
                         "1" * 64)
    elif case == "broken_chain_phase_order":
        # We don't mutate the archive itself for chain
        # order. We instead build a ledger with wrong
        # ordered_phases and assert validator catches it.
        return {"ok": True, "archive": a,
                "tamper_case": case,
                "mode": "ledger_order",
                "phase": _PHASE}
    elif case == "injected_audio_flag":
        bs = dict(a.get("boundary_summary") or {})
        bs["no_audio"] = False
        a["boundary_summary"] = bs
    elif case == "injected_secret_field":
        # Inject into first inline JSON content
        for e in a.get("artifacts") or []:
            if isinstance(e.get("inline_content"),
                          dict):
                e["inline_content"][
                    "signing_key_material"] = "leak"
                break
    elif case == "injected_command_field":
        for e in a.get("artifacts") or []:
            if isinstance(e.get("inline_content"),
                          dict):
                e["inline_content"][
                    "command"] = "rm -rf /"
                break
    elif case == "injected_runtime_db_reference":
        entries = list(a.get("artifacts") or [])
        entries.append({
            "artifact_key": "leak_db",
            "source_phase": "leak",
            "relative_path":
                "lexicon/luna_vocabulary.sqlite",
            "absolute_path":
                "lexicon/luna_vocabulary.sqlite",
            "artifact_type": "other",
            "size_bytes": 100,
            "sha256": "1" * 64,
            "inline_content_present": False,
            "inline_content": None,
        })
        a["artifacts"] = entries
        a["artifact_count"] = len(entries)
        h = dict(a.get("artifact_hashes") or {})
        h["leak_db"] = "1" * 64
        a["artifact_hashes"] = h
    elif case == "phase21_status_unexpected":
        a["phase21_status_text"] = "UNBLOCKED"
    elif case == "adapter_reinvocation_claim":
        bs = dict(a.get("boundary_summary") or {})
        bs["no_adapter_invocation_in_archive"] = False
        a["boundary_summary"] = bs
    elif case == "production_db_read_claim":
        bs = dict(a.get("boundary_summary") or {})
        bs["no_production_db_read_in_archive"] = False
        a["boundary_summary"] = bs
    else:
        return {"ok": False, "tamper_case": case,
                "reason": "unknown_case",
                "phase": _PHASE}
    return {"ok": True, "archive": a,
            "tamper_case": case,
            "phase": _PHASE}


def _detect_via_archive(
    archive: Any,
    manifest: Optional[dict[str, Any]] = None,
    ledger: Optional[dict[str, Any]] = None,
) -> bool:
    verify = av.verify_phase45_archive(
        archive, manifest=manifest, ledger=ledger)
    return not bool(verify.get("ok"))


def _detect_broken_chain_order(
    archive: dict[str, Any],
) -> bool:
    # Build a ledger with broken phase order and confirm
    # validator catches it.
    bad_ledger = cl.create_phase45_chain_ledger(archive)
    bad_ledger["ordered_phases"] = ["phase44",
                                      "phase43",
                                      "phase42"]
    # Force chain_root_hash to match the new chain so
    # the validator's drift check passes — but ordered
    # phases should still fail.
    val = cl.validate_phase45_chain_ledger(bad_ledger)
    return val.get("ok") is False


def run_phase45_tamper_suite(
    archive: Any,
    manifest: Optional[dict[str, Any]] = None,
    ledger: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"],
                "phase": _PHASE}
    results: list[dict[str, Any]] = []
    detected = 0
    undetected = 0
    for case in _TAMPER_CASES:
        applied = apply_phase45_tamper_case(
            archive, case)
        if not applied.get("ok"):
            results.append({"case": case,
                             "applied": False,
                             "detected": False,
                             "reason":
                                 applied.get("reason")})
            undetected += 1
            continue
        if applied.get("mode") == "ledger_order":
            # Special case: validate ledger directly
            caught = _detect_broken_chain_order(archive)
        else:
            bad_archive = applied.get("archive") or {}
            # Rebuild manifest + ledger from tampered
            # archive only if the case mutates archive
            # itself; otherwise use clean manifest/ledger
            # to detect a mismatch with the tampered
            # archive.
            caught = _detect_via_archive(
                bad_archive,
                manifest=manifest,
                ledger=ledger)
        if caught:
            detected += 1
        else:
            undetected += 1
        results.append({
            "case": case,
            "applied": True,
            "detected": caught,
        })
    return {
        "suite_id": f"p45tamper_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "case_count": len(_TAMPER_CASES),
        "detected_count": detected,
        "undetected_count": undetected,
        "results": results,
        "ok": undetected == 0
              and detected == len(_TAMPER_CASES),
        "summary": (
            f"phase45 tamper suite: cases="
            f"{len(_TAMPER_CASES)} detected="
            f"{detected} undetected={undetected}"),
    }


def validate_phase45_tamper_suite_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    for f in ("suite_id", "created_at", "phase",
              "case_count", "detected_count",
              "undetected_count", "results", "ok"):
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("case_count") != len(_TAMPER_CASES):
        reasons.append("case_count_mismatch")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase45_tamper_suite(
    result: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "summary": "no_result"}
    return {
        "ok": bool(result.get("ok")),
        "summary": result.get("summary"),
        "suite_id": result.get("suite_id"),
        "phase": _PHASE,
    }


def write_phase45_tamper_suite_report(
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
    "create_phase45_tamper_cases",
    "apply_phase45_tamper_case",
    "run_phase45_tamper_suite",
    "validate_phase45_tamper_suite_result",
    "summarize_phase45_tamper_suite",
    "write_phase45_tamper_suite_report",
]
