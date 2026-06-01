"""Phase 44 - Tamper Suite.

Creates 8 controlled tamper cases and verifies the fresh-import
verifier catches each one. Works on deep copies; original
imported bundle is never mutated.
"""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase44_fresh_import_verifier as fiv


_PHASE = "phase44.tamper_suite.v1"


_TAMPER_CASES = (
    "missing_replay_matrix",
    "modified_operator_packet_hash",
    "injected_audio_flag",
    "injected_secret_field",
    "injected_command_field",
    "injected_runtime_db_reference",
    "phase21_status_unexpected",
    "adapter_reinvocation_claim",
)


def create_phase44_tamper_cases(
    imported_bundle: Any,
) -> list[dict[str, Any]]:
    if not isinstance(imported_bundle, dict):
        return []
    return [{"case": c,
              "expected_detection": True}
             for c in _TAMPER_CASES]


def _find_entry(bundle: dict[str, Any],
                  key: str) -> Optional[dict[str, Any]]:
    for e in bundle.get("entries") or []:
        if isinstance(e, dict) \
                and e.get("artifact_key") == key:
            return e
    return None


def _mutate_inline_json(bundle: dict[str, Any],
                          key: str,
                          mutator) -> bool:
    e = _find_entry(bundle, key)
    if not e:
        return False
    p = e.get("imported_path")
    if not isinstance(p, str):
        return False
    path = Path(p)
    if not path.exists() or not path.is_file():
        return False
    try:
        if path.stat().st_size > 4 * 1024 * 1024:
            return False
        obj = json.loads(path.read_text(
            encoding="utf-8", errors="ignore"))
    except Exception:  # noqa: BLE001
        return False
    if not isinstance(obj, dict):
        return False
    mutator(obj)
    try:
        path.write_text(json.dumps(
            obj, ensure_ascii=False, indent=2,
            default=str), encoding="utf-8")
    except Exception:  # noqa: BLE001
        return False
    return True


def apply_phase44_tamper_case(
    imported_bundle: Any,
    tamper_case: str,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reason": "imported_not_dict"}
    bundle = copy.deepcopy(imported_bundle)
    case = str(tamper_case or "")
    if case == "missing_replay_matrix":
        # Inline content of portable_bundle drops replay
        # matrix from its artifacts list. We mutate the
        # portable_bundle's on-disk JSON.
        ok = _mutate_inline_json(
            bundle, "portable_bundle",
            lambda obj: obj.update({"artifacts": [
                a for a in (obj.get("artifacts") or [])
                if isinstance(a, dict)
                and a.get("artifact_key")
                != "phase42_replay_matrix"]}))
        return {"ok": ok, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "modified_operator_packet_hash":
        # Mutate the imported_sha256 on the operator
        # packet entry to drift from source.
        e = _find_entry(bundle, "source_operator_packet")
        if e:
            e["imported_sha256"] = "0" * 64
            return {"ok": True, "bundle": bundle,
                    "tamper_case": case,
                    "phase": _PHASE}
        return {"ok": False, "tamper_case": case,
                "reason": "entry_not_found",
                "phase": _PHASE}
    if case == "injected_audio_flag":
        bs = dict(bundle.get("boundary_summary") or {})
        bs["no_audio"] = False
        bundle["boundary_summary"] = bs
        return {"ok": True, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "injected_secret_field":
        # Inject a secret field into the portable_bundle
        # JSON's top level.
        ok = _mutate_inline_json(
            bundle, "portable_bundle",
            lambda obj: obj.update({
                "signing_key_material": "leak"}))
        return {"ok": ok, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "injected_command_field":
        ok = _mutate_inline_json(
            bundle, "portable_bundle",
            lambda obj: obj.update({
                "command": "rm -rf /"}))
        return {"ok": ok, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "injected_runtime_db_reference":
        # Inject a fake runtime-DB-shaped entry into the
        # bundle's entries list.
        entries = list(bundle.get("entries") or [])
        entries.append({
            "artifact_key": "leak_db",
            "source_path":
                "lexicon/luna_vocabulary.sqlite",
            "imported_path":
                "lexicon/luna_vocabulary.sqlite",
            "source_sha256": "1" * 64,
            "imported_sha256": "1" * 64,
            "size_bytes": 100,
            "sha_matches": True,
        })
        bundle["entries"] = entries
        return {"ok": True, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "phase21_status_unexpected":
        bundle["phase21_status_text"] = "UNBLOCKED"
        return {"ok": True, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    if case == "adapter_reinvocation_claim":
        bs = dict(bundle.get("boundary_summary") or {})
        bs["no_adapter_invocation_on_import"] = False
        bundle["boundary_summary"] = bs
        return {"ok": True, "bundle": bundle,
                "tamper_case": case,
                "phase": _PHASE}
    return {"ok": False, "tamper_case": case,
            "reason": "unknown_case",
            "phase": _PHASE}


def run_phase44_tamper_suite(
    imported_bundle: Any,
    import_manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"],
                "phase": _PHASE}
    results: list[dict[str, Any]] = []
    detected = 0
    undetected = 0
    for case in _TAMPER_CASES:
        applied = apply_phase44_tamper_case(
            imported_bundle, case)
        if not applied.get("ok"):
            results.append({
                "case": case,
                "applied": False,
                "reason": applied.get("reason"),
                "detected": False,
            })
            undetected += 1
            continue
        bad_bundle = applied.get("bundle") or {}
        fresh = fiv.verify_phase44_imported_bundle_fresh(
            bad_bundle, import_manifest=import_manifest)
        # Re-check hashes against on-disk for the
        # JSON-mutated cases
        is_caught = not bool(fresh.get("ok"))
        if is_caught:
            detected += 1
        else:
            undetected += 1
        results.append({
            "case": case,
            "applied": True,
            "fresh_ok": bool(fresh.get("ok")),
            "detected": is_caught,
            "fresh_summary": fresh.get("summary"),
        })
    # Restore on-disk artifacts that we mutated so the
    # original imported bundle stays clean for subsequent
    # checks.
    _restore_on_disk_artifacts(imported_bundle)
    return {
        "suite_id":
            f"p44tamper_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "case_count": len(_TAMPER_CASES),
        "detected_count": detected,
        "undetected_count": undetected,
        "results": results,
        "ok": undetected == 0
              and detected == len(_TAMPER_CASES),
        "summary": (
            f"phase44 tamper suite: cases="
            f"{len(_TAMPER_CASES)} detected="
            f"{detected} undetected={undetected}"),
    }


def _restore_on_disk_artifacts(
    imported_bundle: dict[str, Any],
) -> None:
    """Some tamper cases mutate JSON files on disk in the
    fresh-checkout workspace. Recopy from source so the
    workspace returns to a clean state."""
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        sp = e.get("source_path")
        ip = e.get("imported_path")
        if not (isinstance(sp, str)
                and isinstance(ip, str)):
            continue
        try:
            src = Path(sp)
            dst = Path(ip)
            if src.exists() and src.is_file() \
                    and dst.parent.exists():
                dst.write_bytes(src.read_bytes())
        except Exception:  # noqa: BLE001
            pass


def validate_phase44_tamper_suite_result(
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


def summarize_phase44_tamper_suite(
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


def write_phase44_tamper_suite_report(
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
    "create_phase44_tamper_cases",
    "apply_phase44_tamper_case",
    "run_phase44_tamper_suite",
    "validate_phase44_tamper_suite_result",
    "summarize_phase44_tamper_suite",
    "write_phase44_tamper_suite_report",
]
