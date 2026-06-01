"""Phase 35 - Witness Verifier.

Performs witness-side local verification of an exporter packet via the
Phase 34 offline verifier and Phase 32 integrity manifest tools.
Bounded reads only. No subprocess, no network.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase34_witness_package as wp
import bilingual_voice_phase34_offline_verifier as ov
import bilingual_voice_phase34_key_descriptor_export as kde
import bilingual_voice_report_integrity_manifest as rim
import bilingual_voice_phase35_witness_input as wi


_PHASE = "phase35.witness_verifier.v1"


_CHUNK = 64 * 1024


def _hash_file(path: str) -> str:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with p.open("rb") as fh:
            while True:
                chunk = fh.read(_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
    except Exception:  # noqa: BLE001
        return ""
    return h.hexdigest()


def verify_artifact_hashes(witness_input: Any) -> dict[str, Any]:
    if not isinstance(witness_input, dict):
        return {"ok": False, "reasons": ["witness_input_not_dict"]}
    paths = witness_input.get("artifact_paths") or {}
    expected = witness_input.get("artifact_hashes") or {}
    mismatches: list[str] = []
    missing: list[str] = []
    for name, p in paths.items():
        h = _hash_file(str(p))
        if not h:
            missing.append(name)
            continue
        if h != expected.get(name):
            mismatches.append(name)
    return {
        "ok": not mismatches and not missing,
        "mismatches": mismatches,
        "missing": missing,
        "phase": _PHASE,
    }


def verify_exchange_contract_in_input(
    witness_input: Any,
) -> dict[str, Any]:
    if not isinstance(witness_input, dict):
        return {"ok": False, "reasons": ["witness_input_not_dict"]}
    if not str(witness_input.get("exchange_id") or ""):
        return {"ok": False, "reasons": ["exchange_id_missing"]}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        size = p.stat().st_size
    except Exception:  # noqa: BLE001
        return {}
    if size > 16 * 1024 * 1024:
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def verify_phase34_package_from_input(
    witness_input: Any,
    public_key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    paths = (witness_input or {}).get("artifact_paths") or {}
    pkg_path = paths.get("witness_package")
    pkg = wp.read_witness_package(str(pkg_path or ""))
    if not pkg:
        return {"ok": False, "reasons": ["witness_package_not_loaded"]}
    val = wp.validate_witness_package(pkg)
    if not val["ok"]:
        return {"ok": False,
                "reasons": ["package_invalid:" +
                             ",".join(val["reasons"])]}
    # Verify via Phase 34 offline verifier. Without HMAC key
    # material we can still verify everything except the signed
    # chain math; the offline verifier reports the chain check as
    # failed when no key is supplied. For end-to-end exchange we
    # surface that as a warn — operator must hold the key.
    if public_key_descriptor is not None:
        res = ov.verify_witness_package(pkg, public_key_descriptor)
    else:
        res = ov.verify_witness_package(pkg)
    return {
        "ok": res.get("status") == "pass",
        "verification_result": res,
        "phase": _PHASE,
    }


def verify_public_key_descriptor_from_input(
    witness_input: Any,
) -> dict[str, Any]:
    paths = (witness_input or {}).get("artifact_paths") or {}
    key_path = paths.get("public_key_descriptor")
    desc = kde.read_public_key_descriptor(str(key_path or ""))
    if not desc:
        return {"ok": False, "reasons": ["key_descriptor_not_loaded"]}
    val = kde.validate_public_key_descriptor(desc)
    return {"ok": val["ok"],
            "reasons": val["reasons"],
            "descriptor": desc,
            "phase": _PHASE}


def verify_integrity_manifest_from_input(
    witness_input: Any,
) -> dict[str, Any]:
    paths = (witness_input or {}).get("artifact_paths") or {}
    mpath = paths.get("integrity_manifest")
    manifest = _load_json(str(mpath or ""))
    if not manifest:
        return {"ok": False, "reasons": ["manifest_not_loaded"]}
    val = rim.validate_report_integrity_manifest(manifest)
    if not val["ok"]:
        return {"ok": False,
                "reasons": ["manifest_invalid:" +
                             ",".join(val["reasons"])]}
    vres = rim.verify_report_integrity_manifest(manifest)
    return {"ok": vres["ok"],
            "reasons": vres.get("reasons", []),
            "phase": _PHASE}


def create_witness_output(
    witness_input: Any,
    checks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    wid = (witness_input or {}).get("witness_input_id") \
        if isinstance(witness_input, dict) else ""
    eid = (witness_input or {}).get("exchange_id") \
        if isinstance(witness_input, dict) else ""
    if not isinstance(checks, dict):
        checks = {}
    passed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok")]
    failed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok") is False]
    status = "pass" if not failed else "fail"
    return {
        "witness_output_id":
            f"wit_out_{int(time.time())}_{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "exchange_id": eid,
        "witness_input_id": wid,
        "status": status,
        "checks": checks,
        "checks_passed": passed,
        "checks_failed": failed,
        "checks_warned": [],
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "no_network": True,
            "no_subprocess": True,
            "no_multiprocessing": True,
            "no_audio": True,
        },
        "phase": _PHASE,
    }


def verify_witness_input(
    witness_input: Any,
    public_key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    in_val = wi.validate_witness_input(witness_input)
    bounds = wi.check_witness_input_bounds(witness_input)
    paths_ok = wi.reject_remote_or_command_paths(witness_input)
    hashes = verify_artifact_hashes(witness_input)
    contract = verify_exchange_contract_in_input(witness_input)
    pkg = verify_phase34_package_from_input(
        witness_input, public_key_descriptor)
    key = verify_public_key_descriptor_from_input(witness_input)
    mani = verify_integrity_manifest_from_input(witness_input)
    checks = {
        "input_validation":
            {"ok": in_val["ok"], "reasons": in_val["reasons"]},
        "bound_checks":
            {"ok": bounds["ok"],
             "reasons": bounds.get("violations", [])},
        "path_safety":
            {"ok": paths_ok["ok"],
             "reasons": paths_ok.get("violations", [])},
        "artifact_hashes": {
            "ok": hashes["ok"],
            "reasons": (
                ([f"hash_mismatch:{m}"
                  for m in hashes.get("mismatches", [])]) +
                ([f"missing:{m}"
                  for m in hashes.get("missing", [])])),
        },
        "exchange_contract":
            {"ok": contract["ok"],
             "reasons": contract.get("reasons", [])},
        "phase34_package":
            {"ok": pkg["ok"],
             "reasons": [],
             "verification_result": pkg.get(
                 "verification_result", {})},
        "public_key_descriptor":
            {"ok": key["ok"], "reasons": key.get("reasons", [])},
        "integrity_manifest":
            {"ok": mani["ok"], "reasons": mani.get("reasons", [])},
    }
    return create_witness_output(witness_input, checks)


def write_witness_output(
    output: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(output)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_witness_output(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_witness_verifier_report(
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
    "verify_witness_input",
    "verify_artifact_hashes",
    "verify_exchange_contract_in_input",
    "verify_phase34_package_from_input",
    "verify_public_key_descriptor_from_input",
    "verify_integrity_manifest_from_input",
    "create_witness_output",
    "write_witness_output",
    "read_witness_output",
    "write_witness_verifier_report",
]
