"""Phase 45 - Archive Verifier (read-only)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase45_archive_manifest as bm
import bilingual_voice_phase45_chain_ledger as cl


_PHASE = "phase45.archive_verifier.v1"


_REQUIRED_RESULT_FIELDS = (
    "verification_id", "created_at", "phase",
    "presence_check", "hash_check",
    "chain_integrity_check", "boundary_check",
    "phase21_check", "no_runtime_state_check",
    "ok", "summary",
)


_REQUIRED_KEYS = (
    "phase42_audit_contract",
    "phase42_replay_matrix",
    "phase42_operator_packet",
    "phase43_portable_bundle",
    "phase43_bundle_manifest",
    "phase43_operator_packet",
    "phase44_imported_bundle",
    "phase44_import_manifest",
    "phase44_roundtrip_receipt",
    "phase44_operator_packet",
)


_BANNED_INLINE_KEYS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


_RUNTIME_DB_TOKENS = (".sqlite", ".sqlite3", ".db")


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def verify_phase45_artifact_presence(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    entries = archive.get("artifacts") or []
    present = {e.get("artifact_key") for e in entries
                if isinstance(e, dict)}
    missing = [k for k in _REQUIRED_KEYS
                if k not in present]
    return {"ok": not missing,
            "missing": missing,
            "present_count": len(present),
            "phase": _PHASE}


def verify_phase45_artifact_hashes(
    archive: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    mismatches: list[str] = []
    declared = archive.get("artifact_hashes") or {}
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if not isinstance(sha, str) or len(sha) != 64:
            mismatches.append(f"bad_sha256:{k}")
            continue
        if declared.get(k) != sha:
            mismatches.append(
                f"declared_vs_entry:{k}")
        inline = e.get("inline_content")
        if inline is not None and k:
            # Stability check for inline JSON
            inline_hash = _stable_hash(inline)
            if e.get("inline_content_hash") and \
                    e["inline_content_hash"] \
                    != inline_hash:
                mismatches.append(
                    f"inline_content_hash_drift:{k}")
    if isinstance(manifest, dict):
        mres = bm.verify_phase45_archive_manifest(
            archive, manifest)
        if not mres.get("ok"):
            mismatches.extend(
                "manifest:" + r
                for r in mres.get("reasons", []))
    return {"ok": not mismatches,
            "mismatches": mismatches,
            "phase": _PHASE}


def verify_phase45_chain_integrity(
    archive: Any,
    ledger: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    if ledger is None:
        ledger = cl.create_phase45_chain_ledger(archive)
    lval = cl.validate_phase45_chain_ledger(ledger)
    if not lval.get("ok"):
        return {"ok": False,
                "reasons": ["ledger_invalid:"
                            + ",".join(
                                lval.get("reasons",
                                          []))],
                "phase": _PHASE}
    cres = cl.verify_phase45_chain_links(ledger,
                                            archive=archive)
    return {"ok": cres.get("ok") is True,
            "reasons": cres.get("reasons", []),
            "phase": _PHASE}


def verify_phase45_boundary_claims(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    reasons: list[str] = []
    bs = archive.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_in_archive",
              "no_production_db_read_in_archive"):
        if bs.get(k) is not True:
            reasons.append(f"boundary_false:{k}")
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        rp = str(e.get("relative_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if rp.endswith(tok):
                reasons.append(
                    f"runtime_db_in_archive:"
                    f"{e.get('artifact_key')}")
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            for bk in _BANNED_INLINE_KEYS:
                if bk in inline and inline.get(bk) \
                        not in (None, "", False, [],
                                 {}):
                    reasons.append(
                        f"banned_inline_field:"
                        f"{e.get('artifact_key')}:{bk}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase45_phase21_claim(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    txt = str(archive.get(
        "phase21_status_text") or "")
    if txt not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        return {"ok": False,
                "reasons":
                    [f"phase21_status_unexpected:{txt}"],
                "phase": _PHASE}
    return {"ok": True, "phase21_status_text": txt,
            "phase": _PHASE}


def verify_phase45_no_runtime_state_dependency(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    bs = archive.get("boundary_summary") or {}
    if bs.get(
            "no_production_db_read_in_archive") \
            is not True:
        return {"ok": False,
                "reasons":
                    ["no_production_db_read_must_be_true"],
                "phase": _PHASE}
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        for path_key in ("relative_path",
                          "absolute_path"):
            v = str(e.get(path_key) or "").lower()
            for tok in _RUNTIME_DB_TOKENS:
                if v.endswith(tok):
                    return {
                        "ok": False,
                        "reasons":
                            [f"runtime_db_path:"
                             f"{e.get('artifact_key')}"
                             f":{path_key}"],
                        "phase": _PHASE,
                    }
    return {"ok": True, "phase": _PHASE}


def create_phase45_archive_verification_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    presence = checks.get("presence") or {}
    hash_ = checks.get("hash") or {}
    chain = checks.get("chain") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    nrs = checks.get("no_runtime_state") or {}
    ok = all([
        bool(presence.get("ok")),
        bool(hash_.get("ok")),
        bool(chain.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
        bool(nrs.get("ok")),
    ])
    return {
        "verification_id":
            f"p45ver_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "presence_check": presence,
        "hash_check": hash_,
        "chain_integrity_check": chain,
        "boundary_check": boundary,
        "phase21_check": p21,
        "no_runtime_state_check": nrs,
        "ok": ok,
        "summary": (
            f"phase45 verify: presence_ok="
            f"{presence.get('ok')} hash_ok="
            f"{hash_.get('ok')} chain_ok="
            f"{chain.get('ok')} boundary_ok="
            f"{boundary.get('ok')} phase21_ok="
            f"{p21.get('ok')} no_runtime_state_ok="
            f"{nrs.get('ok')}"),
    }


def verify_phase45_archive(
    archive: Any,
    manifest: Optional[dict[str, Any]] = None,
    ledger: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    checks = {
        "presence":
            verify_phase45_artifact_presence(archive),
        "hash":
            verify_phase45_artifact_hashes(
                archive, manifest=manifest),
        "chain":
            verify_phase45_chain_integrity(
                archive, ledger=ledger),
        "boundary":
            verify_phase45_boundary_claims(archive),
        "phase21":
            verify_phase45_phase21_claim(archive),
        "no_runtime_state":
            verify_phase45_no_runtime_state_dependency(
                archive),
    }
    return create_phase45_archive_verification_result(
        checks)


def write_phase45_archive_verification_report(
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
    "verify_phase45_archive",
    "verify_phase45_artifact_presence",
    "verify_phase45_artifact_hashes",
    "verify_phase45_chain_integrity",
    "verify_phase45_boundary_claims",
    "verify_phase45_phase21_claim",
    "verify_phase45_no_runtime_state_dependency",
    "create_phase45_archive_verification_result",
    "write_phase45_archive_verification_report",
]
