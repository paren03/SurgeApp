"""Phase 43 - Fresh-Checkout Verifier.

Verifies a portable bundle as if on a fresh checkout. Reads only
the bundle's inline content (or recomputes hashes against the
bundle entries themselves if inline content is absent). Does NOT
read production DBs. Does NOT invoke any adapter.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase43_bundle_manifest as bm


_PHASE = "phase43.fresh_checkout_verifier.v1"


_REQUIRED_RESULT_FIELDS = (
    "verification_id", "created_at", "phase",
    "presence_check", "hash_check",
    "phase42_claims_check",
    "boundary_claims_check",
    "phase21_claim_check",
    "ok", "summary",
)


_REQUIRED_BUNDLE_ARTIFACT_KEYS = (
    "phase42_audit_contract",
    "phase42_trace_batch",
    "phase42_coherence_audit",
    "phase42_replay_matrix",
    "phase42_drift_stability_matrix",
    "phase42_operator_packet",
    "phase42_operator_markdown",
    "phase42_report",
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


def verify_phase43_artifact_presence(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    entries = bundle.get("artifacts") or []
    present_keys = {e.get("artifact_key")
                     for e in entries
                     if isinstance(e, dict)}
    missing = [k for k in _REQUIRED_BUNDLE_ARTIFACT_KEYS
                if k not in present_keys]
    return {"ok": not missing,
            "missing": missing,
            "present_count": len(present_keys),
            "phase": _PHASE}


def verify_phase43_artifact_hashes(
    bundle: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    entries = bundle.get("artifacts") or []
    declared = bundle.get("artifact_hashes") or {}
    mismatches: list[str] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        inline = e.get("inline_content")
        if k and isinstance(sha, str) and len(sha) == 64:
            # Cross-check entry sha matches declared
            if declared.get(k) != sha:
                mismatches.append(
                    f"declared_vs_entry:{k}")
        else:
            mismatches.append(f"missing_sha256:{k}")
        # If inline JSON present, re-derive a content
        # hash and compare to the declared sha
        # IMPORTANT: inline content is the parsed object;
        # the declared sha is over the file bytes. We
        # cannot recompute the file-byte sha from a
        # parsed object — instead we re-canonicalize the
        # inline content and check that *this* hash is
        # stable across re-runs (tamper detection).
        if inline is not None and k:
            inline_hash = _stable_hash(inline)
            # Reject if inline_hash field exists in entry
            # and disagrees with re-derived
            if e.get("inline_content_hash") and \
                    e["inline_content_hash"] != inline_hash:
                mismatches.append(
                    f"inline_content_hash_drift:{k}")
    # Manifest cross-check
    if isinstance(manifest, dict):
        m_check = bm.verify_phase43_bundle_manifest(
            bundle, manifest)
        if not m_check.get("ok"):
            mismatches.extend(
                "manifest:" + r
                for r in m_check.get("reasons", []))
    return {"ok": not mismatches,
            "mismatches": mismatches,
            "phase": _PHASE}


def verify_phase43_phase42_claims(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    entries = bundle.get("artifacts") or []
    by_key = {e.get("artifact_key"): e
               for e in entries
               if isinstance(e, dict)}
    op_pkt = (by_key.get("phase42_operator_packet")
              or {}).get("inline_content")
    coh = (by_key.get("phase42_coherence_audit")
           or {}).get("inline_content")
    rmat = (by_key.get("phase42_replay_matrix")
             or {}).get("inline_content")
    reasons: list[str] = []
    if not isinstance(op_pkt, dict):
        reasons.append("operator_packet_missing_or_invalid")
    else:
        if str(op_pkt.get("audit_status") or "") not in (
                "ok", "ok_with_warnings"):
            reasons.append(
                f"operator_packet_audit_status:"
                f"{op_pkt.get('audit_status')}")
        ac = (op_pkt.get("adapter_coverage") or {})
        covered = ac.get("covered_in_success") or []
        for must in (
                "dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter",
                "memory_continuity_audit_metadata_adapter"):
            if must not in covered:
                reasons.append(
                    f"adapter_not_covered:{must}")
    if not isinstance(coh, dict):
        reasons.append(
            "coherence_audit_missing_or_invalid")
    elif coh.get("ok") is not True:
        reasons.append("coherence_audit_not_ok")
    if not isinstance(rmat, dict):
        reasons.append("replay_matrix_missing_or_invalid")
    elif str(rmat.get("compatibility_status") or "") \
            != "ok":
        reasons.append(
            f"replay_matrix_compat:"
            f"{rmat.get('compatibility_status')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase43_boundary_claims(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    reasons: list[str] = []
    # Bundle's own boundary_summary must hold
    bs = bundle.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_reinvocation_in_bundle",
              "no_production_db_in_bundle"):
        if bs.get(k) is not True:
            reasons.append(f"boundary_false:{k}")
    # Reject runtime-DB references anywhere in bundle
    # entry absolute paths
    for e in bundle.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        rp = str(e.get("relative_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if rp.endswith(tok):
                reasons.append(
                    f"runtime_db_in_bundle:"
                    f"{e.get('artifact_key')}")
    # Reject banned inline keys
    for e in bundle.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            for k in _BANNED_INLINE_KEYS:
                if k in inline and inline.get(k) not in (
                        None, "", False, [], {}):
                    reasons.append(
                        f"banned_inline_field:"
                        f"{e.get('artifact_key')}:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase43_phase21_claim(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    txt = str(bundle.get("phase21_status_text") or "")
    if txt not in ("BLOCKED", "STAGED_AWAITING_OPERATOR"):
        return {"ok": False,
                "reasons":
                    [f"phase21_status_unexpected:{txt}"],
                "phase": _PHASE}
    return {"ok": True, "phase21_status_text": txt,
            "phase": _PHASE}


def create_phase43_fresh_checkout_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    presence = checks.get("presence") or {}
    hash_ = checks.get("hash") or {}
    p42 = checks.get("phase42_claims") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    ok = all([
        bool(presence.get("ok")),
        bool(hash_.get("ok")),
        bool(p42.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
    ])
    return {
        "verification_id":
            f"p43fc_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "presence_check": presence,
        "hash_check": hash_,
        "phase42_claims_check": p42,
        "boundary_claims_check": boundary,
        "phase21_claim_check": p21,
        "ok": ok,
        "summary": (
            f"phase43 fresh-checkout: presence_ok="
            f"{presence.get('ok')} hash_ok="
            f"{hash_.get('ok')} p42_ok={p42.get('ok')} "
            f"boundary_ok={boundary.get('ok')} "
            f"phase21_ok={p21.get('ok')}"),
    }


def verify_phase43_bundle_fresh_checkout(
    bundle: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    checks = {
        "presence":
            verify_phase43_artifact_presence(bundle),
        "hash":
            verify_phase43_artifact_hashes(
                bundle, manifest=manifest),
        "phase42_claims":
            verify_phase43_phase42_claims(bundle),
        "boundary":
            verify_phase43_boundary_claims(bundle),
        "phase21":
            verify_phase43_phase21_claim(bundle),
    }
    return create_phase43_fresh_checkout_result(checks)


def write_phase43_fresh_checkout_report(
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
    "verify_phase43_bundle_fresh_checkout",
    "verify_phase43_artifact_presence",
    "verify_phase43_artifact_hashes",
    "verify_phase43_phase42_claims",
    "verify_phase43_boundary_claims",
    "verify_phase43_phase21_claim",
    "create_phase43_fresh_checkout_result",
    "write_phase43_fresh_checkout_report",
]
