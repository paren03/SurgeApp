"""Phase 48 - Fresh-Checkout Capsule Verifier (read-only)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase48_capsule_manifest as cm


_PHASE = "phase48.fresh_checkout_verifier.v1"


_REQUIRED_ARTIFACT_KEYS = (
    "phase47_federation_contract",
    "phase47_federation_graph",
    "phase47_federation_manifest",
    "phase47_verification_result",
    "phase47_drift_report",
    "phase47_tamper_suite_result",
    "phase47_operator_packet",
    "phase47_status_dashboard",
    "phase47_dashboard_markdown",
    "phase47_report",
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


def verify_phase48_capsule_artifact_presence(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    present = {e.get("artifact_key")
                for e in capsule.get("artifacts") or []
                if isinstance(e, dict)}
    missing = [k for k in _REQUIRED_ARTIFACT_KEYS
                if k not in present]
    return {"ok": not missing,
            "missing": missing,
            "present_count": len(present),
            "phase": _PHASE}


def verify_phase48_capsule_hashes(
    capsule: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    mismatches: list[str] = []
    declared = capsule.get("artifact_hashes") or {}
    for e in capsule.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if not isinstance(sha, str) or len(sha) != 64:
            mismatches.append(f"bad_sha:{k}")
            continue
        if declared.get(k) != sha:
            mismatches.append(
                f"declared_vs_entry:{k}")
    # Re-derive capsule_root_hash
    rec = _stable_hash(declared)
    if rec != capsule.get("capsule_root_hash"):
        mismatches.append("capsule_root_hash_drift")
    if isinstance(manifest, dict):
        mres = cm.verify_phase48_capsule_manifest(
            capsule, manifest)
        if not mres.get("ok"):
            mismatches.extend(
                "manifest:" + r
                for r in mres.get("reasons", []))
    return {"ok": not mismatches,
            "mismatches": mismatches,
            "phase": _PHASE}


def verify_phase48_federation_claims(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    reasons: list[str] = []
    entries = capsule.get("artifacts") or []
    by_key = {e.get("artifact_key"): e
              for e in entries
              if isinstance(e, dict)}
    fed_graph = (by_key.get(
        "phase47_federation_graph") or {}).get(
        "inline_content")
    fed_manifest = (by_key.get(
        "phase47_federation_manifest") or {}).get(
        "inline_content")
    fed_verify = (by_key.get(
        "phase47_verification_result") or {}).get(
        "inline_content")
    tamper = (by_key.get(
        "phase47_tamper_suite_result") or {}).get(
        "inline_content")
    op_pkt = (by_key.get(
        "phase47_operator_packet") or {}).get(
        "inline_content")
    if not isinstance(fed_graph, dict):
        reasons.append("federation_graph_missing")
    else:
        if not (isinstance(fed_graph.get(
                "federation_root_hash"), str)
                and len(fed_graph.get(
                    "federation_root_hash")) == 64):
            reasons.append(
                "federation_root_hash_invalid")
        if int(fed_graph.get(
                "checkout_count") or 0) < 2:
            reasons.append(
                "federation_checkout_count_lt_2")
    if not isinstance(fed_manifest, dict):
        reasons.append("federation_manifest_missing")
    # Cross-check: federation_root_hash must match
    # between graph and manifest. Catches tamper of
    # inline_content when on-disk SHA is unchanged.
    if (isinstance(fed_graph, dict)
            and isinstance(fed_manifest, dict)):
        g_root = fed_graph.get("federation_root_hash")
        m_root = fed_manifest.get(
            "federation_root_hash")
        if (isinstance(g_root, str)
                and isinstance(m_root, str)
                and g_root != m_root):
            reasons.append(
                "federation_root_hash_graph_vs_manifest_mismatch")
        # Cross-check against operator packet too,
        # which also embeds federation_root_hash.
        if isinstance(op_pkt, dict):
            o_root = op_pkt.get(
                "federation_root_hash")
            if (isinstance(o_root, str)
                    and isinstance(g_root, str)
                    and o_root != g_root):
                reasons.append(
                    "federation_root_hash_graph_vs_operator_packet_mismatch")
    if not isinstance(fed_verify, dict):
        reasons.append("federation_verification_missing")
    elif fed_verify.get("ok") is not True:
        reasons.append("federation_verification_not_ok")
    if not isinstance(tamper, dict):
        reasons.append("tamper_suite_missing")
    elif tamper.get("ok") is not True:
        reasons.append("tamper_suite_not_ok")
    if isinstance(op_pkt, dict):
        if str(op_pkt.get("phase47_status") or "") \
                not in ("ok", "ok_with_warnings"):
            reasons.append(
                f"operator_packet_status:"
                f"{op_pkt.get('phase47_status')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase48_boundary_claims(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    reasons: list[str] = []
    bs = capsule.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_in_capsule",
              "no_production_db_read_in_capsule"):
        if bs.get(k) is not True:
            reasons.append(f"boundary_false:{k}")
    # Reject runtime-DB paths
    for e in capsule.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        rp = str(e.get("relative_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if rp.endswith(tok):
                reasons.append(
                    f"runtime_db_in_capsule:"
                    f"{e.get('artifact_key')}")
    # Inline-content banned-key check
    for e in capsule.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            for bk in _BANNED_INLINE_KEYS:
                if bk in inline and inline.get(bk) \
                        not in (None, "", False,
                                 [], {}):
                    reasons.append(
                        f"banned_inline_field:"
                        f"{e.get('artifact_key')}:{bk}")
    if capsule.get("adapter_allowlist_count") != 5:
        reasons.append("adapter_count_not_5")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase48_phase21_claim(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    reasons: list[str] = []
    txt = str(capsule.get(
        "phase21_status_text") or "")
    if txt not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        reasons.append(f"capsule_phase21:{txt}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase48_no_runtime_state_dependency(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"]}
    reasons: list[str] = []
    bs = capsule.get("boundary_summary") or {}
    if bs.get(
            "no_production_db_read_in_capsule") \
            is not True:
        reasons.append(
            "no_production_db_read_must_be_true")
    for e in capsule.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        for path_key in ("relative_path",
                          "absolute_path"):
            v = str(e.get(path_key) or "").lower()
            for tok in _RUNTIME_DB_TOKENS:
                if v.endswith(tok):
                    reasons.append(
                        f"runtime_db_path:"
                        f"{e.get('artifact_key')}:"
                        f"{path_key}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def create_phase48_fresh_checkout_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    presence = checks.get("presence") or {}
    hash_ = checks.get("hash") or {}
    federation = checks.get("federation") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    nrs = checks.get("no_runtime_state") or {}
    ok = all([
        bool(presence.get("ok")),
        bool(hash_.get("ok")),
        bool(federation.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
        bool(nrs.get("ok")),
    ])
    return {
        "verification_id":
            f"p48fcv_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "presence_check": presence,
        "hash_check": hash_,
        "federation_check": federation,
        "boundary_check": boundary,
        "phase21_check": p21,
        "no_runtime_state_check": nrs,
        "ok": ok,
        "summary": (
            f"phase48 fresh: presence={presence.get('ok')} "
            f"hash={hash_.get('ok')} "
            f"federation={federation.get('ok')} "
            f"boundary={boundary.get('ok')} "
            f"phase21={p21.get('ok')} "
            f"no_runtime_state={nrs.get('ok')}"),
    }


def verify_phase48_capsule_fresh_checkout(
    capsule: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    checks = {
        "presence":
            verify_phase48_capsule_artifact_presence(
                capsule),
        "hash":
            verify_phase48_capsule_hashes(
                capsule, manifest=manifest),
        "federation":
            verify_phase48_federation_claims(capsule),
        "boundary":
            verify_phase48_boundary_claims(capsule),
        "phase21":
            verify_phase48_phase21_claim(capsule),
        "no_runtime_state":
            verify_phase48_no_runtime_state_dependency(
                capsule),
    }
    return create_phase48_fresh_checkout_result(checks)


def write_phase48_fresh_checkout_report(
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
    "verify_phase48_capsule_fresh_checkout",
    "verify_phase48_capsule_artifact_presence",
    "verify_phase48_capsule_hashes",
    "verify_phase48_federation_claims",
    "verify_phase48_boundary_claims",
    "verify_phase48_phase21_claim",
    "verify_phase48_no_runtime_state_dependency",
    "create_phase48_fresh_checkout_result",
    "write_phase48_fresh_checkout_report",
]
