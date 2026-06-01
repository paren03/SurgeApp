"""Phase 44 - Fresh Import Verifier.

Verifies the imported bundle in the workspace using only the
imported artifacts. No production DB reads. No adapter invocation.
No subprocess / network / multiprocessing.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase44.fresh_import_verifier.v1"


_REQUIRED_RESULT_FIELDS = (
    "verification_id", "created_at", "phase",
    "presence_check", "hash_check",
    "boundary_check", "phase21_check",
    "no_runtime_state_check",
    "ok", "summary",
)


_REQUIRED_IMPORT_ARTIFACT_KEYS = (
    "portable_bundle",
    "bundle_manifest",
    "source_operator_packet",
    "source_status_dashboard",
    "source_phase43_report",
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


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    if not p.exists() or not p.is_file():
        return ""
    try:
        with p.open("rb") as fh:
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                h.update(chunk)
    except Exception:  # noqa: BLE001
        return ""
    return h.hexdigest()


def verify_phase44_import_artifact_presence(
    imported_bundle: Any,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    entries = imported_bundle.get("entries") or []
    present_keys = {e.get("artifact_key")
                     for e in entries
                     if isinstance(e, dict)}
    missing = [k for k in _REQUIRED_IMPORT_ARTIFACT_KEYS
                if k not in present_keys]
    return {"ok": not missing,
            "missing": missing,
            "present_count": len(present_keys),
            "phase": _PHASE}


def verify_phase44_import_hashes(
    imported_bundle: Any,
    import_manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    mismatches: list[str] = []
    declared: dict[str, str] = {}
    if isinstance(import_manifest, dict):
        declared = dict(
            import_manifest.get(
                "imported_artifact_hashes") or {})
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha_source = e.get("source_sha256")
        sha_imported = e.get("imported_sha256")
        if not (isinstance(sha_source, str)
                and len(sha_source) == 64):
            mismatches.append(f"bad_source_sha:{k}")
            continue
        if not (isinstance(sha_imported, str)
                and len(sha_imported) == 64):
            mismatches.append(f"bad_imported_sha:{k}")
            continue
        if sha_source != sha_imported:
            mismatches.append(f"source_vs_imported:{k}")
        # Re-hash the imported file on disk
        ip = e.get("imported_path")
        if isinstance(ip, str):
            actual = _sha256_file(Path(ip))
            if actual and actual != sha_imported:
                mismatches.append(
                    f"imported_disk_drift:{k}")
        if declared and declared.get(k) and declared.get(
                k) != sha_imported:
            mismatches.append(f"manifest_drift:{k}")
    return {"ok": not mismatches,
            "mismatches": mismatches,
            "phase": _PHASE}


def verify_phase44_import_boundary_claims(
    imported_bundle: Any,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    reasons: list[str] = []
    bs = imported_bundle.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_on_import",
              "no_production_db_read_on_import"):
        if bs.get(k) is not True:
            reasons.append(f"boundary_false:{k}")
    # Reject runtime-DB imported paths
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        ip = str(e.get("imported_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if ip.endswith(tok):
                reasons.append(
                    f"runtime_db_in_imported:"
                    f"{e.get('artifact_key')}")
        sp = str(e.get("source_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if sp.endswith(tok):
                reasons.append(
                    f"runtime_db_in_source:"
                    f"{e.get('artifact_key')}")
    # Inline-content banned-key check (read JSON files
    # from workspace, bounded)
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        ip = e.get("imported_path")
        if not isinstance(ip, str):
            continue
        path = Path(ip)
        if not path.exists() or not path.is_file():
            continue
        if path.suffix.lower() != ".json":
            continue
        try:
            if path.stat().st_size > 4 * 1024 * 1024:
                continue
            body = path.read_text(encoding="utf-8",
                                    errors="ignore")
            obj = json.loads(body)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(obj, dict):
            for bk in _BANNED_INLINE_KEYS:
                if bk in obj and obj.get(bk) not in (
                        None, "", False, [], {}):
                    reasons.append(
                        f"banned_field:"
                        f"{e.get('artifact_key')}:{bk}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase44_import_phase21_status(
    imported_bundle: Any,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    txt = str(imported_bundle.get(
        "phase21_status_text") or "")
    if txt not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        return {"ok": False,
                "reasons":
                    [f"phase21_status_unexpected:{txt}"],
                "phase": _PHASE}
    return {"ok": True, "phase21_status_text": txt,
            "phase": _PHASE}


def verify_phase44_no_runtime_state_dependency(
    imported_bundle: Any,
) -> dict[str, Any]:
    """Confirm verification of this bundle does NOT require
    any production DB, runtime memory file, or live Luna
    service. The check is structural: bundle declares its
    own self-sufficiency, and inspector source must not
    open production DBs nor depend on runtime state."""
    if not isinstance(imported_bundle, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    bs = imported_bundle.get("boundary_summary") or {}
    if bs.get("no_production_db_read_on_import") \
            is not True:
        return {"ok": False,
                "reasons":
                    ["no_production_db_read_must_be_true"],
                "phase": _PHASE}
    # No artifact_key should reference a runtime DB path
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        for path_key in ("source_path",
                          "imported_path"):
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


def create_phase44_fresh_import_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    presence = checks.get("presence") or {}
    hash_ = checks.get("hash") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    nrs = checks.get("no_runtime_state") or {}
    ok = all([
        bool(presence.get("ok")),
        bool(hash_.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
        bool(nrs.get("ok")),
    ])
    return {
        "verification_id":
            f"p44fcv_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "presence_check": presence,
        "hash_check": hash_,
        "boundary_check": boundary,
        "phase21_check": p21,
        "no_runtime_state_check": nrs,
        "ok": ok,
        "summary": (
            f"phase44 fresh-import: presence_ok="
            f"{presence.get('ok')} hash_ok="
            f"{hash_.get('ok')} boundary_ok="
            f"{boundary.get('ok')} phase21_ok="
            f"{p21.get('ok')} no_runtime_state_ok="
            f"{nrs.get('ok')}"),
    }


def verify_phase44_imported_bundle_fresh(
    imported_bundle: Any,
    import_manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    checks = {
        "presence":
            verify_phase44_import_artifact_presence(
                imported_bundle),
        "hash":
            verify_phase44_import_hashes(
                imported_bundle,
                import_manifest=import_manifest),
        "boundary":
            verify_phase44_import_boundary_claims(
                imported_bundle),
        "phase21":
            verify_phase44_import_phase21_status(
                imported_bundle),
        "no_runtime_state":
            verify_phase44_no_runtime_state_dependency(
                imported_bundle),
    }
    return create_phase44_fresh_import_result(checks)


def write_phase44_fresh_import_report(
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
    "verify_phase44_imported_bundle_fresh",
    "verify_phase44_import_artifact_presence",
    "verify_phase44_import_hashes",
    "verify_phase44_import_boundary_claims",
    "verify_phase44_import_phase21_status",
    "verify_phase44_no_runtime_state_dependency",
    "create_phase44_fresh_import_result",
    "write_phase44_fresh_import_report",
]
