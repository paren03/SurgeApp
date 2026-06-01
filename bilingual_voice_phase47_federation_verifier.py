"""Phase 47 - Federation Verifier."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase47_timeline_importer as imp
import bilingual_voice_phase47_federation_graph as fg
import bilingual_voice_phase47_federation_manifest as fm


_PHASE = "phase47.federation_verifier.v1"


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


def verify_phase47_imported_timelines(
    imported_timelines: Any,
) -> dict[str, Any]:
    if not isinstance(imported_timelines, list):
        return {"ok": False,
                "reasons": ["imported_not_list"]}
    reasons: list[str] = []
    seen_cid: set[str] = set()
    for i in imported_timelines:
        val = imp.validate_phase47_imported_timeline(i)
        if not val.get("ok"):
            reasons.append(
                f"imported_invalid:"
                + ",".join(val.get("reasons", [])))
            continue
        pkg = i.get("package") or {}
        cid = str(pkg.get("checkout_id") or "")
        if cid and cid in seen_cid:
            reasons.append(f"duplicate_checkout:{cid}")
        if cid:
            seen_cid.add(cid)
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase47_graph(
    graph: Any,
    imported_timelines: Optional[list[Any]] = None,
) -> dict[str, Any]:
    val = fg.validate_phase47_federation_graph(graph)
    if not val.get("ok"):
        return {"ok": False,
                "reasons": val.get("reasons", []),
                "phase": _PHASE}
    return fg.verify_phase47_graph_timeline_roots(
        graph, imported_timelines=imported_timelines)


def verify_phase47_manifest(
    manifest: Any,
    graph: Optional[dict[str, Any]] = None,
    imported_timelines: Optional[list[Any]] = None,
) -> dict[str, Any]:
    val = fm.validate_phase47_federation_manifest(
        manifest)
    if not val.get("ok"):
        return {"ok": False,
                "reasons": val.get("reasons", []),
                "phase": _PHASE}
    if isinstance(graph, dict) and isinstance(
            imported_timelines, list):
        return fm.verify_phase47_federation_manifest(
            graph, imported_timelines, manifest)
    return {"ok": True, "reasons": [], "phase": _PHASE}


def verify_phase47_boundary_claims(
    records: Any,
) -> dict[str, Any]:
    """Records is a dict containing graph + imported
    timelines."""
    reasons: list[str] = []
    if not isinstance(records, dict):
        return {"ok": False,
                "reasons": ["records_not_dict"]}
    graph = records.get("graph") or {}
    if not isinstance(graph, dict):
        reasons.append("graph_not_dict")
    else:
        bs = graph.get("boundary_summary") or {}
        for k in ("no_audio", "no_tts",
                  "no_subprocess", "no_network",
                  "no_multiprocessing",
                  "no_main_runtime_integration",
                  "no_adapter_invocation_in_federation",
                  "no_production_db_read_in_federation"):
            if bs.get(k) is not True:
                reasons.append(f"graph_boundary:{k}")
    imported = records.get("imported_timelines") or []
    if not isinstance(imported, list):
        reasons.append("imported_not_list")
    else:
        for i in imported:
            pkg = (i or {}).get("package") or {}
            bs = pkg.get("boundary_summary") or {}
            if bs.get(
                    "no_adapter_invocation_in_timeline"
                    ) is not True:
                reasons.append(
                    f"checkout_boundary_adapter:"
                    f"{pkg.get('checkout_id')}")
            for k in _BANNED_INLINE_KEYS:
                if k in pkg and pkg.get(k) not in (
                        None, "", False, [], {}):
                    reasons.append(
                        f"banned_inline_pkg:"
                        f"{pkg.get('checkout_id')}:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase47_phase21_history(
    records: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(records, dict):
        return {"ok": False,
                "reasons": ["records_not_dict"]}
    imported = records.get("imported_timelines") or []
    for i in imported:
        pkg = (i or {}).get("package") or {}
        cid = pkg.get("checkout_id")
        p21 = str(pkg.get("phase21_status_text") or "")
        if p21 not in ("BLOCKED",
                        "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"checkout_phase21:{cid}:{p21}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase47_no_runtime_state_dependency(
    records: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(records, dict):
        return {"ok": False,
                "reasons": ["records_not_dict"]}
    graph = records.get("graph") or {}
    bs = graph.get("boundary_summary") or {}
    if bs.get(
            "no_production_db_read_in_federation") \
            is not True:
        reasons.append(
            "no_production_db_read_must_be_true")
    imported = records.get("imported_timelines") or []
    for i in imported:
        ip = str((i or {}).get(
            "imported_path") or "").lower()
        for tok in _RUNTIME_DB_TOKENS:
            if ip.endswith(tok):
                reasons.append(
                    f"runtime_db_path:"
                    f"{(i or {}).get('package', {}).get('checkout_id')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def create_phase47_federation_verification_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    imported = checks.get("imported") or {}
    graph = checks.get("graph") or {}
    manifest = checks.get("manifest") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    nrs = checks.get("no_runtime_state") or {}
    ok = all([
        bool(imported.get("ok")),
        bool(graph.get("ok")),
        bool(manifest.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
        bool(nrs.get("ok")),
    ])
    return {
        "verification_id":
            f"p47ver_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "imported_check": imported,
        "graph_check": graph,
        "manifest_check": manifest,
        "boundary_check": boundary,
        "phase21_check": p21,
        "no_runtime_state_check": nrs,
        "ok": ok,
        "summary": (
            f"phase47 verify: imported={imported.get('ok')} "
            f"graph={graph.get('ok')} "
            f"manifest={manifest.get('ok')} "
            f"boundary={boundary.get('ok')} "
            f"phase21={p21.get('ok')} "
            f"no_runtime_state={nrs.get('ok')}"),
    }


def verify_phase47_federation(
    imported_timelines: Optional[list[Any]] = None,
    graph: Optional[dict[str, Any]] = None,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    records = {
        "imported_timelines": imported_timelines or [],
        "graph": graph or {},
        "manifest": manifest or {},
    }
    checks = {
        "imported":
            verify_phase47_imported_timelines(
                imported_timelines or []),
        "graph":
            verify_phase47_graph(
                graph or {},
                imported_timelines=imported_timelines),
        "manifest":
            verify_phase47_manifest(
                manifest or {},
                graph=graph,
                imported_timelines=imported_timelines),
        "boundary":
            verify_phase47_boundary_claims(records),
        "phase21":
            verify_phase47_phase21_history(records),
        "no_runtime_state":
            verify_phase47_no_runtime_state_dependency(
                records),
    }
    return create_phase47_federation_verification_result(
        checks)


def write_phase47_federation_verification_report(
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
    "verify_phase47_federation",
    "verify_phase47_imported_timelines",
    "verify_phase47_graph",
    "verify_phase47_manifest",
    "verify_phase47_boundary_claims",
    "verify_phase47_phase21_history",
    "verify_phase47_no_runtime_state_dependency",
    "create_phase47_federation_verification_result",
    "write_phase47_federation_verification_report",
]
