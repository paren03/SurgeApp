"""Phase 47 - Federation Graph."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase47.federation_graph.v1"


_REQUIRED_GRAPH_FIELDS = (
    "graph_id", "created_at", "phase",
    "checkout_count", "checkout_nodes",
    "timeline_root_hashes",
    "timeline_manifest_hashes",
    "edges",
    "federation_root_hash",
    "phase21_status_summary",
    "adapter_allowlist_summary",
    "boundary_summary",
    "rehearsal_dry_run_only",
)


_BANNED_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _packages_from(imported: list[Any]
                     ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in imported or []:
        if isinstance(i, dict):
            p = i.get("package")
            if isinstance(p, dict):
                out.append(p)
    return out


def create_phase47_federation_graph(
    imported_timelines: Any,
) -> dict[str, Any]:
    if not isinstance(imported_timelines, list):
        return {"graph_id": "", "phase": _PHASE,
                "status": "refused",
                "reason": "imported_not_list"}
    pkgs = _packages_from(imported_timelines)
    if len(pkgs) < 2:
        return {"graph_id": "", "phase": _PHASE,
                "status": "refused",
                "reason": "need_min_2_checkouts",
                "count": len(pkgs)}
    nodes: list[dict[str, Any]] = []
    timeline_roots: dict[str, str] = {}
    manifest_roots: dict[str, str] = {}
    p21_history: dict[str, str] = {}
    allowlist_counts: dict[str, int] = {}
    boundary_intact: dict[str, bool] = {}
    pkg_hashes: dict[str, str] = {}
    for p in pkgs:
        cid = str(p.get("checkout_id") or "")
        nodes.append({
            "checkout_id": cid,
            "package_id": p.get("package_id"),
            "timeline_id": p.get("timeline_id"),
            "manifest_id": p.get("manifest_id"),
            "timeline_root_hash":
                p.get("timeline_root_hash"),
            "manifest_root_hash":
                p.get("manifest_root_hash"),
            "package_hash": p.get("package_hash"),
            "phase21_status_text":
                p.get("phase21_status_text"),
            "adapter_allowlist_count":
                int(p.get(
                    "adapter_allowlist_count") or 0),
            "boundary_summary":
                dict(p.get("boundary_summary") or {}),
        })
        timeline_roots[cid] = str(
            p.get("timeline_root_hash") or "")
        manifest_roots[cid] = str(
            p.get("manifest_root_hash") or "")
        p21_history[cid] = str(
            p.get("phase21_status_text") or "")
        allowlist_counts[cid] = int(
            p.get("adapter_allowlist_count") or 0)
        boundary_intact[cid] = (
            (p.get("boundary_summary") or {}).get(
                "no_adapter_invocation_in_timeline")
            is True
            and (p.get("boundary_summary") or {}).get(
                "no_production_db_read_in_timeline")
            is True)
        pkg_hashes[cid] = str(
            p.get("package_hash") or "")
    # Build edges (chain order = import order)
    sorted_cids = [n.get("checkout_id") for n in nodes]
    edges: list[dict[str, Any]] = []
    for i in range(1, len(nodes)):
        prev = nodes[i - 1]
        cur = nodes[i]
        ok = (prev.get("checkout_id")
              != cur.get("checkout_id"))
        edges.append({
            "index": i,
            "from_checkout_id":
                prev.get("checkout_id"),
            "to_checkout_id": cur.get("checkout_id"),
            "from_timeline_root":
                prev.get("timeline_root_hash"),
            "to_timeline_root":
                cur.get("timeline_root_hash"),
            "edge_hash": _stable_hash({
                "from_id": prev.get("checkout_id"),
                "to_id": cur.get("checkout_id"),
                "from_root":
                    prev.get("timeline_root_hash"),
                "to_root":
                    cur.get("timeline_root_hash"),
            }),
            "ok": ok,
        })
    federation_root = _stable_hash({
        "checkouts": sorted_cids,
        "timeline_roots": timeline_roots,
        "manifest_roots": manifest_roots,
        "package_hashes": pkg_hashes,
    })
    return {
        "graph_id":
            f"p47g_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "checkout_count": len(nodes),
        "checkout_nodes": nodes,
        "timeline_root_hashes": timeline_roots,
        "timeline_manifest_hashes": manifest_roots,
        "package_hashes": pkg_hashes,
        "edges": edges,
        "federation_root_hash": federation_root,
        "phase21_status_summary": p21_history,
        "adapter_allowlist_summary":
            allowlist_counts,
        "boundary_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_main_runtime_integration": True,
            "no_adapter_invocation_in_federation":
                True,
            "no_production_db_read_in_federation":
                True,
            "per_checkout_boundary_intact":
                boundary_intact,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Graph chains checkouts in import order; "
            "federation_root_hash is content-addressed.",
            "checkout_ids must be distinct.",
        ],
    }


def validate_phase47_federation_graph(
    graph: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(graph, dict):
        return {"ok": False,
                "reasons": ["graph_not_dict"]}
    for f in _REQUIRED_GRAPH_FIELDS:
        if f not in graph:
            reasons.append(f"missing_field:{f}")
    if graph.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    for k in _BANNED_FIELDS:
        if k in graph and graph.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    cc = graph.get("checkout_count")
    if not (isinstance(cc, int) and cc >= 2):
        reasons.append("checkout_count_lt_2")
    nodes = graph.get("checkout_nodes") or []
    if not isinstance(nodes, list) or len(nodes) != cc:
        reasons.append("nodes_count_mismatch")
    seen: set[str] = set()
    for n in nodes:
        if not isinstance(n, dict):
            reasons.append("node_not_dict")
            continue
        cid = str(n.get("checkout_id") or "")
        if cid and cid in seen:
            reasons.append(f"duplicate_checkout_id:{cid}")
        if cid:
            seen.add(cid)
        trh = n.get("timeline_root_hash")
        if not (isinstance(trh, str) and len(trh) == 64):
            reasons.append(
                f"bad_timeline_root_hash:{cid}")
        p21 = str(n.get("phase21_status_text") or "")
        if p21 not in ("BLOCKED",
                        "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"phase21_unexpected:{cid}:{p21}")
        if int(n.get(
                "adapter_allowlist_count") or 0) != 5:
            reasons.append(
                f"adapter_count_not_5:{cid}")
        bs = n.get("boundary_summary") or {}
        if bs.get(
                "no_adapter_invocation_in_timeline") \
                is not True:
            reasons.append(
                f"node_boundary_adapter_invocation:"
                f"{cid}")
    edges = graph.get("edges") or []
    expected_edges = max(0, len(nodes) - 1)
    if len(edges) != expected_edges:
        reasons.append("edge_count_mismatch")
    # Federation root rederive
    cids = [str(n.get("checkout_id") or "")
            for n in nodes]
    troots = {n.get("checkout_id"):
              str(n.get("timeline_root_hash") or "")
              for n in nodes
              if isinstance(n, dict)}
    mroots = {n.get("checkout_id"):
              str(n.get("manifest_root_hash") or "")
              for n in nodes
              if isinstance(n, dict)}
    phashes = {n.get("checkout_id"):
                 str(n.get("package_hash") or "")
                 for n in nodes
                 if isinstance(n, dict)}
    expected_root = _stable_hash({
        "checkouts": cids,
        "timeline_roots": troots,
        "manifest_roots": mroots,
        "package_hashes": phashes,
    })
    if expected_root != graph.get(
            "federation_root_hash"):
        reasons.append("federation_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase47_graph_timeline_roots(
    graph: Any,
    imported_timelines: Optional[list[Any]] = None,
) -> dict[str, Any]:
    if not isinstance(graph, dict):
        return {"ok": False,
                "reasons": ["graph_not_dict"]}
    reasons: list[str] = []
    if imported_timelines is not None:
        pkgs = _packages_from(imported_timelines)
        expected = {p.get("checkout_id"):
                    str(p.get(
                        "timeline_root_hash") or "")
                    for p in pkgs}
        observed = dict(
            graph.get("timeline_root_hashes") or {})
        if expected != observed:
            reasons.append(
                "timeline_roots_disagree_with_imported")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase47_graph_breaks(
    graph: Any,
    imported_timelines: Optional[list[Any]] = None,
) -> dict[str, Any]:
    val = validate_phase47_federation_graph(graph)
    if not val.get("ok"):
        return {"broken": True,
                "reasons": val.get("reasons", []),
                "phase": _PHASE}
    roots = verify_phase47_graph_timeline_roots(
        graph, imported_timelines)
    if not roots.get("ok"):
        return {"broken": True,
                "reasons": roots.get("reasons", []),
                "phase": _PHASE}
    return {"broken": False, "reasons": [],
            "phase": _PHASE}


def summarize_phase47_federation_graph(
    graph: Any,
) -> dict[str, Any]:
    if not isinstance(graph, dict):
        return {"ok": False, "summary": "no_graph"}
    return {
        "ok": True,
        "summary": (
            f"phase47 graph: checkouts="
            f"{graph.get('checkout_count')} "
            f"edges={len(graph.get('edges') or [])} "
            f"federation_root="
            f"{(graph.get('federation_root_hash') or '')[:16]}"),
        "graph_id": graph.get("graph_id"),
        "phase": _PHASE,
    }


def write_phase47_federation_graph(
    graph: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(graph)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase47_federation_graph_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase47_federation_graph(report,
                                            output_path)


__all__ = [
    "create_phase47_federation_graph",
    "validate_phase47_federation_graph",
    "verify_phase47_graph_timeline_roots",
    "detect_phase47_graph_breaks",
    "summarize_phase47_federation_graph",
    "write_phase47_federation_graph",
    "write_phase47_federation_graph_report",
]
