"""Phase 47 - Timeline Importer.

Simulates importing Phase 46 timelines from N local 'checkouts'.
Each checkout is a fresh Phase 46 timeline run, labeled with a
distinct checkout_id. No subprocess, no network, no multiprocessing.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase46_runtime as p46rt


_PHASE = "phase47.timeline_importer.v1"


_MAX_INLINE_BYTES = 4 * 1024 * 1024


_URL_PREFIXES = (
    "http://", "https://", "ftp://", "file://",
    "smb://", "ssh://", "git://", "ws://", "wss://",
    "http:/", "https:/", "ftp:/", "file:/",
    "smb:/", "ssh:/", "git:/", "ws:/", "wss:/",
)


_SHELL_METACHARS = (
    "|", "&", ";", "`", "$", "(", ")",
    "\n", "\r",
)


_PATH_TRAVERSAL_TOKENS = ("..",)


_FORBIDDEN_PATH_TOKENS = (
    "/local_secret_handoff/",
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/",
    "/corpus_sources/english/incoming/",
    "/corpus_sources/russian/incoming/",
    "/.claude/",
)


_RUNTIME_DB_EXTENSIONS = (".sqlite", ".sqlite3", ".db")
_AUDIO_EXTENSIONS = (".wav", ".mp3", ".ogg", ".flac",
                      ".m4a", ".aac", ".opus")


_BANNED_INLINE_KEYS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def _project_relative_path(p: str) -> str:
    root = Path(__file__).resolve().parent
    try:
        ap = Path(p).resolve()
        rel = ap.relative_to(root)
        return str(rel).replace("\\", "/")
    except Exception:  # noqa: BLE001
        return str(p or "").replace("\\", "/")


def _reject_path(p: str) -> tuple[bool, str]:
    s = str(p or "")
    norm = s.replace("\\", "/")
    low = norm.lower()
    for pre in _URL_PREFIXES:
        if low.startswith(pre):
            return True, f"url_scheme:{pre}"
    for ch in _SHELL_METACHARS:
        if ch in s:
            return True, f"shell_metachar:{repr(ch)}"
    for tok in _PATH_TRAVERSAL_TOKENS:
        if (f"/{tok}/" in norm
                or norm.startswith(f"{tok}/")
                or norm.endswith(f"/{tok}")):
            return True, f"path_traversal:{tok}"
    rel = _project_relative_path(s)
    rlow = rel.lower()
    rcheck = (rel if rel.startswith("/")
              else "/" + rel).lower()
    for tok in _FORBIDDEN_PATH_TOKENS:
        if tok in rcheck:
            return True, f"forbidden_path_token:{tok}"
    for ext in _RUNTIME_DB_EXTENSIONS:
        if rlow.endswith(ext):
            return True, f"runtime_db_ext:{ext}"
    for ext in _AUDIO_EXTENSIONS:
        if rlow.endswith(ext):
            return True, f"audio_ext:{ext}"
    return False, ""


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_phase47_import_workspace(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    if output_dir:
        base = Path(output_dir)
    else:
        base = (root / "bilingual_stack"
                     / "voice_adapter_phase47"
                     / "imported_timelines")
    workspace = (base
                  / f"federation_{int(time.time())}_"
                    f"{uuid.uuid4().hex[:8]}")
    try:
        workspace.mkdir(parents=True, exist_ok=True)
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"mkdir_failed:{e}",
                "phase": _PHASE}
    return {
        "ok": True,
        "workspace_path": str(workspace),
        "phase": _PHASE,
    }


def create_phase47_timeline_package_from_phase46(
    checkout_id: str,
    source_dir: Optional[str] = None,
    archive_count: int = 3,
) -> dict[str, Any]:
    """Run Phase 46 fresh to produce a timeline package
    labelled by checkout_id. Each call produces a fresh
    Phase 46 timeline; we then snapshot its in-memory
    output without ever touching production DBs."""
    out = p46rt.run_phase46_long_horizon_timeline(
        archive_count=archive_count)
    timeline = out.get("timeline") or {}
    manifest = out.get("timeline_manifest") or {}
    verification = out.get("verification_result") or {}
    tamper = out.get("tamper_suite_result") or {}
    pkt = out.get("operator_packet") or {}
    dash = out.get("status_dashboard") or {}
    # Snapshot only the salient summaries — no large
    # nested inline content
    package = {
        "checkout_id": str(checkout_id or ""),
        "package_id":
            f"p47pkg_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase": "phase46",
        "timeline_id": timeline.get("timeline_id"),
        "timeline_root_hash":
            timeline.get("timeline_root_hash"),
        "archive_count":
            int(timeline.get("archive_count") or 0),
        "manifest_id": manifest.get("manifest_id"),
        "manifest_root_hash":
            manifest.get("manifest_root_hash"),
        "verification_ok": bool(verification.get("ok")),
        "tamper_suite_ok": bool(tamper.get("ok")),
        "tamper_detected_count":
            int(tamper.get("detected_count") or 0),
        "tamper_case_count":
            int(tamper.get("case_count") or 0),
        "phase21_status_text":
            timeline.get("phase21_status_text",
                          "BLOCKED"),
        "boundary_summary":
            dict(timeline.get("boundary_summary") or {}),
        "operator_packet_status":
            pkt.get("phase46_status"),
        "dashboard_status":
            dash.get("phase46_status"),
        "adapter_allowlist_count": 5,
        "adapter_allowlist": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter",
            "safety_redaction_trace_metadata_adapter",
            "memory_continuity_audit_metadata_adapter",
        ],
        "production_baseline_expected": {
            "english_words": 2814,
            "russian_words": 2518,
            "russian_phrases": 35,
            "bilingual_concepts": 26,
            "bilingual_entry_links": 52,
            "live_pack_manifests": 90,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Package snapshots Phase 46 timeline + "
            "manifest + verification summary only.",
            "No inline transcripts; no signing material; "
            "no operator_id.",
        ],
    }
    package["package_hash"] = _stable_hash({
        "checkout_id": package["checkout_id"],
        "timeline_id": package["timeline_id"],
        "timeline_root_hash":
            package["timeline_root_hash"],
        "manifest_root_hash":
            package["manifest_root_hash"],
    })
    return package


def import_phase47_timeline_package(
    package: Any,
    workspace_dir: Optional[str] = None,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False,
                "reason": "package_not_dict",
                "phase": _PHASE}
    if workspace_dir:
        rej, reason = _reject_path(workspace_dir)
        if rej:
            return {"ok": False, "reason": reason,
                    "phase": _PHASE}
        ws = Path(workspace_dir)
    else:
        wsr = create_phase47_import_workspace()
        if not wsr.get("ok"):
            return {"ok": False,
                    "reason": wsr.get("reason"),
                    "phase": _PHASE}
        ws = Path(wsr["workspace_path"])
    ws.mkdir(parents=True, exist_ok=True)
    # Reject banned inline keys
    for k in _BANNED_INLINE_KEYS:
        if k in package and package.get(k) not in (
                None, "", False, [], {}):
            return {"ok": False,
                    "reason": f"banned_field:{k}",
                    "phase": _PHASE}
    fname = (f"checkout_{package.get('checkout_id')}_"
              f"{package.get('package_id')}.json")
    target = ws / fname
    try:
        target.write_text(json.dumps(
            package, ensure_ascii=False, indent=2,
            default=str), encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"write_failed:{e}",
                "phase": _PHASE}
    return {
        "ok": True,
        "phase": _PHASE,
        "package": package,
        "imported_path": str(target),
        "workspace_dir": str(ws),
        "imported_at": time.time(),
    }


def import_n_phase47_timeline_packages(
    n: int = 2,
    workspace_dir: Optional[str] = None,
    archive_count: int = 3,
) -> list[dict[str, Any]]:
    n = max(2, min(int(n or 2), 8))
    if workspace_dir is None:
        ws = create_phase47_import_workspace()
        workspace_dir = ws.get("workspace_path") \
            if ws.get("ok") else None
    out: list[dict[str, Any]] = []
    for i in range(n):
        checkout_id = (
            f"checkout_{i+1}_"
            f"{uuid.uuid4().hex[:8]}")
        pkg = create_phase47_timeline_package_from_phase46(
            checkout_id=checkout_id,
            archive_count=archive_count)
        imp = import_phase47_timeline_package(
            pkg, workspace_dir=workspace_dir)
        out.append(imp)
    return out


_REQUIRED_IMPORT_FIELDS = (
    "ok", "phase", "package",
    "imported_path", "workspace_dir",
    "imported_at",
)


_BANNED_IMPORT_FIELDS = _BANNED_INLINE_KEYS


def validate_phase47_imported_timeline(
    imported: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(imported, dict):
        return {"ok": False,
                "reasons": ["imported_not_dict"]}
    for f in _REQUIRED_IMPORT_FIELDS:
        if f not in imported:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_IMPORT_FIELDS:
        if k in imported and imported.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    pkg = imported.get("package") or {}
    if not isinstance(pkg, dict):
        reasons.append("package_not_dict")
    else:
        for k in ("checkout_id", "timeline_id",
                  "timeline_root_hash",
                  "phase21_status_text",
                  "boundary_summary",
                  "adapter_allowlist_count"):
            if k not in pkg:
                reasons.append(f"missing_pkg_field:{k}")
        for k in _BANNED_IMPORT_FIELDS:
            if k in pkg and pkg.get(k) not in (
                    None, "", False, [], {}):
                reasons.append(f"banned_pkg_field:{k}")
        p21 = str(pkg.get("phase21_status_text") or "")
        if p21 not in ("BLOCKED",
                        "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"phase21_unexpected:{p21}")
        if pkg.get("adapter_allowlist_count") != 5:
            reasons.append(
                f"adapter_count_not_5:"
                f"{pkg.get('adapter_allowlist_count')}")
        trh = pkg.get("timeline_root_hash")
        if not (isinstance(trh, str) and len(trh) == 64):
            reasons.append("bad_timeline_root_hash")
        ph = pkg.get("package_hash")
        if not (isinstance(ph, str) and len(ph) == 64):
            reasons.append("bad_package_hash")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase47_imported_timeline(
    imported: Any,
) -> dict[str, Any]:
    if not isinstance(imported, dict):
        return {"ok": False, "summary": "no_imported"}
    pkg = imported.get("package") or {}
    return {
        "ok": bool(imported.get("ok")),
        "summary": (
            f"phase47 imported: checkout="
            f"{pkg.get('checkout_id')} "
            f"timeline_root="
            f"{(pkg.get('timeline_root_hash') or '')[:16]} "
            f"phase21="
            f"{pkg.get('phase21_status_text')}"),
        "package_id": pkg.get("package_id"),
        "phase": _PHASE,
    }


def write_phase47_imported_timeline(
    imported: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(imported)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase47_timeline_importer_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase47_imported_timeline(report,
                                              output_path)


__all__ = [
    "create_phase47_import_workspace",
    "create_phase47_timeline_package_from_phase46",
    "import_phase47_timeline_package",
    "import_n_phase47_timeline_packages",
    "validate_phase47_imported_timeline",
    "summarize_phase47_imported_timeline",
    "write_phase47_imported_timeline",
    "write_phase47_timeline_importer_report",
]
