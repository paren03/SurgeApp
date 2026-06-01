"""Phase 44 - Bundle Importer.

Local Python file-copy of the Phase 43 portable bundle into a
fresh-checkout simulation workspace. No subprocess, no network,
no multiprocessing.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase44.bundle_importer.v1"


_MAX_BYTES = 4 * 1024 * 1024


_URL_PREFIXES = (
    "http://", "https://", "ftp://", "file://",
    "smb://", "ssh://", "git://", "ws://", "wss://",
    # Windows pathlib collapses // -> / when a URL is
    # passed to Path(...). Catch both forms.
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


def _project_relative_path(p: str) -> str:
    """Return the path relative to the project root (the
    directory holding this module). If the path is already
    a non-absolute repo-relative path, return it
    normalized; otherwise strip the resolved root prefix
    so worktree paths under '.claude/worktrees/' don't
    false-positive the '/.claude/' exclusion."""
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
    # URL / shell metacharacter checks run on the raw
    # input — these are caller-injected hazards.
    for pre in _URL_PREFIXES:
        if low.startswith(pre):
            return True, f"url_scheme:{pre}"
    for ch in _SHELL_METACHARS:
        if ch in s:
            return True, f"shell_metachar:{repr(ch)}"
    for tok in _PATH_TRAVERSAL_TOKENS:
        if f"/{tok}/" in norm or norm.startswith(
                f"{tok}/") or norm.endswith(f"/{tok}"):
            return True, f"path_traversal:{tok}"
    # Forbidden path-token + extension checks run on the
    # PROJECT-RELATIVE path so worktree containers under
    # '.claude/worktrees/' aren't conflated with .claude/
    # settings directories.
    rel = _project_relative_path(s)
    rlow = rel.lower()
    # Always include leading slash for token matching
    rcheck = "/" + rel if not rel.startswith("/") else rel
    rclow = rcheck.lower()
    for tok in _FORBIDDEN_PATH_TOKENS:
        if tok in rclow:
            return True, f"forbidden_path_token:{tok}"
    for ext in _RUNTIME_DB_EXTENSIONS:
        if rlow.endswith(ext):
            return True, f"runtime_db_ext:{ext}"
    for ext in _AUDIO_EXTENSIONS:
        if rlow.endswith(ext):
            return True, f"audio_ext:{ext}"
    return False, ""


def _sha256_streaming(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            scanned = 0
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                scanned += len(chunk)
                if scanned > _MAX_BYTES:
                    return ""
                h.update(chunk)
    except Exception:  # noqa: BLE001
        return ""
    return h.hexdigest()


def _default_source_paths() -> dict[str, Path]:
    root = Path(__file__).resolve().parent
    base = root / "bilingual_stack" / "voice_adapter_phase43"
    return {
        "portable_bundle":
            base / "portable_bundles"
                / "portable_bundle.json",
        "bundle_manifest":
            base / "bundle_manifests"
                / "bundle_manifest.json",
        "source_operator_packet":
            base / "operator_packets"
                / "operator_packet.json",
        "source_status_dashboard":
            base / "dashboards"
                / "STATUS_DASHBOARD.json",
        "source_phase43_report":
            root
                / "PHASE43_CROSS_MACHINE_PORTABILITY_REPORT.md",
    }


def create_phase44_import_workspace(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    if output_dir:
        base = Path(output_dir)
    else:
        base = (root / "bilingual_stack"
                     / "voice_adapter_phase44"
                     / "fresh_checkout_simulation")
    workspace = base / f"workspace_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    try:
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / "artifacts").mkdir(parents=True,
                                         exist_ok=True)
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"mkdir_failed:{e}",
                "phase": _PHASE}
    return {
        "ok": True,
        "workspace_path": str(workspace),
        "artifacts_dir": str(workspace / "artifacts"),
        "phase": _PHASE,
    }


def import_phase43_bundle_to_workspace(
    source_bundle_path: Optional[str] = None,
    source_manifest_path: Optional[str] = None,
    workspace_dir: Optional[str] = None,
) -> dict[str, Any]:
    paths = _default_source_paths()
    if source_bundle_path:
        paths["portable_bundle"] = Path(source_bundle_path)
    if source_manifest_path:
        paths["bundle_manifest"] = \
            Path(source_manifest_path)
    if workspace_dir:
        artifacts_dir = Path(workspace_dir) / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
    else:
        ws = create_phase44_import_workspace()
        if not ws.get("ok"):
            return {"ok": False,
                    "reason": ws.get("reason"),
                    "phase": _PHASE}
        artifacts_dir = Path(ws["artifacts_dir"])
    entries: list[dict[str, Any]] = []
    missing: list[str] = []
    rejected: list[dict[str, Any]] = []
    for key, src in paths.items():
        sp = str(src)
        rej, reason = _reject_path(sp)
        if rej:
            rejected.append({"key": key,
                              "reason": reason})
            continue
        if not src.exists() or not src.is_file():
            missing.append(key)
            continue
        try:
            size = src.stat().st_size
        except Exception:  # noqa: BLE001
            missing.append(key)
            continue
        if size > _MAX_BYTES:
            rejected.append({"key": key,
                              "reason":
                                  f"too_large:{size}"})
            continue
        sha = _sha256_streaming(src)
        target = artifacts_dir / f"{key}{src.suffix}"
        try:
            body = src.read_bytes()
            target.write_bytes(body)
        except Exception as e:  # noqa: BLE001
            rejected.append({"key": key,
                              "reason":
                                  f"copy_failed:{e}"})
            continue
        # Re-hash the copied file and compare
        copy_sha = _sha256_streaming(target)
        sha_match = (sha == copy_sha
                      and isinstance(sha, str)
                      and len(sha) == 64)
        entries.append({
            "artifact_key": key,
            "source_path": sp,
            "imported_path": str(target),
            "source_sha256": sha,
            "imported_sha256": copy_sha,
            "size_bytes": int(size),
            "sha_matches": sha_match,
        })
    return {
        "ok": not missing and not rejected,
        "phase": _PHASE,
        "import_id":
            f"p44import_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "workspace_artifacts_dir": str(artifacts_dir),
        "entries": entries,
        "missing": missing,
        "rejected": rejected,
        "imported_count": len(entries),
        "phase21_status_text": "BLOCKED",
        "boundary_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_main_runtime_integration": True,
            "no_adapter_invocation_on_import": True,
            "no_production_db_read_on_import": True,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Bundle imported via Python file read/write "
            "only.",
            "No subprocess, no network, no "
            "multiprocessing.",
            "Each imported file's SHA-256 re-derived and "
            "compared against source.",
        ],
    }


_REQUIRED_IMPORT_FIELDS = (
    "ok", "phase", "import_id", "created_at",
    "workspace_artifacts_dir", "entries",
    "imported_count", "phase21_status_text",
    "boundary_summary", "rehearsal_dry_run_only",
)


_BANNED_IMPORT_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def validate_phase44_imported_bundle(
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
    if imported.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    entries = imported.get("entries") or []
    for e in entries:
        if not isinstance(e, dict):
            reasons.append("entry_not_dict")
            continue
        if not e.get("sha_matches"):
            reasons.append(
                f"sha_mismatch:{e.get('artifact_key')}")
        if not isinstance(e.get("source_sha256"), str) \
                or len(e.get("source_sha256")) != 64:
            reasons.append(
                f"bad_source_sha:{e.get('artifact_key')}")
        if not isinstance(e.get("imported_sha256"), str) \
                or len(e.get("imported_sha256")) != 64:
            reasons.append(
                f"bad_imported_sha:"
                f"{e.get('artifact_key')}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase44_imported_bundle(
    imported: Any,
) -> dict[str, Any]:
    if not isinstance(imported, dict):
        return {"ok": False, "summary": "no_imported"}
    return {
        "ok": bool(imported.get("ok")),
        "summary": (
            f"phase44 import: count="
            f"{imported.get('imported_count')} "
            f"missing="
            f"{len(imported.get('missing') or [])} "
            f"rejected="
            f"{len(imported.get('rejected') or [])} "
            f"phase21="
            f"{imported.get('phase21_status_text')}"),
        "import_id": imported.get("import_id"),
        "phase": _PHASE,
    }


def write_phase44_imported_bundle(
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


def write_phase44_bundle_importer_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase44_imported_bundle(report,
                                           output_path)


__all__ = [
    "create_phase44_import_workspace",
    "import_phase43_bundle_to_workspace",
    "validate_phase44_imported_bundle",
    "summarize_phase44_imported_bundle",
    "write_phase44_imported_bundle",
    "write_phase44_bundle_importer_report",
]
