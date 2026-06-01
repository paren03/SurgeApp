"""Phase 43 - Bundle Builder.

Collects Phase 42 JSON/Markdown artifacts and the Phase 42 report
into a portable witness bundle. Hashes via streaming SHA-256.
Excludes runtime DBs, secrets, audio, corpus incoming, .claude/,
backups, and local_secret_handoff.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase43.bundle_builder.v1"


_MAX_INLINE_BYTES = 512 * 1024
_MAX_HASH_BYTES = 16 * 1024 * 1024


# Artifact key -> relative path under repo root
_PHASE42_ARTIFACT_PATHS = {
    "phase42_audit_contract":
        "bilingual_stack/voice_adapter_phase42/"
        "contracts/audit_contract.json",
    "phase42_trace_batch":
        "bilingual_stack/voice_adapter_phase42/"
        "trace_runs/trace_batch.json",
    "phase42_coherence_audit":
        "bilingual_stack/voice_adapter_phase42/"
        "coherence_audits/coherence_audit.json",
    "phase42_replay_matrix":
        "bilingual_stack/voice_adapter_phase42/"
        "replay_projections/replay_matrix.json",
    "phase42_drift_stability_matrix":
        "bilingual_stack/voice_adapter_phase42/"
        "drift_matrices/drift_stability_matrix.json",
    "phase42_operator_packet":
        "bilingual_stack/voice_adapter_phase42/"
        "operator_packets/operator_packet.json",
    "phase42_operator_markdown":
        "bilingual_stack/voice_adapter_phase42/"
        "dashboards/OPERATOR_PACKET.md",
    "phase42_report":
        "PHASE42_MULTI_TRACE_COHERENCE_AUDIT_REPORT.md",
}


_EXCLUDED_TOKENS = (
    ".sqlite", ".sqlite3", ".db",
    ".wav", ".mp3", ".ogg", ".flac",
    ".m4a", ".aac", ".opus",
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/", "/local_secret_handoff/",
    "/corpus_sources/english/incoming/",
    "/corpus_sources/russian/incoming/",
    "/.claude/",
)


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
                if scanned > _MAX_HASH_BYTES:
                    return ""
                h.update(chunk)
    except Exception:  # noqa: BLE001
        return ""
    return h.hexdigest()


def _is_excluded(path_str: str) -> bool:
    norm = str(path_str).replace("\\", "/").lower()
    for tok in _EXCLUDED_TOKENS:
        if tok in norm:
            return True
        if tok.startswith(".") and norm.endswith(tok):
            return True
    return False


def _classify(path: Path) -> str:
    s = str(path).replace("\\", "/").lower()
    if s.endswith(".json"):
        return "json"
    if s.endswith(".md"):
        return "markdown"
    return "other"


def collect_phase42_bundle_artifacts(
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    root = Path(base_dir) if base_dir else \
        Path(__file__).resolve().parent
    entries: list[dict[str, Any]] = []
    missing: list[str] = []
    excluded: list[str] = []
    for key, rel in _PHASE42_ARTIFACT_PATHS.items():
        p = root / rel
        # Exclusion check uses the declared relative path
        # only, so a worktree path that happens to contain
        # ".claude/" doesn't false-positive the audit.
        if _is_excluded(rel):
            excluded.append(key)
            continue
        if not p.exists() or not p.is_file():
            missing.append(key)
            continue
        try:
            size = p.stat().st_size
        except Exception:  # noqa: BLE001
            missing.append(key)
            continue
        kind = _classify(p)
        sha = _sha256_streaming(p)
        inline_content: Any = None
        if size <= _MAX_INLINE_BYTES:
            try:
                body = p.read_text(encoding="utf-8",
                                    errors="ignore")
                if kind == "json":
                    try:
                        inline_content = json.loads(body)
                    except Exception:  # noqa: BLE001
                        inline_content = None
                else:
                    inline_content = body
            except Exception:  # noqa: BLE001
                inline_content = None
        entries.append({
            "artifact_key": key,
            "relative_path": rel,
            "absolute_path": str(p),
            "artifact_type": kind,
            "size_bytes": int(size),
            "sha256": sha,
            "inline_content_present":
                inline_content is not None,
            "inline_content": inline_content,
        })
    return {
        "collection_id":
            f"p43coll_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "phase": _PHASE,
        "root": str(root),
        "entries": entries,
        "missing": missing,
        "excluded_keys": excluded,
        "count": len(entries),
    }


def compute_phase43_artifact_hashes(
    artifacts: Any,
) -> dict[str, str]:
    if not isinstance(artifacts, dict):
        return {}
    out: dict[str, str] = {}
    for e in artifacts.get("entries") or []:
        if not isinstance(e, dict):
            continue
        key = e.get("artifact_key")
        sha = e.get("sha256")
        if key and isinstance(sha, str) and len(sha) == 64:
            out[key] = sha
    return out


def create_phase43_portable_bundle(
    contract: Optional[dict[str, Any]] = None,
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    artifacts = collect_phase42_bundle_artifacts(
        base_dir=base_dir)
    hashes = compute_phase43_artifact_hashes(artifacts)
    return {
        "bundle_id":
            f"p43bndl_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase": "phase42",
        "contract_id":
            (contract or {}).get("contract_id", ""),
        "artifacts": artifacts.get("entries") or [],
        "artifact_count": artifacts.get("count") or 0,
        "missing_artifacts":
            artifacts.get("missing") or [],
        "excluded_artifact_keys":
            artifacts.get("excluded_keys") or [],
        "artifact_hashes": hashes,
        "phase21_status_text": "BLOCKED",
        "production_baseline_expected": {
            "english_words": 2814,
            "russian_words": 2518,
            "russian_phrases": 35,
            "bilingual_concepts": 26,
            "bilingual_entry_links": 52,
            "live_pack_manifests": 90,
        },
        "boundary_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_main_runtime_integration": True,
            "no_adapter_reinvocation_in_bundle": True,
            "no_production_db_in_bundle": True,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Bundle is portable JSON/Markdown only.",
            "Inline content capped at 512 KB per file.",
            "SHA-256 hashes computed via streaming "
            "read, capped at 16 MB.",
            "Runtime DBs, secrets, audio, corpus "
            "incoming, .claude/, backups, and "
            "local_secret_handoff are excluded.",
        ],
    }


_REQUIRED_BUNDLE_FIELDS = (
    "bundle_id", "created_at", "phase",
    "source_phase", "artifacts",
    "artifact_count", "artifact_hashes",
    "phase21_status_text",
    "production_baseline_expected",
    "boundary_summary",
    "rehearsal_dry_run_only",
)


_BANNED_BUNDLE_FIELDS = (
    "raw_transcript", "full_transcript",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex",
    "sealed_payload", "audio_bytes",
    "audio_path", "audio_file",
    "command", "command_line",
)


def validate_phase43_portable_bundle(
    bundle: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(bundle, dict):
        return {"ok": False,
                "reasons": ["bundle_not_dict"]}
    for f in _REQUIRED_BUNDLE_FIELDS:
        if f not in bundle:
            reasons.append(f"missing_field:{f}")
    if bundle.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    for k in _BANNED_BUNDLE_FIELDS:
        if k in bundle and bundle.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    # No entry's RELATIVE path should match excluded
    # tokens (absolute paths can legitimately live inside
    # a worktree such as ".claude/worktrees/...").
    for e in bundle.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        rp = str(e.get("relative_path") or "")
        if _is_excluded(rp):
            reasons.append(
                f"excluded_artifact_in_bundle:"
                f"{e.get('artifact_key')}")
        sz = e.get("size_bytes")
        if not (isinstance(sz, int) and sz >= 0):
            reasons.append(
                f"bad_size_bytes:{e.get('artifact_key')}")
        sha = e.get("sha256")
        if not isinstance(sha, str) or len(sha) != 64:
            reasons.append(
                f"bad_sha256:{e.get('artifact_key')}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase43_portable_bundle(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False, "summary": "no_bundle"}
    return {
        "ok": True,
        "summary": (
            f"phase43 bundle: artifacts="
            f"{bundle.get('artifact_count')} "
            f"missing="
            f"{len(bundle.get('missing_artifacts') or [])} "
            f"phase21="
            f"{bundle.get('phase21_status_text')}"),
        "bundle_id": bundle.get("bundle_id"),
        "phase": _PHASE,
    }


def write_phase43_portable_bundle(
    bundle: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(bundle)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase43_bundle_builder_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase43_portable_bundle(report, output_path)


__all__ = [
    "collect_phase42_bundle_artifacts",
    "compute_phase43_artifact_hashes",
    "create_phase43_portable_bundle",
    "validate_phase43_portable_bundle",
    "summarize_phase43_portable_bundle",
    "write_phase43_portable_bundle",
    "write_phase43_bundle_builder_report",
]
