"""Phase 45 - Multi-Bundle Archive Builder.

Collects Phase 42/43/44 JSON+Markdown+report artifacts into a
single portable archive. Streaming SHA-256, 512 KB inline cap.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase45.archive_builder.v1"


_MAX_INLINE_BYTES = 512 * 1024
_MAX_HASH_BYTES = 16 * 1024 * 1024


_ARTIFACT_PATHS: dict[str, tuple[str, str]] = {
    # phase 42
    "phase42_audit_contract": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "contracts/audit_contract.json"),
    "phase42_trace_batch": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "trace_runs/trace_batch.json"),
    "phase42_coherence_audit": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "coherence_audits/coherence_audit.json"),
    "phase42_replay_matrix": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "replay_projections/replay_matrix.json"),
    "phase42_drift_stability_matrix": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "drift_matrices/drift_stability_matrix.json"),
    "phase42_operator_packet": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "operator_packets/operator_packet.json"),
    "phase42_operator_markdown": (
        "phase42",
        "bilingual_stack/voice_adapter_phase42/"
        "dashboards/OPERATOR_PACKET.md"),
    # phase 43
    "phase43_portable_bundle": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "portable_bundles/portable_bundle.json"),
    "phase43_bundle_manifest": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "bundle_manifests/bundle_manifest.json"),
    "phase43_fresh_checkout_result": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "fresh_checkout_outputs/"
        "fresh_checkout_result.json"),
    "phase43_portability_audit": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "portability_audits/portability_audit.json"),
    "phase43_operator_packet": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "operator_packets/operator_packet.json"),
    "phase43_status_dashboard_json": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "dashboards/STATUS_DASHBOARD.json"),
    "phase43_status_dashboard_md": (
        "phase43",
        "bilingual_stack/voice_adapter_phase43/"
        "dashboards/STATUS_DASHBOARD.md"),
    # phase 44
    "phase44_imported_bundle": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "imported_bundles/imported_bundle.json"),
    "phase44_import_manifest": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "roundtrip_manifests/import_manifest.json"),
    "phase44_fresh_import_result": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "verification_outputs/"
        "fresh_import_result.json"),
    "phase44_tamper_suite": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "tamper_tests/tamper_suite.json"),
    "phase44_roundtrip_receipt": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "reports/roundtrip_receipt.json"),
    "phase44_operator_packet": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "operator_packets/operator_packet.json"),
    "phase44_status_dashboard_json": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "dashboards/STATUS_DASHBOARD.json"),
    "phase44_status_dashboard_md": (
        "phase44",
        "bilingual_stack/voice_adapter_phase44/"
        "dashboards/STATUS_DASHBOARD.md"),
    # reports
    "phase42_report": (
        "phase42",
        "PHASE42_MULTI_TRACE_COHERENCE_AUDIT_REPORT.md"),
    "phase43_report": (
        "phase43",
        "PHASE43_CROSS_MACHINE_PORTABILITY_REPORT.md"),
    "phase44_report": (
        "phase44",
        "PHASE44_CROSS_MACHINE_IMPORT_SIMULATION_REPORT.md"),
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


def _is_excluded(rel: str) -> bool:
    norm = rel.replace("\\", "/").lower()
    rcheck = norm if norm.startswith("/") else "/" + norm
    for tok in _EXCLUDED_TOKENS:
        if tok.startswith(".") and norm.endswith(tok):
            return True
        if not tok.startswith(".") and tok in rcheck:
            return True
    return False


def _classify(p: Path) -> str:
    s = str(p).lower()
    if s.endswith(".json"):
        return "json"
    if s.endswith(".md"):
        return "markdown"
    return "other"


def collect_phase45_archive_artifacts(
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    root = Path(base_dir) if base_dir else \
        Path(__file__).resolve().parent
    entries: list[dict[str, Any]] = []
    missing: list[str] = []
    excluded: list[str] = []
    for key, (src_phase, rel) in _ARTIFACT_PATHS.items():
        if _is_excluded(rel):
            excluded.append(key)
            continue
        p = root / rel
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
            "source_phase": src_phase,
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
            f"p45coll_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "phase": _PHASE,
        "root": str(root),
        "entries": entries,
        "missing": missing,
        "excluded_keys": excluded,
        "count": len(entries),
    }


def compute_phase45_archive_hashes(
    artifacts: Any,
) -> dict[str, str]:
    if not isinstance(artifacts, dict):
        return {}
    out: dict[str, str] = {}
    for e in artifacts.get("entries") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if k and isinstance(sha, str) and len(sha) == 64:
            out[k] = sha
    return out


def create_phase45_archive(
    contract: Optional[dict[str, Any]] = None,
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    artifacts = collect_phase45_archive_artifacts(
        base_dir=base_dir)
    hashes = compute_phase45_archive_hashes(artifacts)
    phase_counts: dict[str, int] = {}
    for e in artifacts.get("entries") or []:
        if not isinstance(e, dict):
            continue
        sp = e.get("source_phase") or "unknown"
        phase_counts[sp] = phase_counts.get(sp, 0) + 1
    return {
        "archive_id":
            f"p45arc_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phases":
            sorted(phase_counts.keys()),
        "phase_counts": phase_counts,
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
            "no_adapter_invocation_in_archive": True,
            "no_production_db_read_in_archive": True,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Archive aggregates Phase 42/43/44 portable "
            "JSON+Markdown+report artifacts only.",
            "Inline content capped at 512 KB per file; "
            "SHA-256 streamed (cap 16 MB).",
            "Runtime DBs, secrets, audio, corpus "
            "incoming, .claude/, backups, "
            "local_secret_handoff: all excluded.",
        ],
    }


_REQUIRED_ARCHIVE_FIELDS = (
    "archive_id", "created_at", "phase",
    "source_phases", "phase_counts",
    "artifacts", "artifact_count",
    "artifact_hashes",
    "phase21_status_text",
    "production_baseline_expected",
    "boundary_summary",
    "rehearsal_dry_run_only",
)


_BANNED_ARCHIVE_FIELDS = (
    "raw_transcript", "full_transcript",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex",
    "sealed_payload", "audio_bytes",
    "audio_path", "audio_file",
    "command", "command_line",
)


def validate_phase45_archive(
    archive: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(archive, dict):
        return {"ok": False,
                "reasons": ["archive_not_dict"]}
    for f in _REQUIRED_ARCHIVE_FIELDS:
        if f not in archive:
            reasons.append(f"missing_field:{f}")
    if archive.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    for k in _BANNED_ARCHIVE_FIELDS:
        if k in archive and archive.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        rp = str(e.get("relative_path") or "")
        if _is_excluded(rp):
            reasons.append(
                f"excluded_artifact_in_archive:"
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


def summarize_phase45_archive(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"ok": False, "summary": "no_archive"}
    return {
        "ok": True,
        "summary": (
            f"phase45 archive: artifacts="
            f"{archive.get('artifact_count')} "
            f"phases={archive.get('source_phases')} "
            f"missing="
            f"{len(archive.get('missing_artifacts') or [])} "
            f"phase21="
            f"{archive.get('phase21_status_text')}"),
        "archive_id": archive.get("archive_id"),
        "phase": _PHASE,
    }


def write_phase45_archive(
    archive: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(archive)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase45_archive_builder_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase45_archive(report, output_path)


__all__ = [
    "collect_phase45_archive_artifacts",
    "compute_phase45_archive_hashes",
    "create_phase45_archive",
    "validate_phase45_archive",
    "summarize_phase45_archive",
    "write_phase45_archive",
    "write_phase45_archive_builder_report",
]
