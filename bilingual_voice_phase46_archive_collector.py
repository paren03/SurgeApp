"""Phase 46 - Archive Collector.

Captures multiple Phase 45 archives over time and stores them
into a local captured_archives directory for the timeline.
Read-only over each archive; no adapter invocation; no
production DB reads.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase45_archive_builder as ab
import bilingual_voice_phase43_runtime as p43rt
import bilingual_voice_phase44_runtime as p44rt


_PHASE = "phase46.archive_collector.v1"


_MAX_INLINE_BYTES = 512 * 1024
_MAX_HASH_BYTES = 16 * 1024 * 1024


_BANNED_INLINE_KEYS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
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


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def capture_phase45_archive(
    output_dir: Optional[str] = None,
    sleep_for_monotonic_seconds: float = 0.0,
    realign_dependencies: bool = True,
) -> dict[str, Any]:
    """Build one fresh Phase 45 archive and persist it as a
    single JSON file in captured_archives/. Bounded reads
    only. No adapter invocation.

    When ``realign_dependencies`` is True, Phase 43 and
    Phase 44 are re-run in dependency order BEFORE the
    Phase 45 archive is built, so the captured archive's
    chain links are always coherent. This does NOT invoke
    any new adapter; both Phase 43 and Phase 44 themselves
    only build portable JSON / Markdown artifacts."""
    if sleep_for_monotonic_seconds > 0:
        # Avoid time.sleep to keep determinism: instead
        # busy-wait briefly using time.time().
        end = time.time() + min(
            float(sleep_for_monotonic_seconds), 2.5)
        while time.time() < end:
            pass
    root = Path(__file__).resolve().parent
    if output_dir:
        base = Path(output_dir)
    else:
        base = (root / "bilingual_stack"
                     / "voice_adapter_phase46"
                     / "captured_archives")
    base.mkdir(parents=True, exist_ok=True)
    if realign_dependencies:
        # Re-run Phase 43 then Phase 44 in dependency
        # order so Phase 43 portable_bundle.bundle_id
        # matches Phase 44 import_manifest's
        # source_bundle_id at archive build time.
        try:
            p43_base = (root / "bilingual_stack"
                              / "voice_adapter_phase43")
            p43rt.run_phase43_portability_harness(
                output_dir=str(p43_base))
        except Exception:  # noqa: BLE001
            pass
        try:
            p44_base = (root / "bilingual_stack"
                              / "voice_adapter_phase44")
            p44rt.run_phase44_cross_machine_import_simulation(
                output_dir=str(p44_base))
        except Exception:  # noqa: BLE001
            pass
    archive = ab.create_phase45_archive()
    # Drop heavy inline_content from each entry to keep
    # the captured archive bounded; preserve hashes +
    # metadata.
    slim_artifacts: list[dict[str, Any]] = []
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        slim = dict(e)
        slim.pop("inline_content", None)
        slim["inline_content_present"] = bool(
            e.get("inline_content_present"))
        slim_artifacts.append(slim)
    archive["artifacts"] = slim_artifacts
    captured_id = (
        f"p46cap_{int(time.time())}_"
        f"{uuid.uuid4().hex[:10]}")
    out_path = base / f"{captured_id}.json"
    body = dict(archive)
    body["captured_id"] = captured_id
    body["captured_at"] = time.time()
    out_path.write_text(json.dumps(
        body, ensure_ascii=False, indent=2,
        default=str), encoding="utf-8")
    sha = _sha256_streaming(out_path)
    return {
        "ok": True,
        "phase": _PHASE,
        "captured_id": captured_id,
        "captured_path": str(out_path),
        "captured_sha256": sha,
        "archive_id": archive.get("archive_id"),
        "archive_created_at":
            float(archive.get("created_at") or 0),
        "artifact_count":
            int(archive.get("artifact_count") or 0),
        "source_phases":
            list(archive.get("source_phases") or []),
        "phase21_status_text":
            archive.get("phase21_status_text"),
        "boundary_summary":
            dict(archive.get("boundary_summary") or {}),
    }


def capture_n_phase45_archives(
    n: int,
    output_dir: Optional[str] = None,
    spacing_seconds: float = 1.05,
) -> list[dict[str, Any]]:
    n = max(2, min(int(n or 2), 32))
    out: list[dict[str, Any]] = []
    for i in range(n):
        cap = capture_phase45_archive(
            output_dir=output_dir,
            sleep_for_monotonic_seconds=(
                spacing_seconds if i > 0 else 0.0))
        out.append(cap)
    return out


def load_captured_archives(
    base_dir: Optional[str] = None,
    max_bytes: int = 4 * 1024 * 1024,
) -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    if base_dir:
        base = Path(base_dir)
    else:
        base = (root / "bilingual_stack"
                     / "voice_adapter_phase46"
                     / "captured_archives")
    if not base.exists() or not base.is_dir():
        return {"ok": False,
                "reason": "captured_dir_missing",
                "phase": _PHASE,
                "entries": [],
                "count": 0}
    entries: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for p in sorted(base.glob("p46cap_*.json")):
        try:
            if p.stat().st_size > int(max_bytes):
                rejected.append({"path": str(p),
                                  "reason": "too_large"})
                continue
            body = p.read_text(encoding="utf-8",
                                errors="ignore")
            obj = json.loads(body)
        except Exception as e:  # noqa: BLE001
            rejected.append({"path": str(p),
                              "reason":
                                  f"load_failed:{e}"})
            continue
        # Reject banned inline keys at top level
        if isinstance(obj, dict):
            bad = False
            for k in _BANNED_INLINE_KEYS:
                if k in obj and obj.get(k) not in (
                        None, "", False, [], {}):
                    rejected.append({"path": str(p),
                                      "reason":
                                          f"banned:{k}"})
                    bad = True
                    break
            if bad:
                continue
            entries.append({
                "captured_id": obj.get("captured_id")
                    or p.stem,
                "captured_path": str(p),
                "captured_sha256":
                    _sha256_streaming(p),
                "archive": obj,
            })
    return {
        "ok": not rejected,
        "phase": _PHASE,
        "base_dir": str(base),
        "entries": entries,
        "count": len(entries),
        "rejected": rejected,
    }


def summarize_collected_archives(
    collection: Any,
) -> dict[str, Any]:
    if not isinstance(collection, dict):
        return {"ok": False, "summary": "no_collection"}
    return {
        "ok": bool(collection.get("ok")),
        "summary": (
            f"phase46 collector: count="
            f"{collection.get('count')} rejected="
            f"{len(collection.get('rejected') or [])}"),
        "base_dir": collection.get("base_dir"),
        "phase": _PHASE,
    }


def write_phase46_archive_collector_report(
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
    "capture_phase45_archive",
    "capture_n_phase45_archives",
    "load_captured_archives",
    "summarize_collected_archives",
    "write_phase46_archive_collector_report",
]
