"""Phase 16 - Dual Corpus Chunked Importer.

Streams a registered corpus source row-by-row, validates each row, classifies
default metadata, deduplicates within the current run, and routes accepted
rows into the appropriate sovereign lexicon store:

  * Language 'en' -> ``english_knowledge_ingestion`` + ``cognitive_lexicon_store``
  * Language 'ru' -> ``russian_knowledge_ingestion`` + ``russian_lexicon_store``

Hard guarantees:
  * Never loads a full source into memory.
  * Default ``dry_run=True`` - nothing is written unless caller explicitly sets
    ``dry_run=False``.
  * Honors ``max_entries`` and a default cap of 25,000 unless
    ``allow_full_source=True``.
  * Slang / street source types auto-tag slang/street register if missing.
  * Vulgar / offensive are NEVER auto-attached to a row.
  * Rows with sensitive markers but no labels are downgraded to
    ``recognition_only`` AND ``do_not_use_unprompted`` instead of being added blind.
  * Operational unsafe content (how-to instructions) is REJECTED, not stored.
  * Quality gate must pass before a non-dry-run is allowed.
  * Writes a per-source ``corpus_import_report.json`` next to the source file.
  * Persists progress through ``dual_corpus_checkpoint`` so a partial run can
    be resumed.

No daemon, no auto-runner.
"""

from __future__ import annotations

import csv
import io
import json
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import dual_corpus_registry as reg
import dual_corpus_checkpoint as ckpt
import dual_corpus_quality_gate as qg


HARD_BATCH_CAP = 5000
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_ENTRIES = 25000


SLANG_LIKE_SOURCE_TYPES = {"slang_list", "street_talk_list"}


def _parse_jsonl_line(line: str) -> Optional[dict[str, Any]]:
    s = line.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else {"_value": obj}


def stream_jsonl_rows(path: str | Path, start_line: int = 0,
                      max_rows: Optional[int] = None
                      ) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield (line_number, byte_offset_after, row_dict) for each parsed row.

    ``start_line`` is 0-based; rows BEFORE start_line are skipped but still
    counted for byte-offset accuracy.
    """
    p = Path(path)
    if not p.exists() or not p.is_file():
        return
    cap = None if max_rows is None else max(1, int(max_rows))
    emitted = 0
    with p.open("rb") as fh:
        line_no = 0
        for raw in fh:
            try:
                line = raw.decode("utf-8", errors="replace")
            except Exception:
                line_no += 1
                continue
            if line_no >= start_line:
                obj = _parse_jsonl_line(line)
                if obj is not None:
                    yield line_no, fh.tell(), obj
                    emitted += 1
                    if cap is not None and emitted >= cap:
                        return
            line_no += 1


def stream_txt_rows(path: str | Path, start_line: int = 0,
                    max_rows: Optional[int] = None
                    ) -> Iterator[tuple[int, int, dict[str, Any]]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return
    cap = None if max_rows is None else max(1, int(max_rows))
    emitted = 0
    with p.open("rb") as fh:
        line_no = 0
        for raw in fh:
            try:
                line = raw.decode("utf-8", errors="replace")
            except Exception:
                line_no += 1
                continue
            if line_no >= start_line:
                w = line.strip()
                if w:
                    yield line_no, fh.tell(), {"word": w}
                    emitted += 1
                    if cap is not None and emitted >= cap:
                        return
            line_no += 1


def stream_csv_rows(path: str | Path, start_line: int = 0,
                    max_rows: Optional[int] = None
                    ) -> Iterator[tuple[int, int, dict[str, Any]]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return
    cap = None if max_rows is None else max(1, int(max_rows))
    emitted = 0
    with p.open("rb") as fh:
        header: Optional[list[str]] = None
        line_no = 0
        for raw in fh:
            try:
                line = raw.decode("utf-8", errors="replace")
            except Exception:
                line_no += 1
                continue
            cells = next(csv.reader(io.StringIO(line)), [])
            if header is None:
                header = [c.strip() for c in cells]
            else:
                if line_no >= start_line:
                    if header and any(c.strip() for c in cells):
                        row = {(header[i] if i < len(header) else f"col{i}"): cells[i]
                               for i in range(len(cells))}
                        yield line_no, fh.tell(), row
                        emitted += 1
                        if cap is not None and emitted >= cap:
                            return
            line_no += 1


def _stream(path: Path, fmt: str, start_line: int,
            max_rows: Optional[int]) -> Iterator[tuple[int, int, dict[str, Any]]]:
    if fmt == "jsonl":
        yield from stream_jsonl_rows(path, start_line, max_rows)
    elif fmt == "txt":
        yield from stream_txt_rows(path, start_line, max_rows)
    elif fmt == "csv":
        yield from stream_csv_rows(path, start_line, max_rows)
    else:
        return


def classify_default_metadata(row: dict[str, Any],
                              language: str,
                              source_type: str) -> dict[str, Any]:
    """Inject default register/safety/coverage hints based on source_type.

    Vulgar/offensive are NEVER attached automatically. Slang/street types
    auto-add 'slang' or 'street' register when no register tag is set.
    """
    row = dict(row)
    reg_tags = list(row.get("register_tags") or [])
    cov = list(row.get("coverage_categories") or [])
    safety = list(row.get("safety_tags") or [])

    if source_type in SLANG_LIKE_SOURCE_TYPES:
        if source_type == "slang_list" and "slang" not in reg_tags:
            reg_tags.append("slang")
        if source_type == "street_talk_list" and "street" not in reg_tags:
            reg_tags.append("street")
        if "slang_street_talk" not in cov:
            cov.append("slang_street_talk")
    if source_type == "idiom_list" and "idioms_phrases" not in cov:
        cov.append("idioms_phrases")
    if source_type == "profession_job_list" and "professions_jobs" not in cov:
        cov.append("professions_jobs")

    row["register_tags"] = reg_tags
    row["coverage_categories"] = cov
    row["safety_tags"] = safety
    row.setdefault("language", language)
    return row


def normalize_corpus_row(row: dict[str, Any], language: str,
                         source_type: str) -> dict[str, Any]:
    r = classify_default_metadata(row, language, source_type)
    if "phrase" in r and "word" not in r:
        r["word"] = r["phrase"]
    w = (r.get("word") or "").strip()
    r["word"] = w
    return r


def validate_normalized_row(row: dict[str, Any]) -> dict[str, Any]:
    """Lightweight pre-filter applied BEFORE the language-specific validator."""
    w = (row.get("word") or "").strip()
    if not w:
        return {"ok": False, "reason": "missing_word"}
    if len(w) > 200:
        return {"ok": False, "reason": "word_too_long"}

    # Operational unsafe content rejected outright.
    text_blobs: list[str] = []
    for k in ("definition", "examples", "notes", "explanation"):
        v = row.get(k)
        if isinstance(v, str):
            text_blobs.append(v)
        elif isinstance(v, list):
            text_blobs.extend(str(x) for x in v)
    big = " ".join(text_blobs).lower()
    for marker in qg._SENSITIVE_FREE_TEXT_MARKERS:
        if marker in big:
            return {"ok": False,
                    "reason": f"operational_unsafe_content: {marker!r}"}
    return {"ok": True}


def _ensure_sensitive_unlabeled_downgrade(row: dict[str, Any]) -> dict[str, Any]:
    """If detect_unsafe_unlabeled flags 'needs_recognition_only' and the row
    carries no recognition_only tag, mark it as recognition_only +
    do_not_use_unprompted instead of inheriting bare vulgar/offensive."""
    s = qg.detect_unsafe_unlabeled(row)
    if s["needs_recognition_only"]:
        safety = set(row.get("safety_tags") or [])
        register = set(row.get("register_tags") or [])
        safety.add("recognition_only")
        safety.add("do_not_use_unprompted")
        register.add("recognition_only")
        register.add("do_not_use_unprompted")
        row["safety_tags"] = sorted(safety)
        row["register_tags"] = sorted(register)
    return row


def detect_duplicate(seen: set[str], word: str) -> bool:
    key = (word or "").strip().lower()
    if not key:
        return False
    if key in seen:
        return True
    seen.add(key)
    return False


def write_rejection(rejection_log_path: Path,
                    rejection: dict[str, Any]) -> None:
    rejection_log_path.parent.mkdir(parents=True, exist_ok=True)
    with rejection_log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rejection, ensure_ascii=False) + "\n")


def _route_en_row(row: dict[str, Any], db_path: Optional[str | Path],
                  pack_id: str) -> dict[str, Any]:
    import cognitive_lexicon_store as enlex
    import english_knowledge_ingestion as enkg
    v = enkg.validate_english_entry(row)
    if not v["ok"]:
        return {"ok": False, "reason": v.get("reason", "validation_failed")}
    norm = v["normalized"]
    enlex.add_word(
        source="corpus_import",
        pack_source=norm.get("pack_source") or "corpus_import",
        pack_id=norm.get("pack_id") or pack_id,
        db_path=db_path,
        **{k: norm[k] for k in (
            "word", "definition", "synonyms", "examples", "tags",
            "language", "frequency_score", "word_level",
            "register_tags", "safety_tags", "coverage_categories",
        )},
    )
    return {"ok": True}


def _route_ru_row(row: dict[str, Any], db_path: Optional[str | Path],
                  pack_id: str) -> dict[str, Any]:
    import russian_lexicon_store as rulex
    import russian_knowledge_ingestion as rukg
    v = rukg.validate_russian_entry(row)
    if not v["ok"]:
        return {"ok": False, "reason": v.get("reason", "validation_failed")}
    norm = dict(v["normalized"])
    if not norm.get("pack_source"):
        norm["pack_source"] = "corpus_import"
    norm.setdefault("pack_id", pack_id)
    rulex.add_word(source="corpus_import", db_path=db_path, **norm)
    return {"ok": True}


def get_corpus_import_stats(report_path: str | Path) -> dict[str, Any]:
    p = Path(report_path)
    if not p.exists():
        return {"ok": False, "error": "report_not_found", "path": str(p)}
    try:
        return {"ok": True, "report": json.loads(p.read_text(encoding="utf-8"))}
    except Exception as e:
        return {"ok": False, "error": f"read_failed: {e}", "path": str(p)}


def write_corpus_import_report(payload: dict[str, Any],
                               output_path: str | Path) -> str:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body["report_generated_at"] = time.time()
    out.write_text(json.dumps(body, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    return str(out)


def import_file(
    path: str | Path,
    language: str,
    source_type: str,
    expected_format: str,
    *,
    corpus_id: str = "ad_hoc",
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_entries: Optional[int] = None,
    dry_run: bool = True,
    allow_full_source: bool = False,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
    checkpoint_id: Optional[str] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    rejections_dir: Optional[str | Path] = None,
    reports_dir: Optional[str | Path] = None,
    quality_min_score: float = 0.75,
    skip_quality_gate: bool = False,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    if expected_format not in ("jsonl", "txt", "csv"):
        return {"ok": False,
                "error": f"invalid_expected_format: {expected_format!r}"}

    bs = max(1, min(int(batch_size), HARD_BATCH_CAP))
    cap = max_entries
    if cap is None:
        cap = None if allow_full_source else DEFAULT_MAX_ENTRIES
    else:
        cap = max(1, int(cap))

    # Quality gate (unless explicitly skipped, e.g. for synthetic tests).
    quality_report: Optional[dict[str, Any]] = None
    if not skip_quality_gate:
        quality_report = qg.generate_quality_gate_report(
            p, expected_format, language,
            sample_size=min(100, cap if cap is not None else 100))
        gate = qg.should_allow_import(quality_report,
                                      min_quality_score=quality_min_score)
        if not gate["ok"] and not dry_run:
            return {"ok": False, "error": "quality_gate_blocked",
                    "quality_report": quality_report, "gate_decision": gate}

    # Checkpoint
    start_line = 0
    if checkpoint_id:
        loaded = ckpt.load_checkpoint(checkpoint_id, db_path=checkpoint_db_path)
        if loaded:
            start_line = int(loaded.get("last_line_number") or 0)
        else:
            return {"ok": False,
                    "error": f"checkpoint_not_found: {checkpoint_id!r}"}
    else:
        created = ckpt.create_checkpoint(
            corpus_id=corpus_id, source_path=str(p),
            language=language, db_path=checkpoint_db_path,
            notes=f"chunked_import dry_run={dry_run}")
        checkpoint_id = created["checkpoint_id"]

    rdir = Path(rejections_dir) if rejections_dir else (
        Path("corpus_sources") / ("english" if language == "en" else "russian")
        / "rejected")
    rep_dir = Path(reports_dir) if reports_dir else (
        Path("corpus_sources") / ("english" if language == "en" else "russian")
        / "reports")
    rdir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    rejection_log = rdir / f"{p.stem}.rejected.jsonl"

    accepted = 0
    rejected = 0
    duplicates = 0
    batches = 0
    seen_in_run: set[str] = set()
    last_line = start_line
    last_offset = 0
    rejection_samples: list[dict[str, Any]] = []

    pack_id = f"corpus_{language}_{source_type}_{int(time.time())}"

    for line_no, byte_off, row in _stream(p, expected_format, start_line, cap):
        last_line = line_no + 1
        last_offset = byte_off
        norm = normalize_corpus_row(row, language, source_type)
        pre = validate_normalized_row(norm)
        if not pre["ok"]:
            rejected += 1
            rec = {"line": line_no, "reason": pre["reason"],
                   "preview": (norm.get("word") or "")[:80]}
            if len(rejection_samples) < 20:
                rejection_samples.append(rec)
            if not dry_run:
                write_rejection(rejection_log, rec)
            continue
        norm = _ensure_sensitive_unlabeled_downgrade(norm)
        if detect_duplicate(seen_in_run, norm["word"]):
            duplicates += 1
            continue

        if dry_run:
            accepted += 1
        else:
            if language == "en":
                r = _route_en_row(norm, en_db_path, pack_id)
            else:
                r = _route_ru_row(norm, ru_db_path, pack_id)
            if r["ok"]:
                accepted += 1
            else:
                rejected += 1
                rec = {"line": line_no, "reason": r.get("reason", "route_failed"),
                       "preview": (norm.get("word") or "")[:80]}
                if len(rejection_samples) < 20:
                    rejection_samples.append(rec)
                write_rejection(rejection_log, rec)

        if (accepted + rejected + duplicates) and \
                (accepted + rejected + duplicates) % bs == 0:
            batches += 1
            ckpt.update_checkpoint(checkpoint_id,
                                   last_byte_offset=last_offset,
                                   last_line_number=last_line,
                                   accepted_count=accepted,
                                   rejected_count=rejected,
                                   duplicate_count=duplicates,
                                   batch_count=batches,
                                   db_path=checkpoint_db_path)

    ckpt.update_checkpoint(checkpoint_id,
                           last_byte_offset=last_offset,
                           last_line_number=last_line,
                           accepted_count=accepted,
                           rejected_count=rejected,
                           duplicate_count=duplicates,
                           batch_count=batches + 1,
                           db_path=checkpoint_db_path)
    ckpt.mark_checkpoint_complete(checkpoint_id,
                                  notes=f"dry_run={dry_run}",
                                  db_path=checkpoint_db_path)

    report_payload = {
        "ok": True,
        "path": str(p),
        "language": language,
        "source_type": source_type,
        "expected_format": expected_format,
        "corpus_id": corpus_id,
        "checkpoint_id": checkpoint_id,
        "dry_run": dry_run,
        "allow_full_source": allow_full_source,
        "max_entries_applied": cap,
        "batch_size_applied": bs,
        "accepted": accepted,
        "rejected": rejected,
        "duplicates": duplicates,
        "batches": batches + 1,
        "rejection_samples": rejection_samples,
        "quality_report": quality_report,
        "pack_id": pack_id,
    }
    report_path = rep_dir / f"{p.stem}.corpus_import_report.json"
    write_corpus_import_report(report_payload, report_path)
    report_payload["report_path"] = str(report_path)
    return report_payload


def import_corpus(
    corpus_id: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_entries: Optional[int] = None,
    dry_run: bool = True,
    allow_full_source: bool = False,
    resume_checkpoint_id: Optional[str] = None,
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
    rejections_dir: Optional[str | Path] = None,
    reports_dir: Optional[str | Path] = None,
    quality_min_score: float = 0.75,
    skip_quality_gate: bool = False,
) -> dict[str, Any]:
    rec = reg.get_corpus_source(corpus_id, db_path=registry_db_path)
    if rec is None:
        return {"ok": False, "error": f"corpus_id_not_found: {corpus_id!r}"}
    reg.update_corpus_status(corpus_id, "in_progress",
                             notes=f"chunked dry_run={dry_run}",
                             db_path=registry_db_path)
    result = import_file(
        path=rec["source_path"],
        language=rec["language"],
        source_type=rec["source_type"],
        expected_format=rec["expected_format"],
        corpus_id=corpus_id,
        batch_size=batch_size,
        max_entries=max_entries,
        dry_run=dry_run,
        allow_full_source=allow_full_source,
        en_db_path=en_db_path,
        ru_db_path=ru_db_path,
        checkpoint_id=resume_checkpoint_id,
        checkpoint_db_path=checkpoint_db_path,
        rejections_dir=rejections_dir,
        reports_dir=reports_dir,
        quality_min_score=quality_min_score,
        skip_quality_gate=skip_quality_gate,
    )
    new_status = "completed" if result.get("ok") else "failed"
    reg.update_corpus_status(corpus_id, new_status,
                             notes=result.get("error", ""),
                             db_path=registry_db_path)
    return result


__all__ = [
    "HARD_BATCH_CAP",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_MAX_ENTRIES",
    "stream_jsonl_rows",
    "stream_txt_rows",
    "stream_csv_rows",
    "normalize_corpus_row",
    "validate_normalized_row",
    "classify_default_metadata",
    "detect_duplicate",
    "write_rejection",
    "write_corpus_import_report",
    "get_corpus_import_stats",
    "import_file",
    "import_corpus",
]
