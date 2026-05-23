"""English Sovereign Knowledge + Vocabulary — chunked ingestion peer.

Mirror of `russian_knowledge_ingestion` for Track A. Local files only.
Bounded batches. Validates rows. Preserves register_tags / safety_tags /
coverage_categories / domain_tags. Emits both an ingestion report (JSON)
and a pack manifest via `pack_manifest.create_pack_manifest`.

Supported formats: .jsonl, .json (top-level list or dict), .csv, .txt.
"""

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import cognitive_lexicon_store as lex
import coverage_taxonomy as tax
import pack_manifest as pm

FEATURE_FLAG = "LUNA_VOCABULARY_RUNTIME"

DEFAULT_BATCH_SIZE = 500
HARD_MAX_BATCH_SIZE = 5000
PREVIEW_HARD_MAX = 100


def _flag_enabled() -> bool:
    return os.environ.get(FEATURE_FLAG, "").strip() in ("1", "true", "yes", "on")


def _clamp_batch(n: Optional[int]) -> int:
    if n is None:
        return DEFAULT_BATCH_SIZE
    try:
        v = int(n)
    except (TypeError, ValueError):
        return DEFAULT_BATCH_SIZE
    if v <= 0:
        return DEFAULT_BATCH_SIZE
    return min(v, HARD_MAX_BATCH_SIZE)


def _to_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [s.strip() for s in v.split(",") if s.strip()]
    return []


def _to_float(v: Any) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _stream_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"__error__": "non_dict_entry"}
            except json.JSONDecodeError:
                yield {"__error__": "json_decode", "__raw__": ln[:200]}


def _stream_json_array(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            yield {"__error__": f"json_decode: {e}"}
            return
    if isinstance(data, list):
        for item in data:
            yield item if isinstance(item, dict) else {"__error__": "non_dict_entry"}
    elif isinstance(data, dict):
        yield data
    else:
        yield {"__error__": "unexpected_top_level"}


def _stream_txt(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            t = ln.strip()
            if not t or t.startswith("#"):
                continue
            yield {"word": t}


def _stream_csv(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}


def _stream(path: Path) -> Iterator[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        yield from _stream_jsonl(path)
    elif suffix == ".json":
        yield from _stream_json_array(path)
    elif suffix == ".csv":
        yield from _stream_csv(path)
    else:
        yield from _stream_txt(path)


def validate_english_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return {ok, reason, normalized}. Never raises."""
    if not isinstance(entry, dict):
        return {"ok": False, "reason": "not_a_dict", "normalized": {}}
    if "__error__" in entry:
        return {"ok": False, "reason": entry["__error__"], "normalized": {}}
    word = (entry.get("word") or entry.get("lemma") or "").strip().lower()
    if not word:
        return {"ok": False, "reason": "missing_word", "normalized": {}}
    if not all(0x0020 <= ord(c) <= 0x024F or c == "'" or c == "-" for c in word):
        return {"ok": False, "reason": "non_latin_chars", "normalized": {}}

    reg_v = tax.validate_register_tags(_to_list(entry.get("register_tags")))
    saf_v = tax.validate_safety_tags(_to_list(entry.get("safety_tags")))
    cov_v = tax.validate_coverage_categories(_to_list(entry.get("coverage_categories")))

    norm = {
        "word": word[:128],
        "definition": (entry.get("definition") or entry.get("definition_en") or "")[:2000],
        "synonyms": _to_list(entry.get("synonyms")),
        "examples": _to_list(entry.get("examples")),
        "tags": _to_list(entry.get("tags") or entry.get("domain")),
        "language": (entry.get("language") or "en").strip()[:8].lower(),
        "frequency_score": _to_float(entry.get("frequency_score")),
        "word_level": (entry.get("word_level") or "plain").strip()[:32].lower(),
        "register_tags": reg_v["accepted"],
        "safety_tags": saf_v["accepted"],
        "coverage_categories": cov_v["accepted"],
        "pack_source": (entry.get("pack_source") or "").strip()[:120],
        "pack_id": (entry.get("pack_id") or "").strip()[:64],
    }
    return {"ok": True, "reason": None, "normalized": norm,
            "taxonomy_rejected": {
                "register_tags": reg_v["rejected"],
                "safety_tags": saf_v["rejected"],
                "coverage_categories": cov_v["rejected"],
            }}


def validate_english_phrase(entry: dict[str, Any]) -> dict[str, Any]:
    """English phrases are stored in `words` table with multi-word `word` value."""
    if not isinstance(entry, dict):
        return {"ok": False, "reason": "not_a_dict", "normalized": {}}
    phrase = (entry.get("phrase") or entry.get("text") or "").strip().lower()
    if not phrase:
        return {"ok": False, "reason": "missing_phrase", "normalized": {}}
    proxy = dict(entry)
    proxy["word"] = phrase
    proxy.setdefault("tags", _to_list(entry.get("domain_tags")) + ["phrase"])
    return validate_english_entry(proxy)


def preview_ingestion(path: str | Path, limit: int = 25) -> dict[str, Any]:
    p = Path(path)
    cap = max(1, min(int(limit), PREVIEW_HARD_MAX))
    if not p.exists():
        return {"path": str(p), "error": "file_not_found",
                "previewed": 0, "ok": 0, "rejected": 0, "samples": []}
    samples: list[dict[str, Any]] = []
    ok = 0
    rejected = 0
    try:
        for i, entry in enumerate(_stream(p)):
            if i >= cap:
                break
            v = validate_english_entry(entry)
            samples.append({"row": i, "ok": v["ok"], "reason": v["reason"],
                            "word": v["normalized"].get("word", "")})
            if v["ok"]:
                ok += 1
            else:
                rejected += 1
    except Exception as e:
        return {"path": str(p), "error": f"stream_failed: {e}",
                "previewed": len(samples), "ok": ok, "rejected": rejected,
                "samples": samples}
    return {"path": str(p), "error": None, "previewed": len(samples),
            "ok": ok, "rejected": rejected, "samples": samples}


def _ingest(
    path: str | Path,
    source: str,
    limit: Optional[int],
    batch_size: int,
    db_path: Optional[str | Path],
    kind: str,
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "error": "file_not_found", "added": 0,
                "rejected": 0, "report_path": None, "manifest_path": None}
    bs = _clamp_batch(batch_size)
    lex.init_db(db_path)
    pid = pack_id or f"{kind}_{int(time.time())}"
    added = 0
    rejected = 0
    duplicates = 0
    rejection_samples: list[dict[str, Any]] = []
    seen = 0
    seen_words: set[str] = set()
    aggregate_categories: set[str] = set()
    aggregate_register: set[str] = set()
    aggregate_safety: set[str] = set()
    aggregate_domain: set[str] = set()
    try:
        for entry in _stream(p):
            if limit is not None and seen >= int(limit):
                break
            seen += 1
            v = (validate_english_phrase(entry) if kind == "phrase"
                 else validate_english_entry(entry))
            if v["ok"]:
                norm = v["normalized"]
                if norm["word"] in seen_words:
                    duplicates += 1
                else:
                    seen_words.add(norm["word"])
                    lex.add_word(
                        source=source,
                        pack_source=norm.get("pack_source") or source,
                        pack_id=norm.get("pack_id") or pid,
                        db_path=db_path,
                        **{k: norm[k] for k in (
                            "word", "definition", "synonyms", "examples", "tags",
                            "language", "frequency_score", "word_level",
                            "register_tags", "safety_tags", "coverage_categories",
                        )},
                    )
                    added += 1
                    aggregate_categories.update(norm["coverage_categories"])
                    aggregate_register.update(norm["register_tags"])
                    aggregate_safety.update(norm["safety_tags"])
                    aggregate_domain.update(norm["tags"])
            else:
                rejected += 1
                if len(rejection_samples) < 20:
                    rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
            if added and added % bs == 0:
                pass  # batch boundary; per-call connection already commits
    except Exception as e:
        return {"path": str(p), "error": f"ingest_failed: {e}",
                "added": added, "rejected": rejected, "duplicates": duplicates,
                "rejection_samples": rejection_samples,
                "report_path": None, "manifest_path": None}

    report_payload = {
        "path": str(p), "kind": kind, "source": source,
        "pack_id": pid, "added": added, "rejected": rejected,
        "duplicates": duplicates,
        "rejection_samples": rejection_samples,
        "batch_size": bs, "limit": limit,
        "report_generated_at": time.time(),
    }
    report_path = Path(str(p) + ".en_ingest_report.json")
    report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2),
                           encoding="utf-8")

    manifest = pm.create_pack_manifest(
        source_name=source,
        language="en",
        coverage_categories=sorted(aggregate_categories),
        register_tags=sorted(aggregate_register),
        safety_tags=sorted(aggregate_safety),
        domain_tags=sorted(aggregate_domain),
        row_count=seen,
        accepted_count=added,
        rejected_count=rejected,
        duplicate_count=duplicates,
        source_path=str(p),
        import_report_path=str(report_path),
        pack_id=pid,
        notes=f"english_ingestion kind={kind}",
    )
    manifest_path = Path(str(p) + ".en_pack_manifest.json")
    pm.write_pack_manifest(manifest, manifest_path)

    return {"path": str(p), "error": None, "added": added, "rejected": rejected,
            "duplicates": duplicates,
            "rejection_samples": rejection_samples,
            "report_path": str(report_path),
            "manifest_path": str(manifest_path),
            "pack_id": pid}


def ingest_word_list(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    return _ingest(path, source=source, limit=limit, batch_size=batch_size,
                   db_path=db_path, kind="word", pack_id=pack_id)


def ingest_phrase_list(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    return _ingest(path, source=source, limit=limit, batch_size=batch_size,
                   db_path=db_path, kind="phrase", pack_id=pack_id)


def ingest_topic_pack(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    """Topic-pack: each entry may carry 'words' / 'phrases' arrays."""
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "error": "file_not_found", "added_words": 0,
                "added_phrases": 0, "rejected": 0, "report_path": None,
                "manifest_path": None}
    bs = _clamp_batch(batch_size)
    lex.init_db(db_path)
    pid = pack_id or f"topic_{int(time.time())}"
    added_w = 0
    added_p = 0
    rejected = 0
    duplicates = 0
    rejection_samples: list[dict[str, Any]] = []
    seen = 0
    seen_words: set[str] = set()
    cats: set[str] = set()
    regs: set[str] = set()
    safs: set[str] = set()
    doms: set[str] = set()
    try:
        for entry in _stream(p):
            if limit is not None and seen >= int(limit):
                break
            seen += 1
            if not isinstance(entry, dict):
                rejected += 1
                continue
            words_raw = entry.get("words")
            phrases_raw = entry.get("phrases")
            words: list[Any] = words_raw if isinstance(words_raw, list) else []
            phrases: list[Any] = phrases_raw if isinstance(phrases_raw, list) else []
            if not words and not phrases:
                v = validate_english_entry(entry)
                if v["ok"]:
                    norm = v["normalized"]
                    if norm["word"] in seen_words:
                        duplicates += 1
                    else:
                        seen_words.add(norm["word"])
                        lex.add_word(source=source, pack_source=source, pack_id=pid,
                                     db_path=db_path,
                                     **{k: norm[k] for k in (
                                         "word", "definition", "synonyms", "examples", "tags",
                                         "language", "frequency_score", "word_level",
                                         "register_tags", "safety_tags", "coverage_categories",
                                     )})
                        added_w += 1
                        cats.update(norm["coverage_categories"])
                        regs.update(norm["register_tags"])
                        safs.update(norm["safety_tags"])
                        doms.update(norm["tags"])
                    continue
                rejected += 1
                if len(rejection_samples) < 20:
                    rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
                continue
            for w in words[:bs]:
                v = validate_english_entry(w if isinstance(w, dict) else {"word": str(w)})
                if v["ok"]:
                    norm = v["normalized"]
                    if norm["word"] in seen_words:
                        duplicates += 1
                    else:
                        seen_words.add(norm["word"])
                        lex.add_word(source=source, pack_source=source, pack_id=pid,
                                     db_path=db_path,
                                     **{k: norm[k] for k in (
                                         "word", "definition", "synonyms", "examples", "tags",
                                         "language", "frequency_score", "word_level",
                                         "register_tags", "safety_tags", "coverage_categories",
                                     )})
                        added_w += 1
                        cats.update(norm["coverage_categories"])
                        regs.update(norm["register_tags"])
                        safs.update(norm["safety_tags"])
                        doms.update(norm["tags"])
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
            for ph in phrases[:bs]:
                v = validate_english_phrase(ph if isinstance(ph, dict) else {"phrase": str(ph)})
                if v["ok"]:
                    norm = v["normalized"]
                    if norm["word"] in seen_words:
                        duplicates += 1
                    else:
                        seen_words.add(norm["word"])
                        lex.add_word(source=source, pack_source=source, pack_id=pid,
                                     db_path=db_path,
                                     **{k: norm[k] for k in (
                                         "word", "definition", "synonyms", "examples", "tags",
                                         "language", "frequency_score", "word_level",
                                         "register_tags", "safety_tags", "coverage_categories",
                                     )})
                        added_p += 1
                        cats.update(norm["coverage_categories"])
                        regs.update(norm["register_tags"])
                        safs.update(norm["safety_tags"])
                        doms.update(norm["tags"])
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
    except Exception as e:
        return {"path": str(p), "error": f"ingest_failed: {e}",
                "added_words": added_w, "added_phrases": added_p,
                "rejected": rejected, "duplicates": duplicates,
                "rejection_samples": rejection_samples,
                "report_path": None, "manifest_path": None}

    report = {
        "path": str(p), "kind": "topic_pack", "source": source, "pack_id": pid,
        "added_words": added_w, "added_phrases": added_p,
        "rejected": rejected, "duplicates": duplicates,
        "rejection_samples": rejection_samples,
        "batch_size": bs, "limit": limit,
        "report_generated_at": time.time(),
    }
    report_path = Path(str(p) + ".en_ingest_report.json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                           encoding="utf-8")

    manifest = pm.create_pack_manifest(
        source_name=source, language="en",
        coverage_categories=sorted(cats), register_tags=sorted(regs),
        safety_tags=sorted(safs), domain_tags=sorted(doms),
        row_count=seen, accepted_count=added_w + added_p,
        rejected_count=rejected, duplicate_count=duplicates,
        source_path=str(p), import_report_path=str(report_path),
        pack_id=pid, notes="english_ingestion kind=topic_pack",
    )
    manifest_path = Path(str(p) + ".en_pack_manifest.json")
    pm.write_pack_manifest(manifest, manifest_path)

    return {"path": str(p), "error": None, "added_words": added_w,
            "added_phrases": added_p, "rejected": rejected,
            "duplicates": duplicates,
            "rejection_samples": rejection_samples,
            "report_path": str(report_path),
            "manifest_path": str(manifest_path),
            "pack_id": pid}


def write_ingestion_report(entries: dict[str, Any], output_path: str | Path) -> str:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(entries)
    payload.setdefault("report_generated_at", time.time())
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    return str(out)


def emit_pack_manifest(*args, **kwargs) -> dict[str, Any]:
    """Thin wrapper around pack_manifest.create_pack_manifest for parity with the spec."""
    return pm.create_pack_manifest(*args, **kwargs)


__all__ = [
    "FEATURE_FLAG",
    "DEFAULT_BATCH_SIZE",
    "HARD_MAX_BATCH_SIZE",
    "ingest_word_list",
    "ingest_phrase_list",
    "ingest_topic_pack",
    "preview_ingestion",
    "validate_english_entry",
    "validate_english_phrase",
    "write_ingestion_report",
    "emit_pack_manifest",
]
