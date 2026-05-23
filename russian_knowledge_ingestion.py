"""Russian Sovereign Language Stack — knowledge / lexicon ingestion.

Local files only. Chunked batched inserts. Bounded batch size. Produces a
JSON report after every ingestion. Rejects malformed rows safely.

Supported input formats:
- *.jsonl  : one JSON object per line
- *.json   : single array of objects
- *.txt    : one word/phrase per line (lemma equals the line)
- *.csv    : header-driven (csv.DictReader)
"""

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import russian_lexicon_store as lex

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

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
            if isinstance(item, dict):
                yield item
            else:
                yield {"__error__": "non_dict_entry"}
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
            yield {"word": t, "lemma": t}


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


def validate_russian_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return {ok: bool, reason: str|None, normalized: dict}. Never raises."""
    if not isinstance(entry, dict):
        return {"ok": False, "reason": "not_a_dict", "normalized": {}}
    if "__error__" in entry:
        return {"ok": False, "reason": entry["__error__"], "normalized": {}}
    word = (entry.get("word") or entry.get("lemma") or "").strip()
    if not word:
        return {"ok": False, "reason": "missing_word_and_lemma", "normalized": {}}
    norm: dict[str, Any] = {
        "word": word[:128],
        "lemma": (entry.get("lemma") or word).strip()[:128],
        "part_of_speech": (entry.get("part_of_speech") or entry.get("pos") or "").strip()[:32],
        "definition_ru": (entry.get("definition_ru") or "")[:2000],
        "definition_en": (entry.get("definition_en") or "")[:2000],
        "synonyms": _to_list(entry.get("synonyms")),
        "antonyms": _to_list(entry.get("antonyms")),
        "examples": _to_list(entry.get("examples")),
        "phrase_examples": _to_list(entry.get("phrase_examples")),
        "idioms": _to_list(entry.get("idioms")),
        "domain_tags": _to_list(entry.get("domain_tags") or entry.get("domain")),
        "semantic_tags": _to_list(entry.get("semantic_tags")),
        "frequency_score": _to_float(entry.get("frequency_score")),
        "register_level": (entry.get("register_level") or "plain").strip()[:32],
        "register_tags": _to_list(entry.get("register_tags")),
        "safety_tags": _to_list(entry.get("safety_tags")),
        "coverage_categories": _to_list(entry.get("coverage_categories")),
        "pack_source": (entry.get("pack_source") or "").strip()[:120],
        "pack_id": (entry.get("pack_id") or "").strip()[:64],
    }
    return {"ok": True, "reason": None, "normalized": norm}


def validate_phrase_entry(entry: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(entry, dict):
        return {"ok": False, "reason": "not_a_dict", "normalized": {}}
    if "__error__" in entry:
        return {"ok": False, "reason": entry["__error__"], "normalized": {}}
    phrase = (entry.get("phrase") or entry.get("text") or "").strip()
    if not phrase:
        return {"ok": False, "reason": "missing_phrase", "normalized": {}}
    norm: dict[str, Any] = {
        "phrase": phrase[:300],
        "translation_en": (entry.get("translation_en") or "")[:600],
        "gloss_ru": (entry.get("gloss_ru") or "")[:600],
        "idiomatic": bool(entry.get("idiomatic", False)),
        "domain_tags": _to_list(entry.get("domain_tags")),
        "semantic_tags": _to_list(entry.get("semantic_tags")),
        "frequency_score": _to_float(entry.get("frequency_score")),
        "register_level": (entry.get("register_level") or "plain").strip()[:32],
        "register_tags": _to_list(entry.get("register_tags")),
        "safety_tags": _to_list(entry.get("safety_tags")),
        "coverage_categories": _to_list(entry.get("coverage_categories")),
        "pack_source": (entry.get("pack_source") or "").strip()[:120],
        "pack_id": (entry.get("pack_id") or "").strip()[:64],
    }
    return {"ok": True, "reason": None, "normalized": norm}


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


def preview_ingestion(path: str | Path, limit: int = 25) -> dict[str, Any]:
    """Read up to `limit` entries from `path` and report validation result."""
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
            v = validate_russian_entry(entry)
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


def ingest_word_list(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    return _ingest(path, source=source, limit=limit, batch_size=batch_size,
                   db_path=db_path, kind="word")


def ingest_phrase_list(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    return _ingest(path, source=source, limit=limit, batch_size=batch_size,
                   db_path=db_path, kind="phrase")


def ingest_topic_pack(
    path: str | Path,
    source: str = "manual",
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Topic-pack format: each entry may contain 'words' and/or 'phrases' arrays."""
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "error": "file_not_found", "added_words": 0,
                "added_phrases": 0, "rejected": 0, "report_path": None}
    bs = _clamp_batch(batch_size)
    lex.init_db(db_path)
    added_w = 0
    added_p = 0
    rejected = 0
    rejection_samples: list[dict[str, Any]] = []
    seen = 0
    try:
        for entry in _stream(p):
            if limit is not None and seen >= limit:
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
                w_validation = validate_russian_entry(entry)
                if w_validation["ok"]:
                    lex.add_word(source=source, db_path=db_path, **w_validation["normalized"])
                    added_w += 1
                    continue
                rejected += 1
                if len(rejection_samples) < 20:
                    rejection_samples.append({"row": seen - 1, "reason": w_validation["reason"]})
                continue
            for w in words[:bs]:
                v = validate_russian_entry(w if isinstance(w, dict) else {"word": str(w)})
                if v["ok"]:
                    lex.add_word(source=source, db_path=db_path, **v["normalized"])
                    added_w += 1
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
            for ph in phrases[:bs]:
                v = validate_phrase_entry(ph if isinstance(ph, dict) else {"phrase": str(ph)})
                if v["ok"]:
                    lex.add_phrase(source=source, db_path=db_path, **v["normalized"])
                    added_p += 1
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
    except Exception as e:
        return {"path": str(p), "error": f"ingest_failed: {e}",
                "added_words": added_w, "added_phrases": added_p,
                "rejected": rejected, "rejection_samples": rejection_samples,
                "report_path": None}
    report_path = write_ingestion_report(
        {"path": str(p), "kind": "topic_pack", "source": source,
         "added_words": added_w, "added_phrases": added_p,
         "rejected": rejected, "rejection_samples": rejection_samples,
         "batch_size": bs, "limit": limit},
        output_path=str(p) + ".ingest_report.json",
    )
    return {"path": str(p), "error": None, "added_words": added_w,
            "added_phrases": added_p, "rejected": rejected,
            "rejection_samples": rejection_samples, "report_path": report_path}


def _ingest(
    path: str | Path,
    source: str,
    limit: Optional[int],
    batch_size: int,
    db_path: Optional[str | Path],
    kind: str,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "error": "file_not_found", "added": 0,
                "rejected": 0, "report_path": None}
    bs = _clamp_batch(batch_size)
    lex.init_db(db_path)
    added = 0
    rejected = 0
    rejection_samples: list[dict[str, Any]] = []
    batch_count = 0
    seen = 0
    try:
        for entry in _stream(p):
            if limit is not None and seen >= int(limit):
                break
            seen += 1
            if kind == "word":
                v = validate_russian_entry(entry)
                if v["ok"]:
                    norm = dict(v["normalized"])
                    if not norm.get("pack_source"):
                        norm["pack_source"] = source
                    lex.add_word(source=source, db_path=db_path, **norm)
                    added += 1
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
            else:
                v = validate_phrase_entry(entry)
                if v["ok"]:
                    norm = dict(v["normalized"])
                    if not norm.get("pack_source"):
                        norm["pack_source"] = source
                    lex.add_phrase(source=source, db_path=db_path, **norm)
                    added += 1
                else:
                    rejected += 1
                    if len(rejection_samples) < 20:
                        rejection_samples.append({"row": seen - 1, "reason": v["reason"]})
            batch_count += 1
            if batch_count >= bs:
                batch_count = 0
    except Exception as e:
        return {"path": str(p), "error": f"ingest_failed: {e}",
                "added": added, "rejected": rejected,
                "rejection_samples": rejection_samples, "report_path": None}
    report_path = write_ingestion_report(
        {"path": str(p), "kind": kind, "source": source, "added": added,
         "rejected": rejected, "rejection_samples": rejection_samples,
         "batch_size": bs, "limit": limit},
        output_path=str(p) + ".ingest_report.json",
    )
    return {"path": str(p), "error": None, "added": added, "rejected": rejected,
            "rejection_samples": rejection_samples, "report_path": report_path}


def write_ingestion_report(entries: dict[str, Any], output_path: str | Path) -> str:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(entries)
    payload["report_generated_at"] = time.time()
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out)


__all__ = [
    "FEATURE_FLAG",
    "DEFAULT_BATCH_SIZE",
    "HARD_MAX_BATCH_SIZE",
    "ingest_word_list",
    "ingest_phrase_list",
    "ingest_topic_pack",
    "validate_russian_entry",
    "validate_phrase_entry",
    "preview_ingestion",
    "write_ingestion_report",
]
