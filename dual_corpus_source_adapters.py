"""Phase 17 - Dual Corpus Source Adapters.

Normalizes multiple local corpus source formats into Luna's canonical
English/Russian pack row format. Streaming-only. No full-file loads. No
network.

Supported adapter_type values:
    luna_jsonl
    wiktextract_jsonl
    simple_word_list_txt
    frequency_word_list_txt
    phrase_list_txt
    idiom_list_txt
    slang_list_txt
    profession_job_csv
    domain_terms_csv
    bilingual_glossary_csv
    russian_morphology_csv
    mixed_jsonl

Canonical normalized row fields (subset present per source):
    word, phrase, lemma, part_of_speech, definition, definition_ru,
    definition_en, example, language, coverage_categories, register_tags,
    safety_tags, domain_tags, source, pack_source, pack_id, frequency_score,
    register_level, metadata
"""

from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from typing import Any, Iterator, Optional


ADAPTER_TYPES: tuple[str, ...] = (
    "luna_jsonl",
    "wiktextract_jsonl",
    "simple_word_list_txt",
    "frequency_word_list_txt",
    "phrase_list_txt",
    "idiom_list_txt",
    "slang_list_txt",
    "profession_job_csv",
    "domain_terms_csv",
    "bilingual_glossary_csv",
    "russian_morphology_csv",
    "mixed_jsonl",
)

SLANG_LIKE_SOURCE_TYPES = {"slang_list", "street_talk_list"}
SLANG_LIKE_ADAPTER_TYPES = {"slang_list_txt"}

_OPERATIONAL_UNSAFE_MARKERS = (
    "how to make", "step by step", "step-by-step", "instructions to",
    "how to build", "how to synthesize", "exploit", "bypass",
    "kill yourself", "kill themselves", "self-harm", "harm yourself",
)


def _peek_text_lines(path: Path, n_lines: int = 5) -> list[str]:
    out: list[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                out.append(line.rstrip("\r\n"))
                if len(out) >= n_lines:
                    break
    except Exception:
        return out
    return out


def _looks_like_jsonl(lines: list[str]) -> bool:
    n_ok = 0
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
            if isinstance(obj, (dict, list)):
                n_ok += 1
        except Exception:
            return False
    return n_ok >= 1


def _looks_like_csv(path: Path) -> bool:
    lines = _peek_text_lines(path, 3)
    if not lines:
        return False
    if "," not in lines[0]:
        return False
    if _looks_like_jsonl([lines[0]]):
        return False
    return True


def _looks_like_wiktextract(lines: list[str]) -> bool:
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and ("senses" in obj or "lang_code" in obj):
                return True
        except Exception:
            return False
    return False


def _looks_like_frequency(lines: list[str]) -> bool:
    n = 0
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) >= 2:
            try:
                float(parts[-1])
                n += 1
            except Exception:
                pass
    return n >= 1


def _looks_like_phrase(lines: list[str]) -> bool:
    for ln in lines:
        s = ln.strip()
        if " " in s and len(s.split()) >= 2:
            return True
    return False


def detect_adapter_type(path: str | Path,
                        declared_type: Optional[str] = None) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found",
                "adapter_type": None, "path": str(p)}
    if declared_type and declared_type in ADAPTER_TYPES:
        return {"ok": True, "adapter_type": declared_type,
                "source": "declared", "path": str(p)}
    ext = p.suffix.lower()
    lines = _peek_text_lines(p, 5)
    if not lines:
        return {"ok": False, "error": "empty_or_unreadable",
                "adapter_type": None, "path": str(p)}

    if ext == ".jsonl" or _looks_like_jsonl(lines):
        if _looks_like_wiktextract(lines):
            return {"ok": True, "adapter_type": "wiktextract_jsonl",
                    "source": "detected", "path": str(p)}
        return {"ok": True, "adapter_type": "luna_jsonl",
                "source": "detected", "path": str(p)}
    if ext == ".csv" or _looks_like_csv(p):
        head = lines[0].lower()
        if "lemma" in head or "morph" in head or "морф" in head:
            return {"ok": True, "adapter_type": "russian_morphology_csv",
                    "source": "detected", "path": str(p)}
        if "en" in head.split(",") and ("ru" in head.split(",")
                                        or "rus" in head.split(",")):
            return {"ok": True, "adapter_type": "bilingual_glossary_csv",
                    "source": "detected", "path": str(p)}
        if "profession" in head or "job" in head:
            return {"ok": True, "adapter_type": "profession_job_csv",
                    "source": "detected", "path": str(p)}
        return {"ok": True, "adapter_type": "domain_terms_csv",
                "source": "detected", "path": str(p)}
    if ext == ".txt":
        if _looks_like_frequency(lines):
            return {"ok": True, "adapter_type": "frequency_word_list_txt",
                    "source": "detected", "path": str(p)}
        if _looks_like_phrase(lines):
            return {"ok": True, "adapter_type": "phrase_list_txt",
                    "source": "detected", "path": str(p)}
        return {"ok": True, "adapter_type": "simple_word_list_txt",
                "source": "detected", "path": str(p)}
    return {"ok": False, "error": "unrecognized_format",
            "adapter_type": None, "path": str(p)}


def _has_operational_unsafe(text_blobs: list[str]) -> Optional[str]:
    big = " ".join(text_blobs).lower()
    for m in _OPERATIONAL_UNSAFE_MARKERS:
        if m in big:
            return m
    return None


def _ensure_register_for_slang(register_tags: list[str],
                               coverage_categories: list[str],
                               source_type: str,
                               adapter_type: str) -> tuple[list[str], list[str]]:
    rt = list(register_tags)
    cv = list(coverage_categories)
    if source_type in SLANG_LIKE_SOURCE_TYPES or adapter_type in SLANG_LIKE_ADAPTER_TYPES:
        if adapter_type == "slang_list_txt" or source_type == "slang_list":
            if "slang" not in rt:
                rt.append("slang")
        if source_type == "street_talk_list":
            if "street" not in rt:
                rt.append("street")
        if "slang_street_talk" not in cv:
            cv.append("slang_street_talk")
    return rt, cv


def _ensure_sensitive_handled(safety_tags: list[str],
                              register_tags: list[str]) -> tuple[list[str], list[str]]:
    st = list(safety_tags)
    rt = list(register_tags)
    if "sensitive" in st and "recognition_only" not in st:
        st.append("recognition_only")
    if {"vulgar", "offensive"} & set(st) and "recognition_only" not in st:
        st.append("recognition_only")
        if "do_not_use_unprompted" not in st:
            st.append("do_not_use_unprompted")
        if "recognition_only" not in rt:
            rt.append("recognition_only")
        if "do_not_use_unprompted" not in rt:
            rt.append("do_not_use_unprompted")
    return st, rt


def _build_canonical(language: str, source: str = "corpus_adapter") -> dict[str, Any]:
    return {
        "word": "", "phrase": "", "lemma": "", "part_of_speech": "",
        "definition": "", "definition_ru": "", "definition_en": "",
        "example": "", "language": language,
        "coverage_categories": [], "register_tags": [],
        "safety_tags": [], "domain_tags": [],
        "source": source, "pack_source": "", "pack_id": "",
        "frequency_score": 0.0, "register_level": "",
        "metadata": {},
    }


def _finalize(norm: dict[str, Any], source_type: str,
              adapter_type: str) -> dict[str, Any]:
    rt, cv = _ensure_register_for_slang(
        norm.get("register_tags") or [],
        norm.get("coverage_categories") or [],
        source_type, adapter_type)
    st, rt = _ensure_sensitive_handled(norm.get("safety_tags") or [], rt)
    norm["register_tags"] = sorted(set(rt))
    norm["coverage_categories"] = sorted(set(cv))
    norm["safety_tags"] = sorted(set(st))
    domain = norm.get("domain_tags") or []
    norm["domain_tags"] = sorted({str(x) for x in domain if x})
    text_blobs: list[str] = []
    for k in ("definition", "definition_en", "definition_ru", "example"):
        v = norm.get(k)
        if isinstance(v, str):
            text_blobs.append(v)
    marker = _has_operational_unsafe(text_blobs)
    if marker:
        return {"ok": False, "reason": f"operational_unsafe: {marker!r}",
                "normalized": None}
    if not (norm.get("word") or norm.get("phrase")):
        return {"ok": False, "reason": "missing_word_and_phrase",
                "normalized": None}
    return {"ok": True, "normalized": norm}


# ----------------- Per-adapter normalizers -----------------

def normalize_luna_jsonl_row(row: dict[str, Any], language: str,
                             source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict", "normalized": None}
    n = _build_canonical(language)
    n["word"] = str(row.get("word") or "").strip()
    n["phrase"] = str(row.get("phrase") or "").strip()
    n["lemma"] = str(row.get("lemma") or "").strip()
    n["part_of_speech"] = str(row.get("part_of_speech") or row.get("pos") or "").strip()
    n["definition"] = str(row.get("definition") or "").strip()
    n["definition_ru"] = str(row.get("definition_ru") or "").strip()
    n["definition_en"] = str(row.get("definition_en") or "").strip()
    n["example"] = str(row.get("example") or "").strip()
    cats = row.get("coverage_categories") or []
    regs = row.get("register_tags") or []
    safs = row.get("safety_tags") or []
    doms = row.get("domain_tags") or row.get("tags") or []
    n["coverage_categories"] = [str(x) for x in (cats if isinstance(cats, list) else [])]
    n["register_tags"] = [str(x) for x in (regs if isinstance(regs, list) else [])]
    n["safety_tags"] = [str(x) for x in (safs if isinstance(safs, list) else [])]
    n["domain_tags"] = [str(x) for x in (doms if isinstance(doms, list) else [])]
    try:
        n["frequency_score"] = float(row.get("frequency_score") or 0.0)
    except Exception:
        n["frequency_score"] = 0.0
    n["register_level"] = str(row.get("register_level") or row.get("word_level") or "").strip()
    n["source"] = str(row.get("source") or "luna_jsonl")
    n["pack_source"] = str(row.get("pack_source") or "")
    n["pack_id"] = str(row.get("pack_id") or "")
    md = row.get("metadata")
    n["metadata"] = md if isinstance(md, dict) else {}
    return _finalize(n, source_type, "luna_jsonl")


def normalize_wiktextract_row(row: dict[str, Any], language: str,
                              source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict", "normalized": None}
    n = _build_canonical(language, source="wiktextract")
    n["word"] = str(row.get("word") or "").strip()
    n["lemma"] = str(row.get("lemma") or row.get("word") or "").strip()
    n["part_of_speech"] = str(row.get("pos") or row.get("part_of_speech") or "").strip()
    defs: list[str] = []
    ex: list[str] = []
    senses = row.get("senses")
    if isinstance(senses, list):
        for s in senses[:5]:
            if not isinstance(s, dict):
                continue
            gloss = s.get("glosses") or s.get("definition")
            if isinstance(gloss, list) and gloss:
                defs.append(str(gloss[0]))
            elif isinstance(gloss, str):
                defs.append(gloss)
            ex_list = s.get("examples")
            if isinstance(ex_list, list):
                for e in ex_list[:2]:
                    if isinstance(e, dict):
                        ex.append(str(e.get("text") or ""))
                    elif isinstance(e, str):
                        ex.append(e)
    n["definition"] = "; ".join(d for d in defs if d)[:1000]
    n["example"] = " | ".join(e for e in ex if e)[:1000]
    lc = (row.get("lang_code") or row.get("language") or "").lower()
    if lc.startswith("ru"):
        n["language"] = "ru"
    elif lc.startswith("en"):
        n["language"] = "en"
    n["coverage_categories"] = ["core_vocabulary"]
    n["register_tags"] = ["standard"]
    n["source"] = "wiktextract"
    n["metadata"] = {"wiktextract_keys": sorted(list(row.keys()))[:20]}
    return _finalize(n, source_type, "wiktextract_jsonl")


def normalize_simple_word_row(line: str, language: str,
                              source_type: str) -> dict[str, Any]:
    word = (line or "").strip()
    if not word:
        return {"ok": False, "reason": "empty_line", "normalized": None}
    n = _build_canonical(language, source="simple_word_list")
    n["word"] = word
    n["coverage_categories"] = ["core_vocabulary"]
    n["register_tags"] = ["standard"]
    return _finalize(n, source_type, "simple_word_list_txt")


def normalize_frequency_word_row(line: str, language: str,
                                 source_type: str) -> dict[str, Any]:
    s = (line or "").strip()
    if not s:
        return {"ok": False, "reason": "empty_line", "normalized": None}
    parts = re.split(r"[\s\t]+", s)
    if len(parts) < 2:
        return {"ok": False, "reason": "freq_row_needs_word_and_score",
                "normalized": None}
    try:
        score = float(parts[-1])
    except Exception:
        return {"ok": False, "reason": "freq_score_not_numeric",
                "normalized": None}
    word = " ".join(parts[:-1]).strip()
    if not word:
        return {"ok": False, "reason": "empty_word_token",
                "normalized": None}
    n = _build_canonical(language, source="frequency_word_list")
    n["word"] = word
    n["frequency_score"] = max(0.0, min(1.0, score)) if score <= 1.0 else min(1.0, score / 1_000_000.0)
    n["metadata"] = {"raw_frequency": score}
    n["coverage_categories"] = ["core_vocabulary"]
    n["register_tags"] = ["standard"]
    return _finalize(n, source_type, "frequency_word_list_txt")


def normalize_phrase_row(line: str, language: str,
                         source_type: str) -> dict[str, Any]:
    s = (line or "").strip()
    if not s:
        return {"ok": False, "reason": "empty_line", "normalized": None}
    n = _build_canonical(language, source="phrase_list")
    n["phrase"] = s
    n["word"] = s
    cov = "idioms_phrases" if source_type == "idiom_list" else "core_vocabulary"
    n["coverage_categories"] = [cov]
    n["register_tags"] = ["standard"]
    return _finalize(n, source_type,
                     "idiom_list_txt" if source_type == "idiom_list" else "phrase_list_txt")


def normalize_csv_row(row: dict[str, str], language: str,
                      source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict", "normalized": None}
    lower = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
    word = lower.get("word") or lower.get("term") or lower.get("entry") or ""
    if not word:
        return {"ok": False, "reason": "csv_missing_word", "normalized": None}
    n = _build_canonical(language, source="csv_corpus")
    n["word"] = word
    n["definition"] = lower.get("definition") or lower.get("meaning") or ""
    n["part_of_speech"] = lower.get("pos") or lower.get("part_of_speech") or ""
    n["example"] = lower.get("example") or ""
    cov_cell = lower.get("coverage") or lower.get("category") or ""
    reg_cell = lower.get("register") or lower.get("registers") or ""
    safe_cell = lower.get("safety") or ""
    dom_cell = lower.get("domain") or lower.get("tags") or ""
    n["coverage_categories"] = [x.strip() for x in cov_cell.split(";") if x.strip()] or ["core_vocabulary"]
    n["register_tags"] = [x.strip() for x in reg_cell.split(";") if x.strip()] or ["standard"]
    n["safety_tags"] = [x.strip() for x in safe_cell.split(";") if x.strip()]
    n["domain_tags"] = [x.strip() for x in dom_cell.split(";") if x.strip()]
    if source_type == "profession_job_list" and "professions_jobs" not in n["coverage_categories"]:
        n["coverage_categories"].append("professions_jobs")
    adapter = {
        "profession_job_list": "profession_job_csv",
        "domain_terms": "domain_terms_csv",
    }.get(source_type, "domain_terms_csv")
    return _finalize(n, source_type, adapter)


def normalize_bilingual_glossary_row(row: dict[str, str],
                                     source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict", "normalized": None}
    lower = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
    en = lower.get("en") or lower.get("english")
    ru = lower.get("ru") or lower.get("rus") or lower.get("russian")
    if not en and not ru:
        return {"ok": False, "reason": "bilingual_missing_pair",
                "normalized": None}
    lang = "en" if en else "ru"
    n = _build_canonical(lang, source="bilingual_glossary")
    n["word"] = en if en else (ru or "")
    n["definition_en"] = lower.get("definition_en") or ""
    n["definition_ru"] = lower.get("definition_ru") or ""
    n["metadata"] = {"en_term": en, "ru_term": ru}
    cov_cell = lower.get("coverage") or "core_vocabulary"
    reg_cell = lower.get("register") or "standard"
    n["coverage_categories"] = [x.strip() for x in cov_cell.split(";") if x.strip()]
    n["register_tags"] = [x.strip() for x in reg_cell.split(";") if x.strip()]
    return _finalize(n, source_type, "bilingual_glossary_csv")


def normalize_russian_morphology_row(row: dict[str, str],
                                     source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict", "normalized": None}
    lower = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
    word = lower.get("word") or lower.get("слово") or ""
    lemma = lower.get("lemma") or lower.get("лемма") or word
    pos = lower.get("pos") or lower.get("part_of_speech") or lower.get("часть_речи") or ""
    if not word:
        return {"ok": False, "reason": "ru_morph_missing_word",
                "normalized": None}
    n = _build_canonical("ru", source="russian_morphology")
    n["word"] = word
    n["lemma"] = lemma
    n["part_of_speech"] = pos
    n["definition"] = lower.get("definition") or ""
    n["coverage_categories"] = ["core_vocabulary"]
    n["register_tags"] = ["standard"]
    morph_extra = {k: v for k, v in lower.items()
                   if k not in ("word", "lemma", "pos", "part_of_speech",
                                "definition", "слово", "лемма", "часть_речи")}
    n["metadata"] = {"morph": morph_extra}
    return _finalize(n, source_type, "russian_morphology_csv")


def normalize_source_row(raw: Any, adapter_type: str, language: str,
                         source_type: str) -> dict[str, Any]:
    if adapter_type not in ADAPTER_TYPES:
        return {"ok": False, "reason": f"unknown_adapter_type: {adapter_type!r}",
                "normalized": None}
    if adapter_type == "luna_jsonl" or adapter_type == "mixed_jsonl":
        return normalize_luna_jsonl_row(raw, language, source_type)
    if adapter_type == "wiktextract_jsonl":
        return normalize_wiktextract_row(raw, language, source_type)
    if adapter_type == "simple_word_list_txt":
        return normalize_simple_word_row(raw if isinstance(raw, str) else "",
                                         language, source_type)
    if adapter_type == "frequency_word_list_txt":
        return normalize_frequency_word_row(raw if isinstance(raw, str) else "",
                                            language, source_type)
    if adapter_type == "phrase_list_txt" or adapter_type == "idiom_list_txt":
        st = "idiom_list" if adapter_type == "idiom_list_txt" else source_type
        return normalize_phrase_row(raw if isinstance(raw, str) else "",
                                    language, st)
    if adapter_type == "slang_list_txt":
        st = source_type if source_type in SLANG_LIKE_SOURCE_TYPES else "slang_list"
        return normalize_simple_word_row(raw if isinstance(raw, str) else "",
                                         language, st)
    if adapter_type in ("profession_job_csv", "domain_terms_csv"):
        return normalize_csv_row(raw if isinstance(raw, dict) else {},
                                 language, source_type)
    if adapter_type == "bilingual_glossary_csv":
        return normalize_bilingual_glossary_row(raw if isinstance(raw, dict) else {},
                                                source_type)
    if adapter_type == "russian_morphology_csv":
        return normalize_russian_morphology_row(raw if isinstance(raw, dict) else {},
                                                source_type)
    return {"ok": False, "reason": "unrouted_adapter", "normalized": None}


def _iter_jsonl(path: Path) -> Iterator[Any]:
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                yield {"_unparsed": s[:200]}


def _iter_txt(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield line.rstrip("\r\n")


def _iter_csv(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}


def iter_normalized_rows(path: str | Path, adapter_type: str,
                         language: str, source_type: str,
                         max_rows: Optional[int] = None
                         ) -> Iterator[dict[str, Any]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return
    cap = None if max_rows is None else max(1, int(max_rows))
    n = 0
    if adapter_type.endswith("_jsonl") or adapter_type in ("luna_jsonl",
                                                            "wiktextract_jsonl",
                                                            "mixed_jsonl"):
        it: Iterator[Any] = _iter_jsonl(p)
    elif adapter_type.endswith("_csv"):
        it = _iter_csv(p)
    else:
        it = _iter_txt(p)
    for raw in it:
        res = normalize_source_row(raw, adapter_type, language, source_type)
        yield res
        n += 1
        if cap is not None and n >= cap:
            return


def write_adapter_preview(path: str | Path, adapter_type: str,
                          language: str, source_type: str,
                          output_path: str | Path,
                          limit: int = 50) -> dict[str, Any]:
    cap = max(1, min(int(limit), 200))
    samples: list[dict[str, Any]] = []
    ok = 0
    rej = 0
    for res in iter_normalized_rows(path, adapter_type, language, source_type,
                                    max_rows=cap):
        samples.append(res)
        if res.get("ok"):
            ok += 1
        else:
            rej += 1
    payload = {
        "path": str(path), "adapter_type": adapter_type,
        "language": language, "source_type": source_type,
        "limit": cap, "ok": ok, "rejected": rej,
        "samples": samples,
    }
    outp = Path(output_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return {"ok": True, "preview_path": str(outp),
            "rows_ok": ok, "rows_rejected": rej}


__all__ = [
    "ADAPTER_TYPES",
    "detect_adapter_type",
    "normalize_luna_jsonl_row",
    "normalize_wiktextract_row",
    "normalize_simple_word_row",
    "normalize_frequency_word_row",
    "normalize_phrase_row",
    "normalize_csv_row",
    "normalize_bilingual_glossary_row",
    "normalize_russian_morphology_row",
    "normalize_source_row",
    "iter_normalized_rows",
    "write_adapter_preview",
]
