"""Phase 21A - Dual Corpus Source Acceptance Validator.

Validates real operator-provided corpus files BEFORE Phase 21 real import.
Read-only, streaming, bounded. Returns per-row classifications without
touching the source file or any production DB.

Rejects:
    * malformed rows
    * invalid taxonomy / register / safety tags
    * language mismatch
    * prompt-injection-like rows
    * operational-unsafe content
    * unlabeled sensitive / vulgar / offensive rows

Honors a hard row cap (default 1000) so a huge file never triggers a full
read.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Iterator, Optional

from coverage_taxonomy import (
    COVERAGE_CATEGORIES, REGISTER_TAGS, SAFETY_TAGS,
)


SUPPORTED_LANGS = ("en", "ru")
SUPPORTED_FORMATS = ("jsonl", "txt", "csv")
SUPPORTED_SOURCE_TYPES = (
    "word_list", "phrase_list", "idiom_list", "slang_list",
    "street_talk_list", "profession_job_list", "domain_terms",
    "semantic_clusters", "topic_pack", "mixed_jsonl",
    "bilingual_glossary_csv", "russian_morphology_csv",
)

_DEFAULT_VALIDATION_LIMIT = 1000
_HARD_VALIDATION_LIMIT = 5000


_COV_SET = set(COVERAGE_CATEGORIES)
_REG_SET = set(REGISTER_TAGS)
_SAFETY_SET = set(SAFETY_TAGS)


_OPERATIONAL_UNSAFE_MARKERS = (
    "how to make", "step by step", "step-by-step",
    "instructions to bypass", "instructions to make",
    "how to build", "how to synthesize",
    "exploit", "bypass authentication", "bypass auth",
    "kill yourself", "kill themselves", "self-harm", "harm yourself",
)

_PROMPT_INJECTION_MARKERS = (
    "ignore previous instructions",
    "ignore all previous",
    "system prompt:",
    "you are now",
    "disregard prior",
    "act as if you are",
    "override your rules",
    "</system>",
    "<|im_start|>system",
)


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_VALIDATION_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_VALIDATION_LIMIT
    return max(1, min(v, _HARD_VALIDATION_LIMIT))


# ----------------- Field-level checks -----------------

def validate_required_fields(row: dict[str, Any], language: str,
                             source_type: str) -> dict[str, Any]:
    missing: list[str] = []
    word = (row.get("word") or "").strip()
    phrase = (row.get("phrase") or "").strip()
    if source_type in ("phrase_list", "idiom_list"):
        if not phrase and not word:
            missing.append("phrase")
    else:
        if not word and not phrase:
            missing.append("word")
    if source_type == "russian_morphology_csv":
        if not (row.get("lemma") or row.get("word")):
            missing.append("lemma")
    return {"ok": not missing, "missing": missing}


def validate_language_match(row: dict[str, Any],
                            language: str) -> dict[str, Any]:
    text = (row.get("word") or row.get("phrase") or "").strip()
    if not text:
        return {"ok": True, "reason": "empty", "detected": None}
    has_cyr = any("Ѐ" <= c <= "ӿ" for c in text)
    declared = (row.get("language") or "").strip().lower()
    detected = "ru" if has_cyr else "en"
    if declared and declared not in ("en", "ru"):
        return {"ok": False, "reason": f"bad_declared_language: {declared!r}",
                "detected": detected}
    if language == "ru" and not has_cyr:
        return {"ok": False, "reason": "ru_target_but_no_cyrillic",
                "detected": detected}
    if language == "en" and has_cyr:
        return {"ok": False, "reason": "en_target_but_cyrillic_detected",
                "detected": detected}
    if declared and declared != language:
        return {"ok": False,
                "reason": f"declared_language_mismatch: {declared!r} vs {language!r}",
                "detected": detected}
    return {"ok": True, "reason": "ok", "detected": detected}


def _coerce_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            d = json.loads(v)
            if isinstance(d, list):
                return [str(x) for x in d]
        except Exception:
            return []
    return []


def validate_taxonomy_fields(row: dict[str, Any]) -> dict[str, Any]:
    cats = _coerce_list(row.get("coverage_categories")
                        or row.get("coverage_categories_json") or [])
    invalid = [c for c in cats if c not in _COV_SET]
    return {"ok": not invalid, "invalid": invalid, "present": cats}


def validate_register_fields(row: dict[str, Any]) -> dict[str, Any]:
    regs = _coerce_list(row.get("register_tags")
                        or row.get("register_tags_json") or [])
    invalid = [r for r in regs if r not in _REG_SET]
    return {"ok": not invalid, "invalid": invalid, "present": regs}


def validate_safety_fields(row: dict[str, Any]) -> dict[str, Any]:
    safes = _coerce_list(row.get("safety_tags")
                         or row.get("safety_tags_json") or [])
    invalid = [s for s in safes if s not in _SAFETY_SET]
    return {"ok": not invalid, "invalid": invalid, "present": safes}


def _gather_text(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in ("definition", "definition_en", "definition_ru",
              "example", "examples", "notes", "explanation"):
        v = row.get(k)
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, list):
            parts.extend(str(x) for x in v)
    return " ".join(parts).lower()


def detect_prompt_injection_like_content(row: dict[str, Any]) -> dict[str, Any]:
    blob = _gather_text(row)
    hits = [m for m in _PROMPT_INJECTION_MARKERS if m in blob]
    return {"ok": not hits, "hits": hits[:5]}


def detect_operational_unsafe_content(row: dict[str, Any]) -> dict[str, Any]:
    blob = _gather_text(row)
    hits = [m for m in _OPERATIONAL_UNSAFE_MARKERS if m in blob]
    return {"ok": not hits, "hits": hits[:5]}


def detect_unlabeled_sensitive_or_vulgar(row: dict[str, Any]) -> dict[str, Any]:
    safety = set(_coerce_list(row.get("safety_tags") or []))
    register = set(_coerce_list(row.get("register_tags") or []))
    flagged: list[str] = []
    if ({"vulgar", "offensive"} & register) and not (safety & {"recognition_only"}):
        flagged.append("vulgar_or_offensive_register_without_recognition_only_safety")
    if ({"vulgar", "offensive"} & safety) and "recognition_only" not in safety:
        flagged.append("vulgar_or_offensive_safety_without_recognition_only")
    return {"ok": not flagged, "flagged": flagged}


# ----------------- Row classification -----------------

def classify_row_acceptance(row: Any, language: str,
                            source_type: str) -> dict[str, Any]:
    """Classify a single row. Returns
       {"verdict": "accept"|"warn"|"reject", "reasons": [...], "row": row}."""
    if isinstance(row, str):
        row = {"word": row.strip()}
    if not isinstance(row, dict):
        return {"verdict": "reject", "reasons": ["row_not_dict_or_string"],
                "row": row}
    reasons: list[str] = []
    warns: list[str] = []

    rf = validate_required_fields(row, language, source_type)
    if not rf["ok"]:
        reasons.append("missing_required:" + ",".join(rf["missing"]))

    lm = validate_language_match(row, language)
    if not lm["ok"]:
        reasons.append(f"language_mismatch:{lm['reason']}")

    tx = validate_taxonomy_fields(row)
    if not tx["ok"]:
        reasons.append("invalid_taxonomy:" + ",".join(tx["invalid"]))

    rg = validate_register_fields(row)
    if not rg["ok"]:
        reasons.append("invalid_register:" + ",".join(rg["invalid"]))

    sf = validate_safety_fields(row)
    if not sf["ok"]:
        reasons.append("invalid_safety:" + ",".join(sf["invalid"]))

    pi = detect_prompt_injection_like_content(row)
    if not pi["ok"]:
        reasons.append("prompt_injection_like:" + ",".join(pi["hits"]))

    op = detect_operational_unsafe_content(row)
    if not op["ok"]:
        reasons.append("operational_unsafe:" + ",".join(op["hits"]))

    sens = detect_unlabeled_sensitive_or_vulgar(row)
    if not sens["ok"]:
        # WARN, not reject - the repair preview can fix this.
        warns.extend(sens["flagged"])

    # Missing definition is a soft warn for non-TXT sources where defaults
    # cover for it; reject for explicit knowledge packs.
    if source_type in ("topic_pack", "domain_terms") and not (
            row.get("definition") or row.get("definition_ru")
            or row.get("definition_en") or row.get("gloss")):
        warns.append("missing_definition_in_knowledge_pack")

    if reasons:
        return {"verdict": "reject", "reasons": reasons,
                "warns": warns, "row_preview": _row_preview(row)}
    if warns:
        return {"verdict": "warn", "reasons": [], "warns": warns,
                "row_preview": _row_preview(row)}
    return {"verdict": "accept", "reasons": [], "warns": [],
            "row_preview": _row_preview(row)}


def _row_preview(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {k: row.get(k) for k in ("word", "phrase", "language",
                                        "coverage_categories",
                                        "register_tags", "safety_tags")}
    return {"_value": str(row)[:120]}


# ----------------- Stream validators -----------------

def _iter_jsonl(path: Path, limit: int) -> Iterator[Any]:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                yield {"_unparsed": s[:200]}
            n += 1
            if n >= limit:
                return


def _iter_txt(path: Path, limit: int) -> Iterator[str]:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield line.rstrip("\r\n")
            n += 1
            if n >= limit:
                return


def _iter_csv(path: Path, limit: int) -> Iterator[dict[str, str]]:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}
            n += 1
            if n >= limit:
                return


def validate_jsonl_stream(path: str | Path, language: str, source_type: str,
                          limit: int = _DEFAULT_VALIDATION_LIMIT
                          ) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return [{"verdict": "reject", "reasons": ["file_not_found"]}]
    cap = _clamp(limit)
    out: list[dict[str, Any]] = []
    for row in _iter_jsonl(p, cap):
        out.append(classify_row_acceptance(row, language, source_type))
    return out


def validate_txt_stream(path: str | Path, language: str, source_type: str,
                        limit: int = _DEFAULT_VALIDATION_LIMIT
                        ) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return [{"verdict": "reject", "reasons": ["file_not_found"]}]
    cap = _clamp(limit)
    out: list[dict[str, Any]] = []
    for line in _iter_txt(p, cap):
        text = (line or "").strip()
        if not text:
            continue
        if source_type in ("phrase_list", "idiom_list"):
            row: dict[str, Any] = {"phrase": text}
        else:
            row = {"word": text}
        row["language"] = language
        out.append(classify_row_acceptance(row, language, source_type))
    return out


def validate_csv_stream(path: str | Path, language: str, source_type: str,
                        limit: int = _DEFAULT_VALIDATION_LIMIT
                        ) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return [{"verdict": "reject", "reasons": ["file_not_found"]}]
    cap = _clamp(limit)
    out: list[dict[str, Any]] = []
    try:
        for row in _iter_csv(p, cap):
            # CSV column normalization: lower-case header keys.
            norm = {(k or "").strip().lower(): (v or "").strip()
                    for k, v in row.items()}
            # Aliases
            if "term" in norm and "word" not in norm:
                norm["word"] = norm["term"]
            if "entry" in norm and "word" not in norm:
                norm["word"] = norm["entry"]
            if "coverage" in norm:
                norm["coverage_categories"] = [
                    x.strip() for x in norm["coverage"].split(";") if x.strip()]
            if "register" in norm:
                norm["register_tags"] = [
                    x.strip() for x in norm["register"].split(";") if x.strip()]
            if "safety" in norm:
                norm["safety_tags"] = [
                    x.strip() for x in norm["safety"].split(";") if x.strip()]
            norm.setdefault("language", language)
            out.append(classify_row_acceptance(norm, language, source_type))
    except Exception as e:
        out.append({"verdict": "reject",
                    "reasons": [f"csv_parse_failed: {e}"]})
    return out


def validate_source_file(path: str | Path, language: str, source_type: str,
                         expected_format: str,
                         limit: int = _DEFAULT_VALIDATION_LIMIT
                         ) -> dict[str, Any]:
    if language not in SUPPORTED_LANGS:
        return {"ok": False, "error": f"unsupported_language: {language!r}"}
    if expected_format not in SUPPORTED_FORMATS:
        return {"ok": False,
                "error": f"unsupported_format: {expected_format!r}"}
    if source_type not in SUPPORTED_SOURCE_TYPES:
        return {"ok": False,
                "error": f"unsupported_source_type: {source_type!r}"}
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    if expected_format == "jsonl":
        rows = validate_jsonl_stream(p, language, source_type, limit=limit)
    elif expected_format == "txt":
        rows = validate_txt_stream(p, language, source_type, limit=limit)
    else:
        rows = validate_csv_stream(p, language, source_type, limit=limit)
    summary = summarize_acceptance(rows)
    return {"ok": True, "path": str(p), "language": language,
            "source_type": source_type, "expected_format": expected_format,
            "limit": _clamp(limit),
            "rows": rows, "summary": summary}


def summarize_acceptance(validation_rows: list[dict[str, Any]]
                         ) -> dict[str, Any]:
    n = len(validation_rows)
    accept = sum(1 for r in validation_rows if r.get("verdict") == "accept")
    warn = sum(1 for r in validation_rows if r.get("verdict") == "warn")
    reject = sum(1 for r in validation_rows if r.get("verdict") == "reject")
    # reason frequency
    reason_counts: dict[str, int] = {}
    for r in validation_rows:
        for reason in (r.get("reasons") or []):
            head = reason.split(":", 1)[0]
            reason_counts[head] = reason_counts.get(head, 0) + 1
    return {"n": n,
            "accept": accept, "warn": warn, "reject": reject,
            "acceptance_rate": round(accept / n, 4) if n else 0.0,
            "warn_rate": round(warn / n, 4) if n else 0.0,
            "reject_rate": round(reject / n, 4) if n else 0.0,
            "reason_counts": reason_counts}


def write_acceptance_report(report: dict[str, Any],
                            output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    import time
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "SUPPORTED_LANGS", "SUPPORTED_FORMATS", "SUPPORTED_SOURCE_TYPES",
    "validate_source_file",
    "validate_jsonl_stream",
    "validate_txt_stream",
    "validate_csv_stream",
    "validate_required_fields",
    "validate_language_match",
    "validate_taxonomy_fields",
    "validate_safety_fields",
    "validate_register_fields",
    "detect_prompt_injection_like_content",
    "detect_operational_unsafe_content",
    "detect_unlabeled_sensitive_or_vulgar",
    "classify_row_acceptance",
    "summarize_acceptance",
    "write_acceptance_report",
]
