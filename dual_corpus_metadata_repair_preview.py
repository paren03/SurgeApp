"""Phase 21A - Dual Corpus Metadata Repair Preview.

Suggests conservative metadata repairs WITHOUT mutating source files. All
output goes only to ``corpus_sources/phase21a/repair_previews/``.

Defaults are intentionally conservative:
    * sensitive / vulgar / offensive uncertainty -> recognition_only +
      do_not_use_unprompted
    * slang_list source -> slang register
    * street_talk_list source -> street register
    * idiom_list source -> idioms_phrases coverage
    * profession_job_list source -> professions_jobs coverage
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import dual_corpus_source_acceptance_validator as val


PHASE21A_PREVIEW_DIR = Path("corpus_sources/phase21a/repair_previews")


_SOURCE_DEFAULT_COVERAGE = {
    "word_list": ["core_vocabulary"],
    "phrase_list": ["core_vocabulary"],
    "idiom_list": ["idioms_phrases"],
    "slang_list": ["slang_street_talk"],
    "street_talk_list": ["slang_street_talk"],
    "profession_job_list": ["professions_jobs"],
    "domain_terms": ["core_vocabulary"],
    "semantic_clusters": ["core_vocabulary"],
    "topic_pack": ["core_vocabulary"],
    "mixed_jsonl": ["core_vocabulary"],
    "bilingual_glossary_csv": ["core_vocabulary"],
    "russian_morphology_csv": ["core_vocabulary"],
}

_SOURCE_DEFAULT_REGISTER = {
    "word_list": ["standard"],
    "phrase_list": ["standard"],
    "idiom_list": ["standard"],
    "slang_list": ["slang"],
    "street_talk_list": ["street"],
    "profession_job_list": ["standard", "professional"],
    "domain_terms": ["standard"],
    "semantic_clusters": ["standard"],
    "topic_pack": ["standard"],
    "mixed_jsonl": ["standard"],
    "bilingual_glossary_csv": ["standard"],
    "russian_morphology_csv": ["standard"],
}


_SENSITIVE_DEFAULT_SAFETY = ["recognition_only", "do_not_use_unprompted"]


def infer_default_coverage_categories(source_type: str,
                                      language: str) -> list[str]:
    return list(_SOURCE_DEFAULT_COVERAGE.get(source_type,
                                              ["core_vocabulary"]))


def infer_default_register_tags(source_type: str,
                                row: dict[str, Any]) -> list[str]:
    base = list(_SOURCE_DEFAULT_REGISTER.get(source_type, ["standard"]))
    declared = row.get("register_tags") or []
    if isinstance(declared, str):
        try:
            d = json.loads(declared)
            declared = d if isinstance(d, list) else []
        except Exception:
            declared = []
    return sorted(set([*base, *[str(x) for x in declared]]))


def infer_default_safety_tags(source_type: str,
                              row: dict[str, Any]) -> list[str]:
    declared = row.get("safety_tags") or []
    if isinstance(declared, str):
        try:
            d = json.loads(declared)
            declared = d if isinstance(d, list) else []
        except Exception:
            declared = []
    out = set(str(x) for x in declared if x)
    sens = val.detect_unlabeled_sensitive_or_vulgar(row)
    if not sens["ok"]:
        out.update(_SENSITIVE_DEFAULT_SAFETY)
    return sorted(out)


def infer_domain_tags(source_type: str,
                      row: dict[str, Any]) -> list[str]:
    declared = row.get("domain_tags") or row.get("tags") or []
    if isinstance(declared, str):
        try:
            d = json.loads(declared)
            declared = d if isinstance(d, list) else []
        except Exception:
            declared = []
    return sorted({str(x) for x in declared if x})


def propose_row_repair(row: dict[str, Any], language: str,
                       source_type: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"ok": False, "reason": "row_not_dict",
                "proposed_changes": {}}
    confidence = 1.0
    reasons: list[str] = []
    proposed: dict[str, Any] = {}

    existing_cov = row.get("coverage_categories") or []
    if isinstance(existing_cov, str):
        try:
            existing_cov = json.loads(existing_cov)
        except Exception:
            existing_cov = []
    if not existing_cov:
        proposed["coverage_categories"] = infer_default_coverage_categories(
            source_type, language)
        reasons.append("coverage_inferred_from_source_type")
        confidence *= 0.9

    existing_reg = row.get("register_tags") or []
    if isinstance(existing_reg, str):
        try:
            existing_reg = json.loads(existing_reg)
        except Exception:
            existing_reg = []
    if not existing_reg:
        proposed["register_tags"] = infer_default_register_tags(source_type,
                                                                 row)
        reasons.append("register_inferred_from_source_type")
        confidence *= 0.9

    sens = val.detect_unlabeled_sensitive_or_vulgar(row)
    if not sens["ok"]:
        new_safety = infer_default_safety_tags(source_type, row)
        if new_safety:
            proposed["safety_tags"] = new_safety
            reasons.append("safety_downgraded_to_recognition_only")
            confidence *= 0.7

    domain = infer_domain_tags(source_type, row)
    if domain and not row.get("domain_tags"):
        proposed["domain_tags"] = domain
        reasons.append("domain_tags_normalized")

    if not row.get("source"):
        proposed["source"] = source_type
        reasons.append("source_field_defaulted_to_source_type")

    return {"ok": True,
            "row_preview": {k: row.get(k) for k in
                            ("word", "phrase", "language",
                             "coverage_categories", "register_tags",
                             "safety_tags")},
            "proposed_changes": proposed,
            "confidence": round(confidence, 3),
            "reasons": reasons}


def preview_repairs_for_file(path: str | Path, language: str,
                             source_type: str, expected_format: str,
                             limit: int = 1000) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    if expected_format not in val.SUPPORTED_FORMATS:
        return {"ok": False,
                "error": f"unsupported_format: {expected_format!r}"}
    cap = max(1, min(int(limit), 5000))
    raw_iter = None
    if expected_format == "jsonl":
        raw_iter = val._iter_jsonl(p, cap)
    elif expected_format == "txt":
        def _gen():
            for line in val._iter_txt(p, cap):
                s = (line or "").strip()
                if not s:
                    continue
                if source_type in ("phrase_list", "idiom_list"):
                    yield {"phrase": s, "language": language}
                else:
                    yield {"word": s, "language": language}
        raw_iter = _gen()
    else:
        def _gen2():
            for row in val._iter_csv(p, cap):
                norm = {(k or "").strip().lower(): (v or "").strip()
                        for k, v in row.items()}
                if "term" in norm and "word" not in norm:
                    norm["word"] = norm["term"]
                norm.setdefault("language", language)
                yield norm
        raw_iter = _gen2()
    proposals: list[dict[str, Any]] = []
    n = 0
    for row in raw_iter:
        proposals.append(propose_row_repair(row, language, source_type))
        n += 1
    accepted = sum(1 for p_ in proposals if not p_.get("proposed_changes"))
    changed = sum(1 for p_ in proposals if p_.get("proposed_changes"))
    return {"ok": True, "path": str(path), "language": language,
            "source_type": source_type,
            "expected_format": expected_format,
            "rows_inspected": n,
            "rows_already_clean": accepted,
            "rows_with_proposed_repair": changed,
            "proposals": proposals}


def write_repair_preview(preview: dict[str, Any],
                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(preview)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_repaired_copy_preview_only(path: str | Path,
                                     output_path: str | Path,
                                     repairs: dict[str, Any],
                                     limit: int = 1000) -> str:
    """Write a PREVIEW-ONLY copy of the source with proposed repairs
    applied to each row. The output goes to repair_previews/ and never
    overwrites incoming files."""
    out = Path(output_path)
    if "repair_previews" not in out.parts:
        # Refuse to write outside repair_previews/.
        out = PHASE21A_PREVIEW_DIR / out.name
    out.parent.mkdir(parents=True, exist_ok=True)
    cap = max(1, min(int(limit), 5000))
    proposals = (repairs or {}).get("proposals") or []
    proposal_by_word: dict[str, dict[str, Any]] = {}
    for p_ in proposals:
        rp = p_.get("row_preview") or {}
        key = (rp.get("word") or rp.get("phrase") or "").strip()
        if key:
            proposal_by_word[key] = p_.get("proposed_changes") or {}
    p = Path(path)
    if not p.exists():
        out.write_text("", encoding="utf-8")
        return str(out)
    n = 0
    with out.open("w", encoding="utf-8") as wh, \
            p.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            try:
                row = json.loads(s)
            except Exception:
                row = {"_unparsed": s[:200]}
            if isinstance(row, dict):
                key = (row.get("word") or row.get("phrase") or "").strip()
                if key and key in proposal_by_word:
                    for k, v in proposal_by_word[key].items():
                        row[k] = v
                row["_phase21a_preview"] = True
            wh.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
            if n >= cap:
                break
    return str(out)


__all__ = [
    "PHASE21A_PREVIEW_DIR",
    "infer_default_coverage_categories",
    "infer_default_register_tags",
    "infer_default_safety_tags",
    "infer_domain_tags",
    "propose_row_repair",
    "preview_repairs_for_file",
    "write_repair_preview",
    "write_repaired_copy_preview_only",
]
