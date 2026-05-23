"""Phase 21A - Operator Corpus Staging Utility.

Operator-facing helpers: discover incoming files, inspect, validate, preview
metadata repairs, write reports + templates + guides.

Does NOT touch production DBs. Does NOT alter operator source files. Does
NOT move files. All output goes only under
``corpus_sources/phase21a/``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import dual_corpus_source_acceptance_validator as val
import dual_corpus_metadata_repair_preview as rep
import phase21a_staging_readiness_gate as gate


PHASE21A_REQUIRED_PRIOR = (
    "PHASE21_OPERATOR_STAGING_REQUIRED_REPORT.md",
    "phase21_operator_stage_runner.py",
    "test_phase21_operator_staged_first_import.py",
    "PHASE20_MILLION_READINESS_GATE_REPORT.md",
    "dual_vocab_backup_restore.py",
    "dual_import_batch_ledger.py",
    "dual_million_stage_planner.py",
    "dual_post_stage_quality_audit.py",
    "dual_retrieval_sla_eval.py",
    "dual_index_consistency_checker.py",
    "dual_safety_regression_auditor.py",
    "phase20_million_readiness_runner.py",
    "dual_corpus_registry.py",
    "dual_corpus_chunked_importer.py",
    "dual_corpus_quality_gate.py",
    "dual_corpus_checkpoint.py",
    "dual_corpus_source_adapters.py",
    "dual_corpus_pilot_import_planner.py",
    "dual_retrieval_quality_eval.py",
    "dual_coverage_reporter.py",
    "dual_retrieval_index_builder.py",
    "dual_dedupe_collision_reporter.py",
)


PHASE21A_BASE = Path("corpus_sources/phase21a")
PHASE21A_TEMPLATES_DIR = PHASE21A_BASE / "templates"
PHASE21A_VALIDATION_DIR = PHASE21A_BASE / "validation_reports"
PHASE21A_REPAIR_DIR = PHASE21A_BASE / "repair_previews"
PHASE21A_REJECTED_DIR = PHASE21A_BASE / "rejected_previews"
PHASE21A_READY_DIR = PHASE21A_BASE / "ready_reports"
PHASE21A_GUIDE_DIR = PHASE21A_BASE / "operator_guides"
PHASE21A_FIXTURE_DIR = PHASE21A_BASE / "fixtures"

_ALL_FOLDERS = (PHASE21A_BASE, PHASE21A_TEMPLATES_DIR,
                PHASE21A_VALIDATION_DIR, PHASE21A_REPAIR_DIR,
                PHASE21A_REJECTED_DIR, PHASE21A_READY_DIR,
                PHASE21A_GUIDE_DIR, PHASE21A_FIXTURE_DIR)


def verify_phase21a_preflight() -> dict[str, Any]:
    missing = [f for f in PHASE21A_REQUIRED_PRIOR if not Path(f).exists()]
    return {"ok": not missing,
            "missing_files": missing,
            "checked": list(PHASE21A_REQUIRED_PRIOR)}


def setup_phase21a_folders() -> dict[str, Any]:
    out: dict[str, str] = {}
    for d in _ALL_FOLDERS:
        d.mkdir(parents=True, exist_ok=True)
        out[d.name] = str(d)
    return {"ok": True, "folders": out}


# ----------------- Templates -----------------

_EN_JSONL_ROWS = [
    {"word": "lighthouse", "definition":
     "a tower with a bright light at the top to guide ships",
     "language": "en", "examples":
     ["The lighthouse warned ships of the rocks."],
     "tags": ["coastal", "navigation"], "domain_tags":
     ["coastal", "navigation"],
     "coverage_categories": ["core_vocabulary"],
     "register_tags": ["standard"], "safety_tags": [],
     "frequency_score": 0.42, "register_level": "common",
     "source": "operator_template", "pack_source": "phase21a_en_template",
     "pack_id": "phase21a_en_template"},
    {"word": "engineer",
     "definition": "a person who designs and builds machines or systems",
     "language": "en", "examples": ["She works as a software engineer."],
     "tags": ["job"], "domain_tags": ["job"],
     "coverage_categories": ["professions_jobs", "core_vocabulary"],
     "register_tags": ["standard", "professional"], "safety_tags": [],
     "frequency_score": 0.71, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_en_template",
     "pack_id": "phase21a_en_template"},
    {"word": "verse",
     "definition": "a single line or group of lines in a poem",
     "language": "en", "examples": ["She wrote a verse about the sea."],
     "tags": ["poetry"], "domain_tags": ["poetry"],
     "coverage_categories": ["poetry_literary"],
     "register_tags": ["poetic", "academic"], "safety_tags": [],
     "frequency_score": 0.46, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_en_template",
     "pack_id": "phase21a_en_template"},
    {"word": "ledger",
     "definition": "a book or record used to keep financial accounts",
     "language": "en", "examples": ["The bookkeeper updated the ledger."],
     "tags": ["finance"], "domain_tags": ["finance"],
     "coverage_categories": ["business_finance"],
     "register_tags": ["standard", "business"], "safety_tags": [],
     "frequency_score": 0.48, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_en_template",
     "pack_id": "phase21a_en_template"},
    {"word": "essence",
     "definition": "the most basic and important quality of something",
     "language": "en", "examples":
     ["The essence of the argument is simple."],
     "tags": ["abstract"], "domain_tags": ["abstract"],
     "coverage_categories": ["philosophy_abstract"],
     "register_tags": ["academic", "philosophical"], "safety_tags": [],
     "frequency_score": 0.55, "register_level": "rare",
     "source": "operator_template",
     "pack_source": "phase21a_en_template",
     "pack_id": "phase21a_en_template"},
]


_RU_JSONL_ROWS = [
    {"word": "маяк", "lemma": "маяк", "part_of_speech": "noun",
     "definition": "башня с ярким светом наверху, направляющая корабли",
     "language": "ru", "examples":
     ["Маяк предупреждал корабли о скалах."],
     "tags": ["coastal"], "domain_tags": ["coastal"],
     "coverage_categories": ["core_vocabulary"],
     "register_tags": ["standard"], "safety_tags": [],
     "frequency_score": 0.40, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_ru_template",
     "pack_id": "phase21a_ru_template"},
    {"word": "инженер", "lemma": "инженер", "part_of_speech": "noun",
     "definition": "человек, проектирующий и создающий машины или системы",
     "language": "ru", "examples":
     ["Она работает инженером-программистом."],
     "tags": ["job"], "domain_tags": ["job"],
     "coverage_categories": ["professions_jobs", "core_vocabulary"],
     "register_tags": ["standard", "professional"], "safety_tags": [],
     "frequency_score": 0.69, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_ru_template",
     "pack_id": "phase21a_ru_template"},
    {"word": "стих", "lemma": "стих", "part_of_speech": "noun",
     "definition": "строка или строфа стихотворения",
     "language": "ru", "examples": ["Она написала стих о море."],
     "tags": ["poetry"], "domain_tags": ["poetry"],
     "coverage_categories": ["poetry_literary"],
     "register_tags": ["poetic", "academic"], "safety_tags": [],
     "frequency_score": 0.44, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_ru_template",
     "pack_id": "phase21a_ru_template"},
    {"word": "бюджет", "lemma": "бюджет", "part_of_speech": "noun",
     "definition": "финансовый план доходов и расходов",
     "language": "ru", "examples":
     ["Семейный бюджет требует планирования."],
     "tags": ["finance"], "domain_tags": ["finance"],
     "coverage_categories": ["business_finance"],
     "register_tags": ["standard", "business"], "safety_tags": [],
     "frequency_score": 0.50, "register_level": "common",
     "source": "operator_template",
     "pack_source": "phase21a_ru_template",
     "pack_id": "phase21a_ru_template"},
    {"word": "сущность", "lemma": "сущность", "part_of_speech": "noun",
     "definition": "самое важное и основное качество чего-либо",
     "language": "ru", "examples": ["Сущность аргумента проста."],
     "tags": ["abstract"], "domain_tags": ["abstract"],
     "coverage_categories": ["philosophy_abstract"],
     "register_tags": ["academic", "philosophical"], "safety_tags": [],
     "frequency_score": 0.52, "register_level": "rare",
     "source": "operator_template",
     "pack_source": "phase21a_ru_template",
     "pack_id": "phase21a_ru_template"},
]


_EN_PHRASE_ROWS = [
    {"phrase": "break the ice", "language": "en",
     "definition": "to start a conversation in a relaxed way",
     "examples": ["She broke the ice with a joke."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_phrases",
     "pack_id": "phase21a_en_phrases"},
    {"phrase": "on the same page", "language": "en",
     "definition": "to share an understanding with someone",
     "examples": ["The team is finally on the same page."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_phrases",
     "pack_id": "phase21a_en_phrases"},
    {"phrase": "take it easy", "language": "en",
     "definition": "to relax and not worry",
     "examples": ["Take it easy, we have time."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["informal", "standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_phrases",
     "pack_id": "phase21a_en_phrases"},
    {"phrase": "sleep on it", "language": "en",
     "definition": "to delay a decision until the next day",
     "examples": ["Let me sleep on it and answer tomorrow."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_phrases",
     "pack_id": "phase21a_en_phrases"},
    {"phrase": "in a nutshell", "language": "en",
     "definition": "in summary, briefly",
     "examples": ["In a nutshell, we agree."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_phrases",
     "pack_id": "phase21a_en_phrases"},
]


_RU_PHRASE_ROWS = [
    {"phrase": "сломать лёд", "language": "ru",
     "definition": "начать разговор в непринуждённой форме",
     "examples": ["Она сломала лёд шуткой."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_phrases",
     "pack_id": "phase21a_ru_phrases"},
    {"phrase": "на одной волне", "language": "ru",
     "definition": "иметь общее понимание с кем-то",
     "examples": ["Команда наконец-то на одной волне."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["informal", "standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_phrases",
     "pack_id": "phase21a_ru_phrases"},
    {"phrase": "не за что", "language": "ru",
     "definition": "вежливый ответ на благодарность",
     "examples": ["— Спасибо! — Не за что."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_phrases",
     "pack_id": "phase21a_ru_phrases"},
    {"phrase": "в двух словах", "language": "ru",
     "definition": "кратко, в общих чертах",
     "examples": ["В двух словах: мы согласны."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_phrases",
     "pack_id": "phase21a_ru_phrases"},
    {"phrase": "до завтра", "language": "ru",
     "definition": "прощание до следующего дня",
     "examples": ["До завтра, спокойной ночи."],
     "coverage_categories": ["idioms_phrases"],
     "register_tags": ["informal", "standard"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_phrases",
     "pack_id": "phase21a_ru_phrases"},
]


_EN_SLANG_ROWS = [
    {"word": "homie", "language": "en",
     "definition": "an informal term for a close friend",
     "examples": ["Hey homie, how are you?"],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_slang",
     "pack_id": "phase21a_en_slang"},
    {"word": "buddy", "language": "en",
     "definition": "an informal term for a friend",
     "examples": ["Hey buddy."],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang", "informal"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_slang",
     "pack_id": "phase21a_en_slang"},
    {"word": "chill", "language": "en",
     "definition": "informal: to relax",
     "examples": ["Let's chill at the park."],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang", "informal"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_en_slang",
     "pack_id": "phase21a_en_slang"},
]


_RU_SLANG_ROWS = [
    {"word": "чувак", "language": "ru", "lemma": "чувак",
     "part_of_speech": "noun",
     "definition": "неформально: парень, друг",
     "examples": ["Привет, чувак!"],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang", "informal"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_slang",
     "pack_id": "phase21a_ru_slang"},
    {"word": "крутой", "language": "ru", "lemma": "крутой",
     "part_of_speech": "adj",
     "definition": "неформально: классный, отличный",
     "examples": ["Крутой фильм."],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang", "informal"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_slang",
     "pack_id": "phase21a_ru_slang"},
    {"word": "тусить", "language": "ru", "lemma": "тусить",
     "part_of_speech": "verb",
     "definition": "неформально: проводить время с друзьями",
     "examples": ["Тусим в парке."],
     "coverage_categories": ["slang_street_talk"],
     "register_tags": ["slang", "informal"], "safety_tags": [],
     "source": "operator_template",
     "pack_source": "phase21a_ru_slang",
     "pack_id": "phase21a_ru_slang"},
]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _write_csv(path: Path, header: list[str],
               rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        fh.write(",".join(header) + "\n")
        for r in rows:
            fh.write(",".join(c.replace(",", " ") for c in r) + "\n")


def _write_txt(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_operator_templates() -> dict[str, Any]:
    base = PHASE21A_TEMPLATES_DIR
    base.mkdir(parents=True, exist_ok=True)
    _write_jsonl(base / "english_words_jsonl_template.jsonl", _EN_JSONL_ROWS)
    _write_jsonl(base / "russian_words_jsonl_template.jsonl", _RU_JSONL_ROWS)
    _write_jsonl(base / "english_phrases_jsonl_template.jsonl",
                 _EN_PHRASE_ROWS)
    _write_jsonl(base / "russian_phrases_jsonl_template.jsonl",
                 _RU_PHRASE_ROWS)
    _write_jsonl(base / "english_slang_jsonl_template.jsonl",
                 _EN_SLANG_ROWS)
    _write_jsonl(base / "russian_slang_jsonl_template.jsonl",
                 _RU_SLANG_ROWS)
    _write_csv(base / "english_domain_terms_csv_template.csv",
               ["word", "definition", "pos", "coverage", "register", "safety", "domain"],
               [["vector", "a quantity with magnitude and direction", "noun",
                 "science_math", "standard;technical;academic", "", "mathematics"],
                ["hash", "a function mapping data to a fixed-size value", "noun",
                 "coding_technology", "standard;technical;coding", "", "software"],
                ["ligament", "a band of tissue connecting bones", "noun",
                 "medicine_health", "standard;medical", "", "anatomy"]])
    _write_csv(base / "russian_domain_terms_csv_template.csv",
               ["word", "definition", "pos", "coverage", "register", "safety", "domain"],
               [["вектор", "величина с длиной и направлением", "noun",
                 "science_math", "standard;technical;academic", "", "математика"],
                ["хэш", "функция отображения данных в значение фиксированного размера",
                 "noun", "coding_technology", "standard;technical;coding", "",
                 "программирование"],
                ["связка", "ткань, соединяющая кости", "noun",
                 "medicine_health", "standard;medical", "", "анатомия"]])
    _write_csv(base / "bilingual_glossary_csv_template.csv",
               ["en", "ru", "definition_en", "definition_ru", "coverage", "register"],
               [["engineer", "инженер", "designs and builds machines",
                 "проектирует и строит машины",
                 "professions_jobs", "standard;professional"],
                ["ledger", "главная книга", "a book of financial records",
                 "книга финансовых записей",
                 "business_finance", "standard;business"]])
    _write_txt(base / "simple_word_list_txt_template.txt",
               ["lighthouse", "engineer", "verse", "ledger", "essence"])
    _write_txt(base / "phrase_list_txt_template.txt",
               ["break the ice", "on the same page", "take it easy",
                "sleep on it", "in a nutshell"])
    listing = sorted(p.name for p in base.glob("*"))
    return {"ok": True, "templates_written": listing}


def write_operator_staging_guide(output_path: Optional[str | Path] = None
                                 ) -> str:
    p = Path(output_path) if output_path is not None else \
        PHASE21A_GUIDE_DIR / "OPERATOR_STAGING_GUIDE.md"
    p.parent.mkdir(parents=True, exist_ok=True)
    body = [
        "# Phase 21A - Operator Staging Guide",
        "",
        "## Where to place files",
        "",
        "- English: `D:\\SurgeApp\\corpus_sources\\english\\incoming\\`",
        "- Russian: `D:\\SurgeApp\\corpus_sources\\russian\\incoming\\`",
        "",
        "## Recommended first-import size",
        "5,000 - 10,000 rows per language. Smaller files (< 5k rows) are "
        "permitted but will only enable a dry-run-only readiness, not real "
        "import escalation.",
        "",
        "## Supported file formats",
        "",
        "- `.jsonl` - one JSON object per line. **Preferred.**",
        "- `.txt`  - one term or phrase per line.",
        "- `.csv`  - header row required.",
        "",
        "## Required metadata",
        "",
        "JSONL rows should include:",
        "- `word` (or `phrase` for phrase/idiom sources)",
        "- `language`: `en` or `ru`",
        "- `definition` (recommended)",
        "- `coverage_categories`: list of canonical category strings",
        "- `register_tags`: list of canonical register strings",
        "- `safety_tags`: list (empty for benign rows)",
        "- `domain_tags`: list (optional)",
        "- Russian rows may include `lemma`, `part_of_speech` for "
        "morphology preservation.",
        "",
        "## How to tag slang, street, vulgar, offensive, and sensitive terms",
        "",
        "- **slang_list / street_talk_list** sources auto-receive `slang` "
        "or `street` register tags during repair preview.",
        "- **vulgar** or **offensive** terms MUST also receive "
        "`safety_tags: [\"recognition_only\", \"do_not_use_unprompted\"]`.",
        "- Luna can recognize and explain recognition_only terms, but will "
        "not use them as her own suggestion. With explicit user prompting "
        "(`is_user_prompted=True`), the softening rules permit them where "
        "the operator's mode allows.",
        "",
        "## How recognition_only works",
        "",
        "A term marked `recognition_only` will be returned by the indexed "
        "retrieval (so Luna recognizes it), but the safety filter will "
        "exclude it from `suggestion`-context outputs.",
        "",
        "## How do_not_use_unprompted works",
        "",
        "A term marked `do_not_use_unprompted` is blocked from "
        "any output unless `is_user_prompted=True` is explicitly passed by "
        "the caller.",
        "",
        "## How to rerun Phase 21 after staging files",
        "",
        "```",
        "python -c \"import phase21a_operator_corpus_staging as p21a; "
        "print(p21a.discover_incoming_files())\"",
        "python test_phase21a_operator_corpus_staging.py",
        "python test_phase21_operator_staged_first_import.py",
        "```",
        "",
        "After the validator + readiness gate report "
        "`READY_FOR_PHASE21_REAL_IMPORT`, the operator runs the Phase 21 "
        "runner with `allow_real_import=True`.",
        "",
        "## What NOT to include",
        "",
        "- Step-by-step operational instructions to bypass security,",
        "- prompt-injection markers (`ignore previous instructions`, etc.),",
        "- private personal data,",
        "- copyrighted works without permission.",
        "",
        "## Why no internet / download is performed",
        "",
        "Luna's sovereign stack is local-only. Network access is never "
        "used during ingest. The operator is responsible for obtaining "
        "corpus files legally and placing them under the `incoming/` "
        "folders before any import is attempted.",
    ]
    p.write_text("\n".join(body), encoding="utf-8")
    return str(p)


# ----------------- Discovery + inspect + validate -----------------

def discover_incoming_files(language: Optional[str] = None,
                            limit: int = 100) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), 1000))
    langs = ("en", "ru") if language is None else (language,)
    out: list[dict[str, Any]] = []
    for lang in langs:
        sub = "english" if lang == "en" else "russian"
        base = Path("corpus_sources") / sub / "incoming"
        if not base.exists():
            continue
        for p in sorted(base.iterdir()):
            if not p.is_file():
                continue
            if len(out) >= cap:
                return out
            try:
                size = p.stat().st_size
            except Exception:
                size = 0
            out.append({"path": str(p), "language": lang,
                        "file_name": p.name, "size_bytes": size,
                        "suffix": p.suffix.lower()})
    return out


def _detect_format(path: Path, hint: Optional[str] = None) -> str:
    if hint and hint in val.SUPPORTED_FORMATS:
        return hint
    ext = path.suffix.lower()
    if ext == ".jsonl":
        return "jsonl"
    if ext == ".csv":
        return "csv"
    return "txt"


def inspect_incoming_file(path: str | Path, language: str,
                          expected_format: Optional[str] = None,
                          limit: int = 100) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    fmt = _detect_format(p, expected_format)
    sample: list[Any] = []
    try:
        if fmt == "jsonl":
            for row in val._iter_jsonl(p, max(1, min(int(limit), 200))):
                sample.append(row)
        elif fmt == "csv":
            for row in val._iter_csv(p, max(1, min(int(limit), 200))):
                sample.append(row)
        else:
            for line in val._iter_txt(p, max(1, min(int(limit), 200))):
                sample.append({"word": line.strip()})
    except Exception as e:
        return {"ok": False, "error": f"read_failed: {e}",
                "path": str(p)}
    return {"ok": True, "path": str(p), "language": language,
            "expected_format": fmt, "rows_returned": len(sample),
            "sample": sample}


def validate_incoming_file(path: str | Path, language: str,
                           source_type: str, expected_format: str,
                           limit: int = 1000) -> dict[str, Any]:
    res = val.validate_source_file(path, language, source_type,
                                    expected_format, limit=limit)
    if res.get("ok"):
        rep_path = (PHASE21A_VALIDATION_DIR
                    / f"{Path(path).stem}.{language}.acceptance.json")
        val.write_acceptance_report(res, rep_path)
        res["report_path"] = str(rep_path)
    return res


def preview_metadata_repairs(path: str | Path, language: str,
                             source_type: str, expected_format: str,
                             limit: int = 1000) -> dict[str, Any]:
    preview = rep.preview_repairs_for_file(path, language, source_type,
                                            expected_format, limit=limit)
    if preview.get("ok"):
        out = PHASE21A_REPAIR_DIR / f"{Path(path).stem}.{language}.repair.json"
        rep.write_repair_preview(preview, out)
        preview["preview_path"] = str(out)
    return preview


def write_rejected_preview(path: str | Path,
                           validation_result: dict[str, Any],
                           output_path: Optional[str | Path] = None
                           ) -> str:
    out = Path(output_path) if output_path is not None else \
        PHASE21A_REJECTED_DIR / f"{Path(path).stem}.rejected.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    rejected_rows = [r for r in (validation_result.get("rows") or [])
                     if r.get("verdict") == "reject"]
    with out.open("w", encoding="utf-8") as fh:
        for r in rejected_rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return str(out)


def write_ready_for_phase21_report(validation_results: list[dict[str, Any]],
                                   output_path: str | Path) -> str:
    decision = gate.produce_phase21_ready_decision(validation_results)
    return gate.write_phase21_ready_report(decision, output_path)


def build_phase21_rerun_instructions(output_path: Optional[str | Path] = None
                                     ) -> str:
    p = Path(output_path) if output_path is not None else \
        PHASE21A_GUIDE_DIR / "RERUN_PHASE21.md"
    p.parent.mkdir(parents=True, exist_ok=True)
    body = [
        "# Re-running Phase 21 after staging files",
        "",
        "After files are placed in `corpus_sources/english/incoming/` and "
        "`corpus_sources/russian/incoming/`, follow this sequence:",
        "",
        "1. `python test_phase21a_operator_corpus_staging.py` "
        "(validates files + writes acceptance/repair previews)",
        "2. Read the latest report under "
        "`corpus_sources/phase21a/ready_reports/`. State must be "
        "`READY_FOR_PHASE21_REAL_IMPORT` or `READY_FOR_DRY_RUN_ONLY`.",
        "3. `python test_phase21_operator_staged_first_import.py`",
        "4. If the state is `READY_FOR_PHASE21_REAL_IMPORT`, run the "
        "guarded import explicitly:",
        "",
        "```python",
        "import phase21_operator_stage_runner as r",
        "r.setup_phase21_folders()",
        "sources = r.discover_operator_staged_sources()",
        "enriched = r.register_phase21_sources(sources)",
        "gated   = r.run_phase21_quality_gates(enriched)",
        "plan    = r.build_phase21_stage_plan(gated, "
        "max_total_per_language=10000)",
        "snap    = r.create_phase21_backup_snapshot(label='first_real')",
        "dr      = r.run_phase21_dry_runs(plan)",
        "rr      = r.run_phase21_real_import(plan, allow_real_import=True, "
        "quality_reports=gated, dry_run_reports=dr, "
        "backup_snapshot_id=snap['snapshot_id'])",
        "```",
        "",
        "5. Inspect `rr['results']` and the post-import audits under "
        "`corpus_sources/phase21/`.",
    ]
    p.write_text("\n".join(body), encoding="utf-8")
    return str(p)


__all__ = [
    "PHASE21A_BASE", "PHASE21A_TEMPLATES_DIR", "PHASE21A_VALIDATION_DIR",
    "PHASE21A_REPAIR_DIR", "PHASE21A_REJECTED_DIR", "PHASE21A_READY_DIR",
    "PHASE21A_GUIDE_DIR", "PHASE21A_FIXTURE_DIR",
    "PHASE21A_REQUIRED_PRIOR",
    "verify_phase21a_preflight",
    "setup_phase21a_folders",
    "write_operator_templates",
    "write_operator_staging_guide",
    "discover_incoming_files",
    "inspect_incoming_file",
    "validate_incoming_file",
    "preview_metadata_repairs",
    "write_rejected_preview",
    "write_ready_for_phase21_report",
    "build_phase21_rerun_instructions",
]
