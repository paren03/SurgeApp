"""Phase 18 - Bilingual Pilot Corpus Import, Retrieval Hardening, and Scale
Validation.

Coordinates the Phase 18 pilot workflow on TOP of Phase 16/17 modules. Does
NOT add main-runtime integration, daemons, schedulers, or background loops.
No network. No full-file load.

Real pilot import is OFF by default. The caller must pass
``allow_real_import=True`` AND each plan must pass quality gate AND dry-run
AND have explicit ``max_entries`` to permit a real write.
"""

from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any, Optional

import dual_corpus_registry as reg
import dual_corpus_quality_gate as qg
import dual_corpus_source_adapters as adp
import dual_corpus_pilot_import_planner as pip_
import dual_retrieval_quality_eval as rqe
import dual_coverage_reporter as cr


PHASE17_REQUIRED = (
    "PHASE17_SOURCE_ADAPTERS_AND_RETRIEVAL_EVAL_REPORT.md",
    "test_phase17_source_adapters_and_retrieval_eval.py",
    "dual_corpus_source_adapters.py",
    "dual_corpus_pilot_import_planner.py",
    "dual_retrieval_quality_eval.py",
    "dual_coverage_reporter.py",
    "PHASE16_MILLION_SCALE_READINESS_REPORT.md",
    "dual_corpus_registry.py",
    "dual_corpus_chunked_importer.py",
    "dual_corpus_quality_gate.py",
    "dual_corpus_checkpoint.py",
)


DEFAULT_PILOT_MAX_PER_SOURCE = 5000
HARD_PILOT_TOTAL_CAP_PER_LANG = 25000

PHASE18_PILOT_DIR = Path("corpus_sources/pilot_imports/phase18")
PHASE18_EVAL_DIR = Path("corpus_sources/evaluations/phase18")
PHASE18_COVERAGE_DIR = Path("corpus_sources/coverage_reports/phase18")
PHASE18_SYNTH_DIR = Path("corpus_sources/quality_samples/phase18_synthetic")
PHASE18_REPORTS_DIR = Path("corpus_sources/reports/phase18")


def verify_phase17_preflight() -> dict[str, Any]:
    """Return {ok, missing_files} - does NOT mutate anything."""
    missing = [f for f in PHASE17_REQUIRED if not Path(f).exists()]
    return {"ok": not missing,
            "missing_files": missing,
            "checked": list(PHASE17_REQUIRED)}


def _adapter_to_source_type(adapter: str) -> str:
    """Map detected adapter type to the chunked-importer source_type."""
    return {
        "luna_jsonl": "word_list",
        "wiktextract_jsonl": "word_list",
        "simple_word_list_txt": "word_list",
        "frequency_word_list_txt": "word_list",
        "phrase_list_txt": "phrase_list",
        "idiom_list_txt": "idiom_list",
        "slang_list_txt": "slang_list",
        "profession_job_csv": "profession_job_list",
        "domain_terms_csv": "domain_terms",
        "bilingual_glossary_csv": "domain_terms",
        "russian_morphology_csv": "word_list",
        "mixed_jsonl": "mixed_jsonl",
    }.get(adapter, "word_list")


def classify_discovered_source(path: str | Path,
                               language: Optional[str] = None
                               ) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    det = adp.detect_adapter_type(p)
    if not det.get("ok"):
        return {"ok": False, "error": det.get("error", "adapter_not_recognized"),
                "path": str(p)}
    adapter = det["adapter_type"]
    if adapter.endswith("_jsonl"):
        fmt = "jsonl"
    elif adapter.endswith("_csv"):
        fmt = "csv"
    else:
        fmt = "txt"
    if language is None:
        parts = p.parts
        language = ("en" if "english" in parts else
                    "ru" if "russian" in parts else "en")
    return {"ok": True, "path": str(p), "adapter_type": adapter,
            "expected_format": fmt, "language": language,
            "source_type": _adapter_to_source_type(adapter)}


def discover_phase18_sources(language: Optional[str] = None,
                             limit: int = 100) -> list[dict[str, Any]]:
    raw = pip_.discover_incoming_sources(language=language, limit=limit)
    out: list[dict[str, Any]] = []
    for entry in raw:
        cls = classify_discovered_source(entry["path"], entry["language"])
        if cls.get("ok"):
            out.append({**entry, **cls})
    return out


def register_discovered_sources(sources: list[dict[str, Any]],
                                registry_db_path: Optional[str | Path] = None,
                                ) -> list[dict[str, Any]]:
    reg.init_registry(registry_db_path)
    enriched: list[dict[str, Any]] = []
    for s in sources:
        r = reg.register_corpus_source(
            language=s["language"], source_type=s["source_type"],
            expected_format=s["expected_format"], source_path=s["path"],
            notes="phase18_discovery",
            db_path=registry_db_path)
        if r.get("ok"):
            enriched.append({**s, "corpus_id": r["corpus_id"],
                             "source_sha256": r["source_sha256"],
                             "row_estimate": r["row_estimate"]})
        else:
            enriched.append({**s, "register_error": r.get("error")})
    return enriched


def run_quality_gates_for_sources(source_records: list[dict[str, Any]]
                                  ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in source_records:
        if "register_error" in s:
            out.append({**s, "quality_report": None,
                        "quality_ok": False,
                        "quality_error": s["register_error"]})
            continue
        rep = qg.generate_quality_gate_report(
            s["path"], s["expected_format"], s["language"],
            sample_size=100)
        gate = qg.should_allow_import(rep, min_quality_score=0.75)
        out.append({**s, "quality_report": rep, "quality_ok": gate["ok"],
                    "quality_gate_decision": gate})
    return out


def build_pilot_plans_for_sources(
    source_records: list[dict[str, Any]],
    max_entries_per_source: int = DEFAULT_PILOT_MAX_PER_SOURCE,
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for s in source_records:
        if not s.get("quality_ok"):
            plans.append({"source": s, "plan": None,
                          "skipped": True,
                          "skip_reason": "quality_gate_closed"})
            continue
        bp = pip_.build_pilot_plan(
            source_path=s["path"], language=s["language"],
            source_type=s["source_type"],
            adapter_type=s["adapter_type"],
            target_entries=min(max_entries_per_source,
                               DEFAULT_PILOT_MAX_PER_SOURCE))
        if not bp.get("ok"):
            plans.append({"source": s, "plan": None,
                          "skipped": True,
                          "skip_reason": bp.get("error", "plan_build_failed")})
            continue
        plan = bp["plan"]
        plans.append({"source": s, "plan": plan, "skipped": False})
        try:
            pip_.write_pilot_plan(plan,
                                  PHASE18_PILOT_DIR / f"{plan['plan_id']}.plan.json")
        except Exception:
            pass
    return plans


def run_phase18_dry_runs(
    plans: list[dict[str, Any]],
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in plans:
        plan = item.get("plan")
        if item.get("skipped") or not plan:
            results.append({**item, "dry_run": None,
                            "dry_run_ok": False,
                            "dry_run_reason": item.get("skip_reason",
                                                       "no_plan")})
            continue
        dr = pip_.run_pilot_dry_run(
            plan, registry_db_path=registry_db_path,
            checkpoint_db_path=checkpoint_db_path,
            en_db_path=en_db_path, ru_db_path=ru_db_path)
        try:
            pip_.write_pilot_result(
                dr, PHASE18_PILOT_DIR / f"{plan['plan_id']}.dry.json")
        except Exception:
            pass
        results.append({**item, "dry_run": dr,
                        "dry_run_ok": bool(dr.get("ok")),
                        "dry_run_reason": dr.get("error", "")})
    return results


def run_phase18_real_pilots(
    plans: list[dict[str, Any]],
    *,
    allow_real_import: bool = False,
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not allow_real_import:
        return {"ok": True, "allow_real_import": False,
                "reason": "real_import_disabled_by_default",
                "results": []}
    # Per-language running total cap.
    used: dict[str, int] = {"en": 0, "ru": 0}
    results: list[dict[str, Any]] = []
    for item in plans:
        plan = item.get("plan")
        if not plan or item.get("skipped"):
            results.append({**item, "real": None,
                            "real_skipped_reason": "no_plan_or_skipped"})
            continue
        if not item.get("dry_run_ok"):
            results.append({**item, "real": None,
                            "real_skipped_reason": "dry_run_failed"})
            continue
        lang = plan["language"]
        max_e = int(plan["safe_max_entries"])
        if max_e <= 0:
            results.append({**item, "real": None,
                            "real_skipped_reason": "max_entries_invalid"})
            continue
        # Enforce Phase 18 hard cap per language across this run.
        if used[lang] + max_e > HARD_PILOT_TOTAL_CAP_PER_LANG:
            results.append({**item, "real": None,
                            "real_skipped_reason":
                            f"hard_cap_exceeded:{lang}"})
            continue
        rr = pip_.run_pilot_import(
            plan, dry_run=False,
            registry_db_path=registry_db_path,
            checkpoint_db_path=checkpoint_db_path,
            en_db_path=en_db_path, ru_db_path=ru_db_path)
        try:
            pip_.write_pilot_result(
                rr, PHASE18_PILOT_DIR / f"{plan['plan_id']}.real.json")
        except Exception:
            pass
        accepted = int(rr.get("accepted") or 0)
        used[lang] += accepted
        results.append({**item, "real": rr,
                        "real_ok": bool(rr.get("ok")),
                        "real_accepted": accepted})
    return {"ok": True, "allow_real_import": True,
            "used_per_language": used, "results": results}


# ----------------- Synthetic fixtures -----------------

_EN_DOMAINS = [
    ("core_vocabulary", ["standard"], [], "a synthetic core word for tests"),
    ("professions_jobs", ["standard", "professional"], [],
     "a synthetic profession-related word"),
    ("science_math", ["standard", "technical", "academic"], [],
     "a synthetic science term"),
    ("coding_technology", ["standard", "technical", "coding"], [],
     "a synthetic software term"),
    ("idioms_phrases", ["standard"], [], "a synthetic idiom"),
]


_RU_DOMAINS = [
    ("core_vocabulary", ["standard"], [], "синтетическое базовое слово"),
    ("professions_jobs", ["standard", "professional"], [],
     "синтетическое слово по профессии"),
    ("science_math", ["standard", "technical", "academic"], [],
     "синтетический научный термин"),
    ("coding_technology", ["standard", "technical", "coding"], [],
     "синтетическое слово по программированию"),
    ("idioms_phrases", ["standard"], [], "синтетический идиоматический оборот"),
]


def _safe_en_word(i: int) -> str:
    return f"phase18_en_w_{i:06d}"


def _safe_ru_word(i: int) -> str:
    return f"фаза18_ру_слово_{i:06d}"


def _make_en_row(i: int, cov: str, regs: list[str],
                 safety: list[str], defn: str) -> dict[str, Any]:
    return {"word": _safe_en_word(i), "language": "en",
            "definition": defn,
            "examples": [f"example sentence for {_safe_en_word(i)}"],
            "tags": ["synthetic", "phase18"],
            "coverage_categories": [cov],
            "register_tags": list(regs),
            "safety_tags": list(safety),
            "frequency_score": round(0.1 + (i % 50) / 100.0, 3),
            "word_level": "common"}


def _make_ru_row(i: int, cov: str, regs: list[str],
                 safety: list[str], defn: str) -> dict[str, Any]:
    return {"word": _safe_ru_word(i), "lemma": _safe_ru_word(i),
            "part_of_speech": "noun", "language": "ru",
            "definition": defn,
            "examples": [f"пример предложения для {_safe_ru_word(i)}"],
            "tags": ["synthetic", "phase18"],
            "coverage_categories": [cov],
            "register_tags": list(regs),
            "safety_tags": list(safety),
            "frequency_score": round(0.1 + (i % 50) / 100.0, 3),
            "word_level": "common"}


def _emit_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _emit_txt(path: Path, words: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(words) + "\n", encoding="utf-8")


def _emit_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        fh.write(",".join(header) + "\n")
        for r in rows:
            fh.write(",".join(c.replace(",", " ") for c in r) + "\n")


def generate_synthetic_phase18_fixtures(
    output_dir: str | Path = PHASE18_SYNTH_DIR,
    rows_per_large_fixture: int = 10000,
    seed: int = 42,
) -> dict[str, Any]:
    od = Path(output_dir)
    od.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    n = max(100, int(rows_per_large_fixture))

    # English JSONL — valid + controlled noise.
    en_rows: list[dict[str, Any]] = []
    for i in range(n):
        cov, regs, saf, defn = _EN_DOMAINS[i % len(_EN_DOMAINS)]
        en_rows.append(_make_en_row(i, cov, regs, saf, defn))
    # 1% controlled duplicates
    for i in range(n // 100):
        en_rows.append(_make_en_row(i, "core_vocabulary", ["standard"], [],
                                    "duplicate fixture row"))
    # 0.5% malformed
    for i in range(n // 200):
        en_rows.append({"definition": "missing word field"})
    # 0.5% controlled recognition_only
    for i in range(n // 200):
        cov = "philosophy_abstract"
        en_rows.append({"word": f"phase18_recog_{i:04d}", "language": "en",
                        "definition": "a synthetic sensitive academic word",
                        "coverage_categories": [cov],
                        "register_tags": ["academic", "recognition_only",
                                          "do_not_use_unprompted"],
                        "safety_tags": ["recognition_only",
                                        "do_not_use_unprompted"]})
    # 0.5% slang/street rows
    for i in range(n // 200):
        en_rows.append({"word": f"phase18_slang_{i:04d}", "language": "en",
                        "definition": "a synthetic slang term",
                        "coverage_categories": ["slang_street_talk"],
                        "register_tags": ["slang"],
                        "safety_tags": []})
    rng.shuffle(en_rows)
    en_jsonl = od / "english_large_jsonl_fixture.jsonl"
    _emit_jsonl(en_jsonl, en_rows)

    # Russian JSONL — same shape
    ru_rows: list[dict[str, Any]] = []
    for i in range(n):
        cov, regs, saf, defn = _RU_DOMAINS[i % len(_RU_DOMAINS)]
        ru_rows.append(_make_ru_row(i, cov, regs, saf, defn))
    for i in range(n // 100):
        ru_rows.append(_make_ru_row(i, "core_vocabulary", ["standard"], [],
                                    "дубликат фикстуры"))
    for i in range(n // 200):
        ru_rows.append({"definition": "без слова"})
    for i in range(n // 200):
        ru_rows.append({"word": f"фаза18_recog_{i:04d}", "language": "ru",
                        "lemma": f"фаза18_recog_{i:04d}",
                        "part_of_speech": "noun",
                        "definition": "синтетическое чувствительное слово",
                        "coverage_categories": ["philosophy_abstract"],
                        "register_tags": ["academic", "recognition_only",
                                          "do_not_use_unprompted"],
                        "safety_tags": ["recognition_only",
                                        "do_not_use_unprompted"]})
    for i in range(n // 200):
        ru_rows.append({"word": f"фаза18_slang_{i:04d}", "language": "ru",
                        "lemma": f"фаза18_slang_{i:04d}",
                        "part_of_speech": "noun",
                        "definition": "синтетический сленговый термин",
                        "coverage_categories": ["slang_street_talk"],
                        "register_tags": ["slang"],
                        "safety_tags": []})
    rng.shuffle(ru_rows)
    ru_jsonl = od / "russian_large_jsonl_fixture.jsonl"
    _emit_jsonl(ru_jsonl, ru_rows)

    # TXT fixtures (smaller — txt is a flat per-line format)
    en_words = [_safe_en_word(i) for i in range(n)]
    en_txt = od / "english_large_txt_fixture.txt"
    _emit_txt(en_txt, en_words)
    ru_words = [_safe_ru_word(i) for i in range(n)]
    ru_txt = od / "russian_large_txt_fixture.txt"
    _emit_txt(ru_txt, ru_words)

    # Bilingual glossary CSV
    gloss_path = od / "bilingual_glossary_fixture.csv"
    gloss_rows = [[f"phase18_en_g_{i:05d}", f"фаза18_ру_g_{i:05d}",
                   "synthetic english gloss", "синтетический русский глосс",
                   "core_vocabulary", "standard"]
                  for i in range(max(100, n // 10))]
    _emit_csv(gloss_path,
              ["en", "ru", "definition_en", "definition_ru",
               "coverage", "register"], gloss_rows)

    # Russian morphology CSV
    morph_path = od / "russian_morphology_fixture.csv"
    morph_rows = [[f"фаза18_morph_{i:05d}", f"фаза18_morph_{i:05d}", "noun",
                   "синтетическое слово морфологии",
                   "genitive", "singular", "masculine"]
                  for i in range(max(100, n // 10))]
    _emit_csv(morph_path,
              ["word", "lemma", "pos", "definition",
               "case", "number", "gender"], morph_rows)

    return {"ok": True, "output_dir": str(od),
            "files": {
                "english_jsonl": str(en_jsonl),
                "russian_jsonl": str(ru_jsonl),
                "english_txt": str(en_txt),
                "russian_txt": str(ru_txt),
                "bilingual_glossary_csv": str(gloss_path),
                "russian_morphology_csv": str(morph_path),
            },
            "rows_per_large_fixture": n}


def run_synthetic_streaming_validation(
    fixtures: Optional[dict[str, str]] = None,
    sample_size: int = 100,
) -> dict[str, Any]:
    """Stream-only validation: parse + quality-gate each fixture without
    writing to any lexicon store."""
    out: dict[str, Any] = {}
    if not fixtures:
        return {"ok": False, "error": "no_fixtures_provided"}
    for label, path in fixtures.items():
        p = Path(path)
        if not p.exists():
            out[label] = {"ok": False, "error": "missing"}
            continue
        if label.endswith("jsonl"):
            fmt = "jsonl"
            lang = "en" if "english" in label else "ru"
        elif label.endswith("csv"):
            fmt = "csv"
            lang = "ru" if "russian" in label else "en"
        else:
            fmt = "txt"
            lang = "en" if "english" in label else "ru"
        rep = qg.generate_quality_gate_report(p, fmt, lang,
                                              sample_size=sample_size)
        out[label] = {"ok": bool(rep.get("ok")),
                      "quality_score": rep.get("quality_score"),
                      "rows_scored": rep.get("rows_scored")}
    return {"ok": True, "results": out}


# ----------------- Post-pilot retrieval + coverage -----------------

def run_post_pilot_retrieval_eval(
    limit: int = 15,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
    output_dir: str | Path = PHASE18_EVAL_DIR,
) -> dict[str, Any]:
    od = Path(output_dir)
    od.mkdir(parents=True, exist_ok=True)
    en = rqe.run_english_retrieval_eval(limit=limit, db_path=en_db_path)
    ru = rqe.run_russian_retrieval_eval(limit=limit, db_path=ru_db_path)
    rqe.write_retrieval_eval_report(en, od / "phase18_retrieval_eval_en.json")
    rqe.write_retrieval_eval_report(ru, od / "phase18_retrieval_eval_ru.json")
    return {"ok": True, "en": en, "ru": ru,
            "en_path": str(od / "phase18_retrieval_eval_en.json"),
            "ru_path": str(od / "phase18_retrieval_eval_ru.json")}


def run_post_pilot_coverage_report(
    output_dir: str | Path = PHASE18_COVERAGE_DIR,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    od = Path(output_dir)
    od.mkdir(parents=True, exist_ok=True)
    res = cr.write_coverage_report(od / "phase18_coverage_report.json",
                                   en_db_path=en_db_path,
                                   ru_db_path=ru_db_path)
    return res


def write_phase18_summary_report(report: dict[str, Any],
                                 output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "PHASE17_REQUIRED",
    "DEFAULT_PILOT_MAX_PER_SOURCE",
    "HARD_PILOT_TOTAL_CAP_PER_LANG",
    "PHASE18_PILOT_DIR",
    "PHASE18_EVAL_DIR",
    "PHASE18_COVERAGE_DIR",
    "PHASE18_SYNTH_DIR",
    "PHASE18_REPORTS_DIR",
    "verify_phase17_preflight",
    "discover_phase18_sources",
    "classify_discovered_source",
    "register_discovered_sources",
    "run_quality_gates_for_sources",
    "build_pilot_plans_for_sources",
    "run_phase18_dry_runs",
    "run_phase18_real_pilots",
    "generate_synthetic_phase18_fixtures",
    "run_synthetic_streaming_validation",
    "run_post_pilot_retrieval_eval",
    "run_post_pilot_coverage_report",
    "write_phase18_summary_report",
]
