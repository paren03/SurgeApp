"""Phase 17 - Source Adapters, Pilot Planning, Retrieval Evaluation, and
Coverage Reporting.

Synthetic-fixtures-only harness. No real corpora are downloaded. No internet.
No daemons. Production lexicon DBs are read-only inspected; pilot writes go to
temp DBs.

Suites:
    A  Pre-flight (Phase 16 + earlier artifacts)
    B  Adapter detection
    C  Streaming normalization
    D  Pilot planner
    E  Retrieval evaluation
    F  Coverage reporter
    G  Templates
    H  Isolation
    I  No daemons / no recursion / bounded streaming
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")

import dual_corpus_source_adapters as adp
import dual_corpus_pilot_import_planner as pip_
import dual_retrieval_quality_eval as rqe
import dual_coverage_reporter as cr


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _temp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase17_"))


def _write(p: Path, text: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _write_jsonl(p: Path, rows: list[dict]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return p


# -------------------- Suite A: Pre-flight --------------------

PHASE16_FILES = [
    "PHASE16_MILLION_SCALE_READINESS_REPORT.md",
    "test_phase16_million_scale_readiness.py",
    "dual_corpus_registry.py",
    "dual_corpus_chunked_importer.py",
    "dual_corpus_quality_gate.py",
    "dual_corpus_checkpoint.py",
]


def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    for f in PHASE16_FILES:
        _check(suite, f"phase16_{f}_exists", Path(f).exists(), f"missing: {f}")
    _check(suite, "corpus_sources_folder_exists",
           Path("corpus_sources").exists(), "")
    # The planner must be able to recognize a missing-prereq scenario at
    # least at the file-existence level — verified by directly checking the
    # constant list against disk.
    _check(suite, "clean_failure_path_documented",
           all(Path(f).exists() for f in PHASE16_FILES),
           "one or more Phase 16 files missing — Phase 17 would refuse to mutate")


# -------------------- Suite B: Adapter detection --------------------

def suite_B_detection() -> None:
    suite = "B_DETECTION"
    td = _temp_dir()

    luna = _write_jsonl(td / "en_luna.jsonl",
                        [{"word": "alpha", "definition": "first letter",
                          "language": "en"}])
    wikt = _write_jsonl(td / "en_wikt.jsonl",
                        [{"word": "alpha", "lang_code": "en",
                          "senses": [{"glosses": ["first letter"]}]}])
    simple = _write(td / "simple.txt", "lighthouse\nengineer\nessence\n")
    freq = _write(td / "freq.txt", "the 0.99\nof 0.92\nand 0.91\n")
    phr = _write(td / "phr.txt", "good morning\nbreak the ice\ntake it easy\n")
    prof = _write(td / "prof.csv", "word,definition,profession\n"
                                    "engineer,builds things,yes\n")
    dom = _write(td / "dom.csv", "word,definition,domain\n"
                                  "vector,a quantity,math\n")
    gloss = _write(td / "gloss.csv", "en,ru,definition_en\n"
                                      "ledger,главная книга,a book\n")
    bad = td / "nope.bin"
    bad.write_bytes(b"\x00\x01\x02\x03")

    _check(suite, "detects_luna_jsonl",
           adp.detect_adapter_type(luna)["adapter_type"] == "luna_jsonl", "")
    _check(suite, "detects_wiktextract_jsonl",
           adp.detect_adapter_type(wikt)["adapter_type"] == "wiktextract_jsonl", "")
    _check(suite, "detects_simple_word_list",
           adp.detect_adapter_type(simple)["adapter_type"] == "simple_word_list_txt", "")
    _check(suite, "detects_frequency_word_list",
           adp.detect_adapter_type(freq)["adapter_type"] == "frequency_word_list_txt", "")
    _check(suite, "detects_phrase_list",
           adp.detect_adapter_type(phr)["adapter_type"] == "phrase_list_txt", "")
    prof_det = adp.detect_adapter_type(prof)
    _check(suite, "detects_profession_csv",
           prof_det["adapter_type"] == "profession_job_csv", str(prof_det))
    dom_det = adp.detect_adapter_type(dom)
    _check(suite, "detects_domain_csv",
           dom_det["adapter_type"] == "domain_terms_csv", str(dom_det))
    gloss_det = adp.detect_adapter_type(gloss)
    _check(suite, "detects_bilingual_glossary_csv",
           gloss_det["adapter_type"] == "bilingual_glossary_csv",
           str(gloss_det))

    miss = adp.detect_adapter_type(td / "missing.txt")
    _check(suite, "missing_file_clean_failure",
           miss.get("ok") is False
           and miss.get("error") == "file_not_found", str(miss))


# -------------------- Suite C: Streaming normalization --------------------

def suite_C_normalize() -> None:
    suite = "C_NORMALIZE"

    en = adp.normalize_luna_jsonl_row(
        {"word": "alpha", "definition": "first letter", "language": "en",
         "frequency_score": 0.42, "coverage_categories": ["core_vocabulary"]},
        "en", "word_list")
    _check(suite, "en_jsonl_ok", en["ok"], str(en))
    _check(suite, "en_jsonl_freq_preserved",
           en["normalized"]["frequency_score"] == 0.42, str(en))

    ru = adp.normalize_luna_jsonl_row(
        {"word": "альфа", "lemma": "альфа", "part_of_speech": "noun",
         "definition": "первая буква", "language": "ru",
         "coverage_categories": ["core_vocabulary"]},
        "ru", "word_list")
    _check(suite, "ru_jsonl_ok", ru["ok"], str(ru))
    _check(suite, "ru_lemma_preserved",
           ru["normalized"]["lemma"] == "альфа", str(ru))
    _check(suite, "ru_pos_preserved",
           ru["normalized"]["part_of_speech"] == "noun", "")

    wikt = adp.normalize_wiktextract_row(
        {"word": "alpha", "lang_code": "en",
         "senses": [{"glosses": ["first letter"]}]},
        "en", "word_list")
    _check(suite, "wikt_ok", wikt["ok"], str(wikt))
    _check(suite, "wikt_definition_built",
           "first letter" in wikt["normalized"]["definition"], str(wikt))

    txt = adp.normalize_simple_word_row("lighthouse", "en", "word_list")
    _check(suite, "simple_txt_ok",
           txt["ok"] and txt["normalized"]["word"] == "lighthouse", str(txt))

    freq = adp.normalize_frequency_word_row("the 0.99", "en", "word_list")
    _check(suite, "freq_ok",
           freq["ok"]
           and freq["normalized"]["word"] == "the"
           and freq["normalized"]["frequency_score"] >= 0.0, str(freq))

    phr = adp.normalize_phrase_row("good morning", "en", "phrase_list")
    _check(suite, "phrase_ok",
           phr["ok"] and phr["normalized"]["phrase"] == "good morning",
           str(phr))

    csv_ = adp.normalize_csv_row(
        {"word": "engineer", "definition": "builds things",
         "coverage": "professions_jobs", "register": "standard;professional"},
        "en", "profession_job_list")
    _check(suite, "csv_ok", csv_["ok"], str(csv_))
    _check(suite, "csv_register_split",
           "professional" in csv_["normalized"]["register_tags"], str(csv_))

    bad = adp.normalize_luna_jsonl_row({"definition": "no word"}, "en",
                                       "word_list")
    _check(suite, "rejects_missing_word",
           bad["ok"] is False, str(bad))

    bad2 = adp.normalize_luna_jsonl_row(
        {"word": "x",
         "definition": "step by step instructions to bypass auth"},
        "en", "word_list")
    _check(suite, "rejects_operational_unsafe", bad2["ok"] is False, str(bad2))

    # Streaming + max_rows + does-not-load-whole-file
    td = _temp_dir()
    big = _write_jsonl(td / "big.jsonl",
                       [{"word": f"w_{i}", "language": "en"}
                        for i in range(500)])
    rows = list(adp.iter_normalized_rows(big, "luna_jsonl", "en", "word_list",
                                         max_rows=25))
    _check(suite, "max_rows_respected", len(rows) == 25, str(len(rows)))
    _check(suite, "all_results_have_ok_key",
           all("ok" in r for r in rows), "")

    # Slang adapter must auto-add slang register
    slang_rows = list(adp.iter_normalized_rows(
        Path("corpus_sources/templates/slang_list_template.txt"),
        "slang_list_txt", "en", "slang_list", max_rows=2))
    if slang_rows:
        _check(suite, "slang_register_autoadded",
               "slang" in (slang_rows[0]["normalized"]["register_tags"]
                           if slang_rows[0]["ok"] else []),
               str(slang_rows[0]))


# -------------------- Suite D: Pilot planner --------------------

def suite_D_planner() -> None:
    suite = "D_PLANNER"
    td = _temp_dir()
    src = _write_jsonl(td / "incoming" / "en.jsonl",
                       [{"word": f"good_{i}", "language": "en",
                         "definition": "a clear synthetic definition",
                         "coverage_categories": ["core_vocabulary"],
                         "register_tags": ["standard"]} for i in range(120)])

    # File-size + estimated_rows pickers
    bs1 = pip_.choose_batch_size(500, 100)
    _check(suite, "batch_size_small_file", bs1 <= 50 and bs1 >= 10,
           f"bs1={bs1}")
    bs2 = pip_.choose_batch_size(100_000_000, 200_000)
    _check(suite, "batch_size_large_file", bs2 >= 500, f"bs2={bs2}")
    sme = pip_.choose_safe_max_entries(50_000, "word_list", 0.9)
    _check(suite, "safe_max_high_quality_capped_at_target",
           sme == pip_.DEFAULT_TARGET_ENTRIES, f"sme={sme}")
    sme2 = pip_.choose_safe_max_entries(50_000, "slang_list", 0.9)
    _check(suite, "safe_max_slang_capped_lower",
           sme2 <= 300, f"sme2={sme2}")
    sme3 = pip_.choose_safe_max_entries(50_000, "word_list", 0.5)
    _check(suite, "safe_max_low_quality_capped",
           sme3 <= 200, f"sme3={sme3}")

    plan = pip_.build_pilot_plan(src, "en", "word_list",
                                 target_entries=80)
    _check(suite, "plan_built", plan.get("ok") is True, str(plan)[:200])
    p = plan["plan"]
    _check(suite, "dry_run_default", p["dry_run_default"] is True, "")
    _check(suite, "required_quality_gate", p["required_quality_gate"] is True, "")
    _check(suite, "rollback_key_present", bool(p["rollback_key"]), "")
    _check(suite, "safe_max_respects_target",
           p["safe_max_entries"] <= 80, str(p["safe_max_entries"]))

    # write/read
    out = td / "plan.json"
    pip_.write_pilot_plan(p, out)
    _check(suite, "plan_writes_to_disk", out.exists(), "")

    # Dry run via plan into temp DB
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    en_db = td / "en.sqlite3"
    dry = pip_.run_pilot_dry_run(p, registry_db_path=reg_db,
                                 checkpoint_db_path=ck_db,
                                 en_db_path=en_db)
    _check(suite, "dry_run_ok", dry.get("ok") is True, str(dry)[:200])
    _check(suite, "dry_run_accepted",
           dry["accepted"] <= p["safe_max_entries"], str(dry["accepted"]))

    # Force a failed quality gate by hand and confirm refusal.
    bad_plan = dict(p)
    bad_plan["quality_report"] = {"ok": False, "operational_unsafe_count": 5}
    refused = pip_.run_pilot_import(bad_plan, dry_run=False,
                                    registry_db_path=reg_db,
                                    checkpoint_db_path=ck_db,
                                    en_db_path=en_db)
    _check(suite, "failed_qgate_refused",
           refused.get("ok") is False
           and refused.get("error") == "quality_gate_blocked", str(refused))

    # Discovery is bounded
    seed = pip_.discover_incoming_sources(language="en", limit=5)
    _check(suite, "discovery_bounded",
           isinstance(seed, list) and len(seed) <= 5, str(len(seed)))


# -------------------- Suite E: Retrieval evaluation --------------------

def suite_E_eval() -> None:
    suite = "E_EVAL"
    qs = rqe.build_eval_queries()
    _check(suite, "eval_queries_built",
           len(qs["en"]) == 12 and len(qs["ru"]) == 12,
           f"en={len(qs['en'])} ru={len(qs['ru'])}")

    en_eval = rqe.run_english_retrieval_eval(limit=10)
    _check(suite, "en_eval_runs", en_eval.get("ok") is True, "")
    _check(suite, "en_eval_bounded",
           en_eval["bounds_ok"] is True, str(en_eval)[:200])
    _check(suite, "en_eval_has_12_queries",
           en_eval["n_queries"] == 12, str(en_eval["n_queries"]))

    ru_eval = rqe.run_russian_retrieval_eval(limit=10)
    _check(suite, "ru_eval_runs", ru_eval.get("ok") is True, "")
    _check(suite, "ru_eval_bounded",
           ru_eval["bounds_ok"] is True, str(ru_eval)[:200])
    _check(suite, "ru_eval_has_12_queries",
           ru_eval["n_queries"] == 12, str(ru_eval["n_queries"]))

    # Safety check works on synthetic results
    rs = [{"word": "x", "safety_tags": ["recognition_only", "do_not_use_unprompted"]}]
    chk = rqe.check_safety_policy_on_results(rs, mode="teacher",
                                             is_user_prompted=False)
    _check(suite, "safety_blocks_do_not_use",
           chk["ok"] is False
           and chk["do_not_use_violation_count"] == 1, str(chk))

    # Coverage check
    cov = rqe.check_category_coverage(
        [{"word": "x", "coverage_categories": ["professions_jobs"]}],
        ["professions_jobs"])
    _check(suite, "category_coverage_detected",
           cov["hit_rows"] == 1 and cov["ok"], str(cov))

    # Bounds check
    b = rqe.check_result_bounds([{}] * 10, 5)
    _check(suite, "bounds_violation_flagged",
           b["ok"] is False, str(b))

    td = _temp_dir()
    rep_path = td / "eval.json"
    rqe.write_retrieval_eval_report(en_eval, rep_path)
    _check(suite, "eval_report_written", rep_path.exists(), "")


# -------------------- Suite F: Coverage reporter --------------------

def suite_F_coverage() -> None:
    suite = "F_COVERAGE"
    totals = cr.count_entries_by_language()
    _check(suite, "totals_en_present", totals["en_words"] >= 0, str(totals))
    _check(suite, "totals_ru_present", totals["ru_words"] >= 0, str(totals))
    _check(suite, "totals_ru_phrases_present",
           totals["ru_phrases"] >= 0, str(totals))

    en_cov = cr.count_entries_by_coverage_category("en")
    ru_cov = cr.count_entries_by_coverage_category("ru")
    _check(suite, "en_coverage_has_21_keys",
           len(en_cov) == 21, str(len(en_cov)))
    _check(suite, "ru_coverage_has_21_keys",
           len(ru_cov) == 21, str(len(ru_cov)))

    en_reg = cr.count_entries_by_register_tag("en")
    _check(suite, "en_register_has_22_keys",
           len(en_reg) == 22, str(len(en_reg)))

    en_safe = cr.count_entries_by_safety_tag("en")
    _check(suite, "en_safety_has_4_keys",
           len(en_safe) == 4, str(len(en_safe)))

    low = cr.identify_low_coverage_categories("en", min_entries=10)
    _check(suite, "low_coverage_callable",
           isinstance(low, list), str(type(low)))

    gaps = cr.identify_missing_metadata("en", limit=5)
    _check(suite, "metadata_gaps_callable",
           isinstance(gaps, dict)
           and "rows_with_no_coverage" in gaps, str(gaps))

    bal = cr.compare_english_russian_category_balance()
    _check(suite, "balance_has_per_category",
           isinstance(bal.get("per_category"), list)
           and len(bal["per_category"]) == 21, str(len(bal["per_category"])))

    td = _temp_dir()
    rep = cr.write_coverage_report(td / "coverage.json")
    _check(suite, "coverage_report_written",
           rep.get("ok") and Path(rep["report_path"]).exists(),
           str(rep)[:200])


# -------------------- Suite G: Templates --------------------

TEMPLATE_FILES = [
    "english_luna_jsonl_template.jsonl",
    "russian_luna_jsonl_template.jsonl",
    "profession_job_csv_template.csv",
    "domain_terms_csv_template.csv",
    "bilingual_glossary_csv_template.csv",
    "russian_morphology_csv_template.csv",
    "simple_word_list_template.txt",
    "phrase_list_template.txt",
    "slang_list_template.txt",
]


def suite_G_templates() -> None:
    suite = "G_TEMPLATES"
    base = Path("corpus_sources/templates")
    for name in TEMPLATE_FILES:
        p = base / name
        _check(suite, f"{name}_exists", p.exists(), "")
    # Parse English JSONL
    en_p = base / "english_luna_jsonl_template.jsonl"
    if en_p.exists():
        rows = list(adp.iter_normalized_rows(en_p, "luna_jsonl", "en",
                                             "word_list", max_rows=10))
        ok_rows = [r for r in rows if r["ok"]]
        _check(suite, "english_jsonl_template_parses",
               len(ok_rows) >= 3, str(len(ok_rows)))
        # tags check
        for r in ok_rows:
            n = r["normalized"]
            ok_cov = all(c for c in n["coverage_categories"])
            ok_reg = all(rg for rg in n["register_tags"])
            _check(suite, f"en_tpl_row_{n['word']}_has_valid_tags",
                   ok_cov and ok_reg, str(n))
    # Parse Russian JSONL
    ru_p = base / "russian_luna_jsonl_template.jsonl"
    if ru_p.exists():
        rows = list(adp.iter_normalized_rows(ru_p, "luna_jsonl", "ru",
                                             "word_list", max_rows=10))
        ok_rows = [r for r in rows if r["ok"]]
        _check(suite, "russian_jsonl_template_parses",
               len(ok_rows) >= 3, str(len(ok_rows)))
        for r in ok_rows:
            n = r["normalized"]
            _check(suite, f"ru_tpl_row_{n['word']}_lemma_present",
                   bool(n["lemma"]), str(n))
    # CSV templates parse
    csv_p = base / "profession_job_csv_template.csv"
    if csv_p.exists():
        rows = list(adp.iter_normalized_rows(csv_p, "profession_job_csv",
                                             "en", "profession_job_list",
                                             max_rows=10))
        ok_rows = [r for r in rows if r["ok"]]
        _check(suite, "profession_csv_template_parses",
               len(ok_rows) >= 3, str(len(ok_rows)))


# -------------------- Suite H: Isolation --------------------

PHASE17_FILES = [
    "dual_corpus_source_adapters.py",
    "dual_corpus_pilot_import_planner.py",
    "dual_retrieval_quality_eval.py",
    "dual_coverage_reporter.py",
]


def suite_H_isolation() -> None:
    suite = "H_ISOLATION"
    import re
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
    # Concrete daemon/background USAGE patterns (not docstring mentions).
    DAEMON_USAGE_PATTERNS = (
        r"threading\.Thread\s*\(",
        r"multiprocessing\.Process\s*\(",
        r"asyncio\.create_task\s*\(",
        r"subprocess\.Popen\s*\(",
        r"^\s*(import|from)\s+schedule(\s|$|\.|,)",
        r"^\s*(import|from)\s+apscheduler",
        r"BackgroundScheduler\s*\(",
        r"threading\.Timer\s*\(",
        r"^\s*while\s+True\s*:",
    )
    NETWORK_USAGE_PATTERNS = (
        r"^\s*(import|from)\s+(urllib|requests|httpx|aiohttp|socket|ftplib)\b",
        r"urlopen\s*\(",
        r"http\.client",
    )
    for fname in PHASE17_FILES:
        p = Path(fname)
        if not p.exists():
            _check(suite, f"{fname}_exists", False, "missing")
            continue
        text = p.read_text(encoding="utf-8")
        # Forbidden imports
        bad: list[str] = []
        for line in text.splitlines():
            for forb in FORBIDDEN:
                if re.search(rf"^(import|from)\s+\S*{re.escape(forb)}", line):
                    bad.append(line.strip())
        _check(suite, f"{fname}_no_forbidden_imports",
               not bad, "; ".join(bad[:3]))
        # Network usage
        net_hits: list[str] = []
        for pat in NETWORK_USAGE_PATTERNS:
            for m in re.finditer(pat, text, flags=re.MULTILINE):
                net_hits.append(m.group(0).strip())
        _check(suite, f"{fname}_no_network_tokens",
               not net_hits, "; ".join(net_hits[:3]))
        # Daemon / background usage
        d_hits: list[str] = []
        for pat in DAEMON_USAGE_PATTERNS:
            for m in re.finditer(pat, text, flags=re.MULTILINE):
                d_hits.append(m.group(0).strip())
        _check(suite, f"{fname}_no_daemon_tokens",
               not d_hits, "; ".join(d_hits[:3]))


# -------------------- Suite I: Streaming + recursion bounds --------------------

def suite_I_bounds() -> None:
    suite = "I_BOUNDS"
    # iter_normalized_rows must yield in <= len(rows) and respect max_rows
    td = _temp_dir()
    p = _write_jsonl(td / "stream.jsonl",
                     [{"word": f"w_{i}", "language": "en"} for i in range(2000)])
    n = 0
    for _ in adp.iter_normalized_rows(p, "luna_jsonl", "en", "word_list",
                                      max_rows=100):
        n += 1
        if n > 200:
            break
    _check(suite, "iter_normalized_max_rows_holds", n == 100, str(n))

    # Quality gate sample is bounded
    import dual_corpus_quality_gate as qg
    s = qg.sample_corpus(p, "jsonl", sample_size=50,
                          strategy="head_middle_tail")
    _check(suite, "qg_sample_bounded",
           len(s["rows"]) <= 50, str(len(s["rows"])))

    # Coverage reporter is read-only — counts before/after should be identical
    before = cr.count_entries_by_language()
    _ = cr.count_entries_by_coverage_category("en")
    _ = cr.count_entries_by_register_tag("ru")
    after = cr.count_entries_by_language()
    _check(suite, "coverage_reporter_is_read_only", before == after,
           f"{before} vs {after}")


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_DETECTION", suite_B_detection),
        ("C_NORMALIZE", suite_C_normalize),
        ("D_PLANNER", suite_D_planner),
        ("E_EVAL", suite_E_eval),
        ("F_COVERAGE", suite_F_coverage),
        ("G_TEMPLATES", suite_G_templates),
        ("H_ISOLATION", suite_H_isolation),
        ("I_BOUNDS", suite_I_bounds),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 17 Source Adapters / Retrieval Eval ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
