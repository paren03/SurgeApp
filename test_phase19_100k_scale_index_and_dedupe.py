"""Phase 19 - 100K Scale, Indexing, Deduplication, and Performance Hardening.

Synthetic-fixtures-only validation. No real corpora ingested. Production EN/RU
lexicons inspected read-only; any writes are routed to temp DBs.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
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

import dual_scale_import_planner as sip
import dual_retrieval_index_builder as idx
import dual_dedupe_collision_reporter as dedup
import dual_import_performance_benchmark as bench
import phase19_scale_runner as p19


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase19_"))


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    pre = sip.verify_phase18_preflight()
    _check(suite, "preflight_ok", pre["ok"],
           "missing=" + ",".join(pre["missing_files"]))
    for f in sip.PHASE19_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Folder setup --------------------

def suite_B_folders() -> None:
    suite = "B_FOLDERS"
    res = p19.setup_phase19_folders()
    _check(suite, "setup_ok", res.get("ok") is True, str(res))
    for d in (p19.PHASE19_SCALE_DIR, p19.PHASE19_BENCH_DIR,
              p19.PHASE19_DEDUPE_DIR, p19.PHASE19_INDEX_DIR,
              p19.PHASE19_EVAL_DIR, p19.PHASE19_COVERAGE_DIR,
              p19.PHASE19_SYNTH_DIR, p19.PHASE19_REPORTS_DIR):
        _check(suite, f"folder_{d.name}_exists", d.exists(), str(d))


# -------------------- C: Scale planner --------------------

def suite_C_scale_planner() -> None:
    suite = "C_SCALE_PLANNER"
    cands = sip.discover_scale_candidates(language="en", limit=10)
    _check(suite, "discovery_returns_list",
           isinstance(cands, list) and len(cands) <= 10, str(len(cands)))

    # estimate_candidate_capacity over a synthetic file
    td = _td()
    syn = td / "syn.jsonl"
    with syn.open("w", encoding="utf-8") as fh:
        for i in range(500):
            fh.write(json.dumps({"word": f"w_{i}", "language": "en"}) + "\n")
    cap = sip.estimate_candidate_capacity({"path": str(syn)})
    _check(suite, "capacity_estimate_ok",
           cap.get("ok") is True
           and cap["row_estimate"] >= 500, str(cap))
    _check(suite, "capacity_clamp_to_25k",
           cap["capacity_within_phase19_cap"] <= sip.HARD_PER_SOURCE_CAP,
           str(cap))

    # choose_scale_batches clamps
    sz = sip.choose_scale_batches(500_000, 50_000, 9_000)
    _check(suite, "batches_clamp_target",
           sz["total_target"] == sip.HARD_TARGET_PER_LANG, str(sz))
    _check(suite, "batches_clamp_per_source",
           sz["per_source_cap"] == sip.HARD_PER_SOURCE_CAP, str(sz))
    _check(suite, "batches_clamp_batch_size",
           sz["batch_size"] == sip.HARD_BATCH_SIZE, str(sz))

    # build_scale_plan basic
    srcs = [{"path": str(syn), "language": "en", "row_estimate": 500}]
    bp = sip.build_scale_plan(srcs, language="en",
                              target_total=60_000,
                              per_source_cap=20_000, batch_size=2_000)
    _check(suite, "scale_plan_built", bp.get("ok") is True, str(bp)[:200])
    plan = bp["plan"]
    for required_flag in ("quality_gate_required", "dry_run_required",
                          "checkpoint_required", "manifest_required",
                          "rollback_required"):
        _check(suite, f"plan_requires_{required_flag}",
               plan[required_flag] is True, "")
    _check(suite, "plan_allow_full_source_false",
           plan["allow_full_source"] is False, "")
    _check(suite, "plan_target_within_cap",
           plan["target_total"] <= sip.HARD_TARGET_PER_LANG, "")
    _check(suite, "plan_per_source_within_cap",
           plan["per_source_cap"] <= sip.HARD_PER_SOURCE_CAP, "")
    _check(suite, "plan_batch_within_cap",
           plan["batch_size"] <= sip.HARD_BATCH_SIZE, "")

    # build_scale_plan refuses allow_full_source
    refused = sip.build_scale_plan(srcs, language="en",
                                   allow_full_source=True)
    _check(suite, "rejects_allow_full_source",
           refused.get("ok") is False, str(refused))

    # enforce_phase19_caps clamps over-large plan
    over = dict(plan)
    over["target_total"] = 999_999
    over["per_source_cap"] = 999_999
    over["batch_size"] = 999_999
    enf = sip.enforce_phase19_caps(over)
    _check(suite, "enforce_clamps_target",
           enf["plan"]["target_total"] == sip.HARD_TARGET_PER_LANG, "")
    _check(suite, "enforce_clamps_per_source",
           enf["plan"]["per_source_cap"] == sip.HARD_PER_SOURCE_CAP, "")
    _check(suite, "enforce_clamps_batch_size",
           enf["plan"]["batch_size"] == sip.HARD_BATCH_SIZE, "")
    over2 = dict(plan)
    over2["allow_full_source"] = True
    enf2 = sip.enforce_phase19_caps(over2)
    _check(suite, "enforce_refuses_allow_full_source",
           enf2.get("ok") is False, str(enf2))

    # write/read roundtrip
    out = td / "p.json"
    sip.write_scale_plan(plan, out)
    rt = sip.read_scale_plan(out)
    _check(suite, "plan_write_read_roundtrip",
           rt is not None and rt["scale_plan_id"] == plan["scale_plan_id"],
           str(rt)[:100] if rt else "None")

    sum_ = sip.summarize_scale_plan(plan)
    _check(suite, "summary_includes_caps",
           sum_["phase19_caps_enforced"]["target_total_max"]
           == sip.HARD_TARGET_PER_LANG, str(sum_))


# -------------------- D: Retrieval index builder --------------------

def suite_D_index_builder() -> None:
    suite = "D_INDEX_BUILDER"
    fts = idx.detect_sqlite_fts5_support()
    _check(suite, "fts_detection_returns_dict",
           isinstance(fts, dict) and "fts5_available" in fts, str(fts))

    td = _td()
    en_db = td / "en.sqlite3"
    ru_db = td / "ru.sqlite3"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    enlex.init_db(en_db)
    rulex.init_db(ru_db)
    for i in range(120):
        enlex.add_word(word=f"ix_en_{i}", source="ix_test",
                       language="en", db_path=en_db,
                       coverage_categories=["core_vocabulary"],
                       register_tags=["standard"])
        rulex.add_word(word=f"ix_ru_{i}", lemma=f"ix_ru_{i}",
                       source="ix_test", db_path=ru_db,
                       coverage_categories=["core_vocabulary"],
                       register_tags=["standard"])

    en_ix = idx.ensure_english_indexes(en_db)
    _check(suite, "ensure_en_indexes",
           en_ix.get("ok") is True
           and any("words" in x for x in en_ix["all_indexes"]), str(en_ix))
    ru_ix = idx.ensure_russian_indexes(ru_db)
    _check(suite, "ensure_ru_indexes",
           ru_ix.get("ok") is True, str(ru_ix))

    en_fts = idx.build_english_fts_index(rebuild=True, limit=80,
                                         db_path=en_db)
    _check(suite, "en_fts_or_fallback_built",
           en_fts.get("ok") is True and en_fts["indexed_rows"] <= 80,
           str(en_fts))
    ru_fts = idx.build_russian_fts_index(rebuild=True, limit=80,
                                         db_path=ru_db)
    _check(suite, "ru_fts_or_fallback_built",
           ru_fts.get("ok") is True and ru_fts["indexed_rows"] <= 80,
           str(ru_fts))

    # Query must require + honor limit
    rows = idx.query_english_index("ix_en", limit=10, db_path=en_db)
    _check(suite, "query_en_bounded_at_10", len(rows) <= 10, str(len(rows)))
    rows_ru = idx.query_russian_index("ix_ru", limit=10, db_path=ru_db)
    _check(suite, "query_ru_bounded_at_10", len(rows_ru) <= 10, str(len(rows_ru)))

    # Hard upper bound clamp (caller asks 9999, we cap at 200)
    rows_max = idx.query_english_index("ix_en", limit=9999, db_path=en_db)
    _check(suite, "query_hard_limit_clamped", len(rows_max) <= 200,
           str(len(rows_max)))

    # Category / register / safety helpers
    cat = idx.query_by_category("en", "core_vocabulary", limit=20,
                                db_path=en_db)
    _check(suite, "query_by_category_bounded",
           isinstance(cat, list) and len(cat) <= 20, str(len(cat)))
    reg_ = idx.query_by_register("en", "standard", limit=20, db_path=en_db)
    _check(suite, "query_by_register_bounded",
           isinstance(reg_, list) and len(reg_) <= 20, str(len(reg_)))
    saf = idx.query_by_safety("en", "recognition_only", limit=20,
                              db_path=en_db)
    _check(suite, "query_by_safety_bounded",
           isinstance(saf, list) and len(saf) <= 20, str(len(saf)))

    health = idx.index_health_report(en_db_path=en_db, ru_db_path=ru_db)
    _check(suite, "health_has_en_ru_blocks",
           "english" in health and "russian" in health, "")
    out = td / "ixrep.json"
    idx.write_index_report(health, out)
    _check(suite, "health_report_written", out.exists(), "")


# -------------------- E: Dedupe reporter --------------------

def suite_E_dedupe() -> None:
    suite = "E_DEDUPE"
    td = _td()
    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    # Seed unique rows
    for i in range(30):
        enlex.add_word(word=f"dd_{i}", source="seed",
                       pack_id=f"pack_a_{i % 3}", language="en",
                       db_path=en_db,
                       coverage_categories=["core_vocabulary"],
                       register_tags=["standard"])
    # Confirm uniqueness in stored DB
    conn = sqlite3.connect(str(en_db))
    try:
        n_rows = conn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
    finally:
        conn.close()

    dups = dedup.find_exact_duplicates("en", limit=100, db_path=en_db)
    _check(suite, "exact_dup_returns_list",
           isinstance(dups, list), str(type(dups)))
    coll = dedup.find_pack_collisions("en", limit=100, db_path=en_db)
    _check(suite, "pack_coll_returns_list",
           isinstance(coll, list), str(type(coll)))
    cross = dedup.find_cross_category_reuse("en", limit=100, db_path=en_db)
    _check(suite, "cross_returns_list",
           isinstance(cross, list), "")
    miss = dedup.find_missing_pack_ids("en", limit=100, db_path=en_db)
    _check(suite, "missing_pack_ids_bounded",
           isinstance(miss, list) and len(miss) <= 100, str(len(miss)))
    miss_s = dedup.find_missing_safety_tags("en", limit=100, db_path=en_db)
    _check(suite, "missing_safety_bounded",
           isinstance(miss_s, list) and len(miss_s) <= 100, str(len(miss_s)))
    miss_r = dedup.find_missing_register_tags("en", limit=100, db_path=en_db)
    _check(suite, "missing_register_bounded",
           isinstance(miss_r, list) and len(miss_r) <= 100, str(len(miss_r)))

    sev = dedup.score_duplicate_severity({"key": "x", "count": 6})
    _check(suite, "severity_high_for_6",
           sev["severity_label"] == "high"
           and sev["severity_score"] == 0.8, str(sev))
    sev0 = dedup.score_duplicate_severity({"key": "x", "count": 1})
    _check(suite, "severity_none_for_1",
           sev0["severity_label"] == "none", str(sev0))

    # dry_run mark_duplicate_candidates does NOT mutate
    conn = sqlite3.connect(str(en_db))
    try:
        before_n = conn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
    finally:
        conn.close()
    r = dedup.mark_duplicate_candidates("en",
                                        [{"key": "dd_0", "count": 2},
                                         {"key": "dd_1", "count": 3}],
                                        dry_run=True, db_path=en_db)
    _check(suite, "dry_run_no_mutation",
           r["dry_run"] is True and "no_mutation_performed" in r["note"],
           str(r))
    conn = sqlite3.connect(str(en_db))
    try:
        after_n = conn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
    finally:
        conn.close()
    _check(suite, "row_count_unchanged_after_dry_run",
           before_n == after_n, f"{before_n} -> {after_n}")

    # Real annotate still NEVER deletes; only annotates
    r2 = dedup.mark_duplicate_candidates("en",
                                         [{"key": "dd_0", "count": 2}],
                                         dry_run=False, db_path=en_db)
    _check(suite, "annotate_no_delete",
           r2.get("ok") and "non_destructive_annotation_only" in r2.get("note", ""),
           str(r2))
    conn = sqlite3.connect(str(en_db))
    try:
        after2 = conn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
    finally:
        conn.close()
    _check(suite, "row_count_unchanged_after_annotate",
           before_n == after2, f"{before_n} -> {after2}")

    out = td / "dedupe.json"
    dedup.write_dedupe_report({"summary": "synthetic", "en": dups}, out)
    _check(suite, "dedupe_report_written", out.exists(), "")


# -------------------- F: Performance benchmark --------------------

def suite_F_benchmark() -> None:
    suite = "F_BENCHMARK"
    td = _td()
    en_path = td / "bm_en.jsonl"
    ru_path = td / "bm_ru.jsonl"
    en = bench.create_synthetic_scale_fixture("en", en_path, rows=3000)
    ru = bench.create_synthetic_scale_fixture("ru", ru_path, rows=3000)
    _check(suite, "en_fixture_ok", en.get("ok") is True
           and en["rows_written"] == 3000, str(en)[:120])
    _check(suite, "ru_fixture_ok", ru.get("ok") is True
           and ru["rows_written"] == 3000, str(ru)[:120])

    sr = bench.benchmark_streaming_read(en_path, max_rows=1500)
    _check(suite, "streaming_read_honors_max_rows",
           sr.get("rows_read") == 1500, str(sr))

    di = bench.benchmark_dry_run_import(en_path, language="en",
                                        max_entries=500)
    _check(suite, "dry_run_import_ok",
           di.get("ok") is True and di["accepted"] <= 500, str(di)[:200])

    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    for i in range(50):
        enlex.add_word(word=f"bm_w_{i}", source="bm", language="en",
                       db_path=en_db,
                       coverage_categories=["core_vocabulary"],
                       register_tags=["standard"])
    ib = bench.benchmark_index_build("en", limit=50, en_db_path=en_db)
    _check(suite, "index_build_ok", ib.get("ok") is True, str(ib)[:200])
    iq = bench.benchmark_index_query("en", ["bm_w"], limit=10, en_db_path=en_db)
    _check(suite, "index_query_bounded",
           iq.get("ok") is True
           and all(p["bounds_ok"] for p in iq["per_query"]), str(iq))

    re_ = bench.benchmark_retrieval_eval(limit=10)
    _check(suite, "retrieval_eval_runs",
           re_.get("ok") is True and re_["en_bounds_ok"], str(re_)[:200])

    out = td / "bench.json"
    bench.write_benchmark_report({"sr": sr, "di": di, "ib": ib, "iq": iq,
                                  "re": re_}, out)
    _check(suite, "benchmark_report_written", out.exists(), "")


# -------------------- G: Phase 19 runner --------------------

def suite_G_runner() -> None:
    suite = "G_RUNNER"
    pre = p19.verify_phase19_preflight()
    _check(suite, "runner_preflight_ok", pre["ok"], str(pre)[:200])

    folds = p19.setup_phase19_folders()
    _check(suite, "runner_setup_ok", folds["ok"], str(folds))

    # Synthetic fixtures - keep small for harness speed
    fx = p19.create_phase19_synthetic_fixtures(rows_per_language=1000)
    _check(suite, "runner_fixtures_ok", fx["ok"], str(fx)[:200])
    fixtures = {"en": fx["en"], "ru": fx["ru"]}

    qg_res = p19.run_phase19_quality_gates(fixtures)
    _check(suite, "runner_quality_gates_en_open",
           qg_res["en"]["gate_open"] is True, str(qg_res["en"])[:200])
    _check(suite, "runner_quality_gates_ru_open",
           qg_res["ru"]["gate_open"] is True, str(qg_res["ru"])[:200])

    plans = p19.build_phase19_scale_plans(fixtures)
    _check(suite, "runner_plans_built",
           plans["en"]["ok"] and plans["ru"]["ok"],
           f"en={plans['en'].get('error')}; ru={plans['ru'].get('error')}")

    td = _td()
    drs = p19.run_phase19_dry_runs(plans, max_per_lang=500,
                                   checkpoint_db_path=td / "ck.sqlite3")
    _check(suite, "runner_dry_runs_ok",
           drs["en"]["ok"] and drs["ru"]["ok"], str(drs)[:200])

    # Index build over production read-only is harmless
    ib = p19.run_phase19_index_builds(limit=100)
    _check(suite, "runner_indexes_built",
           "en_normal" in ib and "ru_normal" in ib, "")

    dd = p19.run_phase19_dedupe_reports(limit=100)
    _check(suite, "runner_dedupe_ok",
           "en" in dd and "ru" in dd
           and Path(dd["report_path"]).exists(), "")

    rev = p19.run_phase19_retrieval_evals(limit=10)
    _check(suite, "runner_retrieval_evals_ok",
           rev["ok"] and rev["en"]["bounds_ok"] and rev["ru"]["bounds_ok"],
           "")

    cov = p19.run_phase19_coverage_reports()
    _check(suite, "runner_coverage_ok",
           cov.get("ok") and Path(cov["report_path"]).exists(), "")

    # Real scale is OFF by default
    off = p19.run_phase19_real_scale_imports(plans=plans)
    _check(suite, "runner_real_scale_off_by_default",
           off["allow_real_import"] is False
           and off["reason"] == "real_scale_import_disabled_by_default",
           str(off))

    # Exceeding 100k is refused
    over = p19.run_phase19_real_scale_imports(
        plans=plans, allow_real_import=True,
        max_total_per_language=200_000)
    _check(suite, "runner_refuses_over_100k_request",
           over.get("ok") is False
           and "max_total_per_language" in (over.get("error") or ""),
           str(over))


# -------------------- H: Safety/policy --------------------

def suite_H_safety() -> None:
    suite = "H_SAFETY"
    td = _td()
    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    # Plant a recognition_only and a do_not_use_unprompted row + a vulgar row.
    enlex.add_word(word="reco_only_word", source="t", language="en",
                   db_path=en_db,
                   coverage_categories=["philosophy_abstract"],
                   register_tags=["academic", "recognition_only"],
                   safety_tags=["recognition_only"])
    enlex.add_word(word="dont_use_word", source="t", language="en",
                   db_path=en_db,
                   coverage_categories=["philosophy_abstract"],
                   register_tags=["academic", "do_not_use_unprompted"],
                   safety_tags=["do_not_use_unprompted"])
    enlex.add_word(word="vulgar_word", source="t", language="en",
                   db_path=en_db,
                   coverage_categories=["slang_street_talk"],
                   register_tags=["vulgar"],
                   safety_tags=["vulgar"])
    enlex.add_word(word="benign_word", source="t", language="en",
                   db_path=en_db,
                   coverage_categories=["core_vocabulary"],
                   register_tags=["standard"], safety_tags=[])

    # Indexed retrieval still returns the rows (it's a recognition step)
    rows = idx.query_english_index("word", limit=50, db_path=en_db)
    have = {r.get("word"): r for r in rows}
    _check(suite, "indexed_query_returns_all_four",
           len(have) == 4, sorted(have.keys()))

    # Safety filter from the Phase 17 evaluator MUST block the dangerous ones.
    import dual_retrieval_quality_eval as rqe
    decision = rqe.check_safety_policy_on_results(
        rows, mode="teacher", is_user_prompted=False)
    _check(suite, "do_not_use_unprompted_blocked",
           "dont_use_word" in decision["do_not_use_violations"],
           str(decision))
    _check(suite, "vulgar_flagged_in_teacher_mode",
           "vulgar_word" in decision["vulgar_in_teacher_mode"],
           str(decision))
    _check(suite, "recognition_only_marked_as_recognized_not_suggested",
           "reco_only_word" in decision["suggestion_only_recognized"],
           str(decision))


# -------------------- I: Isolation --------------------

PHASE19_FILES = [
    "dual_scale_import_planner.py",
    "dual_retrieval_index_builder.py",
    "dual_dedupe_collision_reporter.py",
    "dual_import_performance_benchmark.py",
    "phase19_scale_runner.py",
]


def suite_I_isolation() -> None:
    suite = "I_ISOLATION"
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
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
    for fname in PHASE19_FILES:
        p = Path(fname)
        _check(suite, f"{fname}_exists", p.exists(), "")
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8")
        bad: list[str] = []
        for line in text.splitlines():
            for forb in FORBIDDEN:
                if re.search(rf"^(import|from)\s+\S*{re.escape(forb)}", line):
                    bad.append(line.strip())
        _check(suite, f"{fname}_no_forbidden_imports",
               not bad, "; ".join(bad[:3]))
        net = [m.group(0) for pat in NETWORK_USAGE_PATTERNS
               for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_network",
               not net, "; ".join(net[:3]))
        dh = [m.group(0) for pat in DAEMON_USAGE_PATTERNS
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_daemon",
               not dh, "; ".join(dh[:3]))


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_FOLDERS", suite_B_folders),
        ("C_SCALE_PLANNER", suite_C_scale_planner),
        ("D_INDEX_BUILDER", suite_D_index_builder),
        ("E_DEDUPE", suite_E_dedupe),
        ("F_BENCHMARK", suite_F_benchmark),
        ("G_RUNNER", suite_G_runner),
        ("H_SAFETY", suite_H_safety),
        ("I_ISOLATION", suite_I_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 19 100K Scale / Index / Dedupe ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
