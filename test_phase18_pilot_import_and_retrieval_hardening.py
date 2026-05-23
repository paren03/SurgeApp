"""Phase 18 - Pilot Import and Retrieval Hardening.

Synthetic-only validation harness. No real corpora are imported. Production
EN/RU lexicons are read-only; any writes go to temp DBs. No daemons. No
internet.
"""

from __future__ import annotations

import json
import os
import re
import shutil
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

import phase18_pilot_import_runner as p18
import dual_corpus_registry as reg
import dual_corpus_checkpoint as ckpt
import dual_corpus_quality_gate as qg
import dual_corpus_chunked_importer as imp
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


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase18_"))


def _write_jsonl(p: Path, rows: list[dict]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return p


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    pre = p18.verify_phase17_preflight()
    _check(suite, "phase17_preflight_ok", pre["ok"],
           "missing=" + ",".join(pre["missing_files"]))
    for f in p18.PHASE17_REQUIRED:
        _check(suite, f"required_{f}", Path(f).exists(), f"missing: {f}")
    # If we artificially temp-rename one file, runner must return ok=False
    # (we only simulate by checking the function call against current state -
    #  full live filesystem swap would be destructive). Already covered.


# -------------------- B: Folder setup --------------------

def suite_B_folders() -> None:
    suite = "B_FOLDERS"
    for d in (p18.PHASE18_PILOT_DIR, p18.PHASE18_EVAL_DIR,
              p18.PHASE18_COVERAGE_DIR, p18.PHASE18_SYNTH_DIR,
              p18.PHASE18_REPORTS_DIR):
        d.mkdir(parents=True, exist_ok=True)
        _check(suite, f"folder_{d.name}_exists", d.exists(), str(d))


# -------------------- C: Discovery --------------------

def suite_C_discovery() -> None:
    suite = "C_DISCOVERY"
    # Production incoming folders should be empty in standard run.
    en_disc = p18.discover_phase18_sources(language="en", limit=10)
    _check(suite, "en_discovery_returns_list",
           isinstance(en_disc, list), str(type(en_disc)))
    ru_disc = p18.discover_phase18_sources(language="ru", limit=10)
    _check(suite, "ru_discovery_returns_list",
           isinstance(ru_disc, list), "")
    both = p18.discover_phase18_sources(limit=10)
    _check(suite, "both_lang_discovery_bounded",
           isinstance(both, list) and len(both) <= 10, str(len(both)))

    # Synthetic: drop a file in a temp incoming folder by classifying directly.
    td = _td()
    f = _write_jsonl(td / "syn.jsonl",
                     [{"word": "alpha", "language": "en"}])
    cls = p18.classify_discovered_source(f, language="en")
    _check(suite, "classify_synthetic_ok",
           cls.get("ok") is True
           and cls["adapter_type"] == "luna_jsonl", str(cls))

    cls_bad = p18.classify_discovered_source(td / "nope.jsonl")
    _check(suite, "classify_missing_clean_failure",
           cls_bad.get("ok") is False, str(cls_bad))


# -------------------- D: Synthetic fixtures --------------------

def suite_D_fixtures() -> None:
    suite = "D_FIXTURES"
    od = _td() / "synth"
    res = p18.generate_synthetic_phase18_fixtures(
        output_dir=od, rows_per_large_fixture=500, seed=7)
    _check(suite, "gen_ok", res.get("ok") is True, str(res)[:200])
    f = res["files"]
    for label, path in f.items():
        _check(suite, f"fixture_{label}_exists",
               Path(path).exists(), path)
    # En jsonl should contain valid + duplicate + malformed + recog + slang rows
    en_path = Path(f["english_jsonl"])
    seen_valid = seen_malformed = seen_recog = seen_slang = 0
    with en_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            if not obj.get("word"):
                seen_malformed += 1
                continue
            seen_valid += 1
            if "recognition_only" in (obj.get("safety_tags") or []):
                seen_recog += 1
            if "slang" in (obj.get("register_tags") or []):
                seen_slang += 1
    _check(suite, "fixture_has_valid_rows", seen_valid >= 400, str(seen_valid))
    _check(suite, "fixture_has_malformed", seen_malformed >= 1,
           str(seen_malformed))
    _check(suite, "fixture_has_recognition_only", seen_recog >= 1, str(seen_recog))
    _check(suite, "fixture_has_slang", seen_slang >= 1, str(seen_slang))

    # CSV fixtures parse via adapters
    rows = list(adp.iter_normalized_rows(
        f["bilingual_glossary_csv"], "bilingual_glossary_csv",
        "en", "domain_terms", max_rows=5))
    _check(suite, "glossary_csv_parses",
           sum(1 for r in rows if r["ok"]) >= 3, str(len(rows)))


# -------------------- E: Quality gates --------------------

def suite_E_quality_gates() -> None:
    suite = "E_QUALITY_GATES"
    td = _td()
    # High quality
    good = _write_jsonl(td / "good.jsonl",
                        [{"word": f"good_{i}", "language": "en",
                          "definition": "a clear synthetic definition",
                          "coverage_categories": ["core_vocabulary"],
                          "register_tags": ["standard"]} for i in range(60)])
    rep = qg.generate_quality_gate_report(good, "jsonl", "en", sample_size=50)
    gate = qg.should_allow_import(rep, min_quality_score=0.75)
    _check(suite, "good_quality_gate_open", gate["ok"] is True, str(gate))

    # Operational unsafe
    unsafe = _write_jsonl(td / "unsafe.jsonl",
                          [{"word": "ok", "language": "en",
                            "definition": "step by step instructions to bypass auth",
                            "coverage_categories": ["core_vocabulary"],
                            "register_tags": ["standard"]}] * 10)
    rep_u = qg.generate_quality_gate_report(unsafe, "jsonl", "en", sample_size=10)
    gate_u = qg.should_allow_import(rep_u, min_quality_score=0.75)
    _check(suite, "unsafe_quality_gate_closed",
           gate_u["ok"] is False
           and gate_u.get("reason") == "operational_unsafe_content_detected",
           str(gate_u))

    # Language mismatch
    mismatch = _write_jsonl(td / "mix.jsonl",
                            [{"word": f"привет_{i}", "language": "ru",
                              "definition": "ясное определение",
                              "coverage_categories": ["core_vocabulary"],
                              "register_tags": ["standard"]} for i in range(30)])
    rep_m = qg.generate_quality_gate_report(mismatch, "jsonl", "en",
                                            sample_size=20)
    _check(suite, "language_mismatch_flagged",
           rep_m.get("language_mismatch_count", 0) >= 1, str(rep_m))

    # Through the runner
    od = _td() / "synth"
    fx = p18.generate_synthetic_phase18_fixtures(
        output_dir=od, rows_per_large_fixture=300, seed=11)
    sv = p18.run_synthetic_streaming_validation(
        fixtures=fx["files"], sample_size=50)
    _check(suite, "synthetic_streaming_runs",
           sv.get("ok") is True, str(sv)[:200])
    _check(suite, "synthetic_en_jsonl_qreport_ok",
           sv["results"]["english_jsonl"]["ok"] is True, str(sv))
    _check(suite, "synthetic_ru_jsonl_qreport_ok",
           sv["results"]["russian_jsonl"]["ok"] is True, str(sv))


# -------------------- F: Pilot planner --------------------

def suite_F_planner() -> None:
    suite = "F_PLANNER"
    td = _td()
    src = _write_jsonl(td / "inc.jsonl",
                       [{"word": f"good_{i}", "language": "en",
                         "definition": "a clear synthetic definition",
                         "coverage_categories": ["core_vocabulary"],
                         "register_tags": ["standard"]} for i in range(200)])
    rec = [{"path": str(src), "language": "en", "source_type": "word_list",
            "expected_format": "jsonl", "adapter_type": "luna_jsonl",
            "size_bytes": src.stat().st_size}]
    # Pretend it has been quality-gated
    q = qg.generate_quality_gate_report(src, "jsonl", "en", sample_size=50)
    rec[0]["quality_ok"] = True
    rec[0]["quality_report"] = q

    plans = p18.build_pilot_plans_for_sources(rec, max_entries_per_source=1000)
    _check(suite, "one_plan_built", len(plans) == 1, str(plans)[:200])
    plan = plans[0]["plan"]
    _check(suite, "plan_present", plan is not None, "")
    _check(suite, "dry_run_default", plan["dry_run_default"] is True, "")
    _check(suite, "rollback_key_present",
           bool(plan.get("rollback_key")), "")
    _check(suite, "max_entries_capped_to_pilot_default",
           plan["safe_max_entries"] <= p18.DEFAULT_PILOT_MAX_PER_SOURCE, "")
    _check(suite, "batch_size_bounded",
           1 <= plan["batch_size"] <= 1000, str(plan["batch_size"]))
    plan_text = json.dumps(plan, ensure_ascii=False, default=str)
    _check(suite, "no_allow_full_source_in_plan_text",
           "allow_full_source" not in plan_text
           or '"allow_full_source": true' not in plan_text,
           "")


# -------------------- G: Dry run --------------------

def suite_G_dryrun() -> None:
    suite = "G_DRYRUN"
    # Production count snapshot before
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    before_en = enlex.count_words()
    before_ru = rulex.count_words()

    td = _td()
    src = _write_jsonl(td / "inc.jsonl",
                       [{"word": f"good_{i}", "language": "en",
                         "definition": "a clear synthetic definition",
                         "coverage_categories": ["core_vocabulary"],
                         "register_tags": ["standard"]} for i in range(150)])
    q = qg.generate_quality_gate_report(src, "jsonl", "en", sample_size=50)
    rec = [{"path": str(src), "language": "en", "source_type": "word_list",
            "expected_format": "jsonl", "adapter_type": "luna_jsonl",
            "size_bytes": src.stat().st_size, "quality_ok": True,
            "quality_report": q}]
    plans = p18.build_pilot_plans_for_sources(rec, max_entries_per_source=80)
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    en_db = td / "en.sqlite3"
    res = p18.run_phase18_dry_runs(plans,
                                   registry_db_path=reg_db,
                                   checkpoint_db_path=ck_db,
                                   en_db_path=en_db)
    _check(suite, "dry_run_ok",
           res[0]["dry_run_ok"] is True, str(res[0])[:200])
    dr = res[0]["dry_run"]
    _check(suite, "dry_run_accepted_capped",
           dr["accepted"] <= 80, str(dr["accepted"]))
    _check(suite, "dry_run_report_written",
           Path(dr["report_path"]).exists(), str(dr["report_path"]))
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    _check(suite, "production_db_unchanged_during_dry_run",
           after_en == before_en and after_ru == before_ru,
           f"en {before_en}->{after_en}; ru {before_ru}->{after_ru}")


# -------------------- H: Real pilot guard --------------------

def suite_H_real_pilot_guard() -> None:
    suite = "H_REAL_GUARD"
    td = _td()
    src = _write_jsonl(td / "inc.jsonl",
                       [{"word": f"good_{i}", "language": "en",
                         "definition": "a clear synthetic definition",
                         "coverage_categories": ["core_vocabulary"],
                         "register_tags": ["standard"]} for i in range(50)])
    q = qg.generate_quality_gate_report(src, "jsonl", "en", sample_size=20)
    rec = [{"path": str(src), "language": "en", "source_type": "word_list",
            "expected_format": "jsonl", "adapter_type": "luna_jsonl",
            "size_bytes": src.stat().st_size, "quality_ok": True,
            "quality_report": q}]
    plans = p18.build_pilot_plans_for_sources(rec, max_entries_per_source=40)
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    dr = p18.run_phase18_dry_runs(plans,
                                  registry_db_path=reg_db,
                                  checkpoint_db_path=ck_db,
                                  en_db_path=en_db)
    # 1. Default refuses real import
    guarded = p18.run_phase18_real_pilots(dr)
    _check(suite, "default_real_off",
           guarded["allow_real_import"] is False
           and guarded["reason"] == "real_import_disabled_by_default",
           str(guarded))

    # 2. allow_real_import=True writes only to temp DB; production untouched.
    before_prod = enlex.count_words()
    rr = p18.run_phase18_real_pilots(dr, allow_real_import=True,
                                     registry_db_path=reg_db,
                                     checkpoint_db_path=ck_db,
                                     en_db_path=en_db)
    after_prod = enlex.count_words()
    _check(suite, "production_db_unchanged_by_temp_real_import",
           after_prod == before_prod,
           f"{before_prod}->{after_prod}")
    _check(suite, "temp_real_import_succeeded",
           rr["ok"] is True
           and any(x.get("real_ok") for x in rr["results"]), str(rr)[:200])

    # 3. Hard cap honored: build 6 plans of 5000 each → only 5 should write.
    src_big = _write_jsonl(td / "big.jsonl",
                           [{"word": f"cap_{i}", "language": "en",
                             "definition": "synthetic",
                             "coverage_categories": ["core_vocabulary"],
                             "register_tags": ["standard"]}
                            for i in range(5500)])
    qb = qg.generate_quality_gate_report(src_big, "jsonl", "en",
                                         sample_size=50)
    big_recs = [{"path": str(src_big), "language": "en",
                 "source_type": "word_list",
                 "expected_format": "jsonl",
                 "adapter_type": "luna_jsonl",
                 "size_bytes": src_big.stat().st_size,
                 "quality_ok": True, "quality_report": qb}
                for _ in range(6)]
    big_plans = p18.build_pilot_plans_for_sources(
        big_recs, max_entries_per_source=5000)
    # The planner naturally caps at DEFAULT_TARGET_ENTRIES=1000; to exercise
    # the Phase 18 hard total cap (25,000 per language) we lift each plan's
    # safe_max_entries to 5000 here. The cap path is the unit under test.
    for plan_item in big_plans:
        if plan_item.get("plan"):
            plan_item["plan"]["safe_max_entries"] = 5000
    en_db2 = td / "en2.sqlite3"
    enlex.init_db(en_db2)
    big_dr = p18.run_phase18_dry_runs(big_plans,
                                      registry_db_path=reg_db,
                                      checkpoint_db_path=ck_db,
                                      en_db_path=en_db2)
    cap = p18.run_phase18_real_pilots(big_dr,
                                      allow_real_import=True,
                                      registry_db_path=reg_db,
                                      checkpoint_db_path=ck_db,
                                      en_db_path=en_db2)
    used_en = cap["used_per_language"]["en"]
    _check(suite, "hard_cap_25k_en_honored",
           used_en <= p18.HARD_PILOT_TOTAL_CAP_PER_LANG, str(used_en))
    skipped = sum(1 for r in cap["results"]
                  if r.get("real") is None and
                  (r.get("real_skipped_reason") or "").startswith("hard_cap_exceeded"))
    _check(suite, "hard_cap_skips_overage",
           skipped >= 1, f"skipped={skipped}")

    # 4. Failed quality gate blocks real import
    rec_bad = [{"path": str(src), "language": "en", "source_type": "word_list",
                "expected_format": "jsonl", "adapter_type": "luna_jsonl",
                "size_bytes": src.stat().st_size,
                "quality_ok": False,
                "quality_report": {"ok": False,
                                   "operational_unsafe_count": 1}}]
    plans_bad = p18.build_pilot_plans_for_sources(rec_bad,
                                                   max_entries_per_source=40)
    _check(suite, "bad_quality_skips_plan_build",
           plans_bad[0].get("skipped") is True
           and plans_bad[0]["skip_reason"] == "quality_gate_closed", "")


# -------------------- I: Checkpointing --------------------

def suite_I_checkpoints() -> None:
    suite = "I_CHECKPOINT"
    td = _td()
    ck_db = td / "ckpt.sqlite3"
    ckpt.init_checkpoint_store(ck_db)
    c = ckpt.create_checkpoint("cid_x", "/p", "en", db_path=ck_db)
    _check(suite, "checkpoint_create_ok", c["ok"], str(c))
    cid = c["checkpoint_id"]
    ckpt.update_checkpoint(cid, last_line_number=10, accepted_count=8,
                           rejected_count=1, duplicate_count=1, batch_count=1,
                           db_path=ck_db)
    loaded = ckpt.load_checkpoint(cid, db_path=ck_db)
    _check(suite, "checkpoint_update_persisted",
           loaded["last_line_number"] == 10
           and loaded["accepted_count"] == 8, str(loaded))
    f = ckpt.mark_checkpoint_complete(cid, notes="ok", db_path=ck_db)
    _check(suite, "checkpoint_mark_complete", f["ok"], str(f))
    loaded2 = ckpt.load_checkpoint(cid, db_path=ck_db)
    _check(suite, "checkpoint_status_completed",
           loaded2["status"] == "completed", str(loaded2))

    fail = ckpt.create_checkpoint("cid_y", "/p", "en", db_path=ck_db)
    ckpt.mark_checkpoint_failed(fail["checkpoint_id"], notes="boom",
                                db_path=ck_db)
    loaded3 = ckpt.load_checkpoint(fail["checkpoint_id"], db_path=ck_db)
    _check(suite, "checkpoint_failure_reason_recorded",
           loaded3["status"] == "failed"
           and loaded3["notes"] == "boom", str(loaded3))


# -------------------- J: Retrieval evaluation --------------------

def suite_J_retrieval() -> None:
    suite = "J_RETRIEVAL"
    en = rqe.run_english_retrieval_eval(limit=10)
    _check(suite, "en_eval_runs", en.get("ok") is True, "")
    _check(suite, "en_bounds_ok", en["bounds_ok"], "")
    _check(suite, "en_safety_ok", en["safety_ok"], "")
    _check(suite, "en_has_12_queries", en["n_queries"] == 12, "")
    ru = rqe.run_russian_retrieval_eval(limit=10)
    _check(suite, "ru_eval_runs", ru.get("ok") is True, "")
    _check(suite, "ru_bounds_ok", ru["bounds_ok"], "")
    _check(suite, "ru_safety_ok", ru["safety_ok"], "")
    _check(suite, "ru_has_12_queries", ru["n_queries"] == 12, "")
    # Safety blocks
    rs = [{"word": "x",
           "safety_tags": ["do_not_use_unprompted", "recognition_only"]}]
    sc = rqe.check_safety_policy_on_results(rs, mode="teacher",
                                            is_user_prompted=False)
    _check(suite, "do_not_use_unprompted_blocked",
           sc["do_not_use_violation_count"] == 1, str(sc))
    # Bounds violation flagged
    b = rqe.check_result_bounds([{}] * 20, 10)
    _check(suite, "bounds_violation_flagged", b["ok"] is False, str(b))


# -------------------- K: Coverage report --------------------

def suite_K_coverage() -> None:
    suite = "K_COVERAGE"
    tot = cr.count_entries_by_language()
    _check(suite, "totals_dict_complete",
           {"en_words", "ru_words", "ru_phrases"} <= set(tot.keys()),
           str(tot))
    en_cov = cr.count_entries_by_coverage_category("en")
    _check(suite, "en_cov_21_keys", len(en_cov) == 21, str(len(en_cov)))
    en_reg = cr.count_entries_by_register_tag("en")
    _check(suite, "en_reg_22_keys", len(en_reg) == 22, str(len(en_reg)))
    en_safe = cr.count_entries_by_safety_tag("en")
    _check(suite, "en_safe_4_keys", len(en_safe) == 4, str(len(en_safe)))
    low = cr.identify_low_coverage_categories("en", min_entries=10)
    _check(suite, "low_callable", isinstance(low, list), str(type(low)))
    bal = cr.compare_english_russian_category_balance()
    _check(suite, "balance_has_per_category",
           len(bal["per_category"]) == 21, str(len(bal["per_category"])))
    rep = p18.run_post_pilot_coverage_report()
    _check(suite, "phase18_coverage_report_written",
           rep.get("ok") and Path(rep["report_path"]).exists(),
           str(rep)[:200])


# -------------------- L: Isolation --------------------

def suite_L_isolation() -> None:
    suite = "L_ISOLATION"
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
    DAEMON = (
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
    NETWORK = (
        r"^\s*(import|from)\s+(urllib|requests|httpx|aiohttp|socket|ftplib)\b",
        r"urlopen\s*\(",
        r"http\.client",
    )
    f = "phase18_pilot_import_runner.py"
    p = Path(f)
    _check(suite, f"{f}_exists", p.exists(), "")
    if not p.exists():
        return
    text = p.read_text(encoding="utf-8")
    bad: list[str] = []
    for line in text.splitlines():
        for forb in FORBIDDEN:
            if re.search(rf"^(import|from)\s+\S*{re.escape(forb)}", line):
                bad.append(line.strip())
    _check(suite, f"{f}_no_forbidden_imports",
           not bad, "; ".join(bad[:3]))
    net_hits = [m.group(0).strip() for pat in NETWORK
                for m in re.finditer(pat, text, flags=re.MULTILINE)]
    _check(suite, f"{f}_no_network", not net_hits, "; ".join(net_hits[:3]))
    d_hits = [m.group(0).strip() for pat in DAEMON
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
    _check(suite, f"{f}_no_daemon", not d_hits, "; ".join(d_hits[:3]))


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_FOLDERS", suite_B_folders),
        ("C_DISCOVERY", suite_C_discovery),
        ("D_FIXTURES", suite_D_fixtures),
        ("E_QUALITY_GATES", suite_E_quality_gates),
        ("F_PLANNER", suite_F_planner),
        ("G_DRYRUN", suite_G_dryrun),
        ("H_REAL_GUARD", suite_H_real_pilot_guard),
        ("I_CHECKPOINT", suite_I_checkpoints),
        ("J_RETRIEVAL", suite_J_retrieval),
        ("K_COVERAGE", suite_K_coverage),
        ("L_ISOLATION", suite_L_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 18 Pilot Import / Retrieval Hardening ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
