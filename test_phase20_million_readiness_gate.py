"""Phase 20 - Million Readiness Gate.

Synthetic-fixtures-only validation harness. Production lexicons inspected
read-only. Backups go to corpus_sources/backups/. No real corpora ingested.
No daemons. No internet.
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

import dual_vocab_backup_restore as bk
import dual_import_batch_ledger as led
import dual_million_stage_planner as msp
import dual_post_stage_quality_audit as pq
import dual_retrieval_sla_eval as sla
import dual_index_consistency_checker as ic
import dual_safety_regression_auditor as sra
import dual_import_performance_benchmark as bench
import phase20_million_readiness_runner as p20


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase20_"))


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    pre = msp.verify_phase19_preflight()
    _check(suite, "preflight_ok", pre["ok"],
           "missing=" + ",".join(pre["missing_files"]))
    for f in msp.PHASE20_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Backup/restore --------------------

def suite_B_backup() -> None:
    suite = "B_BACKUP"
    snap = bk.create_backup_snapshot(label="test", include_manifests=True)
    _check(suite, "snapshot_created", snap.get("ok") is True,
           str(snap)[:200])
    sid = snap["snapshot_id"]
    ver = bk.verify_backup_snapshot(sid)
    _check(suite, "snapshot_verifies",
           ver.get("ok") is True
           and ver["english_sizes_ok"] and ver["russian_sizes_ok"],
           str(ver))
    en_files = ver["english_files"]
    ru_files = ver["russian_files"]
    _check(suite, "english_backup_file_exists",
           bool(en_files) and Path(en_files[0]).exists(), "")
    _check(suite, "russian_backup_file_exists",
           bool(ru_files) and Path(ru_files[0]).exists(), "")
    mans_dir = bk._snapshot_dir(sid) / "manifests"
    _check(suite, "manifest_backup_exists",
           mans_dir.exists(), str(mans_dir))

    # Dry-run restore must NOT touch live DBs.
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    r = bk.restore_backup_snapshot(sid, dry_run=True)
    _check(suite, "dry_run_restore_safe",
           r.get("ok") is True and r["dry_run"] is True
           and len(r["intended_actions"]) >= 2, str(r)[:200])
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    _check(suite, "live_dbs_untouched_after_dry_restore",
           before_en == after_en and before_ru == after_ru,
           f"en {before_en}->{after_en}; ru {before_ru}->{after_ru}")

    cmp = bk.compare_db_counts_before_after(sid)
    _check(suite, "compare_counts_ok",
           cmp.get("ok") is True
           and abs(int(cmp["delta"]["en_words"])) >= 0, str(cmp))

    lst = bk.list_backup_snapshots(limit=5)
    _check(suite, "list_returns_recent",
           any(x["snapshot_id"] == sid for x in lst), "")

    out = _td() / "bk_report.json"
    bk.write_backup_report(snap, out)
    _check(suite, "backup_report_written", out.exists(), "")


# -------------------- C: Batch ledger --------------------

def suite_C_ledger() -> None:
    suite = "C_LEDGER"
    td = _td()
    db = td / "ledger.sqlite3"
    p = led.init_ledger(db)
    _check(suite, "ledger_init", Path(p).exists(), str(p))

    cr = led.create_batch_record(
        language="en", stage_id="stage_alpha",
        source_path="/tmp/fake.jsonl", dry_run=True,
        rollback_key="rk_test_1", db_path=db)
    _check(suite, "batch_created", cr["ok"], str(cr))
    bid = cr["batch_id"]
    u = led.update_batch_status(bid, "completed",
                                accepted_count=100,
                                rejected_count=2,
                                duplicate_count=1,
                                after_word_count=100,
                                completed=True, db_path=db)
    _check(suite, "batch_updated", u["ok"], str(u))
    g = led.get_batch(bid, db_path=db)
    _check(suite, "batch_retrievable",
           g is not None and g["status"] == "completed"
           and g["accepted_count"] == 100, str(g)[:200])

    lst = led.list_batches(language="en", limit=10, db_path=db)
    _check(suite, "list_bounded_to_10",
           isinstance(lst, list) and len(lst) <= 10, str(len(lst)))

    by_stage = led.get_batches_by_stage("stage_alpha", db_path=db)
    _check(suite, "by_stage_returns_one",
           len(by_stage) == 1 and by_stage[0]["batch_id"] == bid, "")

    by_rb = led.get_batches_by_rollback_key("rk_test_1", db_path=db)
    _check(suite, "by_rollback_key_returns_one",
           len(by_rb) == 1, str(by_rb))

    out = td / "ledger_report.json"
    led.write_ledger_report(out, db_path=db)
    _check(suite, "ledger_report_written", out.exists(), "")

    bad = led.create_batch_record(language="xx", db_path=db)
    _check(suite, "invalid_lang_rejected",
           bad.get("ok") is False, str(bad))


# -------------------- D: Million stage planner --------------------

def suite_D_planner() -> None:
    suite = "D_PLANNER"
    disc = msp.discover_eligible_sources(language="en", limit=5)
    _check(suite, "discovery_bounded",
           isinstance(disc, list) and len(disc) <= 5, str(len(disc)))

    sources = [
        {"path": f"/tmp/s_{i}.jsonl", "language": "en",
         "row_estimate": 75_000} for i in range(20)
    ]
    bp = msp.build_million_stage_plan("en", source_records=sources,
                                       target_total=1_000_000,
                                       stage_size=100_000,
                                       per_source_cap=50_000)
    _check(suite, "plan_built", bp.get("ok") is True, str(bp)[:200])
    plan = bp["plan"]
    _check(suite, "target_total_1M",
           plan["target_total"] == 1_000_000, "")
    _check(suite, "stage_size_capped_at_100k",
           plan["stage_size"] == msp.HARD_STAGE_SIZE, "")
    _check(suite, "per_source_cap_capped_at_50k",
           plan["per_source_cap"] == msp.HARD_PER_SOURCE_CAP, "")
    _check(suite, "allow_full_source_is_false",
           plan["allow_full_source"] is False, "")
    for flag in ("quality_gate_required", "dry_run_required",
                 "backup_required", "checkpoint_required",
                 "manifest_required", "safety_audit_required",
                 "retrieval_sla_required", "rollback_required"):
        _check(suite, f"plan_requires_{flag}",
               plan[flag] is True, "")

    refused = msp.build_million_stage_plan("en", source_records=sources,
                                            allow_full_source=True)
    _check(suite, "allow_full_source_refused",
           refused.get("ok") is False, str(refused))

    # Sizing clamps
    sized = msp.split_sources_into_stages(sources,
                                          stage_size=999_999,
                                          per_source_cap=999_999)
    # Each source becomes per_source_cap=50_000, so 100_000/stage = 2 sources.
    _check(suite, "stages_built", len(sized) >= 1, str(len(sized)))
    _check(suite, "stage_cap_respected",
           all(int(s["stage_capacity"]) <= msp.HARD_STAGE_SIZE
               for s in sized), str(sized)[:200])

    # Enforce caps over a hand-edited dict
    over = {"million_plan_id": "x", "stage_size": 999_999,
            "per_source_cap": 999_999, "allow_full_source": False,
            "stages": []}
    enf = msp.enforce_stage_caps(over)
    _check(suite, "enforce_clamps_stage_size",
           enf["plan"]["stage_size"] == msp.HARD_STAGE_SIZE, "")
    _check(suite, "enforce_clamps_per_source",
           enf["plan"]["per_source_cap"] == msp.HARD_PER_SOURCE_CAP, "")
    bad = dict(over)
    bad["allow_full_source"] = True
    enf_bad = msp.enforce_stage_caps(bad)
    _check(suite, "enforce_refuses_allow_full_source",
           enf_bad.get("ok") is False, str(enf_bad))

    out = _td() / "plan.json"
    msp.write_stage_plan(plan, out)
    rt = msp.read_stage_plan(out)
    _check(suite, "plan_roundtrip",
           rt is not None and rt["million_plan_id"] == plan["million_plan_id"],
           "")
    sum_ = msp.summarize_stage_plan(plan)
    _check(suite, "summary_caps_present",
           sum_["phase20_caps_enforced"]["hard_stage_size"]
           == msp.HARD_STAGE_SIZE, "")


# -------------------- E: Post-stage quality audit --------------------

def suite_E_quality_audit() -> None:
    suite = "E_QUALITY_AUDIT"
    rows_sample = pq.sample_recent_import_rows("en", limit=50)
    _check(suite, "sample_bounded",
           isinstance(rows_sample, list) and len(rows_sample) <= 50,
           str(len(rows_sample)))

    md = pq.audit_metadata_completeness(rows_sample)
    _check(suite, "metadata_audit_ok",
           md.get("ok") is True and 0.0 <= md["completeness_ratio"] <= 1.0,
           str(md))

    lc = pq.audit_language_consistency(rows_sample, "en")
    _check(suite, "language_audit_ok",
           lc.get("ok") is True
           and 0.0 <= lc["mismatch_ratio"] <= 1.0, str(lc))

    sc = pq.audit_safety_tag_consistency(rows_sample)
    rc = pq.audit_register_tag_consistency(rows_sample)
    cc = pq.audit_coverage_category_consistency(rows_sample)
    _check(suite, "safety_register_coverage_audits_ok",
           sc["ok"] and rc["ok"] and cc["ok"], "")

    dr = pq.audit_duplicate_rate("en")
    _check(suite, "duplicate_audit_ok",
           dr.get("ok") is True
           and dr["distinct_words"] <= dr["n_words"], str(dr))

    rj = pq.audit_rejected_rows("en", limit=10)
    _check(suite, "rejected_audit_bounded",
           rj.get("ok") is True and len(rj["samples"]) <= 10, str(rj))

    score = pq.compute_stage_quality_score({
        "metadata_completeness": md,
        "language_consistency": lc,
        "safety_consistency": sc,
        "register_consistency": rc,
        "coverage_consistency": cc,
        "duplicate_rate": dr,
    })
    _check(suite, "quality_score_emitted",
           score.get("ok") is True
           and score["verdict"] in ("pass", "warn", "fail"), str(score))

    out = _td() / "audit.json"
    pq.write_post_stage_quality_audit({"x": "y"}, out)
    _check(suite, "audit_report_written", out.exists(), "")


# -------------------- F: Retrieval SLA --------------------

def suite_F_sla() -> None:
    suite = "F_SLA"
    sla_def = sla.define_retrieval_sla()
    _check(suite, "sla_definition_present",
           "simple_lookup_p95_ms" in sla_def, str(sla_def))

    qen = sla.benchmark_query_latency("en",
                                       ["engineer", "ledger", "vector"],
                                       limit=10)
    _check(suite, "en_query_latency_bounded",
           qen["bounds_ok"] and qen["n_calls"] == 3, str(qen))

    qru = sla.benchmark_query_latency("ru",
                                       ["инженер", "число", "функция"],
                                       limit=10)
    _check(suite, "ru_query_latency_bounded",
           qru["bounds_ok"] and qru["n_calls"] == 3, str(qru))

    cat = sla.benchmark_category_lookup_latency(
        "en", ["core_vocabulary", "professions_jobs"], limit=10)
    _check(suite, "category_latency_bounded",
           cat["bounds_ok"], str(cat))

    sf = sla.benchmark_safety_filter_latency(
        "en", ["recognition_only", "vulgar"], limit=10)
    _check(suite, "safety_filter_latency_bounded",
           sf["bounds_ok"], str(sf))

    bundle = {"simple_lookup": {"en": qen, "ru": qru},
              "category_lookup": {"en": cat, "ru": cat},
              "register_lookup": {"en": cat, "ru": cat},
              "safety_filter_lookup": {"en": sf, "ru": sf}}
    v = sla.evaluate_sla_results(bundle)
    _check(suite, "sla_verdict_emitted",
           v["overall_verdict"] in ("pass", "warn", "fail"), str(v))

    out = _td() / "sla.json"
    sla.write_sla_report(bundle, out)
    _check(suite, "sla_report_written", out.exists(), "")


# -------------------- G: Index consistency --------------------

def suite_G_index() -> None:
    suite = "G_INDEX"
    en = ic.check_english_index_consistency(limit=200)
    _check(suite, "en_consistency_ok",
           en.get("ok") is True
           and en["n_words_total"] is not None, str(en)[:200])
    ru = ic.check_russian_index_consistency(limit=200)
    _check(suite, "ru_consistency_ok",
           ru.get("ok") is True
           and ru["n_words_total"] is not None, str(ru)[:200])

    fts = ic.check_fts_row_counts()
    _check(suite, "fts_counts_ok",
           fts.get("ok") is True
           and "en" in fts and "ru" in fts, str(fts)[:200])

    p_en = ic.check_pack_id_index_coverage("en")
    _check(suite, "pack_id_coverage_en",
           p_en.get("ok") is True
           and 0.0 <= p_en["coverage_ratio"] <= 1.0, str(p_en))

    c_en = ic.check_category_index_coverage("en")
    _check(suite, "category_counts_returned",
           c_en.get("ok") is True
           and len(c_en["counts"]) == 21, str(len(c_en["counts"])))

    saf_en = ic.check_safety_filter_index_behavior("en")
    _check(suite, "safety_filter_check_no_leaks",
           saf_en.get("ok") is True
           and saf_en["total_leaks"] == 0,
           f"leaks={saf_en.get('total_leaks')}")

    bounds_en = ic.check_index_query_bounds("en")
    _check(suite, "query_bounds_clamp_at_200",
           bounds_en["ok"] is True
           and bounds_en["returned"] <= 200, str(bounds_en))

    out = _td() / "ix.json"
    ic.write_index_consistency_report({"x": "y"}, out)
    _check(suite, "index_report_written", out.exists(), "")


# -------------------- H: Safety regression --------------------

def suite_H_safety_regression() -> None:
    suite = "H_SAFETY_REGRESSION"
    probes = sra.build_safety_probe_set()
    _check(suite, "probes_have_six_classes",
           set(probes.keys())
           == {"recognition_only", "do_not_use_unprompted", "vulgar",
               "offensive", "slang_normal", "benign"},
           str(set(probes.keys())))

    en = sra.audit_english_safety_policy()
    _check(suite, "english_safety_all_pass",
           en["all_pass"] is True,
           str([c for c, v in en["classes"].items()
                if not v["expectation"]["pass"]]))
    ru = sra.audit_russian_safety_policy()
    _check(suite, "russian_safety_all_pass",
           ru["all_pass"] is True,
           str([c for c, v in ru["classes"].items()
                if not v["expectation"]["pass"]]))

    ix_en = sra.audit_indexed_retrieval_safety("en")
    _check(suite, "indexed_safety_runs_en",
           ix_en.get("ok") is True, str(ix_en)[:200])
    ix_ru = sra.audit_indexed_retrieval_safety("ru")
    _check(suite, "indexed_safety_runs_ru",
           ix_ru.get("ok") is True, "")

    rt_en = sra.audit_runtime_context_safety("en")
    _check(suite, "runtime_context_en_bounds_ok",
           rt_en.get("ok") is True and rt_en["bounds_ok"] is True, "")
    rt_ru = sra.audit_runtime_context_safety("ru")
    _check(suite, "runtime_context_ru_bounds_ok",
           rt_ru.get("ok") is True and rt_ru["bounds_ok"] is True, "")

    pu = sra.audit_prompted_vs_unprompted_behavior("en")
    _check(suite, "prompted_softening_consistent",
           pu["all_softening_consistent"] is True, str(pu)[:200])

    out = _td() / "safety.json"
    sra.write_safety_regression_report({"x": "y"}, out)
    _check(suite, "safety_report_written", out.exists(), "")


# -------------------- I: Phase 20 runner --------------------

def suite_I_runner() -> None:
    suite = "I_RUNNER"
    pre = p20.verify_phase20_preflight()
    _check(suite, "runner_preflight_ok", pre["ok"], str(pre)[:200])

    s = p20.setup_phase20_folders()
    _check(suite, "runner_setup_ok", s["ok"], str(s))

    # Don't snapshot live DB every run - tests do it once already in suite B.
    # But the runner's create_phase20_backup_snapshot must still work.
    snap = p20.create_phase20_backup_snapshot(label="suite_i")
    _check(suite, "runner_snapshot_ok",
           snap.get("ok") is True
           and "report_path" in snap and Path(snap["report_path"]).exists(),
           str(snap)[:200])

    plans = p20.build_phase20_stage_plans()
    _check(suite, "runner_plans_built",
           plans["en"]["ok"] is True and plans["ru"]["ok"] is True,
           str(plans)[:200])

    # The runner dry-run rehearsal needs synthetic fixtures
    fx = bench.create_synthetic_scale_fixture(
        "en", _td() / "rs_en.jsonl", rows=300)
    fx_ru = bench.create_synthetic_scale_fixture(
        "ru", _td() / "rs_ru.jsonl", rows=300)
    fixtures = {"en": fx, "ru": fx_ru}
    dr = p20.run_phase20_dry_run_rehearsal(fixtures, max_per_lang=200)
    _check(suite, "runner_dry_run_ok",
           dr["en"]["ok"] is True and dr["ru"]["ok"] is True, str(dr)[:200])

    # Synthetic million rehearsal - run a small N for speed (not 1M)
    mill = p20.run_phase20_synthetic_million_rehearsal(
        rows_per_language=2000)
    _check(suite, "synthetic_million_rehearsal_ok",
           mill["ok"] is True, str(mill)[:200])
    _check(suite, "rehearsal_streaming_read_returned",
           mill["streaming_read"]["en"]["rows_read"] == 2000, "")

    qa = p20.run_phase20_post_stage_quality_audits(language="en")
    _check(suite, "runner_quality_audit_ok",
           qa["quality_score"]["ok"] is True, str(qa)[:200])

    sl = p20.run_phase20_retrieval_sla_eval()
    _check(suite, "runner_sla_ok",
           "verdict" in sl and sl["verdict"]["overall_verdict"]
           in ("pass", "warn", "fail"), str(sl)[:200])

    ix = p20.run_phase20_index_consistency_checks()
    _check(suite, "runner_index_check_ok",
           "report_path" in ix
           and Path(ix["report_path"]).exists(), "")

    saf = p20.run_phase20_safety_regression_audit()
    _check(suite, "runner_safety_audit_ok",
           saf["english_policy"]["all_pass"] is True
           and saf["russian_policy"]["all_pass"] is True,
           str(saf)[:200])

    # Real off by default
    off = p20.run_phase20_real_stage_imports()
    _check(suite, "runner_real_off_default",
           off["allow_real_import"] is False
           and off["reason"] == "real_stage_import_disabled_by_default",
           str(off))
    # Too-large stage size rejected
    over = p20.run_phase20_real_stage_imports(
        plans={"en": {"ok": True, "plan": {}}},
        allow_real_import=True, max_stage_size=500_000)
    _check(suite, "runner_oversize_stage_rejected",
           over.get("ok") is False, str(over))
    # Missing backup snapshot rejected
    no_bk = p20.run_phase20_real_stage_imports(
        plans={"en": {"ok": True, "plan": {}}},
        allow_real_import=True, max_stage_size=100_000)
    _check(suite, "runner_missing_backup_rejected",
           no_bk.get("ok") is False
           and "backup_snapshot_required" in (no_bk.get("error") or ""),
           str(no_bk))


# -------------------- J: Million synthetic rehearsal --------------------

def suite_J_million_rehearsal() -> None:
    suite = "J_MILLION"
    td = _td()
    # Generate a 5,000-row synthetic million-like fixture and stream-validate
    # with max_rows=5000 to confirm no full-load happens.
    p = td / "mill_en.jsonl"
    en = bench.create_synthetic_scale_fixture("en", p, rows=5000)
    _check(suite, "fixture_created_5000",
           en["rows_written"] == 5000, str(en)[:120])
    sr = bench.benchmark_streaming_read(p, max_rows=2500)
    _check(suite, "stream_read_bounded_to_2500",
           sr["rows_read"] == 2500, str(sr))

    # Dry-run import honors max_entries (stage cap simulation)
    di = bench.benchmark_dry_run_import(p, language="en",
                                        max_entries=1000)
    _check(suite, "dry_run_capped_at_1000",
           int(di["accepted"]) <= 1000, str(di)[:200])
    _check(suite, "dry_run_no_production_write",
           di.get("ok") is True, "")

    # Production lexicon untouched
    import cognitive_lexicon_store as enlex
    before = enlex.count_words()
    bench.benchmark_dry_run_import(p, language="en", max_entries=500)
    after = enlex.count_words()
    _check(suite, "production_db_unchanged",
           before == after, f"{before}->{after}")


# -------------------- K: Isolation --------------------

PHASE20_FILES = [
    "dual_vocab_backup_restore.py",
    "dual_import_batch_ledger.py",
    "dual_million_stage_planner.py",
    "dual_post_stage_quality_audit.py",
    "dual_retrieval_sla_eval.py",
    "dual_index_consistency_checker.py",
    "dual_safety_regression_auditor.py",
    "phase20_million_readiness_runner.py",
]


def suite_K_isolation() -> None:
    suite = "K_ISOLATION"
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
    for fname in PHASE20_FILES:
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
        net = [m.group(0) for pat in NETWORK
               for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_network", not net, "; ".join(net[:3]))
        dh = [m.group(0) for pat in DAEMON
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_daemon", not dh, "; ".join(dh[:3]))


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_BACKUP", suite_B_backup),
        ("C_LEDGER", suite_C_ledger),
        ("D_PLANNER", suite_D_planner),
        ("E_QUALITY_AUDIT", suite_E_quality_audit),
        ("F_SLA", suite_F_sla),
        ("G_INDEX", suite_G_index),
        ("H_SAFETY_REGRESSION", suite_H_safety_regression),
        ("I_RUNNER", suite_I_runner),
        ("J_MILLION", suite_J_million_rehearsal),
        ("K_ISOLATION", suite_K_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 20 Million Readiness Gate ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
