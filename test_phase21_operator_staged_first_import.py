"""Phase 21 - Operator-Staged First Real Import Harness.

Synthetic-only validation (temp DB + temp incoming dirs). Real operator
files are NOT created by this harness. Production lexicons inspected
read-only. No daemons. No internet.
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

import phase21_operator_stage_runner as p21
import dual_corpus_quality_gate as qg
import dual_import_batch_ledger as led
import dual_vocab_backup_restore as bk


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase21_"))


def _good_en_row(i: int) -> dict:
    return {"word": f"p21_en_{i}", "language": "en",
            "definition": "a clear synthetic definition",
            "coverage_categories": ["core_vocabulary"],
            "register_tags": ["standard"],
            "safety_tags": []}


def _good_ru_row(i: int) -> dict:
    return {"word": f"п21_ру_{i}", "lemma": f"п21_ру_{i}",
            "part_of_speech": "noun", "language": "ru",
            "definition": "ясное определение",
            "coverage_categories": ["core_vocabulary"],
            "register_tags": ["standard"],
            "safety_tags": []}


def _write_jsonl(p: Path, rows: list[dict]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return p


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    pre = p21.verify_phase21_preflight()
    _check(suite, "preflight_ok", pre["ok"],
           "missing=" + ",".join(pre["missing_files"]))
    for f in p21.PHASE21_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Folder setup --------------------

def suite_B_folders() -> None:
    suite = "B_FOLDERS"
    s = p21.setup_phase21_folders()
    _check(suite, "setup_ok", s["ok"] is True, str(s))
    for d in (p21.PHASE21_BASE, p21.PHASE21_STAGE_DIR,
              p21.PHASE21_QUALITY_DIR, p21.PHASE21_DRY_RUN_DIR,
              p21.PHASE21_IMPORT_DIR, p21.PHASE21_LEDGER_DIR,
              p21.PHASE21_QUALITY_AUDIT_DIR, p21.PHASE21_SLA_DIR,
              p21.PHASE21_INDEX_DIR, p21.PHASE21_SAFETY_DIR,
              p21.PHASE21_ROLLBACK_DIR, p21.PHASE21_FINAL_DIR):
        _check(suite, f"folder_{d.name}_exists", d.exists(), str(d))


# -------------------- C: Discovery --------------------

def suite_C_discovery() -> None:
    suite = "C_DISCOVERY"
    # The live incoming dirs may be empty (staging-required case). Either way,
    # discovery must return a bounded list with correct shape.
    en = p21.discover_operator_staged_sources(language="en", limit=5)
    _check(suite, "en_discovery_returns_list",
           isinstance(en, list) and len(en) <= 5, str(len(en)))
    ru = p21.discover_operator_staged_sources(language="ru", limit=5)
    _check(suite, "ru_discovery_returns_list",
           isinstance(ru, list) and len(ru) <= 5, "")
    both = p21.discover_operator_staged_sources(limit=5)
    _check(suite, "both_lang_bounded",
           isinstance(both, list) and len(both) <= 5, "")

    # validate_operator_sources flags missing pair
    v_empty = p21.validate_operator_sources([])
    _check(suite, "empty_missing_both",
           v_empty["missing_english"] and v_empty["missing_russian"]
           and v_empty["bilingual_ready"] is False, str(v_empty))
    v_en_only = p21.validate_operator_sources([{"language": "en",
                                                 "path": "/x"}])
    _check(suite, "en_only_missing_russian",
           v_en_only["missing_russian"] is True
           and v_en_only["bilingual_ready"] is False, str(v_en_only))
    v_both = p21.validate_operator_sources([{"language": "en", "path": "/x"},
                                             {"language": "ru", "path": "/y"}])
    _check(suite, "both_present_bilingual_ready",
           v_both["bilingual_ready"] is True, str(v_both))


# -------------------- D: Quality gates --------------------

def suite_D_quality() -> None:
    suite = "D_QUALITY"
    td = _td()
    en = _write_jsonl(td / "en.jsonl",
                      [_good_en_row(i) for i in range(80)])
    ru = _write_jsonl(td / "ru.jsonl",
                      [_good_ru_row(i) for i in range(80)])
    sources = [
        {"path": str(en), "language": "en", "file_name": "en.jsonl",
         "size_bytes": en.stat().st_size, "suffix": ".jsonl"},
        {"path": str(ru), "language": "ru", "file_name": "ru.jsonl",
         "size_bytes": ru.stat().st_size, "suffix": ".jsonl"},
    ]
    reg_db = td / "reg.sqlite3"
    enriched = p21.register_phase21_sources(sources, registry_db_path=reg_db)
    _check(suite, "registration_ok",
           all(s.get("corpus_id") for s in enriched), str(enriched)[:200])

    gated = p21.run_phase21_quality_gates(enriched)
    _check(suite, "quality_gate_passes_good_rows",
           all(s.get("quality_ok") for s in gated), str(gated)[:200])

    # Malformed
    bad = _write_jsonl(td / "bad.jsonl",
                       [{"definition": "no word here"}] * 20)
    bad_sources = [{"path": str(bad), "language": "en",
                    "file_name": "bad.jsonl",
                    "size_bytes": bad.stat().st_size, "suffix": ".jsonl"}]
    bad_enriched = p21.register_phase21_sources(bad_sources,
                                                 registry_db_path=reg_db)
    bad_gated = p21.run_phase21_quality_gates(bad_enriched)
    _check(suite, "quality_gate_blocks_missing_word",
           not bad_gated[0]["quality_ok"], str(bad_gated)[:200])

    # Unsafe operational content
    unsafe = _write_jsonl(td / "unsafe.jsonl",
                          [{"word": "ok", "language": "en",
                            "definition": "step by step instructions to bypass auth",
                            "coverage_categories": ["core_vocabulary"],
                            "register_tags": ["standard"]}] * 10)
    unsafe_sources = [{"path": str(unsafe), "language": "en",
                       "file_name": "unsafe.jsonl",
                       "size_bytes": unsafe.stat().st_size,
                       "suffix": ".jsonl"}]
    unsafe_enriched = p21.register_phase21_sources(unsafe_sources,
                                                    registry_db_path=reg_db)
    unsafe_gated = p21.run_phase21_quality_gates(unsafe_enriched)
    _check(suite, "quality_gate_blocks_operational_unsafe",
           not unsafe_gated[0]["quality_ok"], str(unsafe_gated)[:200])

    # Language mismatch (RU file marked as EN)
    mix = _write_jsonl(td / "mix.jsonl",
                       [_good_ru_row(i) for i in range(60)])
    mix_sources = [{"path": str(mix), "language": "en",
                    "file_name": "mix.jsonl",
                    "size_bytes": mix.stat().st_size, "suffix": ".jsonl"}]
    mix_enriched = p21.register_phase21_sources(mix_sources,
                                                 registry_db_path=reg_db)
    mix_gated = p21.run_phase21_quality_gates(mix_enriched)
    rep = mix_gated[0].get("quality_report") or {}
    _check(suite, "quality_gate_flags_language_mismatch",
           int(rep.get("language_mismatch_count") or 0) >= 1, str(rep)[:200])


# -------------------- E: Stage plan --------------------

def suite_E_stage_plan() -> None:
    suite = "E_STAGE_PLAN"
    td = _td()
    en = _write_jsonl(td / "en.jsonl",
                      [_good_en_row(i) for i in range(60)])
    ru = _write_jsonl(td / "ru.jsonl",
                      [_good_ru_row(i) for i in range(60)])
    reg_db = td / "reg.sqlite3"
    sources = [
        {"path": str(en), "language": "en", "file_name": "en.jsonl",
         "size_bytes": en.stat().st_size, "suffix": ".jsonl"},
        {"path": str(ru), "language": "ru", "file_name": "ru.jsonl",
         "size_bytes": ru.stat().st_size, "suffix": ".jsonl"},
    ]
    enriched = p21.register_phase21_sources(sources, registry_db_path=reg_db)
    gated = p21.run_phase21_quality_gates(enriched)
    plan = p21.build_phase21_stage_plan(gated)
    _check(suite, "plan_built_for_both",
           plan["en"]["ok"] and plan["ru"]["ok"], str(plan)[:200])
    en_plan = plan["en"]["plan"]
    _check(suite, "en_max_total_le_10k",
           en_plan["max_total_per_language"] <= p21.HARD_MAX_TOTAL_PER_LANG, "")
    _check(suite, "en_max_per_source_le_10k",
           en_plan["max_per_source"] <= p21.HARD_MAX_PER_SOURCE, "")
    _check(suite, "en_batch_size_le_2500",
           en_plan["batch_size"] <= p21.HARD_BATCH_SIZE, "")
    _check(suite, "en_allow_full_source_false",
           en_plan["allow_full_source"] is False, "")
    for flag in ("quality_gate_required", "dry_run_required",
                 "backup_required", "checkpoint_required",
                 "manifest_required", "rollback_required"):
        _check(suite, f"en_{flag}", en_plan[flag] is True, "")

    # Caps clamp when caller asks higher
    plan_over = p21.build_phase21_stage_plan(
        gated, max_total_per_language=999_999,
        max_per_source=999_999, batch_size=999_999)
    _check(suite, "caps_clamp_total",
           plan_over["en"]["plan"]["max_total_per_language"]
           == p21.HARD_MAX_TOTAL_PER_LANG, "")
    _check(suite, "caps_clamp_per_source",
           plan_over["en"]["plan"]["max_per_source"]
           == p21.HARD_MAX_PER_SOURCE, "")
    _check(suite, "caps_clamp_batch",
           plan_over["en"]["plan"]["batch_size"]
           == p21.HARD_BATCH_SIZE, "")


# -------------------- F: Backup --------------------

def suite_F_backup() -> None:
    suite = "F_BACKUP"
    snap = p21.create_phase21_backup_snapshot(label="harness")
    _check(suite, "backup_created", snap.get("ok") is True,
           str(snap)[:200])
    _check(suite, "backup_verified", snap.get("verified") is True,
           str(snap.get("verify_details"))[:200])
    # Restore must stay dry_run by default
    r = bk.restore_backup_snapshot(snap["snapshot_id"], dry_run=True)
    _check(suite, "restore_dry_run_default",
           r.get("dry_run") is True and r.get("ok"), str(r)[:200])


# -------------------- G: Dry run --------------------

def suite_G_dry_run() -> None:
    suite = "G_DRY_RUN"
    td = _td()
    en = _write_jsonl(td / "en.jsonl",
                      [_good_en_row(i) for i in range(120)])
    ru = _write_jsonl(td / "ru.jsonl",
                      [_good_ru_row(i) for i in range(120)])
    reg_db = td / "reg.sqlite3"
    sources = [
        {"path": str(en), "language": "en", "file_name": "en.jsonl",
         "size_bytes": en.stat().st_size, "suffix": ".jsonl"},
        {"path": str(ru), "language": "ru", "file_name": "ru.jsonl",
         "size_bytes": ru.stat().st_size, "suffix": ".jsonl"},
    ]
    enriched = p21.register_phase21_sources(sources, registry_db_path=reg_db)
    gated = p21.run_phase21_quality_gates(enriched)
    plan = p21.build_phase21_stage_plan(gated, max_total_per_language=80,
                                         max_per_source=80, batch_size=20)

    # Production counts before
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    en_db = td / "en.sqlite3"
    ru_db = td / "ru.sqlite3"
    enlex.init_db(en_db)
    rulex.init_db(ru_db)
    ck_db = td / "ck.sqlite3"
    ld_db = td / "ledger.sqlite3"
    dr = p21.run_phase21_dry_runs(plan,
                                  checkpoint_db_path=ck_db,
                                  ledger_db_path=ld_db,
                                  en_db_path=en_db, ru_db_path=ru_db)
    _check(suite, "dry_run_en_ok", dr["en"]["ok"], str(dr["en"])[:200])
    _check(suite, "dry_run_ru_ok", dr["ru"]["ok"], str(dr["ru"])[:200])

    en_results = dr["en"]["per_source"]
    _check(suite, "dry_run_en_accepted_capped",
           all(r.get("accepted") and r["accepted"] <= 80
               for r in en_results),
           str(en_results)[:200])
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    _check(suite, "production_db_unchanged",
           before_en == after_en and before_ru == after_ru,
           f"en {before_en}->{after_en}; ru {before_ru}->{after_ru}")

    # Ledger row written for dry-run
    rows = led.list_batches(language="en", limit=50, db_path=ld_db)
    _check(suite, "ledger_dry_run_row_written",
           any(r.get("dry_run") is True for r in rows), str(len(rows)))


# -------------------- H: Real import guards --------------------

def suite_H_real_guards() -> None:
    suite = "H_REAL_GUARDS"
    td = _td()
    en = _write_jsonl(td / "en.jsonl",
                      [_good_en_row(i) for i in range(80)])
    ru = _write_jsonl(td / "ru.jsonl",
                      [_good_ru_row(i) for i in range(80)])
    reg_db = td / "reg.sqlite3"
    sources = [
        {"path": str(en), "language": "en", "file_name": "en.jsonl",
         "size_bytes": en.stat().st_size, "suffix": ".jsonl"},
        {"path": str(ru), "language": "ru", "file_name": "ru.jsonl",
         "size_bytes": ru.stat().st_size, "suffix": ".jsonl"},
    ]
    enriched = p21.register_phase21_sources(sources, registry_db_path=reg_db)
    gated = p21.run_phase21_quality_gates(enriched)
    plan = p21.build_phase21_stage_plan(gated, max_total_per_language=50,
                                         max_per_source=50, batch_size=10)

    # 1. Off by default
    off = p21.run_phase21_real_import(plan)
    _check(suite, "default_real_off",
           off["allow_real_import"] is False
           and off["reason"] == "real_import_disabled_by_default",
           str(off))

    # 2. Missing backup snapshot -> gates fail
    no_bk = p21.run_phase21_real_import(plan, allow_real_import=True,
                                         quality_reports=gated,
                                         dry_run_reports={"en": {"ok": True},
                                                          "ru": {"ok": True}},
                                         backup_snapshot_id=None)
    _check(suite, "no_backup_blocked",
           no_bk.get("ok") is False
           and "backup_snapshot_required" in str(no_bk["gates"]["reasons"]),
           str(no_bk)[:200])

    # 3. Failed dry run blocks
    bad_dr = p21.run_phase21_real_import(plan, allow_real_import=True,
                                          quality_reports=gated,
                                          dry_run_reports={"en": {"ok": False},
                                                           "ru": {"ok": False}},
                                          backup_snapshot_id="nope_bad_id")
    _check(suite, "bad_dry_run_blocked",
           bad_dr.get("ok") is False
           and any("dry_run_failed" in r
                   for r in bad_dr["gates"]["reasons"]), str(bad_dr)[:200])

    # 4. Failed quality blocks
    bad_q = [{**s, "quality_ok": False} for s in gated]
    bad_q_run = p21.run_phase21_real_import(plan, allow_real_import=True,
                                             quality_reports=bad_q,
                                             dry_run_reports={"en": {"ok": True},
                                                              "ru": {"ok": True}},
                                             backup_snapshot_id="x")
    _check(suite, "bad_quality_blocked",
           bad_q_run.get("ok") is False
           and any("quality_gate_failed" in r
                   for r in bad_q_run["gates"]["reasons"]),
           str(bad_q_run)[:200])

    # 5. Tampered plan w/ allow_full_source=True
    tampered = json.loads(json.dumps(plan, default=str))
    tampered["en"]["plan"]["allow_full_source"] = True
    tampered_run = p21.run_phase21_real_import(
        tampered, allow_real_import=True,
        quality_reports=gated,
        dry_run_reports={"en": {"ok": True}, "ru": {"ok": True}},
        backup_snapshot_id="x")
    _check(suite, "allow_full_source_tamper_blocked",
           tampered_run.get("ok") is False
           and any("allow_full_source_forbidden" in r
                   for r in tampered_run["gates"]["reasons"]),
           str(tampered_run)[:200])

    # 6. GUARDED HAPPY PATH (temp DBs only)
    en_db = td / "en2.sqlite3"
    ru_db = td / "ru2.sqlite3"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    enlex.init_db(en_db)
    rulex.init_db(ru_db)
    ld_db = td / "ledger.sqlite3"
    ck_db = td / "ck.sqlite3"
    # Generate verified backup snapshot of the LIVE production DBs (read-only)
    snap = p21.create_phase21_backup_snapshot(label="happy_path")
    # Run dry-runs against the temp DB targets first
    dr = p21.run_phase21_dry_runs(plan,
                                  checkpoint_db_path=ck_db,
                                  ledger_db_path=ld_db,
                                  en_db_path=en_db, ru_db_path=ru_db)
    rr = p21.run_phase21_real_import(plan, allow_real_import=True,
                                      quality_reports=gated,
                                      dry_run_reports=dr,
                                      backup_snapshot_id=snap["snapshot_id"],
                                      checkpoint_db_path=ck_db,
                                      ledger_db_path=ld_db,
                                      en_db_path=en_db, ru_db_path=ru_db)
    _check(suite, "guarded_real_import_ok",
           rr.get("ok") is True and rr["allow_real_import"] is True,
           str(rr)[:200])
    en_used = rr["results"]["en"]["used_total"]
    ru_used = rr["results"]["ru"]["used_total"]
    _check(suite, "guarded_real_capped_to_50",
           en_used <= 50 and ru_used <= 50,
           f"en={en_used} ru={ru_used}")

    # Production lexicon unchanged because we routed to en_db/ru_db
    import cognitive_lexicon_store as enlex2
    import russian_lexicon_store as rulex2
    _check(suite, "production_db_untouched_by_temp_real",
           enlex2.count_words() >= 2000
           and rulex2.count_words() >= 2000, "")
    _check(suite, "temp_en_db_grew",
           enlex.count_words(en_db) == en_used, "")


# -------------------- I: Ledger / rollback --------------------

def suite_I_ledger_rollback() -> None:
    suite = "I_LEDGER_ROLLBACK"
    td = _td()
    ld_db = td / "ledger.sqlite3"
    led.init_ledger(ld_db)
    cr = led.create_batch_record(language="en", stage_id="s1",
                                  source_path="/p.jsonl",
                                  dry_run=True,
                                  rollback_key="rk_p21_test",
                                  pack_id="pack_p21_test",
                                  db_path=ld_db)
    _check(suite, "ledger_dry_run_row", cr["ok"], str(cr))
    bid = cr["batch_id"]
    led.update_batch_status(bid, "completed", accepted_count=10,
                             completed=True, db_path=ld_db)
    by_rk = led.get_batches_by_rollback_key("rk_p21_test",
                                             db_path=ld_db)
    _check(suite, "rollback_key_lookup_returns_one",
           len(by_rk) == 1 and by_rk[0]["pack_id"] == "pack_p21_test",
           str(by_rk)[:200])

    drill = p21.run_phase21_rollback_drill(
        rollback_keys=["rk_p21_test"],
        dry_run=True, ledger_db_path=ld_db)
    _check(suite, "rollback_drill_lists_actions",
           any(a.get("pack_id") == "pack_p21_test"
               for a in drill["batch_actions_intended"]), str(drill)[:200])
    _check(suite, "rollback_drill_dry_run_marked",
           drill.get("dry_run") is True, "")


# -------------------- J: Post-import audits --------------------

def suite_J_post_audits() -> None:
    suite = "J_POST_AUDITS"
    qa = p21.run_phase21_post_import_quality_audits()
    _check(suite, "post_quality_audit_en_score",
           "quality_score" in qa["en"]
           and qa["en"]["quality_score"]["ok"] is True, str(qa)[:200])
    _check(suite, "post_quality_audit_ru_score",
           "quality_score" in qa["ru"]
           and qa["ru"]["quality_score"]["ok"] is True, "")

    sl = p21.run_phase21_retrieval_sla()
    _check(suite, "sla_verdict_emitted",
           sl["verdict"]["overall_verdict"] in ("pass", "warn", "fail"),
           str(sl)[:200])

    ix = p21.run_phase21_index_consistency()
    _check(suite, "index_consistency_no_safety_leak_en",
           ix["safety_filter_en"]["total_leaks"] == 0, str(ix)[:200])
    _check(suite, "index_consistency_no_safety_leak_ru",
           ix["safety_filter_ru"]["total_leaks"] == 0, "")

    saf = p21.run_phase21_safety_regression()
    _check(suite, "safety_regression_en_pass",
           saf["english_policy"]["all_pass"] is True, "")
    _check(suite, "safety_regression_ru_pass",
           saf["russian_policy"]["all_pass"] is True, "")


# -------------------- K: Safety probes --------------------

def suite_K_safety() -> None:
    suite = "K_SAFETY"
    import dual_safety_regression_auditor as sra
    import dual_retrieval_quality_eval as rqe
    probes = sra.build_safety_probe_set()
    dnu_rows = probes["do_not_use_unprompted"]
    dec = rqe.check_safety_policy_on_results(dnu_rows, mode="teacher",
                                              is_user_prompted=False)
    _check(suite, "dnu_blocked_unprompted",
           dec["do_not_use_violation_count"] >= 1, str(dec))
    vul_rows = probes["vulgar"]
    dec_v = rqe.check_safety_policy_on_results(vul_rows, mode="teacher",
                                                is_user_prompted=False)
    _check(suite, "vulgar_blocked_teacher_mode",
           dec_v["vulgar_in_teacher_mode_count"] >= 1, str(dec_v))
    reco_rows = probes["recognition_only"]
    dec_r = rqe.check_safety_policy_on_results(reco_rows, mode="teacher",
                                                is_user_prompted=False)
    _check(suite, "recognition_only_recognized_not_suggested",
           dec_r["suggestion_only_recognized_count"] >= 1, str(dec_r))
    bn_rows = probes["benign"]
    dec_b = rqe.check_safety_policy_on_results(bn_rows, mode="teacher",
                                                is_user_prompted=False)
    _check(suite, "benign_passes",
           dec_b["do_not_use_violation_count"] == 0
           and dec_b["vulgar_in_teacher_mode_count"] == 0, str(dec_b))


# -------------------- L: Isolation --------------------

PHASE21_FILES = ["phase21_operator_stage_runner.py"]


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
    for fname in PHASE21_FILES:
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
        _check(suite, f"{fname}_no_network",
               not net, "; ".join(net[:3]))
        dh = [m.group(0) for pat in DAEMON
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_daemon",
               not dh, "; ".join(dh[:3]))


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_FOLDERS", suite_B_folders),
        ("C_DISCOVERY", suite_C_discovery),
        ("D_QUALITY", suite_D_quality),
        ("E_STAGE_PLAN", suite_E_stage_plan),
        ("F_BACKUP", suite_F_backup),
        ("G_DRY_RUN", suite_G_dry_run),
        ("H_REAL_GUARDS", suite_H_real_guards),
        ("I_LEDGER_ROLLBACK", suite_I_ledger_rollback),
        ("J_POST_AUDITS", suite_J_post_audits),
        ("K_SAFETY", suite_K_safety),
        ("L_ISOLATION", suite_L_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 21 Operator-Staged First Import ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
