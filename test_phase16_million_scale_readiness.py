"""Phase 16 - Million-Scale Corpus Readiness, Chunked Import Engine, and
Quality Gate.

Synthetic-fixtures-only test harness. No real corpora are downloaded. No real
ingestion of large external files. No daemons. No background pollers. No
network access.

All temp data goes under tempfile.mkdtemp(). The harness does NOT touch:
  * Program S
  * tier / probe / attestation
  * worker.py
  * luna_modules

Nine suites: A registry, B checkpoint, C streamers, D normalization+routing,
E quality gate, F chunked importer (dry_run), G chunked importer (real-write
into temp DBs), H resume-from-checkpoint, I safety boundaries.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

# Force UTF-8 stdout for Cyrillic content on Windows.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")

import dual_corpus_registry as reg
import dual_corpus_checkpoint as ckpt
import dual_corpus_quality_gate as qg
import dual_corpus_chunked_importer as imp


PASS = "[PASS]"
FAIL = "[FAIL]"


_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name, PASS if cond else FAIL + (": " + detail if detail else "")))


def _temp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase16_"))


# -------------------- Synthetic fixtures --------------------

def _write_jsonl(p: Path, rows: list[dict]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return p


def _write_txt(p: Path, words: list[str]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(words) + "\n", encoding="utf-8")
    return p


def _write_csv(p: Path, header: list[str], rows: list[list[str]]) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        fh.write(",".join(header) + "\n")
        for r in rows:
            fh.write(",".join(r) + "\n")
    return p


def _en_good_row(word: str, defn: str = "synthetic definition") -> dict:
    return {
        "word": word, "definition": defn, "language": "en",
        "synonyms": [], "examples": [],
        "tags": ["synthetic"],
        "register_tags": ["standard"],
        "coverage_categories": ["core_vocabulary"],
        "safety_tags": [],
        "frequency_score": 0.5,
        "word_level": "common",
    }


def _ru_good_row(word: str, defn: str = "синтетическое определение") -> dict:
    return {
        "word": word, "definition": defn, "language": "ru",
        "synonyms": [], "examples": [],
        "tags": ["synthetic"],
        "register_tags": ["standard"],
        "coverage_categories": ["core_vocabulary"],
        "safety_tags": [],
        "frequency_score": 0.5,
        "word_level": "common",
    }


# -------------------- Suite A: Registry --------------------

def suite_A_registry() -> None:
    suite = "A_REGISTRY"
    td = _temp_dir()
    reg_db = td / "reg.sqlite3"

    src = _write_jsonl(td / "incoming" / "en" / "synthetic_words.jsonl",
                       [_en_good_row(f"alpha_{i}") for i in range(20)])

    # init + register
    reg.init_registry(reg_db)
    r = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=src,
        declared_categories=["core_vocabulary"],
        declared_registers=["standard"],
        declared_safety=[],
        notes="suite A synthetic",
        db_path=reg_db)
    _check(suite, "register_ok", r.get("ok") is True, str(r))
    _check(suite, "register_sha256_present",
           bool(r.get("source_sha256")), str(r.get("source_sha256")))
    _check(suite, "register_row_estimate_ge_20",
           int(r.get("row_estimate", 0)) >= 20,
           str(r.get("row_estimate")))

    cid = r["corpus_id"]
    rec = reg.get_corpus_source(cid, db_path=reg_db)
    _check(suite, "get_returns_record", rec is not None, "")
    _check(suite, "registered_status",
           rec["status"] == "registered" if rec else False, "")

    listing = reg.list_corpus_sources(language="en", db_path=reg_db)
    _check(suite, "list_finds_record",
           any(x["corpus_id"] == cid for x in listing), "")

    u = reg.update_corpus_status(cid, "queued", notes="ready",
                                 db_path=reg_db)
    _check(suite, "update_status_ok", u["ok"], str(u))
    rec2 = reg.get_corpus_source(cid, db_path=reg_db)
    _check(suite, "status_changed_to_queued",
           rec2["status"] == "queued" if rec2 else False, "")

    bad = reg.register_corpus_source(
        language="xx", source_type="word_list",
        expected_format="jsonl", source_path=src, db_path=reg_db)
    _check(suite, "rejects_invalid_language", bad.get("ok") is False, str(bad))

    bad2 = reg.register_corpus_source(
        language="en", source_type="not_a_type",
        expected_format="jsonl", source_path=src, db_path=reg_db)
    _check(suite, "rejects_invalid_source_type",
           bad2.get("ok") is False, str(bad2))

    missing = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=td / "nope.jsonl",
        db_path=reg_db)
    _check(suite, "rejects_missing_file",
           missing.get("ok") is False
           and missing.get("error") == "file_not_found", str(missing))

    pv = reg.preview_corpus_source(src, "jsonl", limit=5)
    _check(suite, "preview_returns_rows",
           pv["ok"] and len(pv["samples"]) == 5, str(pv))


# -------------------- Suite B: Checkpoint --------------------

def suite_B_checkpoint() -> None:
    suite = "B_CHECKPOINT"
    td = _temp_dir()
    ck = td / "ckpt.sqlite3"
    ckpt.init_checkpoint_store(ck)

    cr = ckpt.create_checkpoint(
        corpus_id="cid_test", source_path="/tmp/x.jsonl",
        language="en", notes="suite B", db_path=ck)
    _check(suite, "create_ok", cr["ok"], str(cr))
    cid = cr["checkpoint_id"]

    loaded = ckpt.load_checkpoint(cid, db_path=ck)
    _check(suite, "load_returns_record", loaded is not None, "")
    _check(suite, "initial_offsets_zero",
           loaded["last_byte_offset"] == 0 and loaded["last_line_number"] == 0,
           str(loaded))

    u = ckpt.update_checkpoint(cid, last_byte_offset=512,
                               last_line_number=100,
                               accepted_count=80,
                               rejected_count=10,
                               duplicate_count=5,
                               batch_count=3,
                               db_path=ck)
    _check(suite, "update_ok", u["ok"], str(u))
    loaded2 = ckpt.load_checkpoint(cid, db_path=ck)
    _check(suite, "update_persisted",
           loaded2["last_byte_offset"] == 512
           and loaded2["accepted_count"] == 80, str(loaded2))

    fc = ckpt.mark_checkpoint_complete(cid, notes="done", db_path=ck)
    _check(suite, "mark_complete_ok", fc["ok"], str(fc))
    loaded3 = ckpt.load_checkpoint(cid, db_path=ck)
    _check(suite, "status_completed",
           loaded3["status"] == "completed", str(loaded3))

    bad = ckpt.create_checkpoint(corpus_id="x", source_path="/y",
                                 language="xx", db_path=ck)
    _check(suite, "rejects_invalid_lang",
           bad.get("ok") is False, str(bad))

    listing = ckpt.list_checkpoints(corpus_id="cid_test", db_path=ck)
    _check(suite, "list_returns_one", len(listing) >= 1, "")


# -------------------- Suite C: Streamers --------------------

def suite_C_streamers() -> None:
    suite = "C_STREAMERS"
    td = _temp_dir()
    jp = _write_jsonl(td / "j.jsonl",
                      [_en_good_row(f"sx_{i}") for i in range(50)])
    tp = _write_txt(td / "t.txt", [f"wx_{i}" for i in range(40)])
    cp = _write_csv(td / "c.csv",
                    ["word", "definition", "language"],
                    [[f"cx_{i}", "syn", "en"] for i in range(30)])

    rows_j = list(imp.stream_jsonl_rows(jp, start_line=0, max_rows=10))
    _check(suite, "jsonl_max_rows_respected", len(rows_j) == 10, str(len(rows_j)))
    _check(suite, "jsonl_yields_dicts",
           all(isinstance(r[2], dict) for r in rows_j), "")

    rows_t = list(imp.stream_txt_rows(tp, start_line=5, max_rows=10))
    _check(suite, "txt_start_line_respected",
           rows_t[0][2]["word"] == "wx_5" if rows_t else False, str(rows_t[:2]))
    _check(suite, "txt_max_rows_respected", len(rows_t) == 10, "")

    rows_c = list(imp.stream_csv_rows(cp, start_line=0, max_rows=5))
    _check(suite, "csv_returns_dicts",
           all(isinstance(r[2], dict) for r in rows_c), "")
    _check(suite, "csv_has_word_column",
           all("word" in r[2] for r in rows_c), "")

    # Streamer must not hold the file open after exhaustion.
    consumed = list(imp.stream_jsonl_rows(jp))
    _check(suite, "jsonl_full_consume_ok", len(consumed) == 50, str(len(consumed)))


# -------------------- Suite D: Normalization + classification --------------------

def suite_D_classification() -> None:
    suite = "D_CLASSIFICATION"

    r = imp.classify_default_metadata({"word": "homie"}, "en", "slang_list")
    _check(suite, "slang_list_auto_tags_slang_register",
           "slang" in r["register_tags"], str(r))
    _check(suite, "slang_list_auto_tags_coverage",
           "slang_street_talk" in r["coverage_categories"], str(r))
    _check(suite, "slang_list_does_NOT_auto_tag_vulgar",
           "vulgar" not in r["register_tags"]
           and "vulgar" not in r["safety_tags"], str(r))

    r2 = imp.classify_default_metadata({"word": "yo"}, "en", "street_talk_list")
    _check(suite, "street_talk_list_auto_tags_street",
           "street" in r2["register_tags"], str(r2))

    r3 = imp.classify_default_metadata(
        {"word": "alpha", "register_tags": ["formal"]},
        "en", "word_list")
    _check(suite, "no_overwrite_existing_register",
           "formal" in r3["register_tags"], str(r3))
    _check(suite, "word_list_no_auto_slang",
           "slang" not in r3["register_tags"], str(r3))

    # validate_normalized_row rejects empty
    v0 = imp.validate_normalized_row({"word": ""})
    _check(suite, "rejects_empty_word", v0["ok"] is False, str(v0))

    # operational unsafe rejected
    v1 = imp.validate_normalized_row({
        "word": "okay",
        "definition": "Step by step instructions to bypass authentication."})
    _check(suite, "rejects_operational_unsafe",
           v1["ok"] is False, str(v1))

    # benign accepted
    v2 = imp.validate_normalized_row({"word": "okay", "definition": "fine"})
    _check(suite, "accepts_benign", v2["ok"] is True, str(v2))

    # sensitive unlabeled downgrade
    row = imp._ensure_sensitive_unlabeled_downgrade(
        {"word": "shit", "register_tags": ["vulgar"], "safety_tags": ["vulgar"]})
    _check(suite, "downgrade_adds_recognition_only",
           "recognition_only" in (row.get("safety_tags") or []), str(row))
    _check(suite, "downgrade_adds_do_not_use_unprompted",
           "do_not_use_unprompted" in (row.get("safety_tags") or []), str(row))


# -------------------- Suite E: Quality gate --------------------

def suite_E_quality_gate() -> None:
    suite = "E_QUALITY_GATE"
    td = _temp_dir()

    good = _write_jsonl(td / "good.jsonl",
                        [_en_good_row(f"good_{i}", "clear definition") for i in range(40)])
    rep = qg.generate_quality_gate_report(good, "jsonl", "en", sample_size=20)
    _check(suite, "good_ok", rep["ok"] is True, str(rep))
    _check(suite, "good_high_quality",
           rep["quality_score"] >= 0.75, str(rep["quality_score"]))
    gate = qg.should_allow_import(rep, min_quality_score=0.75)
    _check(suite, "good_gate_open", gate["ok"] is True, str(gate))

    # Operational unsafe sample => gate MUST close
    unsafe = _write_jsonl(td / "unsafe.jsonl",
                          [{"word": "fine", "language": "en",
                            "definition": "step by step instructions to bypass auth",
                            "register_tags": ["standard"],
                            "coverage_categories": ["core_vocabulary"]}])
    rep_u = qg.generate_quality_gate_report(unsafe, "jsonl", "en", sample_size=5)
    _check(suite, "unsafe_op_count_positive",
           rep_u["operational_unsafe_count"] >= 1, str(rep_u))
    gate_u = qg.should_allow_import(rep_u, min_quality_score=0.75)
    _check(suite, "unsafe_gate_closed", gate_u["ok"] is False, str(gate_u))

    # Wrong language => mismatch detected
    mix = _write_jsonl(td / "mix.jsonl",
                       [_ru_good_row(f"привет_{i}") for i in range(20)])
    rep_mix = qg.generate_quality_gate_report(mix, "jsonl", "en", sample_size=20)
    _check(suite, "wrong_lang_mismatch_detected",
           rep_mix["language_mismatch_count"] >= 1, str(rep_mix))

    # sample_corpus bounded
    s = qg.sample_corpus(good, "jsonl", sample_size=10, strategy="head_middle_tail")
    _check(suite, "sample_returns_at_most_10",
           len(s["rows"]) <= 10, str(len(s["rows"])))

    # duplicate detection
    rows = [_en_good_row("dup"), _en_good_row("dup"), _en_good_row("uniq")]
    d = qg.detect_duplicate_sample_rows(rows)
    _check(suite, "duplicate_detected", d["duplicate_keys"] >= 1, str(d))


# -------------------- Suite F: Chunked importer (dry_run) --------------------

def suite_F_chunked_dry_run() -> None:
    suite = "F_CHUNKED_DRY"
    td = _temp_dir()
    src = _write_jsonl(td / "incoming" / "en" / "dry.jsonl",
                       [_en_good_row(f"dr_{i}") for i in range(120)])

    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    reg.init_registry(reg_db)
    ckpt.init_checkpoint_store(ck_db)
    r = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=src, db_path=reg_db)
    cid = r["corpus_id"]

    res = imp.import_corpus(
        corpus_id=cid,
        batch_size=20, max_entries=80,
        dry_run=True,
        registry_db_path=reg_db,
        checkpoint_db_path=ck_db,
        rejections_dir=td / "rej",
        reports_dir=td / "rep",
        skip_quality_gate=False)
    _check(suite, "dry_run_ok", res.get("ok") is True, str(res)[:300])
    _check(suite, "dry_run_accepted_count",
           res["accepted"] == 80, str(res["accepted"]))
    _check(suite, "dry_run_no_rejection_log_file",
           not (Path(td / "rej") / "dry.rejected.jsonl").exists(), "")
    _check(suite, "dry_run_writes_report",
           Path(res["report_path"]).exists(), str(res["report_path"]))
    _check(suite, "dry_run_pack_id_present",
           bool(res.get("pack_id")), "")
    _check(suite, "dry_run_quality_report_attached",
           res.get("quality_report") is not None, "")

    # default cap (no max_entries, no allow_full_source)
    src2 = _write_jsonl(td / "big.jsonl",
                        [_en_good_row(f"bg_{i}") for i in range(80)])
    rb = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=src2, db_path=reg_db)
    res2 = imp.import_corpus(
        corpus_id=rb["corpus_id"], batch_size=50,
        dry_run=True, registry_db_path=reg_db,
        checkpoint_db_path=ck_db,
        rejections_dir=td / "rej", reports_dir=td / "rep")
    _check(suite, "default_max_entries_enforced",
           res2["max_entries_applied"] == imp.DEFAULT_MAX_ENTRIES,
           str(res2["max_entries_applied"]))


# -------------------- Suite G: Chunked importer (real-write into temp DBs) --------------------

def suite_G_real_write() -> None:
    suite = "G_REAL_WRITE"
    td = _temp_dir()
    en_db = td / "en.sqlite3"
    ru_db = td / "ru.sqlite3"
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"

    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    enlex.init_db(en_db)
    rulex.init_db(ru_db)
    before_en = enlex.count_words(en_db)
    before_ru = rulex.count_words(ru_db)

    en_src = _write_jsonl(td / "en_corp.jsonl",
                          [_en_good_row(f"en_corp_{i}", "clear definition")
                           for i in range(60)])
    ru_src = _write_jsonl(td / "ru_corp.jsonl",
                          [_ru_good_row(f"ру_слово_{i}", "ясное определение")
                           for i in range(50)])

    reg.init_registry(reg_db)
    en_r = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=en_src, db_path=reg_db)
    ru_r = reg.register_corpus_source(
        language="ru", source_type="word_list",
        expected_format="jsonl", source_path=ru_src, db_path=reg_db)

    res_en = imp.import_corpus(
        corpus_id=en_r["corpus_id"], batch_size=10,
        max_entries=40, dry_run=False,
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, ru_db_path=ru_db,
        rejections_dir=td / "rej", reports_dir=td / "rep",
        skip_quality_gate=True)
    _check(suite, "en_real_write_ok", res_en.get("ok") is True, str(res_en)[:300])
    _check(suite, "en_added_40", res_en["accepted"] == 40, str(res_en["accepted"]))
    after_en = enlex.count_words(en_db)
    _check(suite, "en_db_grew_by_40",
           after_en - before_en == 40, f"{before_en}->{after_en}")

    res_ru = imp.import_corpus(
        corpus_id=ru_r["corpus_id"], batch_size=10,
        max_entries=30, dry_run=False,
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, ru_db_path=ru_db,
        rejections_dir=td / "rej", reports_dir=td / "rep",
        skip_quality_gate=True)
    _check(suite, "ru_real_write_ok", res_ru.get("ok") is True, str(res_ru)[:300])
    _check(suite, "ru_added_30", res_ru["accepted"] == 30, str(res_ru["accepted"]))
    after_ru = rulex.count_words(ru_db)
    _check(suite, "ru_db_grew_by_30",
           after_ru - before_ru == 30, f"{before_ru}->{after_ru}")

    # Registry status should be 'completed' on both
    en_rec = reg.get_corpus_source(en_r["corpus_id"], db_path=reg_db)
    ru_rec = reg.get_corpus_source(ru_r["corpus_id"], db_path=reg_db)
    _check(suite, "en_status_completed",
           en_rec["status"] == "completed" if en_rec else False, "")
    _check(suite, "ru_status_completed",
           ru_rec["status"] == "completed" if ru_rec else False, "")


# -------------------- Suite H: Resume from checkpoint --------------------

def suite_H_resume() -> None:
    suite = "H_RESUME"
    td = _temp_dir()
    src = _write_jsonl(td / "incoming" / "en" / "rs.jsonl",
                       [_en_good_row(f"rs_{i}") for i in range(100)])
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    reg.init_registry(reg_db)
    rr = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=src, db_path=reg_db)
    cid = rr["corpus_id"]

    # Round 1: import first 40
    r1 = imp.import_corpus(
        corpus_id=cid, batch_size=10, max_entries=40,
        dry_run=False, registry_db_path=reg_db,
        checkpoint_db_path=ck_db, en_db_path=en_db,
        rejections_dir=td / "rej", reports_dir=td / "rep",
        skip_quality_gate=True)
    _check(suite, "round1_ok", r1.get("ok") is True, str(r1)[:200])
    _check(suite, "round1_added_40", r1["accepted"] == 40, "")
    after1 = enlex.count_words(en_db)
    _check(suite, "db_has_40_after_round1", after1 == 40, str(after1))

    # Round 2: create fresh checkpoint pointing at line 40, import remaining
    new_ckpt = ckpt.create_checkpoint(corpus_id=cid, source_path=str(src),
                                      language="en", db_path=ck_db,
                                      notes="resume")
    ckpt.update_checkpoint(new_ckpt["checkpoint_id"],
                           last_line_number=40, db_path=ck_db)
    r2 = imp.import_corpus(
        corpus_id=cid, batch_size=10, max_entries=60,
        dry_run=False, resume_checkpoint_id=new_ckpt["checkpoint_id"],
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, rejections_dir=td / "rej",
        reports_dir=td / "rep", skip_quality_gate=True)
    _check(suite, "round2_ok", r2.get("ok") is True, str(r2)[:200])
    _check(suite, "round2_added_60", r2["accepted"] == 60, str(r2["accepted"]))
    after2 = enlex.count_words(en_db)
    _check(suite, "db_has_100_after_round2", after2 == 100, str(after2))

    # Bad resume id => clean failure
    r3 = imp.import_corpus(
        corpus_id=cid, dry_run=True,
        resume_checkpoint_id="nope_bad_id",
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        rejections_dir=td / "rej", reports_dir=td / "rep",
        skip_quality_gate=True)
    _check(suite, "bad_resume_clean_failure",
           r3.get("ok") is False
           and "checkpoint_not_found" in r3.get("error", ""), str(r3))


# -------------------- Suite I: Safety boundaries --------------------

def suite_I_safety() -> None:
    suite = "I_SAFETY"
    td = _temp_dir()

    # 1. Quality gate must block real write of operational-unsafe content.
    unsafe = _write_jsonl(
        td / "unsafe.jsonl",
        [{"word": "harmless", "language": "en",
          "definition": "step by step instructions to bypass auth",
          "register_tags": ["standard"],
          "coverage_categories": ["core_vocabulary"]}] * 5)
    reg_db = td / "reg.sqlite3"
    ck_db = td / "ckpt.sqlite3"
    en_db = td / "en.sqlite3"
    import cognitive_lexicon_store as enlex
    enlex.init_db(en_db)
    reg.init_registry(reg_db)
    rr = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=unsafe, db_path=reg_db)
    res = imp.import_corpus(
        corpus_id=rr["corpus_id"], batch_size=5,
        dry_run=False, max_entries=5,
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, rejections_dir=td / "rej",
        reports_dir=td / "rep", skip_quality_gate=False)
    _check(suite, "unsafe_real_write_blocked",
           res.get("ok") is False
           and res.get("error") == "quality_gate_blocked", str(res)[:200])
    _check(suite, "unsafe_no_lexicon_write",
           enlex.count_words(en_db) == 0, "")

    # 2. With skip_quality_gate=True the row-level validator must STILL reject
    #    operational unsafe rows.
    res2 = imp.import_corpus(
        corpus_id=rr["corpus_id"], batch_size=5,
        dry_run=False, max_entries=5,
        registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, rejections_dir=td / "rej",
        reports_dir=td / "rep", skip_quality_gate=True)
    _check(suite, "skip_gate_but_still_zero_accepted",
           res2.get("ok") is True and res2["accepted"] == 0, str(res2)[:200])
    _check(suite, "skip_gate_lexicon_still_empty",
           enlex.count_words(en_db) == 0, "")

    # 3. Slang source type must NOT auto-add vulgar/offensive.
    slang_src = _write_jsonl(td / "slang.jsonl",
                             [_en_good_row(f"sl_{i}", "casual term")
                              for i in range(10)])
    s_reg = reg.register_corpus_source(
        language="en", source_type="slang_list",
        expected_format="jsonl", source_path=slang_src, db_path=reg_db)
    res3 = imp.import_corpus(
        corpus_id=s_reg["corpus_id"], batch_size=10, max_entries=10,
        dry_run=False, registry_db_path=reg_db, checkpoint_db_path=ck_db,
        en_db_path=en_db, rejections_dir=td / "rej",
        reports_dir=td / "rep", skip_quality_gate=True)
    _check(suite, "slang_real_write_ok",
           res3.get("ok") is True and res3["accepted"] == 10, str(res3)[:200])
    # Spot-check the lexicon: no row should carry vulgar/offensive.
    import sqlite3
    conn = sqlite3.connect(str(en_db))
    try:
        cur = conn.execute(
            "SELECT safety_tags_json, register_tags_json FROM words "
            "WHERE word LIKE 'sl_%'")
        rows = cur.fetchall()
    finally:
        conn.close()
    bad = []
    for st_json, rt_json in rows:
        st = set(json.loads(st_json or "[]"))
        rt = set(json.loads(rt_json or "[]"))
        if {"vulgar", "offensive"} & (st | rt):
            bad.append((st, rt))
    _check(suite, "slang_no_vulgar_or_offensive",
           len(bad) == 0, f"unexpected_bad_rows={bad}")

    # 4. Default cap protection
    big_src = _write_jsonl(td / "big2.jsonl",
                           [_en_good_row(f"bg2_{i}") for i in range(40)])
    bg_reg = reg.register_corpus_source(
        language="en", source_type="word_list",
        expected_format="jsonl", source_path=big_src, db_path=reg_db)
    res4 = imp.import_corpus(
        corpus_id=bg_reg["corpus_id"], batch_size=10,
        dry_run=True, registry_db_path=reg_db,
        checkpoint_db_path=ck_db, en_db_path=en_db,
        rejections_dir=td / "rej", reports_dir=td / "rep",
        skip_quality_gate=True)
    _check(suite, "default_cap_present",
           res4["max_entries_applied"] == imp.DEFAULT_MAX_ENTRIES,
           str(res4["max_entries_applied"]))


# -------------------- Forbidden-import scan --------------------

def suite_J_forbidden_imports() -> None:
    suite = "J_FORBIDDEN_IMPORTS"
    import re
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
    targets = [
        "dual_corpus_registry.py",
        "dual_corpus_checkpoint.py",
        "dual_corpus_quality_gate.py",
        "dual_corpus_chunked_importer.py",
    ]
    for fname in targets:
        p = Path(fname)
        if not p.exists():
            _check(suite, f"{fname}_exists", False, "missing")
            continue
        text = p.read_text(encoding="utf-8")
        violations: list[str] = []
        for line in text.splitlines():
            for forb in FORBIDDEN:
                if re.search(rf"^(import|from)\s+\S*{re.escape(forb)}", line):
                    violations.append(f"{line.strip()!r}")
        _check(suite, f"{fname}_no_forbidden_imports",
               not violations, "; ".join(violations[:5]))


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_REGISTRY", suite_A_registry),
        ("B_CHECKPOINT", suite_B_checkpoint),
        ("C_STREAMERS", suite_C_streamers),
        ("D_CLASSIFICATION", suite_D_classification),
        ("E_QUALITY_GATE", suite_E_quality_gate),
        ("F_CHUNKED_DRY", suite_F_chunked_dry_run),
        ("G_REAL_WRITE", suite_G_real_write),
        ("H_RESUME", suite_H_resume),
        ("I_SAFETY", suite_I_safety),
        ("J_FORBIDDEN_IMPORTS", suite_J_forbidden_imports),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False, f"{e!r}\n{traceback.format_exc()}")

    fails = [r for r in _results if not r[2].startswith(PASS)]
    print(f"=== Phase 16 Million-Scale Readiness ===")
    print(f"Total checks: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
