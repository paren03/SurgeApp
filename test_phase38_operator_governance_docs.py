"""Phase 38 test harness — operator governance docs + status dashboard."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path


_TOTAL = 0
_PASS = 0
_FAIL = 0
_FAILURES: list[str] = []


def _check(name: str, ok: bool, detail: str = "") -> None:
    global _TOTAL, _PASS, _FAIL
    _TOTAL += 1
    if ok:
        _PASS += 1
    else:
        _FAIL += 1
        _FAILURES.append(f"{name}: FAIL {detail}".strip())


_ROOT = Path(__file__).resolve().parent


_PHASE38_MODULES = (
    "bilingual_voice_phase38_governance_ledger",
    "bilingual_voice_phase38_operator_readme",
    "bilingual_voice_phase38_verification_checklist",
    "bilingual_voice_phase38_rollback_matrix",
    "bilingual_voice_phase38_commit_safety_audit",
    "bilingual_voice_phase38_status_dashboard",
    "bilingual_voice_phase38_integrity_sweep",
)


_PHASE_REPORTS = (
    "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
    "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
    "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
    "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
    "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
    "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
    "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
    "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
    "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
    "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
    "PHASE36_KEY_HANDOFF_ENVELOPE_REPORT.md",
    "PHASE37_SAFETY_TRACE_ADAPTER_GOVERNANCE_REPORT.md",
)


def suite_a_preflight() -> None:
    # Module sources exist
    for m in _PHASE38_MODULES:
        p = _ROOT / f"{m}.py"
        _check(f"A::file_exists::{m}", p.exists(),
               str(p))
    # Phase 27-37 reports + Phase 25/26 present
    for r in _PHASE_REPORTS:
        _check(f"A::report_present::{r}",
               (_ROOT / r).exists(), r)
    # Folder layout
    base = _ROOT / "bilingual_stack" / "governance_phase38"
    for sub in ("readmes", "checklists", "rollback",
                 "dashboards", "integrity", "reports",
                 "fixtures"):
        _check(f"A::folder::{sub}", (base / sub).exists(),
               str(base / sub))
    # Imports succeed
    for m in _PHASE38_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_governance_ledger() -> None:
    import bilingual_voice_phase38_governance_ledger as gl
    reports = gl.collect_phase_reports(_ROOT)
    _check("B::collect_returns_dict", isinstance(reports, dict))
    for phase in (27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37):
        _check(f"B::report_summary_phase_{phase}",
               phase in reports)
        if phase in reports:
            r = reports[phase]
            _check(f"B::report_present_phase_{phase}",
                   r.get("present") is True,
                   r.get("filename", ""))
    ledger = gl.build_boundary_guarantee_ledger(_ROOT)
    _check("B::ledger_is_dict", isinstance(ledger, dict))
    _check("B::ledger_latest_phase",
           ledger.get("latest_phase") == 37,
           str(ledger.get("latest_phase")))
    _check("B::ledger_regression_total",
           ledger.get("full_regression_total_expected") == 6185)
    pi = ledger.get("production_invariants") or {}
    for k, v in (("english_words", 2814),
                  ("russian_words", 2518),
                  ("russian_phrases", 35),
                  ("bilingual_concepts", 26),
                  ("bilingual_entry_links", 52),
                  ("live_pack_manifests", 90)):
        _check(f"B::invariant::{k}", pi.get(k) == v,
               str(pi.get(k)))
    entries = ledger.get("entries") or []
    _check("B::entries_len_11", len(entries) == 11,
           str(len(entries)))
    seen = {e.get("phase") for e in entries}
    _check("B::entries_cover_27_37",
           seen == set(range(27, 38)),
           str(seen))
    for e in entries:
        _check(f"B::entry_has_title::{e.get('phase')}",
               bool(e.get("title")))
        _check(f"B::entry_has_forbidden::{e.get('phase')}",
               isinstance(e.get("forbidden_runtime_actions"),
                          list)
               and len(e["forbidden_runtime_actions"]) >= 4)
        _check(f"B::entry_has_rollback::{e.get('phase')}",
               bool(e.get("rollback_summary")))
    val = gl.validate_boundary_guarantee_ledger(ledger)
    _check("B::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = gl.validate_boundary_guarantee_ledger("notdict")
    _check("B::validate_rejects_non_dict",
           bad.get("ok") is False)
    summary = gl.summarize_boundary_guarantees(ledger)
    _check("B::summary_ok", summary.get("ok") is True)
    _check("B::summary_adapter_count_distinct",
           summary.get("adapter_count_distinct") == 4,
           str(summary.get("adapter_count_distinct")))
    # Round-trip write
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ledger.json"
        path = gl.write_boundary_guarantee_ledger(
            ledger, str(out))
        _check("B::write_ledger_path",
               Path(path).exists() and Path(path).is_file())
        loaded = json.loads(out.read_text(encoding="utf-8"))
        _check("B::roundtrip_latest_phase",
               loaded.get("latest_phase") == 37)
        out2 = Path(td) / "ledger_report.json"
        path2 = gl.write_governance_ledger_report(
            {"ok": True, "k": "v"}, str(out2))
        _check("B::write_ledger_report_path",
               Path(path2).exists())


def suite_c_operator_readme() -> None:
    import bilingual_voice_phase38_governance_ledger as gl
    import bilingual_voice_phase38_operator_readme as rd
    ledger = gl.build_boundary_guarantee_ledger(_ROOT)
    readme = rd.create_operator_governance_readme(ledger)
    _check("C::readme_is_dict", isinstance(readme, dict))
    body = readme.get("body") or ""
    _check("C::readme_body_nonempty", len(body) > 800,
           str(len(body)))
    for needle in (
        "Phase 27-37", "metadata-only", "dummy_metadata_adapter",
        "bilingual_segment_metadata_adapter",
        "prosody_density_metadata_adapter",
        "safety_redaction_trace_metadata_adapter",
        "rollback", "verify",
        "Phase 21",
        "local_secret_handoff",
        ".wav", ".mp3",
        "2814", "2518", "26", "52", "90",
    ):
        _check(f"C::readme_contains::{needle}",
               needle in body, needle)
    sections = readme.get("sections") or []
    for s in ("plain_language_boundary_summary",
              "adapter_allowlist", "forbidden_actions",
              "verification_workflow", "commit_safety",
              "phase21_import_status", "phase_by_phase",
              "rollback"):
        _check(f"C::section::{s}", s in sections, s)
    # Section-builders return non-empty strings
    for fn_name in (
        "create_plain_language_boundary_summary",
        "create_adapter_allowlist_section",
        "create_forbidden_actions_section",
        "create_verification_workflow_section",
        "create_commit_safety_section",
        "create_phase21_import_status_section",
    ):
        s = getattr(rd, fn_name)()
        _check(f"C::builder_nonempty::{fn_name}",
               isinstance(s, str) and len(s) > 50)
    # Write the README to disk under governance_phase38/readmes/
    out_md = (_ROOT / "bilingual_stack" / "governance_phase38"
                   / "readmes"
                   / "OPERATOR_GOVERNANCE_README_PHASE27_37.md")
    path = rd.write_operator_governance_readme(
        readme, str(out_md))
    _check("C::readme_written",
           Path(path).exists() and Path(path).is_file())
    text = Path(path).read_text(encoding="utf-8")
    _check("C::readme_disk_contains_phase37",
           "Phase 37" in text)


def suite_d_verification_checklist() -> None:
    import bilingual_voice_phase38_verification_checklist as vc
    cl = vc.create_verification_checklist()
    _check("D::checklist_is_dict", isinstance(cl, dict))
    val = vc.validate_verification_checklist(cl)
    _check("D::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = vc.validate_verification_checklist("notdict")
    _check("D::validate_rejects_non_dict",
           bad.get("ok") is False)
    cats = cl.get("categories") or []
    for must in ("git_status", "no_audio_files",
                  "no_secret_leakage",
                  "local_secret_handoff_gitignore",
                  "production_db_unchanged",
                  "manifest_count_unchanged",
                  "no_tts_imports",
                  "no_subprocess_powershell_sapi_piper",
                  "rollback_readiness"):
        _check(f"D::category::{must}", must in cats, must)
    qcc = cl.get("quick_check_commands") or []
    _check("D::quick_commands_len_3+",
           len(qcc) >= 3, str(len(qcc)))
    for c in qcc:
        _check("D::qcc_has_name", bool(c.get("name")))
        _check("D::qcc_has_command", bool(c.get("command")))
        _check("D::qcc_has_expect", bool(c.get("expect")))
    deep = cl.get("deep_check_steps") or []
    _check("D::deep_steps_len_10+",
           len(deep) >= 10, str(len(deep)))
    fts = cl.get("failure_triage_steps") or []
    _check("D::triage_len_5+", len(fts) >= 5, str(len(fts)))
    triage_codes = {t.get("code") for t in fts}
    for code in ("harness_fail", "invariant_drift",
                  "audio_file_present", "secret_leak",
                  "local_secret_handoff_leaked_to_git"):
        _check(f"D::triage_code::{code}",
               code in triage_codes, code)
    # Section-builders return correct types
    _check("D::create_quick_check_commands_returns_list",
           isinstance(vc.create_quick_check_commands(), list))
    _check("D::create_deep_check_steps_returns_list",
           isinstance(vc.create_deep_check_steps(), list))
    _check("D::create_failure_triage_steps_returns_list",
           isinstance(vc.create_failure_triage_steps(), list))
    # Write to disk
    out = (_ROOT / "bilingual_stack" / "governance_phase38"
                / "checklists"
                / "OPERATOR_VERIFICATION_CHECKLIST.json")
    path = vc.write_verification_checklist(cl, str(out))
    _check("D::checklist_written", Path(path).exists())
    loaded = json.loads(out.read_text(encoding="utf-8"))
    _check("D::checklist_disk_phase",
           loaded.get("phase", "").startswith("phase38"))


def suite_e_rollback_matrix() -> None:
    import bilingual_voice_phase38_rollback_matrix as rm
    mtx = rm.create_rollback_matrix()
    _check("E::matrix_is_dict", isinstance(mtx, dict))
    _check("E::auto_destructive_false",
           mtx.get("auto_destructive_commands_executed")
           is False)
    val = rm.validate_rollback_matrix(mtx)
    _check("E::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = rm.validate_rollback_matrix(
        {"matrix_id": "x", "created_at": 1, "phase": "x",
         "general_guidance": "x", "phases": {},
         "auto_destructive_commands_executed": True})
    _check("E::validate_rejects_auto_destructive_true",
           bad.get("ok") is False
           and any("auto_destructive" in r
                   for r in bad.get("reasons", [])))
    phases = mtx.get("phases") or {}
    for p in range(27, 38):
        _check(f"E::phase_{p}_present",
               p in phases or str(p) in phases)
        entry = phases.get(p) or phases.get(str(p)) or {}
        _check(f"E::phase_{p}_title",
               bool(entry.get("title")))
        files = entry.get("files_created") or []
        _check(f"E::phase_{p}_files_9",
               len(files) == 9, f"{p}:{len(files)}")
        _check(f"E::phase_{p}_db_impact_zero",
               entry.get("db_impact") == "zero")
        _check(f"E::phase_{p}_prior_phases_green",
               entry.get("prior_phases_green_without_it")
               is True)
        _check(f"E::phase_{p}_files_modified_empty",
               entry.get("files_modified") == [])
    groups = rm.create_phase_file_groups()
    _check("E::file_groups_len_11", len(groups) == 11)
    s = rm.summarize_rollback_steps(mtx)
    _check("E::summary_ok", s.get("ok") is True)
    # Write to disk
    out = (_ROOT / "bilingual_stack" / "governance_phase38"
                / "rollback" / "ROLLBACK_MATRIX.json")
    path = rm.write_rollback_matrix(mtx, str(out))
    _check("E::matrix_written", Path(path).exists())


def suite_f_commit_safety_audit() -> None:
    import bilingual_voice_phase38_commit_safety_audit as cs
    safe = cs.get_commit_safe_patterns()
    forb = cs.get_commit_forbidden_patterns()
    for k in ("safe_source", "safe_report", "safe_folder"):
        _check(f"F::safe_has::{k}", k in safe)
    for k in ("forbidden_file_extensions",
              "forbidden_path_tokens",
              "skipped_runtime_patterns"):
        _check(f"F::forb_has::{k}", k in forb)
    sample = [
        "?? bilingual_voice_phase38_status_dashboard.py",
        "?? test_phase38_operator_governance_docs.py",
        "?? PHASE38_OPERATOR_GOVERNANCE_README_REPORT.md",
        "?? bilingual_stack/voice_adapter_phase37/",
        "?? bilingual_stack/voice_adapter_phase36/"
            "local_secret_handoff/.gitignore",
        "?? bilingual_stack/voice_adapter_phase36/"
            "local_secret_handoff/seal_test.json",
        "?? lexicon/luna_vocabulary.sqlite",
        "?? bilingual_stack/bilingual_links.sqlite",
        "?? ruvector.db",
        "?? .claude/settings.local.json",
        "?? corpus_sources/backups/snap.tar.gz",
        "?? corpus_sources/quality_samples/x.txt",
        "?? corpus_sources/phase20/ledger.sqlite3",
        "?? random_voice_render.wav",
        "?? captured_clip.mp3",
        "?? mysterious_file.bin",
    ]
    cls = cs.classify_git_status_items(sample)
    _check("F::classify_dict", isinstance(cls, dict))
    items = cls.get("classifications") or []
    by_path = {c["path"]: c for c in items}
    _check("F::source_safe",
           by_path[
               "bilingual_voice_phase38_status_dashboard.py"
           ]["category"] == "safe_source")
    _check("F::harness_safe",
           by_path[
               "test_phase38_operator_governance_docs.py"
           ]["category"] == "safe_source")
    _check("F::report_safe",
           by_path[
               "PHASE38_OPERATOR_GOVERNANCE_README_REPORT.md"
           ]["category"] == "safe_report")
    _check("F::voice_adapter_folder_safe",
           by_path[
               "bilingual_stack/voice_adapter_phase37/"
           ]["category"] == "safe_folder")
    _check("F::gitignore_safe",
           by_path[
               "bilingual_stack/voice_adapter_phase36/"
               "local_secret_handoff/.gitignore"
           ]["category"] == "safe_gitignore")
    _check("F::handoff_seal_forbidden",
           by_path[
               "bilingual_stack/voice_adapter_phase36/"
               "local_secret_handoff/seal_test.json"
           ]["category"] == "forbidden_path_token")
    _check("F::runtime_db_skipped",
           by_path["lexicon/luna_vocabulary.sqlite"]
           ["category"] == "skipped_runtime")
    _check("F::links_db_skipped",
           by_path["bilingual_stack/bilingual_links.sqlite"]
           ["category"] == "skipped_runtime")
    _check("F::ruvector_skipped",
           by_path["ruvector.db"]["category"]
           == "skipped_runtime")
    _check("F::claude_skipped",
           by_path[".claude/settings.local.json"]["category"]
           == "skipped_runtime")
    _check("F::corpus_backups_skipped",
           by_path["corpus_sources/backups/snap.tar.gz"]
           ["category"] == "skipped_runtime")
    _check("F::corpus_quality_skipped",
           by_path["corpus_sources/quality_samples/x.txt"]
           ["category"] == "skipped_runtime")
    _check("F::corpus_ledger_skipped",
           by_path["corpus_sources/phase20/ledger.sqlite3"]
           ["category"] == "skipped_runtime")
    _check("F::wav_forbidden",
           by_path["random_voice_render.wav"]["category"]
           == "forbidden_extension")
    _check("F::mp3_forbidden",
           by_path["captured_clip.mp3"]["category"]
           == "forbidden_extension")
    _check("F::unclassified",
           by_path["mysterious_file.bin"]["category"]
           == "unclassified")
    audit = cs.audit_commit_safety(sample)
    _check("F::audit_is_dict", isinstance(audit, dict))
    _check("F::audit_forbidden_count>=3",
           audit.get("forbidden_count", 0) >= 3,
           str(audit.get("forbidden_count")))
    _check("F::audit_total_matches",
           audit.get("total_items") == len(sample))
    _check("F::audit_ok_false_when_forbidden_present",
           audit.get("ok") is False)
    # Clean audit (only safe items) should be ok=True
    clean = cs.audit_commit_safety([
        "?? PHASE38_OPERATOR_GOVERNANCE_README_REPORT.md",
        "?? bilingual_voice_phase38_status_dashboard.py",
    ])
    _check("F::audit_ok_when_only_safe",
           clean.get("ok") is True)
    s = cs.summarize_commit_safety(audit)
    _check("F::summary_returned", isinstance(s, dict))
    # Write to disk
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "audit.json"
        path = cs.write_commit_safety_report(audit, str(out))
        _check("F::audit_written", Path(path).exists())


def suite_g_status_dashboard() -> None:
    import bilingual_voice_phase38_status_dashboard as sd
    dash = sd.create_governance_status_dashboard()
    _check("G::dashboard_is_dict", isinstance(dash, dict))
    val = sd.validate_governance_status_dashboard(dash)
    _check("G::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = sd.validate_governance_status_dashboard("notdict")
    _check("G::validate_rejects_non_dict",
           bad.get("ok") is False)
    # Adapter count + latest phase + invariants
    _check("G::adapter_count_4",
           dash.get("adapter_count") == 4)
    _check("G::latest_phase_37",
           dash.get("latest_phase") == 37)
    _check("G::regression_6185",
           dash.get("full_regression_total_expected") == 6185)
    _check("G::harness_count_30",
           dash.get("harness_count_expected") == 30,
           str(dash.get("harness_count_expected")))
    allowed = dash.get("allowed_callable_adapters") or []
    for a in ("dummy_metadata_adapter",
              "bilingual_segment_metadata_adapter",
              "prosody_density_metadata_adapter",
              "safety_redaction_trace_metadata_adapter"):
        _check(f"G::adapter_listed::{a}", a in allowed)
    blocked = dash.get("blocked_boundaries") or []
    for b in ("audio_generation", "tts_invocation",
              "subprocess_execution", "powershell_invocation",
              "sapi_invocation", "piper_invocation",
              "audio_file_write", "network_call",
              "socket_open", "multiprocessing",
              "main_runtime_integration",
              "program_s_modification",
              "tier_probe_attestation_modification",
              "worker_or_luna_modules_modification",
              "production_signing_secret_storage",
              "git_commit_of_signing_secret"):
        _check(f"G::blocked::{b}", b in blocked, b)
    pc = dash.get("production_counts") or {}
    for k, v in (("english_words", 2814),
                  ("russian_words", 2518),
                  ("russian_phrases", 35),
                  ("bilingual_concepts", 26),
                  ("bilingual_entry_links", 52),
                  ("live_pack_manifests", 90)):
        _check(f"G::pc::{k}", pc.get(k) == v, str(pc.get(k)))
    cis = str(dash.get("corpus_import_status") or "")
    _check("G::corpus_blocked_marker_present",
           "BLOCKED" in cis)
    nxt = dash.get("next_recommended_phases") or []
    _check("G::next_phases_3", len(nxt) >= 3)
    # Validator catches drift
    drift = dict(dash)
    drift["production_counts"] = dict(pc)
    drift["production_counts"]["english_words"] = 9999
    bad2 = sd.validate_governance_status_dashboard(drift)
    _check("G::validator_catches_drift",
           bad2.get("ok") is False)
    drift2 = dict(dash)
    drift2["corpus_import_status"] = "all fine"
    bad3 = sd.validate_governance_status_dashboard(drift2)
    _check("G::validator_catches_unblock_text",
           bad3.get("ok") is False)
    md = sd.create_dashboard_markdown(dash)
    _check("G::markdown_nonempty",
           isinstance(md, str) and len(md) > 400)
    for needle in ("Latest phase", "Full regression",
                    "Allowed callable adapters",
                    "Blocked boundaries", "BLOCKED",
                    "2814", "2518"):
        _check(f"G::md_contains::{needle}",
               needle in md, needle)
    # Write JSON + MD to disk
    base = (_ROOT / "bilingual_stack" / "governance_phase38"
                 / "dashboards")
    out_json = base / "GOVERNANCE_STATUS_DASHBOARD.json"
    out_md = base / "GOVERNANCE_STATUS_DASHBOARD.md"
    p1 = sd.write_governance_status_dashboard(dash, str(out_json))
    p2 = sd.write_governance_status_markdown(md, str(out_md))
    _check("G::json_written", Path(p1).exists())
    _check("G::md_written", Path(p2).exists())


def suite_h_integrity_sweep() -> None:
    import bilingual_voice_phase38_integrity_sweep as isw
    rep = isw.create_integrity_sweep_report(_ROOT)
    _check("H::report_is_dict", isinstance(rep, dict))
    val = isw.validate_integrity_sweep_report(rep)
    _check("H::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = isw.validate_integrity_sweep_report("notdict")
    _check("H::validate_rejects_non_dict",
           bad.get("ok") is False)
    # Reports must be present per pre-flight already
    rp = rep.get("phase_reports") or {}
    _check("H::reports_ok", rp.get("ok") is True,
           ",".join(rp.get("missing", [])))
    _check("H::reports_present_13",
           rp.get("present_count") == 13,
           str(rp.get("present_count")))
    # No audio under voice_adapter_phase*/
    aud = rep.get("audio_artifacts") or {}
    _check("H::no_audio", aud.get("ok") is True,
           ",".join(aud.get("audio_files", [])))
    # No runtime DB inside governance_phase38 / voice_adapter_phase*
    dbs = rep.get("runtime_db_artifacts") or {}
    _check("H::no_runtime_db", dbs.get("ok") is True,
           ",".join(dbs.get("db_files", [])))
    # No secret leakage in public artifacts
    sec = rep.get("secret_leakage") or {}
    _check("H::no_secret_leakage", sec.get("ok") is True,
           json.dumps(sec.get("hits", []))[:300])
    # No forbidden imports in Phase 27-38 source
    fi = rep.get("forbidden_imports") or {}
    _check("H::no_forbidden_imports", fi.get("ok") is True,
           json.dumps(fi.get("hits", []))[:300])
    # Local-secret-handoff has its .gitignore
    gi = rep.get("local_secret_handoff_gitignore") or {}
    _check("H::gi_present",
           gi.get("gitignore_present") is True)
    _check("H::gi_matches",
           gi.get("gitignore_matches_expected") is True)
    _check("H::overall_ok", rep.get("ok") is True)
    # Write to disk
    out = (_ROOT / "bilingual_stack" / "governance_phase38"
                / "integrity" / "INTEGRITY_SWEEP.json")
    path = isw.write_integrity_sweep_report(rep, str(out))
    _check("H::sweep_written", Path(path).exists())


def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = _ROOT / "russian_stack" / "russian_lexicon.sqlite"
    link_db = _ROOT / "bilingual_stack" / "bilingual_links.sqlite"
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_2814", n == 2814, f"got {n}")
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute(
            "SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute(
            "SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("I::ru_2518", nw == 2518, f"got {nw}")
        _check("I::ru_phr_35", np_ == 35, f"got {np_}")
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("I::concepts_26", nc >= 26)
        _check("I::links_52", nl >= 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    _check("I::manifests_90", len(live) == 90, str(len(live)))
    # No audio anywhere under governance_phase38/
    audio = []
    base = _ROOT / "bilingual_stack" / "governance_phase38"
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_governance_phase38",
           not audio, ",".join(audio))


def suite_j_isolation() -> None:
    # NOTE: This harness file itself is intentionally excluded from
    # the J-suite scan -- it is the scanner, and necessarily lists
    # the forbidden-token strings as scan targets. The integrity
    # sweep covers the module files directly.
    files = [f"{m}.py" for m in _PHASE38_MODULES]
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", "comtypes", "win32com",
    )
    forbidden_exec = (
        "subprocess.run", "subprocess.Popen", "subprocess.call",
        "os.system(", "shell=True", "os.popen",
        "ctypes.windll", "powershell ", "powershell.exe",
    )
    forbidden_net = (
        "urllib.request", "http.client", "requests.", "httpx.",
        "socket.socket",
    )
    forbidden_runtime = (
        "luna_modules", "import worker", "from worker",
        "tier_progression", "probe_attestation",
        "attestation_signer",
    )
    forbidden_threading = (
        "threading.Thread", "multiprocessing.Process",
        "multiprocessing.Pool",
        "daemon=True", "asyncio.create_task", "schedule.every",
    )
    for fn in files:
        p = _ROOT / fn
        if not p.exists():
            _check(f"J::file_exists::{fn}", False, fn)
            continue
        src = p.read_text(encoding="utf-8")
        for tok in forbidden_audio:
            _check(f"J::{fn}::no_audio:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_exec:
            _check(f"J::{fn}::no_exec:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_net:
            _check(f"J::{fn}::no_net:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_runtime:
            _check(f"J::{fn}::no_runtime:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_threading:
            _check(f"J::{fn}::no_daemon:{tok.strip()}",
                   tok not in src)
    # Secret-leakage scan over governance_phase38/ output dirs
    import bilingual_voice_phase36_secret_boundary as sb
    scan_dirs = [
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "readmes",
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "checklists",
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "rollback",
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "dashboards",
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "integrity",
        _ROOT / "bilingual_stack" / "governance_phase38"
              / "reports",
    ]
    for d in scan_dirs:
        if not d.exists():
            _check(f"J::leak_scan_dir_present:{d.name}",
                   True)
            continue
        scan = sb.validate_no_secret_leakage_in_directory(str(d))
        _check(f"J::no_leak_in:{d.name}",
               scan["ok"],
               json.dumps(scan.get("leaks", []))[:200])


def suite_k_regression_smoke() -> None:
    # Re-import every Phase 27-37 callable runtime + every Phase 38
    # module. Confirms no Phase 38 source broke a prior import.
    upstream_runtimes = [
        "bilingual_voice_adapter_phase28_runtime",
        "bilingual_voice_adapter_phase29_runtime",
        "bilingual_voice_adapter_phase30_runtime",
        "bilingual_voice_adapter_phase31_runtime",
        "bilingual_voice_adapter_phase33_runtime",
        "bilingual_voice_phase34_export_runtime",
        "bilingual_voice_phase35_exchange_runtime",
        "bilingual_voice_phase36_handoff_runtime",
        "bilingual_voice_adapter_phase37_runtime",
    ]
    for m in upstream_runtimes:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    # Re-import Phase 38 modules a second time (idempotency)
    for m in _PHASE38_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    # End-to-end: ledger + readme + checklist + matrix + dashboard
    # + integrity sweep all execute without exception in sequence
    try:
        import bilingual_voice_phase38_governance_ledger as gl
        import bilingual_voice_phase38_operator_readme as rd
        import bilingual_voice_phase38_verification_checklist \
            as vc
        import bilingual_voice_phase38_rollback_matrix as rm
        import bilingual_voice_phase38_status_dashboard as sd
        import bilingual_voice_phase38_integrity_sweep as isw
        ledger = gl.build_boundary_guarantee_ledger(_ROOT)
        readme = rd.create_operator_governance_readme(ledger)
        checklist = vc.create_verification_checklist()
        matrix = rm.create_rollback_matrix()
        dash = sd.create_governance_status_dashboard()
        sweep = isw.create_integrity_sweep_report(_ROOT)
        _check("K::e2e_ledger_ok",
               gl.validate_boundary_guarantee_ledger(ledger)
               .get("ok") is True)
        _check("K::e2e_readme_body",
               bool(readme.get("body")))
        _check("K::e2e_checklist_ok",
               vc.validate_verification_checklist(checklist)
               .get("ok") is True)
        _check("K::e2e_matrix_ok",
               rm.validate_rollback_matrix(matrix).get("ok")
               is True)
        _check("K::e2e_dashboard_ok",
               sd.validate_governance_status_dashboard(dash)
               .get("ok") is True)
        _check("K::e2e_sweep_ok",
               isw.validate_integrity_sweep_report(sweep)
               .get("ok") is True)
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_governance_ledger),
        ("C", suite_c_operator_readme),
        ("D", suite_d_verification_checklist),
        ("E", suite_e_rollback_matrix),
        ("F", suite_f_commit_safety_audit),
        ("G", suite_g_status_dashboard),
        ("H", suite_h_integrity_sweep),
        ("I", suite_i_production_safety),
        ("J", suite_j_isolation),
        ("K", suite_k_regression_smoke),
    ]
    for name, fn in suites:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            _check(f"{name}::suite_uncaught", False, str(e))
    print(f"Total: {_TOTAL} | Pass: {_PASS} | Fail: {_FAIL}")
    if _FAILURES:
        print("--- failures ---")
        for f in _FAILURES[:80]:
            print(f)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
