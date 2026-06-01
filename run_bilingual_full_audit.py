"""Run the full coverage gap report + Russian morphology audit after a
deep link pass. Writes two consolidated reports under
bilingual_stack/reports/ and prints headline numbers.

Origin: 2026-06-01 operator directive to upgrade EN-RU linking to the
full extent. After bilingual_deep_link_pass.py expanded concept coverage,
this script captures the AFTER state.

Uses only existing Phase 22 / Phase 21A APIs. No new modules.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import bilingual_coverage_gap_reporter as gap
import russian_morphology_upgrade_path as morph

REPORTS_DIR = Path(__file__).resolve().parent / "bilingual_stack" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _ts() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def run_gap_report() -> dict:
    """Aggregate every public gap-reporter function into one report."""
    out = {"ts": _now_iso(), "version": 1}
    try:
        out["linked_concepts"] = gap.count_linked_concepts()
    except Exception as exc:  # noqa: BLE001
        out["linked_concepts_error"] = f"{type(exc).__name__}: {exc}"
    for label, fn in [
        ("links_by_category", lambda: gap.count_links_by_category()),
        ("unlinked_english_by_category",
            lambda: gap.count_unlinked_english_by_category(limit=200)),
        ("unlinked_russian_by_category",
            lambda: gap.count_unlinked_russian_by_category(limit=200)),
        ("category_imbalances",
            lambda: gap.identify_category_imbalances(min_gap=10)),
        ("missing_profession_links",
            lambda: gap.identify_missing_profession_links(limit=50)),
        ("missing_trade_links",
            lambda: gap.identify_missing_trade_links(limit=50)),
        ("missing_poetry_philosophy_links",
            lambda: gap.identify_missing_poetry_philosophy_links(limit=50)),
        ("slang_link_cautions",
            lambda: gap.identify_slang_link_cautions(limit=50)),
    ]:
        try:
            out[label] = fn()
        except Exception as exc:  # noqa: BLE001
            out[f"{label}_error"] = f"{type(exc).__name__}: {exc}"

    out_path = REPORTS_DIR / f"bilingual_coverage_gap_report_{_ts()}.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    out["_report_path"] = str(out_path)
    return out


def run_morphology_audit() -> dict:
    """Run every public morphology-upgrade function."""
    out = {"ts": _now_iso(), "version": 1}
    try:
        out["backend_status"] = morph.get_morphology_backend_status()
    except Exception as exc:  # noqa: BLE001
        out["backend_status_error"] = f"{type(exc).__name__}: {exc}"
    for label, fn in [
        ("backend_detection", morph.detect_morphology_backend),
        ("entry_audit",
            lambda: morph.audit_russian_entries_for_morphology(limit=500)),
    ]:
        try:
            out[label] = fn()
        except Exception as exc:  # noqa: BLE001
            out[f"{label}_error"] = f"{type(exc).__name__}: {exc}"
    for label, fn in [
        ("missing_lemmas",
            lambda: morph.identify_missing_lemmas(limit=200)),
        ("missing_pos",
            lambda: morph.identify_missing_pos(limit=200)),
        ("repair_proposals",
            lambda: morph.propose_morphology_repairs(limit=200)),
    ]:
        try:
            data = fn()
            out[f"{label}_count"] = len(data) if data else 0
            out[f"{label}_sample"] = data[:5] if data else []
        except Exception as exc:  # noqa: BLE001
            out[f"{label}_error"] = f"{type(exc).__name__}: {exc}"

    out_path = REPORTS_DIR / f"russian_morphology_audit_{_ts()}.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False,
                                    default=str), encoding="utf-8")
    out["_report_path"] = str(out_path)
    return out


def headline(gap_report: dict, morph_report: dict) -> dict:
    """Top-level numbers worth surfacing to the operator.

    The keys returned by count_linked_concepts() are `concepts`,
    `english_entry_links`, `russian_entry_links`, `glossary_links`
    (matched to Phase 22 reporter implementation, not the earlier draft
    spec). Likewise backend_status is wrapped: backend.backend.active_backend.
    """
    linked = gap_report.get("linked_concepts") or {}
    backend_outer = morph_report.get("backend_status") or {}
    backend = backend_outer.get("backend") or {}
    return {
        "ts": _now_iso(),
        "concepts": linked.get("concepts"),
        "links_en": linked.get("english_entry_links"),
        "links_ru": linked.get("russian_entry_links"),
        "links_total": ((linked.get("english_entry_links") or 0)
                        + (linked.get("russian_entry_links") or 0)),
        "glossary_links": linked.get("glossary_links"),
        "category_imbalances_count":
            len(gap_report.get("category_imbalances") or []),
        "morphology_active_backend":
            backend.get("active_backend"),
        "morphology_pymorphy3_available":
            backend.get("pymorphy3_available"),
        "morphology_pymorphy2_available":
            backend.get("pymorphy2_available"),
        "morphology_nltk_available":
            backend.get("nltk_available"),
        "morphology_wordfreq_available":
            backend.get("wordfreq_available"),
        "missing_lemmas_count":
            morph_report.get("missing_lemmas_count"),
        "missing_pos_count":
            morph_report.get("missing_pos_count"),
        "repair_proposals_count":
            morph_report.get("repair_proposals_count"),
        "gap_report_path": gap_report.get("_report_path"),
        "morphology_report_path": morph_report.get("_report_path"),
    }


if __name__ == "__main__":
    print("=== gap report ===")
    g = run_gap_report()
    print("=== morphology audit ===")
    m = run_morphology_audit()
    print("=== headline ===")
    print(json.dumps(headline(g, m), indent=2, ensure_ascii=False))
