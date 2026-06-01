"""Audit: how many concepts in `bilingual_concept_links.sqlite` have
mismatched POS classes between their EN and RU entry_links?

The first semantic sample pass (`bilingual_main_semantic_sample_pass`,
threshold 0.65, no POS alignment) is known to have a noise tail. This
script characterizes the noise quantitatively:

  - For each concept, fetch its EN entry_link.part_of_speech AND its
    RU entry_link.part_of_speech.
  - Canonicalize via the same POS_CLASS_MAP used in
    `bilingual_main_semantic_pos_aligned_pass`.
  - Classify: matched-class | cross-class | unknown-class.

Outputs a JSON report with counts per pass + per POS-pair combination.
Pure read-only — no DB mutation. Future cleanup can use this report
to decide on a delete / archive / annotate policy.
"""
from __future__ import annotations

import collections
import json
import sqlite3
import time
from pathlib import Path

from bilingual_main_semantic_pos_aligned_pass import POS_CLASS_MAP

CONCEPT_DB = Path(__file__).resolve().parent / "bilingual_concept_links.sqlite"
REPORTS_DIR = (Path(__file__).resolve().parent / "memory"
               / "bilingual_main_reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _pos_class(pos: str) -> str:
    return POS_CLASS_MAP.get((pos or "").strip().lower(), "other")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def audit_cross_class() -> dict:
    conn = sqlite3.connect(str(CONCEPT_DB), timeout=10.0)
    conn.row_factory = sqlite3.Row

    by_pass: dict[str, collections.Counter] = {}
    cross_class_concepts: list[dict] = []
    sample_size = 8  # cap how many examples per pass we record
    samples_recorded: dict[str, int] = collections.Counter()
    total = 0
    no_links = 0
    try:
        # Pull each concept once with its EN + RU entry_links.
        for c in conn.execute("SELECT concept_id, canonical_label_en, "
                                "canonical_label_ru, notes FROM concepts"):
            cid = c["concept_id"]
            total += 1
            en_pos, ru_pos = "", ""
            for r in conn.execute(
                    "SELECT language, part_of_speech FROM entry_links "
                    "WHERE concept_id = ?", (cid,)):
                if r["language"] == "en":
                    en_pos = r["part_of_speech"] or ""
                elif r["language"] == "ru":
                    ru_pos = r["part_of_speech"] or ""
            if not en_pos and not ru_pos:
                no_links += 1
                continue
            en_class = _pos_class(en_pos)
            ru_class = _pos_class(ru_pos)
            # Determine which pass this concept came from via notes prefix.
            notes = c["notes"] or ""
            if notes.startswith("main_latin_extract"):
                pass_name = "latin_extract"
            elif notes.startswith("main_semantic_pos_aligned"):
                pass_name = "semantic_pos_aligned"
            elif notes.startswith("main_semantic_sample"):
                pass_name = "semantic_unfiltered"
            else:
                pass_name = "other"
            by_pass.setdefault(pass_name, collections.Counter())
            if not en_class or not ru_class:
                bucket = "unknown_class"
            elif en_class == ru_class:
                bucket = f"matched:{en_class}"
            else:
                bucket = f"cross:{en_class}->{ru_class}"
            by_pass[pass_name][bucket] += 1
            # Record a few cross-class examples for the report.
            if (en_class and ru_class and en_class != ru_class
                    and samples_recorded[pass_name] < sample_size):
                cross_class_concepts.append({
                    "pass": pass_name,
                    "en_label": c["canonical_label_en"],
                    "ru_label": c["canonical_label_ru"],
                    "en_pos": en_pos,
                    "ru_pos": ru_pos,
                    "en_class": en_class,
                    "ru_class": ru_class,
                    "notes": notes,
                })
                samples_recorded[pass_name] += 1
    finally:
        conn.close()

    summary: dict = {
        "ts": _now_iso(),
        "concept_db": str(CONCEPT_DB),
        "total_concepts": total,
        "no_link_records": no_links,
        "by_pass": {},
        "cross_class_samples": cross_class_concepts,
    }
    for pass_name, counter in by_pass.items():
        matched = sum(v for k, v in counter.items()
                      if k.startswith("matched:"))
        cross = sum(v for k, v in counter.items()
                    if k.startswith("cross:"))
        unknown = counter.get("unknown_class", 0)
        total_p = matched + cross + unknown
        summary["by_pass"][pass_name] = {
            "total": total_p,
            "matched_class": matched,
            "cross_class": cross,
            "unknown_class": unknown,
            "cross_class_pct": round(100.0 * cross / max(total_p, 1), 1),
            "buckets": dict(counter.most_common(20)),
        }
    return summary


if __name__ == "__main__":
    rep = audit_cross_class()
    out_path = REPORTS_DIR / (
        f"bilingual_concept_audit_cross_class_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(rep, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    # Compact stdout summary
    print(json.dumps({
        "total_concepts": rep["total_concepts"],
        "by_pass_summary": {
            p: {
                "total": d["total"],
                "matched_class": d["matched_class"],
                "cross_class": d["cross_class"],
                "cross_class_pct": d["cross_class_pct"],
            } for p, d in rep["by_pass"].items()
        },
    }, indent=2, ensure_ascii=False))
    print(f"\nfull report -> {out_path}")
