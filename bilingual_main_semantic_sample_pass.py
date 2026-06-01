"""Semantic-similarity bilingual linker at SAMPLE scale on main.

A scoped version of the worktree's `bilingual_semantic_link_pass.py`,
ported to main's 1M+1M corpus via `bilingual_main_adapter`. Samples N
unlinked EN rows + M unlinked RU rows (deterministic via ORDER BY id +
LIMIT/OFFSET), embeds both with `paraphrase-multilingual-MiniLM-L12-v2`,
finds nearest EN per RU by cosine, threshold-gates, writes pairs.

Why sample
==========
A full 1M × 1M embedding pass would take ~6 hours of CPU. A 5k × 5k
sample takes ~3 minutes total (load model ~5s + embed 10k texts ~30s
+ similarity matrix is trivial at 5k × 5k). The sample demonstrates
yield, proves the integration end-to-end, and produces a real
incremental result. A full pass is the natural next session.

Doctrine
========
- Main DB opened READ-ONLY (sqlite URI mode=ro).
- Writes only to `bilingual_concept_links.sqlite` (separate file).
- Dedupes against existing entry_links on insert; safe to re-run.
- Confidence = clamp(0.55, cosine * 0.85, 0.80) — semantic guesses,
  lower than the exact_match pass's 0.9.
- link_method = "heuristic" (pre-blessed in valid set).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bilingual_main_adapter as _mainadapt
import bilingual_concept_links_db as _cdb

MAIN_DB = _mainadapt.MAIN_DB_PATH
CONCEPT_DB = _cdb.CONCEPT_DB_PATH
REPORTS_DIR = Path(__file__).resolve().parent / "memory" / "bilingual_main_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_THRESHOLD = 0.65
DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_unlinked_sample(language: str, sample_size: int,
                            existing_linked: set[str]
                            ) -> List[Dict[str, Any]]:
    """Stream main's table, take the first ``sample_size`` rows whose
    word is NOT already linked. Returns adapter-shape dicts."""
    table = "english_words" if language == "en" else "russian_words"
    conn = sqlite3.connect(
        f"file:{MAIN_DB.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    out: List[Dict[str, Any]] = []
    try:
        cursor = conn.execute(f"SELECT * FROM {table} ORDER BY id")
        for r in cursor:
            d = dict(r)
            w = (d.get("word") or "").strip()
            if not w:
                continue
            if w.lower() in existing_linked:
                continue
            out.append(d)
            if len(out) >= sample_size:
                break
    finally:
        conn.close()
    return out


def _en_text(r: Dict[str, Any]) -> str:
    """Text fed to encoder for an EN row. word + first sentence of def."""
    parts = [str(r.get("word") or "")]
    defn = str(r.get("definition") or "").strip()
    if defn:
        # First 200 chars give enough context without overweighting long defs.
        parts.append(defn[:200])
    return ". ".join(p for p in parts if p)


def _ru_text(r: Dict[str, Any]) -> str:
    parts = [str(r.get("word") or "")]
    defn = str(r.get("definition") or "").strip()
    if defn:
        parts.append(defn[:200])
    return ". ".join(p for p in parts if p)


def semantic_sample_pass(
    en_sample: int = 5000,
    ru_sample: int = 5000,
    threshold: float = DEFAULT_THRESHOLD,
    model_name: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    t0 = time.monotonic()
    report: Dict[str, Any] = {
        "ts": _now_iso(),
        "pass": "main_semantic_sample",
        "main_db": str(MAIN_DB),
        "concept_db": str(CONCEPT_DB),
        "en_sample": en_sample,
        "ru_sample": ru_sample,
        "threshold": threshold,
        "model": model_name,
        "report_version": 1,
    }
    print(f"loading existing linked-words set...", flush=True)
    en_seen, ru_seen = _cdb.existing_linked_words()
    report["pre_en_seen"] = len(en_seen)
    report["pre_ru_seen"] = len(ru_seen)

    print(f"sampling {en_sample:,} unlinked EN rows...", flush=True)
    t_load = time.monotonic()
    en_rows = _load_unlinked_sample("en", en_sample, en_seen)
    report["en_sampled"] = len(en_rows)
    print(f"  {len(en_rows):,} EN rows in {time.monotonic()-t_load:.1f}s",
          flush=True)

    print(f"sampling {ru_sample:,} unlinked RU rows...", flush=True)
    t_load = time.monotonic()
    ru_rows = _load_unlinked_sample("ru", ru_sample, ru_seen)
    report["ru_sampled"] = len(ru_rows)
    print(f"  {len(ru_rows):,} RU rows in {time.monotonic()-t_load:.1f}s",
          flush=True)

    if not en_rows or not ru_rows:
        report["ok"] = True
        report["created_concepts"] = 0
        report["note"] = "no_samples"
        report["elapsed_s"] = round(time.monotonic() - t0, 2)
        return report

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except Exception as exc:  # noqa: BLE001
        report["ok"] = False
        report["error"] = f"import_failed: {type(exc).__name__}: {exc}"
        report["elapsed_s"] = round(time.monotonic() - t0, 2)
        return report

    print(f"loading model {model_name}...", flush=True)
    t_model = time.monotonic()
    model = SentenceTransformer(model_name)
    report["model_load_s"] = round(time.monotonic() - t_model, 2)

    en_texts = [_en_text(r) for r in en_rows]
    ru_texts = [_ru_text(r) for r in ru_rows]
    print(f"embedding {len(en_texts):,} EN + {len(ru_texts):,} RU rows...",
          flush=True)
    t_emb = time.monotonic()
    en_emb = model.encode(en_texts, convert_to_numpy=True,
                           show_progress_bar=False, batch_size=64,
                           normalize_embeddings=True)
    ru_emb = model.encode(ru_texts, convert_to_numpy=True,
                           show_progress_bar=False, batch_size=64,
                           normalize_embeddings=True)
    report["embed_s"] = round(time.monotonic() - t_emb, 2)

    # Cosine similarity matrix (5k × 5k = 100MB at float32 — fine).
    print(f"computing similarity matrix {len(ru_emb):,} × {len(en_emb):,}...",
          flush=True)
    t_sim = time.monotonic()
    sim = ru_emb @ en_emb.T
    best_en_idx = sim.argmax(axis=1)
    best_sim = sim.max(axis=1)
    report["sim_compute_s"] = round(time.monotonic() - t_sim, 2)

    # Greedy: highest-sim first, each EN used at most once.
    pair_candidates: List[Tuple[float, int, int]] = []
    for ru_idx, (en_idx, s) in enumerate(zip(best_en_idx, best_sim)):
        s_f = float(s)
        if s_f < threshold:
            continue
        pair_candidates.append((s_f, int(ru_idx), int(en_idx)))
    pair_candidates.sort(key=lambda x: -x[0])
    report["candidates_above_threshold"] = len(pair_candidates)

    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    used_en: set[int] = set()
    register_skipped = 0
    for sim_val, ru_idx, en_idx in pair_candidates:
        if en_idx in used_en:
            continue
        en = en_rows[en_idx]
        r = ru_rows[ru_idx]
        e_word = (en.get("word") or "").strip()
        r_word = (r.get("word") or "").strip()
        if not e_word or not r_word:
            continue
        # Compose
        cid = _cdb._new_id("concept")
        now = _now_iso()
        try:
            en_cov = set(json.loads(en.get("coverage_categories") or "[]"))
        except Exception:  # noqa: BLE001
            en_cov = set()
        try:
            ru_cov = set(json.loads(r.get("coverage_categories") or "[]"))
        except Exception:  # noqa: BLE001
            ru_cov = set()
        coverage = sorted(en_cov | ru_cov)
        registers = sorted(
            {en.get("register") or "", r.get("register") or ""} - {""})
        conf = _cdb.clamp_confidence(max(0.55, min(0.80, sim_val * 0.85)))
        notes = (f"main_semantic_sample sim={sim_val:.3f} "
                 f"en_id={en.get('id')} ru_id={r.get('id')}")
        concept_rows.append((
            cid, e_word, r_word,
            _cdb.json_list(coverage),
            _cdb.json_list(coverage),
            _cdb.json_list(registers or ["standard"]),
            "[]",
            now, now, notes,
        ))
        link_rows.append((
            _cdb._new_id("link"), cid, "en",
            "main_english_words", str(en.get("id")), e_word, "",
            "", en.get("pos") or "",
            conf, "heuristic", now, notes,
        ))
        link_rows.append((
            _cdb._new_id("link"), cid, "ru",
            "main_russian_words", str(r.get("id")), r_word, "",
            "", r.get("pos") or "",
            conf, "heuristic", now, notes,
        ))
        used_en.add(en_idx)

    report["queued_concepts"] = len(concept_rows)
    report["register_skipped"] = register_skipped

    if concept_rows:
        t_ins = time.monotonic()
        ins = _cdb.insert_concept_batch(
            concept_rows, link_rows,
            audit_message=(f"semantic_sample en_sample={en_sample} "
                            f"ru_sample={ru_sample} "
                            f"threshold={threshold} "
                            f"created={len(concept_rows)}"))
        report["insert"] = ins
        report["insert_s"] = round(time.monotonic() - t_ins, 2)
    else:
        report["insert_skipped"] = "no_pairs_above_threshold"

    report["ok"] = True
    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--en-sample", type=int, default=5000)
    p.add_argument("--ru-sample", type=int, default=5000)
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    args = p.parse_args()
    report = semantic_sample_pass(
        en_sample=args.en_sample,
        ru_sample=args.ru_sample,
        threshold=args.threshold)
    out_path = REPORTS_DIR / (
        f"bilingual_main_semantic_sample_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nreport -> {out_path}")
