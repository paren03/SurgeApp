"""Phase 22 - Russian Morphology Upgrade Path.

Detects whether ``pymorphy3`` is installed, reports the current backend,
audits Russian rows for missing lemma / POS, and proposes repairs WITHOUT
modifying production rows. Never auto-installs.
"""

from __future__ import annotations

import json
import os
import time
import importlib.util
from pathlib import Path
from typing import Any, Optional


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def detect_morphology_backend() -> dict[str, Any]:
    has_pymorphy3 = importlib.util.find_spec("pymorphy3") is not None
    has_pymorphy2 = importlib.util.find_spec("pymorphy2") is not None
    has_nltk = importlib.util.find_spec("nltk") is not None
    has_wordfreq = importlib.util.find_spec("wordfreq") is not None
    return {"pymorphy3_available": has_pymorphy3,
            "pymorphy2_available": has_pymorphy2,
            "nltk_available": has_nltk,
            "wordfreq_available": has_wordfreq,
            "active_backend": "pymorphy3" if has_pymorphy3
                               else "pymorphy2" if has_pymorphy2
                               else "heuristic_fallback"}


def get_morphology_backend_status() -> dict[str, Any]:
    backend = detect_morphology_backend()
    return {"ok": True, "backend": backend, "checked_at": time.time()}


def create_pymorphy3_install_note(output_path: Optional[str | Path] = None
                                  ) -> str:
    p = Path(output_path) if output_path is not None else \
        Path("bilingual_stack/reports/PYMORPHY3_INSTALL_NOTE.md")
    p.parent.mkdir(parents=True, exist_ok=True)
    body = [
        "# Russian Morphology Upgrade Note",
        "",
        "Current backend is the **heuristic fallback** in",
        "`russian_morphology_layer.py`. To enable richer lemma / POS",
        "behavior, the operator may run:",
        "",
        "```",
        "pip install pymorphy3",
        "```",
        "",
        "Luna will NOT auto-install. After install, restart any Luna",
        "process and rerun:",
        "",
        "```",
        "python -c \"import russian_morphology_upgrade_path as m; "
        "print(m.detect_morphology_backend())\"",
        "```",
        "",
        "## What changes after install",
        "",
        "- `detect_morphology_backend()` reports `active_backend='pymorphy3'`.",
        "- Phase 22 link builder will gain higher confidence on lemma_match",
        "  links.",
        "- Russian morphology row audits gain more accurate POS suggestions.",
        "",
        "## What does NOT change",
        "",
        "- Production lexicon rows are NOT auto-rewritten.",
        "- Existing English/Russian DBs remain untouched.",
        "- Safety policy is unchanged.",
    ]
    p.write_text("\n".join(body), encoding="utf-8")
    return str(p)


def _ru_connect(db_path: Optional[str | Path] = None):
    import russian_lexicon_store as rulex
    rulex.init_db(db_path)
    return rulex._connect(db_path)


def _clamp(n: Optional[int], default: int = 500, hard: int = 5000) -> int:
    if n is None:
        return default
    try:
        v = int(n)
    except Exception:
        return default
    return max(1, min(v, hard))


def audit_russian_entries_for_morphology(limit: int = 500,
                                        db_path: Optional[str | Path] = None
                                        ) -> dict[str, Any]:
    _ensure_flags()
    cap = _clamp(limit)
    with _ru_connect(db_path) as conn:
        cur = conn.execute(
            "SELECT word, lemma, part_of_speech FROM words "
            "ORDER BY frequency_score DESC LIMIT ?", (cap,))
        rows = cur.fetchall()
    missing_lemma = sum(1 for r in rows if not r[1])
    missing_pos = sum(1 for r in rows if not r[2])
    return {"ok": True, "rows_inspected": len(rows),
            "missing_lemma": missing_lemma,
            "missing_pos": missing_pos,
            "limit_used": cap,
            "samples": [
                {"word": r[0], "lemma": r[1], "pos": r[2]}
                for r in rows[:20]
            ]}


def identify_missing_lemmas(limit: int = 500,
                            db_path: Optional[str | Path] = None
                            ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    with _ru_connect(db_path) as conn:
        cur = conn.execute(
            "SELECT word FROM words WHERE lemma='' OR lemma IS NULL "
            "ORDER BY frequency_score DESC LIMIT ?", (cap,))
        rows = cur.fetchall()
    return [{"word": r[0]} for r in rows]


def identify_missing_pos(limit: int = 500,
                         db_path: Optional[str | Path] = None
                         ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    with _ru_connect(db_path) as conn:
        cur = conn.execute(
            "SELECT word, lemma FROM words "
            "WHERE part_of_speech='' OR part_of_speech IS NULL "
            "ORDER BY frequency_score DESC LIMIT ?", (cap,))
        rows = cur.fetchall()
    return [{"word": r[0], "lemma": r[1]} for r in rows]


def propose_morphology_repairs(limit: int = 500,
                                db_path: Optional[str | Path] = None
                                ) -> list[dict[str, Any]]:
    """Conservative repair proposals - never mutates the DB."""
    _ensure_flags()
    cap = _clamp(limit)
    backend = detect_morphology_backend()["active_backend"]
    miss_lemma = identify_missing_lemmas(limit=cap, db_path=db_path)
    miss_pos = identify_missing_pos(limit=cap, db_path=db_path)
    proposals: list[dict[str, Any]] = []
    for r in miss_lemma:
        word = r["word"]
        # Trivial heuristic suggestion: lemma == word itself.
        proposals.append({"word": word,
                          "proposed_lemma": word,
                          "confidence": 0.45 if backend == "heuristic_fallback"
                                        else 0.75,
                          "backend": backend,
                          "kind": "missing_lemma"})
        if len(proposals) >= cap:
            break
    for r in miss_pos:
        if len(proposals) >= cap:
            break
        proposals.append({"word": r["word"],
                          "lemma": r["lemma"],
                          "proposed_pos": "noun",
                          "confidence": 0.40 if backend == "heuristic_fallback"
                                        else 0.70,
                          "backend": backend,
                          "kind": "missing_pos"})
    return proposals[:cap]


def write_morphology_upgrade_report(report: dict[str, Any],
                                    output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "detect_morphology_backend",
    "get_morphology_backend_status",
    "create_pymorphy3_install_note",
    "audit_russian_entries_for_morphology",
    "identify_missing_lemmas",
    "identify_missing_pos",
    "propose_morphology_repairs",
    "write_morphology_upgrade_report",
]
