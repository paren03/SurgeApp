"""Phase 21A - Staging Readiness Gate.

Reads acceptance reports and per-language file presence to decide whether
operator-staged files are ready for Phase 21 real import.

Returns one of:
    NOT_READY_NO_FILES
    NOT_READY_MISSING_ENGLISH
    NOT_READY_MISSING_RUSSIAN
    NOT_READY_LOW_ROW_COUNT
    NOT_READY_VALIDATION_FAILURES
    NOT_READY_SAFETY_BLOCKERS
    READY_FOR_DRY_RUN_ONLY
    READY_FOR_PHASE21_REAL_IMPORT
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


READINESS_STATES = (
    "NOT_READY_NO_FILES",
    "NOT_READY_MISSING_ENGLISH",
    "NOT_READY_MISSING_RUSSIAN",
    "NOT_READY_LOW_ROW_COUNT",
    "NOT_READY_VALIDATION_FAILURES",
    "NOT_READY_SAFETY_BLOCKERS",
    "READY_FOR_DRY_RUN_ONLY",
    "READY_FOR_PHASE21_REAL_IMPORT",
)


PHASE21A_READY_DIR = Path("corpus_sources/phase21a/ready_reports")


_SAFETY_BLOCKER_REASONS = (
    "operational_unsafe", "prompt_injection_like",
)


def load_acceptance_reports(report_dir: str | Path
                            ) -> list[dict[str, Any]]:
    d = Path(report_dir)
    if not d.exists():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(d.glob("*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out


def _by_lang(reports: list[dict[str, Any]], lang: str
             ) -> list[dict[str, Any]]:
    return [r for r in reports if r.get("language") == lang]


def check_minimum_file_presence(reports: Optional[list[dict[str, Any]]] = None
                                ) -> dict[str, Any]:
    if reports is None:
        en = list((Path("corpus_sources/english/incoming")
                   ).glob("*")) if Path(
            "corpus_sources/english/incoming").exists() else []
        ru = list((Path("corpus_sources/russian/incoming")
                   ).glob("*")) if Path(
            "corpus_sources/russian/incoming").exists() else []
        return {"ok": bool(en) and bool(ru),
                "en_files": [str(p) for p in en if p.is_file()],
                "ru_files": [str(p) for p in ru if p.is_file()]}
    en = _by_lang(reports, "en")
    ru = _by_lang(reports, "ru")
    return {"ok": bool(en) and bool(ru),
            "en_reports": len(en), "ru_reports": len(ru)}


def check_minimum_row_counts(reports: list[dict[str, Any]],
                             min_rows: int = 5000) -> dict[str, Any]:
    en = _by_lang(reports, "en")
    ru = _by_lang(reports, "ru")
    en_total = sum(int((r.get("summary") or {}).get("n", 0)) for r in en)
    ru_total = sum(int((r.get("summary") or {}).get("n", 0)) for r in ru)
    return {"ok": en_total >= min_rows and ru_total >= min_rows,
            "min_rows": int(min_rows),
            "en_total": en_total, "ru_total": ru_total}


def check_validation_pass_rate(reports: list[dict[str, Any]],
                               min_acceptance_rate: float = 0.90
                               ) -> dict[str, Any]:
    per_lang: dict[str, dict[str, float]] = {}
    for lang in ("en", "ru"):
        rs = _by_lang(reports, lang)
        n = 0
        accepted = 0
        for r in rs:
            s = r.get("summary") or {}
            n += int(s.get("n", 0))
            accepted += int(s.get("accept", 0))
        rate = round(accepted / n, 4) if n else 0.0
        per_lang[lang] = {"n": float(n), "accept": float(accepted),
                          "rate": rate}
    ok = all(per_lang[l]["rate"] >= float(min_acceptance_rate)
             for l in ("en", "ru") if per_lang[l]["n"] > 0)
    return {"ok": ok, "threshold": float(min_acceptance_rate),
            "per_language": per_lang}


def check_safety_blockers(reports: list[dict[str, Any]]) -> dict[str, Any]:
    blockers: list[dict[str, Any]] = []
    for r in reports:
        rc = (r.get("summary") or {}).get("reason_counts") or {}
        for k in _SAFETY_BLOCKER_REASONS:
            if int(rc.get(k, 0)) > 0:
                blockers.append({"path": r.get("path"),
                                 "language": r.get("language"),
                                 "reason": k,
                                 "count": int(rc[k])})
    return {"ok": not blockers, "blockers": blockers}


def check_metadata_completeness(reports: list[dict[str, Any]],
                                min_metadata_rate: float = 0.95
                                ) -> dict[str, Any]:
    per_lang: dict[str, dict[str, float]] = {}
    for lang in ("en", "ru"):
        rs = _by_lang(reports, lang)
        n = 0
        cover_or_register_missing = 0
        for r in rs:
            s = r.get("summary") or {}
            n += int(s.get("n", 0))
            rc = s.get("reason_counts") or {}
            cover_or_register_missing += int(rc.get("invalid_taxonomy", 0))
            cover_or_register_missing += int(rc.get("invalid_register", 0))
            cover_or_register_missing += int(rc.get("invalid_safety", 0))
        complete = max(0, n - cover_or_register_missing)
        rate = round(complete / n, 4) if n else 0.0
        per_lang[lang] = {"n": float(n), "complete": float(complete),
                          "rate": rate}
    ok = all(per_lang[l]["rate"] >= float(min_metadata_rate)
             for l in ("en", "ru") if per_lang[l]["n"] > 0)
    return {"ok": ok, "threshold": float(min_metadata_rate),
            "per_language": per_lang}


def check_quality_gate_eligibility(reports: list[dict[str, Any]]
                                   ) -> dict[str, Any]:
    """A simple all-of-the-above eligibility shortcut."""
    pres = check_minimum_file_presence(reports)
    pass_rate = check_validation_pass_rate(reports)
    blockers = check_safety_blockers(reports)
    return {"ok": (pres["ok"] and pass_rate["ok"] and blockers["ok"]),
            "presence": pres, "pass_rate": pass_rate,
            "safety_blockers": blockers}


def evaluate_language_pair_ready(english_reports: list[dict[str, Any]],
                                 russian_reports: list[dict[str, Any]]
                                 ) -> dict[str, Any]:
    en_n = sum(int((r.get("summary") or {}).get("n", 0))
               for r in english_reports)
    ru_n = sum(int((r.get("summary") or {}).get("n", 0))
               for r in russian_reports)
    return {"ok": bool(english_reports) and bool(russian_reports),
            "en_count": len(english_reports),
            "ru_count": len(russian_reports),
            "en_rows": en_n, "ru_rows": ru_n}


def produce_phase21_ready_decision(
    reports: Optional[list[dict[str, Any]]] = None,
    min_rows: int = 5000,
    min_acceptance_rate: float = 0.90,
    min_metadata_rate: float = 0.95,
    file_presence: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Roll up the gates into a single decision."""
    presence = file_presence if file_presence is not None else \
        check_minimum_file_presence(reports)
    if not presence["ok"]:
        en_present = bool(presence.get("en_files")
                          or presence.get("en_reports"))
        ru_present = bool(presence.get("ru_files")
                          or presence.get("ru_reports"))
        if not en_present and not ru_present:
            state = "NOT_READY_NO_FILES"
        elif not en_present:
            state = "NOT_READY_MISSING_ENGLISH"
        else:
            state = "NOT_READY_MISSING_RUSSIAN"
        return {"state": state, "reasons": [state.lower()],
                "presence": presence}
    rs = reports or []
    blockers = check_safety_blockers(rs)
    if not blockers["ok"]:
        return {"state": "NOT_READY_SAFETY_BLOCKERS",
                "reasons": ["safety_blockers_found"],
                "details": blockers,
                "presence": presence}
    pass_rate = check_validation_pass_rate(rs, min_acceptance_rate)
    if not pass_rate["ok"]:
        return {"state": "NOT_READY_VALIDATION_FAILURES",
                "reasons": ["acceptance_rate_below_threshold"],
                "details": pass_rate,
                "presence": presence}
    rows_ok = check_minimum_row_counts(rs, min_rows)
    metadata_ok = check_metadata_completeness(rs, min_metadata_rate)
    if not rows_ok["ok"]:
        return {"state": "READY_FOR_DRY_RUN_ONLY"
                if (rows_ok["en_total"] > 0 and rows_ok["ru_total"] > 0)
                else "NOT_READY_LOW_ROW_COUNT",
                "reasons": ["row_count_below_recommended"],
                "details": {"rows": rows_ok, "metadata": metadata_ok},
                "presence": presence}
    if not metadata_ok["ok"]:
        return {"state": "READY_FOR_DRY_RUN_ONLY",
                "reasons": ["metadata_completeness_below_threshold"],
                "details": {"rows": rows_ok, "metadata": metadata_ok},
                "presence": presence}
    return {"state": "READY_FOR_PHASE21_REAL_IMPORT",
            "reasons": ["all_gates_pass"],
            "details": {"rows": rows_ok, "metadata": metadata_ok,
                        "pass_rate": pass_rate, "blockers": blockers},
            "presence": presence}


def write_phase21_ready_report(report: dict[str, Any],
                               output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "READINESS_STATES",
    "PHASE21A_READY_DIR",
    "load_acceptance_reports",
    "evaluate_language_pair_ready",
    "check_minimum_file_presence",
    "check_minimum_row_counts",
    "check_validation_pass_rate",
    "check_safety_blockers",
    "check_metadata_completeness",
    "check_quality_gate_eligibility",
    "produce_phase21_ready_decision",
    "write_phase21_ready_report",
]
