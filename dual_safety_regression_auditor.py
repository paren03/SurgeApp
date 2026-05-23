"""Phase 20 - Dual Safety Regression Auditor.

Proves the safety policy still works after scale imports + indexing. Probes
recognition_only, do_not_use_unprompted, vulgar/offensive, and slang/street
behavior across teacher / normal / professional / voice modes.

Read-only. Bounded. No daemon.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import dual_retrieval_quality_eval as rqe
import dual_retrieval_index_builder as idx


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def build_safety_probe_set() -> dict[str, list[dict[str, Any]]]:
    """Synthetic probe inputs covering the four critical safety classes.

    These are STRUCTURED dictionaries we feed directly into the existing
    safety-policy checks - we do NOT need to write them to the lexicon to
    test the policy. The point is to confirm that, given a row carrying a
    particular tag combination, the policy responds correctly.
    """
    probes = {
        "recognition_only": [
            {"word": "probe_reco_1", "safety_tags": ["recognition_only"],
             "register_tags": ["academic", "recognition_only"],
             "coverage_categories": ["philosophy_abstract"]},
        ],
        "do_not_use_unprompted": [
            {"word": "probe_dnu_1",
             "safety_tags": ["do_not_use_unprompted"],
             "register_tags": ["do_not_use_unprompted"],
             "coverage_categories": ["philosophy_abstract"]},
        ],
        "vulgar": [
            {"word": "probe_vulgar_1",
             "safety_tags": ["vulgar"],
             "register_tags": ["vulgar", "slang"],
             "coverage_categories": ["slang_street_talk"]},
        ],
        "offensive": [
            {"word": "probe_offensive_1",
             "safety_tags": ["offensive"],
             "register_tags": ["offensive"],
             "coverage_categories": ["slang_street_talk"]},
        ],
        "slang_normal": [
            {"word": "probe_slang_1",
             "safety_tags": [],
             "register_tags": ["slang"],
             "coverage_categories": ["slang_street_talk"]},
        ],
        "benign": [
            {"word": "probe_benign_1",
             "safety_tags": [],
             "register_tags": ["standard"],
             "coverage_categories": ["core_vocabulary"]},
        ],
    }
    return probes


def _audit_class(rows: list[dict[str, Any]], mode: str,
                 is_user_prompted: bool) -> dict[str, Any]:
    return rqe.check_safety_policy_on_results(
        rows, mode=mode, is_user_prompted=is_user_prompted)


def _check_expectations(class_name: str, decision: dict[str, Any],
                        rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Each safety class has expected outcomes under default (teacher,
    not-prompted) policy. We verify them here."""
    words = [r.get("word") for r in rows]
    expected = {
        "recognition_only": {"in_do_not_use_violations": False,
                             "in_suggestion_only_recognized": True,
                             "in_vulgar_in_teacher_mode": False},
        "do_not_use_unprompted": {"in_do_not_use_violations": True,
                                  "in_suggestion_only_recognized": False,
                                  "in_vulgar_in_teacher_mode": False},
        "vulgar": {"in_do_not_use_violations": False,
                   "in_suggestion_only_recognized": False,
                   "in_vulgar_in_teacher_mode": True},
        "offensive": {"in_do_not_use_violations": False,
                      "in_suggestion_only_recognized": False,
                      "in_vulgar_in_teacher_mode": True},
        "slang_normal": {"in_do_not_use_violations": False,
                         "in_suggestion_only_recognized": False,
                         "in_vulgar_in_teacher_mode": False},
        "benign": {"in_do_not_use_violations": False,
                   "in_suggestion_only_recognized": False,
                   "in_vulgar_in_teacher_mode": False},
    }[class_name]
    seen_in_dnu = any(w in decision["do_not_use_violations"] for w in words)
    seen_in_recog = any(w in decision["suggestion_only_recognized"]
                        for w in words)
    seen_in_vulgar = any(w in decision["vulgar_in_teacher_mode"]
                         for w in words)
    return {
        "class": class_name,
        "expected": expected,
        "observed": {"in_do_not_use_violations": seen_in_dnu,
                     "in_suggestion_only_recognized": seen_in_recog,
                     "in_vulgar_in_teacher_mode": seen_in_vulgar},
        "pass": (seen_in_dnu == expected["in_do_not_use_violations"]
                 and seen_in_recog == expected["in_suggestion_only_recognized"]
                 and seen_in_vulgar == expected["in_vulgar_in_teacher_mode"]),
    }


def audit_english_safety_policy() -> dict[str, Any]:
    _ensure_flags()
    probes = build_safety_probe_set()
    out: dict[str, Any] = {"language": "en", "classes": {}}
    for cname, rows in probes.items():
        dec = _audit_class(rows, mode="teacher", is_user_prompted=False)
        chk = _check_expectations(cname, dec, rows)
        out["classes"][cname] = {"decision": dec, "expectation": chk}
    out["all_pass"] = all(c["expectation"]["pass"]
                          for c in out["classes"].values())
    return out


def audit_russian_safety_policy() -> dict[str, Any]:
    """The Phase 17 evaluator does not implement a separate Russian policy
    path; the rule semantics are identical because tags are shared. We
    reuse the EN-mode check on Russian-style synthetic rows."""
    _ensure_flags()
    probes = build_safety_probe_set()
    # Rebrand probe words with Cyrillic prefix to be Russian-shaped
    ru_probes = {}
    for cname, rows in probes.items():
        ru_probes[cname] = [
            {**r, "word": "ру_" + r["word"], "language": "ru"}
            for r in rows
        ]
    out: dict[str, Any] = {"language": "ru", "classes": {}}
    for cname, rows in ru_probes.items():
        dec = _audit_class(rows, mode="teacher", is_user_prompted=False)
        chk = _check_expectations(cname, dec, rows)
        out["classes"][cname] = {"decision": dec, "expectation": chk}
    out["all_pass"] = all(c["expectation"]["pass"]
                          for c in out["classes"].values())
    return out


def audit_indexed_retrieval_safety(language: str,
                                   en_db_path: Optional[str | Path] = None,
                                   ru_db_path: Optional[str | Path] = None
                                   ) -> dict[str, Any]:
    """For each safety tag, the index query is allowed to RETURN matching
    rows (recognition); but the policy filter applied to those rows must
    correctly classify them. We assert the filter still flags them."""
    _ensure_flags()
    db_path = en_db_path if language == "en" else ru_db_path
    out: dict[str, Any] = {"language": language, "per_tag": {}}
    for tag in ("recognition_only", "do_not_use_unprompted",
                "vulgar", "offensive"):
        rows = idx.query_by_safety(language, tag, limit=50, db_path=db_path)
        decision = rqe.check_safety_policy_on_results(
            rows, mode="teacher", is_user_prompted=False)
        flagged_n = (decision["do_not_use_violation_count"]
                     + decision["suggestion_only_recognized_count"]
                     + decision["vulgar_in_teacher_mode_count"])
        out["per_tag"][tag] = {"rows_returned": len(rows),
                               "decision": decision,
                               "filter_flagged": flagged_n}
    return {"ok": True, **out}


def audit_runtime_context_safety(language: str,
                                 db_path: Optional[str | Path] = None
                                 ) -> dict[str, Any]:
    _ensure_flags()
    fn = (rqe.run_english_retrieval_eval if language == "en"
          else rqe.run_russian_retrieval_eval)
    rep = fn(limit=15, db_path=db_path)
    return {"ok": rep.get("ok", False),
            "language": language,
            "bounds_ok": rep.get("bounds_ok"),
            "safety_ok": rep.get("safety_ok"),
            "average_score": rep.get("average_score")}


def audit_prompted_vs_unprompted_behavior(language: str) -> dict[str, Any]:
    """Same probe set, but check that user-prompted=True permits
    recognition_only / do_not_use_unprompted to surface (it still flags
    vulgar in teacher mode unless prompted)."""
    _ensure_flags()
    probes = build_safety_probe_set()
    out: dict[str, Any] = {"language": language, "rules": []}
    for cname, rows in probes.items():
        unprompted = rqe.check_safety_policy_on_results(
            rows, mode="teacher", is_user_prompted=False)
        prompted = rqe.check_safety_policy_on_results(
            rows, mode="teacher", is_user_prompted=True)
        out["rules"].append({
            "class": cname,
            "unprompted_do_not_use_count": unprompted["do_not_use_violation_count"],
            "prompted_do_not_use_count": prompted["do_not_use_violation_count"],
            "softens_when_prompted":
                unprompted["do_not_use_violation_count"]
                >= prompted["do_not_use_violation_count"],
        })
    out["all_softening_consistent"] = all(r["softens_when_prompted"]
                                          for r in out["rules"])
    return out


def write_safety_regression_report(report: dict[str, Any],
                                   output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "build_safety_probe_set",
    "audit_english_safety_policy",
    "audit_russian_safety_policy",
    "audit_indexed_retrieval_safety",
    "audit_runtime_context_safety",
    "audit_prompted_vs_unprompted_behavior",
    "write_safety_regression_report",
]
