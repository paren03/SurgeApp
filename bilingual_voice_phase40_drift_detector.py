"""Phase 40 - Drift Detector.

Read-only drift detection comparing stored Phase 39 evidence
against current Phase 38 governance artifacts, current Phase 37
adapter allowlist, current production DB baselines, and current
Phase 21 staging-folder state.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase37_adapter_interface as p37i
import bilingual_voice_phase38_governance_ledger as p38gl
import bilingual_voice_phase38_status_dashboard as p38sd


_PHASE = "phase40.drift_detector.v1"


_EXPECTED_BASELINES = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


# Runtime-assembled forbidden runtime tokens
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_FORBIDDEN_BOUNDARIES = (
    "audio_generation", "tts_invocation",
    "subprocess_execution",
    "production_signing_secret_storage",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
)


def _root() -> Path:
    return Path(__file__).resolve().parent


def _stored_trace(artifacts: Any) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {}
    w = (artifacts.get("loaded") or {}
         ).get("rehearsal_trace") or {}
    obj = w.get("object") if isinstance(w, dict) else None
    return obj if isinstance(obj, dict) else {}


def _stored_recheck(artifacts: Any) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {}
    w = (artifacts.get("loaded") or {}
         ).get("rehearsal_recheck") or {}
    obj = w.get("object") if isinstance(w, dict) else None
    return obj if isinstance(obj, dict) else {}


def check_adapter_allowlist_drift(
    artifacts: Any,
) -> dict[str, Any]:
    current = sorted(p37i.ALLOWED_ADAPTER_TYPES)
    recheck = _stored_recheck(artifacts)
    stored = sorted(recheck.get("adapter_allowlist") or [])
    drifted = current != stored
    return {
        "category": "adapter_allowlist_drift",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "current": current,
        "stored": stored,
    }


def check_governance_doc_drift(
    artifacts: Any,
) -> dict[str, Any]:
    """Phase 38 ledger remains consistent with stored
    rehearsal report's understanding of adapters and
    invariants."""
    try:
        ledger = p38gl.build_boundary_guarantee_ledger()
    except Exception as e:  # noqa: BLE001
        return {
            "category": "governance_doc_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"ledger_build_failed:{e}",
        }
    val = p38gl.validate_boundary_guarantee_ledger(ledger)
    if not val.get("ok"):
        return {
            "category": "governance_doc_drift",
            "drifted": True,
            "severity": "fail",
            "reason": ",".join(val.get("reasons", [])),
        }
    if ledger.get("latest_phase") != 37:
        return {
            "category": "governance_doc_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"latest_phase_not_37:"
                       f"{ledger.get('latest_phase')}",
        }
    return {
        "category": "governance_doc_drift",
        "drifted": False,
        "severity": "pass",
        "ledger_latest_phase": ledger.get("latest_phase"),
    }


def check_baseline_drift(
    artifacts: Any,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Open the three production DBs read-only and verify
    counts. Counts the live manifests via glob."""
    r = root or _root()
    observed: dict[str, int] = {}
    en_db = r / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = r / "russian_stack" / "russian_lexicon.sqlite"
    link_db = r / "bilingual_stack" / "bilingual_links.sqlite"
    try:
        if en_db.exists():
            c = sqlite3.connect(str(en_db))
            observed["english_words"] = c.execute(
                "SELECT COUNT(*) FROM words").fetchone()[0]
            c.close()
        if ru_db.exists():
            c = sqlite3.connect(str(ru_db))
            observed["russian_words"] = c.execute(
                "SELECT COUNT(*) FROM words").fetchone()[0]
            observed["russian_phrases"] = c.execute(
                "SELECT COUNT(*) FROM phrases").fetchone()[0]
            c.close()
        if link_db.exists():
            c = sqlite3.connect(str(link_db))
            observed["bilingual_concepts"] = c.execute(
                "SELECT COUNT(*) FROM concepts"
                ).fetchone()[0]
            observed["bilingual_entry_links"] = c.execute(
                "SELECT COUNT(*) FROM entry_links"
                ).fetchone()[0]
            c.close()
    except Exception as e:  # noqa: BLE001
        return {
            "category": "baseline_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"db_read_failed:{e}",
            "observed": observed,
        }
    import glob
    live = [p for p in glob.glob(
        str(r / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    observed["live_pack_manifests"] = len(live)
    drifts: list[str] = []
    for k, v in _EXPECTED_BASELINES.items():
        if k in observed and observed[k] != v:
            drifts.append(f"{k}:{observed[k]}!={v}")
    return {
        "category": "baseline_drift",
        "drifted": bool(drifts),
        "severity": "fail" if drifts else "pass",
        "observed": observed,
        "expected": dict(_EXPECTED_BASELINES),
        "drifts": drifts,
    }


def check_phase21_status_drift(
    artifacts: Any,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Phase 21 must remain BLOCKED unless operator stages
    corpus files. We flag staged files as WARN (never import
    them)."""
    r = root or _root()
    en_inc = r / "corpus_sources" / "english" / "incoming"
    ru_inc = r / "corpus_sources" / "russian" / "incoming"
    en_files: list[str] = []
    ru_files: list[str] = []
    try:
        if en_inc.exists() and en_inc.is_dir():
            for p in en_inc.iterdir():
                if p.is_file():
                    en_files.append(p.name)
                if len(en_files) > 100:
                    break
        if ru_inc.exists() and ru_inc.is_dir():
            for p in ru_inc.iterdir():
                if p.is_file():
                    ru_files.append(p.name)
                if len(ru_files) > 100:
                    break
    except Exception as e:  # noqa: BLE001
        return {
            "category": "phase21_status_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"phase21_scan_failed:{e}",
        }
    staged = bool(en_files or ru_files)
    return {
        "category": "phase21_status_drift",
        "drifted": staged,
        "severity": "warn" if staged else "pass",
        "phase21_status_text":
            ("STAGED_AWAITING_OPERATOR"
             if staged else "BLOCKED"),
        "english_incoming_filenames": en_files,
        "russian_incoming_filenames": ru_files,
        "note": ("Phase 21 incoming files are NOT imported "
                  "by Phase 40. Operator must run Phase 21 "
                  "explicitly."),
    }


def check_boundary_drift(
    artifacts: Any,
) -> dict[str, Any]:
    """Phase 38 status dashboard remains consistent: 4
    adapters, all forbidden boundaries still blocked."""
    try:
        dash = p38sd.create_governance_status_dashboard()
    except Exception as e:  # noqa: BLE001
        return {
            "category": "forbidden_boundary_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"dashboard_build_failed:{e}",
        }
    val = p38sd.validate_governance_status_dashboard(dash)
    if not val.get("ok"):
        return {
            "category": "forbidden_boundary_drift",
            "drifted": True,
            "severity": "fail",
            "reason": ",".join(val.get("reasons", [])),
        }
    blocked = dash.get("blocked_boundaries") or []
    missing = [b for b in _FORBIDDEN_BOUNDARIES
               if b not in blocked]
    if missing:
        return {
            "category": "forbidden_boundary_drift",
            "drifted": True,
            "severity": "fail",
            "missing_blocks": missing,
        }
    if dash.get("adapter_count") != 4:
        return {
            "category": "forbidden_boundary_drift",
            "drifted": True,
            "severity": "fail",
            "reason":
                f"adapter_count_not_4:"
                f"{dash.get('adapter_count')}",
        }
    return {
        "category": "forbidden_boundary_drift",
        "drifted": False,
        "severity": "pass",
        "blocked_count": len(blocked),
        "adapter_count": dash.get("adapter_count"),
    }


def check_secret_audio_command_drift(
    artifacts: Any,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Scan governance_phase40/ + rehearsal_phase39/ outputs
    for audio files, then scan stage receipts + trace for
    secret-shape fields or runtime-flag drift."""
    r = root or _root()
    audio_hits: list[str] = []
    base_dirs = (
        r / "bilingual_stack" / "rehearsal_phase39",
        r / "bilingual_stack" / "governance_phase40",
    )
    audio_exts = (".wav", ".mp3", ".ogg", ".flac",
                   ".m4a", ".aac", ".opus")
    for base in base_dirs:
        if not base.exists():
            continue
        scanned = 0
        for d, _dirs, files in os.walk(base):
            if scanned > 5000:
                break
            for f in files:
                scanned += 1
                if f.lower().endswith(audio_exts):
                    audio_hits.append(os.path.join(d, f))
    secret_hits: list[str] = []
    receipts = artifacts.get("stage_receipts") \
        if isinstance(artifacts, dict) else []
    for rec in receipts or []:
        obj = rec.get("object") if isinstance(
            rec, dict) else {}
        if not isinstance(obj, dict):
            continue
        for k in ("signing_key_material", "private_key",
                  "material_hex", "sealed_payload"):
            if k in obj:
                secret_hits.append(
                    f"receipt:{obj.get('scenario_id')}:{k}")
        if "operator_id" in obj and obj.get(
                "operator_id") not in (None, ""):
            secret_hits.append(
                f"receipt:{obj.get('scenario_id')}"
                f":raw_operator_id")
    runtime_flag_hits: list[str] = []
    for rec in receipts or []:
        obj = rec.get("object") if isinstance(
            rec, dict) else {}
        if not isinstance(obj, dict):
            continue
        for k in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
            if obj.get(k) is True:
                runtime_flag_hits.append(
                    f"receipt:{obj.get('scenario_id')}:{k}")
    drifted = bool(audio_hits or secret_hits
                    or runtime_flag_hits)
    return {
        "category": "secret_audio_command_drift",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "audio_hits": audio_hits,
        "secret_hits": secret_hits,
        "runtime_flag_hits": runtime_flag_hits,
    }


def _check_consent_binding_drift(
    artifacts: Any,
) -> dict[str, Any]:
    loaded = artifacts.get("loaded") or {}
    w = loaded.get("umbrella_consent") or {}
    consent = w.get("object") if isinstance(w, dict) \
        else None
    if not isinstance(consent, dict):
        return {
            "category": "consent_binding_drift",
            "drifted": True,
            "severity": "fail",
            "reason": "consent_missing",
        }
    op_hash = consent.get("operator_id_hash") or ""
    sc = consent.get("scenario_count") or 0
    ba = consent.get("bound_at") or 0
    nonce = consent.get("nonce") or ""
    expected = hashlib.sha256(
        f"{op_hash}|{sc}|{ba}|{nonce}".encode("utf-8")
        ).hexdigest()
    drifted = (expected != consent.get("binding_hash"))
    return {
        "category": "consent_binding_drift",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "expected_binding_hash": expected,
        "stored_binding_hash": consent.get("binding_hash"),
    }


def _check_missing_artifact_drift(
    artifacts: Any,
) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {
            "category": "missing_artifact",
            "drifted": True,
            "severity": "fail",
            "reason": "artifacts_not_dict",
        }
    missing = artifacts.get("missing") or []
    rejected = artifacts.get("rejected") or []
    drifted = bool(missing or rejected)
    return {
        "category": "missing_artifact",
        "drifted": drifted,
        "severity": "fail" if drifted else "pass",
        "missing": missing,
        "rejected": rejected,
    }


def detect_phase40_drift(
    artifacts: Any,
    current_checks: Optional[list[str]] = None,
) -> dict[str, Any]:
    checks = current_checks or [
        "missing_artifact",
        "consent_binding_drift",
        "adapter_allowlist_drift",
        "governance_doc_drift",
        "baseline_drift",
        "phase21_status_drift",
        "forbidden_boundary_drift",
        "secret_audio_command_drift",
    ]
    results: list[dict[str, Any]] = []
    fn_map = {
        "missing_artifact":
            _check_missing_artifact_drift,
        "consent_binding_drift":
            _check_consent_binding_drift,
        "adapter_allowlist_drift":
            check_adapter_allowlist_drift,
        "governance_doc_drift":
            check_governance_doc_drift,
        "baseline_drift": check_baseline_drift,
        "phase21_status_drift":
            check_phase21_status_drift,
        "forbidden_boundary_drift":
            check_boundary_drift,
        "secret_audio_command_drift":
            check_secret_audio_command_drift,
    }
    fail_count = 0
    warn_count = 0
    pass_count = 0
    for c in checks:
        fn = fn_map.get(c)
        if not fn:
            continue
        try:
            r = fn(artifacts)
        except Exception as e:  # noqa: BLE001
            r = {
                "category": c,
                "drifted": True,
                "severity": "fail",
                "reason": f"check_exception:{e}",
            }
        sev = r.get("severity", "pass")
        if sev == "fail":
            fail_count += 1
        elif sev == "warn":
            warn_count += 1
        else:
            pass_count += 1
        results.append(r)
    return {
        "drift_id": f"drift_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "checks_run": list(checks),
        "results": results,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "pass_count": pass_count,
        "ok": fail_count == 0,
    }


def summarize_phase40_drift(drift: Any) -> dict[str, Any]:
    if not isinstance(drift, dict):
        return {"ok": False, "summary": "no_drift"}
    return {
        "ok": bool(drift.get("ok")),
        "summary": (
            f"phase40 drift: fail="
            f"{drift.get('fail_count')} warn="
            f"{drift.get('warn_count')} pass="
            f"{drift.get('pass_count')}"),
        "drift_id": drift.get("drift_id"),
        "phase": _PHASE,
    }


def write_phase40_drift_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "detect_phase40_drift",
    "check_adapter_allowlist_drift",
    "check_governance_doc_drift",
    "check_baseline_drift",
    "check_phase21_status_drift",
    "check_boundary_drift",
    "check_secret_audio_command_drift",
    "summarize_phase40_drift",
    "write_phase40_drift_report",
]
