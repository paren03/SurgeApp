"""Phase 40 - Bounded Local Replay Loader.

Loads stored Phase 39 rehearsal artifacts via bounded local reads
only. Rejects URL-shaped paths, shell-metacharacter paths,
runtime DBs / backups / synthetic corpora, and any path inside
local_secret_handoff. No subprocess. No network.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase40.replay_loader.v1"


_DEFAULT_MAX_BYTES = 2_000_000


# URL-shaped scheme prefixes
_URL_PREFIXES = (
    "http://", "https://", "ftp://", "file://",
    "smb://", "ssh://", "git://", "ws://", "wss://",
)


# Shell-metacharacter set we refuse outright.
_SHELL_METACHARS = ("|", "&", ";", "`", "$", "$(", "${",
                     "\n", "\r", "<", ">", "*", "?",
                     "(", ")")


_FORBIDDEN_PATH_TOKENS = (
    "/local_secret_handoff/",
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/",
)


_RUNTIME_DB_EXTENSIONS = (".sqlite", ".sqlite3", ".db")


def reject_unsafe_replay_paths(
    paths: list[str],
) -> dict[str, Any]:
    if not isinstance(paths, list):
        return {"ok": False, "rejected_all": True,
                "reasons": ["paths_not_list"]}
    rejected: list[dict[str, Any]] = []
    accepted: list[str] = []
    for raw in paths:
        s = str(raw or "")
        norm = s.replace("\\", "/")
        reason = ""
        for pre in _URL_PREFIXES:
            if norm.lower().startswith(pre):
                reason = f"url_scheme:{pre}"
                break
        if not reason:
            for ch in _SHELL_METACHARS:
                if ch in s:
                    reason = f"shell_metachar:{repr(ch)}"
                    break
        if not reason:
            for tok in _FORBIDDEN_PATH_TOKENS:
                if tok in norm:
                    reason = f"forbidden_path_token:{tok}"
                    break
        if not reason:
            for ext in _RUNTIME_DB_EXTENSIONS:
                if norm.lower().endswith(ext):
                    reason = f"runtime_db_ext:{ext}"
                    break
        if reason:
            rejected.append({"path": s, "reason": reason})
        else:
            accepted.append(s)
    return {
        "ok": not rejected,
        "rejected_count": len(rejected),
        "accepted_count": len(accepted),
        "rejected": rejected,
        "accepted": accepted,
        "phase": _PHASE,
    }


def load_json_artifact(
    path: str,
    max_bytes: int = _DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    res = reject_unsafe_replay_paths([path])
    if not res["ok"]:
        return {
            "ok": False,
            "path": path,
            "reason":
                (res["rejected"][0]["reason"]
                 if res["rejected"] else "rejected"),
            "phase": _PHASE,
        }
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "path": str(p),
                "reason": "not_found", "phase": _PHASE}
    try:
        size = p.stat().st_size
    except Exception:  # noqa: BLE001
        return {"ok": False, "path": str(p),
                "reason": "stat_failed", "phase": _PHASE}
    if size > int(max_bytes):
        return {"ok": False, "path": str(p),
                "reason": f"too_large:{size}>"
                          f"{int(max_bytes)}",
                "phase": _PHASE}
    try:
        body = p.read_text(encoding="utf-8",
                            errors="ignore")
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "path": str(p),
                "reason": f"read_failed:{e}",
                "phase": _PHASE}
    try:
        obj = json.loads(body)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "path": str(p),
                "reason": f"json_decode_failed:{e}",
                "phase": _PHASE}
    return {
        "ok": True,
        "path": str(p),
        "size_bytes": size,
        "object": obj,
        "phase": _PHASE,
    }


def load_phase39_replay_artifacts(
    base_dir: Optional[str] = None,
    max_bytes: int = _DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    base = Path(base_dir) if base_dir else (
        Path(__file__).resolve().parent
        / "bilingual_stack" / "rehearsal_phase39")
    targets = {
        "rehearsal_contract":
            base / "contracts" / "rehearsal_contract.json",
        "umbrella_consent":
            base / "consents" / "umbrella_consent.json",
        "rehearsal_trace":
            base / "traces" / "rehearsal_trace.json",
        "rehearsal_recheck":
            base / "recheck" / "rehearsal_recheck.json",
        "rehearsal_report":
            base / "reports" / "rehearsal_report.json",
    }
    loaded: dict[str, Any] = {}
    missing: list[str] = []
    rejected: list[dict[str, Any]] = []
    for key, p in targets.items():
        res = load_json_artifact(str(p), max_bytes=max_bytes)
        if res.get("ok"):
            loaded[key] = res
        else:
            if res.get("reason") == "not_found":
                missing.append(key)
            else:
                rejected.append({"key": key,
                                  "reason": res.get(
                                      "reason")})
    # Stage receipts: load all matching files in stages/
    stages_dir = base / "stages"
    stage_receipts: list[dict[str, Any]] = []
    if stages_dir.exists() and stages_dir.is_dir():
        receipt_paths = sorted(
            str(p) for p in stages_dir.glob("receipt_*.json")
            if p.is_file())
        scan = reject_unsafe_replay_paths(receipt_paths)
        for rp in scan.get("accepted", []):
            r = load_json_artifact(rp, max_bytes=max_bytes)
            if r.get("ok"):
                stage_receipts.append(r)
    return {
        "ok": not missing and not rejected,
        "base_dir": str(base),
        "loaded": loaded,
        "stage_receipts": stage_receipts,
        "stage_receipt_count": len(stage_receipts),
        "missing": missing,
        "rejected": rejected,
        "phase": _PHASE,
    }


def validate_loaded_replay_artifacts(
    artifacts: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(artifacts, dict):
        return {"ok": False,
                "reasons": ["artifacts_not_dict"]}
    loaded = artifacts.get("loaded") or {}
    for must in ("rehearsal_contract", "umbrella_consent",
                  "rehearsal_trace", "rehearsal_recheck",
                  "rehearsal_report"):
        if must not in loaded:
            reasons.append(f"missing_loaded:{must}")
    receipts = artifacts.get("stage_receipts") or []
    if not isinstance(receipts, list) or len(receipts) < 1:
        reasons.append("no_stage_receipts")
    return {"ok": not reasons, "reasons": reasons}


def summarize_loaded_replay_artifacts(
    artifacts: Any,
) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {"ok": False, "summary": "no_artifacts"}
    loaded = artifacts.get("loaded") or {}
    sizes = {k: v.get("size_bytes")
             for k, v in loaded.items()
             if isinstance(v, dict)}
    return {
        "ok": bool(artifacts.get("ok")),
        "summary": (
            f"phase40 loader: loaded={len(loaded)} "
            f"stage_receipts="
            f"{artifacts.get('stage_receipt_count')} "
            f"missing={len(artifacts.get('missing') or [])} "
            f"rejected={len(artifacts.get('rejected') or [])}"),
        "sizes_bytes": sizes,
        "phase": _PHASE,
    }


def write_phase40_replay_loader_report(
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
    "reject_unsafe_replay_paths",
    "load_json_artifact",
    "load_phase39_replay_artifacts",
    "validate_loaded_replay_artifacts",
    "summarize_loaded_replay_artifacts",
    "write_phase40_replay_loader_report",
]
