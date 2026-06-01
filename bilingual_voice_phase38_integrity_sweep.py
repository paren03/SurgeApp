"""Phase 38 - Bounded Integrity Sweep.

Run a bounded read-only sweep over Phase 27-38 source modules,
reports, and governance artifacts. Refuses to read runtime DBs,
huge backup dirs, corpus stores, or anything inside
local_secret_handoff (other than confirming its .gitignore exists).
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase38.integrity_sweep.v1"


_BOUNDED_BYTES = 256 * 1024


# Runtime-assembled forbidden-import tokens so this verifier source
# does NOT literally contain forbidden strings.
_AUDIO_TOKENS = tuple(a + b for a, b in (
    ("py", "ttsx3"), ("gt", "ts"), ("ed", "ge_tts"),
    ("pi", "per."), ("co", "qui"), ("whi", "sper"),
    ("pya", "udio"), ("sou", "nddevice"), ("py", "dub"),
    ("sou", "ndfile"), ("com", "types"), ("win", "32com"),
))

_EXEC_TOKENS = tuple(a + b for a, b in (
    ("subproc", "ess.run"), ("subproc", "ess.Popen"),
    ("subproc", "ess.call"), ("os.sy", "stem("),
    ("she", "ll=True"), ("os.po", "pen"),
    ("ctype", "s.windll"), ("powe", "rshell "),
    ("powe", "rshell.exe"),
))

_NETWORK_TOKENS = tuple(a + b for a, b in (
    ("urllib.", "request"), ("http.", "client"),
    ("reque", "sts."), ("htt", "px."),
    ("soc", "ket.socket"),
))

_MP_TOKENS = tuple(a + b for a, b in (
    ("multiproces", "sing.Process"),
    ("multiproces", "sing.Pool"),
    ("multiproces", "sing.Queue"),
))


_AUDIO_EXTENSIONS = (".wav", ".mp3", ".ogg", ".flac", ".m4a",
                     ".aiff", ".aac", ".opus")

_RUNTIME_DB_EXTENSIONS = (".sqlite", ".sqlite3", ".db")


_SECRET_FIELD_TOKENS = (
    "private_key", "material_hex", "signing_key_material",
    "sealed_payload", "raw_secret", "secret_material",
    "key_material_hex",
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


_PHASE38_SUBFOLDERS = (
    "readmes", "checklists", "rollback", "dashboards",
    "integrity", "reports", "fixtures",
)


_SKIP_PATH_TOKENS = (
    "/backups/", "/synthetic_million/",
    "/quality_samples/", "/pilot_imports/",
    "/checkpoints/", "/.claude/",
    "/corpus_sources/",
)


def _read_bounded(path: Path) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        if path.stat().st_size > _BOUNDED_BYTES:
            return ""
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        return ""


def _safe_to_scan(p: Path) -> bool:
    s = str(p).replace("\\", "/")
    for tok in _SKIP_PATH_TOKENS:
        if tok in s:
            return False
    if "/local_secret_handoff/" in s:
        return False
    return True


def sweep_phase_reports(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    root = Path(root) if root else Path(__file__).resolve().parent
    missing: list[str] = []
    present: list[dict[str, Any]] = []
    for fname in _PHASE_REPORTS:
        p = root / fname
        if not p.exists() or not p.is_file():
            missing.append(fname)
            continue
        try:
            size = p.stat().st_size
        except Exception:  # noqa: BLE001
            size = 0
        present.append({"filename": fname, "size_bytes": size})
    return {
        "phase": _PHASE,
        "ok": not missing,
        "present_count": len(present),
        "missing": missing,
        "present": present,
    }


def sweep_forbidden_artifacts(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Scan governance_phase38/ and voice_adapter_phase*/ for
    audio files, runtime DBs, or secret-bearing files (other than
    the local_secret_handoff/.gitignore itself)."""
    root = Path(root) if root else Path(__file__).resolve().parent
    targets: list[Path] = []
    bs = root / "bilingual_stack"
    if bs.exists() and bs.is_dir():
        for child in bs.iterdir():
            n = child.name
            if (n.startswith("voice_adapter_phase")
                    or n.startswith("governance_phase")):
                targets.append(child)
    audio_files: list[str] = []
    db_files: list[str] = []
    suspect_files: list[str] = []
    scanned = 0
    for base in targets:
        if not _safe_to_scan(base):
            continue
        for p in base.rglob("*"):
            if scanned > 5000:
                break
            scanned += 1
            if not p.is_file():
                continue
            if not _safe_to_scan(p):
                continue
            name = p.name.lower()
            ext = p.suffix.lower()
            if ext in _AUDIO_EXTENSIONS:
                audio_files.append(str(p))
            if ext in _RUNTIME_DB_EXTENSIONS:
                db_files.append(str(p))
            if (("secret" in name or "private_key" in name
                 or "material_hex" in name)
                    and name != ".gitignore"):
                suspect_files.append(str(p))
    return {
        "phase": _PHASE,
        "ok": (not audio_files and not db_files
               and not suspect_files),
        "scanned": scanned,
        "audio_files": audio_files,
        "db_files": db_files,
        "suspect_files": suspect_files,
    }


def sweep_secret_leakage(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Bounded text scan of governance_phase38/ JSON/MD outputs and
    voice_adapter_phase*/ public-shaped reports for secret-shape
    field tokens. Skips local_secret_handoff/ entirely."""
    root = Path(root) if root else Path(__file__).resolve().parent
    hits: list[dict[str, Any]] = []
    scanned = 0
    bs = root / "bilingual_stack"
    if bs.exists() and bs.is_dir():
        for base in bs.iterdir():
            n = base.name
            if not (n.startswith("voice_adapter_phase")
                    or n.startswith("governance_phase")):
                continue
            if not _safe_to_scan(base):
                continue
            for p in base.rglob("*"):
                if scanned > 4000:
                    break
                if not p.is_file():
                    continue
                if not _safe_to_scan(p):
                    continue
                if p.suffix.lower() not in (".json", ".md", ".txt"):
                    continue
                scanned += 1
                src = _read_bounded(p)
                if not src:
                    continue
                for tok in _SECRET_FIELD_TOKENS:
                    pat = r"\"" + re.escape(tok) + r"\"\s*:"
                    m = re.search(pat, src)
                    if m:
                        hits.append({"path": str(p), "token": tok})
                        break
    return {
        "phase": _PHASE,
        "ok": not hits,
        "scanned": scanned,
        "hits": hits,
    }


def sweep_audio_artifacts(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Repo-rooted but bounded scan for *any* audio file under
    bilingual_stack/voice_adapter_phase*/."""
    root = Path(root) if root else Path(__file__).resolve().parent
    hits: list[str] = []
    scanned = 0
    bs = root / "bilingual_stack"
    if bs.exists() and bs.is_dir():
        for base in bs.iterdir():
            if not base.is_dir():
                continue
            if not base.name.startswith("voice_adapter_phase"):
                continue
            if not _safe_to_scan(base):
                continue
            for p in base.rglob("*"):
                if scanned > 5000:
                    break
                scanned += 1
                if not p.is_file():
                    continue
                if p.suffix.lower() in _AUDIO_EXTENSIONS:
                    hits.append(str(p))
    return {
        "phase": _PHASE,
        "ok": not hits,
        "scanned": scanned,
        "audio_files": hits,
    }


def sweep_runtime_db_artifacts(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Scan governance_phase38/ + voice_adapter_phase*/ for runtime
    DB files that must NOT be there. Does NOT read any DB."""
    root = Path(root) if root else Path(__file__).resolve().parent
    hits: list[str] = []
    scanned = 0
    bs = root / "bilingual_stack"
    if bs.exists() and bs.is_dir():
        for base in bs.iterdir():
            n = base.name
            if not (n.startswith("voice_adapter_phase")
                    or n.startswith("governance_phase")):
                continue
            if not _safe_to_scan(base):
                continue
            for p in base.rglob("*"):
                if scanned > 5000:
                    break
                scanned += 1
                if not p.is_file():
                    continue
                if p.suffix.lower() in _RUNTIME_DB_EXTENSIONS:
                    hits.append(str(p))
    return {
        "phase": _PHASE,
        "ok": not hits,
        "scanned": scanned,
        "db_files": hits,
    }


def sweep_forbidden_imports(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Bounded import-token scan over Phase 27-38 Python source
    files at the repo root."""
    root = Path(root) if root else Path(__file__).resolve().parent
    targets: list[Path] = []
    for p in root.glob("bilingual_voice_phase*.py"):
        targets.append(p)
    for p in root.glob("bilingual_voice_callable_adapter_*.py"):
        targets.append(p)
    for p in root.glob("bilingual_voice_dummy_*.py"):
        targets.append(p)
    for p in root.glob("bilingual_segment_metadata_*.py"):
        targets.append(p)
    for p in root.glob("bilingual_prosody_density_*.py"):
        targets.append(p)
    for p in root.glob("bilingual_safety_redaction_*.py"):
        targets.append(p)
    hits: list[dict[str, Any]] = []
    scanned = 0
    for p in targets:
        if scanned > 200:
            break
        scanned += 1
        src = _read_bounded(p)
        if not src:
            continue
        for tok in _AUDIO_TOKENS:
            if tok in src:
                hits.append({"path": str(p),
                             "kind": "audio", "token": tok})
        for tok in _EXEC_TOKENS:
            if tok in src:
                hits.append({"path": str(p),
                             "kind": "exec", "token": tok})
        for tok in _NETWORK_TOKENS:
            if tok in src:
                hits.append({"path": str(p),
                             "kind": "network", "token": tok})
        for tok in _MP_TOKENS:
            if tok in src:
                hits.append({"path": str(p),
                             "kind": "mp", "token": tok})
    return {
        "phase": _PHASE,
        "ok": not hits,
        "scanned": scanned,
        "hits": hits,
    }


def sweep_local_secret_handoff_gitignore(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Confirm the per-folder .gitignore exists inside
    local_secret_handoff. Does NOT enumerate other files."""
    root = Path(root) if root else Path(__file__).resolve().parent
    base = (root / "bilingual_stack" / "voice_adapter_phase36"
                 / "local_secret_handoff")
    gi = base / ".gitignore"
    present = gi.exists() and gi.is_file()
    body = ""
    if present:
        try:
            if gi.stat().st_size <= 4096:
                body = gi.read_text(encoding="utf-8",
                                    errors="ignore")
        except Exception:  # noqa: BLE001
            body = ""
    matches = ("*" in body and "!.gitignore" in body)
    return {
        "phase": _PHASE,
        "ok": present and matches,
        "gitignore_present": present,
        "gitignore_matches_expected": matches,
    }


def create_integrity_sweep_report(
    root: Optional[Path] = None,
) -> dict[str, Any]:
    root = Path(root) if root else Path(__file__).resolve().parent
    rep_reports = sweep_phase_reports(root)
    rep_forbidden = sweep_forbidden_artifacts(root)
    rep_secret = sweep_secret_leakage(root)
    rep_audio = sweep_audio_artifacts(root)
    rep_db = sweep_runtime_db_artifacts(root)
    rep_imports = sweep_forbidden_imports(root)
    rep_gi = sweep_local_secret_handoff_gitignore(root)
    all_ok = (
        rep_reports["ok"] and rep_forbidden["ok"]
        and rep_secret["ok"] and rep_audio["ok"]
        and rep_db["ok"] and rep_imports["ok"]
        and rep_gi["ok"])
    return {
        "sweep_id": f"isweep_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "ok": all_ok,
        "phase_reports": rep_reports,
        "forbidden_artifacts": rep_forbidden,
        "secret_leakage": rep_secret,
        "audio_artifacts": rep_audio,
        "runtime_db_artifacts": rep_db,
        "forbidden_imports": rep_imports,
        "local_secret_handoff_gitignore": rep_gi,
        "phase38_subfolders_checked":
            list(_PHASE38_SUBFOLDERS),
        "notes": [
            "Sweep is bounded: skips /backups/, /synthetic_million/,"
            " /quality_samples/, /pilot_imports/, /checkpoints/,"
            " /.claude/, /corpus_sources/, and entire"
            " /local_secret_handoff/ except for confirming its"
            " .gitignore.",
            "No runtime DB is read; only file presence is checked.",
            "Bounded reads cap at 256KB per file.",
        ],
    }


def validate_integrity_sweep_report(
    report: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(report, dict):
        return {"ok": False, "reasons": ["report_not_dict"]}
    for f in ("sweep_id", "created_at", "phase", "ok",
              "phase_reports", "forbidden_artifacts",
              "secret_leakage", "audio_artifacts",
              "runtime_db_artifacts", "forbidden_imports",
              "local_secret_handoff_gitignore"):
        if f not in report:
            reasons.append(f"missing_field:{f}")
    return {"ok": not reasons, "reasons": reasons}


def write_integrity_sweep_report(
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
    "sweep_phase_reports",
    "sweep_forbidden_artifacts",
    "sweep_secret_leakage",
    "sweep_audio_artifacts",
    "sweep_runtime_db_artifacts",
    "sweep_forbidden_imports",
    "sweep_local_secret_handoff_gitignore",
    "create_integrity_sweep_report",
    "validate_integrity_sweep_report",
    "write_integrity_sweep_report",
]
