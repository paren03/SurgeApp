"""Phase 35 - Witness Input.

Bounded-read view of an exporter packet, suitable for handing to the
witness verifier. Rejects URL / shell / oversized / command-like paths.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase35.witness_input.v1"


_MAX_DEFAULT_BYTES = 5_000_000


_URL_OR_REMOTE_RE = re.compile(
    r"^(https?|ftp|ftps|file|smb|ssh|sftp|git|ws|wss|mailto):",
    re.IGNORECASE)


_SHELL_META_RE = re.compile(r"[;|&`$<>\n\r]")


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id", "private_key", "secret",
    "signing_key_material", "material_hex",
    "socket", "url", "remote_host", "remote_port",
    "http_url", "https_url",
)


_REQUIRED_FIELDS = (
    "witness_input_id", "created_at", "exchange_id",
    "exporter_packet_id", "artifact_paths",
    "artifact_hashes", "artifact_sizes",
    "max_artifact_bytes", "boundary_summary",
    "bound_check_results", "phase",
)


def _new_id() -> str:
    return f"wit_in_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _is_bad_path(p: str) -> tuple[bool, str]:
    s = str(p or "")
    if not s:
        return True, "empty_path"
    if _URL_OR_REMOTE_RE.match(s):
        return True, "remote_or_url_scheme"
    if _SHELL_META_RE.search(s):
        return True, "shell_metacharacter"
    if "\x00" in s:
        return True, "null_byte"
    if len(s) > 1024:
        return True, "path_too_long"
    return False, ""


def _scan_forbidden(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _FORBIDDEN_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def create_witness_input(
    exporter_packet: Any,
    max_artifact_bytes: int = _MAX_DEFAULT_BYTES,
) -> dict[str, Any]:
    pkt = exporter_packet if isinstance(exporter_packet,
                                          dict) else {}
    cap = max(1, min(int(max_artifact_bytes or 1),
                       _MAX_DEFAULT_BYTES * 2))
    paths = pkt.get("artifact_paths") or {}
    hashes = pkt.get("artifact_hashes") or {}
    sizes = pkt.get("artifact_sizes") or {}
    bound_results: dict[str, dict[str, Any]] = {}
    for name, p in paths.items():
        bad, reason = _is_bad_path(str(p))
        size_ok = True
        if str(name) in sizes:
            sz = int(sizes.get(str(name)) or 0)
            size_ok = sz <= cap
            if not size_ok:
                reason = (reason + ";" if reason else "") + \
                    f"size_exceeds_{cap}"
        bound_results[name] = {
            "ok": (not bad) and size_ok,
            "reasons": ([reason] if reason else []),
            "size_bytes": int(sizes.get(name) or 0),
        }
    return {
        "witness_input_id": _new_id(),
        "created_at": time.time(),
        "exchange_id": str(pkt.get("exchange_id") or ""),
        "exporter_packet_id": str(pkt.get("packet_id") or ""),
        "artifact_paths": dict(paths),
        "artifact_hashes": dict(hashes),
        "artifact_sizes": dict(sizes),
        "max_artifact_bytes": cap,
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "no_network": True,
            "no_subprocess": True,
            "no_multiprocessing": True,
            "no_audio": True,
            "bounded_read_enforced": True,
        },
        "bound_check_results": bound_results,
        "phase": _PHASE,
    }


def validate_witness_input(witness_input: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(witness_input, dict):
        return {"ok": False, "reasons": ["witness_input_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in witness_input:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(witness_input)
    if hits:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(hits))))
    bsum = witness_input.get("boundary_summary") or {}
    for k in ("execution_blocked", "dry_run", "no_network",
              "no_subprocess", "no_multiprocessing", "no_audio",
              "bounded_read_enforced"):
        if bsum.get(k) is not True:
            reasons.append(f"boundary_{k}_not_true")
    # Each bound result must be ok
    bcr = witness_input.get("bound_check_results") or {}
    for name, r in bcr.items():
        if isinstance(r, dict) and r.get("ok") is False:
            reasons.append(f"bound_check_failed:{name}:" +
                           ",".join(r.get("reasons", [])))
    # Required artifacts present in paths (operator_guide optional)
    paths = witness_input.get("artifact_paths") or {}
    for required in ("witness_package", "public_key_descriptor",
                     "integrity_manifest"):
        if required not in paths:
            reasons.append(f"missing_required_artifact:{required}")
    try:
        json.dumps(witness_input, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def check_witness_input_bounds(witness_input: Any) -> dict[str, Any]:
    if not isinstance(witness_input, dict):
        return {"ok": False, "reasons": ["witness_input_not_dict"]}
    cap = int(witness_input.get("max_artifact_bytes") or 0)
    sizes = witness_input.get("artifact_sizes") or {}
    bad: list[str] = []
    for name, sz in sizes.items():
        try:
            if int(sz) > cap:
                bad.append(f"{name}:{sz}>{cap}")
        except Exception:  # noqa: BLE001
            bad.append(f"{name}:size_unparseable")
    return {"ok": not bad, "violations": bad, "phase": _PHASE}


def reject_remote_or_command_paths(
    witness_input: Any,
) -> dict[str, Any]:
    if not isinstance(witness_input, dict):
        return {"ok": False, "reasons": ["witness_input_not_dict"]}
    bad: list[str] = []
    for name, p in (witness_input.get("artifact_paths") or {}).items():
        is_bad, reason = _is_bad_path(str(p))
        if is_bad:
            bad.append(f"{name}:{reason}")
    return {"ok": not bad, "violations": bad, "phase": _PHASE}


def summarize_witness_input(witness_input: Any) -> dict[str, Any]:
    if not isinstance(witness_input, dict):
        return {"ok": False, "summary": "no_input"}
    return {
        "ok": True,
        "summary": (
            f"phase35 witness input: id="
            f"{witness_input.get('witness_input_id')} "
            f"artifacts={len(witness_input.get('artifact_paths') or {})}"),
        "witness_input_id": witness_input.get("witness_input_id"),
        "exchange_id": witness_input.get("exchange_id"),
        "phase": _PHASE,
    }


def write_witness_input(
    witness_input: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(witness_input)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_witness_input(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_witness_input_report(
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
    "create_witness_input",
    "validate_witness_input",
    "check_witness_input_bounds",
    "reject_remote_or_command_paths",
    "summarize_witness_input",
    "write_witness_input",
    "read_witness_input",
    "write_witness_input_report",
]
