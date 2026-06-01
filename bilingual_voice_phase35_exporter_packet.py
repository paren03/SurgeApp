"""Phase 35 - Exporter Packet.

References Phase 34 witness artifacts by local path + streamed
SHA-256 hash + size. Never carries secret material.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase35.exporter_packet.v1"


_CHUNK = 64 * 1024


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


_EXCLUDED_PATTERNS = (
    "/backups/", "/synthetic_million/", "/quality_samples/",
    "/pilot_imports/", "/checkpoints/", "ledger.sqlite",
    "ruvector.db", "luna_vocabulary.sqlite",
    "russian_lexicon.sqlite", "russian_memory.sqlite",
    "bilingual_links.sqlite",
)


# Reject path-like inputs that look remote / shell / scheme-prefixed
_URL_OR_REMOTE_RE = re.compile(
    r"^(https?|ftp|ftps|file|smb|ssh|sftp|git|ws|wss|mailto):",
    re.IGNORECASE)


_SHELL_META_RE = re.compile(r"[;|&`$<>\n\r]")


def _new_id() -> str:
    return f"expk_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _is_excluded(path: str) -> bool:
    s = str(path).replace("\\", "/").lower()
    return any(p in s for p in (e.lower()
                                 for e in _EXCLUDED_PATTERNS))


def _is_remote_or_command_path(p: str) -> tuple[bool, str]:
    s = str(p or "")
    if _URL_OR_REMOTE_RE.match(s):
        return True, "url_or_remote_scheme"
    if _SHELL_META_RE.search(s):
        return True, "shell_metacharacter"
    if "\x00" in s:
        return True, "null_byte"
    if len(s) > 1024:
        return True, "path_too_long"
    return False, ""


def _hash_file(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "reason": "file_not_found",
                "path": str(p)}
    if _is_excluded(str(p)):
        return {"ok": False, "reason": "excluded_pattern",
                "path": str(p)}
    rem, reason = _is_remote_or_command_path(str(p))
    if rem:
        return {"ok": False, "reason": reason, "path": str(p)}
    h = hashlib.sha256()
    size = 0
    try:
        with p.open("rb") as fh:
            while True:
                chunk = fh.read(_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"read_error:{type(e).__name__}",
                "path": str(p)}
    return {"ok": True, "path": str(p),
            "sha256": h.hexdigest(), "size_bytes": size}


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "exchange_id",
    "artifact_paths", "artifact_hashes", "artifact_sizes",
    "exporter_summary", "boundary_summary",
    "no_secret_statement", "no_audio_statement",
    "no_network_statement", "no_subprocess_statement",
    "metadata",
)


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


def create_exporter_packet(
    package_path: str,
    public_key_path: str,
    manifest_path: str,
    guide_path: str = "",
    contract: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    artifact_paths: dict[str, str] = {
        "witness_package": str(package_path or ""),
        "public_key_descriptor": str(public_key_path or ""),
        "integrity_manifest": str(manifest_path or ""),
    }
    if guide_path:
        artifact_paths["operator_guide"] = str(guide_path)
    artifact_hashes: dict[str, str] = {}
    artifact_sizes: dict[str, int] = {}
    skipped: list[dict[str, Any]] = []
    for name, ap in artifact_paths.items():
        r = _hash_file(ap)
        if r["ok"]:
            artifact_hashes[name] = r["sha256"]
            artifact_sizes[name] = r["size_bytes"]
        else:
            skipped.append({"name": name, "path": ap,
                             "reason": r["reason"]})
    ec = contract if isinstance(contract, dict) else {}
    exchange_id = str(ec.get("exchange_id") or
                       f"xch_{int(time.time())}_"
                       f"{uuid.uuid4().hex[:10]}")
    return {
        "packet_id": _new_id(),
        "created_at": time.time(),
        "exchange_id": exchange_id,
        "artifact_paths": artifact_paths,
        "artifact_hashes": artifact_hashes,
        "artifact_sizes": artifact_sizes,
        "skipped_artifacts": skipped,
        "exporter_summary": {
            "exporter_id": str(ec.get("exporter_id") or
                                 "local_exporter"),
            "total_artifacts": len(artifact_paths),
            "hashed_artifacts": len(artifact_hashes),
            "skipped_count": len(skipped),
        },
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "test_only": True,
            "no_network": True,
            "no_subprocess": True,
            "no_multiprocessing": True,
            "no_audio": True,
        },
        "no_secret_statement":
            "exporter packet carries no signing secret material",
        "no_audio_statement":
            "exporter packet carries no audio bytes or paths",
        "no_network_statement":
            "exporter packet is local-file-only; no URLs",
        "no_subprocess_statement":
            "exporter packet does not spawn or reference any process",
        "metadata": dict(metadata or {}),
        "phase": _PHASE,
    }


def compute_packet_artifact_hashes(
    packet: Any,
) -> dict[str, dict[str, Any]]:
    if not isinstance(packet, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for name, path in (packet.get("artifact_paths") or {}).items():
        out[name] = _hash_file(str(path))
    return out


def validate_exporter_packet(packet: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(packet, dict):
        return {"ok": False, "reasons": ["packet_not_dict"]}
    for f in _REQUIRED_PACKET_FIELDS:
        if f not in packet:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(packet)
    if hits:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(hits))))
    bsum = packet.get("boundary_summary") or {}
    for k in ("execution_blocked", "dry_run", "no_network",
              "no_subprocess", "no_multiprocessing", "no_audio"):
        if bsum.get(k) is not True:
            reasons.append(f"boundary_{k}_not_true")
    # Required artifacts hashed (operator_guide optional)
    paths = packet.get("artifact_paths") or {}
    hashes = packet.get("artifact_hashes") or {}
    for name in ("witness_package", "public_key_descriptor",
                 "integrity_manifest"):
        if name not in paths:
            reasons.append(f"missing_artifact:{name}")
            continue
        if name not in hashes:
            reasons.append(f"missing_hash:{name}")
    # Reject URL/remote/command-like paths
    for name, p in paths.items():
        rem, reason = _is_remote_or_command_path(str(p))
        if rem:
            reasons.append(f"bad_path:{name}:{reason}")
    try:
        json.dumps(packet, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_exporter_packet(packet: Any) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": True,
        "summary": (
            f"phase35 exporter packet: packet_id="
            f"{packet.get('packet_id')} "
            f"hashed={len(packet.get('artifact_hashes') or {})} "
            f"skipped={len(packet.get('skipped_artifacts') or [])}"),
        "packet_id": packet.get("packet_id"),
        "exchange_id": packet.get("exchange_id"),
        "phase": _PHASE,
    }


def write_exporter_packet(
    packet: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(packet)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_exporter_packet(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_exporter_packet_report(
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
    "create_exporter_packet",
    "validate_exporter_packet",
    "compute_packet_artifact_hashes",
    "summarize_exporter_packet",
    "write_exporter_packet",
    "read_exporter_packet",
    "write_exporter_packet_report",
]
