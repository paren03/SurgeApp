"""Phase 35 - Handshake Record.

Records the exporter → witness → verification result handoff for a
single Phase 35 exchange. Detects exchange_id mismatch, artifact hash
mismatch, and repeated/duplicate exchange_id (replay).
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase35.handshake_record.v1"


_REQUIRED_FIELDS = (
    "handshake_id", "created_at", "exchange_id",
    "exporter_packet_id", "witness_input_id",
    "witness_output_id", "artifact_hash_summary",
    "verification_status", "replay_protection_summary",
    "boundary_summary", "receipt_summary", "notes", "phase",
)


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


_SEEN_EXCHANGE_IDS: set[str] = set()


def _new_id() -> str:
    return f"hsh_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_summary(hashes: Any) -> str:
    try:
        body = json.dumps(hashes or {}, sort_keys=True, default=str,
                          ensure_ascii=False)
    except Exception:  # noqa: BLE001
        return ""
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_handshake_record(
    exchange_contract: Any,
    exporter_packet: Any,
    witness_input: Any,
    witness_output: Any,
) -> dict[str, Any]:
    ec = exchange_contract if isinstance(exchange_contract,
                                           dict) else {}
    pkt = exporter_packet if isinstance(exporter_packet,
                                          dict) else {}
    win = witness_input if isinstance(witness_input, dict) else {}
    wout = witness_output if isinstance(witness_output,
                                          dict) else {}
    ec_eid = str(ec.get("exchange_id") or "")
    pkt_eid = str(pkt.get("exchange_id") or "")
    win_eid = str(win.get("exchange_id") or "")
    wout_eid = str(wout.get("exchange_id") or "")
    exchange_id = ec_eid or pkt_eid or win_eid or wout_eid
    seen_before = exchange_id in _SEEN_EXCHANGE_IDS
    if exchange_id:
        _SEEN_EXCHANGE_IDS.add(exchange_id)
    pkt_hashes = pkt.get("artifact_hashes") or {}
    win_hashes = win.get("artifact_hashes") or {}
    hash_mismatch = any(
        win_hashes.get(name) != pkt_hashes.get(name)
        for name in set(pkt_hashes.keys()) | set(win_hashes.keys()))
    eid_mismatch = len({e for e in (ec_eid, pkt_eid, win_eid,
                                       wout_eid) if e}) > 1
    return {
        "handshake_id": _new_id(),
        "created_at": time.time(),
        "exchange_id": exchange_id,
        "exporter_packet_id": str(pkt.get("packet_id") or ""),
        "witness_input_id": str(win.get("witness_input_id") or ""),
        "witness_output_id": str(wout.get("witness_output_id") or ""),
        "artifact_hash_summary": _hash_summary(pkt_hashes),
        "verification_status": str(wout.get("status") or "unknown"),
        "replay_protection_summary": {
            "exchange_id_seen_before": bool(seen_before),
            "exchange_id_mismatch_detected": bool(eid_mismatch),
            "artifact_hash_mismatch_detected": bool(hash_mismatch),
        },
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "no_network": True,
            "no_subprocess": True,
            "no_multiprocessing": True,
            "no_audio": True,
        },
        "receipt_summary": {
            "checks_passed": list(wout.get("checks_passed") or []),
            "checks_failed": list(wout.get("checks_failed") or []),
            "checks_warned": list(wout.get("checks_warned") or []),
        },
        "notes": ("phase35 handshake record; local file-based; "
                  "no network; no subprocess"),
        "phase": _PHASE,
    }


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


def validate_handshake_record(record: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(record, dict):
        return {"ok": False, "reasons": ["record_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in record:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(record)
    if hits:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(hits))))
    bsum = record.get("boundary_summary") or {}
    for k in ("execution_blocked", "dry_run", "no_network",
              "no_subprocess", "no_multiprocessing", "no_audio"):
        if bsum.get(k) is not True:
            reasons.append(f"boundary_{k}_not_true")
    try:
        json.dumps(record, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_handshake_record(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {"ok": False, "summary": "no_record"}
    rp = record.get("replay_protection_summary") or {}
    return {
        "ok": True,
        "summary": (
            f"phase35 handshake: status="
            f"{record.get('verification_status')} "
            f"exchange_id_mismatch="
            f"{bool(rp.get('exchange_id_mismatch_detected'))} "
            f"hash_mismatch="
            f"{bool(rp.get('artifact_hash_mismatch_detected'))}"),
        "handshake_id": record.get("handshake_id"),
        "phase": _PHASE,
    }


def detect_replay_or_mismatch(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {"ok": False, "reasons": ["record_not_dict"]}
    rp = record.get("replay_protection_summary") or {}
    flags: list[str] = []
    if rp.get("exchange_id_seen_before"):
        flags.append("replay_exchange_id")
    if rp.get("exchange_id_mismatch_detected"):
        flags.append("exchange_id_mismatch")
    if rp.get("artifact_hash_mismatch_detected"):
        flags.append("artifact_hash_mismatch")
    return {"ok": not flags, "flags": flags, "phase": _PHASE}


def write_handshake_record(
    record: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(record)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_handshake_record(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_handshake_record_report(
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
    "create_handshake_record",
    "validate_handshake_record",
    "summarize_handshake_record",
    "detect_replay_or_mismatch",
    "write_handshake_record",
    "read_handshake_record",
    "write_handshake_record_report",
]
