"""Phase 36 - Public Descriptor Bridge.

Derives a public descriptor (no secret material) from a Phase 36
sealed envelope. Compatible with Phase 34 public descriptor shape.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase36.public_descriptor_bridge.v1"


_FORBIDDEN_PUBLIC_FIELDS = (
    "private_key", "secret", "material_hex",
    "signing_key_material", "sealed_payload",
    "raw_key", "hmac_key",
)


_FORBIDDEN_LABEL_TOKENS = (
    "prod", "production", "live", "real", "kms", "cloud", "external",
)


_REQUIRED_PUBLIC_FIELDS = (
    "key_id", "key_label", "algorithm", "test_only",
    "public_fingerprint", "envelope_id", "created_at", "phase",
)


def create_public_descriptor_from_handoff(
    envelope: Any,
) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    label = str(envelope.get("key_label") or "")
    if any(t in label.lower() for t in _FORBIDDEN_LABEL_TOKENS):
        return {"ok": False,
                "reasons": [f"forbidden_label:{label}"]}
    if envelope.get("test_only") is not True:
        return {"ok": False, "reasons": ["test_only_required"]}
    return {
        "key_id": str(envelope.get("key_id") or ""),
        "key_label": label,
        "algorithm": str(envelope.get("algorithm") or
                          "HMAC-SHA256"),
        "test_only": True,
        "public_fingerprint": str(
            envelope.get("public_fingerprint") or ""),
        "envelope_id": str(envelope.get("envelope_id") or ""),
        "created_at": float(envelope.get("created_at")
                              or time.time()),
        "phase": _PHASE,
        # Phase 34's `fingerprint` field name kept for compatibility
        # with Phase 34 public descriptor validators.
        "fingerprint": str(
            envelope.get("public_fingerprint") or ""),
    }


def validate_public_descriptor_from_handoff(
    descriptor: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(descriptor, dict):
        return {"ok": False, "reasons": ["descriptor_not_dict"]}
    for f in _REQUIRED_PUBLIC_FIELDS:
        if f not in descriptor:
            reasons.append(f"missing_field:{f}")
    for k in _FORBIDDEN_PUBLIC_FIELDS:
        if k in descriptor:
            reasons.append(f"forbidden_field:{k}")
    if descriptor.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    label_l = str(descriptor.get("key_label") or "").lower()
    if any(t in label_l for t in _FORBIDDEN_LABEL_TOKENS):
        reasons.append(
            f"forbidden_label:{descriptor.get('key_label')}")
    fp = str(descriptor.get("public_fingerprint") or "")
    if len(fp) != 64:
        reasons.append("fingerprint_bad_length")
    return {"ok": not reasons, "reasons": reasons}


def compare_handoff_to_public_descriptor(
    envelope: Any,
    descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(envelope, dict) or \
            not isinstance(descriptor, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    if str(descriptor.get("key_id")) != \
            str(envelope.get("key_id")):
        reasons.append("key_id_mismatch")
    if str(descriptor.get("algorithm")) != \
            str(envelope.get("algorithm")):
        reasons.append("algorithm_mismatch")
    if str(descriptor.get("public_fingerprint")) != \
            str(envelope.get("public_fingerprint")):
        reasons.append("fingerprint_mismatch")
    if str(descriptor.get("envelope_id")) != \
            str(envelope.get("envelope_id")):
        reasons.append("envelope_id_mismatch")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def write_handoff_public_descriptor(
    descriptor: dict[str, Any],
    output_path: str,
) -> str:
    if not isinstance(descriptor, dict):
        raise ValueError("descriptor_not_dict")
    for k in _FORBIDDEN_PUBLIC_FIELDS:
        if k in descriptor:
            raise ValueError(
                f"refuse_to_write_secret_field:{k}")
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(descriptor)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_handoff_public_descriptor(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_public_descriptor_bridge_report(
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
    "create_public_descriptor_from_handoff",
    "validate_public_descriptor_from_handoff",
    "compare_handoff_to_public_descriptor",
    "write_handoff_public_descriptor",
    "read_handoff_public_descriptor",
    "write_public_descriptor_bridge_report",
]
