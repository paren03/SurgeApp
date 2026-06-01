"""Phase 34 - Public Key Descriptor Export.

Strips signing key material so a public/test descriptor can be shipped
alongside the witness package. Never writes raw key material.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase34.key_descriptor_export.v1"


_FORBIDDEN_KEY_FIELDS = (
    "private_key", "secret", "material_hex",
    "signing_key_material",
)

_FORBIDDEN_LABEL_TOKENS = (
    "prod", "production", "live", "real", "kms", "cloud", "external",
)


_REQUIRED_PUBLIC_FIELDS = (
    "key_id", "key_label", "algorithm", "test_only",
    "created_at", "fingerprint", "phase",
)


def _fingerprint(key_descriptor: dict[str, Any]) -> str:
    """Stable identity fingerprint of a key descriptor that does NOT
    derive from raw material — only from id + label + algorithm."""
    parts = "|".join((
        str(key_descriptor.get("key_id") or ""),
        str(key_descriptor.get("key_label") or
            key_descriptor.get("label") or ""),
        str(key_descriptor.get("algorithm") or ""),
    ))
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def create_public_test_key_descriptor(
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(key_descriptor, dict):
        return {"ok": False, "reasons": ["key_descriptor_not_dict"]}
    label = str(key_descriptor.get("label") or
                 key_descriptor.get("key_label") or "")
    label_l = label.lower()
    if any(t in label_l for t in _FORBIDDEN_LABEL_TOKENS):
        return {"ok": False,
                "reasons": [f"forbidden_label:{label}"]}
    if key_descriptor.get("test_only") is not True:
        return {"ok": False, "reasons": ["test_only_required"]}
    return {
        "key_id": str(key_descriptor.get("key_id") or ""),
        "key_label": label,
        "algorithm": str(key_descriptor.get("algorithm") or
                          "HMAC-SHA256"),
        "test_only": True,
        "created_at": float(key_descriptor.get("created_at")
                              or time.time()),
        "fingerprint": _fingerprint(key_descriptor),
        "phase": _PHASE,
        "notes": ("phase34 public key descriptor; no raw material; "
                  "test-only"),
    }


def validate_public_key_descriptor(
    descriptor: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(descriptor, dict):
        return {"ok": False, "reasons": ["descriptor_not_dict"]}
    for f in _REQUIRED_PUBLIC_FIELDS:
        if f not in descriptor:
            reasons.append(f"missing_field:{f}")
    for k in _FORBIDDEN_KEY_FIELDS:
        if k in descriptor:
            reasons.append(f"forbidden_field:{k}")
    if descriptor.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    label_l = str(descriptor.get("key_label") or "").lower()
    if any(t in label_l for t in _FORBIDDEN_LABEL_TOKENS):
        reasons.append(f"forbidden_label:{descriptor.get('key_label')}")
    fp = str(descriptor.get("fingerprint") or "")
    if len(fp) != 64:
        reasons.append("fingerprint_bad_length")
    return {"ok": not reasons, "reasons": reasons}


def strip_key_secret_material(
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(key_descriptor, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in key_descriptor.items():
        ks = str(k).lower()
        if ks in _FORBIDDEN_KEY_FIELDS:
            continue
        out[k] = v
    return out


def compare_key_descriptor_identity(
    private_descriptor: Any,
    public_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(private_descriptor, dict) or \
            not isinstance(public_descriptor, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    if str(public_descriptor.get("key_id")) != \
            str(private_descriptor.get("key_id")):
        reasons.append("key_id_mismatch")
    pub_alg = str(public_descriptor.get("algorithm") or "")
    priv_alg = str(private_descriptor.get("algorithm") or "")
    if pub_alg != priv_alg:
        reasons.append("algorithm_mismatch")
    expected_fp = _fingerprint(private_descriptor)
    if str(public_descriptor.get("fingerprint")) != expected_fp:
        reasons.append("fingerprint_mismatch")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def write_public_key_descriptor(
    descriptor: Any,
    output_path: str,
) -> str:
    if not isinstance(descriptor, dict):
        raise ValueError("descriptor_not_dict")
    for k in _FORBIDDEN_KEY_FIELDS:
        if k in descriptor:
            raise ValueError(f"refuse_to_write_secret_field:{k}")
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(descriptor)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_public_key_descriptor(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_key_descriptor_export_report(
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
    "create_public_test_key_descriptor",
    "validate_public_key_descriptor",
    "strip_key_secret_material",
    "compare_key_descriptor_identity",
    "write_public_key_descriptor",
    "read_public_key_descriptor",
    "write_key_descriptor_export_report",
]
