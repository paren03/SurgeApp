"""Phase 36 - Local Sealed Key Handoff Envelope.

Local-only, test-only envelope. Secret material lives in
`sealed_payload`; the envelope as a whole may only be written into
`local_secret_handoff` and only with `allow_secret_write=True`.

The sealing here is a deterministic local test wrapping (base64 +
fixed obfuscation key derived from `envelope_id` + `consent_marker`).
This is explicitly NOT production encryption. It exists only so the
test harness can prove the protocol's behavior without writing raw
HMAC key bytes verbatim into a single JSON line.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase36_secret_boundary as sb


_PHASE = "phase36.handoff_envelope.v1"


_FORBIDDEN_LABEL_TOKENS = (
    "prod", "production", "live", "real", "kms", "cloud", "external",
)


_REQUIRED_ENVELOPE_FIELDS = (
    "envelope_id", "created_at", "envelope_label", "test_only",
    "algorithm", "key_id", "key_label", "consent_marker_hash",
    "secret_material_present", "sealed_payload",
    "public_fingerprint", "allowed_use", "forbidden_use",
    "expiration_hint", "cleanup_instructions", "metadata",
)


_PUBLIC_FORBIDDEN_FIELDS_IN_REPORTS = (
    "private_key", "secret", "material_hex",
    "signing_key_material", "raw_key", "hmac_key",
    "sealed_payload",
)


def _new_id() -> str:
    return f"henv_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_consent_marker(consent_marker: str,
                          envelope_id: str) -> str:
    if not consent_marker:
        return ""
    h = hashlib.sha256()
    h.update(str(envelope_id or "").encode("utf-8"))
    h.update(b"|")
    h.update(str(consent_marker or "").encode("utf-8"))
    return h.hexdigest()


def _fingerprint(key_descriptor: dict[str, Any]) -> str:
    parts = "|".join((
        str(key_descriptor.get("key_id") or ""),
        str(key_descriptor.get("label") or
            key_descriptor.get("key_label") or ""),
        str(key_descriptor.get("algorithm") or ""),
    ))
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def _wrap_key(material_hex: str, envelope_id: str,
              consent_marker: str) -> str:
    """Deterministic test wrapping. NOT production encryption."""
    mat = bytes.fromhex(material_hex)
    seed = (envelope_id + "|" + consent_marker).encode("utf-8")
    keystream = hashlib.sha256(seed).digest()
    # Repeat keystream to cover material; XOR
    out = bytearray()
    for i, b in enumerate(mat):
        out.append(b ^ keystream[i % len(keystream)])
    return base64.b64encode(bytes(out)).decode("ascii")


def _unwrap_key(sealed_b64: str, envelope_id: str,
                consent_marker: str) -> str:
    try:
        wrapped = base64.b64decode(sealed_b64)
    except Exception:  # noqa: BLE001
        return ""
    seed = (envelope_id + "|" + consent_marker).encode("utf-8")
    keystream = hashlib.sha256(seed).digest()
    out = bytearray()
    for i, b in enumerate(wrapped):
        out.append(b ^ keystream[i % len(keystream)])
    return out.hex()


def create_key_handoff_envelope(
    key_descriptor: Any,
    consent_marker: str = "",
    envelope_label: str = "phase36_test_handoff",
) -> dict[str, Any]:
    if not isinstance(key_descriptor, dict):
        return {"ok": False,
                "reasons": ["key_descriptor_not_dict"]}
    if not consent_marker:
        return {"ok": False,
                "reasons": ["consent_marker_required"]}
    if key_descriptor.get("test_only") is not True:
        return {"ok": False,
                "reasons": ["test_only_required"]}
    label = str(key_descriptor.get("label") or
                 key_descriptor.get("key_label") or "")
    label_l = label.lower()
    if any(t in label_l for t in _FORBIDDEN_LABEL_TOKENS):
        return {"ok": False,
                "reasons": [f"forbidden_label:{label}"]}
    envelope_id = _new_id()
    material_hex = str(key_descriptor.get("material_hex") or "")
    if len(material_hex) < 32:
        return {"ok": False,
                "reasons": ["material_hex_too_short"]}
    sealed = _wrap_key(material_hex, envelope_id, consent_marker)
    return {
        "envelope_id": envelope_id,
        "created_at": time.time(),
        "envelope_label": str(envelope_label or
                                "phase36_test_handoff"),
        "test_only": True,
        "algorithm": str(key_descriptor.get("algorithm") or
                          "HMAC-SHA256"),
        "key_id": str(key_descriptor.get("key_id") or ""),
        "key_label": label,
        "consent_marker_hash":
            _hash_consent_marker(consent_marker, envelope_id),
        "secret_material_present": True,
        "sealed_payload": sealed,
        "public_fingerprint": _fingerprint(key_descriptor),
        "allowed_use": ["local_offline_verification",
                         "phase34_witness_verification",
                         "phase35_exchange_verification"],
        "forbidden_use": [
            "production_signing", "cloud_kms_handoff",
            "network_transfer", "audio_synthesis",
            "tts_invocation", "subprocess_execution",
            "git_commit", "ci_pipeline_secret",
        ],
        "expiration_hint": time.time() + 3600,
        "cleanup_instructions": [
            "Rotate the test signing key after each verification "
            "run.",
            "Delete the envelope file when verification is "
            "complete.",
            "Confirm `git status` shows no envelope files staged.",
            "If reuse is needed, generate a fresh consent_marker.",
        ],
        "metadata": {},
        "phase": _PHASE,
        "sealing_note": ("DETERMINISTIC LOCAL TEST WRAPPING; "
                          "NOT PRODUCTION ENCRYPTION; never use "
                          "for real secret protection"),
    }


def validate_key_handoff_envelope(envelope: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    for f in _REQUIRED_ENVELOPE_FIELDS:
        if f not in envelope:
            reasons.append(f"missing_field:{f}")
    if envelope.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    label_l = str(envelope.get("key_label") or "").lower()
    if any(t in label_l for t in _FORBIDDEN_LABEL_TOKENS):
        reasons.append(f"forbidden_label:{envelope.get('key_label')}")
    if not str(envelope.get("consent_marker_hash") or ""):
        reasons.append("consent_marker_hash_missing")
    fp = str(envelope.get("public_fingerprint") or "")
    if len(fp) != 64:
        reasons.append("fingerprint_bad_length")
    return {"ok": not reasons, "reasons": reasons}


def seal_key_handoff_envelope(
    envelope: dict[str, Any],
) -> dict[str, Any]:
    # `sealed_payload` is already produced at create-time. This
    # function returns a shallow copy with the same payload — useful
    # for callers that prefer an explicit seal step.
    if not isinstance(envelope, dict):
        return {}
    return dict(envelope)


def unseal_key_handoff_envelope(
    sealed_envelope: Any,
    consent_marker: str = "",
) -> dict[str, Any]:
    if not isinstance(sealed_envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    if not consent_marker:
        return {"ok": False, "reasons": ["consent_marker_missing"]}
    expected = _hash_consent_marker(
        consent_marker, sealed_envelope.get("envelope_id") or "")
    if expected != sealed_envelope.get("consent_marker_hash"):
        return {"ok": False, "reasons": ["consent_marker_mismatch"]}
    material_hex = _unwrap_key(
        str(sealed_envelope.get("sealed_payload") or ""),
        str(sealed_envelope.get("envelope_id") or ""),
        consent_marker)
    if not material_hex:
        return {"ok": False, "reasons": ["unwrap_failed"]}
    # Return an in-memory descriptor compatible with Phase 32 HMAC
    # verify path, but flagged as in-memory-only.
    return {
        "ok": True,
        "key_descriptor": {
            "key_id": sealed_envelope.get("key_id"),
            "label": sealed_envelope.get("key_label"),
            "algorithm": sealed_envelope.get("algorithm"),
            "test_only": True,
            "material_hex": material_hex,
            "created_at": sealed_envelope.get("created_at"),
            "phase": "phase32.signing_policy.v1",
        },
        "phase": _PHASE,
        "in_memory_only": True,
    }


def summarize_key_handoff_envelope(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return {"ok": False, "summary": "no_envelope"}
    return {
        "ok": True,
        "summary": (
            f"phase36 handoff envelope: id="
            f"{envelope.get('envelope_id')} "
            f"label={envelope.get('envelope_label')} "
            f"test_only={envelope.get('test_only')} "
            f"fingerprint={envelope.get('public_fingerprint')}"),
        "envelope_id": envelope.get("envelope_id"),
        "public_fingerprint": envelope.get("public_fingerprint"),
        "phase": _PHASE,
    }


def write_key_handoff_envelope(
    envelope: dict[str, Any],
    output_path: str,
    allow_secret_write: bool = False,
) -> str:
    if not isinstance(envelope, dict):
        raise ValueError("envelope_not_dict")
    # The envelope carries sealed secret material. Refuse to write
    # unless explicitly allowed AND path is inside
    # local_secret_handoff.
    if not allow_secret_write:
        raise ValueError(
            "refuse_secret_write_without_explicit_allow_flag")
    if not sb.is_secret_safe_path(str(output_path)):
        raise ValueError(
            "refuse_secret_write_outside_local_secret_handoff")
    loc = sb.validate_secret_artifact_location(str(output_path))
    if not loc["ok"]:
        raise ValueError(f"refuse_secret_write:{loc['reason']}")
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(envelope)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_key_handoff_envelope(
    path: str,
    consent_marker: str = "",
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(body, dict):
        return {}
    if consent_marker:
        expected = _hash_consent_marker(
            consent_marker, body.get("envelope_id") or "")
        if expected != body.get("consent_marker_hash"):
            return {}
    return body


def write_key_handoff_envelope_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    # A "report" must never carry secret material. Refuse to write
    # if any forbidden field appears.
    if not isinstance(report, dict):
        raise ValueError("report_not_dict")
    hits = []
    for k in _PUBLIC_FORBIDDEN_FIELDS_IN_REPORTS:
        if k in report:
            hits.append(k)
    if hits:
        raise ValueError(
            "refuse_to_write_report_with_secret_fields:" +
            ",".join(hits))
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_key_handoff_envelope",
    "validate_key_handoff_envelope",
    "seal_key_handoff_envelope",
    "unseal_key_handoff_envelope",
    "summarize_key_handoff_envelope",
    "write_key_handoff_envelope",
    "read_key_handoff_envelope",
    "write_key_handoff_envelope_report",
]
