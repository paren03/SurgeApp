"""Phase 39 - Single Operator Consent Orchestrator.

Generates a single umbrella consent that authorizes the entire
rehearsal pass. Per-scenario Phase 29 consent tokens are still
issued by Phase 29 internally; this umbrella consent records the
operator's one-shot authorization.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from pathlib import Path
from typing import Any


_PHASE = "phase39.consent_orchestrator.v1"


_REQUIRED_UMBRELLA_FIELDS = (
    "consent_id", "created_at", "phase",
    "operator_id_hash", "scenario_count", "nonce",
    "bound_at", "expiry_at", "binding_hash",
    "rehearsal_dry_run_only",
)


def _hash(value: str) -> str:
    return hashlib.sha256(
        (value or "").encode("utf-8")).hexdigest()


def create_umbrella_consent(
    operator_id: str,
    scenario_count: int,
    ttl_seconds: int = 600,
) -> dict[str, Any]:
    if not operator_id:
        return {
            "consent_id": "",
            "phase": _PHASE,
            "status": "refused",
            "reason": "operator_id_missing",
            "rehearsal_dry_run_only": True,
        }
    if scenario_count <= 0 or scenario_count > 50:
        return {
            "consent_id": "",
            "phase": _PHASE,
            "status": "refused",
            "reason": "scenario_count_out_of_range",
            "rehearsal_dry_run_only": True,
        }
    now = time.time()
    nonce = secrets.token_hex(16)
    op_hash = _hash(operator_id)
    binding = _hash(
        f"{op_hash}|{scenario_count}|{int(now)}|{nonce}")
    return {
        "consent_id": f"rconsent_{int(now)}",
        "created_at": now,
        "phase": _PHASE,
        "status": "ok",
        "operator_id_hash": op_hash,
        "scenario_count": int(scenario_count),
        "nonce": nonce,
        "bound_at": int(now),
        "expiry_at": int(now) + max(60, int(ttl_seconds)),
        "binding_hash": binding,
        "rehearsal_dry_run_only": True,
        "notes": [
            "Umbrella consent records operator's one-shot "
            "authorization for the whole rehearsal pass.",
            "Per-scenario Phase 29 consent tokens are issued "
            "internally by the Phase 37 pipeline.",
            "No raw operator_id is carried; only its SHA-256 "
            "hash.",
            "No production secret is stored; nonce is fresh "
            "per call.",
        ],
    }


def validate_umbrella_consent(
    consent: Any,
    now: float | None = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(consent, dict):
        return {"ok": False, "reasons": ["consent_not_dict"]}
    if consent.get("status") != "ok":
        return {"ok": False,
                "reasons": [f"status:{consent.get('status')}"]}
    for f in _REQUIRED_UMBRELLA_FIELDS:
        if f not in consent:
            reasons.append(f"missing_field:{f}")
    if consent.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    # Forbid raw operator_id surface
    if "operator_id" in consent and consent.get(
            "operator_id") not in (None, ""):
        reasons.append("raw_operator_id_must_not_appear")
    # Expiry
    t = float(now if now is not None else time.time())
    if consent.get("expiry_at") and t > float(
            consent["expiry_at"]):
        reasons.append("consent_expired")
    # Binding hash re-derive
    op_hash = consent.get("operator_id_hash") or ""
    sc = consent.get("scenario_count") or 0
    ba = consent.get("bound_at") or 0
    nonce = consent.get("nonce") or ""
    expected = _hash(f"{op_hash}|{sc}|{ba}|{nonce}")
    if expected != consent.get("binding_hash"):
        reasons.append("binding_hash_mismatch")
    return {"ok": not reasons, "reasons": reasons}


def bind_consent_to_contract(
    consent: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    cv = validate_umbrella_consent(consent)
    if not cv.get("ok"):
        return {"ok": False, "reasons": cv.get("reasons", []),
                "phase": _PHASE}
    if not isinstance(contract, dict):
        return {"ok": False,
                "reasons": ["contract_not_dict"],
                "phase": _PHASE}
    if contract.get("scenario_count") != consent.get(
            "scenario_count"):
        return {"ok": False,
                "reasons": ["scenario_count_mismatch"],
                "phase": _PHASE}
    if contract.get("rehearsal_dry_run_only") is not True:
        return {"ok": False,
                "reasons": ["contract_not_dry_run_only"],
                "phase": _PHASE}
    return {
        "ok": True,
        "bound_at": int(time.time()),
        "consent_id": consent.get("consent_id"),
        "contract_id": contract.get("contract_id"),
        "binding_hash": consent.get("binding_hash"),
        "phase": _PHASE,
    }


def summarize_umbrella_consent(
    consent: Any,
) -> dict[str, Any]:
    if not isinstance(consent, dict):
        return {"ok": False, "summary": "no_consent"}
    return {
        "ok": consent.get("status") == "ok",
        "summary": (
            f"phase39 umbrella consent: id="
            f"{consent.get('consent_id') or 'none'} "
            f"scenarios={consent.get('scenario_count')}"),
        "consent_id": consent.get("consent_id"),
        "phase": _PHASE,
    }


def write_umbrella_consent(
    consent: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(consent)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_umbrella_consent",
    "validate_umbrella_consent",
    "bind_consent_to_contract",
    "summarize_umbrella_consent",
    "write_umbrella_consent",
]
