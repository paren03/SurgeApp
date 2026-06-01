"""Phase 35 - Local Exchange Runtime (standalone).

Composes Phase 34 export + exchange contract + exporter packet +
witness input + witness verification + handshake record. Pure local
file-based handoff. No subprocess, no multiprocessing, no network.
"""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase34_export_runtime as p34
import bilingual_voice_phase34_witness_package as wp
import bilingual_voice_phase34_key_descriptor_export as kde
import bilingual_voice_phase35_exchange_contract as xc
import bilingual_voice_phase35_exporter_packet as ep
import bilingual_voice_phase35_witness_input as wi
import bilingual_voice_phase35_witness_verifier as wv
import bilingual_voice_phase35_handshake_record as hsr
import bilingual_voice_phase35_operator_exchange_guide as oeg
import bilingual_voice_report_integrity_manifest as rim


_PHASE = "phase35.exchange_runtime.v1"


def _new_id() -> str:
    return f"p35_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _write_artifact(obj: Any, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2,
                                default=str), encoding="utf-8")
    return str(path)


def create_phase35_local_exchange(
    user_text: str,
    draft_response_text: str = "",
    operator_id: str = "operator_local",
    approve: bool = True,
    preferred_adapter: Optional[str] = None,
    output_dir: Optional[str] = None,
    limit: int = 25,
) -> dict[str, Any]:
    base = Path(output_dir or
                  tempfile.mkdtemp(prefix="phase35_exchange_"))
    base.mkdir(parents=True, exist_ok=True)

    # 1. Phase 34 export (produces witness package + public key)
    export = p34.create_phase34_witness_export(
        user_text=user_text,
        draft_response_text=draft_response_text,
        operator_id=operator_id, approve=bool(approve),
        preferred_adapter=preferred_adapter, sign_evidence=True,
        limit=limit)
    if export["status"] != "ok":
        return {
            "phase35_id": _new_id(),
            "phase34_export": export,
            "exchange_contract": {},
            "exporter_packet": {},
            "witness_input": {},
            "witness_output": {},
            "handshake_record": {},
            "operator_guide": {},
            "status": "refused",
            "safety_summary": {
                "audio_generated": False, "tts_invoked": False,
                "subprocess_used": False, "network_used": False,
                "signing_secret_exported": False},
            "isolation_summary": {
                "program_s_touched": False,
                "gated_internals_touched": False,
                "core_runtime_modules_touched": False,
                "daemon_spawned": False},
            "gap_notes": ["phase34_export_refused"],
            "phase": _PHASE,
        }

    package = export["witness_package"]
    public_key = export["public_key_descriptor"]

    # 2. Write Phase 34 artifacts to local disk
    pkg_path = base / "witness_package.json"
    wp.write_witness_package(package, str(pkg_path))
    key_path = base / "public_key_descriptor.json"
    kde.write_public_key_descriptor(public_key, str(key_path))
    guide = oeg.create_phase35_operator_exchange_guide()
    guide_path = base / "operator_exchange_guide.json"
    oeg.write_phase35_operator_exchange_guide(guide, str(guide_path))

    # Integrity manifest over our exchange artifacts
    manifest = rim.create_report_integrity_manifest(
        [str(pkg_path), str(key_path), str(guide_path)],
        manifest_id="phase35_exchange")
    manifest_path = base / "exchange_integrity_manifest.json"
    rim.write_report_integrity_manifest(manifest, str(manifest_path))

    # 3. Exchange contract
    contract = xc.create_exchange_contract(
        exporter_id="local_exporter",
        witness_id="local_witness")
    contract_path = base / "exchange_contract.json"
    xc.write_exchange_contract_report(contract, str(contract_path))

    # 4. Exporter packet (hashes the local artifacts)
    packet = ep.create_exporter_packet(
        package_path=str(pkg_path),
        public_key_path=str(key_path),
        manifest_path=str(manifest_path),
        guide_path=str(guide_path),
        contract=contract)
    packet_path = base / "exporter_packet.json"
    ep.write_exporter_packet(packet, str(packet_path))

    # 5. Witness input (bounded view of packet)
    witness_input = wi.create_witness_input(packet)
    input_path = base / "witness_input.json"
    wi.write_witness_input(witness_input, str(input_path))

    # 6. Witness verification — pass the actual private signing key
    # so HMAC chain math works. The public descriptor still shipped
    # inside the witness package for identity comparison.
    # We don't have the private key here in plain form, but
    # Phase 34's signing module re-signs with a fresh key per
    # process; the witness package's signed_audit_chain was signed
    # by that key. To make the local exchange demo verifiable
    # end-to-end we accept that the chain verification step uses
    # the same in-process witness verifier (which, when no key is
    # supplied, marks the package's phase34_package check as fail).
    # For the local handshake test we therefore pass through the
    # PUBLIC descriptor (this is honest: an external operator only
    # has the public descriptor) and treat the chain-math failure
    # as expected outside the originating process.
    #
    # When we want a clean "pass" demo (e.g., harness suite H), we
    # also expose the underlying private key on the export so the
    # verifier can be called with it. The export carries it in
    # memory only — never written to disk.
    key_for_chain = (export.get("phase33_output") or {}).get(
        "signed_evidence") or {}
    # We don't actually have the raw key in `export` (good — Phase 34
    # explicitly does not export it). Use the in-process witness
    # verifier which re-validates everything that doesn't require
    # the secret AND accepts a graceful "key not present" outcome.
    output = wv.verify_witness_input(witness_input, public_key)
    output_path = base / "witness_output.json"
    wv.write_witness_output(output, str(output_path))

    # 7. Handshake record
    record = hsr.create_handshake_record(
        contract, packet, witness_input, output)
    record_path = base / "handshake_record.json"
    hsr.write_handshake_record(record, str(record_path))

    return {
        "phase35_id": _new_id(),
        "phase34_export": export,
        "exchange_contract": contract,
        "exporter_packet": packet,
        "witness_input": witness_input,
        "witness_output": output,
        "handshake_record": record,
        "operator_guide": guide,
        "artifact_paths": {
            "witness_package": str(pkg_path),
            "public_key_descriptor": str(key_path),
            "exchange_contract": str(contract_path),
            "exporter_packet": str(packet_path),
            "witness_input": str(input_path),
            "witness_output": str(output_path),
            "handshake_record": str(record_path),
            "exchange_integrity_manifest": str(manifest_path),
            "operator_guide": str(guide_path),
        },
        "status": ("ok" if output["status"] == "pass"
                    else "witness_failed"),
        "safety_summary": {
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "multiprocessing_used": False,
            "signing_secret_exported": False,
        },
        "isolation_summary": {
            "program_s_touched": False,
            "gated_internals_touched": False,
            "core_runtime_modules_touched": False,
            "daemon_spawned": False,
        },
        "gap_notes": [],
        "phase": _PHASE,
    }


def verify_phase35_exchange_from_packet(
    exporter_packet: Any,
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    pkt = exporter_packet if isinstance(exporter_packet,
                                          dict) else {}
    witness_input = wi.create_witness_input(pkt)
    output = wv.verify_witness_input(witness_input)
    record = hsr.create_handshake_record(
        {}, pkt, witness_input, output)
    paths: dict[str, str] = {}
    if output_dir:
        base = Path(output_dir)
        base.mkdir(parents=True, exist_ok=True)
        in_p = base / "witness_input.json"
        out_p = base / "witness_output.json"
        rec_p = base / "handshake_record.json"
        wi.write_witness_input(witness_input, str(in_p))
        wv.write_witness_output(output, str(out_p))
        hsr.write_handshake_record(record, str(rec_p))
        paths = {"witness_input": str(in_p),
                 "witness_output": str(out_p),
                 "handshake_record": str(rec_p)}
    return {
        "witness_input": witness_input,
        "witness_output": output,
        "handshake_record": record,
        "status": output.get("status"),
        "artifact_paths": paths,
        "phase": _PHASE,
    }


def create_phase35_demo_exchange(limit: int = 6) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 6))
    scenarios = [
        ("Hello Luna", None),
        ("Привет Луна", None),
        ("Mix russian and english", None),
        ("English with prosody",
         "prosody_density_metadata_adapter"),
        ("Bilingual segments",
         "bilingual_segment_metadata_adapter"),
        ("Simple Russian", None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, pref in scenarios:
        with tempfile.TemporaryDirectory(
                prefix="phase35_demo_") as td:
            r = create_phase35_local_exchange(
                user_text=ut,
                operator_id="operator_local",
                approve=True,
                preferred_adapter=pref,
                output_dir=td)
        out.append({
            "user_text": ut,
            "status": r["status"],
            "witness_status":
                (r.get("witness_output") or {}).get("status"),
            "handshake_status":
                (r.get("handshake_record") or {}).get(
                    "verification_status"),
            "replay_flags":
                (((r.get("handshake_record") or {}).get(
                    "replay_protection_summary") or {})),
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def validate_phase35_exchange_output(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    required = ("phase35_id", "phase34_export",
                 "exchange_contract", "exporter_packet",
                 "witness_input", "witness_output",
                 "handshake_record", "operator_guide", "status")
    reasons: list[str] = []
    for f in required:
        if f not in output:
            reasons.append(f"missing_field:{f}")
    if output.get("status") == "ok":
        wo = output.get("witness_output") or {}
        if wo.get("status") != "pass":
            reasons.append("status_ok_but_witness_not_pass")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def write_phase35_exchange_runtime_report(
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
    "create_phase35_local_exchange",
    "verify_phase35_exchange_from_packet",
    "create_phase35_demo_exchange",
    "validate_phase35_exchange_output",
    "write_phase35_exchange_runtime_report",
]
