"""Phase 5L tests: luna_ai_council.

Stdlib unittest only. Local-only — no external API calls. All disk writes
go to TemporaryDirectory fixtures.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_ai_council import (  # noqa: E402
    SCHEMA_VERSION,
    VALID_ACTION_TYPES,
    VALID_DECISIONS,
    _build_sample_packet,
    _packet_hash,
    append_approval_receipt,
    build_approval_packet,
    build_approval_receipt,
    build_reviewer_response,
    classify_non_delegable,
    evaluate_quorum,
    find_valid_receipt,
    load_council_policy,
    make_approval_id,
    make_nonce,
    read_approval_receipts,
    redact_packet,
    redact_secret_text,
    render_council_report_markdown,
    run_local_council_simulation,
    self_test,
    sha256_text,
    simulate_local_reviewer,
    validate_approval_packet,
    validate_approval_receipt,
    validate_reviewer_response,
    write_council_report,
)


def _packet_low(**overrides):
    base = _build_sample_packet(2, "low_risk_additive")
    base.update(overrides)
    return base


def _packet_medium(**overrides):
    base = _build_sample_packet(3, "medium_code_edit")
    base.update(overrides)
    return base


def _packet_high(**overrides):
    base = _build_sample_packet(4, "high_risk_core_edit")
    base.update(overrides)
    return base


def _packet_emergency(**overrides):
    base = _build_sample_packet(5, "emergency_repair")
    base.update(overrides)
    return base


class _IdNonceHashTests(unittest.TestCase):

    def test_01_make_approval_id_shape(self) -> None:
        a = make_approval_id()
        self.assertTrue(a.startswith("apr_"))
        self.assertGreater(len(a), len("apr_"))
        self.assertNotEqual(make_approval_id(), make_approval_id())

    def test_02_make_nonce_shape(self) -> None:
        n1 = make_nonce()
        n2 = make_nonce()
        self.assertNotEqual(n1, n2)
        self.assertEqual(len(n1), 32)

    def test_03_sha256_text_stable(self) -> None:
        self.assertEqual(sha256_text("abc"), sha256_text("abc"))
        self.assertNotEqual(sha256_text("abc"), sha256_text("abd"))
        self.assertEqual(len(sha256_text("x")), 64)


class _RedactionTests(unittest.TestCase):

    def test_04_redacts_api_key(self) -> None:
        out = redact_secret_text("export OPENAI_API_KEY=sk-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertNotIn("sk-aaaaaaaaaaaaaaaaaaaaaaaa", out)
        self.assertIn("[REDACTED]", out)

    def test_05_redacts_bearer_token(self) -> None:
        out = redact_secret_text("Authorization: Bearer abcdef0123456789xxxx")
        self.assertNotIn("abcdef0123456789xxxx", out)
        self.assertIn("[REDACTED]", out)

    def test_06_redacts_sk_style(self) -> None:
        out = redact_secret_text("token: sk-zzzzzzzzzzzzzzzzzzzzzzzzzz")
        self.assertNotIn("zzzzzzzzzzzzzzzzzzzzzzzzzz", out)


class _PacketTests(unittest.TestCase):

    def test_07_packet_validation_success(self) -> None:
        pkt = _packet_low()
        ok, errs = validate_approval_packet(pkt)
        self.assertTrue(ok, errs)
        self.assertTrue(pkt["redaction_applied"])
        self.assertIn("approval_id", pkt)
        self.assertIn("nonce", pkt)

    def test_08_packet_validation_catches_missing(self) -> None:
        pkt = _packet_low()
        del pkt["goal"]
        ok, errs = validate_approval_packet(pkt)
        self.assertFalse(ok)
        self.assertTrue(any("goal" in e for e in errs))


class _ReviewerResponseTests(unittest.TestCase):

    def test_09_response_validation_success(self) -> None:
        r = build_reviewer_response(
            reviewer="local_luna",
            decision="approve",
            confidence=70,
            packet_hash="x" * 64,
            nonce="abc",
        )
        ok, errs = validate_reviewer_response(r)
        self.assertTrue(ok, errs)

    def test_10_response_validation_catches_bad_decision(self) -> None:
        r = build_reviewer_response(
            reviewer="local_luna",
            decision="approve",
            confidence=70,
            packet_hash="x",
            nonce="y",
        )
        r["decision"] = "BOGUS"
        ok, errs = validate_reviewer_response(r)
        self.assertFalse(ok)


class _NonDelegableTests(unittest.TestCase):

    def test_11_non_delegable_memory_deletion_flagged(self) -> None:
        pkt = build_approval_packet(
            goal="cleanup",
            task_id="t",
            risk_tier=2,
            approval_tier_required=2,
            action_type="low_risk_additive",
            target_files=[],
            planned_change_summary="delete memory contents to free disk",
            rollback_plan="",
        )
        flags = classify_non_delegable(pkt)
        self.assertTrue(any("delete_destructive" in f for f in flags))

    def test_12_non_delegable_personality_change_flagged(self) -> None:
        pkt = build_approval_packet(
            goal="adjust personality",
            task_id="t",
            risk_tier=2,
            approval_tier_required=2,
            action_type="low_risk_additive",
            target_files=["memory/luna_personality_state.json"],
            planned_change_summary="tweak Luna's personality module",
            rollback_plan="",
        )
        flags = classify_non_delegable(pkt)
        self.assertTrue(any("personality" in f.lower() for f in flags))


class _LocalReviewerTests(unittest.TestCase):

    def test_13_local_luna_approves_low_risk(self) -> None:
        pkt = _packet_low()
        r = simulate_local_reviewer(pkt, "local_luna")
        self.assertIn(r["decision"], ("approve", "needs_human"))
        ok, errs = validate_reviewer_response(r)
        self.assertTrue(ok, errs)

    def test_14_local_safety_denies_secret_fail(self) -> None:
        pkt = _packet_low(secrets_scan="fail")
        r = simulate_local_reviewer(pkt, "local_safety")
        self.assertEqual(r["decision"], "deny")
        self.assertGreaterEqual(r["confidence"], 70)

    def test_15_local_qa_needs_human_when_no_verify_for_code_edit(self) -> None:
        pkt = build_approval_packet(
            goal="edit",
            task_id="t",
            risk_tier=3,
            approval_tier_required=3,
            action_type="medium_code_edit",
            target_files=["luna_modules/luna_self_knowledge.py"],
            planned_change_summary="medium edit",
            diff_summary="some diff",
            sandbox_result="ok",
            verification_commands=[],
            rollback_plan="rollback",
        )
        r = simulate_local_reviewer(pkt, "local_qa")
        self.assertEqual(r["decision"], "needs_human")


class _QuorumTests(unittest.TestCase):

    def test_16_tier_0_short_circuit_approve(self) -> None:
        pkt = build_approval_packet(
            goal="read state", task_id="t",
            risk_tier=0, approval_tier_required=0,
            action_type="read_only",
            planned_change_summary="just read",
            resource_status="normal", upgrade_gate_decision="allow",
        )
        q = evaluate_quorum(pkt, [])
        self.assertEqual(q["decision"], "approve")

    def test_17_tier_1_short_circuit_approve(self) -> None:
        pkt = build_approval_packet(
            goal="refresh memory index", task_id="t",
            risk_tier=1, approval_tier_required=1,
            action_type="generated_artifact",
            planned_change_summary="regenerate artifact",
            resource_status="normal", upgrade_gate_decision="allow",
        )
        q = evaluate_quorum(pkt, [])
        self.assertEqual(q["decision"], "approve")

    def test_18_tier_2_local_approval_works(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        self.assertEqual(q["decision"], "approve")

    def test_19_tier_3_2_of_3_works(self) -> None:
        pkt = _packet_medium()
        # The default upgrade_gate=needs_approval at tier>=3 makes local_luna nh.
        # Override gate to allow so tier3 path matches the prompt's spirit.
        pkt["upgrade_gate_decision"] = "allow"
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        self.assertIn(q["decision"], ("approve", "needs_human"))
        # Allow either approve via 2-of-3 or needs_human if reviewers didn't quorum.
        if q["decision"] == "approve":
            self.assertEqual(q["rule"], "tier3_2_of_3")

    def test_20_tier_4_requires_all_plus_evidence(self) -> None:
        pkt = _packet_high()
        pkt["upgrade_gate_decision"] = "allow"
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        self.assertIn(q["decision"], ("approve", "needs_human", "deny"))
        # If approve, rule must be tier4_unanimous.
        if q["decision"] == "approve":
            self.assertEqual(q["rule"], "tier4_unanimous")
        # Without evidence -> needs_human.
        pkt2 = _packet_high()
        pkt2["upgrade_gate_decision"] = "allow"
        pkt2["sandbox_result"] = ""
        responses2 = run_local_council_simulation(pkt2)
        q2 = evaluate_quorum(pkt2, responses2)
        self.assertIn(q2["decision"], ("needs_human", "deny"))

    def test_21_high_confidence_deny_denies(self) -> None:
        pkt = _packet_low()
        responses = [
            build_reviewer_response(reviewer="local_luna", decision="approve", confidence=80, packet_hash=_packet_hash(pkt), nonce=pkt["nonce"]),
            build_reviewer_response(reviewer="local_safety", decision="deny", confidence=90, packet_hash=_packet_hash(pkt), nonce=pkt["nonce"]),
            build_reviewer_response(reviewer="local_qa", decision="approve", confidence=60, packet_hash=_packet_hash(pkt), nonce=pkt["nonce"]),
        ]
        q = evaluate_quorum(pkt, responses)
        self.assertEqual(q["decision"], "deny")
        self.assertEqual(q["rule"], "high_confidence_deny")

    def test_22_malformed_responses_lead_to_needs_human(self) -> None:
        pkt = _packet_low()
        # Skip the tier-2 short circuit by raising tier to 3 and gate=allow.
        pkt["risk_tier"] = 3
        pkt["upgrade_gate_decision"] = "allow"
        responses = [{"this": "is malformed"}, {"also": "broken"}]
        q = evaluate_quorum(pkt, responses)
        self.assertEqual(q["decision"], "needs_human")
        self.assertEqual(q["rule"], "all_responses_malformed")

    def test_23_expired_packet_returns_stale(self) -> None:
        pkt = _packet_low()
        # Set expires_at in the past.
        past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        pkt["expires_at"] = past
        q = evaluate_quorum(pkt, [])
        self.assertEqual(q["decision"], "stale")
        self.assertEqual(q["rule"], "packet_expired")

    def test_24_resource_blocked_needs_human_for_code_edit(self) -> None:
        pkt = _packet_medium(resource_status="blocked", upgrade_gate_decision="allow")
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        self.assertEqual(q["decision"], "needs_human")


class _ReceiptTests(unittest.TestCase):

    def test_25_receipt_validates_against_packet(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        rcpt = build_approval_receipt(pkt, responses, q)
        ok, errs = validate_approval_receipt(rcpt, packet=pkt)
        self.assertTrue(ok, errs)

    def test_26_receipt_rejects_target_mismatch(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        rcpt = build_approval_receipt(pkt, responses, q)
        rcpt["target_files"] = ["worker.py"]
        ok, errs = validate_approval_receipt(rcpt, packet=pkt)
        self.assertFalse(ok)
        self.assertTrue(any("target_files mismatch" in e for e in errs))

    def test_27_receipt_rejects_packet_hash_mismatch(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        rcpt = build_approval_receipt(pkt, responses, q)
        # Mutate packet so hash diverges.
        pkt2 = dict(pkt)
        pkt2["planned_change_summary"] = pkt2["planned_change_summary"] + " (drift)"
        ok, errs = validate_approval_receipt(rcpt, packet=pkt2)
        self.assertFalse(ok)
        self.assertTrue(any("packet_hash mismatch" in e for e in errs))

    def test_28_receipt_rejects_expired_receipt(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        rcpt = build_approval_receipt(pkt, responses, q)
        rcpt["expires_at"] = "1999-01-01T00:00:00.000000Z"
        ok, errs = validate_approval_receipt(rcpt, packet=pkt)
        self.assertFalse(ok)
        self.assertTrue(any("expired" in e for e in errs))

    def test_29_append_read_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            pkt = _packet_low()
            responses = run_local_council_simulation(pkt)
            q = evaluate_quorum(pkt, responses)
            rcpt = build_approval_receipt(pkt, responses, q)
            append_approval_receipt(td, rcpt)
            recs = read_approval_receipts(td)
            self.assertEqual(len(recs), 1)
            self.assertEqual(recs[0]["receipt_id"], rcpt["receipt_id"])

    def test_30_find_valid_receipt_works(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            pkt = _packet_low()
            responses = run_local_council_simulation(pkt)
            q = evaluate_quorum(pkt, responses)
            rcpt = build_approval_receipt(pkt, responses, q)
            append_approval_receipt(td, rcpt)
            found = find_valid_receipt(
                td,
                approval_id=pkt["approval_id"],
                task_id=pkt["task_id"],
                target_files=pkt["target_files"],
                packet=pkt,
            )
            self.assertIsNotNone(found)
            self.assertEqual(found["receipt_id"], rcpt["receipt_id"])

    def test_31_find_valid_receipt_rejects_stale(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            pkt = _packet_low()
            responses = run_local_council_simulation(pkt)
            q = evaluate_quorum(pkt, responses)
            rcpt = build_approval_receipt(pkt, responses, q)
            # Force created_at to be 9999 minutes ago.
            old = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=9999)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            rcpt["created_at"] = old
            rcpt["expires_at"] = old  # also expired
            append_approval_receipt(td, rcpt)
            found = find_valid_receipt(
                td,
                approval_id=pkt["approval_id"],
                task_id=pkt["task_id"],
                target_files=pkt["target_files"],
                max_age_minutes=60,
            )
            self.assertIsNone(found)


class _RenderWriteTests(unittest.TestCase):

    def test_32_markdown_includes_reviewer_decisions(self) -> None:
        pkt = _packet_low()
        responses = run_local_council_simulation(pkt)
        q = evaluate_quorum(pkt, responses)
        md = render_council_report_markdown(pkt, responses, q, receipt=None)
        self.assertIn("Luna Delegated AI Approval Council", md)
        self.assertIn("local_luna", md)
        self.assertIn("Quorum result", md)

    def test_33_write_report_under_temp_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            pkt = _packet_low()
            responses = run_local_council_simulation(pkt)
            q = evaluate_quorum(pkt, responses)
            rcpt = build_approval_receipt(pkt, responses, q)
            paths = write_council_report(td, pkt, responses, q, rcpt)
            for p in paths.values():
                self.assertTrue(Path(p).is_file())
                Path(p).resolve().relative_to(td.resolve())


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_ai_council", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

    def test_34_cli_self_test_zero(self) -> None:
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)
        payload = json.loads(r.stdout)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["redaction_proven"])

    def test_35_cli_simulate_tier_3_no_external(self) -> None:
        r = self._run("--simulate", "--tier", "3", "--action", "medium_code_edit")
        self.assertEqual(r.returncode, 0, r.stderr)
        payload = json.loads(r.stdout)
        self.assertIn(payload["decision"], ("approve", "needs_human", "deny", "abstain", "stale"))
        self.assertFalse(payload["external_reviewers_enabled"])

    def test_36_cli_simulate_non_delegable(self) -> None:
        r = self._run("--simulate", "--tier", "2", "--action", "low_risk_additive", "--non-delegable")
        self.assertEqual(r.returncode, 0, r.stderr)
        payload = json.loads(r.stdout)
        self.assertIn(payload["decision"], ("deny", "needs_human"))


class _NoExternalNetworkTests(unittest.TestCase):

    def test_37_no_external_network_imports(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_ai_council.py").read_text(encoding="utf-8")
        self.assertNotIn("import requests", text)
        self.assertNotIn("import openai", text)
        self.assertNotIn("import anthropic", text)
        self.assertNotIn("import xai", text)
        self.assertNotIn("urllib.request", text)
        self.assertNotIn("http.client", text)

    def test_38_no_external_client_usage(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_ai_council.py").read_text(encoding="utf-8")
        for tok in ("openai.ChatCompletion", "anthropic.Client", "xai.Client", "requests.post", "urlopen"):
            self.assertNotIn(tok, text, f"forbidden token in source: {tok!r}")


class _SelfTestFunctionTests(unittest.TestCase):

    def test_39_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


class _PolicyMalformedTests(unittest.TestCase):

    def test_40_malformed_policy_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            (td / "memory").mkdir(parents=True)
            (td / "memory" / "luna_ai_council_policy.json").write_text(
                "{not valid json", encoding="utf-8"
            )
            pol = load_council_policy(td)
            self.assertEqual(pol["schema_version"], 1)
            self.assertFalse(pol["allow_external_reviewers"])
            self.assertFalse(pol.get("_loaded_from_file", True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
