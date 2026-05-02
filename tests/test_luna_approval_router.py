"""Phase 5M tests: luna_approval_router.

Stdlib unittest only. All disk writes go to TemporaryDirectory fixtures.
No external API calls, no Aider invocations, no target file modification.
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

from luna_modules.luna_approval_router import (  # noqa: E402
    SCHEMA_VERSION,
    VALID_REQUEST_ACTIONS,
    _build_sample_request,
    _classify_non_delegable,
    build_packet_from_request,
    build_router_request,
    collect_context_evidence,
    evaluate_request_with_local_council,
    infer_action_type,
    infer_approval_tier,
    load_router_policy,
    make_request_id,
    normalize_target_files,
    render_router_report_markdown,
    route_approval_request,
    self_test,
    sha256_json,
    validate_router_request,
    verify_receipt_for_request,
    write_router_report,
)
from luna_modules import luna_ai_council as _ai_council  # noqa: E402


def _seed(td: Path) -> None:
    (td / "memory").mkdir(parents=True, exist_ok=True)
    (td / "logs").mkdir(parents=True, exist_ok=True)
    (td / "logs" / "luna_post_repair_verify_x.txt").write_text(
        "[PASS] No hard failures found.\n[PASS] No warnings found.\n",
        encoding="utf-8",
    )


class _PureHelperTests(unittest.TestCase):

    def test_01_make_request_id_shape(self) -> None:
        a = make_request_id()
        self.assertTrue(a.startswith("req_"))
        self.assertNotEqual(make_request_id(), make_request_id())

    def test_02_sha256_json_stable_for_key_order(self) -> None:
        h1 = sha256_json({"a": 1, "b": 2})
        h2 = sha256_json({"b": 2, "a": 1})
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

    def test_03_normalize_target_files_dedupes_posix(self) -> None:
        out = normalize_target_files(["worker.py", "luna_modules\\luna_logging.py", "worker.py", "./memory/x.json"])
        self.assertEqual(out, ["worker.py", "luna_modules/luna_logging.py", "memory/x.json"])


class _InferenceTests(unittest.TestCase):

    def test_04_generated_artifact_low_tier(self) -> None:
        a = infer_action_type("Refresh capability scorecard report", ["memory/luna_capability_scorecard.json"])
        self.assertEqual(a, "generated_artifact")
        t = infer_approval_tier("Refresh scorecard", ["memory/luna_capability_scorecard.json"])
        self.assertLessEqual(t, 1)

    def test_05_low_risk_additive_tier_2(self) -> None:
        a = infer_action_type("Add small helper", ["luna_modules/example.py"])
        self.assertEqual(a, "low_risk_additive")
        t = infer_approval_tier("Add small helper", ["luna_modules/example.py"])
        self.assertEqual(t, 2)

    def test_06_medium_code_edit_tier_3(self) -> None:
        a = infer_action_type("Refactor self-knowledge module helpers", ["luna_modules/luna_self_knowledge.py"])
        self.assertEqual(a, "medium_code_edit")
        t = infer_approval_tier("Refactor self-knowledge module helpers", ["luna_modules/luna_self_knowledge.py"])
        self.assertEqual(t, 3)

    def test_07_worker_high_risk_core_tier_4(self) -> None:
        a = infer_action_type("Tweak continues_update", ["worker.py"])
        self.assertEqual(a, "high_risk_core_edit")
        t = infer_approval_tier("Tweak continues_update", ["worker.py"])
        self.assertEqual(t, 4)

    def test_08_non_delegable_delete_memory_flagged(self) -> None:
        flags = _classify_non_delegable(
            "Delete memory logs to free disk", ["memory/nightly_updates.md"], "non_delegable", load_router_policy(None),
        )
        self.assertIn("delete_memory", flags)
        self.assertIn("explicit_non_delegable_request", flags)


class _RequestTests(unittest.TestCase):

    def test_09_request_validation_success(self) -> None:
        req = build_router_request(
            goal="add helper",
            target_files=["luna_modules/x.py"],
            requested_action="low_risk_additive",
        )
        ok, errs = validate_router_request(req)
        self.assertTrue(ok, errs)

    def test_10_request_validation_catches_missing_goal(self) -> None:
        req = build_router_request(
            goal="add helper",
            target_files=["luna_modules/x.py"],
            requested_action="low_risk_additive",
        )
        req["goal"] = ""
        ok, errs = validate_router_request(req)
        self.assertFalse(ok)
        self.assertTrue(any("goal" in e for e in errs))


class _PacketTests(unittest.TestCase):

    def test_11_packet_built_validates_with_council(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            packet = build_packet_from_request(td, req)
            ok, errs = _ai_council.validate_approval_packet(packet)
            self.assertTrue(ok, errs)


class _RoutingTests(unittest.TestCase):

    def test_12_tier1_generated_artifact_routes_not_required(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("generated_artifact", "memory/luna_capability_scorecard.json")
            r = route_approval_request(td, req, dry_run=True)
            self.assertIn(r["routing_decision"], ("not_required", "approved", "dry_run"))
            self.assertFalse(r["safe_to_execute_now"])

    def test_13_tier2_low_risk_runs_council(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            r = route_approval_request(td, req, dry_run=True)
            self.assertIn(r["routing_decision"], ("approved", "dry_run", "needs_human"))
            self.assertGreaterEqual(len(r["responses"]), 1)
            self.assertFalse(r["safe_to_execute_now"])

    def test_14_tier3_medium_returns_conservative(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("medium_code_edit", "luna_modules/luna_self_knowledge.py")
            r = route_approval_request(td, req, dry_run=True)
            self.assertIn(r["routing_decision"], ("approved", "dry_run", "needs_human", "denied"))
            self.assertFalse(r["safe_to_execute_now"])

    def test_15_tier4_high_risk_needs_human_unless_full_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("high_risk_core_edit", "worker.py")
            r = route_approval_request(td, req, dry_run=True)
            self.assertIn(r["routing_decision"], ("needs_human", "denied", "dry_run"))
            self.assertFalse(r["safe_to_execute_now"])

    def test_16_non_delegable_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("non_delegable", "memory/nightly_updates.md", goal="Delete memory logs")
            r = route_approval_request(td, req, dry_run=True)
            self.assertEqual(r["routing_decision"], "blocked")
            self.assertGreaterEqual(len(r["non_delegable_flags"]), 1)
            self.assertFalse(r["safe_to_execute_now"])


class _ReceiptPersistenceTests(unittest.TestCase):

    def test_17_dry_run_does_not_append_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            route_approval_request(td, req, dry_run=True, write_receipt=True, write_report=False)
            ledger = td / "memory" / "luna_ai_council_approvals.jsonl"
            self.assertFalse(ledger.is_file())

    def test_18_non_dry_run_with_write_receipt_appends_only_for_approve(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            r = route_approval_request(td, req, dry_run=False, write_receipt=True, write_report=False)
            ledger = td / "memory" / "luna_ai_council_approvals.jsonl"
            if r["routing_decision"] == "approved":
                self.assertTrue(ledger.is_file())
                rows = [
                    json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()
                ]
                self.assertGreaterEqual(len(rows), 1)
                self.assertEqual(rows[0]["decision"], "approve")
            else:
                self.assertFalse(ledger.is_file())


class _ReceiptVerifyTests(unittest.TestCase):

    def _make_request_and_receipt(self, td: Path):
        _seed(td)
        req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
        eval_out = evaluate_request_with_local_council(td, req, write_receipt=False)
        return req, eval_out["receipt"]

    def test_19_verify_receipt_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            req, rcpt = self._make_request_and_receipt(td)
            v = verify_receipt_for_request(td, req, rcpt)
            self.assertTrue(v["ok"], v)

    def test_20_verify_receipt_rejects_target_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            req, rcpt = self._make_request_and_receipt(td)
            req["target_files"] = ["worker.py"]
            v = verify_receipt_for_request(td, req, rcpt)
            self.assertFalse(v["ok"])
            self.assertTrue(any("target_files mismatch" in e for e in v["errors"]))

    def test_21_verify_receipt_rejects_action_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            req, rcpt = self._make_request_and_receipt(td)
            req["requested_action"] = "high_risk_core_edit"
            req["target_files"] = ["worker.py"]
            v = verify_receipt_for_request(td, req, rcpt)
            self.assertFalse(v["ok"])

    def test_22_verify_receipt_rejects_expired(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            req, rcpt = self._make_request_and_receipt(td)
            rcpt["expires_at"] = "1999-01-01T00:00:00.000000Z"
            v = verify_receipt_for_request(td, req, rcpt)
            self.assertFalse(v["ok"])
            self.assertTrue(any("expired" in e for e in v["errors"]))


class _SafeToExecuteTests(unittest.TestCase):

    def test_23_safe_to_execute_now_always_false(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            for action, target in [
                ("read_only", ""),
                ("generated_artifact", "memory/luna_capability_scorecard.json"),
                ("low_risk_additive", "luna_modules/example.py"),
                ("medium_code_edit", "luna_modules/luna_self_knowledge.py"),
                ("high_risk_core_edit", "worker.py"),
                ("non_delegable", "memory/nightly_updates.md"),
            ]:
                req = _build_sample_request(action, target, goal="Some goal" if action != "non_delegable" else "Delete memory logs")
                r = route_approval_request(td, req, dry_run=False, write_receipt=False)
                self.assertFalse(r["safe_to_execute_now"], f"{action} reported safe_to_execute_now=True")


class _RenderTests(unittest.TestCase):

    def test_24_markdown_includes_decision_and_targets(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            r = route_approval_request(td, req, dry_run=True)
            md = render_router_report_markdown(r)
            self.assertIn("Luna Approval Router", md)
            self.assertIn("luna_modules/example.py", md)
            self.assertIn("routing_decision", md)
            self.assertIn("safe_to_execute_now", md)


class _WriteTests(unittest.TestCase):

    def test_25_write_report_under_temp_only(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            td = Path(td_str)
            _seed(td)
            req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
            r = route_approval_request(td, req, dry_run=True, write_report=True)
            paths = write_router_report(td, r)
            for p in paths.values():
                self.assertTrue(Path(p).is_file())
                Path(p).resolve().relative_to(td.resolve())


class _DegradationTests(unittest.TestCase):

    def test_26_optional_modules_missing_degrade(self) -> None:
        # Patch council import to None and ensure routing still functions.
        from luna_modules import luna_approval_router as mod
        original = mod._ai_council
        try:
            mod._ai_council = None
            with tempfile.TemporaryDirectory() as td_str:
                td = Path(td_str)
                _seed(td)
                req = _build_sample_request("low_risk_additive", "luna_modules/example.py")
                r = route_approval_request(td, req, dry_run=True)
                self.assertIn(r["routing_decision"], ("approved", "dry_run", "needs_human"))
                self.assertFalse(r["safe_to_execute_now"])
        finally:
            mod._ai_council = original


class _CliTests(unittest.TestCase):

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        return subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_approval_router", *args],
            cwd=str(_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )

    def test_27_cli_self_test_zero(self) -> None:
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)
        payload = json.loads(r.stdout)
        self.assertTrue(payload["ok"])

    def test_28_cli_generated_artifact_request(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            r = self._run(
                "--request", "Refresh scorecard",
                "--action", "generated_artifact",
                "--target", "memory/luna_capability_scorecard.json",
                "--dry-run",
                "--project-dir", td_str,
            )
            self.assertEqual(r.returncode, 0, r.stderr)
            payload = json.loads(r.stdout)
            self.assertFalse(payload["safe_to_execute_now"])

    def test_29_cli_high_risk_worker_request(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            r = self._run(
                "--request", "Edit worker.py continues_update",
                "--action", "high_risk_core_edit",
                "--target", "worker.py",
                "--dry-run",
                "--project-dir", td_str,
            )
            self.assertEqual(r.returncode, 0, r.stderr)
            payload = json.loads(r.stdout)
            self.assertIn(payload["routing_decision"], ("needs_human", "denied", "dry_run"))
            self.assertFalse(payload["safe_to_execute_now"])

    def test_30_cli_non_delegable_request(self) -> None:
        with tempfile.TemporaryDirectory() as td_str:
            r = self._run(
                "--request", "Delete memory logs",
                "--action", "non_delegable",
                "--target", "memory/nightly_updates.md",
                "--dry-run",
                "--project-dir", td_str,
            )
            self.assertEqual(r.returncode, 0, r.stderr)
            payload = json.loads(r.stdout)
            self.assertEqual(payload["routing_decision"], "blocked")
            self.assertFalse(payload["safe_to_execute_now"])


class _NoNetworkTests(unittest.TestCase):

    def test_31_no_external_network_imports(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_approval_router.py").read_text(encoding="utf-8")
        self.assertNotIn("import requests", text)
        self.assertNotIn("import openai", text)
        self.assertNotIn("import anthropic", text)
        self.assertNotIn("import xai", text)
        self.assertNotIn("urllib.request", text)
        self.assertNotIn("http.client", text)

    def test_32_no_external_client_usage(self) -> None:
        text = (_PROJECT_DIR / "luna_modules" / "luna_approval_router.py").read_text(encoding="utf-8")
        for tok in ("openai.ChatCompletion", "anthropic.Client", "xai.Client", "requests.post", "urlopen"):
            self.assertNotIn(tok, text, f"forbidden token in source: {tok!r}")

    def test_33_no_aider_invocation(self) -> None:
        """Router must not *invoke* Aider. References inside detection lists
        (e.g. high_risk_paths containing 'aider_bridge.py') are legal."""
        text = (_PROJECT_DIR / "luna_modules" / "luna_approval_router.py").read_text(encoding="utf-8")
        for tok in ("python -m aider", "import aider", "Aider(", "aider.run", "aider_bridge.run"):
            self.assertNotIn(tok, text, f"aider invocation in router source: {tok!r}")

    def test_34_no_unsafe_command_invocations(self) -> None:
        """Router must not *call* destructive commands. Keyword strings used
        for non-delegable detection (e.g. 'pip install' inside a tuple) are legal."""
        text = (_PROJECT_DIR / "luna_modules" / "luna_approval_router.py").read_text(encoding="utf-8")
        for tok in ("taskkill", "Stop-Process", "git reset --hard", "git clean -fd",
                    "Remove-Item", "os.system(\"rm", "os.system('rm", "os.system(\"pip",
                    "subprocess.run([\"pip", "subprocess.Popen([\"pip",
                    "subprocess.run([\"taskkill", "subprocess.Popen([\"taskkill"):
            self.assertNotIn(tok, text, f"unsafe invocation in router source: {tok!r}")


class _SelfTestFunctionTests(unittest.TestCase):

    def test_35_self_test_returns_zero(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
