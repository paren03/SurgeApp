"""Tests for luna_council_enforcer — Phase 5O advisory only. 42+ tests."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from luna_modules.luna_council_enforcer import (
    DEFAULT_POLICY,
    SCHEMA_VERSION,
    append_jsonl,
    build_guardian_approval_status,
    classify_action_type,
    classify_non_delegable,
    evaluate_action_enforcement,
    latest_receipts_by_task,
    load_enforcer_policy,
    make_check_id,
    normalize_target_files,
    now_iso,
    read_council_receipts,
    render_enforcer_report_markdown,
    self_test,
    sha256_json,
    target_files_hash,
    verify_receipt_fields,
    write_enforcer_report,
    write_guardian_approval_status,
    write_json_atomic,
)

PYTHON = sys.executable
PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _tmp_project():
    t = tempfile.TemporaryDirectory()
    mem = os.path.join(t.name, "memory")
    os.makedirs(mem, exist_ok=True)
    write_json_atomic(os.path.join(mem, "luna_council_enforcer_policy.json"), DEFAULT_POLICY)
    return t


def _make_receipt(
    tmpdir: str,
    *,
    decision: str = "approve",
    action_type: str = "low_risk_additive",
    target_files=None,
    task_id: str = "task-x",
    created_at: str = None,
    receipt_id: str = None,
    approval_id: str = None,
    extra: dict = None,
) -> dict:
    r = {
        "schema_version": 1,
        "receipt_id": receipt_id or make_check_id("rcpt"),
        "approval_id": approval_id or make_check_id("appr"),
        "created_at": created_at or now_iso(),
        "decision": decision,
        "task_id": task_id,
        "action_type": action_type,
        "target_files": target_files or [],
    }
    if extra:
        r.update(extra)
    path = os.path.join(tmpdir, "memory", "luna_ai_council_approvals.jsonl")
    append_jsonl(path, r)
    return r


def _make_action(
    action_type: str,
    risk_tier: int,
    target_files=None,
    task_id: str = "",
    **kwargs,
) -> dict:
    a = {
        "schema_version": 1,
        "action_id": make_check_id("act"),
        "created_at": now_iso(),
        "source": "test",
        "goal": "test",
        "task_id": task_id,
        "action_type": action_type,
        "risk_tier": risk_tier,
        "target_files": target_files or [],
        "diff_hash": "",
        "planned_commands": [],
        "receipt_id": "",
        "approval_id": "",
        "packet_hash": "",
        "nonce": "",
        "metadata": {},
    }
    a.update(kwargs)
    return a


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestMakeCheckId(unittest.TestCase):
    def test_01_shape(self):
        cid = make_check_id("enf")
        self.assertTrue(cid.startswith("enf-"), cid)
        self.assertGreater(len(cid), 10)

    def test_01b_default_prefix(self):
        self.assertTrue(make_check_id().startswith("enf-"))


class TestSha256Json(unittest.TestCase):
    def test_02_stable_key_order(self):
        self.assertEqual(sha256_json({"b": 2, "a": 1}), sha256_json({"a": 1, "b": 2}))

    def test_02b_different_values_differ(self):
        self.assertNotEqual(sha256_json({"a": 1}), sha256_json({"a": 2}))


class TestNormalizeTargetFiles(unittest.TestCase):
    def test_03_dedupe_posix(self):
        result = normalize_target_files(["b.py", "a.py", "b.py"])
        self.assertEqual(len(result), 2)

    def test_03b_sorted(self):
        result = normalize_target_files(["z.py", "a.py"])
        self.assertEqual(result, sorted(result))

    def test_03c_empty(self):
        self.assertEqual(normalize_target_files([]), [])

    def test_03d_string_input(self):
        self.assertEqual(normalize_target_files("single.py"), ["single.py"])


class TestTargetFilesHash(unittest.TestCase):
    def test_04_stable_regardless_of_order(self):
        self.assertEqual(
            target_files_hash(["b.py", "a.py"]),
            target_files_hash(["a.py", "b.py"]),
        )


class TestPolicyLoad(unittest.TestCase):
    def test_05_loads_defaults(self):
        with _tmp_project() as t:
            p = load_enforcer_policy(t)
            self.assertTrue(p["advisory_only"])
            self.assertIn("receipt_max_age_minutes", p)

    def test_06_malformed_falls_back_safely(self):
        with tempfile.TemporaryDirectory() as t:
            mem = os.path.join(t, "memory")
            os.makedirs(mem)
            with open(os.path.join(mem, "luna_council_enforcer_policy.json"), "w") as f:
                f.write("NOT JSON {{{{")
            p = load_enforcer_policy(t)
            self.assertTrue(p["advisory_only"])


class TestVerifyReceiptFields(unittest.TestCase):
    def test_07_success(self):
        receipt = {
            "schema_version": 1, "receipt_id": "r1", "approval_id": "a1",
            "created_at": now_iso(), "decision": "approve",
            "task_id": "t1", "action_type": "low_risk_additive", "target_files": [],
        }
        ok, missing = verify_receipt_fields(receipt)
        self.assertTrue(ok)
        self.assertEqual(missing, [])

    def test_08_missing_fields_caught(self):
        ok, missing = verify_receipt_fields({"schema_version": 1})
        self.assertFalse(ok)
        self.assertGreater(len(missing), 0)


class TestEnforcementTiers(unittest.TestCase):
    def test_09_tier0_not_required(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("read_only", 0))
            self.assertEqual(r["decision"], "not_required")

    def test_10_tier1_not_required(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("generated_artifact", 1))
            self.assertEqual(r["decision"], "not_required")

    def test_11_tier2_missing_receipt_would_block(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("low_risk_additive", 2))
            self.assertEqual(r["decision"], "would_block")
            self.assertIn("missing_receipt", r["reason"])

    def test_12_tier3_missing_receipt_would_block(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("medium_code_edit", 3))
            self.assertEqual(r["decision"], "would_block")

    def test_13_tier4_missing_receipt_would_block(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("high_risk_core_edit", 4))
            self.assertEqual(r["decision"], "would_block")

    def test_14_tier2_valid_receipt_would_allow(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="t14",
                          target_files=["luna_modules/feat.py"])
            action = _make_action("low_risk_additive", 2,
                                  target_files=["luna_modules/feat.py"], task_id="t14")
            r = evaluate_action_enforcement(t, action)
            self.assertEqual(r["decision"], "would_allow")

    def test_15_tier3_valid_receipt_with_diff_hash_would_allow(self):
        with _tmp_project() as t:
            dh = sha256_json({"patch": "add line"})
            _make_receipt(t, action_type="medium_code_edit", task_id="t15",
                          target_files=["luna_modules/edit.py"], extra={"diff_hash": dh})
            action = _make_action("medium_code_edit", 3,
                                  target_files=["luna_modules/edit.py"],
                                  task_id="t15", diff_hash=dh)
            r = evaluate_action_enforcement(t, action)
            self.assertEqual(r["decision"], "would_allow")

    def test_16_tier3_missing_diff_hash_would_block(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="medium_code_edit", task_id="t16",
                          target_files=["luna_modules/edit2.py"])
            action = _make_action("medium_code_edit", 3,
                                  target_files=["luna_modules/edit2.py"], task_id="t16")
            r = evaluate_action_enforcement(t, action)
            self.assertEqual(r["decision"], "would_block")

    def test_17_target_mismatch_invalid(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="t17",
                          target_files=["other.py"])
            action = _make_action("low_risk_additive", 2,
                                  target_files=["different.py"], task_id="t17")
            r = evaluate_action_enforcement(t, action)
            self.assertIn(r["decision"], ("invalid", "would_block"))

    def test_18_action_type_mismatch_invalid(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="t18")
            action = _make_action("medium_code_edit", 3, task_id="t18")
            r = evaluate_action_enforcement(t, action)
            self.assertIn(r["decision"], ("invalid", "would_block", "stale"))

    def test_19_expired_receipt_stale(self):
        with _tmp_project() as t:
            old_ts = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
            _make_receipt(t, action_type="low_risk_additive", task_id="t19",
                          created_at=old_ts)
            r = evaluate_action_enforcement(t, _make_action("low_risk_additive", 2, task_id="t19"))
            self.assertEqual(r["decision"], "stale")

    def test_20_packet_hash_mismatch_invalid(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="t20",
                          extra={"packet_hash": "aabb"})
            action = _make_action("low_risk_additive", 2, task_id="t20", packet_hash="diff")
            r = evaluate_action_enforcement(t, action)
            self.assertIn(r["decision"], ("invalid", "would_block"))

    def test_21_nonce_mismatch_invalid(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="t21",
                          extra={"nonce": "nonce-aaa"})
            action = _make_action("low_risk_additive", 2, task_id="t21", nonce="nonce-bbb")
            r = evaluate_action_enforcement(t, action)
            self.assertIn(r["decision"], ("invalid", "would_block"))

    def test_22_receipt_decision_deny_would_block(self):
        with _tmp_project() as t:
            _make_receipt(t, decision="deny", action_type="low_risk_additive", task_id="t22")
            r = evaluate_action_enforcement(t, _make_action("low_risk_additive", 2, task_id="t22"))
            self.assertIn(r["decision"], ("invalid", "would_block"))

    def test_23_non_delegable_delete_memory_needs_human(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("delete_memory", 5))
            self.assertIn(r["decision"], ("needs_human", "would_block"))
            self.assertGreater(len(r["non_delegable_flags"]), 0)

    def test_24_non_delegable_identity_change_needs_human(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("change_identity", 5))
            self.assertIn(r["decision"], ("needs_human", "would_block"))

    def test_25_safe_to_execute_now_always_false(self):
        with _tmp_project() as t:
            cases = [
                ("read_only", 0), ("generated_artifact", 1),
                ("low_risk_additive", 2), ("medium_code_edit", 3),
                ("high_risk_core_edit", 4), ("delete_memory", 5),
            ]
            for at, tier in cases:
                r = evaluate_action_enforcement(t, _make_action(at, tier))
                self.assertFalse(r["safe_to_execute_now"], f"must be False for {at}")

    def test_26_advisory_only_always_true(self):
        with _tmp_project() as t:
            r = evaluate_action_enforcement(t, _make_action("read_only", 0))
            self.assertTrue(r["advisory_only"])


class TestReadReceipts(unittest.TestCase):
    def test_27_skips_corrupt_jsonl_rows(self):
        with tempfile.TemporaryDirectory() as t:
            mem = os.path.join(t, "memory")
            os.makedirs(mem)
            path = os.path.join(mem, "luna_ai_council_approvals.jsonl")
            with open(path, "w") as f:
                f.write('{"valid": true}\n')
                f.write("CORRUPT {{{{{\n")
                f.write('{"also_valid": true}\n')
            receipts = read_council_receipts(t)
            self.assertEqual(len(receipts), 2)

    def test_28_latest_receipts_by_task_returns_newest(self):
        with _tmp_project() as t:
            _make_receipt(t, task_id="task-Z", action_type="low_risk_additive",
                          receipt_id="rcpt-old")
            _make_receipt(t, task_id="task-Z", action_type="low_risk_additive",
                          receipt_id="rcpt-new")
            results = latest_receipts_by_task(t, task_id="task-Z", action_type="low_risk_additive")
            self.assertGreater(len(results), 0)
            self.assertEqual(results[0]["receipt_id"], "rcpt-new")


class TestGuardianApprovalStatus(unittest.TestCase):
    def test_29_build_guardian_status_shape(self):
        with _tmp_project() as t:
            status = build_guardian_approval_status(t, pending_actions=[])
            self.assertEqual(status["schema_version"], SCHEMA_VERSION)
            self.assertIn("overall_status", status)
            self.assertTrue(status["advisory_only"])

    def test_30_counts_missing_valid_expired(self):
        with _tmp_project() as t:
            _make_receipt(t, action_type="low_risk_additive", task_id="cnt-v")
            old = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
            _make_receipt(t, action_type="low_risk_additive", task_id="cnt-e", created_at=old)
            status = build_guardian_approval_status(t, pending_actions=[])
            self.assertGreaterEqual(status["receipt_count"], 2)


class TestMarkdown(unittest.TestCase):
    def test_31_markdown_includes_advisory_only_and_decision(self):
        with _tmp_project() as t:
            status = build_guardian_approval_status(
                t, pending_actions=[_make_action("read_only", 0)]
            )
            md = render_enforcer_report_markdown(status)
            self.assertIn("advisory", md.lower())
            self.assertIn("not_required", md)


class TestWriteFiles(unittest.TestCase):
    def test_32_write_status_under_temp_memory(self):
        with _tmp_project() as t:
            status = build_guardian_approval_status(t, pending_actions=[])
            write_guardian_approval_status(t, status)
            out = os.path.join(t, "memory", "luna_guardian_approval_status.json")
            self.assertTrue(os.path.exists(out))

    def test_33_write_report_under_temp_memory(self):
        with _tmp_project() as t:
            status = build_guardian_approval_status(t, pending_actions=[])
            write_enforcer_report(t, status)
            out = os.path.join(t, "memory", "luna_council_enforcer_report.json")
            self.assertTrue(os.path.exists(out))


class TestCLI(unittest.TestCase):
    def _run(self, *args, timeout: int = 30):
        cmd = [PYTHON, "-m", "luna_modules.luna_council_enforcer"] + list(args)
        return subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=timeout
        )

    def test_34_self_test_returns_0(self):
        r = self._run("--self-test")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_35_status_returns_0(self):
        r = self._run("--status")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_36_simulate_tier3_returns_0(self):
        r = self._run("--simulate", "--tier", "3", "--action", "medium_code_edit",
                      "--target", "luna_modules/example.py")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_37_simulate_non_delegable_returns_0(self):
        r = self._run("--simulate", "--non-delegable")
        self.assertEqual(r.returncode, 0, r.stderr)


class TestNoExternalDeps(unittest.TestCase):
    def _src(self) -> str:
        p = pathlib.Path(PROJECT_ROOT) / "luna_modules" / "luna_council_enforcer.py"
        return p.read_text(encoding="utf-8")

    def test_38_no_external_network_calls(self):
        src = self._src()
        for banned in ("import requests", "import httpx", "urllib.request.urlopen"):
            self.assertNotIn(banned, src)

    def test_39_no_openai_anthropic_xai_client(self):
        src = self._src()
        for banned in ("import openai", "import anthropic", "import xai"):
            self.assertNotIn(banned, src)

    def test_40_no_aider_invocation(self):
        src = self._src()
        # allow the word in comments about what we don't do, but no subprocess aider call
        self.assertNotIn("subprocess.*aider", src)
        self.assertNotIn('"aider"', src)

    def test_41_no_taskkill_pip_install_git_reset_delete_queue(self):
        src = self._src()
        # Check no actual shell commands are hard-coded
        for banned in ("taskkill", "pip install", "git reset"):
            self.assertNotIn(banned, src)
        # "delete_queue" must not appear as a standalone callable/command string;
        # the policy list legitimately contains "delete_queues" (plural), which is fine.
        self.assertNotIn('"delete_queue"', src)  # exact standalone literal


class TestSelfTestFunction(unittest.TestCase):
    def test_42_self_test_function_returns_0(self):
        rc = self_test()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
