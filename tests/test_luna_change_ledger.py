"""Phase 5C tests: luna_change_ledger.

Stdlib unittest only. Each test uses tempfile.TemporaryDirectory so the
real memory/luna_change_ledger.jsonl is never touched.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Resolve project root from this test file's location: tests/<this>.py
_THIS = Path(__file__).resolve()
_PROJECT_DIR = _THIS.parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from luna_modules.luna_change_ledger import (  # noqa: E402
    ALLOWED_ACTION_TYPES,
    ALLOWED_STATUSES,
    SCHEMA_VERSION,
    append_change_record,
    build_change_record,
    find_change_records,
    infer_affected_functions,
    make_ledger_id,
    normalize_target_file,
    now_iso,
    read_change_records,
    sha256_file,
    sha256_text,
    summarize_change_records,
    validate_change_record,
)


def _good_record(target: str = "luna_modules/luna_change_ledger.py",
                 actor: str = "luna_cu",
                 status: str = "proposed",
                 action_type: str = "edit",
                 plan_id: str = "",
                 reason: str = "test") -> dict:
    return build_change_record(
        actor=actor,
        action_type=action_type,
        target_files=[target],
        reason=reason,
        status=status,
        line_ranges=[[10, 50]],
        risk_score=2,
        plan_id=plan_id,
    )


class _PrimitiveTests(unittest.TestCase):

    def test_01_now_iso_has_timezone(self) -> None:
        s = now_iso()
        self.assertIsInstance(s, str)
        # Either '+00:00' suffix or 'Z' is acceptable; our impl uses +00:00.
        self.assertTrue(s.endswith("+00:00") or s.endswith("Z"),
                        f"now_iso() should be timezone-aware: {s}")

    def test_02_make_ledger_id_unique_and_prefixed(self) -> None:
        a = make_ledger_id()
        b = make_ledger_id()
        self.assertTrue(a.startswith("chg_"))
        self.assertTrue(b.startswith("chg_"))
        self.assertNotEqual(a, b)

    def test_03_sha256_text_stable(self) -> None:
        h1 = sha256_text("hello world")
        h2 = sha256_text("hello world")
        h3 = sha256_text("hello worlD")
        self.assertEqual(h1, h2)
        self.assertNotEqual(h1, h3)
        self.assertEqual(len(h1), 64)

    def test_04_sha256_file_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "a.txt"
            p.write_text("abc", encoding="utf-8")
            self.assertEqual(len(sha256_file(p)), 64)
            self.assertEqual(sha256_file(Path(td) / "missing.txt"), "")


class _NormalizeTests(unittest.TestCase):

    def test_05_normalize_relative_keeps_relative_posix(self) -> None:
        out = normalize_target_file("luna_modules\\luna_change_ledger.py")
        self.assertEqual(out, "luna_modules/luna_change_ledger.py")

    def test_06_normalize_absolute_under_project_returns_relative(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            project = Path(td)
            sub = project / "luna_modules" / "x.py"
            sub.parent.mkdir(parents=True, exist_ok=True)
            sub.write_text("# x", encoding="utf-8")
            out = normalize_target_file(str(sub), project_dir=project)
            self.assertEqual(out, "luna_modules/x.py")

    def test_07_normalize_blocks_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            project = Path(td) / "proj"
            project.mkdir()
            with self.assertRaises(ValueError):
                normalize_target_file("../../etc/passwd", project_dir=project)

    def test_08_normalize_blocks_outside_project_absolute(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            project = Path(td) / "proj"
            project.mkdir()
            outside = Path(td) / "evil" / "x.py"
            outside.parent.mkdir(parents=True, exist_ok=True)
            outside.write_text("# evil", encoding="utf-8")
            with self.assertRaises(ValueError):
                normalize_target_file(str(outside), project_dir=project)


class _BuildValidateTests(unittest.TestCase):

    def test_09_build_record_round_trips(self) -> None:
        rec = _good_record()
        ok, errors = validate_change_record(rec)
        self.assertTrue(ok, f"valid record rejected: {errors}")
        self.assertEqual(rec["schema_version"], SCHEMA_VERSION)
        self.assertIn("verification", rec)
        self.assertEqual(rec["verification"].get("secret_scan"), "not_run")

    def test_10_validate_catches_missing_required_fields(self) -> None:
        rec = _good_record()
        del rec["actor"]
        ok, errors = validate_change_record(rec)
        self.assertFalse(ok)
        self.assertTrue(
            any("actor" in e for e in errors),
            f"errors should reference actor: {errors}",
        )

    def test_11_validate_catches_empty_target_files(self) -> None:
        rec = _good_record()
        rec["target_files"] = []
        ok, errors = validate_change_record(rec)
        self.assertFalse(ok)
        self.assertTrue(any("target_files" in e for e in errors))

    def test_12_validate_catches_bad_action_type(self) -> None:
        rec = _good_record()
        rec["action_type"] = "obliterate"
        ok, errors = validate_change_record(rec)
        self.assertFalse(ok)
        self.assertTrue(any("action_type" in e for e in errors))

    def test_13_validate_catches_bad_status(self) -> None:
        rec = _good_record()
        rec["status"] = "kinda-done"
        ok, errors = validate_change_record(rec)
        self.assertFalse(ok)
        self.assertTrue(any("status" in e for e in errors))

    def test_14_validate_catches_bad_line_ranges(self) -> None:
        rec = _good_record()
        rec["line_ranges"] = [[10]]  # length 1, invalid
        ok, errors = validate_change_record(rec)
        self.assertFalse(ok)
        self.assertTrue(any("line_ranges" in e for e in errors))


class _AppendReadTests(unittest.TestCase):

    def test_15_append_is_append_only_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ledger = Path(td) / "deep" / "ledger.jsonl"  # parent created
            r1 = _good_record(reason="first")
            r2 = _good_record(reason="second")
            append_change_record(r1, ledger_path=ledger)
            append_change_record(r2, ledger_path=ledger)
            text = ledger.read_text(encoding="utf-8")
            lines = [l for l in text.splitlines() if l.strip()]
            self.assertEqual(len(lines), 2)
            for l in lines:
                obj = json.loads(l)
                self.assertIsInstance(obj, dict)
                self.assertEqual(obj["schema_version"], SCHEMA_VERSION)

    def test_16_append_rejects_invalid_record(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ledger = Path(td) / "ledger.jsonl"
            rec = _good_record()
            del rec["actor"]
            with self.assertRaises(ValueError):
                append_change_record(rec, ledger_path=ledger)
            # Ledger should remain absent or empty
            self.assertFalse(ledger.exists())

    def test_17_read_respects_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ledger = Path(td) / "ledger.jsonl"
            for i in range(5):
                append_change_record(_good_record(reason=f"r{i}"),
                                     ledger_path=ledger)
            self.assertEqual(len(read_change_records(ledger, limit=3)), 3)
            self.assertEqual(len(read_change_records(ledger, limit=None)), 5)
            self.assertEqual(len(read_change_records(ledger, limit=0)), 0)

    def test_18_corrupt_rows_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ledger = Path(td) / "ledger.jsonl"
            append_change_record(_good_record(reason="ok1"),
                                 ledger_path=ledger)
            # Inject a corrupt line
            with open(ledger, "ab") as f:
                f.write(b"this is not valid json\n")
                f.write(b"   \n")  # blank
                f.write(b'{"schema_version": "oops",\n')
            append_change_record(_good_record(reason="ok2"),
                                 ledger_path=ledger)
            recs = read_change_records(ledger)
            # Only the two valid records should come back; corrupt skipped.
            self.assertEqual(len(recs), 2)


class _FindSummarizeTests(unittest.TestCase):

    def setUp(self) -> None:
        self._td_obj = tempfile.TemporaryDirectory()
        self._td = self._td_obj.name
        self.ledger = Path(self._td) / "ledger.jsonl"
        # Seed a few records spanning different actors/targets/plan_ids
        for r in [
            _good_record(target="worker.py", actor="luna_cu",
                         plan_id="plan_A", status="applied",
                         action_type="edit"),
            _good_record(target="aider_bridge.py", actor="aider_bridge",
                         plan_id="plan_B", status="verified",
                         action_type="additive"),
            _good_record(target="luna_modules/luna_change_ledger.py",
                         actor="claude", plan_id="plan_A",
                         status="proposed", action_type="create"),
            _good_record(target="worker.py", actor="luna_cu",
                         plan_id="plan_A", status="rolled_back",
                         action_type="rollback"),
        ]:
            append_change_record(r, ledger_path=self.ledger)

    def tearDown(self) -> None:
        self._td_obj.cleanup()

    def test_19_find_filters_by_target(self) -> None:
        hits = find_change_records(self.ledger, target="worker.py")
        self.assertEqual(len(hits), 2)
        for h in hits:
            self.assertTrue(any("worker.py" in t for t in h["target_files"]))

    def test_20_find_filters_by_actor(self) -> None:
        hits = find_change_records(self.ledger, actor="claude")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["actor"], "claude")

    def test_21_find_filters_by_plan_id(self) -> None:
        hits = find_change_records(self.ledger, plan_id="plan_A")
        self.assertEqual(len(hits), 3)
        for h in hits:
            self.assertEqual(h["plan_id"], "plan_A")

    def test_22_summarize_counts_by_actor_status_target(self) -> None:
        recs = read_change_records(self.ledger)
        s = summarize_change_records(recs)
        self.assertEqual(s["total"], 4)
        self.assertEqual(s["corrupt_count"], 0)
        self.assertEqual(s["by_actor"].get("luna_cu"), 2)
        self.assertEqual(s["by_actor"].get("aider_bridge"), 1)
        self.assertEqual(s["by_actor"].get("claude"), 1)
        self.assertEqual(s["by_status"].get("applied"), 1)
        self.assertEqual(s["by_status"].get("rolled_back"), 1)
        self.assertEqual(s["by_target"].get("worker.py"), 2)
        self.assertIn("by_action_type", s)
        self.assertGreaterEqual(s["by_action_type"].get("edit", 0), 1)


class _InferAffectedFunctionsTests(unittest.TestCase):

    def _make_index(self, td: Path) -> Path:
        idx = {
            "schema_version": 1,
            "generated_at": now_iso(),
            "symbol_count": 3,
            "symbols": [
                {"name": "alpha", "kind": "function", "parent": "",
                 "file": "luna_modules/sample.py",
                 "start_line": 10, "end_line": 25, "risk_level": "low"},
                {"name": "BetaClass", "kind": "class", "parent": "",
                 "file": "luna_modules/sample.py",
                 "start_line": 30, "end_line": 60, "risk_level": "low"},
                {"name": "gamma_method", "kind": "method",
                 "parent": "BetaClass",
                 "file": "luna_modules/sample.py",
                 "start_line": 35, "end_line": 50, "risk_level": "low"},
                {"name": "unrelated", "kind": "function", "parent": "",
                 "file": "luna_modules/other.py",
                 "start_line": 1, "end_line": 5, "risk_level": "low"},
            ],
        }
        path = td / "luna_function_index.json"
        path.write_text(json.dumps(idx), encoding="utf-8")
        return path

    def test_23_infer_overlapping_function(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            idx = self._make_index(Path(td))
            res = infer_affected_functions(
                "luna_modules/sample.py", [[12, 18]], function_index_path=idx)
            names = [r["name"] for r in res]
            self.assertIn("alpha", names)
            self.assertNotIn("BetaClass", names)
            self.assertNotIn("unrelated", names)

    def test_24_infer_overlapping_method_and_class(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            idx = self._make_index(Path(td))
            res = infer_affected_functions(
                "luna_modules/sample.py", [[40, 45]], function_index_path=idx)
            names = [r["name"] for r in res]
            # Both BetaClass (30-60) and gamma_method (35-50) overlap [40,45]
            self.assertIn("BetaClass", names)
            self.assertIn("gamma_method", names)

    def test_25_infer_missing_index_returns_empty(self) -> None:
        res = infer_affected_functions(
            "worker.py", [[1, 5]],
            function_index_path=Path("Z:/no_such_dir/missing.json"))
        self.assertEqual(res, [])

    def test_26_infer_empty_inputs_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            idx = self._make_index(Path(td))
            self.assertEqual(infer_affected_functions("", [[1, 5]],
                                                     function_index_path=idx), [])
            self.assertEqual(infer_affected_functions("worker.py", [],
                                                     function_index_path=idx), [])


class _ConstantsTests(unittest.TestCase):
    def test_27_required_action_status_constants_present(self) -> None:
        # action_type and status enums must be tuples of strings; expanding is
        # a low-risk additive change but removing breaks readers.
        self.assertIsInstance(ALLOWED_ACTION_TYPES, tuple)
        self.assertIsInstance(ALLOWED_STATUSES, tuple)
        self.assertTrue(all(isinstance(x, str) for x in ALLOWED_ACTION_TYPES))
        self.assertTrue(all(isinstance(x, str) for x in ALLOWED_STATUSES))
        # Sanity: the shapes the rest of the codebase will rely on
        self.assertIn("edit", ALLOWED_ACTION_TYPES)
        self.assertIn("rollback", ALLOWED_ACTION_TYPES)
        self.assertIn("proposed", ALLOWED_STATUSES)
        self.assertIn("rolled_back", ALLOWED_STATUSES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
