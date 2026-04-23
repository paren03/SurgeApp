"""Unit tests for luna_modules.luna_io.

Run with:  python -m pytest tests/test_luna_io.py -v

Covers all public functions:
  safe_read_text, safe_write_text, _compile_python_path,
  safe_read_json, write_json_atomic, append_jsonl, append_codex_note

And the internal retry/cleanup helper:
  _atomic_replace
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import textwrap
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from luna_modules.luna_io import (
    _ATOMIC_REPLACE_DELAY,
    _ATOMIC_REPLACE_RETRIES,
    _atomic_replace,
    _compile_python_path,
    append_jsonl,
    safe_read_json,
    safe_read_text,
    safe_write_text,
    write_json_atomic,
)


# ── Shared temp-directory fixture ─────────────────────────────────────────────

class _TempDirMixin(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = Path(tempfile.mkdtemp(prefix="luna_io_test_"))

    def tearDown(self) -> None:
        shutil.rmtree(str(self._tmp), ignore_errors=True)

    def _path(self, name: str) -> Path:
        return self._tmp / name


# ── safe_read_text ─────────────────────────────────────────────────────────────

class TestSafeReadText(_TempDirMixin):
    def test_reads_existing_file(self) -> None:
        p = self._path("hello.txt")
        p.write_text("hello world", encoding="utf-8")
        self.assertEqual(safe_read_text(p), "hello world")

    def test_returns_empty_for_missing_file(self) -> None:
        self.assertEqual(safe_read_text(self._path("nope.txt")), "")

    def test_returns_empty_on_permission_error(self) -> None:
        p = self._path("locked.txt")
        p.write_text("data", encoding="utf-8")
        with patch.object(Path, "read_text", side_effect=PermissionError("denied")):
            result = safe_read_text(p)
        self.assertEqual(result, "")

    def test_reads_unicode(self) -> None:
        p = self._path("unicode.txt")
        content = "café résumé naïve"
        p.write_text(content, encoding="utf-8")
        self.assertEqual(safe_read_text(p), content)

    def test_empty_file_returns_empty_string(self) -> None:
        p = self._path("empty.txt")
        p.write_text("", encoding="utf-8")
        self.assertEqual(safe_read_text(p), "")


# ── _atomic_replace ────────────────────────────────────────────────────────────

class TestAtomicReplace(_TempDirMixin):
    def test_replaces_dest_with_src_content(self) -> None:
        src = self._path("src.tmp")
        dst = self._path("dst.txt")
        src.write_text("new content", encoding="utf-8")
        dst.write_text("old content", encoding="utf-8")
        _atomic_replace(src, dst)
        self.assertEqual(dst.read_text(encoding="utf-8"), "new content")
        self.assertFalse(src.exists(), "src should be gone after replace")

    def test_src_removed_on_success(self) -> None:
        src = self._path("s.tmp")
        dst = self._path("d.txt")
        src.write_text("x", encoding="utf-8")
        _atomic_replace(src, dst)
        self.assertFalse(src.exists())

    def test_creates_dest_if_absent(self) -> None:
        src = self._path("s.tmp")
        dst = self._path("new.txt")
        src.write_text("fresh", encoding="utf-8")
        _atomic_replace(src, dst)
        self.assertTrue(dst.exists())
        self.assertEqual(dst.read_text(encoding="utf-8"), "fresh")

    def test_cleans_temp_on_permission_error(self) -> None:
        src = self._path("s.tmp")
        dst = self._path("d.txt")
        src.write_text("x", encoding="utf-8")
        with patch("os.replace", side_effect=PermissionError("locked")):
            with self.assertRaises(PermissionError):
                _atomic_replace(src, dst)
        self.assertFalse(src.exists(), "temp file must be cleaned up on failure")

    def test_cleans_temp_on_generic_error(self) -> None:
        src = self._path("s.tmp")
        dst = self._path("d.txt")
        src.write_text("x", encoding="utf-8")
        with patch("os.replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                _atomic_replace(src, dst)
        self.assertFalse(src.exists())

    def test_retries_on_permission_error(self) -> None:
        """On Windows (_ATOMIC_REPLACE_RETRIES == 3), should retry before giving up."""
        src = self._path("s.tmp")
        dst = self._path("d.txt")
        src.write_text("x", encoding="utf-8")
        # Fail twice, succeed on the third attempt.
        call_count = {"n": 0}
        original = os.replace
        def _replace_stub(s, d):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise PermissionError("locked")
            return original(s, d)
        with patch("os.replace", side_effect=_replace_stub):
            with patch("time.sleep"):       # don't actually wait
                _atomic_replace(src, dst)
        self.assertGreaterEqual(call_count["n"], 2)

    def test_no_retry_on_non_permission_error(self) -> None:
        """A non-PermissionError should not be retried."""
        src = self._path("s.tmp")
        dst = self._path("d.txt")
        src.write_text("x", encoding="utf-8")
        call_count = {"n": 0}
        def _replace_stub(s, d):
            call_count["n"] += 1
            raise FileNotFoundError("gone")
        with patch("os.replace", side_effect=_replace_stub):
            with self.assertRaises((FileNotFoundError, OSError)):
                _atomic_replace(src, dst)
        self.assertEqual(call_count["n"], 1, "should not retry on non-PermissionError")


# ── safe_write_text ────────────────────────────────────────────────────────────

class TestSafeWriteText(_TempDirMixin):
    def test_writes_content(self) -> None:
        p = self._path("out.txt")
        safe_write_text(p, "hello")
        self.assertEqual(p.read_text(encoding="utf-8"), "hello")

    def test_creates_parent_dirs(self) -> None:
        p = self._tmp / "a" / "b" / "c.txt"
        safe_write_text(p, "deep")
        self.assertTrue(p.exists())
        self.assertEqual(p.read_text(encoding="utf-8"), "deep")

    def test_overwrites_existing_content(self) -> None:
        p = self._path("file.txt")
        safe_write_text(p, "first")
        safe_write_text(p, "second")
        self.assertEqual(p.read_text(encoding="utf-8"), "second")

    def test_no_orphaned_tmp_on_success(self) -> None:
        p = self._path("clean.txt")
        safe_write_text(p, "ok")
        tmps = list(self._tmp.glob("*.tmp"))
        self.assertEqual(tmps, [], f"orphaned .tmp files: {tmps}")

    def test_no_orphaned_tmp_on_failure(self) -> None:
        p = self._path("fail.txt")
        with patch.object(Path, "write_text", side_effect=OSError("disk full")):
            safe_write_text(p, "data")  # must not raise
        tmps = list(self._tmp.glob("*.tmp"))
        self.assertEqual(tmps, [], "no orphaned .tmp after failure")

    def test_unicode_content(self) -> None:
        p = self._path("uni.txt")
        content = "日本語テスト 🚀"
        safe_write_text(p, content)
        self.assertEqual(p.read_text(encoding="utf-8"), content)

    def test_concurrent_writes_are_safe(self) -> None:
        """Many threads writing different content should not corrupt the file."""
        p = self._path("concurrent.txt")
        errors = []
        def _write(n: int) -> None:
            try:
                safe_write_text(p, str(n) * 100)
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=_write, args=(i,)) for i in range(20)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [], f"errors during concurrent write: {errors}")
        # File must exist and contain valid content
        content = p.read_text(encoding="utf-8")
        self.assertGreater(len(content), 0)


# ── _compile_python_path ───────────────────────────────────────────────────────

class TestCompilePythonPath(_TempDirMixin):
    def test_valid_python_returns_true(self) -> None:
        p = self._path("good.py")
        p.write_text("def foo(): pass\n", encoding="utf-8")
        ok, err = _compile_python_path(p)
        self.assertTrue(ok)
        self.assertEqual(err, "")

    def test_syntax_error_returns_false(self) -> None:
        p = self._path("bad.py")
        p.write_text("def foo(:\n    pass\n", encoding="utf-8")
        ok, err = _compile_python_path(p)
        self.assertFalse(ok)
        self.assertIsInstance(err, str)
        self.assertGreater(len(err), 0)

    def test_empty_file_compiles(self) -> None:
        p = self._path("empty.py")
        p.write_text("", encoding="utf-8")
        ok, _ = _compile_python_path(p)
        self.assertTrue(ok)

    def test_no_pyc_left_in_source_dir(self) -> None:
        p = self._path("check.py")
        p.write_text("x = 1\n", encoding="utf-8")
        _compile_python_path(p)
        leftover = list(self._tmp.glob("*.pyc"))
        self.assertEqual(leftover, [], f"pyc files leaked: {leftover}")

    def test_missing_file_returns_false(self) -> None:
        ok, err = _compile_python_path(self._path("missing.py"))
        self.assertFalse(ok)
        self.assertGreater(len(err), 0)


# ── safe_read_json ─────────────────────────────────────────────────────────────

class TestSafeReadJson(_TempDirMixin):
    def test_reads_valid_json(self) -> None:
        p = self._path("data.json")
        p.write_text(json.dumps({"key": "value"}), encoding="utf-8")
        result = safe_read_json(p)
        self.assertEqual(result, {"key": "value"})

    def test_returns_default_for_missing_file(self) -> None:
        result = safe_read_json(self._path("nope.json"), default={"x": 1})
        self.assertEqual(result, {"x": 1})

    def test_default_is_empty_dict(self) -> None:
        result = safe_read_json(self._path("nope.json"))
        self.assertEqual(result, {})

    def test_returns_default_for_empty_file(self) -> None:
        p = self._path("empty.json")
        p.write_text("", encoding="utf-8")
        result = safe_read_json(p, default={"empty": True})
        self.assertEqual(result, {"empty": True})

    def test_returns_default_for_whitespace_only(self) -> None:
        p = self._path("ws.json")
        p.write_text("   \n\t  ", encoding="utf-8")
        self.assertEqual(safe_read_json(p, default=42), 42)

    def test_returns_default_for_invalid_json(self) -> None:
        p = self._path("bad.json")
        p.write_text("{not valid json}", encoding="utf-8")
        result = safe_read_json(p, default={"fallback": True})
        self.assertEqual(result, {"fallback": True})

    def test_reads_list(self) -> None:
        p = self._path("list.json")
        p.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
        self.assertEqual(safe_read_json(p, default=[]), [1, 2, 3])

    def test_reads_nested_dict(self) -> None:
        data = {"a": {"b": {"c": 42}}}
        p = self._path("nested.json")
        p.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(safe_read_json(p), data)

    def test_does_not_raise_on_error(self) -> None:
        p = self._path("err.json")
        p.write_text("{}", encoding="utf-8")
        with patch.object(Path, "read_text", side_effect=PermissionError("denied")):
            result = safe_read_json(p, default={"safe": True})
        self.assertEqual(result, {"safe": True})


# ── write_json_atomic ──────────────────────────────────────────────────────────

class TestWriteJsonAtomic(_TempDirMixin):
    def test_writes_dict(self) -> None:
        p = self._path("out.json")
        write_json_atomic(p, {"hello": "world"})
        data = json.loads(p.read_text(encoding="utf-8"))
        self.assertEqual(data, {"hello": "world"})

    def test_writes_list(self) -> None:
        p = self._path("list.json")
        write_json_atomic(p, [1, 2, 3])
        self.assertEqual(json.loads(p.read_text(encoding="utf-8")), [1, 2, 3])

    def test_overwrites_existing(self) -> None:
        p = self._path("ow.json")
        write_json_atomic(p, {"v": 1})
        write_json_atomic(p, {"v": 2})
        self.assertEqual(json.loads(p.read_text(encoding="utf-8")), {"v": 2})

    def test_creates_parent_dirs(self) -> None:
        p = self._tmp / "deep" / "path" / "file.json"
        write_json_atomic(p, {"ok": True})
        self.assertTrue(p.exists())

    def test_no_orphaned_tmp_on_success(self) -> None:
        p = self._path("clean.json")
        write_json_atomic(p, {})
        tmps = list(self._tmp.glob("*.tmp"))
        self.assertEqual(tmps, [], f"orphaned .tmp: {tmps}")

    def test_no_orphaned_tmp_on_failure(self) -> None:
        # Patch os.replace (the real failure point on Windows) so that
        # _atomic_replace runs its own cleanup code before propagating.
        # Patching _atomic_replace itself would bypass cleanup logic.
        p = self._path("fail.json")
        with patch("os.replace", side_effect=PermissionError("locked")):
            write_json_atomic(p, {})  # must not raise
        tmps = list(self._tmp.glob("*.tmp"))
        self.assertEqual(tmps, [], "no orphaned .tmp after failure")

    def test_output_is_valid_json(self) -> None:
        p = self._path("valid.json")
        write_json_atomic(p, {"nested": [1, {"two": 2}], "unicode": "日本語"})
        raw = p.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        self.assertEqual(parsed["unicode"], "日本語")

    def test_concurrent_writes_leave_no_tmp(self) -> None:
        p = self._path("concurrent.json")
        errors = []
        def _write(n: int) -> None:
            try:
                write_json_atomic(p, {"n": n})
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=_write, args=(i,)) for i in range(20)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [])
        tmps = list(self._tmp.glob("*.tmp"))
        self.assertEqual(tmps, [], f"orphaned .tmp after concurrent writes: {tmps}")

    def test_round_trip_with_safe_read_json(self) -> None:
        p = self._path("roundtrip.json")
        original = {"items": [1, 2, 3], "label": "test"}
        write_json_atomic(p, original)
        recovered = safe_read_json(p)
        self.assertEqual(recovered, original)


# ── append_jsonl ───────────────────────────────────────────────────────────────

class TestAppendJsonl(_TempDirMixin):
    def test_creates_file_on_first_write(self) -> None:
        p = self._path("log.jsonl")
        append_jsonl(p, {"event": "boot"})
        self.assertTrue(p.exists())

    def test_single_row_is_valid_json(self) -> None:
        p = self._path("single.jsonl")
        append_jsonl(p, {"k": "v"})
        data = json.loads(p.read_text(encoding="utf-8").strip())
        self.assertEqual(data, {"k": "v"})

    def test_multiple_rows_each_valid_json(self) -> None:
        p = self._path("multi.jsonl")
        for i in range(5):
            append_jsonl(p, {"i": i})
        lines = [l for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertEqual(len(lines), 5)
        for idx, line in enumerate(lines):
            self.assertEqual(json.loads(line), {"i": idx})

    def test_appends_not_overwrites(self) -> None:
        p = self._path("append.jsonl")
        append_jsonl(p, {"a": 1})
        append_jsonl(p, {"b": 2})
        lines = [l for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertEqual(len(lines), 2)

    def test_creates_parent_dirs(self) -> None:
        p = self._tmp / "sub" / "log.jsonl"
        append_jsonl(p, {"ok": True})
        self.assertTrue(p.exists())

    def test_does_not_raise_on_error(self) -> None:
        p = self._path("err.jsonl")
        with patch("builtins.open", side_effect=PermissionError("denied")):
            append_jsonl(p, {"x": 1})  # must not raise

    def test_unicode_row(self) -> None:
        p = self._path("unicode.jsonl")
        append_jsonl(p, {"msg": "日本語 🚀"})
        data = json.loads(p.read_text(encoding="utf-8").strip())
        self.assertEqual(data["msg"], "日本語 🚀")


# ── append_codex_note (light coverage — touches the real codex file path) ──────

class TestAppendCodexNote(unittest.TestCase):
    def test_does_not_raise(self) -> None:
        """append_codex_note is allowed to silently swallow errors (live file path)."""
        from luna_modules.luna_io import append_codex_note
        # Patch the file open so we don't touch the real codex
        with patch("builtins.open", MagicMock()):
            with patch("luna_modules.luna_io.ensure_layout"):
                append_codex_note("Test title", "Test body")  # must not raise

    def test_does_not_raise_on_io_error(self) -> None:
        from luna_modules.luna_io import append_codex_note
        with patch("builtins.open", side_effect=PermissionError("denied")):
            with patch("luna_modules.luna_io.ensure_layout"):
                append_codex_note("Failing title", "Body")  # must not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
