"""Phase 5D tests: luna_memory_index.

Stdlib unittest only. Every test uses a TemporaryDirectory project root,
so the real D:\\SurgeApp memory/log files are NEVER read or modified.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

# Resolve project root from this test file's location: tests/<this>.py
_THIS = Path(__file__).resolve()
_REAL_PROJECT_DIR = _THIS.parent.parent
if str(_REAL_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_REAL_PROJECT_DIR))

from luna_modules.luna_memory_index import (  # noqa: E402
    SCHEMA_VERSION,
    build_keyword_index,
    build_memory_index,
    build_memory_summaries,
    build_summary_record,
    extract_tags,
    iter_jsonl_records,
    normalize_source_path,
    read_text_tail,
    search_keyword_index,
    search_memory,
    search_sqlite_fts,
    self_test,
    sha256_file,
    sha256_text,
    sqlite_fts5_available,
    summarize_text_block,
    validate_summary_record,
    write_summaries_jsonl,
)


def _seed_temp_project(td: Path) -> Path:
    """Create a tiny synthetic project tree under td and return td."""
    (td / "memory").mkdir(parents=True, exist_ok=True)
    (td / "logs").mkdir(parents=True, exist_ok=True)
    (td / "memory" / "nightly_updates.jsonl").write_text(
        "\n".join([
            '{"ts":"2026-05-01T20:00:00","msg":"phase 3 stabilization complete","tag":"phase3"}',
            '{"ts":"2026-05-01T20:30:00","event":"CU_PAUSED_DIRTY_CORE","msg":"continues_update paused dirty core"}',
            "BAD JSON LINE — should be skipped",
            '{"ts":"2026-05-01T21:00:00","msg":"aider timeout on worker.py","reason":"aider_timeout"}',
        ]),
        encoding="utf-8",
    )
    (td / "logs" / "luna_worker.log").write_text(
        "[2026-05-01 20:31:00] worker import: IMPORT_OK\n"
        "[2026-05-01 20:32:00] continues_update paused; reason=paused_dirty_core\n"
        "[2026-05-01 20:33:00] guardian restart budget OK\n",
        encoding="utf-8",
    )
    (td / "logs" / "aider_bridge.log").write_text(
        "[2026-05-01 19:35:24] Aider finished rc=0\n"
        "[2026-05-01 19:36:36] Processing aider_patch task=...\n",
        encoding="utf-8",
    )
    return td


class _PrimitiveTests(unittest.TestCase):

    def test_01_normalize_source_path_relative(self) -> None:
        self.assertEqual(
            normalize_source_path("memory\\nightly_updates.jsonl"),
            "memory/nightly_updates.jsonl",
        )

    def test_02_sha256_text_stable(self) -> None:
        self.assertEqual(sha256_text("abc"), sha256_text("abc"))
        self.assertNotEqual(sha256_text("abc"), sha256_text("abd"))
        self.assertEqual(len(sha256_text("anything")), 64)

    def test_03_extract_tags_finds_known_tags(self) -> None:
        text = (
            "Phase 3 stabilization. continues_update paused. worker.py "
            "aider timeout reported. guardian restart cooldown."
        )
        tags = extract_tags(text)
        for expected in ("phase3", "continues_update", "worker",
                         "aider", "guardian", "timeout"):
            self.assertIn(expected, tags, f"missing tag: {expected}; got {tags}")


class _ReaderTests(unittest.TestCase):

    def test_04_read_text_tail_handles_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(read_text_tail(Path(td) / "missing.log"), "")

    def test_05_iter_jsonl_skips_corrupt_and_yields_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "a.jsonl"
            p.write_text(
                "\n".join([
                    '{"ts":"2026-05-01T00:00:00","msg":"first"}',
                    "this is garbage",
                    '{"ts":"2026-05-01T00:10:00","msg":"second"}',
                ]),
                encoding="utf-8",
            )
            seen_records = 0
            seen_corrupt = 0
            for rec, _ln, _bp in iter_jsonl_records(p):
                if rec is None:
                    seen_corrupt += 1
                else:
                    seen_records += 1
            self.assertEqual(seen_records, 2)
            self.assertEqual(seen_corrupt, 1)


class _BuildValidateTests(unittest.TestCase):

    def test_06_summarize_text_block_validates(self) -> None:
        rec = summarize_text_block(
            "logs/sample.log",
            "phase 3 worker import OK aider timeout retry",
        )
        ok, errs = validate_summary_record(rec)
        self.assertTrue(ok, f"validation errors: {errs}")
        self.assertEqual(rec["schema_version"], SCHEMA_VERSION)
        self.assertIn("phase3", rec["tags"])

    def test_07_validate_catches_missing_required(self) -> None:
        rec = summarize_text_block("logs/x.log", "phase 3")
        del rec["summary"]
        ok, errs = validate_summary_record(rec)
        self.assertFalse(ok)
        self.assertTrue(any("summary" in e for e in errs))


class _BuildIndexTests(unittest.TestCase):

    def test_08_missing_sources_do_not_crash_build(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proj = Path(td)
            (proj / "memory").mkdir(parents=True, exist_ok=True)
            # No source files at all — build should still succeed
            result = build_memory_summaries(proj)
            self.assertIsInstance(result, dict)
            self.assertEqual(result["records"], [])
            self.assertGreater(len(result["missing_sources"]), 0)

    def test_09_build_memory_index_writes_only_inside_temp_project(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proj = _seed_temp_project(Path(td))
            report = build_memory_index(proj, write=True)
            self.assertTrue(report["ok"])
            self.assertGreaterEqual(report["summary_count"], 2)

            # Every output path must live under proj/memory
            for key, path_str in report["outputs"].items():
                self.assertTrue(path_str.startswith(
                    str(proj).replace("\\", "/")
                ), f"output {key} not under temp project: {path_str}")

            # Confirm files were created
            for fname in ("luna_memory_summaries.jsonl",
                          "luna_memory_index.json",
                          "luna_memory_index_build_report.json"):
                p = proj / "memory" / fname
                self.assertTrue(p.exists(), f"missing output: {p}")
                # And not absurdly large for our tiny inputs (< 200 KB)
                self.assertLess(p.stat().st_size, 200_000)

    def test_10_source_files_are_not_modified(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proj = _seed_temp_project(Path(td))
            src = proj / "memory" / "nightly_updates.jsonl"
            before_bytes = src.read_bytes()
            before_mtime = src.stat().st_mtime
            build_memory_index(proj, write=True)
            after_bytes = src.read_bytes()
            self.assertEqual(before_bytes, after_bytes,
                             "build modified the source file's contents")
            self.assertEqual(before_mtime, src.stat().st_mtime,
                             "build modified the source file's mtime")


class _SearchTests(unittest.TestCase):

    def test_11_keyword_search_returns_phase3(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proj = _seed_temp_project(Path(td))
            report = build_memory_index(proj, write=True)
            records = report.get("_records", [])
            hits = search_keyword_index(records, "phase 3", limit=5)
            self.assertGreaterEqual(len(hits), 1, "expected phase3 hits")
            top = hits[0]
            self.assertGreater(top.get("_score", 0), 0)

    def test_12_search_memory_high_level(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proj = _seed_temp_project(Path(td))
            build_memory_index(proj, write=True)
            hits = search_memory(proj, "aider timeout", limit=3)
            self.assertIsInstance(hits, list)
            # We expect at least one hit either via FTS5 or keyword
            self.assertGreaterEqual(len(hits), 1)

    def test_13_fts5_path_or_fallback_clean(self) -> None:
        # Either FTS5 is supported and works, OR the fallback returns cleanly.
        with tempfile.TemporaryDirectory() as td:
            proj = _seed_temp_project(Path(td))
            report = build_memory_index(proj, write=True)
            fts_report = report.get("fts5_report") or {}
            db_path = proj / "memory" / "luna_fast_recall.sqlite"
            if fts_report.get("fts5_available"):
                self.assertTrue(db_path.exists())
                rows = search_sqlite_fts(db_path, "phase", limit=3)
                # FTS may return 0 or more — must be a list
                self.assertIsInstance(rows, list)
            else:
                # Fallback: no DB created (or empty), keyword search still works
                hits = search_memory(proj, "phase", limit=3)
                self.assertIsInstance(hits, list)


class _CliTests(unittest.TestCase):

    def test_14_self_test_function_exits_clean(self) -> None:
        rc = self_test()
        self.assertEqual(rc, 0)

    def test_15_self_test_via_subprocess(self) -> None:
        # Run the CLI in a child process so we exercise the __main__ path too.
        env = os.environ.copy()
        env["PYTHONPATH"] = str(_REAL_PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        result = subprocess.run(
            [sys.executable, "-m", "luna_modules.luna_memory_index", "--self-test"],
            cwd=str(_REAL_PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        self.assertEqual(result.returncode, 0,
                         f"--self-test exit != 0: stdout={result.stdout!r} "
                         f"stderr={result.stderr!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
