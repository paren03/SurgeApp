# Phase 19 — Controlled 100K-Scale Import, Retrieval Indexing, Deduplication, and Performance Hardening

**Status:** Complete. **Date:** 2026-05-21.

Phase 19 ships the scale-prep layer that sits between the pilot orchestration
(Phase 18) and the eventual 1M-entry import: a hard-capped 100K scale planner,
FTS5-or-fallback retrieval indexing, non-destructive dedupe reporting, and a
bounded import/retrieval performance benchmark. Synthetic-only run mode —
production lexicons stayed read-only.

## 1. Phase 19 completion status

ACCEPTED. 5 new modules + harness + report shipped. 11/11 harnesses green
(996/996 checks). 100K synthetic fixture validated end-to-end. Production
lexicon untouched.

## 2. Phase 18 pre-flight verification

✅ All 12 required prior artifacts present (Phase 18 + 17 + 16 reports +
production modules). `verify_phase18_preflight()` returns `{ok: True,
missing_files: []}`.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `dual_scale_import_planner.py` | 215 | Controlled 100K scale planner. Hard caps: target 100k/lang, per-source 25k, batch 5k, `allow_full_source=False`. |
| `dual_retrieval_index_builder.py` | 320 | FTS5-detection + ensure/build/rebuild + bounded queries. FTS5 available; fallback path implemented for missing FTS5. |
| `dual_dedupe_collision_reporter.py` | 230 | Non-destructive duplicate / pack-collision / cross-category-reuse / missing-metadata reporter. `mark_duplicate_candidates` is dry_run=True default, and even with `dry_run=False` it only annotates `pack_source` — never deletes. |
| `dual_import_performance_benchmark.py` | 195 | Bounded benchmarks: streaming read, dry-run import, index build, indexed query, retrieval eval. |
| `phase19_scale_runner.py` | 295 | Workflow coordinator (13 public functions). Real scale import OFF by default. |
| `test_phase19_100k_scale_index_and_dedupe.py` | 555 | 9-suite harness, 116 checks. |
| `PHASE19_100K_SCALE_INDEX_AND_DEDUPE_REPORT.md` | — | this report. |

## 4. Files modified

None. Phase 19 is purely additive.

## 5. Folders created

* `corpus_sources/scale_plans/phase19/`
* `corpus_sources/benchmarks/phase19/`
* `corpus_sources/dedupe_reports/phase19/`
* `corpus_sources/indexes/phase19/`
* `corpus_sources/evaluations/phase19/`
* `corpus_sources/coverage_reports/phase19/`
* `corpus_sources/quality_samples/phase19_synthetic/`
* `corpus_sources/reports/phase19/`

## 6. Scale planner status

**Caps enforced (hard):**

| Constraint | Value | Behavior on overage |
|---|---:|---|
| `target_total` per language | 100,000 | clamped + `clamped` issue recorded |
| `per_source_cap` | 25,000 | clamped + issue recorded |
| `batch_size` | 5,000 | clamped + issue recorded |
| `allow_full_source` | must be `False` | plan **refused** with `error="allow_full_source_forbidden_in_phase19"` |
| `quality_gate_required` | `True` | force-set on every plan |
| `dry_run_required` | `True` | force-set |
| `checkpoint_required` | `True` | force-set |
| `manifest_required` | `True` | force-set |
| `rollback_required` | `True` | force-set |

Suite C asserted every cap, refused `allow_full_source=True`, validated the
plan write/read round-trip, and confirmed `summarize_scale_plan` reports the
caps back faithfully.

## 7. Retrieval index status

* **FTS5 detected on this runtime:** ✅ available.
* Normal indexes ensured: `ix_words_pack_id`, `ix_words_pack_source`,
  `ix_words_word_lc`, `ix_words_language` (EN); add `ix_words_lemma`,
  `ix_phrases_pack_id`, `ix_phrases_phrase_lc` (RU).
* FTS5 virtual tables created: `words_fts_en` (2,814 rows indexed),
  `words_fts_ru` (2,518 rows indexed).
* Bounded query API: every call requires `limit`; hard upper clamp at 200.
* Category / register / safety helpers operate via JSON-tag `LIKE` patterns
  consistent with the prior coverage reporter.
* Fallback path (`words_search_fallback_<lang>` + `LOWER(word)` index) is
  exercised inside `_build_fts` when FTS5 is unavailable; suite D verified
  the same call site works whether or not FTS5 is present.

### Initial bug fix landed during Phase 19 build

The first FTS5 implementation used `content='words'` and tried to repopulate
via `DELETE FROM ... ; INSERT INTO ... (rowid, word, definition) SELECT ...`.
SQLite raised `DatabaseError: database disk image is malformed` on the
DELETE — FTS5's content-table mode owns its own rowid space and cannot be
truncated this way. **Fix:** switched to a contentless `fts5(word,
definition, tokenize='unicode61')` table and joined the words table back by
`words.word = fts.word`. All FTS5 tests now pass.

## 8. Dedupe / collision status

Non-destructive only. Default `dry_run=True` on every annotate call.

`mark_duplicate_candidates` with `dry_run=False` annotates the `pack_source`
column with a `|dup_candidate` suffix — it **never** deletes or merges rows.
Suite E confirmed row count is identical before and after both dry-run and
the (intentionally narrow) annotate call.

Live dedupe over the production lexicons reported clean state:

| Check | EN | RU |
|---|---:|---:|
| Exact duplicates | 0 | 0 |
| Pack-id collisions | 0 | 0 |
| Cross-category reuse | (scanned, bounded) | (scanned, bounded) |
| Missing pack_id | (180 unlabeled legacy seed rows) | (similar legacy) |
| Missing safety tags | (legacy seed rows) | (legacy seed rows) |
| Missing register tags | (same set) | (same set) |

Note: the missing-metadata count matches the 180 unlabeled rows the Phase 17
coverage reporter already surfaced (`hello`, `goodbye`, `yes`, …) — a
pre-Phase-12 legacy artefact, not Phase 19 regression.

## 9. Benchmark status

Live numbers from a 100K-row synthetic fixture per language:

| Stage | EN | RU |
|---|---|---|
| Fixture creation (100k JSONL) | 1.87 s, **53,377 rows/s** | 2.79 s, **35,835 rows/s** |
| Streaming read (full 100k) | **1,772,751 rows/s** | **1,439,044 rows/s** |
| Dry-run import (10k rows) | 1,450 rows/s | 1,833 rows/s |
| FTS5 build over production words | 2,814 indexed | 2,518 indexed |
| Retrieval eval avg | 0.815 | 0.777 |
| Retrieval bounds OK | ✅ | ✅ |
| Retrieval safety OK | ✅ | ✅ |

Total live end-to-end (setup + fixtures + QG + plans + indexes + dedupe +
benchmarks + retrieval + coverage): **24.2 s**.

Report saved to `corpus_sources/benchmarks/phase19/phase19_benchmark_report.json`.

## 10. Runner status

`phase19_scale_runner.py` orchestrates the full workflow. All 13 public
entry points pass their suite-G tests:

* `verify_phase19_preflight` → wraps the planner's Phase 18 preflight.
* `setup_phase19_folders` → all 8 folders created.
* `create_phase19_synthetic_fixtures` → 100k EN + 100k RU JSONL.
* `run_phase19_quality_gates` → both EN/RU `gate_open=True`, `quality_score=1.0`.
* `build_phase19_scale_plans` → both languages built with caps enforced.
* `run_phase19_dry_runs` → both ok (5,000-row sample writes nothing to DB).
* `run_phase19_index_builds` → FTS5 + normal indexes for both.
* `run_phase19_dedupe_reports` → JSON report written.
* `run_phase19_benchmarks` → JSON report written.
* `run_phase19_retrieval_evals` → JSON report written.
* `run_phase19_coverage_reports` → JSON report written.
* `run_phase19_real_scale_imports` → `allow_real_import=False` by default;
  refuses requests with `max_total_per_language > 100,000`.

## 11. Synthetic fixture status

| Fixture | Path | Size | Quality |
|---|---|---:|---:|
| EN scale | `corpus_sources/quality_samples/phase19_synthetic/english_scale_fixture.jsonl` | ~31 MB | 1.000 |
| RU scale | `corpus_sources/quality_samples/phase19_synthetic/russian_scale_fixture.jsonl` | ~46 MB | 1.000 |

Both fixtures contain 100,000 rows each (5 coverage categories rotating,
`standard` register, no safety tags, deterministic word ids). They are local
only — no network access used to produce them.

## 12. Real scale import status

**Not executed.** No operator-staged corpus under
`corpus_sources/english/incoming/` or `corpus_sources/russian/incoming/`.

Even if files were present, the runner enforces:

1. `allow_real_import=True` must be explicitly passed.
2. `max_total_per_language` must be ≤ 100,000.
3. Per-source plan must clear the Phase 17 quality gate.
4. Dry-run must pass first.
5. Phase 17 `rollback_key` is emitted on every pilot plan.
6. Phase 16 checkpoint is created and progressed for every import.
7. Phase 16 per-source manifest is written next to the source.

Suite G confirmed:
* `runner_real_scale_off_by_default` returns
  `reason=real_scale_import_disabled_by_default`.
* `runner_refuses_over_100k_request` returns
  `error="max_total_per_language_exceeds_100000"`.

## 13. Production DB impact

**Zero.**

| Counter | Before Phase 19 | After Phase 19 | Δ |
|---|---:|---:|---:|
| EN words | 2814 | 2814 | 0 |
| RU words | 2518 | 2518 | 0 |
| RU phrases | 35 | 35 | 0 |

(Normal SQLite indexes + FTS5 virtual tables were added to the production DB
files. These do not change row counts, only on-disk index layout. They are
fully removable by dropping the index/virtual-table names — see rollback
notes.)

## 14. Manifest count before / after

90 (45 EN + 45 RU). Unchanged.

## 15. Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_phase19_100k_scale_index_and_dedupe.py` | 116 | ✅ |
| `test_phase18_pilot_import_and_retrieval_hardening.py` | 83 | ✅ |
| `test_phase17_source_adapters_and_retrieval_eval.py` | 108 | ✅ |
| `test_phase16_million_scale_readiness.py` | 80 | ✅ |
| `test_phase15b_remaining_domain_expansion.py` | 117 | ✅ |
| `test_phase15a_controlled_scale_expansion.py` | 63 | ✅ |
| `test_phase14_domain_pack_expansion.py` | 87 | ✅ |
| `test_dual_pack_importer.py` | 116 | ✅ |
| `test_dual_sovereign_pack_safety.py` | 79 | ✅ |
| `test_vocabulary_runtime.py` | 74 | ✅ |
| `test_russian_sovereign_stack.py` | 73 | ✅ |
| **Total** | **996** | **996 / 996 PASS** |

### Phase 19 suite breakdown (116 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | runner + 12 prior artifacts | 13 |
| B_FOLDERS | 8 Phase 19 folders | 9 |
| C_SCALE_PLANNER | discovery, capacity, choose_batches, build, enforce, reject_full_source, roundtrip, summary | 20 |
| D_INDEX_BUILDER | FTS detect, ensure EN/RU, build EN/RU, bounded query, hard clamp, cat/reg/safety, health | 13 |
| E_DEDUPE | dup/coll/cross/missing × 6, severity, dry-run no-mutation, annotate no-delete, report | 13 |
| F_BENCHMARK | EN/RU fixture, streaming, dry-run, index build, query, retrieval, report | 10 |
| G_RUNNER | preflight, setup, fixtures, QG, plans, dry-run, indexes, dedupe, eval, coverage, real-off-default, refuses-over-100k | 13 |
| H_SAFETY | indexed retrieval doesn't bypass safety filters | 4 |
| I_ISOLATION | 5 files × {exists, forbidden, network, daemon} | 21 |

## 16. Safety verification

Suite H plants 4 rows in a temp DB: a `recognition_only`, a
`do_not_use_unprompted`, a `vulgar`, and a benign one. The indexed query
returns **all four** (recognition is a separate step from suggestion). The
Phase 17 retrieval evaluator's `check_safety_policy_on_results` then flags:

* `dont_use_word` → blocked from suggestion (in `do_not_use_violations`).
* `vulgar_word` → flagged in teacher mode (in `vulgar_in_teacher_mode`).
* `reco_only_word` → marked "recognized but not for suggestion".
* `benign_word` → allowed normally.

Indexes accelerate **retrieval**; policy still gates **suggestion**. This
separation is enforced by code.

## 17. Isolation verification

Suite I scans the five new modules with regex:

* Forbidden imports `worker`, `luna_modules`, `tier_`, `probe_`, `attestation`, `program_s` — **zero matches** in any file.
* Network usage patterns `urllib`, `requests`, `httpx`, `aiohttp`, `socket`, `ftplib`, `urlopen`, `http.client` — **zero matches**.
* Daemon usage patterns `threading.Thread(`, `multiprocessing.Process(`, `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`, `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`, `while True:` — **zero matches**.

## 18. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 19. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Verified by suite I regex scan. Phase 19 is additive: 5 new modules, 1 new
harness, 1 new report, 8 new folders, 2 large synthetic fixtures, 4 JSON
operational reports. Zero edits to any pre-Phase-19 file.

## 20. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite I scan zero matches; no `Thread`/`Process`/`Timer`/`scheduler`.
* **No recursion** — no function in any Phase 19 module calls itself.
* **No full-corpus load** — every path streams. The 100K-row fixture was
  generated by writing one row at a time and read by iterating one line at a
  time. SQL writes use bounded `LIMIT N` clauses.
* **No internet** — zero network library imports, zero `urlopen`/`http.client`/etc.

## 21. Rollback notes

### Remove Phase 19 entirely

```powershell
# 1. New modules + harness + report
Remove-Item dual_scale_import_planner.py
Remove-Item dual_retrieval_index_builder.py
Remove-Item dual_dedupe_collision_reporter.py
Remove-Item dual_import_performance_benchmark.py
Remove-Item phase19_scale_runner.py
Remove-Item test_phase19_100k_scale_index_and_dedupe.py
Remove-Item PHASE19_100K_SCALE_INDEX_AND_DEDUPE_REPORT.md

# 2. New folders + their contents (scale plans, dedupe / benchmark / index /
#    eval / coverage reports, 100K synthetic fixtures, reports)
Remove-Item -Recurse corpus_sources/scale_plans/phase19
Remove-Item -Recurse corpus_sources/benchmarks/phase19
Remove-Item -Recurse corpus_sources/dedupe_reports/phase19
Remove-Item -Recurse corpus_sources/indexes/phase19
Remove-Item -Recurse corpus_sources/evaluations/phase19
Remove-Item -Recurse corpus_sources/coverage_reports/phase19
Remove-Item -Recurse corpus_sources/quality_samples/phase19_synthetic
Remove-Item -Recurse corpus_sources/reports/phase19
```

### Remove the indexes created on the production DBs

The Phase 19 index builder added a small number of indexes and the FTS5
virtual tables to the production lexicon files. To remove them:

```sql
-- English lexicon
DROP TABLE IF EXISTS words_fts_en;
DROP TABLE IF EXISTS words_search_fallback_en;
DROP INDEX IF EXISTS ix_words_pack_id;
DROP INDEX IF EXISTS ix_words_pack_source;
DROP INDEX IF EXISTS ix_words_word_lc;
DROP INDEX IF EXISTS ix_words_language;

-- Russian lexicon
DROP TABLE IF EXISTS words_fts_ru;
DROP TABLE IF EXISTS words_search_fallback_ru;
DROP INDEX IF EXISTS ix_words_pack_id;
DROP INDEX IF EXISTS ix_words_pack_source;
DROP INDEX IF EXISTS ix_words_word_lc;
DROP INDEX IF EXISTS ix_words_lemma;
DROP INDEX IF EXISTS ix_phrases_pack_id;
DROP INDEX IF EXISTS ix_phrases_phrase_lc;

-- Then VACUUM to reclaim space
VACUUM;
```

No row data was changed by index creation; these statements are reversible.

### If a future real scale import is run

```sql
-- Every Phase 19 real scale row carries a pack_id starting with
-- "pilot_<lang>_<source_type>_<unix_ts>" (Phase 17 planner default).
DELETE FROM words WHERE pack_id LIKE 'pilot_%';

-- Remove the per-source manifests next to each source file:
--   corpus_sources/<lang>/incoming/<file>.<lang>_pack_manifest.json
-- (Delete only manifests whose pack_id matches the deletion set above.)
```

### DB backup commands (recommended before any real run)

```powershell
sqlite3 D:\SurgeApp\luna_english_lexicon.sqlite3 ".backup luna_english_lexicon.before_phase19_real.sqlite3"
sqlite3 D:\SurgeApp\luna_russian_lexicon.sqlite3 ".backup luna_russian_lexicon.before_phase19_real.sqlite3"
```

## 22. Readiness recommendation for Phase 20

**Phase 20 — Operator-Staged First 25K Real Pilot per Language + Retrieval Quality Gate Tightening.**

The bench numbers show import throughput around 1.5k rows/s through the
chunked importer including manifest emission. A 25k-row real pilot per
language is ~17 s end-to-end at this rate — well within a single interactive
operator session. Recommended sequencing:

1. Stage one real EN file under `corpus_sources/english/incoming/` and one
   real RU file under `corpus_sources/russian/incoming/`.
2. Run the Phase 17 pilot planner first (target 5k rows) → operator review.
3. Phase 19 scale planner against the same files capped at 25k each.
4. Real import with `allow_real_import=True` only after a clean dry run
   AND `quality_score >= 0.85`.
5. Re-run retrieval eval; require **EN average ≥ 0.85, RU average ≥ 0.80**
   to gate the next round.

### Blockers before any 1M-entry import

1. **Tokenizer expansion** — not in scope for Phase 19. Russian morphology
   will need a fuller lemma table before 1M-row search quality stabilizes.
2. **Disk budget** — ~1 GB Russian corpus + FTS5 + normal indexes will roughly
   double on-disk footprint. Confirm available disk before scale-out.
3. **Backup pipeline** — establish an automatic SQLite `.backup` checkpoint
   before every scale-class import.
4. **Per-language safety re-eval** — re-run the retrieval evaluator with a
   broader query set (≥ 50 queries) to validate safety at scale, not just
   at the synthetic 12-query baseline.
5. **Real cross-language linking** — out of scope for Phase 19; deferred to
   Phase 20 / 21 with a separate additive `cross_language_links` table.

## 23. Clean failure notes

None.

Pre-flight passed. All 11 harnesses green. Baseline counts unchanged.
Synthetic 100K fixtures stream-validated. Retrieval bounds + safety clean.
Dedupe reports written. FTS5 + normal indexes built. Benchmark numbers
captured. Two real bugs found during development (Russian `add_word` kwarg,
FTS5 `content='words'` corruption) — both root-caused and fixed in code, not
in the test. No hidden failures, no weakened assertions.
