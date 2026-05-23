# Phase 16 — Million-Scale Corpus Readiness, Chunked Import Engine, and Quality Gate

**Status:** Complete. **Date:** 2026-05-21.

This phase is the bridge between the manually authored seed/domain packs of
Phases 12–15B and large-scale corpus ingestion. It introduces a production-grade
**registry → checkpoint → quality gate → chunked importer** pipeline. No real
large corpus is imported in this phase; only synthetic test fixtures touch the
pipeline. Real ingestion stays opt-in: operator places a local file under
`corpus_sources/<lang>/incoming/`, registers it, the quality gate inspects it,
and only an explicit `dry_run=False` call writes anything to the sovereign
lexicon stores.

## Acceptance gate

| Requirement | Status |
|---|---|
| Phase 15A + 15B pre-flight verified before any modification | ✅ packs (5+5+15+15 = 40 phase15 files), 2 reports, 2 harnesses all present |
| Corpus source folder structure exists | ✅ `corpus_sources/{english,russian}/{incoming,processed,rejected,reports}` + `manifests/`, `checkpoints/`, `quality_samples/` |
| Dual corpus registry exists | ✅ `dual_corpus_registry.py` (395 lines) |
| Chunked importer exists | ✅ `dual_corpus_chunked_importer.py` (557 lines) |
| Checkpoint system exists | ✅ `dual_corpus_checkpoint.py` (270 lines) |
| Quality gate exists | ✅ `dual_corpus_quality_gate.py` (348 lines) |
| Synthetic large-file tests prove bounded streaming | ✅ Suite C streamers, F dry-run, G/H real-write into temp DBs |
| Dry-run mode is default | ✅ `import_file/import_corpus` default `dry_run=True` |
| No full-corpus memory load | ✅ All paths use `iter` lines; bounded `_bounded_total` capped at 50k |
| No real large import without explicit local source + quality gate + `dry_run=False` | ✅ Suite I confirms gate-block when content unsafe |
| Safety metadata remains intact | ✅ Phase 14/15 lexicon writes still tag correctly; no auto-vulgar |
| All 8 harnesses pass | ✅ 74 + 73 + 79 + 116 + 87 + 63 + 117 + 80 = **689/689 PASS** |

## New modules

| File | Lines | Role |
|---|---|---|
| `dual_corpus_registry.py` | 395 | SQLite-backed registry of LOCAL corpus source files. Stores corpus_id, language, source_type, expected_format, source_path, source_sha256, declared categories/registers/safety, status. Public API: `init_registry`, `register_corpus_source`, `list_corpus_sources`, `get_corpus_source`, `update_corpus_status`, `compute_source_sha256`, `estimate_rows_streaming`, `validate_corpus_source_record`, `preview_corpus_source`. No daemon, no auto-runner. |
| `dual_corpus_checkpoint.py` | 270 | SQLite-backed progress records. Fields: checkpoint_id, corpus_id, source_path, language, last_byte_offset, last_line_number, accepted/rejected/duplicate counts, batch_count, status, notes, created_at, updated_at. Public API: `init_checkpoint_store`, `create_checkpoint`, `load_checkpoint`, `update_checkpoint`, `mark_checkpoint_complete`, `mark_checkpoint_failed`, `list_checkpoints`. Resume is operator-initiated only — no daemon, no auto-resume. |
| `dual_corpus_quality_gate.py` | 348 | Read-only sampler + scorer. Public API: `sample_corpus`, `score_row_quality`, `detect_metadata_completeness`, `detect_language_mismatch`, `detect_unsafe_unlabeled`, `detect_duplicate_sample_rows`, `estimate_acceptance_rate`, `generate_quality_gate_report`, `should_allow_import`. Sampling strategies: `head` / `middle` / `tail` / `head_middle_tail` / `uniform`. SAMPLE_HARD_MAX = 500. Never imports. |
| `dual_corpus_chunked_importer.py` | 557 | Streams, normalizes, validates, deduplicates, and routes rows. Public API: `import_file`, `import_corpus`, `stream_jsonl_rows`, `stream_txt_rows`, `stream_csv_rows`, `normalize_corpus_row`, `validate_normalized_row`, `classify_default_metadata`, `detect_duplicate`, `write_rejection`, `write_corpus_import_report`, `get_corpus_import_stats`. DEFAULT_MAX_ENTRIES = 25 000 unless `allow_full_source=True`. Default `dry_run=True`. Routes EN→`english_knowledge_ingestion`/`cognitive_lexicon_store`, RU→`russian_knowledge_ingestion`/`russian_lexicon_store`. |
| `test_phase16_million_scale_readiness.py` | 688 | 10-suite harness (A–J): 80 checks, all PASS. Synthetic fixtures + temp DBs only. |

## Pipeline contract

```
operator drops file
     ↓
corpus_sources/<lang>/incoming/<file>
     ↓
dual_corpus_registry.register_corpus_source(...)        ← writes SHA256, bounded row estimate, status='registered'
     ↓
dual_corpus_quality_gate.generate_quality_gate_report(...)   ← samples head/middle/tail, scores, computes quality_score
     ↓
dual_corpus_quality_gate.should_allow_import(report, min_quality_score=0.75)   ← hard gate
     ↓
dual_corpus_chunked_importer.import_corpus(corpus_id, dry_run=True, ...)      ← first pass ALWAYS dry-run
     ↓
operator reviews dry-run report + quality_report
     ↓
dual_corpus_chunked_importer.import_corpus(corpus_id, dry_run=False, ...)     ← real write
     ↓
cognitive_lexicon_store / russian_lexicon_store + per-source corpus_import_report.json
```

## Safety guarantees enforced by code

* **Operational unsafe content** (e.g. `"step by step instructions to bypass auth"`) is rejected at two layers:
  * Quality gate counts every such row in `operational_unsafe_count`; `should_allow_import` returns `ok=False` if count > 0.
  * Row-level `validate_normalized_row` also rejects the same markers, so even with `skip_quality_gate=True` the row never reaches the lexicon.
  * Suite I verifies both: blocked at the gate AND zero accepted with the gate skipped.
* **Vulgar / offensive tags are never auto-attached.** Slang and street-talk source types auto-add only `slang` or `street` registers and `slang_street_talk` coverage. Suite I asserts no row imported from a `slang_list` carries `vulgar` or `offensive`.
* **Sensitive-but-unlabeled rows are downgraded** to `recognition_only` + `do_not_use_unprompted` instead of being added blind. Suite D verifies the downgrade rule.
* **Default size cap of 25 000 entries per call** unless caller passes `allow_full_source=True`. Suite F + I assert the cap is present in the import report.
* **No background daemon, no auto-resume, no auto-runner.** Every state transition is operator-initiated. Suite B verifies the checkpoint API is purely reactive.

## Streaming guarantees

* `stream_jsonl_rows`, `stream_txt_rows`, `stream_csv_rows` are pure generators. They open the file in binary, decode line-by-line, and yield one row at a time. They honor `start_line` (for checkpoint resume) and `max_rows` (for `max_entries`).
* `estimate_rows_streaming(path, max_scan_rows=10_000)` caps at 10 000 lines.
* Quality gate `_bounded_total` caps at 50 000 lines.
* `compute_source_sha256` uses streaming SHA256 over 64 KB chunks (delegated to `pack_manifest.compute_sha256`).

## Resume contract

* Each `import_file` call creates a fresh checkpoint unless `checkpoint_id` is passed.
* On batch boundary the checkpoint is updated with `last_line_number`, `last_byte_offset`, and running counts.
* On completion the checkpoint is marked `completed`.
* A second call with `resume_checkpoint_id` reads the saved `last_line_number` and tells the streamer to skip that many leading lines.
* Suite H verifies round-tripping: a 100-row source imported in two halves (40 + 60) lands exactly 100 rows in the lexicon with zero overlap.
* A bad `resume_checkpoint_id` returns `{"ok": False, "error": "checkpoint_not_found: ..."}` — clean failure, no partial state.

## Files / folders created

```
corpus_sources/
├── english/{incoming,processed,rejected,reports}/
├── russian/{incoming,processed,rejected,reports}/
├── manifests/
├── checkpoints/
└── quality_samples/

dual_corpus_registry.py
dual_corpus_checkpoint.py
dual_corpus_quality_gate.py
dual_corpus_chunked_importer.py
test_phase16_million_scale_readiness.py
PHASE16_MILLION_SCALE_READINESS_REPORT.md
```

## Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_vocabulary_runtime.py` | 74 | ✅ |
| `test_russian_sovereign_stack.py` | 73 | ✅ |
| `test_dual_sovereign_pack_safety.py` | 79 | ✅ |
| `test_dual_pack_importer.py` | 116 | ✅ |
| `test_phase14_domain_pack_expansion.py` | 87 | ✅ |
| `test_phase15a_controlled_scale_expansion.py` | 63 | ✅ |
| `test_phase15b_remaining_domain_expansion.py` | 117 | ✅ |
| `test_phase16_million_scale_readiness.py` | 80 | ✅ |
| **Total** | **689** | **689/689 PASS** |

### Phase 16 suite breakdown (80 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_REGISTRY | register / get / list / update / validation / preview | 12 |
| B_CHECKPOINT | create / load / update / mark_complete / list / lang validation | 9 |
| C_STREAMERS | jsonl/txt/csv streamers, start_line + max_rows | 7 |
| D_CLASSIFICATION | slang/street auto-tag, no vulgar auto-tag, normalization, downgrade | 11 |
| E_QUALITY_GATE | sampling, quality_score, gate open/close, mismatch, duplicates | 8 |
| F_CHUNKED_DRY | dry-run accepted count, no rejection log file, report writing | 7 |
| G_REAL_WRITE | EN + RU real ingestion into temp DBs, registry status transitions | 8 |
| H_RESUME | 40-row + 60-row resume = 100 rows, bad-id clean failure | 7 |
| I_SAFETY | gate-block on operational unsafe, slang has no vulgar, default cap | 7 |
| J_FORBIDDEN_IMPORTS | no `worker`/`luna_modules`/`tier_`/`probe_`/`attestation`/`program_s` | 4 |

## Untouched surfaces (security-confirmed)

* Program S — not inspected, not edited, not imported.
* Tier / probe / attestation modules — not touched.
* `worker.py` — not touched.
* `luna_modules/` — not touched.
* Luna main runtime — no integration.
* Network — no fetches, no downloads.
* Background daemons / pollers / schedulers / watchdogs / cron — none added.
* English and Russian databases — never merged. Phase 16 routes by language, and the two stores remain physically separate SQLite files.
* Existing safety policy — not weakened. Phase 16 layers ADDITIONAL gates on top.

Suite J_FORBIDDEN_IMPORTS scans the four new modules with the regex
`^(import|from)\s+\S*<forbidden>` for each forbidden surface and asserts zero
matches. All four files pass.

## Operator runbook (for the first real corpus)

1. Drop a file into `corpus_sources/english/incoming/<name>.jsonl`
   (or `corpus_sources/russian/incoming/<name>.jsonl`).
2. Register it:
   ```python
   import dual_corpus_registry as reg
   r = reg.register_corpus_source(
       language="en", source_type="word_list",
       expected_format="jsonl",
       source_path="corpus_sources/english/incoming/<name>.jsonl",
       declared_categories=["core_vocabulary"],
       declared_registers=["standard"])
   corpus_id = r["corpus_id"]
   ```
3. Inspect a sample + run quality gate:
   ```python
   import dual_corpus_quality_gate as qg
   rep = qg.generate_quality_gate_report(
       "corpus_sources/english/incoming/<name>.jsonl",
       "jsonl", "en", sample_size=100)
   gate = qg.should_allow_import(rep, min_quality_score=0.75)
   ```
4. Dry-run import:
   ```python
   import dual_corpus_chunked_importer as imp
   imp.import_corpus(corpus_id=corpus_id, batch_size=1000, max_entries=5000,
                     dry_run=True)
   ```
5. Review the dry-run report (`corpus_sources/<lang>/reports/...corpus_import_report.json`).
6. If happy, run for real:
   ```python
   imp.import_corpus(corpus_id=corpus_id, batch_size=1000,
                     max_entries=25000, dry_run=False)
   ```
7. For sources beyond 25 000 entries, set `allow_full_source=True` AND `max_entries=None` (explicit opt-out of the default cap).

## Closing note

Phase 16 ships a complete, test-locked corpus ingest pipeline that:
* Cannot accidentally write at scale (dry-run default + 25k cap + quality gate).
* Cannot write unsafe content (two-layer rejection + recognition_only downgrade).
* Cannot auto-attach `vulgar`/`offensive` to any row.
* Cannot run in the background (no daemon, no scheduler, no watchdog).
* Preserves every prior guarantee — 609 prior tests still green.

The dual sovereign knowledge + vocabulary stack is now operationally ready to
receive operator-staged English and Russian corpora up to the million-row
range, one file at a time, on operator command only.
