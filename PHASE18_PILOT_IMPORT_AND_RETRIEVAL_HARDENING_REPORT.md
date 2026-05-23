# Phase 18 — Bilingual Pilot Corpus Import, Retrieval Hardening, and Scale Validation

**Status:** Complete. **Date:** 2026-05-21.

Phase 18 ships the controlled pilot orchestration layer on top of Phase 16
(million-scale readiness) + Phase 17 (adapters / planner / retrieval eval /
coverage). No real corpus files were staged by the operator; Phase 18 ran in
synthetic validation mode end-to-end. Production lexicons stayed read-only.

## 1. Phase 18 completion status

ACCEPTED. Runner + harness shipped, all 10 harnesses (Phase 18 + 9 prior) pass,
880/880 checks green, production lexicon untouched, synthetic 10k-row
fixtures stream-validated for both languages.

## 2. Phase 17 pre-flight verification

✅ All 11 required artifacts present:

| Required artifact | Status |
|---|---|
| `PHASE17_SOURCE_ADAPTERS_AND_RETRIEVAL_EVAL_REPORT.md` | ✅ |
| `test_phase17_source_adapters_and_retrieval_eval.py` | ✅ |
| `dual_corpus_source_adapters.py` | ✅ |
| `dual_corpus_pilot_import_planner.py` | ✅ |
| `dual_retrieval_quality_eval.py` | ✅ |
| `dual_coverage_reporter.py` | ✅ |
| `PHASE16_MILLION_SCALE_READINESS_REPORT.md` | ✅ |
| `dual_corpus_registry.py` | ✅ |
| `dual_corpus_chunked_importer.py` | ✅ |
| `dual_corpus_quality_gate.py` | ✅ |
| `dual_corpus_checkpoint.py` | ✅ |

Earlier reports also verified present: `PHASE15A`, `PHASE15B`, `PHASE14`,
`DUAL_PACK_IMPORTERS_AND_SEED_PACKS_REPORT.md`,
`DUAL_SOVEREIGN_SAFETY_TAXONOMY_REPORT.md`. Pre-flight pass — runner
`verify_phase17_preflight()` returns `{"ok": True}`.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `phase18_pilot_import_runner.py` | 415 | Workflow orchestrator (12 public functions) |
| `test_phase18_pilot_import_and_retrieval_hardening.py` | 530 | 12-suite harness, 83 checks |
| `PHASE18_PILOT_IMPORT_AND_RETRIEVAL_HARDENING_REPORT.md` | — | this report |

## 4. Files modified

None. Phase 18 is purely additive.

## 5. Folders created

* `corpus_sources/pilot_imports/phase18/`
* `corpus_sources/evaluations/phase18/`
* `corpus_sources/coverage_reports/phase18/`
* `corpus_sources/quality_samples/phase18_synthetic/`
* `corpus_sources/reports/phase18/`

## 6. Real local sources discovered

| Folder | Files |
|---|---|
| `corpus_sources/english/incoming/` | 0 |
| `corpus_sources/russian/incoming/` | 0 |

No operator-staged corpora. Phase 18 ran in **synthetic-only validation mode**.
No production-DB-touching real pilot occurred.

## 7. Synthetic fixture status

Generated under `corpus_sources/quality_samples/phase18_synthetic/` at 10,000
rows per large fixture (operator can re-run at smaller / larger sizes via
`generate_synthetic_phase18_fixtures(rows_per_large_fixture=N)`).

| Fixture | Size | Streaming quality_score |
|---|---:|---:|
| `english_large_jsonl_fixture.jsonl` | 3,486,990 B (10,000 valid + 100 duplicates + 50 malformed + 50 recognition_only + 50 slang) | **1.000** |
| `russian_large_jsonl_fixture.jsonl` | 5,085,990 B (same composition, RU) | **0.993** |
| `english_large_txt_fixture.txt` | 210,000 B (10,000 words) | **1.000** |
| `russian_large_txt_fixture.txt` | 350,000 B (10,000 RU words) | **1.000** |
| `bilingual_glossary_fixture.csv` | 146,053 B (1,000 EN/RU pairs) | **1.000** |
| `russian_morphology_fixture.csv` | 139,046 B (1,000 morph rows) | **0.993** |

Every fixture was sampled at 100 rows by the quality gate; no operational
unsafe markers, zero language mismatch on language-matched fixtures, zero
malformed-row leakage past the validator.

## 8. Quality gate status

Suite E + the live synthetic run prove:

* **High-quality synthetic JSONL → gate open** (`quality_score=1.0`).
* **Operational unsafe content → gate closed** (`reason=operational_unsafe_content_detected`).
* **Language mismatch flagged** (`language_mismatch_count >= 1`).
* All six synthetic fixtures cleared the gate with `quality_score >= 0.993`.

Gate enforces:
parse validity · metadata completeness · language match · coverage validity ·
register validity · safety validity · prompt-injection markers · operational
wrongdoing markers · unlabeled-vulgar/offensive downgrade · duplicate fraction ·
malformed-row counting.

## 9. Pilot dry-run status

* Default `dry_run=True` honored on every plan built via
  `build_pilot_plans_for_sources(...)`.
* `safe_max_entries` capped by Phase 17 planner at `DEFAULT_TARGET_ENTRIES=1000`
  per plan unless explicitly mutated.
* `batch_size` selected within bounds (10–1000).
* Dry-run reports written under `corpus_sources/pilot_imports/phase18/`.
* Production EN/RU lexicons unchanged across every dry-run (suite G assertion
  `production_db_unchanged_during_dry_run`).

## 10. Real pilot import status

**No production write occurred.** The harness exercised real-import paths
against **temporary SQLite DBs only** under `tempfile.mkdtemp()`:

| Real-import guard | Behavior |
|---|---|
| `allow_real_import=False` default | Returns `reason=real_import_disabled_by_default` |
| Failed quality gate | Plan-build sets `skipped=True`, `skip_reason=quality_gate_closed` |
| Failed dry-run | Real path sets `real_skipped_reason=dry_run_failed` |
| Per-language cap `25_000` | Plan 6 of 6 × 5000 returns `real_skipped_reason=hard_cap_exceeded:en` |
| `allow_full_source=True` | Not used anywhere in Phase 18 (verified by plan JSON inspection) |

In the temp-DB smoke test:
* 1 small pilot wrote 40 rows to a temp EN DB; production EN unchanged.
* 5 of 6 capped-mode plans wrote 5000 each (25,000 total) to a temp EN DB; the
  6th was skipped by the per-language cap; production EN unchanged.

## 11. Production DB impact

**Zero.** Same as Phase 17.

| Counter | Before Phase 18 | After Phase 18 | Δ |
|---|---:|---:|---:|
| EN words | 2814 | 2814 | 0 |
| RU words | 2518 | 2518 | 0 |
| RU phrases | 35 | 35 | 0 |

## 12. Manifest count before / after

90 (45 EN + 45 RU) → 90 (45 EN + 45 RU). Unchanged.

## 13. Checkpoint status

Suite I confirms the full checkpoint lifecycle:

* `create_checkpoint` returns `{ok:True, checkpoint_id:...}`.
* `update_checkpoint` persists `last_line_number`, `accepted_count`,
  `rejected_count`, `duplicate_count`, `batch_count`.
* `mark_checkpoint_complete` transitions status → `completed`.
* `mark_checkpoint_failed(..., notes="boom")` transitions status → `failed`
  with reason recorded.
* No background resume — resume is explicit operator action only.

## 14. Retrieval evaluation status

Live run against the existing production lexicon (read-only):

| Language | Avg score | Bounds OK | Safety OK | Queries |
|---|---:|---|---|---:|
| EN | **0.815** | ✅ | ✅ | 12 |
| RU | **0.777** | ✅ | ✅ | 12 |

* Every query returned ≤ limit rows (`bounds_ok=True`).
* Zero `do_not_use_unprompted` violations in default (teacher) mode.
* Zero `vulgar`/`offensive` rows surfaced unprompted.
* Suite J assertions cover bounds, category coverage, safety policy,
  do_not_use blocking, bounds-violation detection.

Reports persisted to:
* `corpus_sources/evaluations/phase18/phase18_retrieval_eval_en.json`
* `corpus_sources/evaluations/phase18/phase18_retrieval_eval_ru.json`

## 15. Coverage report status

`run_post_pilot_coverage_report()` wrote
`corpus_sources/coverage_reports/phase18/phase18_coverage_report.json`.

Highlights:
* Totals: EN 2814 / RU 2518 / RU phrases 35.
* Manifest count: 90.
* Recommended next-import targets (categories below 50 entries): **3**.
* All 21 coverage categories × 22 register tags × 4 safety tags counted via
  bounded SQL aggregates.
* Reporter is read-only (suite I_BOUNDS: count-before == count-after).

**Readiness assessment:**

| Target | Status | Blockers |
|---|---|---|
| 100k-entry import | **Ready** | Operator must stage local files; quality gate + pilot planner already proven on synthetic 10k-row fixtures. |
| 1M-entry import | **Not yet** | Need a real corpus staged locally; tokenizer expansion (deferred to a later phase); higher per-source caps gated on a separate phase decision; physical disk budget review for ~1 GB Russian corpus + index growth. |

## 16. Test results

| Harness | Tests | Status |
|---|---:|---|
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
| **Total** | **880** | **880 / 880 PASS** |

### Phase 18 suite breakdown (83 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | Phase 17 verifier + 11 files exist | 12 |
| B_FOLDERS | 5 Phase 18 folders | 5 |
| C_DISCOVERY | EN/RU/both bounded discovery + classify | 5 |
| D_FIXTURES | 6 fixture files + composition (valid/duplicate/malformed/recog/slang) + CSV parse | 11 |
| E_QUALITY_GATES | Good open, unsafe closed, mismatch flagged, streaming validation | 6 |
| F_PLANNER | Plan built, dry-run default, max capped, batch bounded, no allow_full_source | 7 |
| G_DRYRUN | Dry-run ok, accepted cap, report written, production unchanged | 4 |
| H_REAL_GUARD | Off by default, succeeds on temp, hard cap honored + skips overage, bad-quality skip | 6 |
| I_CHECKPOINT | Create/update/complete/failed lifecycle | 5 |
| J_RETRIEVAL | EN+RU runs/bounds/safety/queries, do_not_use block, bounds flag | 10 |
| K_COVERAGE | Totals/21+22+4 keys/low/balance/report | 7 |
| L_ISOLATION | Forbidden imports + network + daemon usage patterns | 4 |

### Fix landed during Phase 18 development

Suite H originally failed `hard_cap_skips_overage` because the Phase 17 planner
caps `safe_max_entries` at `DEFAULT_TARGET_ENTRIES=1000` regardless of the
target passed in. Six 1000-row plans (=6,000 total) never crossed the 25,000
cap. **Fix:** the test now lifts `safe_max_entries=5000` directly on each
plan before invoking the real pilot path. This isolates the unit under test
(the runner's per-language hard cap) from the planner's default target. Real
production usage will still inherit the planner's conservative defaults; the
runner-cap path is the safety net for higher per-plan ceilings.

## 17. Safety verification

* Operational unsafe content rejected at quality-gate AND row-level (two-layer).
* Slang/street adapters auto-tag `slang`/`street` registers but NEVER `vulgar`/`offensive`.
* Sensitive-but-unlabeled rows downgrade to `recognition_only` + `do_not_use_unprompted`.
* Retrieval eval ran 24 queries with **zero** `do_not_use_unprompted` violations and **zero** vulgar/offensive surfacing in default teacher mode.
* Pilot planner refuses real-write when quality gate is closed (`error="quality_gate_blocked"`).
* Per-language 25,000-row cap enforced — overage plans skipped with `real_skipped_reason="hard_cap_exceeded:<lang>"`.
* Synthetic fixtures explicitly include controlled recognition_only rows, slang rows, malformed rows, and duplicates so the safety path is exercised end-to-end.

## 18. Isolation verification

Suite L scans `phase18_pilot_import_runner.py` (and reuses Phase 17 scans for
the four prior modules) against:

* Forbidden imports `worker`, `luna_modules`, `tier_`, `probe_`, `attestation`, `program_s` — zero matches.
* Network usage patterns `urllib`, `requests`, `httpx`, `aiohttp`, `socket`, `ftplib`, `urlopen`, `http.client` — zero matches.
* Daemon usage patterns `threading.Thread(`, `multiprocessing.Process(`, `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`, `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`, `while True:` — zero matches.

## 19. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 20. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Confirmed by isolation regex scan in suite L. Zero forbidden imports.
Zero edits to any non-Phase 18 production module. Phase 18 is additive: 3 new
files + 5 new folders + 6 new synthetic fixtures + 3 JSON reports.

## 21. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite L_ISOLATION asserts no thread/process/scheduler/Timer/while-True patterns. Runner is a pure call-graph: caller → function → caller.
* **No recursion** — no function in `phase18_pilot_import_runner.py` calls itself.
* **No full-corpus load** — all paths route through the Phase 16 generator-based streamers and the Phase 17 adapters. Synthetic 10k-row fixture validation sampled 100 rows each via the quality gate. Suite I/G in Phase 16/17 already covered this; Phase 18 inherits it.
* **No internet** — no `urllib`/`requests`/`httpx`/`urlopen`/etc. anywhere. No file fetched from network.

## 22. Rollback notes

Phase 18 wrote zero rows to production lexicons. To remove Phase 18 entirely:

```powershell
# 1. Remove new modules + harness + report
Remove-Item phase18_pilot_import_runner.py
Remove-Item test_phase18_pilot_import_and_retrieval_hardening.py
Remove-Item PHASE18_PILOT_IMPORT_AND_RETRIEVAL_HARDENING_REPORT.md

# 2. Remove Phase 18 folders + their contents
Remove-Item -Recurse corpus_sources/pilot_imports/phase18
Remove-Item -Recurse corpus_sources/evaluations/phase18
Remove-Item -Recurse corpus_sources/coverage_reports/phase18
Remove-Item -Recurse corpus_sources/quality_samples/phase18_synthetic
Remove-Item -Recurse corpus_sources/reports/phase18
```

If a future operator runs `allow_real_import=True` and **does** write to
production:

```sql
-- Rows: every Phase 18 pilot pack_id starts with "pilot_<lang>_<source_type>_<unix_ts>"
DELETE FROM words WHERE pack_id LIKE 'pilot_%';

-- Manifests: per-source manifests live next to each source file in
-- corpus_sources/<lang>/incoming/<file>.<lang>_pack_manifest.json
-- Remove only manifests whose pack_id matches the deletion set above.
```

Every pilot plan also stores a `rollback_key`:
```json
{"pack_id_prefix": "pilot_<lang>_<source_type>_<ts>",
 "language": "<en|ru>",
 "source_type": "<source_type>",
 "source_path": "<absolute path>"}
```
Combined with the per-source `corpus_import_report.json`, this gives an exact
rollback set for any specific real pilot, even if multiple ran.

No DB backup was taken in Phase 18 because no production write occurred. If a
future real pilot is run, take a SQLite `.backup` snapshot first:

```powershell
sqlite3 D:\SurgeApp\luna_english_lexicon.sqlite3 ".backup luna_english_lexicon.before_phase18_real.sqlite3"
sqlite3 D:\SurgeApp\luna_russian_lexicon.sqlite3 ".backup luna_russian_lexicon.before_phase18_real.sqlite3"
```

## 23. Next recommended phase

**Phase 19 — Operator-Staged First Real Corpus Pilot + Cross-Language Knowledge Linking.**

Once the operator stages an actual local corpus file under
`corpus_sources/english/incoming/` or `corpus_sources/russian/incoming/`,
Phase 19 should:

1. Run the **Phase 18 runner end-to-end** against the real file (discovery →
   register → quality gate → plan → dry-run → human-review → `allow_real_import=True`).
2. Add a **cross-language linker** that detects EN/RU semantic pairs and
   stores them in a new `cross_language_links` table (additive migration on
   both stores, link rows only, never merge the dictionaries).
3. Cap first real pilot at **10,000 rows per language**, then re-evaluate
   retrieval scores and coverage before relaxing.
4. Use the Phase 17 coverage reporter's `recommended_next_imports` to prioritize
   the staging order (the live report flagged 3 categories below 50 entries).

Soft target after Phase 19: ~12k EN words, ~12k RU words, with retrieval
average score still ≥ 0.80 EN / ≥ 0.75 RU, and zero safety violations.

## 24. Clean failure notes

None.

Pre-flight passed. All 10 harnesses green. Baseline counts unchanged.
Synthetic fixtures stream-validated. Retrieval evaluation bounds + safety
both clean. Coverage reporter wrote a valid report. No deferred work.
