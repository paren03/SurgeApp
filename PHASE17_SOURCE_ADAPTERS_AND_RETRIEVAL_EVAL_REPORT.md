# Phase 17 — Source Adapters, Pilot Planning, Retrieval Evaluation, and Coverage Reporting

**Status:** Complete. **Date:** 2026-05-21.

Phase 17 layers an ingest-format normalization tier, a controlled pilot-import
planner, a retrieval-quality evaluator, and a coverage reporter on top of the
Phase 16 million-scale readiness foundation. No real large corpus is ingested.
Production lexicon DBs are read-only inspected.

## 1. Phase 17 completion status

ACCEPTED. All four modules shipped, all 9 harnesses (Phase 17 + 8 prior) PASS,
baseline lexicon untouched.

## 2. Phase 16 pre-flight verification

| Required artifact | Status |
|---|---|
| `PHASE16_MILLION_SCALE_READINESS_REPORT.md` | ✅ present |
| `test_phase16_million_scale_readiness.py` | ✅ present |
| `dual_corpus_registry.py` | ✅ present |
| `dual_corpus_chunked_importer.py` | ✅ present |
| `dual_corpus_quality_gate.py` | ✅ present |
| `dual_corpus_checkpoint.py` | ✅ present |
| `corpus_sources/` folder tree | ✅ present |

Earlier reports also verified present: `PHASE15A`, `PHASE15B`, `PHASE14`,
`DUAL_PACK_IMPORTERS_AND_SEED_PACKS_REPORT.md`,
`DUAL_SOVEREIGN_SAFETY_TAXONOMY_REPORT.md`. Pre-flight pass — no clean-failure
path needed.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `dual_corpus_source_adapters.py` | 466 | 12 adapter types; `detect_adapter_type`, per-format normalizers, `iter_normalized_rows` generator, `write_adapter_preview` |
| `dual_corpus_pilot_import_planner.py` | 248 | `discover_incoming_sources`, `build_pilot_plan`, `choose_safe_max_entries`, `choose_batch_size`, `require_quality_gate_pass`, `run_pilot_dry_run`, `run_pilot_import`, plan/result writers |
| `dual_retrieval_quality_eval.py` | 268 | 12 EN + 12 RU canonical queries, per-row safety/register/coverage scoring, `run_english_retrieval_eval`, `run_russian_retrieval_eval`, `write_retrieval_eval_report` |
| `dual_coverage_reporter.py` | 220 | SQL-aggregate counts by category/register/safety/pack_id, low-coverage detector, metadata-gap detector, EN/RU balance comparator, `write_coverage_report` |
| `test_phase17_source_adapters_and_retrieval_eval.py` | 510 | 9 suites A–I, 108 checks |
| `PHASE17_SOURCE_ADAPTERS_AND_RETRIEVAL_EVAL_REPORT.md` | — | this report |

## 4. Files modified

None. Phase 17 is purely additive.

## 5. Folders and templates created

Folders under `corpus_sources/`:
* `adapters/`
* `pilot_imports/`
* `evaluations/`
* `coverage_reports/`
* `templates/`

Templates under `corpus_sources/templates/` (10 files):
* `english_luna_jsonl_template.jsonl` (5 rows)
* `russian_luna_jsonl_template.jsonl` (5 rows)
* `profession_job_csv_template.csv` (5 rows)
* `domain_terms_csv_template.csv` (5 rows)
* `bilingual_glossary_csv_template.csv` (5 rows)
* `russian_morphology_csv_template.csv` (5 rows)
* `simple_word_list_template.txt` (5 entries)
* `phrase_list_template.txt` (5 phrases)
* `slang_list_template.txt` (5 terms)
* `frequency_word_list_template.txt` (5 entries)

All templates use valid coverage / register / safety tags from
`coverage_taxonomy.py`. All JSONL/CSV templates parse cleanly via the new
adapters.

Real evaluation/coverage artifacts written to disk:
* `corpus_sources/evaluations/retrieval_eval_en.json`
* `corpus_sources/evaluations/retrieval_eval_ru.json`
* `corpus_sources/coverage_reports/coverage_report.json`

## 6. Source adapters status

**12 adapter types implemented**, all bounded streaming:

| Adapter | Source format | Auto-tag rules |
|---|---|---|
| `luna_jsonl` | JSONL of Luna-canonical rows | Passthrough; sensitive→recognition_only |
| `wiktextract_jsonl` | Wiktextract-style JSONL | Pulls `senses[*].glosses` + `examples` |
| `simple_word_list_txt` | One word per line | tags = `core_vocabulary` / `standard` |
| `frequency_word_list_txt` | `word freq` per line | frequency_score preserved/normalized |
| `phrase_list_txt` | One phrase per line | tags = `core_vocabulary` / `standard` |
| `idiom_list_txt` | One idiom per line | tags = `idioms_phrases` |
| `slang_list_txt` | One slang term per line | adds `slang` register; never auto-vulgar |
| `profession_job_csv` | CSV with `word/definition/...` | adds `professions_jobs` if missing |
| `domain_terms_csv` | General CSV | tags from `coverage`/`register` columns |
| `bilingual_glossary_csv` | EN/RU paired columns | routes by primary lang detected |
| `russian_morphology_csv` | RU `word/lemma/pos/...` | preserves `lemma`/`pos` and morph extras |
| `mixed_jsonl` | Mixed shapes | falls through to luna_jsonl normalizer |

Each adapter goes through `_finalize` which:
* enforces sensitive-unlabeled → `recognition_only` + `do_not_use_unprompted`,
* rejects operational-unsafe text markers (`"step by step instructions to bypass auth"` etc.),
* refuses any row missing both `word` and `phrase`.

## 7. Pilot planner status

Implemented per spec. Behavioural invariants verified by suite D:

* `dry_run_default=True` on every plan.
* `required_quality_gate=True` on every plan.
* `choose_safe_max_entries(...)` caps at 200 for quality<0.6, 500 for <0.75,
  300 for slang/street, otherwise `min(target, DEFAULT_TARGET_ENTRIES=1000)`.
* `choose_batch_size(...)` returns 10–50 for tiny files, 200 for <1 MB,
  500 for <50 MB, 1000 for larger.
* `require_quality_gate_pass(...)` delegates to
  `dual_corpus_quality_gate.should_allow_import` with `min_quality_score=0.75`.
* Bad-quality plans are refused with `error="quality_gate_blocked"`.
* `rollback_key` carries `{pack_id_prefix, language, source_type, source_path}`.
* `discover_incoming_sources(...)` is bounded; never opens files.

## 8. Retrieval evaluation status

12 EN + 12 RU canonical queries (professions_jobs, trades_construction,
poetry_literary, philosophy_abstract, business_finance, law_government,
science_math, coding_technology, slang_street_talk [gated],
voice_personality, idioms_phrases, core_vocabulary).

Live run against the existing production lexicon:

| Language | Avg score | Bounds OK | Safety OK | Queries |
|---|---:|---|---|---:|
| EN | **0.815** | ✅ | ✅ | 12 |
| RU | **0.777** | ✅ | ✅ | 12 |

Every row returned by every query was checked for:
* coverage category hit ratio against the query's `expected` categories
* `do_not_use_unprompted` violations (zero)
* `recognition_only` slipping into suggestion context (counted as soft signal,
  zero hard violations in default teacher mode)
* vulgar/offensive in teacher mode without user prompt (zero)
* register fit (street/vulgar without recognition_only → mismatch)

Per-query result limit was 15; the bounds checker confirmed
`returned <= limit` for every query.

## 9. Coverage reporter status

Read-only SQL aggregate against the production EN/RU lexicons. Implemented:

* `count_entries_by_language()` — EN words / RU words / RU phrases.
* `count_entries_by_coverage_category(lang)` — counts for all 21 canonical categories.
* `count_entries_by_register_tag(lang)` — counts for all 22 register tags.
* `count_entries_by_safety_tag(lang)` — counts for all 4 safety tags.
* `count_entries_by_pack_id(lang, limit=100)` — bounded grouping.
* `identify_low_coverage_categories(lang, min_entries=100)` — categories below threshold.
* `identify_missing_metadata(lang, limit=100)` — rows with empty coverage/register.
* `compare_english_russian_category_balance()` — per-category EN/RU diffs.
* `write_coverage_report(output_path)` — produces full JSON report.

`I_BOUNDS::coverage_reporter_is_read_only` proves the reporter does not mutate
the lexicon (count before == count after a full reporter sweep).

## 10. Local pilot import status

No real local corpora are present under `corpus_sources/english/incoming/` or
`corpus_sources/russian/incoming/`. **No real pilot import was executed.**
Synthetic fixtures drove the planner tests through dry-run paths into temp DBs.

To run a real pilot when operator stages a file:

```python
# 1. Drop file into corpus_sources/english/incoming/<name>.jsonl (or russian/)
# 2. Plan
import dual_corpus_pilot_import_planner as pip_
plan = pip_.build_pilot_plan(
    "corpus_sources/english/incoming/<name>.jsonl",
    language="en", source_type="word_list",
    target_entries=1000)["plan"]
pip_.write_pilot_plan(plan, f"corpus_sources/pilot_imports/{plan['plan_id']}.json")

# 3. Dry run (default)
result_dry = pip_.run_pilot_dry_run(plan)
pip_.write_pilot_result(result_dry,
    f"corpus_sources/pilot_imports/{plan['plan_id']}.dry.json")

# 4. Inspect dry run + quality_report. If satisfied, run real:
result_real = pip_.run_pilot_import(plan, dry_run=False)
pip_.write_pilot_result(result_real,
    f"corpus_sources/pilot_imports/{plan['plan_id']}.real.json")
```

## 11. Production DB impact

**Zero**. Production English and Russian lexicons were inspected only.

| Counter | Before Phase 17 | After Phase 17 | Δ |
|---|---:|---:|---:|
| EN words | 2814 | 2814 | 0 |
| RU words | 2518 | 2518 | 0 |
| RU phrases | 35 | 35 | 0 |
| Manifest files | 90 | 90 | 0 |

## 12. Manifest count before / after

Before: 90 (45 EN + 45 RU). After: 90 (45 EN + 45 RU). Unchanged.

## 13. Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_phase17_source_adapters_and_retrieval_eval.py` | 108 | ✅ |
| `test_phase16_million_scale_readiness.py` | 80 | ✅ |
| `test_phase15b_remaining_domain_expansion.py` | 117 | ✅ |
| `test_phase15a_controlled_scale_expansion.py` | 63 | ✅ |
| `test_phase14_domain_pack_expansion.py` | 87 | ✅ |
| `test_dual_pack_importer.py` | 116 | ✅ |
| `test_dual_sovereign_pack_safety.py` | 79 | ✅ |
| `test_vocabulary_runtime.py` | 74 | ✅ |
| `test_russian_sovereign_stack.py` | 73 | ✅ |
| **Total** | **797** | **797 / 797 PASS** |

### Phase 17 suite breakdown (108 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | Phase 16 files + folders + clean-fail path | 8 |
| B_DETECTION | 8 adapter detections + bad-file clean failure | 9 |
| C_NORMALIZE | All per-adapter normalizers + max_rows + streaming | 17 |
| D_PLANNER | size pickers, plan build, dry-run, qgate refusal, discovery bounded | 16 |
| E_EVAL | EN+RU 12-query runs, safety check, coverage check, bounds, report | 11 |
| F_COVERAGE | totals, 21+22+4 keys, gaps, balance, report | 11 |
| G_TEMPLATES | 9 templates exist + parse + tags valid | 22 |
| H_ISOLATION | forbidden imports + network + daemon usage patterns | 12 |
| I_BOUNDS | streaming caps + read-only-reporter | 3 |

## 14. Safety verification

Two-layer safety still holds:

* `dual_corpus_source_adapters._finalize` rejects operational-unsafe text markers (matching the Phase 16 `_SENSITIVE_FREE_TEXT_MARKERS` set).
* Slang/street adapters auto-add `slang`/`street` registers but NEVER `vulgar` or `offensive`.
* Sensitive-but-unlabeled rows are downgraded to `recognition_only` + `do_not_use_unprompted` instead of being passed through naked.
* Retrieval evaluator hard-fails when `do_not_use_unprompted` rows would surface to teacher mode unprompted.
* Pilot planner refuses real-write when the quality gate is closed (`error="quality_gate_blocked"`).
* Live retrieval evaluation produced **zero** `do_not_use_unprompted` violations and **zero** `vulgar`/`offensive` surfacing in default teacher mode across all 24 queries.

## 15. Isolation verification

Suite H verifies all four Phase 17 production modules against:
* Forbidden imports: `worker`, `luna_modules`, `tier_`, `probe_`, `attestation`, `program_s` — zero matches in any file.
* Network usage patterns: `urllib`, `requests`, `httpx`, `aiohttp`, `socket`, `ftplib`, `urlopen`, `http.client` — zero matches.
* Background-daemon usage patterns: `threading.Thread(`, `multiprocessing.Process(`, `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`, `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`, `while True:` — zero matches.

## 16. Confirmation Program S was not touched

✅ No file under `Program S` was opened, read, edited, imported, or referenced.

## 17. Confirmation no tier / probe / attestation / worker / luna_modules files were touched

✅ Confirmed by isolation regex scan in suite H_ISOLATION across all four new
modules. Zero forbidden imports.

## 18. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — Suite H verified no thread, process, scheduler, or
  `while True:` loop in any Phase 17 module.
* **No recursion blow-up** — All iteration is streaming via generators. No
  function calls itself in Phase 17 production modules.
* **No full-corpus load** — `iter_normalized_rows`, `stream_jsonl_rows`,
  `stream_txt_rows`, `stream_csv_rows`, `_iter_jsonl`, `_iter_txt`, `_iter_csv`
  are all generator-based. The 2000-row streaming test in `I_BOUNDS` confirms
  `max_rows` cap holds end-to-end.
* **No internet usage** — Suite H verified no network library import or call
  pattern. No `WebFetch`/`WebSearch` invoked.

## 19. Rollback notes

Phase 17 produced zero writes to the production lexicons. To remove Phase 17:

```powershell
# 1. Remove new modules
Remove-Item dual_corpus_source_adapters.py
Remove-Item dual_corpus_pilot_import_planner.py
Remove-Item dual_retrieval_quality_eval.py
Remove-Item dual_coverage_reporter.py
# 2. Remove harness
Remove-Item test_phase17_source_adapters_and_retrieval_eval.py
# 3. Remove report + templates folder
Remove-Item PHASE17_SOURCE_ADAPTERS_AND_RETRIEVAL_EVAL_REPORT.md
Remove-Item -Recurse corpus_sources/templates
Remove-Item -Recurse corpus_sources/adapters
Remove-Item -Recurse corpus_sources/pilot_imports
Remove-Item -Recurse corpus_sources/evaluations
Remove-Item -Recurse corpus_sources/coverage_reports
```

For any future pilot import that DID write rows, rollback is by `pack_id`:

```sql
DELETE FROM words WHERE pack_id LIKE 'pilot_<lang>_<source_type>_%';
```

The `rollback_key` stored on every pilot plan carries the exact prefix needed.

## 20. Next recommended phase

**Phase 18 — Adaptive Pack Generation From Coverage Gaps + Bilingual Cross-Coverage Auto-Suggestion.**

Use the new coverage reporter's low-coverage list to drive synthesis of small,
test-locked pack drafts under `seed_packs/<lang>/phase18_*.jsonl`. Each draft
should go through the Phase 17 pilot planner + quality gate before being
promoted. Top targets from the live coverage report:

* **EN low-coverage (<100 entries):** `art_music_culture`, `coding_technology`, and 2 more.
* **RU low-coverage (<100 entries):** 3 categories.
* **Recommended next-import targets (<50 entries):** 3 categories total — surfaced by `write_coverage_report.recommended_next_imports`.

Suggested guardrails: Phase 18 stays additive, uses operator-staged drafts
only, runs every promoted draft through the pilot planner + quality gate, and
preserves all Phase 17 retrieval scores.

## 21. Clean failure notes

None. Pre-flight passed, all 9 harnesses green, baseline counts unchanged,
templates parsed cleanly, both retrieval evaluations cleared their bounds and
safety checks, coverage reporter wrote a valid report. No deferred work, no
broken paths.
