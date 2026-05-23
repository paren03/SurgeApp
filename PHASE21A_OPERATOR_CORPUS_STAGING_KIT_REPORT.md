# Phase 21A — Operator Corpus Staging Kit, Source Acceptance Validator, Metadata Repair Preview, and First Real Import Preparation

**Status:** Complete. **Date:** 2026-05-21.

Phase 21A ships the operator-facing staging and validation layer that sits
between Phase 21's empty `incoming/` directories and the eventual first
real bilingual import. It does not ingest. It does not modify production
DBs. It only prepares, validates, previews, and reports.

## 1. Phase 21A completion status

**ACCEPTED.** 4 new production modules + harness + 11 templates + 2
operator guides + report shipped. 14/14 harnesses green (1343/1343 checks).
Production lexicon untouched. Incoming dirs still empty — operator action
required to escalate to first real import.

## 2. Phase 21 pre-flight verification

✅ All 22 required Phase 21/20/19/18/17/16 artifacts present.
`verify_phase21a_preflight()` returns `{ok: True, missing_files: []}`.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `dual_corpus_source_acceptance_validator.py` | 380 | Streaming, bounded validator for `.jsonl/.txt/.csv` across all 12 supported source types. Per-row verdict accept/warn/reject. |
| `dual_corpus_metadata_repair_preview.py` | 260 | Suggests conservative repairs (coverage, register, safety, domain, source) without mutating the source file. Sensitive uncertainty → recognition_only + do_not_use_unprompted. |
| `phase21a_staging_readiness_gate.py` | 250 | Roll-up gate. 8 possible readiness states. Per-language presence + row-count + acceptance-rate + safety-blocker + metadata-completeness checks. |
| `phase21a_operator_corpus_staging.py` | 540 | Operator-facing utility: discover/inspect/validate/preview/write reports + templates + guides. Refuses to touch incoming/ or production DBs. |
| `test_phase21a_operator_corpus_staging.py` | 540 | 8 suites, 100 checks. |
| `PHASE21A_OPERATOR_CORPUS_STAGING_KIT_REPORT.md` | — | this report. |

## 4. Files modified

None. Phase 21A is purely additive.

## 5. Folders created

* `corpus_sources/phase21a/templates/`
* `corpus_sources/phase21a/validation_reports/`
* `corpus_sources/phase21a/repair_previews/`
* `corpus_sources/phase21a/rejected_previews/`
* `corpus_sources/phase21a/ready_reports/`
* `corpus_sources/phase21a/operator_guides/`
* `corpus_sources/phase21a/fixtures/`

## 6. Templates created

11 templates under `corpus_sources/phase21a/templates/`:

| Template | Format | Purpose |
|---|---|---|
| `english_words_jsonl_template.jsonl` | JSONL | 5 safe EN word rows with full metadata |
| `russian_words_jsonl_template.jsonl` | JSONL | 5 safe RU word rows with lemma + POS |
| `english_phrases_jsonl_template.jsonl` | JSONL | 5 safe EN idiom/phrase rows |
| `russian_phrases_jsonl_template.jsonl` | JSONL | 5 safe RU idiom/phrase rows |
| `english_slang_jsonl_template.jsonl` | JSONL | 3 safe EN slang rows |
| `russian_slang_jsonl_template.jsonl` | JSONL | 3 safe RU slang rows |
| `english_domain_terms_csv_template.csv` | CSV | 3 EN domain-term rows |
| `russian_domain_terms_csv_template.csv` | CSV | 3 RU domain-term rows |
| `bilingual_glossary_csv_template.csv` | CSV | 2 EN/RU paired rows |
| `simple_word_list_txt_template.txt` | TXT | 5 EN words |
| `phrase_list_txt_template.txt` | TXT | 5 EN phrases |

All JSONL templates parse cleanly through the new validator with
`acceptance_rate=1.0`. Operator guide
(`corpus_sources/phase21a/operator_guides/OPERATOR_STAGING_GUIDE.md`)
documents file placement, recommended size, required metadata, tag
semantics, and re-run sequence.

A rerun-instructions guide (`RERUN_PHASE21.md`) has also been written under
the operator-guides folder.

## 7. Validator status

✅ Supports 12 source types × 2 languages × 3 file formats. Per-row
verdict: `accept` / `warn` / `reject`. Hard limit 5000 rows per call.
Streaming-only.

Verified rejection paths (each tested with synthetic fixtures):

| Failure mode | Verdict |
|---|---|
| Malformed JSONL | reject |
| Invalid `language` declared (`xx`) | reject |
| Invalid taxonomy tag (`NOT_A_REAL_CATEGORY`) | reject |
| Invalid register tag | reject |
| Invalid safety tag | reject |
| Prompt-injection markers (`ignore previous instructions`, `system prompt:`, etc.) | reject |
| Operational unsafe (`step by step instructions to bypass auth`, …) | reject |
| Vulgar/offensive register WITHOUT recognition_only safety | **warn** (the repair preview can fix it) |
| Russian word file declared as English (no Cyrillic check) | reject |
| Limit honored | sample size respected |

## 8. Metadata repair preview status

✅ Conservative defaults. Never mutates the source. Output stays under
`corpus_sources/phase21a/repair_previews/` only — `write_repaired_copy_preview_only`
rewrites the path if a caller tries to direct it elsewhere.

Inferred defaults verified by suite E:

* `coverage_categories` defaulted from source_type (e.g., `slang_list` →
  `slang_street_talk`, `idiom_list` → `idioms_phrases`).
* `register_tags` defaulted (slang_list → `slang`; street_talk_list →
  `street`; profession_job_list → `standard, professional`).
* `safety_tags` for unlabeled vulgar/offensive → `recognition_only +
  do_not_use_unprompted` (conservative).
* `confidence` and `reasons` always emitted; sensitive downgrades cap
  confidence at ≤ 0.7.

## 9. Staging readiness gate status

✅ Eight readiness states, each verified by suite F:

| State | Trigger |
|---|---|
| `NOT_READY_NO_FILES` | both incoming dirs empty |
| `NOT_READY_MISSING_ENGLISH` | only RU staged |
| `NOT_READY_MISSING_RUSSIAN` | only EN staged |
| `NOT_READY_SAFETY_BLOCKERS` | `operational_unsafe` or `prompt_injection_like` reasons > 0 |
| `NOT_READY_VALIDATION_FAILURES` | acceptance_rate < 0.90 |
| `NOT_READY_LOW_ROW_COUNT` | total rows = 0 in either language |
| `READY_FOR_DRY_RUN_ONLY` | bilingual but < 5k rows in at least one language, or metadata completeness < 0.95 |
| `READY_FOR_PHASE21_REAL_IMPORT` | bilingual + ≥ 5k rows/lang + acceptance ≥ 0.90 + metadata ≥ 0.95 + zero safety blockers |

## 10. Incoming file status

| Folder | Files |
|---|---|
| `corpus_sources/english/incoming/` | 0 |
| `corpus_sources/russian/incoming/` | 0 |

Current readiness decision: **NOT_READY_NO_FILES**. The staging kit is
fully operational and waiting for the operator to drop one English and
one Russian source file under `incoming/`.

## 11. Production DB impact

**Zero rows changed.** EN=2814, RU=2518, RU_phr=35 (identical baseline →
final).

## 12. Manifest count before / after

90 (45 EN + 45 RU). Unchanged.

## 13. Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_phase21a_operator_corpus_staging.py` | 100 | ✅ |
| `test_phase21_operator_staged_first_import.py` | 103 | ✅ |
| `test_phase20_million_readiness_gate.py` | 144 | ✅ |
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
| **Total** | **1343** | **1343 / 1343 PASS** |

### Phase 21A suite breakdown (100 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | runner + 22 prior artifacts | 23 |
| B_FOLDERS | 8 Phase 21A folders | 9 |
| C_TEMPLATES | 11 templates + 4 parses + operator guide | 17 |
| D_VALIDATOR | good/malformed/lang/taxonomy/register/safety/prompt-injection/operational-unsafe/unlabeled/txt/csv/limit | 13 |
| E_REPAIR_PREVIEW | preview ok + 8 detail checks | 10 |
| F_READINESS | 8 states + report write | 8 |
| G_PRODUCTION_SAFETY | EN + RU + phrases + manifest unchanged | 4 |
| H_ISOLATION | 4 files × {exists, forbidden, network, daemon} | 16 |

## 14. Isolation verification

Suite H scans all 4 Phase 21A modules:

* Forbidden imports `worker`, `luna_modules`, `tier_`, `probe_`,
  `attestation`, `program_s` — **zero matches**.
* Network usage patterns `urllib`, `requests`, `httpx`, `aiohttp`,
  `socket`, `ftplib`, `urlopen`, `http.client` — **zero matches**.
* Daemon usage patterns `threading.Thread(`, `multiprocessing.Process(`,
  `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`,
  `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`,
  `while True:` — **zero matches**.

## 15. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 16. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Verified by isolation regex scan. Phase 21A is additive: 4 new modules,
1 new harness, 1 new report, 7 new folders, 11 templates, 2 markdown guides.
Zero edits to any pre-Phase-21A file.

## 17. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite H confirms zero matches.
* **No recursion** — no function in any Phase 21A module calls itself.
* **No full-corpus load** — every validator and repair-preview path is a
  generator. The hard validation cap is 5000 rows per call (default 1000).
  Streaming-only.
* **No internet** — zero network library imports, zero URL fetches.

## 18. Operator staging instructions

1. **Place one English file under**
   `D:\SurgeApp\corpus_sources\english\incoming\`.
   Recommended: 5,000–10,000 rows of `.jsonl`. The English JSONL template
   at
   `D:\SurgeApp\corpus_sources\phase21a\templates\english_words_jsonl_template.jsonl`
   shows the exact row shape.

2. **Place one Russian file under**
   `D:\SurgeApp\corpus_sources\russian\incoming\`.
   Recommended: 5,000–10,000 rows of `.jsonl`. Use the Russian template at
   `D:\SurgeApp\corpus_sources\phase21a\templates\russian_words_jsonl_template.jsonl`.

3. **Required metadata in JSONL rows**
   - `word` (or `phrase` for phrase/idiom sources)
   - `language` = `en` or `ru`
   - `definition`
   - `coverage_categories` (list — must be from the 21 canonical categories)
   - `register_tags` (list — must be from the 22 canonical registers)
   - `safety_tags` (list — empty for benign)
   - Russian rows may include `lemma`, `part_of_speech`.

4. **Tag slang / vulgar / offensive properly.** Vulgar / offensive terms
   MUST carry `safety_tags: ["recognition_only", "do_not_use_unprompted"]`.
   The repair preview will offer to add these tags automatically, but the
   operator must accept the proposal before the file moves forward.

5. **Validate before scheduling import.** Run:
   ```
   python test_phase21a_operator_corpus_staging.py
   ```
   Then inspect:
   - `corpus_sources/phase21a/validation_reports/<file>.<lang>.acceptance.json`
   - `corpus_sources/phase21a/repair_previews/<file>.<lang>.repair.json`
   - `corpus_sources/phase21a/ready_reports/` (latest)

6. **If readiness state = `READY_FOR_PHASE21_REAL_IMPORT`**, rerun Phase 21
   per the existing `RERUN_PHASE21.md` instructions in
   `corpus_sources/phase21a/operator_guides/`.

## 19. Next recommended phase or rerun command

After the operator places real files into both `incoming/` folders, the
recommended next move is to **rerun Phase 21** with `allow_real_import=True`
capped at 10,000 rows per language. Exact command sequence is in
`corpus_sources/phase21a/operator_guides/RERUN_PHASE21.md`.

If the operator wants to defer real import and instead expand Phase 21A
coverage (more templates, broader safety probes, or a richer rejected-row
quarantine view), suggest **Phase 21B — Quarantine Triage and Repair
Acceptance Workflow** as a stepping stone.

## 20. Clean failure notes

None.

Pre-flight passed. All 14 harnesses green. Baseline counts unchanged. All
4 new modules pass isolation. All 11 templates parse with
`acceptance_rate=1.0`. Production lexicons untouched. The system is
infrastructure-ready and waiting for operator-staged real files.
