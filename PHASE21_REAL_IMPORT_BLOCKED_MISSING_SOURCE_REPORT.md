# Phase 21 Real 10K Import — BLOCKED: Missing Operator-Staged Source Files

**Status:** **BLOCKED — STOPPED AT STEP 2 (DISCOVERY).** **Date:** 2026-05-21.

The Phase 21 first-real-import workflow was invoked but immediately halted
because no operator-staged corpus files exist in the incoming folders.
Per the spec's Step 2: "If either folder is empty: STOP. Do not import.
Write a clear report." That gate fired.

**No real import occurred. Production lexicons and bilingual link DB are
unchanged.**

## Step 1 — Pre-flight verification

All 8 required Phase 25 / Phase 21 / Phase 21A artifacts are present:

| File | Status |
|---|---|
| `PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md` | ✅ |
| `PHASE21_OPERATOR_STAGING_REQUIRED_REPORT.md` | ✅ |
| `phase21_operator_stage_runner.py` | ✅ |
| `phase21a_operator_corpus_staging.py` | ✅ |
| `dual_corpus_source_acceptance_validator.py` | ✅ |
| `phase21a_staging_readiness_gate.py` | ✅ |
| `dual_vocab_backup_restore.py` | ✅ |
| `dual_import_batch_ledger.py` | ✅ |

## Baseline counters (read-only)

| Counter | Value |
|---|---:|
| English words | 2,814 |
| Russian words | 2,518 |
| Russian phrases | 35 |
| Manifests | 90 |
| Bilingual concepts | 26 |
| Bilingual entry-links | 52 |

These values are recorded before the discovery gate fired so they can be
used as a witness later: any future post-import audit must match these
exactly until the operator stages files and runs a real import.

## Step 2 — Discovery (where it stopped)

Scanned only the two operator incoming folders.

| Folder | File count |
|---|---:|
| `corpus_sources\english\incoming\` | **0** |
| `corpus_sources\russian\incoming\` | **0** |

Both folders are empty. The Phase 21 spec's Step 2 gate is:

> "If either folder is empty: STOP. Do not import. Write a clear report:
> `PHASE21_REAL_IMPORT_BLOCKED_MISSING_SOURCE_REPORT.md`."

That gate fired. Steps 3–11 (validate sources, build 10K plan, backup, dry
run, safety audit, real import, post-import audit, full regression, final
report) were intentionally skipped.

## What the operator must do to unblock

### 1. Stage one English corpus file under

```
D:\SurgeApp\corpus_sources\english\incoming\
```

### 2. Stage one Russian corpus file under

```
D:\SurgeApp\corpus_sources\russian\incoming\
```

### 3. Supported file formats

* `.jsonl` — **preferred** (one Luna-canonical row per line).
* `.txt` — one word or phrase per line (use `phrase_list` / `idiom_list`
  source types for phrase rows).
* `.csv` — header row required; columns
  `word,definition,pos,coverage,register,safety,domain` recommended.

### 4. Required JSONL fields per row

| Field | Required | Notes |
|---|---|---|
| `word` | yes (or `phrase` for phrase/idiom sources) | the surface form |
| `language` | yes | `en` or `ru` |
| `definition` | recommended | short, plain-language |
| `coverage_categories` | recommended (list) | must be from the 21 canonical categories |
| `register_tags` | recommended (list) | must be from the 22 canonical registers |
| `safety_tags` | optional (list) | empty list for benign rows |
| `domain_tags` | optional (list) | free-form domain |
| `lemma`, `part_of_speech` | optional, Russian rows only | preserves morphology for future upgrade |

### 5. Important tagging rules

* Vulgar / offensive terms **must** also carry
  `safety_tags: ["recognition_only", "do_not_use_unprompted"]`.
* Sensitive medical / legal / cybersecurity vocabulary should be added as
  vocabulary only — never as operational guidance.
* No prompt-injection markers
  (`ignore previous instructions`, `system prompt:`, etc.).
* No step-by-step operational unsafe content.

### 6. First-import sizing

Recommended **5,000–10,000 rows per language** for the first real import.
Smaller files are allowed but will downgrade the staging readiness state
to `READY_FOR_DRY_RUN_ONLY`.

## Where to find canonical examples

Two operator-ready template trees already exist locally:

* `corpus_sources/phase21a/templates/` — Phase 21A staging-kit templates
  (English/Russian JSONL, slang JSONL, CSV glossary, etc.).
* `corpus_sources/templates/` — earlier Phase 17 templates.

Both produce rows that pass the Phase 21A acceptance validator with
`acceptance_rate=1.0`. The operator can copy one template into the
appropriate `incoming/` folder, expand it to 5–10k rows, and rerun the
import workflow.

## Re-run sequence after staging files

```
python -c "import phase21a_operator_corpus_staging as p21a; \
print([s['path'] for s in p21a.discover_incoming_files()])"
python test_phase21a_operator_corpus_staging.py
```

Then inspect:

```
corpus_sources/phase21a/validation_reports/
corpus_sources/phase21a/repair_previews/
corpus_sources/phase21a/ready_reports/
```

If the latest ready_report state is `READY_FOR_PHASE21_REAL_IMPORT`, rerun
the Phase 21 real-import workflow (this task). It will pick up the staged
files at Step 2 and continue through backup → dry run → safety audit →
real import → post-import audit.

## Hard-rule confirmations

* **No internet used.** Discovery is filesystem-only.
* **No download.** No HTTP/FTP/socket call.
* **No fake corpus generated.** Per "Do not create fake real corpus files."
* **Program S untouched.** Not opened, not imported.
* **`tier/probe/attestation/worker.py/luna_modules` untouched.**
* **No daemon, no recursion, no scheduler, no service.**
* **No full-corpus load.** Discovery only `stat()`s file entries.
* **Production lexicons unchanged.** EN=2,814, RU=2,518, RU phrases=35.
* **Bilingual link DB unchanged.** 26 concepts, 52 entry-links.
* **Manifest count unchanged.** 90 (45 EN + 45 RU).
* **Safety policy not weakened.** No rows added, no rules relaxed.

## Rollback / cleanup

No real-import side effects to roll back — nothing was written. This
report is the only artifact produced; remove it after the operator stages
files:

```powershell
Remove-Item PHASE21_REAL_IMPORT_BLOCKED_MISSING_SOURCE_REPORT.md
```

## Decision

**Not ready for 25K stage escalation.** Phase 21 real import has not yet
occurred. 25K escalation is unlocked only after a clean 10K real import +
green post-import audit + green retrieval SLA + green safety regression.
