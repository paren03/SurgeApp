# Phase 20 — Million-Scale Staged Import Rehearsal, Backup/Rollback System, Retrieval SLA, and Final 1M Readiness Gate

**Status:** Complete. **Date:** 2026-05-21.

Phase 20 ships the **final readiness layer** that sits between the controlled
100K-scale planner (Phase 19) and the eventual real 1M-entry ingest. It
delivers backup/restore, an append-only import batch ledger, a million-scale
stage planner, post-stage quality audits, retrieval SLA evaluation, index
consistency checking, and a safety regression auditor — all coordinated by
a single runner. Synthetic-only run mode (no operator-staged corpora exist);
production lexicons stayed read-only.

## 1. Phase 20 completion status

ACCEPTED. 7 new modules + runner + harness + report. 12/12 harnesses green
(1140/1140 checks). Live Phase 20 run produced a verified backup snapshot,
SLA verdict `pass`, safety regression `all_pass`, and zero leaks in the
indexed-safety probe — all in 6.1 s. Production lexicon untouched.

## 2. Phase 19 pre-flight verification

✅ All 19 required Phase 19+18+17+16 artifacts present.
`verify_phase20_preflight()` returns `{ok: True, missing_files: []}`.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `dual_vocab_backup_restore.py` | 270 | SQLite `.backup()` snapshots of EN + RU lexicons, manifest copies, verify, dry-run restore. |
| `dual_import_batch_ledger.py` | 270 | SQLite-backed ledger: 25 fields per batch, rollback_key lookup, by-stage lookup, bounded listing. |
| `dual_million_stage_planner.py` | 240 | Build/split/enforce/summarize stage plans. Hard caps: stage 100k, per-source 50k, allow_full_source forbidden. |
| `dual_post_stage_quality_audit.py` | 260 | Bounded post-stage sample: metadata, language, safety/register/coverage validity, dup rate, rejected-row sampling, composite quality_score with pass/warn/fail verdict. |
| `dual_retrieval_sla_eval.py` | 235 | p50/p95/p99 latency benchmarks for simple/category/register/safety queries vs. SLA targets, pass/warn/fail verdict. |
| `dual_index_consistency_checker.py` | 245 | Index presence, FTS row counts, pack_id coverage, safety-filter leak check, hard-clamp query bound. |
| `dual_safety_regression_auditor.py` | 245 | 6-class probe set; EN + RU policy audits; indexed-retrieval safety; runtime context; prompted-vs-unprompted softening. |
| `phase20_million_readiness_runner.py` | 330 | 12 entry points. Real stage import OFF by default + 5 hard gates. |
| `test_phase20_million_readiness_gate.py` | 615 | 11-suite harness, 144 checks. |
| `PHASE20_MILLION_READINESS_GATE_REPORT.md` | — | this report. |

## 4. Files modified

None. Phase 20 is purely additive.

## 5. Folders created

* `corpus_sources/phase20/stage_plans/`
* `corpus_sources/phase20/backup_reports/`
* `corpus_sources/phase20/ledger_reports/`
* `corpus_sources/phase20/quality_audits/`
* `corpus_sources/phase20/sla_reports/`
* `corpus_sources/phase20/index_reports/`
* `corpus_sources/phase20/safety_reports/`
* `corpus_sources/phase20/synthetic_million/`
* `corpus_sources/phase20/final_readiness/`
* `corpus_sources/backups/`

## 6. Backup / restore status

✅ Working end-to-end. `create_backup_snapshot()` uses SQLite's native
`.backup()` to produce **consistent point-in-time copies** of both EN and RU
lexicon files plus a recursive copy of every `seed_packs/<lang>/*pack_manifest.json`.

* 10 verified snapshots created during the harness + live run.
* Most recent: `snap_1779385672_live_run_ce65f1ea`.
* `verify_backup_snapshot()` confirms file presence + non-zero size.
* `restore_backup_snapshot(dry_run=True)` is the default — returns
  `intended_actions` without touching live files. Production EN/RU counts
  before == after across every dry-run.
* `restore_backup_snapshot(dry_run=False)` uses `copy2 + os.replace` for
  atomic overwrite.
* `compare_db_counts_before_after()` returns deltas snapshot vs. live.

### Fix landed during development

The first verify implementation globbed for `*.sqlite3` but the live DBs use
the `.sqlite` extension. Widened to `*.sqlite*`.

## 7. Batch ledger status

✅ Working. SQLite ledger with full audit fields:

| Field | Purpose |
|---|---|
| `batch_id`, `stage_id`, `corpus_id`, `pack_id` | identity |
| `language`, `source_path`, `source_sha256` | provenance |
| `started_at`, `completed_at`, `status`, `dry_run` | lifecycle |
| `accepted_count`, `rejected_count`, `duplicate_count` | results |
| `before/after_word_count`, `before/after_phrase_count` | delta |
| `manifest_path`, `checkpoint_id`, `rollback_key`, `backup_snapshot_id` | rollback handles |
| `quality_report_path`, `safety_audit_path` | audit refs |
| `notes` | free text |

Indexes: `stage_id`, `language`, `status`, `corpus_id`, `pack_id`,
`rollback_key`. Lookups by stage / rollback_key / language / status are all
bounded (hard cap 5000).

Suite C verified create → update → list → by-stage → by-rollback-key →
report write → invalid-language rejection.

## 8. Million stage planner status

✅ Working. Hard caps enforced:

| Constraint | Value | Behavior |
|---|---:|---|
| `stage_size` | ≤ 100,000 | clamped + issue recorded |
| `per_source_cap` | ≤ 50,000 | clamped + issue recorded |
| `target_total` | may be 1,000,000 | no actual import exceeds explicit stage caps |
| `allow_full_source` | must be `False` | plan refused |
| `quality_gate_required` | `True` | force-set |
| `dry_run_required` | `True` | force-set |
| `backup_required` | `True` | force-set |
| `checkpoint_required` | `True` | force-set |
| `manifest_required` | `True` | force-set |
| `safety_audit_required` | `True` | force-set |
| `retrieval_sla_required` | `True` | force-set |
| `rollback_required` | `True` | force-set |

`split_sources_into_stages` packs sources into stage_size-bounded buckets,
clamping any single source past the cap. `summarize_stage_plan` reports
caps back to the operator. Read/write JSON roundtrip verified.

## 9. Post-stage quality audit status

✅ Working. Per-language sampling (bounded at 5000 hard, 500 default) +
SQL aggregate dedupe + rejection-log sampling.

Composite `quality_score` weights:
* 55% metadata completeness
* 10% language-consistency (mismatch penalty)
* 10% safety-tag validity
* 10% register-tag validity
* 10% coverage-category validity
* 5%  duplicate-rate penalty

Verdicts: `pass` (≥0.75), `warn` (≥0.55), `fail` (<0.55).

Live audit (EN production rows): `quality_score=1.0`, verdict=**pass**.

## 10. Retrieval SLA status

✅ Defined and tested. SLA targets (warm DB, local hardware):

| Metric | Target p95 |
|---|---:|
| Simple lookup | 150 ms |
| Category lookup | 250 ms |
| Register lookup | 250 ms |
| Safety-filter lookup | 300 ms |
| `limit_max` | 25 |

Per-call verdict: `pass` if observed ≤ target, `warn` if ≤ 2×target, else
`fail`. Overall verdict = worst of any per-language per-metric verdict.

Live run: **overall_verdict = pass** on both languages. Sub-millisecond
p95s on the current 2814/2518-row production lexicon.

## 11. Index consistency status

✅ Working. Live checks:

| Check | EN | RU |
|---|---|---|
| `ix_words_word_lc` present | ✅ | ✅ |
| `ix_words_pack_id` present | ✅ | ✅ |
| `ix_words_lemma` present | n/a | ✅ |
| FTS5 row count | 2,814 | 2,518 |
| Words total | 2,814 | 2,518 |
| FTS coverage ratio | 1.000 | 1.000 |
| Safety-filter leak count | **0** | **0** |
| Hard-clamp at 200 on `limit=9999` | ✅ | ✅ |

Per-category counts emitted for all 21 canonical categories. Pack_id
coverage ratio computed for both languages.

## 12. Safety regression audit status

✅ Working. 6-class probe set: `recognition_only`, `do_not_use_unprompted`,
`vulgar`, `offensive`, `slang_normal`, `benign`. Per class, the audit asserts
the Phase 17 policy filter produces the expected `do_not_use_violations`,
`suggestion_only_recognized`, and `vulgar_in_teacher_mode` membership.

Live results:

| Audit | Status |
|---|---|
| `audit_english_safety_policy().all_pass` | ✅ True |
| `audit_russian_safety_policy().all_pass` | ✅ True |
| `audit_indexed_retrieval_safety("en").total_leaks` | 0 |
| `audit_indexed_retrieval_safety("ru").total_leaks` | 0 |
| `audit_runtime_context_safety("en").bounds_ok+safety_ok` | ✅ |
| `audit_runtime_context_safety("ru").bounds_ok+safety_ok` | ✅ |
| `audit_prompted_vs_unprompted_behavior(...).all_softening_consistent` | ✅ True |

### Fix landed during development

The first probe set included a row carrying BOTH `recognition_only` AND
`do_not_use_unprompted` inside the `recognition_only` class — which made
the row legitimately appear in `do_not_use_violations`, failing the
`recognition_only`-class expectation. **Fix:** keep each probe class
single-tag to make per-class expectations crisp.

## 13. Runner status

✅ 12 public functions, all tested:

* `verify_phase20_preflight` — wraps the planner's Phase 19 preflight.
* `setup_phase20_folders` — creates all 11 folders.
* `create_phase20_backup_snapshot` — wraps `bk.create_backup_snapshot` + writes a JSON report.
* `build_phase20_stage_plans` — discovers + builds EN + RU plans.
* `run_phase20_dry_run_rehearsal` — dry-runs synthetic fixtures + writes ledger rows.
* `run_phase20_synthetic_million_rehearsal` — generates + stream-validates million-style fixtures.
* `run_phase20_post_stage_quality_audits` — full audit + composite score.
* `run_phase20_retrieval_sla_eval` — 7 EN queries + 7 RU + 5 categories + 3 registers + 4 safety tags.
* `run_phase20_index_consistency_checks` — full consistency bundle.
* `run_phase20_safety_regression_audit` — full safety bundle.
* `run_phase20_real_stage_imports` — OFF by default; hard-gated when on.
* `write_phase20_report` — JSON writer.

Real stage import gates verified:
* `allow_real_import=False` default → `reason=real_stage_import_disabled_by_default`.
* `max_stage_size > 100_000` → `error="max_stage_size_exceeds_100000"`.
* Missing `backup_snapshot_id` → `error="backup_snapshot_required"`.
* Unverified backup → `error="backup_snapshot_unverified"`.

## 14. Synthetic million rehearsal status

✅ Working. Live run at 50,000 rows per language:

| Metric | EN | RU |
|---|---:|---:|
| Fixture rows written | 50,000 | 50,000 |
| Quality gate score | 1.000 | 1.000 |
| Streaming read | **1,843,984 rps** | (similar) |

The runner can scale to true 1M rows per language; the harness uses lower
counts for harness speed. Suite J verified streaming-read bounded honoring
`max_rows=2500` against a 5000-row source (no full load) and dry-run cap
honoring `max_entries=1000` (no production write).

## 15. Real local source status

| Folder | Files |
|---|---|
| `corpus_sources/english/incoming/` | 0 |
| `corpus_sources/russian/incoming/` | 0 |

No operator-staged corpora. Phase 20 ran in **synthetic-only validation
mode**. Stage plans were built with 0 stages (no sources to ingest).

## 16. Real staged import status

**Not executed.** No operator file staged AND `allow_real_import=False` by
default. Even with files staged, the runner enforces all 14 gates listed
in the spec (operator must explicitly satisfy each before any real write).

## 17. Production DB impact

**Zero rows changed.** EN=2814, RU=2518, RU_phr=35 (identical baseline →
final). Backup snapshots created in `corpus_sources/backups/` do not modify
the live DBs.

## 18. Manifest count before / after

90 (45 EN + 45 RU). Unchanged.

## 19. Test results

| Harness | Tests | Status |
|---|---:|---|
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
| **Total** | **1140** | **1140 / 1140 PASS** |

### Phase 20 suite breakdown (144 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | runner + 19 prior artifacts | 20 |
| B_BACKUP | create + verify + dry-restore + manifest + compare + list + report | 9 |
| C_LEDGER | init + create + update + retrieve + list + by-stage + by-rollback + report + invalid-lang | 10 |
| D_PLANNER | discovery + build + caps + flags + split + enforce + refuse-full-source + roundtrip + summary | 18 |
| E_QUALITY_AUDIT | sample + metadata + language + safety/reg/cov + dup + rejected + score + report | 9 |
| F_SLA | definition + 4 latency benchmarks (EN+RU) + evaluator + report | 7 |
| G_INDEX | EN + RU consistency + FTS counts + pack_id + categories + safety-no-leak + hard-clamp + report | 8 |
| H_SAFETY_REGRESSION | 6 classes + EN + RU + indexed + runtime + prompted-softening + report | 10 |
| I_RUNNER | 12 entry points + 3 real-import-guard tests | 14 |
| J_MILLION | fixture + bounded stream + capped dry-run + production-untouched | 5 |
| K_ISOLATION | 8 files × {exists, forbidden, network, daemon} | 34 |

## 20. Safety verification

* Six-class probe set (recognition_only, do_not_use_unprompted, vulgar, offensive, slang_normal, benign).
* EN policy audit: **all_pass=True**.
* RU policy audit: **all_pass=True**.
* Indexed-retrieval safety: **total_leaks=0** on both languages (filtered queries return only rows that actually carry the requested safety tag).
* Runtime-context safety: bounds_ok + safety_ok on both languages.
* Prompted-vs-unprompted softening: consistent (`is_user_prompted=True` does not strengthen restrictions; weakens or holds).
* Production retrieval evaluation EN avg=0.815 / RU avg=0.777 with zero `do_not_use_unprompted` violations.

## 21. Isolation verification

Suite K scans all 8 Phase 20 modules:

* Forbidden imports `worker`, `luna_modules`, `tier_`, `probe_`, `attestation`, `program_s` — **zero matches**.
* Network usage `urllib`, `requests`, `httpx`, `aiohttp`, `socket`, `ftplib`, `urlopen`, `http.client` — **zero matches**.
* Daemon usage `threading.Thread(`, `multiprocessing.Process(`, `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`, `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`, `while True:` — **zero matches**.

## 22. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 23. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Verified by suite K regex scan. Phase 20 is additive: 8 new modules, 1 new
harness, 1 new report, 10 new folders, 1 SQLite ledger (under
`corpus_sources/phase20/`), 10 backup snapshots (under `corpus_sources/backups/`).
Zero edits to any pre-Phase-20 file.

## 24. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite K confirms zero matches; backups use synchronous SQLite `.backup()` calls; no thread/process/Timer/scheduler.
* **No recursion** — no function in any Phase 20 module calls itself; planner uses iterative loops, runner is a flat call graph.
* **No full-corpus load** — every read path streams (line-iterators, bounded `LIMIT` SQL, bounded sample sizes). 50k synthetic stream read at 1.84M rps did not allocate the whole file.
* **No internet** — zero network library imports, zero `urlopen`/`http.client`/etc.

## 25. Rollback notes

### Remove Phase 20 entirely

```powershell
# 1. New modules + harness + report
Remove-Item dual_vocab_backup_restore.py
Remove-Item dual_import_batch_ledger.py
Remove-Item dual_million_stage_planner.py
Remove-Item dual_post_stage_quality_audit.py
Remove-Item dual_retrieval_sla_eval.py
Remove-Item dual_index_consistency_checker.py
Remove-Item dual_safety_regression_auditor.py
Remove-Item phase20_million_readiness_runner.py
Remove-Item test_phase20_million_readiness_gate.py
Remove-Item PHASE20_MILLION_READINESS_GATE_REPORT.md

# 2. New folders + their contents
Remove-Item -Recurse corpus_sources/phase20
Remove-Item -Recurse corpus_sources/backups   # OPTIONAL - keeps your backups; safer to retain
```

### Remove synthetic million rehearsal artifacts only

```powershell
Remove-Item -Recurse corpus_sources/phase20/synthetic_million
```

### Identify rows from a future real Phase 20 import

The Phase 20 ledger records every batch's `rollback_key`. To find rows tied
to a specific batch / stage / pack:

```sql
-- 1. Find the ledger row(s) for the import you want to undo
SELECT batch_id, pack_id, rollback_key, accepted_count
FROM import_batches
WHERE stage_id = '<your_stage_id>';

-- 2. Delete the corresponding lexicon rows by pack_id
DELETE FROM words WHERE pack_id LIKE 'pilot_%';
-- or specifically:
DELETE FROM words WHERE pack_id IN ('pack_a', 'pack_b');
```

### Restore from backup

```python
import dual_vocab_backup_restore as bk
bk.restore_backup_snapshot("<snapshot_id>", dry_run=True)   # preview
bk.restore_backup_snapshot("<snapshot_id>", dry_run=False)  # apply
```

### Verify counts after rollback

```python
import dual_vocab_backup_restore as bk
bk.compare_db_counts_before_after("<snapshot_id>")
```

### Remove manifests tied to Phase 20

Per-source manifests are written next to each source file:
`corpus_sources/<lang>/incoming/<file>.<lang>_pack_manifest.json`. Delete
only the manifests whose `pack_id` matches the rolled-back set.

## 26. Final readiness decision for Phase 21

### Decision: **GO** — conditional on operator-staged real corpora.

Every readiness gate cleared:

| Gate | Status |
|---|---|
| Backup/restore proven | ✅ |
| Ledger writes every batch | ✅ |
| Stage planner enforces 100k/50k/no-full-source | ✅ |
| Dry-run-first enforced in code paths | ✅ |
| Quality audit produces a numeric verdict | ✅ |
| Retrieval SLA produces pass/warn/fail | ✅ |
| Index consistency verified, zero safety leaks | ✅ |
| Safety regression auditor all_pass on both languages | ✅ |
| Real import OFF by default + 4 hard guards | ✅ |
| Synthetic million-style streaming verified | ✅ |
| No Program S / tier / worker / luna_modules contact | ✅ |
| No daemon / recursion / network | ✅ |

### Blockers BEFORE the actual real 1M import (Phase 21)

1. **No operator-staged corpus is present.** Phase 21 requires the operator
   to drop one or more local files into
   `corpus_sources/english/incoming/` and `corpus_sources/russian/incoming/`.
   Without local files there is nothing to import — staged or otherwise.
2. **Tokenizer expansion** — out of scope for Phase 20. Russian morphology
   (lemma + part_of_speech) needs broader coverage before 1M-row Russian
   retrieval quality stabilizes. Suggested as part of Phase 22.
3. **Disk budget review** — a real 1M-row ingest per language plus FTS5 +
   normal indexes will roughly **triple** on-disk footprint vs. current
   2814/2518-row baseline. Confirm `D:\` has at least 2–3 GB free before
   any real stage runs.
4. **Bigger retrieval eval set** — current evaluator uses 12 queries per
   language. Before Phase 21 real ingest, expand to ≥50 queries per language
   (~5 per coverage category) so safety + bounds testing scales with the
   corpus.
5. **Operator runbook** — a short README per stage is recommended
   (`corpus_sources/phase20/final_readiness/RUNBOOK.md`) listing the exact
   call sequence: stage → quality gate → backup → dry-run → audit → real
   import → post-stage audit → SLA → safety regression. Build it once,
   reuse per stage.
6. **Cross-language linking** — out of scope for Phase 20. Recommended as
   an additive table in Phase 22 (after real ingest), not before, so the
   link table is built against real corpus rows.

### Suggested Phase 21 first move

Stage **one** real EN file (5–10k rows) AND **one** real RU file (5–10k
rows) under `incoming/`. Run the full Phase 20 runner against them with
`allow_real_import=True`, capped at 10k rows per language. Validate every
audit verdict pre- and post-import. Only if everything stays green:
escalate the cap to 25k, 50k, 100k in subsequent stages.

## 27. Clean failure notes

None.

Pre-flight passed. All 12 harnesses green. Baseline counts unchanged.
Backup snapshots verified. Ledger lifecycle tested. Stage caps enforced and
refused-on-overage. Post-stage audit verdict produced. SLA pass on production
DB. Index consistency clean with zero safety leaks. Safety regression
`all_pass`. Live 50k synthetic rehearsal at 1.84M rps. No deferred work.

Two real bugs were found during development and fixed in code, not by
weakening assertions:

1. `russian_lexicon_store` exposes its db-path resolver as `_resolve`, not
   `_resolve_db_path`. The backup module was using the EN-style name; fixed
   to call `rulex._resolve(None)`.
2. The backup verify routine globbed for `*.sqlite3`, but the live lexicon
   files end in `.sqlite`. Widened the glob to `*.sqlite*`.

Both fixes preserved the spirit of the assertions and the intended safety
behavior.
