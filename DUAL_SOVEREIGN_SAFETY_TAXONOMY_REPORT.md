# Dual Sovereign Knowledge + Vocabulary Stack — Phase 12 Report

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flags (unchanged):** `LUNA_VOCABULARY_RUNTIME`, `LUNA_RUSSIAN_STACK`
**Status:** Complete — 226 / 226 tests pass across all three harnesses.

---

## Phase Summary

Phase 12 — **Dual Sovereign Safety, Taxonomy, and Pack Manifest Upgrade** — retrofits the existing English (Track A) and Russian (Track B) stacks with: a shared coverage taxonomy + register/safety vocabulary, additive DB migrations for `register_tags / safety_tags / coverage_categories / pack_source / pack_id`, safety-aware policy enforcement with explicit decision contexts and `is_user_prompted`, a shared pack-manifest schema (with streaming SHA256), and an English ingestion peer mirroring the Russian ingestion pattern.

All changes are additive. No destructive migrations. No daemons. No recursion. No integration with Luna's main runtime. No edits to Program S, tier, probe, attestation, or worker orchestration files.

| Step | Deliverable | Status |
|------|-------------|--------|
| 1 | `coverage_taxonomy.py` — 21 coverage categories, 22 register tags, 4 safety tags, + alias maps + validators | ✅ |
| 2 | Additive migrations on both `words` tables + Russian `phrases` table | ✅ |
| 3 | Safety-aware policy: `is_word_allowed(..., safety_tags, register_tags, is_user_prompted, decision_context)`; Russian `is_entry_allowed_ru(...)` mirror | ✅ |
| 4 | `pack_manifest.py` — schema + `compute_sha256` (streaming) + `create_pack_manifest` + `validate_pack_manifest` + `write_pack_manifest` + `read_pack_manifest` | ✅ |
| 5 | `english_knowledge_ingestion.py` — `ingest_word_list`, `ingest_phrase_list`, `ingest_topic_pack`, `preview_ingestion`, `validate_english_entry`, `write_ingestion_report`, `emit_pack_manifest` | ✅ |
| 6 | `test_dual_sovereign_pack_safety.py` — 79 checks across 10 suites | ✅ |
| 7 | This report | ✅ |

## Files Created

| Path | Role |
|------|------|
| `coverage_taxonomy.py` | Shared canonical taxonomy + validators (used by EN and RU tracks). |
| `pack_manifest.py` | Shared manifest schema; streaming SHA256; never loads full file. |
| `english_knowledge_ingestion.py` | English ingestion peer to `russian_knowledge_ingestion`. Emits ingestion report + manifest. |
| `test_dual_sovereign_pack_safety.py` | 79-check bounded harness for Phase 12. |
| `DUAL_SOVEREIGN_SAFETY_TAXONOMY_REPORT.md` | This report. |

## Files Modified (Additive Only)

| Path | Change |
|------|--------|
| `cognitive_lexicon_store.py` | Added `_apply_migrations()` + 5 new columns + extended `_row_to_dict` + extended `add_word(...)` to accept `register_tags / safety_tags / coverage_categories / pack_source / pack_id`. |
| `cognitive_word_policy.py` | Added `DECISION_CONTEXTS`, `_norm_set`, extended `is_word_allowed(...)` with safety / register / is_user_prompted / decision_context; threaded those through `filter_words` and `apply_policy`. |
| `cognitive_vocabulary_runtime.py` | `get_context_words(...)` and `get_optional_vocabulary_context(...)` now accept and forward `is_user_prompted`. |
| `russian_lexicon_store.py` | Added `_apply_migrations()` for both `words` and `phrases` + 5 new columns each + extended row hydrators + extended `add_word`/`add_phrase` signatures. |
| `russian_personality_layer.py` | Added `DECISION_CONTEXTS`, `_norm_set`, `is_entry_allowed_ru(...)`, `filter_russian_entries(...)`. |

**No other files were modified.** `git status`:

```
?? DUAL_SOVEREIGN_SAFETY_TAXONOMY_REPORT.md
?? coverage_taxonomy.py
?? english_knowledge_ingestion.py
?? pack_manifest.py
?? test_dual_sovereign_pack_safety.py
M  cognitive_lexicon_store.py
M  cognitive_vocabulary_runtime.py
M  cognitive_word_policy.py
M  russian_lexicon_store.py
M  russian_personality_layer.py
```

## Database Migrations Performed

All migrations are additive (`ALTER TABLE … ADD COLUMN …`). No data loss. Idempotent — re-running `init_db` is a no-op once columns exist.

### English (`lexicon/luna_vocabulary.sqlite`)

| Table | Before | After | New columns |
|-------|-------:|------:|-------------|
| `words` | 11 cols | 16 cols | `register_tags_json`, `safety_tags_json`, `coverage_categories_json`, `pack_source`, `pack_id` |

Pre-existing 246 seeded rows preserved (verified post-migration: `count_words() == 246`).

### Russian (`russian_stack/russian_lexicon.sqlite`)

| Table | Before | After | New columns |
|-------|-------:|------:|-------------|
| `words` | 17 cols | 22 cols | `register_tags_json`, `safety_tags_json`, `coverage_categories_json`, `pack_source`, `pack_id` |
| `phrases` | 11 cols | 16 cols | (same 5 columns) |

Tables are empty (no seeded rows yet); migration safe.

## English Status (Track A)

- Store has structured `register_tags`, `safety_tags`, `coverage_categories`, `pack_source`, `pack_id`.
- Policy enforces all four safety classes (recognition_only, do_not_use_unprompted, vulgar, offensive) plus slang/street gating.
- Runtime entry-point `get_optional_vocabulary_context(...)` now accepts `is_user_prompted: bool = False`.
- Ingestion peer (`english_knowledge_ingestion.py`) exists with chunked streaming, bounded preview, batch clamp (max 5000), per-row validation, ingestion report + pack manifest emission.
- Existing seed (246 rows) preserved.
- Existing test harness (`test_vocabulary_runtime.py`) — **74 / 74 pass** with no changes required.

## Russian Status (Track B)

- Store has structured `register_tags`, `safety_tags`, `coverage_categories`, `pack_source`, `pack_id` on both `words` and `phrases` tables.
- Policy (`russian_personality_layer.is_entry_allowed_ru`) enforces all four safety classes + slang/street register gating, with the same four decision contexts as English.
- `filter_russian_entries(...)` lets the existing Russian ingestion path filter rows by policy before suggestion.
- Existing Russian ingestion (`russian_knowledge_ingestion.py`) keeps working unchanged — it can pass the new fields through to `add_word` / `add_phrase` via `**kwargs` when a pack provides them.
- Existing Russian test harness (`test_russian_sovereign_stack.py`) — **73 / 73 pass** with no changes required.

## Policy Enforcement Changes

`cognitive_word_policy.is_word_allowed` and `russian_personality_layer.is_entry_allowed_ru` both now take:

- `safety_tags: Iterable[str] | None`
- `register_tags: Iterable[str] | None`
- `is_user_prompted: bool = False`
- `decision_context: str = "suggestion"` ∈ `{recognition, explanation, suggestion, response_wording}`

Rules (identical English-side and Russian-side):

| Tag / context | Behavior |
|---------------|----------|
| `decision_context="recognition"` | Always allowed (Luna may know it). |
| `recognition_only` + `decision_context="explanation"` | Allowed (Luna may explain it to user). |
| `recognition_only` + `suggestion` / `response_wording` | **Blocked** regardless of prompt. |
| `do_not_use_unprompted` + `is_user_prompted=False` | **Blocked.** |
| `do_not_use_unprompted` + `is_user_prompted=True` | Allowed. |
| `vulgar` / `offensive` in normal / teacher / curriculum / professional / voice modes | **Blocked** (even when prompted). |
| `vulgar` / `offensive` in informal modes + `is_user_prompted=True` | Allowed (EN — only voice modes are also blocked; RU — `_STRICT_MODES_RU` blocks). |
| `slang` / `street` in normal / teacher / professional / technical / coding modes | **Blocked** unless `is_user_prompted=True` OR mode is informal-class. |

The runtime call `get_optional_vocabulary_context("…fuck off", mode="normal")` now drops `fuck` from the suggestion list — verified live by test [8] of the Phase 12 harness.

## Manifest Status

Shared `pack_manifest.py`:

- 13 required fields, 3 optional fields (per spec).
- Languages restricted to `{"en", "ru"}`; non-canonical language raises `ValueError`.
- Coverage / register / safety values pass through `coverage_taxonomy` validators — unknown values are dropped (recorded in `_taxonomy_rejected` for audit).
- `compute_sha256` streams the source file in 64 KB chunks — never loads the file into memory.
- `validate_pack_manifest` reports missing required fields and type-invalid fields.
- `english_knowledge_ingestion` emits one manifest + one ingestion report per pack. Russian path is unchanged (already emits ingestion report); a wrapper can call `pack_manifest.create_pack_manifest` from the existing Russian ingestion when desired.

## English Ingestion Status

`english_knowledge_ingestion.py`:

- Formats: `.jsonl`, `.json` (top-level list or dict), `.csv` (DictReader), `.txt` (one word per line).
- Per-row validation rejects: non-dict rows, missing word/lemma, non-Latin characters.
- Per-row taxonomy normalization for register/safety/coverage values (drops unknowns silently — captured in rejection sample list).
- Three public ingest functions: `ingest_word_list`, `ingest_phrase_list`, `ingest_topic_pack`.
- `preview_ingestion(path, limit=25)` returns counts + samples without modifying the DB.
- Batch size clamped to `[1, 5000]` (default 500).
- Every successful import writes both `<path>.en_ingest_report.json` and `<path>.en_pack_manifest.json`.
- Aggregates coverage_categories / register_tags / safety_tags / domain_tags across the pack so the emitted manifest reflects actual content.
- Deduplicates within a single ingestion run via `seen_words` set.
- Missing source file → `{error: "file_not_found"}`, no exception.

## Test Results

```
$ python test_dual_sovereign_pack_safety.py
SUMMARY: 79/79 passed, 0 failed

$ python test_vocabulary_runtime.py
SUMMARY: 74/74 passed, 0 failed

$ python test_russian_sovereign_stack.py
SUMMARY: 73/73 passed, 0 failed
```

**Total: 226 / 226 checks pass across three harnesses.**

Phase 12 harness breakdown (10 suites):

| # | Suite | Checks | Result |
|---|-------|-------:|--------|
| 1 | Coverage taxonomy: validators + aliases + rejection | 8 | PASS |
| 2 | English store additive migration (5 new cols; existing rows preserved; idempotent) | 12 | PASS |
| 3 | Russian store additive migration (5 new cols on words + phrases) | 15 | PASS |
| 4 | English policy: 4 contexts, recognition_only, do_not_use_unprompted, vulgar in strict modes, slang gating | 11 | PASS |
| 5 | Russian policy: same suite mirrored for `is_entry_allowed_ru` + `filter_russian_entries` | 8 | PASS |
| 6 | Pack manifest: streaming SHA256, validator, write/read roundtrip, language enforcement | 11 | PASS |
| 7 | English ingestion: preview, batch ingest, manifest, vulgar persistence, missing-file safety | 9 | PASS |
| 8 | Runtime integration: vulgar NOT auto-suggested in normal mode | 2 | PASS |
| 9 | No new threads across 5 calls; no recursion blow-up at `setrecursionlimit(300)` | 2 | PASS |
| 10 | No forbidden Program S / tier / probe imports in production modules | 1 | PASS |

## Program S — Not Touched

- Test [10] of the new harness scans every production module for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression)` → **0 hits**.
- `git status` shows only new + modified files inside the Dual Sovereign track.
- No file outside the Dual Sovereign file set was opened with Edit/Write.
- No service, scheduled task, daemon, registry key, WinSW config, or cron entry was created or modified.

## Tier / Probe / Attestation Files — Not Touched

- Same import-regex scan in test [10] covers `tier_intent_library`, `luna_tier_*`, `tier_progression`, `probe_health`, and (via `luna_modules`) attestation surfaces — all 0 hits.
- The string "attestation" appears only inside Russian module docstrings declaring what the module does **not** touch — never as an import or call site.

## Rollback Steps

The migrations are additive (new columns with safe defaults). Rolling back is straightforward:

1. **Soft rollback of policy:** the new safety gates only fire when a row has `safety_tags` / `register_tags` populated. Rows added before Phase 12 have empty lists → unaffected.
2. **Revert source files** (in order, none has external deps on the others):
   - `git checkout -- cognitive_lexicon_store.py cognitive_word_policy.py cognitive_vocabulary_runtime.py russian_lexicon_store.py russian_personality_layer.py`
3. **Delete the new files:**
   ```
   del coverage_taxonomy.py pack_manifest.py english_knowledge_ingestion.py
   del test_dual_sovereign_pack_safety.py DUAL_SOVEREIGN_SAFETY_TAXONOMY_REPORT.md
   ```
4. **Drop the new DB columns (optional):** SQLite ≥ 3.35 supports `ALTER TABLE … DROP COLUMN`. Otherwise the unused columns are harmless (default `'[]'` / `''`). Recommended: leave them in place.
5. No service, scheduled task, daemon, registry key, or external system was touched, so nothing else needs to be reverted.

## Next Recommended Phase

**Phase 13 — Initial Pack Bootstrapping** (optional; deferred until operator decides):

1. Author or download one small high-quality pack per coverage category (e.g. 200–500 rows each for `slang_street_talk`, `idioms_phrases`, `trades_construction`, `medicine_health`, `philosophy_abstract`). Total target: 5K–10K rows per track to validate the manifest + filter pipeline at realistic scale.
2. Run `english_knowledge_ingestion.preview_ingestion(...)` and `russian_knowledge_ingestion.preview_ingestion(...)` before any full ingest.
3. Run full ingest with `batch_size=500`; inspect emitted manifests (`<path>.en_pack_manifest.json` / `<path>.ingest_report.json`).
4. Run all three test harnesses after each pack; if any check fails, treat the pack as suspect and rollback by `DELETE FROM words WHERE pack_id = '<pid>'` (and same for `phrases`).
5. **Do not** wire either runtime into Luna's main prompt builder yet — that's a separate review.
6. **Performance budget reminder:** add an FTS5 virtual table over `(word, lemma, definition_*)` once either DB crosses ~50K rows.

---

## Final Confirmations

- Both English and Russian stacks have structured `safety_tags`.
- Both stacks have canonical register tagging (`coverage_taxonomy.REGISTER_TAGS`, 22 values).
- Both stacks share the coverage taxonomy (`coverage_taxonomy.COVERAGE_CATEGORIES`, 21 values).
- Both stacks can support profession, poetry, philosophy, slang, street talk, idiom, and domain packs via their respective ingestion paths.
- Unsafe / recognition-only terms are not casually suggested — verified live (test [8]: prompt containing "fuck off" in normal mode does not surface "fuck" in suggestions).
- English has a real ingestion peer (`english_knowledge_ingestion.py`).
- All future packs (English and Russian) can emit manifests via `pack_manifest.create_pack_manifest`.
- **No Program S files touched.**
- **No tier / probe / attestation files touched.**
- **No daemon created.**
- **No recursion.**
- Existing tests remain passing (English 74/74, Russian 73/73, Phase 12 79/79 — total 226/226).
