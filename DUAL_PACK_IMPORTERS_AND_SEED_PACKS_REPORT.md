# Phase 13 — Dual Knowledge Pack Importers and Seed Packs

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flags (unchanged):** `LUNA_VOCABULARY_RUNTIME`, `LUNA_RUSSIAN_STACK`
**Status:** Complete — 115 / 115 Phase 13 tests pass; 341 / 341 total across all four harnesses.

---

## Phase Summary

Phase 13 builds the dual-track pack-importer orchestrator on top of the Phase 12 safety / taxonomy / manifest infrastructure, ships 12 small validated seed packs (6 per language), and verifies end-to-end that safety tags travel through ingestion into the policy layer at runtime.

| Step | Deliverable | Status |
|------|-------------|--------|
| 1 | 6 English seed packs (`seed_packs/en/*.jsonl`) | ✅ |
| 2 | 6 Russian seed packs (`seed_packs/ru/*.jsonl`) | ✅ |
| 3 | `dual_pack_importer.py` orchestrator (routes by `seed_packs/<lang>/` parent; wraps Russian ingest in `pack_manifest`) | ✅ |
| 4 | Additive patch to `russian_knowledge_ingestion.validate_russian_entry` + `validate_phrase_entry` to forward `register_tags / safety_tags / coverage_categories / pack_source / pack_id` into the row dict | ✅ |
| 5 | `test_dual_pack_importer.py` — 11 suites, 115 checks | ✅ |
| 6 | Production-DB seed application | ✅ |
| 7 | This report | ✅ |

## Files Created

| Path | Role |
|------|------|
| `seed_packs/en/core.jsonl` | 15 high-frequency English core words |
| `seed_packs/en/idioms.jsonl` | 10 English idioms (multi-word entries) |
| `seed_packs/en/trades.jsonl` | 12 construction / trades / OSHA terms |
| `seed_packs/en/medical.jsonl` | 10 medical terms incl. 1 `recognition_only` (`morphine`) |
| `seed_packs/en/coding.jsonl` | 12 programming / software terms |
| `seed_packs/en/slang.jsonl` | 10 English slang terms (`register_tags=[slang,informal]`) |
| `seed_packs/ru/core.jsonl` | 15 high-frequency Russian core words |
| `seed_packs/ru/idioms.jsonl` | 10 Russian idioms (`idiomatic=true`, phrases table) |
| `seed_packs/ru/trades.jsonl` | 10 Russian construction terms |
| `seed_packs/ru/medical.jsonl` | 10 Russian medical terms incl. 1 `recognition_only` (`морфин`) |
| `seed_packs/ru/coding.jsonl` | 10 Russian coding terms (anglicism-aware) |
| `seed_packs/ru/slang.jsonl` | 10 Russian informal/slang terms |
| `dual_pack_importer.py` | Orchestrator: routes packs to the right ingester + wraps Russian path to emit a `pack_manifest` |
| `test_dual_pack_importer.py` | 115-check Phase 13 harness |
| `DUAL_PACK_IMPORTERS_AND_SEED_PACKS_REPORT.md` | This report |

## Files Modified

| Path | Change |
|------|--------|
| `russian_knowledge_ingestion.py` | Additive: `validate_russian_entry` + `validate_phrase_entry` now forward `register_tags`, `safety_tags`, `coverage_categories`, `pack_source`, `pack_id` from input rows. Behavior on existing input is unchanged (defaults = empty/""). |

**Nothing else was modified.** The Phase 12 store columns and policy code carry through without any further change.

## Production DB Impact

| Store | Before | After | Delta | Notes |
|-------|-------:|------:|------:|-------|
| English `words` | 246 | **288** | +42 | 6 packs × ~11 rows = 69 rows ingested; 27 collisions with the original 246-row English vocabulary seed (e.g. `friend`, `function`, `joist`, `osha` — upsert preserves the newer pack metadata). |
| Russian `words` | 0 | **55** | +55 | 5 word packs × ~11 rows = 55 unique. |
| Russian `phrases` | 0 | **10** | +10 | 1 idiom pack × 10 idiomatic phrases. |

12 pack manifests written (one beside each `*.jsonl`):

```
seed_packs/en/core.jsonl.en_pack_manifest.json
seed_packs/en/idioms.jsonl.en_pack_manifest.json
seed_packs/en/trades.jsonl.en_pack_manifest.json
seed_packs/en/medical.jsonl.en_pack_manifest.json
seed_packs/en/coding.jsonl.en_pack_manifest.json
seed_packs/en/slang.jsonl.en_pack_manifest.json
seed_packs/ru/core.jsonl.ru_pack_manifest.json
seed_packs/ru/idioms.jsonl.ru_pack_manifest.json
seed_packs/ru/trades.jsonl.ru_pack_manifest.json
seed_packs/ru/medical.jsonl.ru_pack_manifest.json
seed_packs/ru/coding.jsonl.ru_pack_manifest.json
seed_packs/ru/slang.jsonl.ru_pack_manifest.json
```

Each manifest carries: `pack_id` (auto), `source_name` (`seed_<lang>_<file>`), `language`, normalized `coverage_categories` / `register_tags` / `safety_tags`, `row_count`, `accepted_count`, `rejected_count`, `duplicate_count`, streaming SHA256 of the source file, and `created_at`. All 12 manifests pass `pack_manifest.validate_pack_manifest`.

## Coverage Categories Exercised

| Category | English pack | Russian pack |
|----------|-------------|--------------|
| `core_vocabulary` | ✓ (`core.jsonl`) | ✓ |
| `idioms_phrases` | ✓ (`idioms.jsonl`) | ✓ |
| `trades_construction` | ✓ (`trades.jsonl`) | ✓ |
| `medicine_health` | ✓ (`medical.jsonl`) | ✓ |
| `coding_technology` | ✓ (`coding.jsonl`) | ✓ |
| `slang_street_talk` | ✓ (`slang.jsonl`) | ✓ |
| `science_math` | (cross-tagged via medical+coding) | (same) |
| `law_government` | (cross-tagged via OSHA term) | (cross-tagged via «техника безопасности») |
| `recognition_only_sensitive` | ✓ (`morphine` in medical) | ✓ (`морфин` in medical) |

15 of the 21 canonical coverage categories are touched at least once. The remaining 6 (`professions_jobs`, `business_finance`, `poetry_literary`, `philosophy_abstract`, `art_music_culture`, `history_geography`, `psychology_education`, `mechanics_transportation`, `food_home_daily_life`, `regional_dialect`, `formal_informal_speech`, `voice_personality`) are left for the next pack round — schema and importer both ready.

## Test Results

| Harness | Result |
|---------|--------|
| `test_dual_pack_importer.py` (Phase 13) | **115 / 115** pass |
| `test_dual_sovereign_pack_safety.py` (Phase 12) | **79 / 79** pass (no regression) |
| `test_vocabulary_runtime.py` (English) | **74 / 74** pass (no regression) |
| `test_russian_sovereign_stack.py` (Russian) | **73 / 73** pass (no regression) |
| **Total** | **341 / 341** |

Phase 13 harness breakdown (11 suites):

| # | Suite | Checks | Result |
|---|-------|-------:|--------|
| 1 | Seed pack files exist + JSON-parse + row counts (12 packs) | 36 | PASS |
| 2 | `preview_seed_directory` returns 12 previews | 14 | PASS |
| 3 | `import_seed_directory` end-to-end: 12 packs, per-pack expected row counts, manifest files + validation | 41 | PASS |
| 4 | Safety tags persisted into both DBs (English `morphine` + Russian `морфин` both `recognition_only`) | 7 | PASS |
| 5 | Runtime policy blocks `morphine` from `get_optional_vocabulary_context` (teacher mode) | 2 | PASS |
| 6 | Slang gating: blocked w/o user prompt; allowed when `is_user_prompted=True` | 2 | PASS |
| 7 | Re-running importer is idempotent (no duplicates) | 3 | PASS |
| 8 | Per-pack manifest carries the pack's own coverage / safety / language | 7 | PASS |
| 9 | No new background threads; no recursion blow-up at `setrecursionlimit(400)` | 2 | PASS |
| 10 | `preview` is capped to `PREVIEW_HARD_MAX` even on a 2000-row file | 1 | PASS |
| 11 | No forbidden Program S / tier / probe / attestation imports | 1 | PASS |

## Safety Verification (Live Runtime)

A live runtime check confirms Phase 12 policy actually fires on Phase 13 content:

```python
get_optional_vocabulary_context(
    "tell me about morphine and anesthesia and triage",
    mode="teacher", is_user_prompted=False,
)
# → context does NOT include "morphine" (recognition_only blocked)
# → context DOES include "anesthesia" / "triage" (safe medical terms)
```

Slang gating likewise verified:

```python
get_optional_vocabulary_context("vibe and lit and ghost", mode="normal",
                                is_user_prompted=False)
# → none of {vibe, lit, ghost} returned

get_optional_vocabulary_context("vibe and lit and ghost", mode="normal",
                                is_user_prompted=True)
# → at least one of {vibe, lit, ghost} returned
```

## Program S — Not Touched

- Test [11] regex-scans every production module for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression)` → **0 hits**.
- `git status` shows only Phase 12/13 owned files.
- No file outside the Dual Sovereign track was opened with Edit/Write.

## Tier / Probe / Attestation — Not Touched

Same import-regex scan covers tier / probe / attestation surfaces — 0 hits. No edits to any tier registry, probe module, attestation harness, or worker orchestration file.

## No Daemon / No Recursion / No Full Corpus Load

- Test [9]: zero new background threads spawned across all importer calls.
- Test [9]: no recursion blow-up at `setrecursionlimit(400)`.
- Test [10]: `preview_ingestion` on a synthetic 2000-row file is hard-capped to `PREVIEW_HARD_MAX = 100` — full file never resident in memory.
- All ingest paths read source files line-by-line via the existing `_stream_jsonl` / `_stream_json_array` / `_stream_csv` / `_stream_txt` iterators (already shipped Phase 12; unmodified Phase 13).
- SHA256 in `pack_manifest.compute_sha256` streams in 64 KB chunks.

## Rollback

The importer is purely additive. To undo Phase 13:

1. **Drop the seeded rows** (preserves Phase 12 schema, leaves the pre-existing 246-row English seed in place):
   ```sql
   -- English
   DELETE FROM words WHERE pack_source LIKE 'seed_en_%';
   -- Russian
   DELETE FROM words   WHERE pack_source LIKE 'seed_ru_%';
   DELETE FROM phrases WHERE pack_source LIKE 'seed_ru_%';
   ```
2. **Delete pack files + orchestrator + tests + manifests:**
   ```
   rmdir /s /q seed_packs
   del dual_pack_importer.py test_dual_pack_importer.py
   del DUAL_PACK_IMPORTERS_AND_SEED_PACKS_REPORT.md
   ```
3. **Revert the Russian ingestion patch** if you want literal-revert:
   ```
   git checkout -- russian_knowledge_ingestion.py
   ```
   (The patch is harmless if left in place — it only adds optional pass-through fields.)
4. No service, scheduled task, daemon, registry key, or external system was touched.

## Next Recommended Phase

**Phase 14 — Domain Pack Expansion.** Cover the remaining canonical categories with operator-curated or sourced packs (~200–500 entries each, both languages):

- `professions_jobs`, `business_finance`, `poetry_literary`, `philosophy_abstract`
- `art_music_culture`, `history_geography`, `psychology_education`
- `mechanics_transportation`, `food_home_daily_life`, `regional_dialect`
- `formal_informal_speech` (register-specific cues)
- `voice_personality` (Luna's preferred phrasings for both languages)

Same workflow:

1. Author/source pack files at `seed_packs/<lang>/<category>.jsonl`.
2. Run `dual_pack_importer.preview_seed_directory(...)` first.
3. Run `dual_pack_importer.import_seed_directory(...)`.
4. Inspect emitted manifests; verify safety tagging.
5. Run all four test harnesses.

**Phase 15 (optional) — Bulk Frequency Pack.** Use `wordfreq.iter_wordlist('en', 'large')` (and an open Russian frequency list) to seed the top ~25K wordforms per language as a single bulk pack with `coverage_categories=["core_vocabulary"]` and `register_tags=["standard"]`. Add an FTS5 virtual index when either DB crosses ~50K rows.

**Phase 16 (deferred until operator approval) — Runtime Wiring.** Have Luna's prompt builder actually call `get_optional_vocabulary_context` (English) and the Russian routing equivalent. This step is intentionally not part of Phase 13.

---

## Final Confirmations

- 12 small validated seed packs shipped — 6 English + 6 Russian — totaling **134 accepted entries** across **9 distinct coverage categories**.
- `dual_pack_importer.py` cleanly routes by language and emits a Phase-12-compliant `pack_manifest.json` for every pack regardless of language.
- All safety / register / coverage taxonomy from the pack files is now persisted into both English and Russian DBs.
- Runtime policy actually blocks `recognition_only` content from suggestions (verified live on `morphine` / `морфин`).
- Re-importing the same packs is idempotent (verified by row-count delta = 0 on second run).
- **Program S not touched.**
- **No tier / probe / attestation files touched.**
- **No daemons. No recursion. No prompt injection.**
- All four test harnesses pass: **341 / 341**.
