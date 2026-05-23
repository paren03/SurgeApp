# Phase 14 — Domain Pack Expansion Report

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flags (unchanged):** `LUNA_VOCABULARY_RUNTIME`, `LUNA_RUSSIAN_STACK`
**Status:** Complete — 429 / 429 tests pass across all 5 harnesses.

---

## Phase 14 Summary

Phase 14 expands the Dual Sovereign Knowledge + Vocabulary Stack across the 14 remaining canonical coverage categories for both English (Track A) and Russian (Track B). 28 new validated seed packs were authored (~30 entries per pack average), the orchestrator was extended to route `voice_personality` packs through the phrase ingestion path, and three small additive patches landed (Russian `pack_source` fallback, English/Russian policy gating `regional` register, Phase-13 harness filters made forward-compatible). All taxonomy validates, all 40 packs emit manifests, all safety classes (`recognition_only`, `do_not_use_unprompted`, `vulgar`, `offensive`, `slang`, `street`, `regional`) gate correctly through the runtime.

## Files Created (Phase 14)

| Path | Role |
|------|------|
| `seed_packs/en/professions_jobs.jsonl` | 49 English profession terms |
| `seed_packs/en/business_finance.jsonl` | 36 English business/finance terms |
| `seed_packs/en/law_government.jsonl` | 35 English legal/government terms |
| `seed_packs/en/science_math.jsonl` | 35 English science/math terms |
| `seed_packs/en/poetry_literary.jsonl` | 32 English poetry/literary terms |
| `seed_packs/en/philosophy_abstract.jsonl` | 30 English philosophy terms |
| `seed_packs/en/art_music_culture.jsonl` | 34 English art/music/culture terms |
| `seed_packs/en/history_geography.jsonl` | 32 English history/geography terms |
| `seed_packs/en/psychology_education.jsonl` | 32 English psychology/education terms |
| `seed_packs/en/mechanics_transportation.jsonl` | 35 English mechanics/transport terms |
| `seed_packs/en/food_home_daily_life.jsonl` | 38 English food/home terms |
| `seed_packs/en/regional_dialect.jsonl` | 30 English regional/dialect terms |
| `seed_packs/en/formal_informal_speech.jsonl` | 31 English formal/informal pairs |
| `seed_packs/en/voice_personality.jsonl` | 24 English Luna-voice phrases |
| `seed_packs/ru/professions_jobs.jsonl` | 39 Russian profession terms |
| `seed_packs/ru/business_finance.jsonl` | 32 Russian business/finance terms |
| `seed_packs/ru/law_government.jsonl` | 31 Russian legal/government terms |
| `seed_packs/ru/science_math.jsonl` | 33 Russian science/math terms |
| `seed_packs/ru/poetry_literary.jsonl` | 31 Russian poetry/literary terms |
| `seed_packs/ru/philosophy_abstract.jsonl` | 30 Russian philosophy terms |
| `seed_packs/ru/art_music_culture.jsonl` | 31 Russian art/music/culture terms |
| `seed_packs/ru/history_geography.jsonl` | 31 Russian history/geography terms |
| `seed_packs/ru/psychology_education.jsonl` | 30 Russian psychology/education terms |
| `seed_packs/ru/mechanics_transportation.jsonl` | 31 Russian mechanics/transport terms |
| `seed_packs/ru/food_home_daily_life.jsonl` | 35 Russian food/home terms |
| `seed_packs/ru/regional_dialect.jsonl` | 25 Russian regional/dialect terms |
| `seed_packs/ru/formal_informal_speech.jsonl` | 30 Russian formal/informal pairs |
| `seed_packs/ru/voice_personality.jsonl` | 25 Russian Luna-voice phrases |
| `test_phase14_domain_pack_expansion.py` | 87-check harness, 12 suites |
| `PHASE14_DOMAIN_PACK_EXPANSION_REPORT.md` | This report |

## Files Modified (additive only)

| Path | Change |
|------|--------|
| `dual_pack_importer.py` | Added `voice_personality.jsonl` to `_PHRASE_FILENAMES` so it routes through phrase ingest. |
| `russian_knowledge_ingestion.py` | `_ingest` now falls back `pack_source` to the `source` argument when the entry omits it (matches the English peer behaviour). Additive — empty entries already get empty pack_source by default. |
| `cognitive_word_policy.py` | `is_word_allowed` slang gate now also covers `regional` register tag, matching the Phase 14 spec. |
| `russian_personality_layer.py` | Same `regional` gate added to `is_entry_allowed_ru`. |
| `test_dual_pack_importer.py` | Phase-13 harness filtered its assertions to Phase-13 packs only (was previously asserting exact totals against the seed_packs/ directory — now forward-compatible with Phase 14+ packs). |

## Packs Added

| Tier | Coverage categories newly seeded | Total packs |
|------|-----------------------------------|------------:|
| Phase 14 — English | 14 (professions, business, law, science, poetry, philosophy, art, history, psychology, mechanics, food/home, regional, formal/informal, voice) | 14 |
| Phase 14 — Russian | Same 14 categories mirrored | 14 |
| **Phase 14 total** | | **28** |
| Cumulative seed_packs/ (Phase 13 + Phase 14) | 20 distinct coverage categories exercised | 40 |

Phase 14 entries (sum of per-pack call counts before dedup): **English ~483, Russian ~434**. Production unique-row delta: **English +453, Russian +404 words + 25 phrases**.

## English Entries Added (production DB)

- **Before Phase 14:** 288 unique English `words` rows (246 from original seed + 42 net from Phase 13).
- **After Phase 14:** **741** unique English `words` rows.
- **Net delta:** **+453** unique rows (with 30 cross-pack / cross-category upsert collisions to the Phase-14 + Phase-13 overlap — e.g. `function`, `recursion`, `engineer`, `feedback` appearing in multiple packs all collapse to one canonical row carrying the last-write pack metadata).

## Russian Words / Phrases Added (production DB)

| Table | Before Phase 14 | After Phase 14 | Delta |
|-------|----------------:|---------------:|------:|
| `russian_stack.words` | 55 | **459** | +404 |
| `russian_stack.phrases` | 10 | **35** | +25 |

(Phrase delta = 10 idioms from Russian `idioms.jsonl` + 25 Luna-voice phrases from `voice_personality.jsonl`.)

## Manifests Written

| Source | Manifests |
|--------|----------:|
| English packs (Phase 13 + Phase 14) | 20 |
| Russian packs (Phase 13 + Phase 14) | 20 |
| **Total** | **40** |

Every manifest carries: `pack_id`, `source_name` (`seed_<lang>_<file>`), `language`, normalized `coverage_categories` / `register_tags` / `safety_tags`, `row_count`, `accepted_count`, `rejected_count`, `duplicate_count`, **streaming SHA256** of the source file, `created_at`, optional `import_report_path` and `notes`. All 40 pass `pack_manifest.validate_pack_manifest`.

## DB Counts Before / After

| Store | Before Phase 14 | After Phase 14 |
|-------|----------------:|---------------:|
| English `cognitive.words` | 288 | **741** |
| Russian `russian_stack.words` | 55 | **459** |
| Russian `russian_stack.phrases` | 10 | **35** |
| Total live rows across both tracks | 353 | **1235** |

## Safety Verification

Live runtime + policy checks (all from `test_phase14_domain_pack_expansion.py`):

| Check | Outcome |
|-------|---------|
| `recognition_only` row blocked from `get_optional_vocabulary_context` in teacher mode unprompted | ✅ |
| `do_not_use_unprompted` row blocked when `is_user_prompted=False` | ✅ |
| `do_not_use_unprompted` row surfaced when `is_user_prompted=True` | ✅ |
| Regional/informal English `mate` blocked from teacher mode unprompted | ✅ |
| Regional/informal English `mate` surfaced when user-prompted | ✅ |
| Russian `recognition_only` row blocked from suggestions by `is_entry_allowed_ru` | ✅ |
| Vulgar/offensive blocked in normal / teacher / professional / voice (Phase 12 carry-over) | ✅ |
| Slang/street/regional gated by mode-or-prompt (now includes `regional`) | ✅ |

## Test Results

```
$ python test_phase14_domain_pack_expansion.py   →  87 / 87 pass
$ python test_dual_pack_importer.py              → 116 / 116 pass
$ python test_dual_sovereign_pack_safety.py      →  79 / 79 pass
$ python test_vocabulary_runtime.py              →  74 / 74 pass
$ python test_russian_sovereign_stack.py         →  73 / 73 pass
TOTAL                                            → 429 / 429
```

Phase 14 harness breakdown (12 suites):

| # | Suite | Checks | Result |
|---|-------|-------:|--------|
| 1 | 28 packs exist + JSON parse | 56 | PASS |
| 2 | All Phase-14 entries validate against `coverage_taxonomy` (0 rejections) | 1 | PASS |
| 3 | End-to-end import of full `seed_packs/`; Phase-14 totals; per-pack manifests valid + sha256 | 5 | PASS |
| 4 | Safety / register / coverage / pack_source persisted into both DBs for sampled rows | 11 | PASS |
| 5 | Synthetic `recognition_only` row NOT surfaced in teacher mode | 1 | PASS |
| 6 | Synthetic `do_not_use_unprompted` row: blocked unprompted, allowed prompted | 2 | PASS |
| 7 | Real Phase-14 regional/informal row (`mate`) gated by mode-or-prompt | 2 | PASS |
| 8 | Russian `is_entry_allowed_ru` blocks Phase-14 `recognition_only` | 2 | PASS |
| 9 | Re-running orchestrator is idempotent (no row growth on either DB) | 3 | PASS |
| 10 | No new background threads; no recursion blow-up at `setrecursionlimit(400)` | 2 | PASS |
| 11 | Preview hard-caps even on a 10K-row input | 1 | PASS |
| 12 | No forbidden Program S / tier / probe / attestation / worker / luna_modules imports | 1 | PASS |

## Program S — Not Touched

- Test [12] regex-scans every production module (14 files) for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression|worker\.py|attestation_)` → **0 hits**.
- `git status` shows only Phase 14 and prior-phase Dual Sovereign files. No edits anywhere else.
- No service, scheduled task, daemon, registry key, WinSW config, or cron entry was created.

## Tier / Probe / Attestation / Worker / luna_modules — Not Touched

Same import-regex scan in test [12] explicitly covers tier_intent_library, luna_tier_, luna_modules, probe_health, repair_task_executor, tier_progression, worker.py, attestation_ — 0 hits. No file in those areas was opened with Edit/Write.

## Rollback

Phase 14 changes are reversible at three granularities:

1. **Soft rollback of new content only** (preserves all Phase 12/13 scaffolding):
   ```sql
   DELETE FROM cognitive.words
     WHERE pack_source IN
       ('seed_en_professions_jobs','seed_en_business_finance',
        'seed_en_law_government','seed_en_science_math',
        'seed_en_poetry_literary','seed_en_philosophy_abstract',
        'seed_en_art_music_culture','seed_en_history_geography',
        'seed_en_psychology_education','seed_en_mechanics_transportation',
        'seed_en_food_home_daily_life','seed_en_regional_dialect',
        'seed_en_formal_informal_speech','seed_en_voice_personality');
   DELETE FROM russian_stack.words
     WHERE pack_source LIKE 'seed_ru_%'
       AND pack_source NOT IN
         ('seed_ru_core','seed_ru_trades','seed_ru_medical',
          'seed_ru_coding','seed_ru_slang');
   DELETE FROM russian_stack.phrases
     WHERE pack_source = 'seed_ru_voice_personality';
   ```
2. **Delete pack files** (manifests + ingest reports remain unless also deleted):
   ```powershell
   Remove-Item seed_packs\en\professions_jobs.jsonl, seed_packs\en\business_finance.jsonl, ...
   Remove-Item seed_packs\ru\professions_jobs.jsonl, seed_packs\ru\business_finance.jsonl, ...
   ```
3. **Revert the 3 additive patches** if you want a literal revert:
   ```
   git checkout -- dual_pack_importer.py russian_knowledge_ingestion.py
   git checkout -- cognitive_word_policy.py russian_personality_layer.py
   git checkout -- test_dual_pack_importer.py
   ```
   All patches are additive and harmless if left in place — they just don't see Phase 14 content.

No service, scheduled task, daemon, registry key, or shared Luna runtime file was modified. Nothing else needs reverting.

## Next Recommended Phase

**Phase 15 — Bulk Frequency Pack (optional, requires operator green-light).**

- Use `wordfreq.iter_wordlist('en', 'large')` to stream the top ~25K English wordforms by Zipf frequency. Save as `seed_packs/en/bulk_top_25k.jsonl` with `coverage_categories=["core_vocabulary"]` and `register_tags=["standard"]`. Run `english_knowledge_ingestion.preview_ingestion` then `ingest_word_list`.
- For Russian, source a comparable open frequency list (e.g. OpenSubtitles Russian frequency, or a Wiktionary dump) and ingest the same way.
- After either reaches ~50K rows, add an FTS5 virtual table to its respective SQLite DB for sub-100ms substring search.
- **Do NOT yet integrate either runtime into Luna's main prompt builder** — that's a separate phase (Phase 16, deferred).

---

## Final Confirmations

- 28 validated seed packs shipped across 14 new coverage categories × 2 languages.
- Per-pack quality prioritized over raw count (averaged 33 EN / 31 RU entries per pack).
- All Phase-14 entries carry coverage / register / safety taxonomy that validates against `coverage_taxonomy.py`.
- Every pack import emits a manifest with streaming SHA256 (no full-file load).
- All five test harnesses pass: **74 + 73 + 79 + 116 + 87 = 429 / 429**.
- Unsafe / recognition_only / do_not_use_unprompted / vulgar / regional entries are correctly gated by the policy layer (verified live in tests [5]–[8]).
- **No Program S files touched.**
- **No tier / probe / attestation / worker / luna_modules files touched.**
- **No daemon. No recursion. No prompt injection. No full-corpus load.**
- Production DB counts: English 288 → 741, Russian 55 → 459 words / 10 → 35 phrases.
