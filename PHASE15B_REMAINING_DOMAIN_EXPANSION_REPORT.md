# Phase 15B — Remaining Domain Expansion Report

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flags (unchanged):** `LUNA_VOCABULARY_RUNTIME`, `LUNA_RUSSIAN_STACK`
**Status:** ✅ Complete — 609 / 609 tests pass across all seven harnesses.

---

## Phase 15B Summary

Phase 15B is the second major scale pass for the Dual Sovereign Knowledge + Vocabulary Stack. It covers the 15 canonical coverage categories that Phase 15A left at smaller seed depth: `business_finance`, `law_government`, `science_math`, `medicine_health`, `coding_technology`, `art_music_culture`, `history_geography`, `psychology_education`, `mechanics_transportation`, `food_home_daily_life`, `regional_dialect`, `formal_informal_speech`, `voice_personality`, `idioms_phrases`, `core_vocabulary`. With Phase 15B applied, every one of the 21 canonical coverage categories has a deep, manifest-backed bilingual pack at ≥75 entries per language (most ≥100), with safety / register / coverage taxonomy enforced at ingest and at runtime.

No code changes were required. The Phase 12–15A pipeline absorbed Phase 15B cleanly.

| Step | Deliverable | Status |
|------|-------------|--------|
| Pre-flight | Phase 15A report + harness + packs verified | ✅ |
| 1 | 15 English `phase15b_<category>.jsonl` packs (≥75 each) | ✅ |
| 2 | 15 Russian `phase15b_<category>.jsonl` packs (≥75 each) | ✅ |
| 3 | `test_phase15b_remaining_domain_expansion.py` (12 suites, 117 checks) | ✅ |
| 4 | All seven harnesses still green (609 / 609) | ✅ |
| 5 | Production-DB import (90 manifests on disk total) | ✅ |
| 6 | This report | ✅ |

## Pre-flight Phase 15A Verification

The Phase 15B harness (suite [A]) verified before any modification:

- `PHASE15A_CONTROLLED_SCALE_EXPANSION_REPORT.md` present ✅
- `test_phase15a_controlled_scale_expansion.py` present ✅
- `seed_packs/en/phase15a_*.jsonl` ≥ 5 found ✅
- `seed_packs/ru/phase15a_*.jsonl` ≥ 5 found ✅
- Production state at Phase 15A end: EN 1,783 / RU 1,502 / phrases 35 / manifests 60 ✅
- All five Phase 12–15A harnesses green at pre-flight ✅

## Categories Expanded (Phase 15B)

| # | Category | EN authored | RU authored |
|---|----------|------------:|------------:|
| 1 | `business_finance` | 78 | 76 |
| 2 | `law_government` | 80 | 78 |
| 3 | `science_math` | 80 | 80 |
| 4 | `medicine_health` | 80 | 78 |
| 5 | `coding_technology` | 80 | 79 |
| 6 | `art_music_culture` | 76 | 76 |
| 7 | `history_geography` | 80 | 80 |
| 8 | `psychology_education` | 77 | 75 |
| 9 | `mechanics_transportation` | 80 | 78 |
| 10 | `food_home_daily_life` | 81 | 78 |
| 11 | `regional_dialect` | 76 | 79 |
| 12 | `formal_informal_speech` | 79 | 80 |
| 13 | `voice_personality` | 76 | 80 |
| 14 | `idioms_phrases` | 78 | 79 |
| 15 | `core_vocabulary` | 78 | 78 |
| **TOTAL authored** | | **~1,179** | **~1,174** |

Every Phase-15B pack ≥ 75 entries (specification minimum). Taxonomy validation passes with **zero rejections**.

## English Packs Created

`seed_packs/en/phase15b_<category>.jsonl` for all 15 categories above.

## Russian Packs Created

`seed_packs/ru/phase15b_<category>.jsonl` for all 15 categories above.

## Files Created

| Path | Role |
|------|------|
| `seed_packs/en/phase15b_*.jsonl` (×15) | English Phase-15B packs |
| `seed_packs/ru/phase15b_*.jsonl` (×15) | Russian Phase-15B packs |
| `test_phase15b_remaining_domain_expansion.py` | 117-check Phase 15B harness (12 suites) |
| `PHASE15B_REMAINING_DOMAIN_EXPANSION_REPORT.md` | This report |

## Files Modified

**None.** Phase 15B is purely additive content. No source code, schema, policy, or earlier test was altered.

## DB Counts Before / After

| Store | Pre-Phase-15B (= Phase-15A end) | Post-Phase-15B | Delta |
|-------|---------------:|---------------:|------:|
| English `cognitive.words` | 1,783 | **2,814** | **+1,031** |
| Russian `russian_stack.words` | 1,502 | **2,518** | **+1,016** |
| Russian `russian_stack.phrases` | 35 | **35** | 0 |
| **Cross-track total** | 3,320 | **5,367** | **+2,047** |

(Some Russian phase15b voice/idiom entries route into the `words` table per the existing convention; idiom-flagged single-row phrases remain in `phrases` only for explicitly idiomatic content like the Russian `idioms.jsonl` from Phase 13. Per-pack counts reflect what landed.)

## English Entries Accepted / Rejected / Duplicates

| | Phase 15B EN |
|--|----:|
| Authored entries | ~1,179 |
| Per-pack import errors | 0 |
| Taxonomy rejections | 0 |
| Manifest-validation failures | 0 |
| Net new rows in production | +1,031 (≈ 148 cross-pack / cross-phase upsert collisions on common terms like `array`, `cache`, `git`, `vinyl_record`, `recipe`, `motivation`) |

## Russian Words / Phrases Added

| | Phase 15B RU |
|--|----:|
| Authored entries | ~1,174 |
| Per-pack import errors | 0 |
| Taxonomy rejections | 0 |
| Net new RU `words` rows | +1,016 |
| Net new RU `phrases` rows | 0 (Phase 15B uses single-word/word-key storage; no idiomatic-flagged additions to the `phrases` table) |

## Manifest Count Before / After

| | Before | After | Delta |
|--|----:|----:|----:|
| Pack manifests on disk | 60 | **90** | +30 |

All 90 manifests pass `pack_manifest.validate_pack_manifest`, every one with a streaming 64-character SHA256 over its source file.

## Safety Verification

Live policy + runtime checks (from `test_phase15b_remaining_domain_expansion.py` suites [E]–[H]):

| Check | English | Russian |
|-------|---------|---------|
| `recognition_only` stored with `safety_tags` | ✅ `opioid_term`, `narcotic_term`, `controlled_substance`, `phishing_term`, `malware_term`, `ransomware_term`, `coup_d_etat`, `regicide` | ✅ `уголовная_ответственность`, `уголовное_дело`, `административный_арест`, `наркотические_вещества_термин`, `контролируемые_вещества`, `опиоиды_термин`, `переворот_термин` |
| `pack_source` populated per phase15b row | ✅ `seed_en_phase15b_<category>` | ✅ `seed_ru_phase15b_<category>` |
| recognition_only NOT surfaced in teacher-mode `get_optional_vocabulary_context` | ✅ confirmed live | ✅ confirmed via `is_entry_allowed_ru` |
| recognition_only ALLOWED for explanation context | ✅ | ✅ |
| All vulgar / offensive / do_not_use_unprompted tags from Phase 13/14/15A still block correctly | ✅ (no regression) | ✅ (no regression) |
| slang/regional/street gating still works | ✅ (no regression) | ✅ (no regression) |
| Phrase entries route correctly (Russian voice/personality multi-word stored in words table per convention) | n/a | ✅ |

## Test Results

```
$ python test_phase15b_remaining_domain_expansion.py    → 117/117
$ python test_phase15a_controlled_scale_expansion.py    →  63/63
$ python test_phase14_domain_pack_expansion.py          →  87/87
$ python test_dual_pack_importer.py                     → 116/116
$ python test_dual_sovereign_pack_safety.py             →  79/79
$ python test_vocabulary_runtime.py                     →  74/74
$ python test_russian_sovereign_stack.py                →  73/73
TOTAL                                                   → 609/609
```

Phase 15B harness breakdown (12 suites):

| # | Suite | Checks | Result |
|---|-------|-------:|--------|
| A | Pre-flight Phase 15A artifacts present | 4 | PASS |
| B | All 30 Phase-15B packs exist, parse, and have ≥75 entries | 90 | PASS |
| C | Every Phase-15B entry's coverage / register / safety validates | 1 | PASS |
| D | Full seed_packs/ import: 30 Phase-15B packs land, manifests valid + sha256, ≥2000 entries | 5 | PASS |
| E | Safety / register / coverage / pack_source persisted into both DBs | 6 | PASS |
| F | English runtime policy: recognition_only blocked in teacher | 1 | PASS |
| G | Russian policy: recognition_only blocked from suggestion, allowed for explanation | 2 | PASS |
| H | Russian phrase routing sanity (voice phrase stored in words table per convention) | 1 | PASS |
| I | Re-import is idempotent across 90 packs | 3 | PASS |
| J | No background threads; no recursion blow-up at `setrecursionlimit(400)` | 2 | PASS |
| K | Preview hard-capped to `PREVIEW_HARD_MAX` on a 30K-row synthetic file | 1 | PASS |
| L | No forbidden Program S / tier / probe / attestation / worker / luna_modules imports | 1 | PASS |

## Isolation Verification

Test [L] regex-scans every production module for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression|worker\.py|attestation_)` → **0 hits** across all 14 production modules. `git status` shows only Dual Sovereign track files (the 30 new Phase 15B pack files, the new test harness, and this report) touched.

## Program S Untouched

- 0 forbidden imports detected by test [L].
- `git status` shows zero modifications to any file outside the Dual Sovereign track.
- No service, scheduled task, daemon, registry key, or shared Luna runtime file modified.

## Tier / Probe / Attestation / Worker / luna_modules Untouched

Same import-regex scan in test [L] explicitly covers all those surfaces — **0 hits**. The string `"attestation"` continues to appear only inside Russian module docstrings declaring what each module does **not** touch; never as an import or call site.

## No Daemon / No Recursion / No Full-Corpus Load

- Test [J]: zero new background threads spawned during the full preview sweep.
- Test [J]: no recursion blow-up at `setrecursionlimit(400)`.
- Test [K]: `preview_ingestion` on a synthetic 30,000-row file hard-caps to `PREVIEW_HARD_MAX = 100` — the file is never resident in memory.
- All ingest paths still read source files line-by-line via the Phase 12 chunked iterators.
- SHA256 in `pack_manifest.compute_sha256` continues to stream in 64 KB chunks.

## Rollback Notes

Phase 15B is fully reversible at three granularities:

1. **Drop only phase15b_ rows (preserves Phase 12–15A):**
   ```sql
   DELETE FROM cognitive.words
     WHERE pack_source LIKE 'seed_en_phase15b_%';
   DELETE FROM russian_stack.words
     WHERE pack_source LIKE 'seed_ru_phase15b_%';
   DELETE FROM russian_stack.phrases
     WHERE pack_source LIKE 'seed_ru_phase15b_%';
   ```
   (Caveat: rows whose `pack_source` was overwritten by a later import of an overlapping word will not match this filter. To target Phase 15B content exactly, scan the corresponding `*_pack_manifest.json` files and `DELETE WHERE word IN (...)` over the manifest's word list.)

2. **Delete Phase-15B pack files + manifests + ingest reports:**
   ```powershell
   Remove-Item seed_packs\en\phase15b_*.jsonl, seed_packs\ru\phase15b_*.jsonl
   Remove-Item seed_packs\en\phase15b_*.json, seed_packs\ru\phase15b_*.json    # manifests + reports
   Remove-Item test_phase15b_remaining_domain_expansion.py
   Remove-Item PHASE15B_REMAINING_DOMAIN_EXPANSION_REPORT.md
   ```

3. **DB backups:** no automatic backup is taken — copy `lexicon/luna_vocabulary.sqlite` and `russian_stack/russian_lexicon.sqlite` to a safe location BEFORE further imports if you want a hard restore point. To identify Phase-15B rows by `pack_id`: each `*_pack_manifest.json` carries a unique `pack_id` (auto-generated as `word_<unix_ts>`); search by that.

Identification of Phase 15B canonical rows:
- `pack_source` values: `seed_en_phase15b_*`, `seed_ru_phase15b_*` (15 per language).
- `pack_id`: per-import unique, recorded in each manifest's `pack_id` field.

No service, scheduled task, daemon, registry key, or external system was touched. Nothing else needs reverting.

## Next Recommended Phase

**Phase 16 — Bulk Frequency Pack (operator approval required).**

With the stack now at ~5.4K bilingual rows across all 21 canonical coverage categories, the next reasonable step is bulk-frequency expansion:

- English: stream `wordfreq.iter_wordlist('en', 'large')` for the top ~25K wordforms; emit one large `seed_packs/en/phase16_bulk_freq_top25k.jsonl` with `coverage_categories=["core_vocabulary"]`, `register_tags=["standard"]`. Use the existing `english_knowledge_ingestion` path.
- Russian: use OpenSubtitles Russian frequency list (or similar open source) for an equivalent top-25K bulk pack.
- After either DB crosses ~50K rows, add an FTS5 virtual table over `(word, lemma, definition_*)` and triggers to keep it in sync.

**Phase 17 (deferred until operator green-light) — Runtime Wiring.** Plug `get_optional_vocabulary_context` (English) and the Russian routing equivalent into Luna's main prompt builder. Still NOT part of Phase 15B.

---

## Final Confirmations

- All 15 selected coverage categories expanded in both English and Russian under canonical `phase15b_<category>.jsonl` naming.
- ≥ 75 entries per pack per language (specification minimum); average ~78 per pack.
- Every entry carries `coverage_categories` / `register_tags` / `safety_tags` (where applicable) / `pack_source` / `pack_id`.
- 90 pack manifests on disk total (60 prior + 30 new), all streaming-SHA256, all passing `validate_pack_manifest`.
- recognition_only / do_not_use_unprompted / vulgar / offensive entries correctly gated by the policy layer in both languages — verified live for new Phase-15B sentinel rows.
- All seven harnesses pass: **609 / 609**.
- **Program S not touched.**
- **No tier / probe / attestation / worker / luna_modules files touched.**
- **No daemons. No recursion blow-up. No prompt injection. No full-corpus load.**
- Production DB counts: English 1,783 → 2,814 (+1,031), Russian 1,502 → 2,518 words (+1,016); 35 phrases unchanged.
- Stack is now ready for Phase 16 (bulk frequency import) when operator gives the go-ahead.
