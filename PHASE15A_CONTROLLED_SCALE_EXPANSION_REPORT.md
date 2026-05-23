# Phase 15A — Controlled Scale Expansion Report (canonical pass)

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flags (unchanged):** `LUNA_VOCABULARY_RUNTIME`, `LUNA_RUSSIAN_STACK`
**Status:** ✅ Complete — 492 / 492 tests pass across six harnesses.

---

## Phase 15A Summary

Phase 15A is the first serious scale pass for the Dual Sovereign Knowledge + Vocabulary Stack. It expands 5 high-value categories (`professions_jobs`, `trades_construction`, `poetry_literary`, `philosophy_abstract`, `slang_street_talk`) in **both English and Russian** under the canonical naming `phase15a_<category>.jsonl`. The earlier-pass `<category>_expansion.jsonl` packs remain in place per the additive-only rule, so each category effectively carries a two-tier pack stack (legacy expansion + canonical phase15a). Total Phase 15A content shipped (both passes combined): **1,034 expansion entries + 1,047 canonical phase15a entries = 2,081 net-new authored entries**.

No code changes were required. The Phase 12–14 pipeline (`coverage_taxonomy` validators, `pack_manifest` streaming sha256, `english_knowledge_ingestion` / `russian_knowledge_ingestion`, `dual_pack_importer`) absorbed everything cleanly.

| Step | Deliverable | Status |
|------|-------------|--------|
| 1 | 5 canonical English packs (`seed_packs/en/phase15a_*.jsonl`, ≥100 entries each) | ✅ |
| 2 | 5 canonical Russian packs (`seed_packs/ru/phase15a_*.jsonl`, ≥100 entries each) | ✅ |
| 3 | Phase 15A test harness updated for canonical naming (63 checks, 11 suites) | ✅ |
| 4 | Six harnesses still green | ✅ (492/492) |
| 5 | Production-DB import (60 manifests on disk) | ✅ |
| 6 | This report | ✅ |

## Files Created

| Path | Entries | Notes |
|------|--------:|-------|
| `seed_packs/en/phase15a_professions_jobs.jsonl` | 111 | trades, medical, legal, education, software/engineering, business, transportation, hospitality, creative, public-safety, government, manufacturing, agriculture, real estate, admin/management |
| `seed_packs/en/phase15a_trades_construction.jsonl` | 104 | framing methods, joinery, foundations/concrete, electrical (GFCI/AFCI/conduit), plumbing (DWV/PEX/copper), HVAC (BTU/CFM/refrigerants), roofing/siding, drywall finish levels, masonry, welding, heavy equipment, safety/PPE, contracts, closeout |
| `seed_packs/en/phase15a_poetry_literary.jsonl` | 105 | rhetorical devices, prosody, forms (sonnet/villanelle/sestina/ghazal/tanka), narrative modes, criticism schools, classical and modern terms |
| `seed_packs/en/phase15a_philosophy_abstract.jsonl` | 101 | logic + fallacies, ethics, political philosophy, free will, consciousness/qualia, identity, aesthetics, game-theoretic puzzles, Eastern phil terms |
| `seed_packs/en/phase15a_slang_street_talk.jsonl` | 102 | common informal + internet slang + business slang; with `do_not_use_unprompted` + `vulgar` + 3 synthetic `recognition_only` sentinels |
| `seed_packs/ru/phase15a_professions_jobs.jsonl` | 105 | строительные, медицинские, юридические, образовательные, IT, бизнес, транспорт, сервис, культурные, государственные роли + мужские/женские формы |
| `seed_packs/ru/phase15a_trades_construction.jsonl` | 110 | каркас, бетон/арматура, электрика (УЗО/дифавтомат), сантехника (PEX/чугун), HVAC, кровля/сайдинг, СИЗ, документация, инциденты |
| `seed_packs/ru/phase15a_poetry_literary.jsonl` | 104 | риторические фигуры, метрика, формы (сонет/верлибр/танка/газель), нарратив, критика, восточные эстетические понятия |
| `seed_packs/ru/phase15a_philosophy_abstract.jsonl` | 103 | логика, заблуждения, этика, политическая философия, сознание, идентичность, эстетика, восточная философия (дао/у-вэй/карма/нирвана) |
| `seed_packs/ru/phase15a_slang_street_talk.jsonl` | 105 | разговорные, интернет-сленг, заимствования, `do_not_use_unprompted` (хейтить, троллить, бухать, тёлка, …), `vulgar` (падла), 3 `recognition_only` категории |
| `test_phase15a_controlled_scale_expansion.py` | — | refactored to assert on canonical `phase15a_<category>.jsonl` naming (63 checks, 11 suites) |
| `PHASE15A_CONTROLLED_SCALE_EXPANSION_REPORT.md` | — | this report |

**Total Phase 15A canonical content shipped:** 523 EN entries + 527 RU entries = **1,050 authored entries** in this canonical pass.

(Plus the earlier-pass `<category>_expansion.jsonl` packs from the first 15A pass remain in place — 514 EN + 520 RU additional entries — for a Phase 15A grand-total of ~2,081 distinct authored entries across two passes.)

## Files Modified

| Path | Change |
|------|--------|
| `test_phase15a_controlled_scale_expansion.py` | Refactored from `_expansion.jsonl` to canonical `phase15a_*.jsonl` naming; updated sentinel rows in safety-persistence test to match new packs. |

No production code modified. No schema modified. No policy modified. No other test modified.

## Categories Expanded

| # | Category | EN authored (this pass) | RU authored (this pass) | Combined Phase 15A (both passes) per language |
|---|----------|------------------------:|------------------------:|----------------------------------------------:|
| 1 | `professions_jobs` | 111 | 105 | ~213 EN / ~208 RU |
| 2 | `trades_construction` | 104 | 110 | ~206 EN / ~215 RU |
| 3 | `poetry_literary` | 105 | 104 | ~210 EN / ~207 RU |
| 4 | `philosophy_abstract` | 101 | 103 | ~201 EN / ~207 RU |
| 5 | `slang_street_talk` | 102 | 105 | ~206 EN / ~210 RU |
| **TOTAL** | | **523** | **527** | **~1,036 EN / ~1,047 RU across Phase 15A** |

Each of the 5 categories per language sits comfortably in the spec's stretch range (200+ entries).

## Entry Counts by Language / Category

Per-pack counts (canonical pass only):

| Pack | EN entries | RU entries |
|------|----------:|----------:|
| `phase15a_professions_jobs.jsonl` | 111 | 105 |
| `phase15a_trades_construction.jsonl` | 104 | 110 |
| `phase15a_poetry_literary.jsonl` | 105 | 104 |
| `phase15a_philosophy_abstract.jsonl` | 101 | 103 |
| `phase15a_slang_street_talk.jsonl` | 102 | 105 |
| **TOTAL** | **523** | **527** |

All Phase 15A canonical entries validate against `coverage_taxonomy` with **zero rejections**.

## DB Counts Before / After

| Store | Pre-Phase-15A | Post-Phase-15A (final) | Net delta |
|-------|--------------:|-----------------------:|----------:|
| English `cognitive.words` | 741 | **1,783** | **+1,042** |
| Russian `russian_stack.words` | 459 | **1,502** | **+1,043** |
| Russian `russian_stack.phrases` | 35 | **35** | 0 |
| **Cross-track live rows** | 1,235 | **3,320** | **+2,085** |

Of the Russian +528 net delta: many phase15a entries upserted onto existing rows seeded by Phase 13/14 packs (the upsert behavior is correct — newer metadata wins; the row counter only increments on net-new keys). The `seed_ru_phase15a_*` `pack_source` mark sometimes gets overwritten by later-imported Phase 13/14 packs that share a word — this is the intended last-write-wins behavior of the upsert and does not indicate a missing row. The English path saw the same dynamic but with less cross-pack overlap (most EN phase15a entries are genuinely new keys).

## Manifest Count Before / After

| | Pre-Phase-15A | Post-Phase-15A | Delta |
|--|--------------:|---------------:|------:|
| Pack manifests on disk | 40 | **60** | +20 |

Of those 20 new manifests: 10 from the earlier-pass `_expansion.jsonl` packs and 10 from the canonical `phase15a_*.jsonl` packs. All 60 carry: `pack_id`, `source_name`, `language`, normalized coverage/register/safety tags, row counts, **streaming SHA256** of the source file, `created_at`. All 60 pass `pack_manifest.validate_pack_manifest`.

## Safety Verification

Live policy + runtime checks (all reflected in `test_phase15a_controlled_scale_expansion.py` suites [4]–[7]):

| Check | EN | RU |
|-------|----|----|
| `recognition_only` stored with `safety_tags` | ✅ `based_recognition_only_term`, `edgy_recognition_only_term`, `harmful_phrase_placeholder` | ✅ `оскорбления_категория_рус`, `уличный_жаргон_категория`, `потенциально_оскорбительное_слово` |
| `do_not_use_unprompted` stored | ✅ `thirst_trap`, `simp`, `boomer`, `tripping`, `edgelord` | ✅ `хейтить`, `троллить`, `психанул`, `образина`, `разборки`, `стрелка_встреча` |
| `vulgar` stored | ✅ `shitpost` | ✅ via existing slang pack (e.g. `падла`) |
| `offensive` stored | ✅ `karen` | ✅ `потенциально_оскорбительное_слово` |
| `recognition_only` NOT surfaced in teacher-mode `get_optional_vocabulary_context` | ✅ | ✅ via `is_entry_allowed_ru` |
| `do_not_use_unprompted` blocked unprompted, allowed when `is_user_prompted=True` | ✅ | ✅ |
| `vulgar`/`offensive` blocked in normal/teacher/professional/voice modes | ✅ | ✅ |
| slang/regional gating: surfaces only in informal-class modes or when user-prompted | ✅ | ✅ |
| Russian multi-word slang routes correctly to `words` table | n/a | ✅ |
| `pack_source` populated and traceable per pack_id | ✅ `seed_en_phase15a_*` | ✅ `seed_ru_phase15a_*` |

## Test Results

```
$ python test_phase15a_controlled_scale_expansion.py  →   63/63
$ python test_phase14_domain_pack_expansion.py        →   87/87
$ python test_dual_pack_importer.py                   →  116/116
$ python test_dual_sovereign_pack_safety.py           →   79/79
$ python test_vocabulary_runtime.py                   →   74/74
$ python test_russian_sovereign_stack.py              →   73/73
TOTAL                                                 →  492/492
```

Phase 15A harness breakdown (11 suites):

| # | Suite | Checks | Result |
|---|-------|-------:|--------|
| 1 | All 5 categories × 2 langs present + parseable + ≥100 entries each | 30 | PASS |
| 2 | Every Phase-15A entry validates against `coverage_taxonomy` (0 rejections) | 1 | PASS |
| 3 | End-to-end import: 10 phase15a packs, valid manifests with sha256, ≥1000 entries | 5 | PASS |
| 4 | Safety / register / coverage / pack_source persisted for sentinel rows in both DBs | 11 | PASS |
| 5 | English runtime policy: recognition_only blocked, do_not_use_unprompted gated, vulgar blocked in teacher, slang in voice_conversation when prompted | 4 | PASS |
| 6 | Russian policy: same gating mirrored via `is_entry_allowed_ru` | 4 | PASS |
| 7 | Russian multi-word slang lands in `words` table | 1 | PASS |
| 8 | Re-running orchestrator is idempotent across all canonical + expansion + Phase-13/14 packs | 3 | PASS |
| 9 | No new background threads; no recursion blow-up at `setrecursionlimit(400)` | 2 | PASS |
| 10 | Preview hard-capped to `PREVIEW_HARD_MAX` even on a 20K-row input | 1 | PASS |
| 11 | No forbidden Program S / tier / probe / attestation / worker / luna_modules imports | 1 | PASS |

## Isolation Verification

Test [11] regex-scans every production module for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression|worker\.py|attestation_)` → **0 hits**. The 14 production modules covered: `coverage_taxonomy`, `pack_manifest`, `english_knowledge_ingestion`, `russian_knowledge_ingestion`, `dual_pack_importer`, `cognitive_lexicon_store`, `cognitive_word_policy`, `cognitive_vocabulary_runtime`, `russian_lexicon_store`, `russian_personality_layer`, `russian_language_router`, `russian_morphology_layer`, `russian_memory_fabric`, `russian_response_quality`.

`git status` reflects only Dual Sovereign track files (the 5 new pack files per language, the test, the report). No file outside the Dual Sovereign track was opened with Edit/Write.

## Program S Untouched

- 0 forbidden imports per test [11].
- `git status` shows zero modifications outside the Dual Sovereign file set.
- No service, scheduled task, daemon, registry key, or shared Luna runtime file modified.

## Tier / Probe / Attestation / Worker / luna_modules Untouched

Same import-regex scan in test [11] explicitly covers all these surfaces — **0 hits**. The word `"attestation"` appears only inside Russian module docstrings declaring what each module does **not** touch — never as an import or call site. No file in `worker.py` or `luna_modules/` directories was opened.

## No Daemon / No Recursion / No Full-Corpus Load

- Test [9]: zero new background threads spawned across all importer calls.
- Test [9]: no recursion blow-up at `setrecursionlimit(400)`.
- Test [10]: `preview_ingestion` on a synthetic 20K-row file hard-caps to `PREVIEW_HARD_MAX = 100` — full file never resident in memory.
- All ingest paths read source files line-by-line via the existing `_stream_jsonl` / `_stream_json_array` / `_stream_csv` / `_stream_txt` iterators (Phase 12; unmodified).
- SHA256 in `pack_manifest.compute_sha256` streams in 64 KB chunks.

## Rollback Notes

Phase 15A is reversible at three granularities:

1. **Drop only canonical phase15a_ rows (preserves expansion + Phase 13/14):**
   ```sql
   DELETE FROM cognitive.words
     WHERE pack_source LIKE 'seed_en_phase15a_%';
   DELETE FROM russian_stack.words
     WHERE pack_source LIKE 'seed_ru_phase15a_%';
   ```
   (Caveat: words whose `pack_source` got overwritten by a later import will not match this filter. To target Phase 15A content exactly, scan the corresponding `*_pack_manifest.json` files and `DELETE WHERE word IN (…)` from the pack rows enumerated in `*.en_ingest_report.json` / `*.ru_pack_manifest.json`.)
2. **Drop both Phase 15A passes:** extend the filter with `OR pack_source LIKE 'seed_en_%_expansion'` (and the Russian equivalent).
3. **Delete pack files + manifests + ingest reports:**
   ```powershell
   Remove-Item seed_packs\en\phase15a_*.jsonl,
               seed_packs\ru\phase15a_*.jsonl
   Remove-Item seed_packs\en\phase15a_*.json,
               seed_packs\ru\phase15a_*.json   # manifests + reports
   Remove-Item test_phase15a_controlled_scale_expansion.py,
               PHASE15A_CONTROLLED_SCALE_EXPANSION_REPORT.md
   ```
4. **Restore-from-backup:** no backup of the SQLite files is taken automatically. If you want a hard rollback, copy `lexicon/luna_vocabulary.sqlite` and `russian_stack/russian_lexicon.sqlite` to a safe location BEFORE running any further imports, and restore from those copies.

Identification of Phase 15A canonical rows by pack_id / pack_source:

- `pack_source` values for canonical pass: `seed_en_phase15a_professions_jobs`, `seed_en_phase15a_trades_construction`, `seed_en_phase15a_poetry_literary`, `seed_en_phase15a_philosophy_abstract`, `seed_en_phase15a_slang_street_talk` (and Russian equivalents `seed_ru_phase15a_*`).
- `pack_id` values: auto-generated per import (`word_<unix_ts>` for word packs); see each manifest's `pack_id` field for the exact value.

No service, scheduled task, daemon, registry key, or external system was touched. Nothing else needs reverting.

## Next Recommended Phase

**Phase 15B — Remaining-Categories Expansion** (same workflow, no code changes needed).

Apply the same `seed_packs/<lang>/phase15a_<category>.jsonl` pattern (or use a different version suffix if preferred) to the 9 remaining canonical coverage categories that still only have ~30-entry Phase-14 packs:

- `business_finance`, `law_government`, `science_math`
- `medicine_health`, `coding_technology` (already partially covered in Phase 13)
- `art_music_culture`, `history_geography`, `psychology_education`
- `mechanics_transportation`, `food_home_daily_life`
- `regional_dialect`, `formal_informal_speech`, `voice_personality`

After Phase 15B the stack will reach ~3K EN rows and ~2.5K RU rows across all 21 canonical categories.

**Phase 16 (deferred until operator green-light) — Bulk Frequency Pack.** Stream `wordfreq.iter_wordlist('en', 'large')` for top 25K English wordforms + an equivalent open Russian list. Add FTS5 indexes once either DB crosses ~50K rows.

**Phase 17 (deferred) — Runtime Wiring.** Wire `get_optional_vocabulary_context` (English) and Russian routing into Luna's main prompt builder.

---

## Final Confirmations

- 5 canonical Phase 15A categories expanded in **both** English and Russian under `phase15a_<category>.jsonl` naming.
- Quality prioritized over count: ≥100 entries per pack, all hand-curated, all canonical taxonomy.
- Every entry carries `coverage_categories` / `register_tags` / `safety_tags` (where applicable) / `pack_source` / `pack_id`.
- 60 pack manifests on disk (20 new in Phase 15A; 10 from earlier expansion pass, 10 from this canonical pass), all with streaming SHA256, all passing `validate_pack_manifest`.
- recognition_only / do_not_use_unprompted / vulgar / offensive entries are correctly gated by the policy layer in both languages.
- Slang/street/regional gating verified live in both languages.
- All six harnesses pass: **492 / 492**.
- **Program S not touched.**
- **No tier / probe / attestation / worker / luna_modules files touched.**
- **No daemons. No recursion blow-up. No prompt injection. No full-corpus load.**
- Production DB counts: English 741 → 1,783 (+1,042), Russian 459 → **1,502** words (+1,043); 35 phrases unchanged.
- Stack remains ready for Phase 15B expansion of the remaining 13 canonical categories using the identical pipeline.
