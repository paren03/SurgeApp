# Russian Sovereign Language Stack — Build Report

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flag:** `LUNA_RUSSIAN_STACK=1`
**Status:** Foundation complete — 73 / 73 tests pass

---

## Task Summary

Stand up Luna's standalone Russian Sovereign Language Stack covering:
language routing, lexicon storage, morphology hints, durable Russian memory,
personality / style policy, knowledge-pack ingestion, and response-quality
scoring. Foundation only — no live integration with Luna's main runtime,
no auto-rewrites of any English/Russian output, no daemons, no recursion,
no large prompt injection. Ready for future near-million-word import via
chunked ingestion (Phase 11).

## Phase-by-Phase Completion

| Phase | Description | Status |
|------:|-------------|--------|
| 1 | Isolation + safety foundation (feature-flag, no daemons, no recursion) | ✅ |
| 2 | Russian language router (`detect_russian_text`, `detect_language_mode`, `should_use_russian_stack`, `route_russian_context`) | ✅ |
| 3 | Russian lexicon store (SQLite; words + phrases; all 10 required functions + `count_words`/`count_phrases`) | ✅ |
| 4 | Russian morphology layer (`normalize`, `guess_lemma`, `detect_part_of_speech`, case/number/gender hints, naturalness score, bounded notes) | ✅ |
| 5 | Russian memory fabric (isolated SQLite store; CRUD + bounded retrieval + transliteration stub) | ✅ |
| 6 | Russian personality layer (8 modes, translation-artifact detector, advisory-only style report) | ✅ |
| 7 | Russian knowledge ingestion (jsonl/json/csv/txt, batched, bounded; word + phrase + topic-pack; ingestion reports) | ✅ |
| 8 | Russian response quality (native-feel / clarity / register-fit scorers; bounded rewrite suggestions; no auto-rewrite) | ✅ |
| 9 | Test harness (`test_russian_sovereign_stack.py`, 73 checks, 11 suites) | ✅ |
| 10 | This report | ✅ |
| 11 | Future Russian Knowledge Pack Importer | 📋 documented as next step (deferred — no source files supplied) |

## Files Created

| Path | Role |
|------|------|
| `russian_stack/` | DB home directory |
| `russian_stack/russian_lexicon.sqlite` | Lexicon SQLite store (schema-only, 0 rows + 0 phrases) |
| `russian_stack/russian_memory.sqlite` | Memory SQLite store (schema-only, 0 rows) |
| `russian_language_router.py` | Cyrillic / mixed-language detection + bounded routing metadata |
| `russian_lexicon_store.py` | SQLite store for words + phrases (words: 17 columns; phrases: 10 columns) |
| `russian_morphology_layer.py` | pymorphy-aware (with heuristic fallback) lemma / POS / case / number / gender hints |
| `russian_memory_fabric.py` | Isolated Russian memory store (CRUD + retrieve_context_ru + transliteration stub) |
| `russian_personality_layer.py` | 8-mode style rules + translation-artifact detector + advisory style report |
| `russian_knowledge_ingestion.py` | Chunked, validated, batched ingestion for words / phrases / topic-packs |
| `russian_response_quality.py` | Native-feel / clarity / register-fit scoring + bounded rewrite suggestions |
| `test_russian_sovereign_stack.py` | 73-check bounded harness |
| `RUSSIAN_SOVEREIGN_STACK_REPORT.md` | This file |

## Files Modified

**None outside the listed new files.** `git status` of the worktree:

```
?? RUSSIAN_SOVEREIGN_STACK_REPORT.md
?? russian_knowledge_ingestion.py
?? russian_language_router.py
?? russian_lexicon_store.py
?? russian_memory_fabric.py
?? russian_morphology_layer.py
?? russian_personality_layer.py
?? russian_response_quality.py
?? russian_stack/
?? test_russian_sovereign_stack.py
```

(Plus the pre-existing entries from previous sessions: `VOCABULARY_RUNTIME_REPORT.md`,
`cognitive_*.py`, `lexicon/`, `test_vocabulary_runtime.py`, and the unrelated
`ruvector.db` artifact noted in the earlier vocabulary report — none of
these belong to this task and none were modified by it.)

## Program S — Not Touched

Hard-confirmed:

- Test [11] greps every production module for `^(import|from)\s+\S*(program_s|tier_intent_library|luna_tier_|luna_modules|probe_health|repair_task_executor|tier_progression)` → **0 hits**.
- `git status` reports no edits to any pre-existing file.
- No file outside the 10 new files was opened via Edit/Write.
- No service, scheduled task, daemon, registry key, WinSW config, or cron entry was created.

## Tier / Probe / Attestation Files — Not Touched

- Same regex scan in test [11] explicitly covers `luna_tier_*`, `tier_intent_library`, `tier_progression`, `probe_health`, and (via `luna_modules`) the attestation surface — all 0 hits.
- Test [11] also confirms all 7 production modules are present at expected paths.
- The string "attestation" appears only in module docstrings declaring what the module does **not** touch — never as an import or call site.

## Database Paths

| Store | Path | Override env var | Row count |
|-------|------|------------------|-----------|
| Russian lexicon | `D:\SurgeApp\russian_stack\russian_lexicon.sqlite` | `LUNA_RUSSIAN_LEXICON_DB` | 0 words / 0 phrases (schema-only) |
| Russian memory | `D:\SurgeApp\russian_stack\russian_memory.sqlite` | `LUNA_RUSSIAN_MEMORY_DB` | 0 memories (schema-only) |

(Resolved paths in this worktree: `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\russian_stack\…`.)

Both DBs use WAL journaling, NORMAL synchronous mode, and per-call connections (no in-memory cache, no persistent connection pool).

## Feature-Flag Behavior

`LUNA_RUSSIAN_STACK` is checked by every module that has runtime side-effects on Luna:

| Setting | `router.should_use_russian_stack` | `router.route_russian_context` | Stores / morphology / personality |
|---------|-----------------------------------|--------------------------------|------------------------------------|
| Unset or `0` | `{"use_russian": False, "reason": "feature_flag_off"}` | `{"enabled": False, "reason": "feature_flag_off"}` | Callable directly (these are data-layer utilities), but routing layer above will not invoke them. |
| `1`, `true`, `yes`, `on` | Bounded routing decision dict | Full bounded routing metadata dict | Used as designed. |

Disabled-safe: tests [1] explicitly verify both router functions return safe results when the flag is unset.

## Optional Dependency Status

| Dependency | Installed? | Used by | Behavior if missing |
|------------|-----------|---------|--------------------|
| `pymorphy3` | **no** | `russian_morphology_layer` (lemma / POS / case / number / gender) | Falls back to rule-based heuristics that mark `source: 'heuristic'` and `confidence ≤ 0.30`. All tests still pass. |
| `pymorphy2` | **no** | Same — secondary fallback | Same. |
| `sqlite3` (stdlib) | yes (always) | Lexicon + memory stores | n/a |

**Install for richer Russian morphology:**

```bash
pip install pymorphy3
```

Once installed, `morph.dependency_status()` will report `pymorphy_available: true`
and `engine: pymorphy3`. Existing callers continue to work unchanged — they
just receive higher-confidence hints with `source: 'pymorphy'`.

## Test Results

```
python test_russian_sovereign_stack.py
============================================================
SUMMARY: 73/73 passed, 0 failed
```

Suite breakdown (11 suites):

| # | Suite | Checks | Result |
|---|-------|--------|--------|
| 1 | Feature flag DISABLED → safe inactive results | 2 | PASS |
| 2 | Router: Cyrillic / mixed / English / explicit-RU-request / user_requested_language | 9 | PASS |
| 3 | Lexicon store: init, add_word, lookup, prefix, contains, tag, synonyms, idioms, hard limits, SQL-injection rejection | 12 | PASS |
| 4 | Morphology: deps, normalize, lemma, POS, case, number, gender, naturalness, bounded notes | 11 | PASS |
| 5 | Memory fabric: init, empty-state, add, search, retrieve, summarize, transliteration, hard limits | 9 | PASS |
| 6 | Personality: profile, 8 modes, invalid-mode fallback, artifact detection, канцелярит, no auto-rewrite | 10 | PASS |
| 7 | Ingestion: preview, word ingest, report file, phrase ingest, topic-pack, batch clamp, missing-file safety | 7 | PASS |
| 8 | Response quality: artifacts, native-feel, clarity, register-fit, bounded rewrites, no auto-rewrite, overall bounded | 8 | PASS |
| 9 | No new threads across 5×3 calls; no recursion blow-up at `setrecursionlimit(300)` | 2 | PASS |
| 10 | After 300 inserts: search_prefix capped, bounded_query capped | 2 | PASS |
| 11 | No forbidden cross-program imports; all 7 production modules present | 2 | PASS |

## Limitations

- **DBs are empty.** Schemas exist; row counts are 0. This is intentional — the foundation is in place but no large import has been run.
- **`pymorphy` not installed in this environment.** All morphology output uses honest heuristic fallbacks with `confidence ≤ 0.30`. Install `pymorphy3` to upgrade.
- **`translate_summary_stub`** is a deterministic ASCII transliteration only — not real translation. No network calls.
- **Russian personality content is intentionally curated and short.** The 8-mode rule set is the seed; expand as Luna's Russian behaviour clarifies, but do not bulk-grow rules without operator review (each rule shapes prompt behaviour).
- **Knowledge-pack ingestion** accepts `jsonl / json / csv / txt`. For `txt` it assumes "one word per line" — adjust the streamer for richer txt formats.
- **`bounded_query` filter allowlist** is intentionally narrow (`word, lemma, part_of_speech, register_level, source`). Extending it requires editing `_ALLOWED_COLS` in `russian_lexicon_store.py`.
- **Phrase search.** Phrase tables are queried via `get_idioms`. A full-text-search index over phrases is not yet built — add `fts5` if you reach > 50K phrases.
- **No live integration with Luna's main runtime.** Calling code must explicitly import the router and decide when to use the stack. The stack does not auto-wire itself.

## Rollback Steps

1. **Soft rollback (recommended):** Unset or set `LUNA_RUSSIAN_STACK=0`. `router.should_use_russian_stack` and `router.route_russian_context` immediately return disabled-safe results. **No code changes required.**
2. **Hard rollback:** Delete the new files / directories:
   ```
   del russian_language_router.py
   del russian_lexicon_store.py
   del russian_morphology_layer.py
   del russian_memory_fabric.py
   del russian_personality_layer.py
   del russian_knowledge_ingestion.py
   del russian_response_quality.py
   del test_russian_sovereign_stack.py
   del RUSSIAN_SOVEREIGN_STACK_REPORT.md
   rmdir /s /q russian_stack
   ```
3. Nothing else needs to be reverted — no service installed, no scheduled task, no registry key, no shared Luna runtime file modified.

## Next Safe Integration Step (Phase 11)

When you're ready to grow Russian coverage toward the near-million-word target:

1. **Phase 11 — Russian Knowledge Pack Importer.** Use the existing `russian_knowledge_ingestion` ingestion path. Source formats already supported: `jsonl`, `json`, `csv`, `txt`. Suggested first import:
   - Download a large open Russian frequency list or a Russian-Wiktionary export.
   - Save as `.jsonl` with one entry per line: `{"word": "...", "lemma": "...", "part_of_speech": "...", "definition_ru": "...", "definition_en": "...", "frequency_score": ..., "register_level": "..."}`
   - Place under a local path, e.g. `D:\SurgeApp\russian_stack\packs\ru_top_50k.jsonl`.
   - Run **preview first**:
     ```python
     import russian_knowledge_ingestion as ingest
     ingest.preview_ingestion("D:/SurgeApp/russian_stack/packs/ru_top_50k.jsonl", limit=25)
     ```
   - Then full ingest (chunked, bounded):
     ```python
     ingest.ingest_word_list(
         "D:/SurgeApp/russian_stack/packs/ru_top_50k.jsonl",
         source="ru_top_50k_v1", batch_size=500,
     )
     ```
   - Read the auto-generated `<path>.ingest_report.json` for added / rejected counts and rejection samples.
2. **Idiom & phrase packs.** Same pattern via `ingest_phrase_list` or `ingest_topic_pack`.
3. **Do NOT bulk-import inside any tier / probe / worker.** Bulk imports must be one-shot operator scripts; the runtime stays read-mostly.
4. **Performance budget.** Up to ~100K rows: existing schema is fine. Beyond that, add an FTS5 virtual table over `(word, lemma, definition_ru, definition_en)` and triggers to keep it in sync.
5. **Install `pymorphy3`** to upgrade lemma / POS / case / number / gender hints from heuristic (confidence ≤ 0.30) to real morphological analysis (`source: 'pymorphy'`, confidence ~0.5–0.95).
6. **Integration with Luna's main prompt builder** — separate session, separate review. The integration must call `router.should_use_russian_stack(prompt, user_requested_language=...)` once per turn and respect the `use_russian` flag. The stack remains advisory; no auto-rewrites are performed by this code.

---

## Final Confirmations

- Russian Sovereign Language Stack foundation **exists** as 7 production modules + 2 SQLite stores + 1 test harness.
- It is **separate from Program S**.
- It **does not touch Program S** (verified by import-statement regex scan in test [11]).
- It **does not touch Luna probes / tiers / attestation** (same scan covers those names).
- It is **controlled by `LUNA_RUSSIAN_STACK=1`** — off => disabled-safe.
- It supports Russian language **routing** (Phase 2).
- It supports Russian **lexicon storage** (Phase 3 — words + phrases + idioms).
- It supports Russian **memory storage** (Phase 5 — isolated, not wired into main memory).
- It supports Russian **morphology hints** (Phase 4 — graceful pymorphy/heuristic split).
- It supports Russian **personality styling** (Phase 6 — 8 modes, advisory only).
- It supports future Russian **knowledge-pack ingestion** (Phase 7 — chunked, bounded, validated, batch-capped).
- It supports bounded Russian **response quality checks** (Phase 8 — never auto-rewrites).
- It **does not create daemons**. Test [9] verifies zero new threads across 15 cross-module calls.
- It **does not create recursive loops**. Test [9] verifies no blow-up at `setrecursionlimit(300)`.
- It **does not load huge word lists into prompts** (HARD_MAX_LIMIT=500 lexicon, =200 memory, =100 morphology, =25 quality; per-result truncation in router; no auto-injection anywhere).
- It is **ready for future near-million Russian lexical/knowledge coverage** through Phase 11's chunked ingestion path.
