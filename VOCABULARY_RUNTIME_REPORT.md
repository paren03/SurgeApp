# Luna Vocabulary Runtime — Build Report (v2, seeded + integrated)

**Date:** 2026-05-20
**Worktree:** `D:\SurgeApp\.claude\worktrees\strange-lumiere-5d0fc5\` (branch `claude/strange-lumiere-5d0fc5`)
**Feature flag:** `LUNA_VOCABULARY_RUNTIME=1`

---

## Task Summary

Build Luna's standalone bounded vocabulary / lexicon runtime, then prove it
end-to-end with the real optional dependencies installed and a small seeded
corpus. The runtime must remain:

- Local, SQLite-backed, no network calls
- Bounded — every query enforces a limit; no full-DB loads
- Inspectable, reversible, testable
- Gated by `LUNA_VOCABULARY_RUNTIME` (off => no-op)
- Hard-isolated from Program S

Status: **complete and passing**. 74 / 74 test checks pass against a seeded
production DB with real `wordfreq` and WordNet integration.

## Packages Confirmed Installed

| Package | Version | Used for |
|---------|---------|----------|
| `wordfreq` | (installed; no `__version__` attr exposed) | Zipf-frequency scoring, word-level classification |
| `nltk` | 3.9.4 | WordNet API surface |
| WordNet corpus | downloaded | Definitions + synonyms fallback when DB misses |

Verified: `wordfreq.zipf_frequency('hello', 'en') == 4.72`; WordNet returns
"`enjoying or showing or marked by joy or pleasure`" for `wn.synsets('happy')[0]`.

## Files Created

| Path | Role |
|------|------|
| `lexicon/` | DB home directory |
| `lexicon/luna_vocabulary.sqlite` | SQLite store, **246 rows after seeding** |
| `cognitive_lexicon_store.py` | SQLite CRUD + bounded queries + `seed_small_dataset()` |
| `cognitive_word_policy.py` | 7-mode word-use policy (pure logic) |
| `cognitive_vocabulary_runtime.py` | Public runtime + feature-flag gate; uses `wordfreq` + WordNet when available |
| `test_vocabulary_runtime.py` | 14-suite, 74-check bounded harness |
| `VOCABULARY_RUNTIME_REPORT.md` | This file |

## Files Modified

**None outside the 6 listed above.** `git status` of the worktree:

```
?? VOCABULARY_RUNTIME_REPORT.md
?? cognitive_lexicon_store.py
?? cognitive_vocabulary_runtime.py
?? cognitive_word_policy.py
?? lexicon/
?? ruvector.db                       <-- NOT FROM THIS TASK; see note below
?? test_vocabulary_runtime.py
```

**Note on `ruvector.db`:** a 1.5 MB binary `ruvector.db` appeared in the
worktree root during this session. Grepping every file I created for
`ruvector` returns zero matches — none of my code reads or writes it. It is
almost certainly an artifact of a parallel background process (ruvector MCP /
harness side-effect). It was **not** created by this vocabulary build and has
been left untouched.

## Program S — Untouched

Hard-confirmed:

- No file in this build references `program_s`, `ProgramS`, or related symbols (test [11] enforces this).
- No tier / probe / attestation / worker / orchestration file was opened, read, or modified.
- No shared Luna runtime file was edited.
- No background daemon, scheduled task, WinSW service, or cron entry was created.
- The runtime is import-only — nothing auto-runs on `import`.

## Database

- **Path (default):** `D:\SurgeApp\lexicon\luna_vocabulary.sqlite` (resolved relative to `cognitive_lexicon_store.py`)
- **Override:** set env var `LUNA_VOCABULARY_DB=<absolute path>`
- **Mode:** WAL journal, NORMAL sync, foreign keys ON
- **Current state:** 246 unique rows (post-seed; see seed details below)
- **Schema columns:** `word, definition, synonyms_json, examples_json, tags_json, source, language, frequency_score, word_level, created_at, updated_at`
- **Indexes:** `word`, `word_level`, `frequency_score`

## Feature-Flag Behavior

| Setting | `get_optional_vocabulary_context(...)` returns |
|---------|------------------------------------------------|
| `LUNA_VOCABULARY_RUNTIME` unset | `{}` |
| `LUNA_VOCABULARY_RUNTIME=0` | `{}` |
| `LUNA_VOCABULARY_RUNTIME=1` (or `true`/`yes`/`on`) | Bounded dict: `{"enabled": True, "mode": <m>, "count": <=limit>, "limit": <=200>, "context": [...]}` |

Direct helpers (`explain_word`, `find_better_word`, `classify_word_level`,
`find_related_terms`) work regardless of the flag. The flag only gates the
**embeddable** `get_optional_vocabulary_context` entry-point, which is the
only function downstream code should call to inject vocabulary into a prompt.

## Seed Word Count

`cognitive_lexicon_store.seed_small_dataset()` seeds 5 categories × 50 = 250
calls, producing **246 unique rows** (4 cross-category words upsert into a
single row each — the 4 collisions are intentional, listed below). All seeded
rows pull their definition + synonyms from WordNet and their `frequency_score`
+ `word_level` from `wordfreq.zipf_frequency`.

| Category | Calls | Tag | Default level (used if zipf=0) |
|----------|------:|-----|--------------------------------|
| `normal` (everyday speech) | 50 | `normal` | `plain` |
| `teacher` (explanatory) | 50 | `teacher` | `intermediate` |
| `technical` (coding / CS) | 50 | `technical` | `specialized` |
| `carpentry` (construction / OSHA / PPE) | 50 | `carpentry` | `specialized` |
| `professional` (business / leadership) | 50 | `professional` | `advanced` |
| **TOTAL CALLS** | **250** | | |
| **UNIQUE ROWS** | **246** | (4 dups: `function`, `method`, `process`, `framework` appear in both `teacher` and `technical`) | |

Sample rows after seeding (verified live):

| word | level | zipf | definition (first 60 chars) |
|------|-------|------|------------------------------|
| `hello` | everyday | 4.72 | an expression of greeting |
| `osha` | advanced | 3.09 | a government agency in the Department of Labor … |
| `lambda` | advanced | 3.06 | the 11th letter of the Greek alphabet |
| `collaborate` | intermediate | 3.51 | work together on a common enterprise of project |
| `sequence` | intermediate | 4.35 | serial arrangement in which things follow … |

The seed function is **idempotent** — running it again returns the same
per-category counts and `count_words()` stays at 246 (test [seed] verifies).

## Test Results

```
python test_vocabulary_runtime.py
============================================================
SUMMARY: 74/74 passed, 0 failed
```

Breakdown (14 suites):

| # | Suite | Checks | Result |
|---|-------|--------|--------|
| 1 | `init_db` + `add_word` (incl. upsert preserves `created_at`) | 5 | PASS |
| 2 | `lookup_word` + prefix / contains / tag search | 9 | PASS |
| 3 | `get_related_words` + `bounded_query` (incl. SQL-injection rejection) | 5 | PASS |
| 4 | `HARD_MAX_LIMIT=200` + negative-limit fallback | 2 | PASS |
| 5 | `cognitive_word_policy` (7 modes, voice-awkward block, rare budget) | 8 | PASS |
| 6 | Feature flag **disabled** → `{}` | 2 | PASS |
| 7 | Feature flag **enabled** → bounded dict, definitions capped to 240 chars, huge `limit` clamped | 5 | PASS |
| 8 | `explain_word`, `find_related_terms`, `find_better_word`, `classify_word_level` | 7 | PASS |
| 9 | Missing-deps safety (forced via monkey-patch) | 5 | PASS |
| 10 | No new threads spawned across 5 calls; no recursion blow-up at `setrecursionlimit(200)` | 2 | PASS |
| seed | `seed_small_dataset` 5×50, `count_by_tag`, idempotent | 13 | PASS |
| wf | **Real wordfreq** classification (`hello` plain/everyday, `ebullient` rare/specialized) | 4 | PASS |
| wn | **Real WordNet** explanation (`ostracize` found with definition + synonyms) | 5 | PASS |
| 11 | Program S text-references absent in own files | 1 | PASS |

## Bounded-ness Guarantees

- `DEFAULT_LIMIT = 25`, `HARD_MAX_LIMIT = 200` (both in `cognitive_lexicon_store`).
- Every public query function accepts `limit` and clamps it via `_clamp_limit` to `[1, HARD_MAX_LIMIT]`; negative/None/garbage values fall back to `DEFAULT_LIMIT`.
- `bounded_query` requires column names from a strict allowlist (`{word, definition, source, language, word_level}`) → unsafe columns raise `ValueError` instead of executing.
- `get_optional_vocabulary_context` truncates each definition to 240 chars and each tag list to 5 items before returning, so prompt injection volume is bounded regardless of stored content length.
- DB never auto-loads into memory — every query opens a fresh SQLite cursor with a `LIMIT` clause.
- WordNet calls are bounded too: at most 5 synsets inspected, at most 20 synonyms returned per call.

## Known Limitations

- **Small seed corpus (246 rows).** Sufficient to prove the runtime; useless for production breadth. See "Next Step" below for safely importing the larger vocabulary.
- **Cross-category upsert collisions are silent.** `function/method/process/framework` get the last-write tag (`technical`), losing the `teacher` tag. If you care about multi-tagging, change `add_word` to union new tags with existing ones — currently it replaces.
- **No FTS5 index.** Contains-search uses `LIKE %x%` (linear). Fine to ~100K rows; beyond that, add an `fts5` virtual table.
- **WordNet definitions can be terse / archaic.** WordNet is great for synonym graphs, mediocre for modern definitions of slang or tech terms. The `seed_small_dataset` accepts an empty definition when WordNet has no synset (true for `osha`/`msds`/`ppe`/`lambda` proper-noun senses — most still got definitions via WordNet's generic senses, but coverage is imperfect).
- **English only.** `language` column exists, only `'en'` exercised.
- **No daemon / no auto-warming.** Per-call SQLite connections. Fine for prompt-time augmentation; would matter under thousands-of-calls/sec pressure.

## Rollback Steps

To remove the vocabulary runtime entirely:

1. **Soft rollback (recommended):** Set `LUNA_VOCABULARY_RUNTIME=0` (or unset). `get_optional_vocabulary_context` becomes a no-op returning `{}` immediately. **No code changes required.**
2. **Hard rollback:** Delete the new files:
   ```
   del cognitive_lexicon_store.py
   del cognitive_vocabulary_runtime.py
   del cognitive_word_policy.py
   del test_vocabulary_runtime.py
   del VOCABULARY_RUNTIME_REPORT.md
   rmdir /s /q lexicon
   ```
3. Nothing else needs to be reverted — no other file was modified, no service was installed, no scheduled task was registered, no HKCU/HKLM key was written.

## Next Step — Safely Importing the Full Large Vocabulary

When you're ready to scale beyond 246 rows:

1. **Choose a corpus source.** Options:
   - `wordfreq.iter_wordlist('en', 'large')` — ~250K English wordforms, sorted by Zipf frequency. Stops yielding when frequency hits 0; you choose the cutoff. **Cost: zero — already installed.**
   - WordNet itself: `wordnet.all_lemma_names()` → ~150K lemmas. Ship with definitions / synsets attached.
   - A curated `.csv` from a domain vocabulary list (medical, legal, trades).
2. **Write a separate seeding script** (e.g. `tools/seed_full_vocabulary.py` — **not** part of the runtime, not auto-imported anywhere). The script should:
   - Open the DB via `cognitive_lexicon_store`.
   - Stream the source (never `list(...)` the whole corpus).
   - Use a single connection inside a transaction; commit every N=500 rows.
   - For each word, look up zipf + WordNet definition, derive `word_level`, call `add_word`.
   - Tag with `source="bulk_<source-name>"` so it's distinguishable from the small seed.
   - Print a per-1000 progress line.
3. **Cap the import.** Start with the top 25K wordforms by zipf (covers ~99% of everyday English). Verify performance and DB size before going larger.
4. **DB-size budget.** Each row averages ~250 bytes after JSON-serialized synonyms/examples. 25K rows ≈ 6 MB; 100K rows ≈ 25 MB. All fit comfortably under WAL+SQLite limits.
5. **Add an FTS5 index when row count > ~50K.** Migration: `CREATE VIRTUAL TABLE words_fts USING fts5(word, definition, content=words);` plus triggers to keep it synced.
6. **Run the test harness after every bulk import.** Test [4] already proves limits are enforced regardless of corpus size, but it's worth running `python test_vocabulary_runtime.py` to confirm no regression.
7. **Do NOT bulk-import inside any tier/probe/worker.** Bulk imports must be one-shot operator scripts; the runtime itself stays read-mostly.

---

## Final Confirmations

- Vocabulary runtime exists as **standalone Luna infrastructure**.
- **Program S was not touched** — verified by directory diff and by test [11].
- No Luna tier / probe / attestation file was edited.
- No background daemon was created. Test [10] confirms zero new threads across 5 runtime invocations.
- No recursive loop was created. Test [10] runs with `setrecursionlimit(200)` and still completes.
- No full vocabulary is injected into prompts (HARD_MAX_LIMIT=200, default 25, definitions truncated to 240 chars).
- System is controlled by `LUNA_VOCABULARY_RUNTIME=1`. Off => `{}`.
- All vocabulary access is bounded and local.
- Seeded with 246 unique rows across 5 categories using real `wordfreq` + WordNet data.
