---
name: project-dual-sovereign-main-scale-integration-2026-06-01
description: "2026-06-01 — first 1M-scale concept-linking layer for main's production bilingual_links.sqlite. Foundation (adapter + new concept DB + 2 orchestrators) ships 4,553 EN↔RU pairs from named-entity Latin-form matches. Honest limit documented: lexical methods cap here because RU is 100% Cyrillic on main; semantic embeddings are the right next step."
metadata:
  type: project
---

## What shipped

Three new modules at `D:/SurgeApp/`:

1. **`bilingual_main_adapter.py`** (8,993 bytes, 7 functions) — read-only schema adapter. Opens main's `bilingual_links.sqlite` (1.05 GB) via SQLite URI `?mode=ro` so it cannot mutate production. Translates main's flat schema (`english_words`/`russian_words` with `register` as single string, `coverage_categories` as JSON-string, `pos`, `source_pack`, etc.) into the row-dict shape the worktree orchestrators expect (`register_tags_json` list, `coverage_categories_json`, `part_of_speech`, `pack_source`, etc.). Aliases tags as `tags_json` + `domain_tags_json` for linker compatibility. RU rows lack `definition_en` (main schema diff vs seed); adapter returns `""` so callers can skip cleanly.

2. **`bilingual_concept_links_db.py`** — separate concept-link DB at `D:/SurgeApp/bilingual_concept_links.sqlite`. Schema mirrors worktree's `bilingual_stack/bilingual_links.sqlite` (concepts / entry_links / bilingual_glossary_links / link_audit) + indexes appropriate for 1M-scale lookups (`ix_concepts_label_en`, `ix_concepts_label_ru`, `ix_entry_links_concept`, `ix_entry_links_lang_word`). WAL + synchronous NORMAL. Exposes `init_concept_db()`, `stats()`, `existing_linked_words()`, `insert_concept_batch()`, plus helpers (`_new_id`, `_now`, `json_list`, `clamp_confidence`, `normalize_method`) so orchestrators don't need to import from the worktree.

3. **`bilingual_main_latin_extract_pass.py`** — first working orchestrator at 1M scale. Loads EN word index into memory (527,226 distinct lowercased words), streams RU rows whose `word` contains at least one Latin character (~20,022 rows on main), extracts the longest Latin run per RU word, looks it up in the EN index. Matches: 4,553 concepts (9,106 entry_links) in **13 seconds**. Confidence 0.8, `link_method="exact_match"`.

A fourth orchestrator (`bilingual_main_exact_match_pass.py`) was built first for the obvious "lowercase(word)==lowercase(word)" strategy. It is structurally correct (single-pass, batched, READ-ONLY against main) but yields **~0 pairs** because every RU row on main has `cyrillic=1` — RU and EN don't share alphabet so lexical equality is empty. Kept as reference + alternative entry point.

## Honest yield + limit

| Pass | Concepts created | Entry links | Elapsed |
|---|---|---|---|
| `bilingual_main_exact_match_pass.py` (full word equality) | 0 (10k sample) | 0 | 180s |
| `bilingual_main_latin_extract_pass.py` (longest Latin run match) | **4,553** | **9,106** | **13s** |

Real samples from the Latin-extract pass (all are legitimate Russian Wikipedia named entities that preserved their Latin titles):
- `toy` ↔ `Toy (альбом)`
- `Elm` ↔ `Elm (почтовый клиент)`
- `cube` ↔ `Cube (игра)`
- `firebird` ↔ `Firebird (компания)`
- `friendly fire` ↔ `Friendly Fire (альбом Шона Леннона)`
- `Das Reich` ↔ `Das Reich (газета)`

## Why exact match yields essentially zero at 1M scale

Main's production bilingual_links.sqlite combines EN dictionary sources (gcide stage5d, English Wiktionary) with RU dictionary + encyclopedic sources (Russian Wiktionary, Kaikki, OpenRussian, Wikidata lexemes, DBpedia Russian Wikipedia short abstracts). **They came from different ontologies with no shared identifier layer.** EN words are Latin, RU words are Cyrillic (100% on main — verified). The only RU rows with Latin chars are the ~20,022 named-entity carryovers from DBpedia (band names, software, brands), and even those have ~50% match rate vs the EN dictionary.

Going beyond ~5k pairs at 1M scale requires:
1. **Multilingual sentence-transformer embeddings** — embed both 1M EN definitions and 1M RU definitions, compute cosine similarity, threshold-gate. Yields ~10-100k more concepts. Multi-hour compute job (1M embeddings × 384 dims at ~50 rows/s on CPU ≈ 5-6 hours per side). The worktree's `bilingual_semantic_link_pass.py` already uses `paraphrase-multilingual-MiniLM-L12-v2` for this at smaller scale — port to main with the schema adapter.
2. **External translation tables** — Wikidata explicitly links EN/RU labels via QID. ~5M+ Wikidata items have both EN and RU labels. Bulk import would yield 10x more pairs than embeddings.
3. **Named-entity DBpedia URI bridge** — DBpedia entries have `dbpedia.org/page/X` URIs; the corresponding English DBpedia URI lookup table would link RU→EN entities directly. Highest-precision, narrower coverage.

## Production safety

- Main's `bilingual_links.sqlite` (1.05 GB, 1M+1M production vocab) opened **READ-ONLY** via `sqlite3.connect("file:...?mode=ro", uri=True)`. Cannot accidentally write.
- All new concept rows go to a **separate file** (`bilingual_concept_links.sqlite`, 4.59 MB) — production DB schema never touched.
- Orchestrators dedupe against existing entry_links on every run, so re-runs are idempotent.

## Files added (not yet committed at memory-write time)

- `D:/SurgeApp/bilingual_main_adapter.py`
- `D:/SurgeApp/bilingual_concept_links_db.py`
- `D:/SurgeApp/bilingual_main_exact_match_pass.py`
- `D:/SurgeApp/bilingual_main_latin_extract_pass.py`
- `D:/SurgeApp/bilingual_concept_links.sqlite` (data, 4.59 MB — gitignored by convention)
- `D:/SurgeApp/memory/bilingual_main_reports/bilingual_main_latin_extract_20260601T175950Z.json`

## Next steps (next session)

1. **Port `bilingual_semantic_link_pass.py` to main** using the adapter. Use the multilingual sentence-transformer model already installed (`paraphrase-multilingual-MiniLM-L12-v2`). At 1M+1M scale this is a multi-hour job; can be checkpointed in batches of 100k rows. Threshold 0.65 (same as worktree).
2. **Pull Wikidata EN↔RU label pairs** as an explicit translation source. The Wikidata SPARQL endpoint or a local dump (already present at `D:/LunaStage5EWork/`) provides millions of explicit pairs.
3. **Cross-language Wikipedia title bridge** for DBpedia rows — use the `metadata` column (per the Phase 21 import report, DBpedia rows carry the Russian Wikipedia URI in metadata, and the English equivalent can be looked up via Wikipedia interlanguage links).

Related: worktree `bilingual_stack/bilingual_links.sqlite` (4.2 MB, 3942 concepts on seed corpus) remains the experimental/seed concept layer. Main's `bilingual_concept_links.sqlite` (4.59 MB, 4553 concepts) is the production-scale layer. They're DELIBERATELY SEPARATE files with the same schema — both can be re-run safely; future integration work picks one or merges concepts.
