# Phase 21 - Operator Staging Required

Status: **WAITING_FOR_OPERATOR**.

Phase 21 infrastructure is ready (runner, harness, folders, report writers). No real local corpus files are present, so **no real import occurred and production DBs were not modified**.

## What is missing
- English files in `corpus_sources/english/incoming/`: MISSING
- Russian files in `corpus_sources/russian/incoming/`: MISSING

## Where to place files
- English: `D:\SurgeApp\corpus_sources\english\incoming\`
- Russian: `D:\SurgeApp\corpus_sources\russian\incoming\`

## Supported file types
- `.jsonl` (Luna-canonical or wiktextract-style)
- `.txt`  (one word per line; or `word freq` for frequency lists)
- `.csv`  (domain terms / profession_job / bilingual_glossary / russian_morphology)

## Required metadata in JSONL rows
- `word` (required)
- `language` (`en` or `ru`)
- `definition` (recommended)
- `coverage_categories` (recommended, list)
- `register_tags` (recommended, list)
- `safety_tags` (optional, list - empty if benign)

## Example file names
- `english_general_5k.jsonl`
- `russian_general_5k.jsonl`
- `english_idioms_500.txt`
- `russian_morphology_1k.csv`

## Re-run after staging
```
python -c "import phase21_operator_stage_runner as p; print(p.discover_operator_staged_sources())"
python test_phase21_operator_staged_first_import.py
```

## Production DB confirmation
Production lexicon counts are unchanged. Run the runner's preflight + discovery functions to verify before any real ingestion attempt.