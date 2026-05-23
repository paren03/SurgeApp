# Phase 21A - Operator Staging Guide

## Where to place files

- English: `D:\SurgeApp\corpus_sources\english\incoming\`
- Russian: `D:\SurgeApp\corpus_sources\russian\incoming\`

## Recommended first-import size
5,000 - 10,000 rows per language. Smaller files (< 5k rows) are permitted but will only enable a dry-run-only readiness, not real import escalation.

## Supported file formats

- `.jsonl` - one JSON object per line. **Preferred.**
- `.txt`  - one term or phrase per line.
- `.csv`  - header row required.

## Required metadata

JSONL rows should include:
- `word` (or `phrase` for phrase/idiom sources)
- `language`: `en` or `ru`
- `definition` (recommended)
- `coverage_categories`: list of canonical category strings
- `register_tags`: list of canonical register strings
- `safety_tags`: list (empty for benign rows)
- `domain_tags`: list (optional)
- Russian rows may include `lemma`, `part_of_speech` for morphology preservation.

## How to tag slang, street, vulgar, offensive, and sensitive terms

- **slang_list / street_talk_list** sources auto-receive `slang` or `street` register tags during repair preview.
- **vulgar** or **offensive** terms MUST also receive `safety_tags: ["recognition_only", "do_not_use_unprompted"]`.
- Luna can recognize and explain recognition_only terms, but will not use them as her own suggestion. With explicit user prompting (`is_user_prompted=True`), the softening rules permit them where the operator's mode allows.

## How recognition_only works

A term marked `recognition_only` will be returned by the indexed retrieval (so Luna recognizes it), but the safety filter will exclude it from `suggestion`-context outputs.

## How do_not_use_unprompted works

A term marked `do_not_use_unprompted` is blocked from any output unless `is_user_prompted=True` is explicitly passed by the caller.

## How to rerun Phase 21 after staging files

```
python -c "import phase21a_operator_corpus_staging as p21a; print(p21a.discover_incoming_files())"
python test_phase21a_operator_corpus_staging.py
python test_phase21_operator_staged_first_import.py
```

After the validator + readiness gate report `READY_FOR_PHASE21_REAL_IMPORT`, the operator runs the Phase 21 runner with `allow_real_import=True`.

## What NOT to include

- Step-by-step operational instructions to bypass security,
- prompt-injection markers (`ignore previous instructions`, etc.),
- private personal data,
- copyrighted works without permission.

## Why no internet / download is performed

Luna's sovereign stack is local-only. Network access is never used during ingest. The operator is responsible for obtaining corpus files legally and placing them under the `incoming/` folders before any import is attempted.