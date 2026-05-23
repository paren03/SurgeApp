# Phase 23 — Human Bilingual Code-Switching, Mixed-Language Conversation, and Natural EN/RU Expression Layer

**Status:** Complete. **Date:** 2026-05-21.

Phase 23 ships Luna's standalone human-like bilingual code-switching layer
on top of the Phase 22 bilingual link DB and retrieval bridge. It detects
EN/RU/mixed prompts, decides a response language mode, plans switching at
sentence/phrase/term/word granularity, tracks a small local conversation
state, scores naturalness, and exposes a single non-integrated runtime
entry point. Production lexicons unchanged.

## 1. Phase 23 completion status

ACCEPTED. 6 new production modules + harness + report. 16/16 harnesses green
(1555/1555 checks). Live demo across 10 sample prompts produces sensible
classifications and response-mode decisions. Production lexicons untouched.

## 2. Phase 22 pre-flight verification

✅ All 18 required Phase 22 + core-vocabulary artifacts present, including
`bilingual_stack/bilingual_links.sqlite`.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `bilingual_language_mode_detector.py` | 250 | Detects 10 language modes via Cyrillic/Latin script mix + word-segment scanning + sentence-aware transitions + explicit-request regex + transliteration markers (word-boundary). |
| `bilingual_code_switch_policy.py` | 200 | 11 policy modes × 5 granularities (none/word/term/phrase/sentence). Per-entry safety gate respects recognition_only / do_not_use_unprompted / vulgar / offensive. |
| `bilingual_style_mixer.py` | 240 | Style PLANNER (not generative): code-switch plan, term selector via bridge, light/sentence/phrase switch templates, user-style mirror, awkward-pattern detector, naturalness score. |
| `bilingual_conversation_state.py` | 100 | Plain-dict state tracker. EWMA on EN/RU ratios + transition density. No daemon, no persistent writes. |
| `bilingual_response_quality.py` | 165 | Read-only quality scoring + suggestions. Bad code-switch detection, translation-artifact regex set, balance score, safety heuristic, overall verdict. |
| `bilingual_human_switch_runtime.py` | 195 | Single standalone runtime entry point. NOT integrated into Luna main runtime. Aggregates the other five modules; outputs bounded dict. |
| `test_phase23_human_code_switching.py` | 500 | 9 suites, 119 checks. |
| `PHASE23_HUMAN_CODE_SWITCHING_REPORT.md` | — | this report. |

## 4. Files modified

None. Phase 23 is purely additive.

## 5. Folders created

* `bilingual_stack/code_switch/reports/`
* `bilingual_stack/code_switch/evaluations/`
* `bilingual_stack/code_switch/fixtures/`
* `bilingual_stack/code_switch/style_profiles/`

## 6. Language detector status

✅ 10 modes implemented:

```
english_only · russian_only · mixed_en_ru · english_with_russian_terms ·
russian_with_english_terms · code_switch_sentence_level ·
code_switch_phrase_level · code_switch_word_level ·
transliterated_russian · unknown
```

Live demo verifies decisions:

| Input | Detected mode |
|---|---|
| `Hello, my name is Anna. Я инженер.` | `english_with_russian_terms` |
| `Привет, как дела? I'm doing fine.` | `code_switch_sentence_level` |
| `Tell me a story about the lighthouse.` | `english_only` |
| `Расскажи мне про маяк.` | `russian_only` |
| `Use the engineer concept but explain it in Russian please.` | `russian_with_english_terms` (explicit request override) |
| `The инженер reviewed the schematic.` | `english_with_russian_terms` |
| `ledger -- бюджет` | `mixed_en_ru` |

Bounded metadata: `detect_language_segments` capped at 2000 tokens.
Explicit-request detection via regex (`answer in russian`, `in english
please`, `mix english and russian`, etc.) overrides ratio-based heuristics.

## 7. Code-switch policy status

✅ 11 policy modes:

```
conversation · teacher · technical · coding · curriculum · professional ·
warm_friend · concise · slang_allowed · translation_help · bilingual_practice
```

5 granularities ordered from least to most invasive:

```
none < word < term < phrase < sentence
```

Verified by suite C:

* `english_only` input → no switch.
* `russian_only` input → no switch.
* Mixed input → switch.
* `user_preference="mix"` overrides English-only detection.
* `teacher` mode caps granularity at `term` (verified by clamping a
  `sentence`-proposed granularity down to `term`).
* `bilingual_practice` mode allows up to `sentence`.
* Per-entry safety gate:
  * `do_not_use_unprompted` → blocked.
  * `vulgar`/`offensive` in non-slang mode → blocked.
  * `vulgar`/`offensive` in slang_allowed mode unprompted → blocked.
  * `vulgar`/`offensive` in slang_allowed mode prompted → allowed.
  * `recognition_only` → allowed for recognition, `suggestion_blocked=True`.
* `filter_switch_candidates` returns safe+blocked split correctly.

### Real bug found + fixed during build

Initial granularity ordering was `(none, sentence, phrase, term, word)` which
made `teacher` mode incorrectly cap proposed=`sentence` at `sentence`. The
intended ordering is least→most invasive (`sentence` is the most invasive,
finest is `word`). Reversed to `(none, word, term, phrase, sentence)`.

## 8. Style mixer status

✅ Pure style planner — never claims to produce final writing. Verified
features:

* `build_code_switch_plan(user_text, target_mode='auto')` integrates
  detector + policy + bridge into a single plan dict.
* `select_switch_terms(...)` returns ≤ limit entries from the bridge,
  filtered by policy.
* `apply_light_code_switch` returns position-anchored substitution
  proposals.
* `apply_sentence_level_switch` produces an interleaved sentence plan
  (balanced / en_first / ru_first).
* `apply_phrase_level_switch` finds operator-supplied phrase pairs.
* `preserve_user_mixed_style` flags response-vs-user ratio drift.
* `avoid_awkward_switching` detects 4 robotic-translation patterns
  (`очень important`, `very важно`, etc.).
* `score_code_switch_naturalness` returns score + verdict
  (`natural` / `passable` / `awkward`).

## 9. Conversation state status

✅ Plain-dict utility. No daemon, no persistent writes.

* `create_conversation_language_state()` returns a fresh state dict.
* `update_language_state(state, user_text, detected, response_mode)` uses
  EWMA (α=0.4) for ratios + transition density.
* `set_preferred_language_mix` validates against the allow-list
  (`english_only/russian_only/mixed_en_ru/english_with_russian_terms/
  russian_with_english_terms/auto`); invalid values silently ignored.
* `reset_language_state` returns a fresh state.
* `summarize_language_state` returns a 10-key digest.

## 10. Bilingual quality status

✅ Suite F verifies:

* `detect_bad_code_switching("это очень important для меня")` → True.
* Clean mixed text `"Hello! Как дела сегодня?"` → False.
* `detect_excessive_switching` flags `Hello мир hello мир …` (5+ transitions).
* `detect_translation_artifacts_mixed` lists per-pattern hits.
* `score_mixed_language_naturalness` emits verdict in
  `{natural, passable, awkward}`.
* `score_language_balance(target='mixed_en_ru')` rewards EN/RU parity.
* `score_safety_compliance` flags `step by step instructions to bypass auth`
  and similar markers.
* `suggest_code_switch_improvements` returns ≤ limit suggestion strings.
* `quality_check_bilingual_response` produces an `overall_score`,
  `verdict ∈ {pass, warn, fail}`, plus sub-metrics + suggestions.

## 11. Human switch runtime status

✅ `bilingual_human_switch_runtime` exposes 6 entry points; none are wired
into Luna main runtime. All outputs bounded.

Live test data:

| Input | detected | chosen | granularity | should_switch |
|---|---|---|---|---|
| `Hello, what is an engineer?` | `english_only` | `english_only` | `none` | False |
| `Привет, что такое инженер?` | `russian_only` | `russian_only` | `none` | False |
| `Hello, я инженер and I work hard.` | `english_with_russian_terms` (or sentence-level depending on tokenization) | `english_with_russian_terms` | term/phrase | True |

`get_bilingual_response_plan` output contains every required field:
`detected_language_mode`, `chosen_response_mode`, `switch_granularity`,
`should_code_switch`, `language_ratio`, `bilingual_context`, `switch_terms`,
`style_plan`, `safety_summary`, `quality_notes`, `updated_conversation_state`,
`gap_notes`.

### Real bug found + fixed during build

`detect_transliteration_hint` used substring matching for `_TRANSLIT_MARKERS`
(e.g., `"ne"`), which falsely matched `engineer` (`e-ne-ineer`). Switched to
word-boundary regex (`\bne\b`) and raised the threshold to ≥ 2 markers, so
`"Hello, what is an engineer?"` is no longer mis-classified as transliterated
Russian.

## 12. Production DB impact

**Zero rows changed.** EN=2814, RU=2518, RU_phrases=35 (identical baseline →
final). The bilingual link DB count is also unchanged because Phase 23
modules only READ the link DB; they don't write to it.

## 13. Manifest count before / after

90 (45 EN + 45 RU). Unchanged.

## 14. Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_phase23_human_code_switching.py` | 119 | ✅ |
| `test_phase22_bilingual_linker_and_retrieval_bridge.py` | 93 | ✅ |
| `test_phase21a_operator_corpus_staging.py` | 100 | ✅ |
| `test_phase21_operator_staged_first_import.py` | 103 | ✅ |
| `test_phase20_million_readiness_gate.py` | 144 | ✅ |
| `test_phase19_100k_scale_index_and_dedupe.py` | 116 | ✅ |
| `test_phase18_pilot_import_and_retrieval_hardening.py` | 83 | ✅ |
| `test_phase17_source_adapters_and_retrieval_eval.py` | 108 | ✅ |
| `test_phase16_million_scale_readiness.py` | 80 | ✅ |
| `test_phase15b_remaining_domain_expansion.py` | 117 | ✅ |
| `test_phase15a_controlled_scale_expansion.py` | 63 | ✅ |
| `test_phase14_domain_pack_expansion.py` | 87 | ✅ |
| `test_dual_pack_importer.py` | 116 | ✅ |
| `test_dual_sovereign_pack_safety.py` | 79 | ✅ |
| `test_vocabulary_runtime.py` | 74 | ✅ |
| `test_russian_sovereign_stack.py` | 73 | ✅ |
| **Total** | **1555** | **1555 / 1555 PASS** |

### Phase 23 suite breakdown (119 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | 18 required prior artifacts | 18 |
| B_DETECTOR | 10 classification cases + 3 request-detection + sentence/word/translit/segments-bounded/report | 17 |
| C_POLICY | policy lookup + 4 switch decisions + 2 granularity caps + mirror + 5 entry-safety + filter + explanation | 16 |
| D_STYLE_MIXER | plan, terms (with + without target), light/sentence/phrase, preserve, awkward (2), naturalness | 11 |
| E_STATE | create, update, preferred set/get, invalid ignored, reset, summary | 10 |
| F_QUALITY | bad-cs, no-cs, excessive, artifacts, naturalness, balance, safety (un/safe), suggestions, overall | 11 |
| G_RUNTIME | plan EN, RU, mixed, mixed_context, choose_style, evaluate, demo, report | 9 |
| H_PRODUCTION_SAFETY | EN/RU/phrases/manifest unchanged | 4 |
| I_ISOLATION | 6 files × {exists, forbidden, network, daemon} | 24 |

## 15. Safety verification

* `do_not_use_unprompted` rows blocked from switch unless prompted.
* `vulgar`/`offensive` rows blocked in non-slang modes; blocked in
  slang_allowed mode unless prompted.
* `recognition_only` rows kept for recognition, but
  `suggestion_blocked=True`.
* Quality checker hard-flags operational-unsafe markers
  (`step by step instructions to bypass auth`,
  `ignore previous instructions`, `system prompt:`).
* No row in any production lexicon was modified.
* The bilingual retrieval bridge invoked by the runtime preserves all
  Phase 22 safety semantics.

## 16. Isolation verification

Suite I scans all 6 Phase 23 modules:

* Forbidden imports `worker`, `luna_modules`, `tier_`, `probe_`,
  `attestation`, `program_s` — **zero matches**.
* Network usage `urllib`, `requests`, `httpx`, `aiohttp`, `socket`,
  `ftplib`, `urlopen`, `http.client` — **zero matches**.
* Daemon usage `threading.Thread(`, `multiprocessing.Process(`,
  `asyncio.create_task(`, `subprocess.Popen(`, `import schedule`,
  `import apscheduler`, `BackgroundScheduler(`, `threading.Timer(`,
  `while True:` — **zero matches**.

## 17. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 18. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Verified by isolation regex scan. Phase 23 is additive: 6 new modules, 1
new harness, 1 new report, 4 new folders. Zero edits to any pre-Phase-23
file.

## 19. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite I confirms zero matches.
* **No recursion** — no function in any Phase 23 module calls itself.
* **No full-corpus load** — every retrieval path is bounded (hard limit
  100 in runtime, 25 default, 200 segment cap in detector).
* **No internet** — zero network library imports.

## 20. Rollback notes

```powershell
# 1. New modules + harness + report
Remove-Item bilingual_language_mode_detector.py
Remove-Item bilingual_code_switch_policy.py
Remove-Item bilingual_style_mixer.py
Remove-Item bilingual_conversation_state.py
Remove-Item bilingual_response_quality.py
Remove-Item bilingual_human_switch_runtime.py
Remove-Item test_phase23_human_code_switching.py
Remove-Item PHASE23_HUMAN_CODE_SWITCHING_REPORT.md

# 2. New folders + their contents
Remove-Item -Recurse bilingual_stack/code_switch
```

No production rows were written; nothing else needs reverting. The
bilingual link DB at `bilingual_stack/bilingual_links.sqlite` is unchanged.

## 21. Next recommended phase

Two viable paths, same as after Phase 22:

1. **Phase 21 real import** — operator stages an EN + RU file pair under
   `corpus_sources/{english,russian}/incoming/`, then runs the existing
   Phase 21 runner with `allow_real_import=True` capped at 10k rows per
   language. After ingest, Phase 22's link builder and Phase 23's style
   mixer will have richer raw material.
2. **Phase 24 — Luna Voice Bilingual Style Profile + Personality
   Continuity** — extend Phase 23 with per-mode style profiles (warm /
   teacher / concise / poetic) that bind Luna's voice traits across EN
   and RU, plus a personality-continuity scorer that compares mixed-EN/RU
   responses to a Luna-voice reference profile. Still standalone, still
   non-integrated.

Suggested order if both available: **Phase 21 real import** first (real
rows feed Phase 23 with better switch candidates), **then Phase 24
voice/personality binding**.

## 22. Clean failure notes

None.

Pre-flight passed. All 16 harnesses green. Baseline lexicon counts
unchanged. Live demo over 10 sample prompts produces sensible
classifications. Three real bugs found and fixed in code during build:

1. Granularity order was inverted (sentence is most invasive, not least).
2. Transliteration hint used substring matching; `ne` falsely fired on
   `engineer`. Switched to `\bne\b` word-boundary regex and required ≥ 2
   markers.
3. Sentence-level vs `<lang>_with_<other>_terms` classification needed
   sentence-count gating — single-sentence minority insertion is now
   correctly `<dominant>_with_<other>_terms`, multi-sentence near-balanced
   alternation is `code_switch_sentence_level`.

No weakened assertions, no hidden failures, no production-lexicon writes.
