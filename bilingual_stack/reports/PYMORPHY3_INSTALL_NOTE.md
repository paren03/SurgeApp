# Russian Morphology Upgrade Note

Current backend is the **heuristic fallback** in
`russian_morphology_layer.py`. To enable richer lemma / POS
behavior, the operator may run:

```
pip install pymorphy3
```

Luna will NOT auto-install. After install, restart any Luna
process and rerun:

```
python -c "import russian_morphology_upgrade_path as m; print(m.detect_morphology_backend())"
```

## What changes after install

- `detect_morphology_backend()` reports `active_backend='pymorphy3'`.
- Phase 22 link builder will gain higher confidence on lemma_match
  links.
- Russian morphology row audits gain more accurate POS suggestions.

## What does NOT change

- Production lexicon rows are NOT auto-rewritten.
- Existing English/Russian DBs remain untouched.
- Safety policy is unchanged.