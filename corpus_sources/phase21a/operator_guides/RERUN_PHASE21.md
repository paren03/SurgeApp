# Re-running Phase 21 after staging files

After files are placed in `corpus_sources/english/incoming/` and `corpus_sources/russian/incoming/`, follow this sequence:

1. `python test_phase21a_operator_corpus_staging.py` (validates files + writes acceptance/repair previews)
2. Read the latest report under `corpus_sources/phase21a/ready_reports/`. State must be `READY_FOR_PHASE21_REAL_IMPORT` or `READY_FOR_DRY_RUN_ONLY`.
3. `python test_phase21_operator_staged_first_import.py`
4. If the state is `READY_FOR_PHASE21_REAL_IMPORT`, run the guarded import explicitly:

```python
import phase21_operator_stage_runner as r
r.setup_phase21_folders()
sources = r.discover_operator_staged_sources()
enriched = r.register_phase21_sources(sources)
gated   = r.run_phase21_quality_gates(enriched)
plan    = r.build_phase21_stage_plan(gated, max_total_per_language=10000)
snap    = r.create_phase21_backup_snapshot(label='first_real')
dr      = r.run_phase21_dry_runs(plan)
rr      = r.run_phase21_real_import(plan, allow_real_import=True, quality_reports=gated, dry_run_reports=dr, backup_snapshot_id=snap['snapshot_id'])
```

5. Inspect `rr['results']` and the post-import audits under `corpus_sources/phase21/`.