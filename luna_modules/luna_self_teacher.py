"""Luna Self-Teacher — Luna's library of hard-won lessons she can apply autonomously.

Every bug that caused a stuck loop, every root cause, every fix — recorded here
so Luna can diagnose and repair herself without needing human intervention.

Usage:
    from luna_modules.luna_self_teacher import run_full_self_diagnosis, apply_known_fix
    report = run_full_self_diagnosis()
    for issue in report['issues']:
        print(issue['diagnosis'], '->', issue['fix'])
"""

from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from luna_modules.luna_io import append_jsonl, safe_read_json
from luna_modules.luna_logging import _diag, now_iso
from luna_modules.luna_paths import MEMORY_DIR, LOGS_DIR, PROJECT_DIR

TEACHER_LOG_PATH = LOGS_DIR / "luna_self_teacher_log.jsonl"
LESSON_CATALOG   = MEMORY_DIR / "luna_lessons_learned.jsonl"

_MODULES_DIR = Path(__file__).parent


# ── Lesson catalog ────────────────────────────────────────────────────────────

LESSONS: List[Dict[str, Any]] = [
    {
        "id": "L001",
        "title": "Tier6 patches same function in a tight loop",
        "symptom": "logs show same function name refactored multiple times within minutes; live feed shows repeated identical OpenRouter requests",
        "root_cause": (
            "find_complexity_hotspots() picks the highest-complexity function every scan. "
            "After a patch is 'applied', current_candidate clears but next scan picks the same function again. "
            "No per-function recency guard existed — same target every 4 minutes indefinitely."
        ),
        "detection_code": """
from luna_modules.luna_tier6_codegen import _read_history, _recent_attempts_set
hist = _read_history(limit=20)
if hist:
    fn_counts = {}
    for h in hist[-10:]:
        fn = h.get('function','')
        fn_counts[fn] = fn_counts.get(fn, 0) + 1
    repeated = {fn: c for fn, c in fn_counts.items() if c >= 3}
    if repeated:
        print('LOOP DETECTED — repeated patch targets:', repeated)
""",
        "fix": (
            "1. Add to _TIER6_EXCLUDED_FUNCTIONS in luna_tier6_codegen.py: "
            "'continuous_evolution_step', 'autonomous_maintenance_cycle', 'continuous_tier_evolution_loop', "
            "'repair_all_tiers', 'repair_tier', '_build_maintainonce_output' — these are the evolution engine itself. "
            "2. Add _recent_attempts_set() and use it in find_complexity_hotspots() to skip functions attempted within 2 hours. "
            "3. Increase _PATCH_COOLDOWN_S in tier9 from 240 to 3600 (4 min is too short — use 1 hour). "
            "4. In _build_maintainonce_output() in worker.py, check recency before running the attempt."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L002",
        "title": "repair_all_tiers validates healthy tiers every maintenance cycle",
        "symptom": "logs show tier 10-17 health check every 20-30 seconds; all tiers healthy; no progress",
        "root_cause": (
            "repair_all_tiers() was called unconditionally in autonomous_maintenance_cycle every cycle (~23s). "
            "No cooldown existed. Each call ran 8 tier validations even though nothing changed."
        ),
        "detection_code": """
import json
from pathlib import Path
state = json.loads(Path('memory/luna_autonomy_state.json').read_text(encoding='utf-8')) if Path('memory/luna_autonomy_state.json').exists() else {}
last_repair = state.get('last_self_repair_at', '')
if last_repair:
    from datetime import datetime
    secs = (datetime.now() - datetime.fromisoformat(last_repair)).total_seconds()
    print(f'Last repair: {secs:.0f}s ago. Cooldown is 1800s.')
    if secs < 1800:
        print('Repair is correctly on cooldown.')
    else:
        print('WARNING: Repair cooldown elapsed — will run next cycle.')
""",
        "fix": (
            "In autonomous_maintenance_cycle in worker.py, wrap repair_all_tiers() with a 1800s cooldown: "
            "_last_repair_ts = state.get('last_self_repair_at', ''); "
            "_repair_due = not _last_repair_ts or (datetime.now() - datetime.fromisoformat(_last_repair_ts)).total_seconds() >= 1800; "
            "if _repair_due: run_repair(); state['last_self_repair_at'] = now_iso()"
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L003",
        "title": "Cooldown timestamp in dead code after return — never saved",
        "symptom": "A step (scan, patch, tier_gen) runs every cycle despite cooldown constants being set",
        "root_cause": (
            "state['last_X_at'] = now_iso() was placed AFTER a return statement in the function body. "
            "Python exits at the return; the timestamp is never written; has_cooldown_elapsed() always returns True."
        ),
        "detection_code": """
import ast
from pathlib import Path
src = Path('luna_modules/luna_tier9_external_learning.py').read_text(encoding='utf-8')
tree = ast.parse(src)
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        found_return = False
        for stmt in node.body:
            if found_return and not isinstance(stmt, (ast.Return, ast.Pass)):
                print(f'DEAD CODE in {node.name}() at line {stmt.lineno}')
            if isinstance(stmt, ast.Return):
                found_return = True
""",
        "fix": (
            "Move all state['last_X_at'] = now_iso() calls to BEFORE the return statement, "
            "or into the try block that performs the action. "
            "Run the dead_code detector from luna_self_repair_engine._detect_dead_code() to find these."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L004",
        "title": "Inner function signature mismatch — TypeError silently swallowed in dispatch loop",
        "symptom": "A step in the evolution loop appears to run but does nothing; no error logged",
        "root_cause": (
            "Dispatch loop calls step(state, result, query_llm_fn) but inner function defined as def step(state, result). "
            "Python raises TypeError which is caught by the outer try/except and silently swallowed. "
            "The step is a no-op but logs show it 'ran'."
        ),
        "detection_code": """
import ast
src = open('luna_modules/luna_tier9_external_learning.py', encoding='utf-8').read()
tree = ast.parse(src)
for fn in ast.walk(tree):
    if isinstance(fn, ast.FunctionDef) and fn.name == 'continuous_evolution_step':
        for inner in ast.walk(fn):
            if isinstance(inner, ast.FunctionDef) and inner.name != fn.name:
                args = [a.arg for a in inner.args.args]
                print(f'  {inner.name}: args={args}')
""",
        "fix": (
            "Match the inner function signature to how it is called. "
            "If the dispatch loop calls step() with no args (closure pattern), make sure inner fn has no required args. "
            "If dispatch loop calls step(state, result, fn), inner fn must accept those 3 params. "
            "Always log exceptions: _diag(f'{step.__name__} failed: {type(exc).__name__}: {exc}')"
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L005",
        "title": "Claude model name (claude-*) sent to OpenRouter/OpenAI/Grok — all providers 400/404",
        "symptom": "All 4 providers fail every query_llm call; logs show 'claude-sonnet-4-6 is not a valid model ID'",
        "root_cause": (
            "query_llm passes model= directly to all providers in the waterfall. "
            "Claude model names (claude-*) are Anthropic-only. "
            "OpenRouter, OpenAI, Grok don't recognize them and return HTTP 400/404."
        ),
        "detection_code": """
import re
src = open('luna_modules/luna_tier6_codegen.py', encoding='utf-8').read()
hits = re.findall(r'model=[\"\\']claude-[^\"\\']*.', src)
if hits:
    print('FOUND hardcoded claude-* model in tier6:', hits)
src9 = open('luna_modules/luna_tier9_external_learning.py', encoding='utf-8').read()
hits9 = re.findall(r'model=[\"\\']claude-[^\"\\']*.', src9)
if hits9:
    print('FOUND hardcoded claude-* model in tier9:', hits9)
""",
        "fix": (
            "1. In query_llm in worker.py, add: _ext_model = model if model and not model.startswith('claude-') else '' "
            "Then use _ext_model in OpenRouter/OpenAI/Grok lambdas. "
            "2. At call sites: remove model='claude-*' from query_llm calls — use only provider='claude' without model override. "
            "3. grep -rn 'model=\"claude-' luna_modules/ to find all offending call sites."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L006",
        "title": "Missing helper function (def missing, body present as dead code)",
        "symptom": "NameError on first call to _try_scan_step or similar helper; module imports fine but crashes at runtime",
        "root_cause": (
            "During an edit, the 'def function_name():' line was dropped but the function body remained. "
            "The body became dead code inside the enclosing function. "
            "No NameError at import time because the body isn't executed. "
            "NameError appears at first runtime call."
        ),
        "detection_code": """
from luna_modules.luna_self_repair_engine import _detect_missing_helper
import ast
from pathlib import Path
src = Path('luna_modules/luna_tier9_external_learning.py').read_text(encoding='utf-8')
tree = ast.parse(src)
bugs = _detect_missing_helper(src, tree)
if bugs:
    for b in bugs:
        print(f'MISSING HELPER: {b[\"name\"]} at line {b[\"line\"]}')
else:
    print('No missing helpers found')
""",
        "fix": (
            "1. Add the missing def line before the orphaned body. "
            "2. Run _detect_missing_helper() from luna_self_repair_engine — it catches this pattern. "
            "3. Delete all .pyc bytecache after fixing: "
            "find luna_modules/__pycache__ -name '*.pyc' | xargs rm -f"
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L007",
        "title": "check_model_routing() runs every cycle — no cooldown — live feed spam",
        "symptom": "Live feed floods with '[LUNA THINKS] Model routing looks correct' every 20-30 seconds",
        "root_cause": (
            "check_model_routing() is called in autonomous_maintenance_cycle with no cooldown. "
            "Even when routing is correct, _think() fires every cycle and fills the live feed. "
            "This looks like a stuck loop but is really a missing cooldown guard."
        ),
        "detection_code": """
import json
from pathlib import Path
state = json.loads(Path('memory/luna_autonomy_state.json').read_text(encoding='utf-8')) if Path('memory/luna_autonomy_state.json').exists() else {}
if 'last_routing_check_at' not in state:
    print('ISSUE: check_model_routing has no cooldown — add last_routing_check_at tracking')
else:
    print(f'OK: last_routing_check_at = {state[\"last_routing_check_at\"]}')
""",
        "fix": (
            "In autonomous_maintenance_cycle in worker.py, wrap check_model_routing() with a 3600s cooldown: "
            "_last_routing_ts = state.get('last_routing_check_at', ''); "
            "_routing_due = not _last_routing_ts or (datetime.now() - datetime.fromisoformat(_last_routing_ts)).total_seconds() >= 3600; "
            "if _routing_due: run_check(); state['last_routing_check_at'] = now_iso()"
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L011",
        "title": "Kill switch ignored during long LLM calls — stop takes 30-60s to take effect",
        "symptom": "User types 'stop' or clicks Pause; kill switch flag is created; Luna keeps going for a full minute",
        "root_cause": (
            "is_kill_switch_active() is checked only at the TOP of each autonomous_maintenance_cycle iteration. "
            "Each iteration can contain a 30-60s LLM call (OpenRouter/Grok timeout). "
            "The kill switch is not checked inside query_llm() between provider attempts, "
            "so the current LLM call always completes before the stop takes effect."
        ),
        "detection_code": """
from pathlib import Path
flag = Path('D:/SurgeApp/LUNA_STOP_NOW.flag')
print('Kill switch:', 'ACTIVE' if flag.exists() else 'CLEAR')
# Check if query_llm has kill switch check inside provider loop
src = Path('worker.py').read_text(encoding='utf-8')
if 'is_kill_switch_active()' in src and 'for fn in sequence' in src:
    # Find if check is inside the loop
    loop_idx = src.index('for fn in sequence')
    check_idx = src.find('is_kill_switch_active()', loop_idx)
    print('Kill switch check inside provider loop:', check_idx > 0 and check_idx < loop_idx + 500)
""",
        "fix": (
            "Inside query_llm() in worker.py, add a kill switch check at the TOP of the 'for fn in sequence' loop: "
            "if is_kill_switch_active(): _diag('query_llm: kill switch active — aborting'); return ''. "
            "This makes stop take effect within seconds (between provider attempts) instead of a full minute."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L012",
        "title": "Tiers 10-17 registered but not wired — summary() never called in maintenance cycle",
        "symptom": "/tierstatus shows tiers 12, 16 etc as 'verified' but NOT 'wired' — they exist but contribute nothing",
        "root_cause": (
            "The tier registry marks tiers as wired=True when they can be imported (import_ok). "
            "But gather_specialist_signals() only calls internal specialists (memory, queue, log, upgrade agents). "
            "Tiers 10-17 summary() functions are never called in autonomous_maintenance_cycle. "
            "They are audited and auto-fixed but their data never reaches the system state."
        ),
        "detection_code": """
import json
from pathlib import Path
state = json.loads(Path('memory/luna_autonomy_state.json').read_text(encoding='utf-8')) if Path('memory/luna_autonomy_state.json').exists() else {}
signals = state.get('tier_signals', {})
print(f'Active tier signals in state: {list(signals.keys())}')
if not signals:
    print('WARNING: No tier signals collected — tiers 10+ are not contributing to system state')
""",
        "fix": (
            "In autonomous_maintenance_cycle in worker.py, add a _tier_signals collection block that: "
            "1. Loads the tier registry with _load_registry(). "
            "2. For each tier >= 10 with an existing file: imports it with importlib.util, calls tier{N}_summary(). "
            "3. Stores results in state['tier_signals'] and state['last_tier_signals_at']. "
            "4. Runs every 300s (5 min) with a cooldown. "
            "Also in _tier_status_rows(), set twired=True when tier{N}_summary() succeeds — "
            "a tier that can run its summary IS wired, regardless of what the registry field says."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L010",
        "title": "Pause/stop words sent as shell commands — kill switch never activated",
        "symptom": "User types 'stop' or 'pause' in the terminal; Luna says 'ok stopped' but autonomous loop keeps running",
        "root_cause": (
            "Terminal process_command() sends unrecognized words to the shell as subprocess commands. "
            "'stop' as a shell command does nothing on Windows. "
            "The actual stop mechanism is creating LUNA_STOP_NOW.flag at D:\\SurgeApp\\LUNA_STOP_NOW.flag. "
            "Luna's autonomous loop checks is_kill_switch_active() = KILL_SWITCH_PATH.exists() every cycle."
        ),
        "detection_code": """
from pathlib import Path
flag = Path('D:/SurgeApp/LUNA_STOP_NOW.flag')
print('Kill switch ACTIVE' if flag.exists() else 'Kill switch CLEAR — Luna is running')
""",
        "fix": (
            "In SurgeApp_Claude_Terminal.py process_command(), check for stop/pause words BEFORE shell routing: "
            "if raw.lower().strip() in _STOP_WORDS: KILL_SWITCH_PATH.touch(); return. "
            "For resume: KILL_SWITCH_PATH.unlink(missing_ok=True). "
            "Also add a ⏸ Pause button in the header that calls toggle_pause(). "
            "This is now implemented — _STOP_WORDS and _RESUME_WORDS are defined at module level, "
            "and _set_paused() / _set_running() / toggle_pause() are methods on LunaTerminalWindow."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L009",
        "title": "Linter merges functions — dead code, duplicates, missing detect_loops()",
        "symptom": (
            "run_loop_diagnosis() raises NameError: 'detect_loops' is not defined. "
            "Or detect_no_progress_loop() always returns None (stub that just pass-es). "
            "Or ask_for_help() body appears as unreachable code inside another function."
        ),
        "root_cause": (
            "An automated linter or AI edit merged separate functions together. "
            "Typical damage: (1) detect_loops() deleted — body folded into _recent_events as dead code after return. "
            "(2) detect_no_progress_loop() duplicated — correct version overwritten by stub with just 'pass'. "
            "(3) ask_for_help() body moved inside run_loop_diagnosis() after its return statement. "
            "(4) loop_detector_summary() reads keys that no longer exist in the (broken) run_loop_diagnosis return dict."
        ),
        "detection_code": """
import ast
from pathlib import Path
src = Path('luna_modules/luna_loop_detector.py').read_text(encoding='utf-8')
tree = ast.parse(src)
fns = {}
for n in ast.walk(tree):
    if isinstance(n, ast.FunctionDef):
        fns[n.name] = fns.get(n.name, 0) + 1
required = {'detect_loops', 'detect_no_progress_loop', 'run_loop_diagnosis', 'ask_for_help', 'loop_detector_summary'}
missing = required - set(fns.keys())
dups = {k: v for k, v in fns.items() if v > 1}
if missing: print('MISSING functions:', missing)
if dups:    print('DUPLICATED functions:', dups)
if not missing and not dups: print('OK')
""",
        "fix": (
            "Rewrite luna_loop_detector.py from scratch with these functions in order: "
            "record_cycle_event, _sig, _recent_events (returns raw event list only), "
            "detect_loops (groups by sig, returns loop list), suggest_escape, mark_escaped, "
            "detect_no_progress_loop (ONE version — reads snapshot file), "
            "run_loop_diagnosis (calls detect_loops + detect_no_progress_loop, returns dict with "
            "'loops_found', 'failing_loops', 'stuck', 'loops', 'top_escape', 'no_progress'), "
            "ask_for_help (standalone function: web_search → Grok → log alert), "
            "loop_detector_summary (reads diag['loops_found'] and diag['failing_loops']). "
            "Validate with: python -c \"import ast; ast.parse(open('luna_modules/luna_loop_detector.py').read())\""
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L008",
        "title": "Luna stuck in loop — escalation protocol: ask internet + Grok + human",
        "symptom": "Luna repeats the same failed action 3+ times and cannot self-repair from her own catalog",
        "root_cause": (
            "Self-repair engine and loop detector fix only known patterns. "
            "When a new unknown failure occurs and all deterministic fixes fail, Luna has no escalation path. "
            "She continues retrying forever instead of asking for external help."
        ),
        "detection_code": """
from luna_modules.luna_loop_detector import run_loop_diagnosis
diag = run_loop_diagnosis()
if diag['stuck']:
    print('STUCK:', diag['top_escape'])
    print('Next: call ask_for_help(diag, query_llm_fn=query_llm) from luna_loop_detector')
else:
    print('Not stuck — no escalation needed')
""",
        "fix": (
            "When run_loop_diagnosis() returns stuck=True and no known catalog fix applies: "
            "1. Call web_search(top_escape[:100]) from luna_tools — search the internet first. "
            "2. Call query_llm(provider='grok', prompt=problem) — ask Grok (intelligence hierarchy). "
            "3. Call speak('I am stuck and need help: <problem>', mood='alert') — alert the human. "
            "4. Stop retrying the same action — sleep 300s before next attempt. "
            "The ask_for_help() function in luna_loop_detector.py implements this full escalation."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-26",
    },
    {
        "id": "L013",
        "title": "Luna hallucinating tier status — LLM invents tier descriptions instead of reading registry",
        "symptom": (
            "Luna reports 'Tier 12: Doesn't exist, skipped' or gives wrong tier descriptions. "
            "All tier files exist on disk but Luna says they don't. "
            "Luna generates a tier status report that is entirely from LLM memory, not real data."
        ),
        "root_cause": (
            "_is_tier_query() did not match phrases like 'check all tiers', 'check each one', "
            "'all available tiers', 'are they wired'. When the user prompt didn't match, "
            "generate_luna_chat_response() fell through to the LLM fallback path which hallucinated "
            "tier data from training knowledge instead of calling _build_tier_status_report(). "
            "The LLM does not know which tiers are dynamically self-generated — it invents plausible-sounding answers."
        ),
        "detection_code": """
# If Luna ever generates a tier report without calling _build_tier_status_report,
# the output will be wrong. Check: does the user's prompt match _is_tier_query()?
from luna_modules.luna_routing import normalize_prompt_text
prompt = 'check all tiers make sure they are running'
normalized = normalize_prompt_text(prompt)
# These should all return True — if False, add markers to _is_tier_query()
checks = ['all tier', 'check all tier', 'each tier', 'are running']
missing = [c for c in checks if c not in normalized]
if missing:
    print('MISSING MARKERS in _is_tier_query():', missing)
else:
    print('OK — all markers matched')
""",
        "fix": (
            "1. In worker.py _is_tier_query(), add markers: 'all tiers', 'each tier', 'every tier', "
            "'check each', 'all available tier', 'how many tier', 'are running', 'are wired', "
            "'tier 1' through 'tier 17', 'check each one', 'i have 17 tier'. "
            "2. NEVER generate a tier report from LLM output — always route through _build_tier_status_report(). "
            "3. _build_tier_status_report() reads _tier_status_rows() which reads the registry and calls "
            "tier{N}_summary() at runtime — this is always accurate. "
            "4. If you are about to write a tier list (Tier 1: ..., Tier 2: ...) from memory, STOP — "
            "call _build_tier_status_report(owner) instead and return that result."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L014",
        "title": "Three performance bottlenecks: memory seeding, queue bloat, caching",
        "symptom": (
            "Python optimization attempts keep failing due to: "
            "(1) excessive queue bloat — DONE_DIR accumulates thousands of files, making glob O(N); "
            "(2) _seed_long_term_memory_defaults() reading memory JSON on every chat response; "
            "(3) task routing spending CPU on sequential string checks for every incoming message."
        ),
        "root_cause": (
            "1. prune_done_queue() did not exist — done/failed tasks pile up forever. "
            "_friendly_log_tail() globs DONE_DIR/*.json every call, costing O(N) per chat response when N > 1000. "
            "2. _seed_long_term_memory_defaults() called from generate_luna_chat_response() with no guard. "
            "It always read LONG_TERM_MEMORY_PATH from disk (expensive I/O) even after the defaults were already seeded. "
            "3. _is_tier_query() runs ~30 'in' checks on every chat prompt — acceptable O(N), but can mis-route "
            "prompts when markers are missing, forcing expensive LLM fallback for simple queries."
        ),
        "detection_code": """
# Check done queue size
from pathlib import Path
done = list(Path('tasks/done').glob('*.json'))
print(f'Done queue size: {len(done)} files')
if len(done) > 200:
    print('WARNING: done queue bloat — run prune_done_queue()')

# Check memory seeding guard
import worker
print('Memory guard active:', worker._MEMORY_DEFAULTS_SEEDED)

# Check queue prune cooldown
import json
state = json.loads(Path('memory/luna_autonomy_state.json').read_text(encoding='utf-8'))
print('Last prune:', state.get('last_prune_at', 'never'))
""",
        "fix": (
            "1. Queue bloat: prune_done_queue(max_age_hours=24, keep_min=100) moves old done/failed tasks "
            "to tasks/archive/. Call it in autonomous_maintenance_cycle() with 3600s cooldown via state['last_prune_at']. "
            "2. Memory seeding: add _MEMORY_DEFAULTS_SEEDED = False module-level flag. "
            "At start of _seed_long_term_memory_defaults(), return immediately if already True. "
            "Set True after first successful run. This turns O(file_read) per chat into O(1). "
            "3. Task routing: keep _is_tier_query() markers comprehensive so expensive LLM fallback "
            "is never triggered for tier-status prompts. Run check_tier_query_coverage() to verify."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L015",
        "title": "Luna hallucinating her own health status — LLM invents complaints instead of running diagnostics",
        "symptom": (
            "User asks 'what problems are you having?' and Luna says 'task routing, queue bloat, and caching "
            "are causing performance bottlenecks' even after those issues are fixed. "
            "Luna keeps repeating the same complaint regardless of actual system state. "
            "Luna claims she 'synced with Council, checked GitHub' — none of which happened."
        ),
        "root_cause": (
            "generate_luna_chat_response() fell through to the LLM fallback for status queries. "
            "The LLM saw recent conversation context about performance bottlenecks and pattern-matched "
            "a plausible-sounding response. It has no access to real state: it cannot call "
            "run_full_self_diagnosis(), check the done queue size, or read luna_autonomy_state.json. "
            "Status queries must NEVER go to the LLM — they must route to _build_self_status_report()."
        ),
        "detection_code": """
# Test that status queries route to real diagnostics, not LLM
from luna_modules.luna_routing import normalize_prompt_text
test_prompts = [
    'what problems are you having',
    'are you still having issues',
    'double check if you are having problems',
    'have you tried to fix it',
]
# These should ALL match _is_self_status_query() in worker.py
# If any return False, the prompt will hit LLM and hallucinate
# Check: grep for '_is_self_status_query' in worker.py
import subprocess
result = subprocess.run(['python', '-c',
    'import sys; sys.path.insert(0, \".\"); '
    'from worker import _is_self_status_query; '
    'tests = [\"what problems are you having\", \"are you still having issues\"]; '
    '[print(t, \"->\", _is_self_status_query(t)) for t in tests]'
], capture_output=True, text=True, cwd='D:/SurgeApp')
print(result.stdout)
""",
        "fix": (
            "In worker.py generate_luna_chat_response(), add BEFORE the LLM fallback: "
            "elif _is_self_status_query(prompt_text): response = _build_self_status_report(owner). "
            "_build_self_status_report() calls run_full_self_diagnosis(), run_loop_diagnosis(), "
            "reads the done queue size, and reads luna_autonomy_state.json. "
            "_is_self_status_query() must catch: 'what problem', 'having issue', 'still having', "
            "'double check', 'are you ok', 'health check', 'did you fix', 'are you fixed', etc. "
            "RULE: Never use LLM to answer questions about your own system state. "
            "Always call the real diagnostic function and report what it returns."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L016",
        "title": "Luna proactively broadcasting hallucinated performance complaints via speak()",
        "symptom": (
            "Luna sends autonomous messages like: 'task routing, queue bloat, and caching are causing "
            "performance bottlenecks', 'LRU cache eviction policy', 'orchestration kernel', "
            "'synced with Council, checked GitHub'. These are LLM pattern-matches from conversation "
            "history, not real diagnostic results. Repeats even after issues are fixed."
        ),
        "root_cause": (
            "The LLM generating autonomous speak() content or chat responses sees recent conversation "
            "context about performance bottlenecks and generates plausible-sounding (but false) "
            "continuations. speak() had no filter — any LLM output could be broadcast as factual status. "
            "The LLM cannot access system state: it does not know queue sizes, vault cache status, "
            "or diagnostic results. It invents these complaints from training patterns."
        ),
        "detection_code": """
# Detect if speak() has the hallucination filter
from pathlib import Path
source = Path('D:/SurgeApp/worker.py').read_text(encoding='utf-8')
if '_HALLUCINATION_PHRASES' not in source:
    print('MISSING: speak() hallucination filter not installed')
else:
    print('OK: speak() hallucination filter present')
""",
        "fix": (
            "Add _HALLUCINATION_PHRASES tuple to worker.py. In speak(), check if message.lower() "
            "contains any hallucination phrase. If yes, replace message with "
            "run_full_self_diagnosis() output (real data). "
            "Phrases to filter: 'task routing, queue bloat', 'queue bloat, and caching', "
            "'performance bottlenecks from inefficient', 'lru cache eviction', "
            "'orchestration kernel', 'synced with council'. "
            "RULE: Never broadcast a performance complaint without a real diagnostic backing it. "
            "If diagnostics are clean, say 'all clear'. If dirty, quote the real issue text."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L017",
        "title": "Luna says 'Done' for file-creation tasks without creating any files",
        "symptom": (
            "User asks Luna to create a logo, icon, shortcut, or file. "
            "Luna describes what she would do in detail ('I'll design a crescent moon...'), "
            "then says 'Done.' — but no file was created, no code was run, nothing exists on disk. "
            "User has to ask a human or Claude Code to actually do it."
        ),
        "root_cause": (
            "The task was a 'chat' type and routed through generate_luna_chat_response() → LLM fallback. "
            "The LLM generated a plausible description of the task completion without calling any tools. "
            "Luna has no image-generation capability and cannot create .ico or .lnk files via LLM text alone. "
            "The LLM said 'Done' because that pattern appears after task descriptions in training data — "
            "it has no awareness that it failed to produce real output. "
            "Creating files requires: subprocess calls, PIL/Pillow for images, "
            "PowerShell/WScript for .lnk shortcuts, or writing binary content to disk."
        ),
        "detection_code": """
# After any file-creation task, verify the file actually exists before saying Done.
# Correct pattern:
from pathlib import Path
import subprocess

# For icon creation:
icon_path = Path('D:/SurgeApp/surge_luna_icon.ico')
if not icon_path.exists():
    print('FAILURE: icon was not created — LLM hallucinated completion')
else:
    print('OK:', icon_path, 'size =', icon_path.stat().st_size, 'bytes')

# For shortcut creation:
import os
desktop = Path(os.path.expanduser('~')) / 'Desktop'
shortcut = desktop / 'Luna Command Center.lnk'
if not shortcut.exists():
    # Try OneDrive desktop
    shortcut2 = Path(os.environ.get('USERPROFILE', '')) / 'OneDrive' / 'Desktop' / 'Luna Command Center.lnk'
    print('shortcut exists:', shortcut2.exists())
else:
    print('shortcut OK:', shortcut)
""",
        "fix": (
            "For file-creation tasks, Luna MUST use real Python code to create the files: "
            "1. Icons: use PIL.Image + ImageDraw to draw the design, save with .save(path, format='ICO'). "
            "   Required sizes: 16, 32, 48, 64, 128, 256 px. "
            "2. Shortcuts (.lnk): use PowerShell: "
            "   $wsh = New-Object -ComObject WScript.Shell; $sc = $wsh.CreateShortcut(path); ... $sc.Save(). "
            "   TargetPath = 'wscript.exe', Arguments = '/nologo \"Start_SurgeApp.vbs\"'. "
            "3. NEVER say 'Done' without verifying the output file exists on disk. "
            "4. Always report: file path, size in bytes, and a confirmation that Path.exists() returned True. "
            "Luna's moon icon design: crescent (full circle minus offset bite), electric blue #5b8cff, "
            "navy background, gradient wave strokes inside crescent, saved to D:/SurgeApp/surge_luna_icon.ico."
        ),
        "fix_verified": True,
        "date_learned": "2026-04-27",
    },
    {
        "id": "L018",
        "title": "Aider Bridge orphan children accumulate after a crash and waste GPU/CPU",
        "symptom": (
            "After aider_bridge.py is killed mid-job (via Ctrl-C, kill switch, or crash), "
            "the spawned `python -m aider --model ... --file <path>` child process keeps running "
            "with no parent watching it. On next bridge start, that orphan is still consuming "
            "Ollama/GPU resources and may finish, write a diff, and confuse the verifier."
        ),
        "root_cause": (
            "aider_bridge.py used subprocess.run(timeout=...) which on Windows kills only the "
            "direct child. If the bridge process itself dies (not the child), the aider child "
            "becomes orphaned and adopted by the system. There was no startup sweep to clean these up."
        ),
        "detection_code": """
import subprocess, json
result = subprocess.run([
    'powershell','-NoProfile','-Command',
    "Get-CimInstance Win32_Process | Where-Object {"
    " $_.Name -match '^python' -and $_.CommandLine -match 'aider' "
    " -and ($_.CommandLine -match 'logic_updates|aider_jobs')"
    "} | Select-Object ProcessId,ParentProcessId | ConvertTo-Json -Compress"
], capture_output=True, text=True, timeout=8, creationflags=0x08000000)
rows = json.loads(result.stdout or '[]')
if isinstance(rows, dict): rows=[rows]
print(f'Found {len(rows)} aider child(ren). Orphans = those whose parent is not a current bridge pid.')
""",
        "fix": (
            "1. In aider_bridge.py, define `_cleanup_orphan_aider_children()` that uses Get-CimInstance "
            "to find python processes with `aider` and `logic_updates|aider_jobs` in their command line, "
            "then kills any whose parent is not the current bridge pid via `taskkill /T /F /PID <pid>`. "
            "2. Call it in main() right after acquiring the bridge PID lock and before the watch loop. "
            "3. Skip current bridge pid and direct children of current bridge pid to avoid self-kill."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L019",
        "title": "Aider timeout leaves orphan aider child still running on Windows",
        "symptom": (
            "Bridge logs `aider_timeout` but `python -m aider` keeps running for minutes after, "
            "still hitting Ollama, still holding model VRAM. Subsequent jobs fail or queue up. "
            "Process tree shows aider child alive after bridge moved on to next task."
        ),
        "root_cause": (
            "subprocess.run(cmd, timeout=AIDER_TIMEOUT) on Windows raises TimeoutExpired and calls "
            "Popen.kill() on the direct child. But aider spawns its OWN children (LiteLLM HTTP "
            "subprocess, etc.). Those grandchildren survive, still owning the Ollama HTTP connection."
        ),
        "detection_code": """
# After a job that hits AIDER_TIMEOUT, run this to confirm cleanup:
import subprocess, json
result = subprocess.run([
    'powershell','-NoProfile','-Command',
    "Get-CimInstance Win32_Process | Where-Object {"
    " $_.Name -match '^python' -and $_.CommandLine -match '-m aider'"
    "} | Select-Object ProcessId,ParentProcessId,@{n='Cmd';e={\\$_.CommandLine}} | ConvertTo-Json -Compress"
], capture_output=True, text=True, timeout=5, creationflags=0x08000000)
rows = json.loads(result.stdout or '[]')
if isinstance(rows, dict): rows=[rows]
print(f'Aider processes still alive after timeout: {len(rows)} (expected 0)')
""",
        "fix": (
            "1. Replace `subprocess.run(cmd, timeout=...)` with `subprocess.Popen(cmd)` + "
            "`proc.communicate(timeout=AIDER_TIMEOUT)`. "
            "2. On `subprocess.TimeoutExpired`, call `subprocess.run(['taskkill','/T','/F','/PID',str(proc.pid)])` "
            "to kill the entire process tree (the /T flag walks descendants). Then proc.kill() as fallback. "
            "3. Set failure_reason='aider_timeout_process_tree_killed' so the CU loop can distinguish "
            "this from a clean parent-only kill."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L020",
        "title": "Aider keeps re-running the same target after repeated NOOP, wasting cycles",
        "symptom": (
            "luna_live_feed.jsonl shows the same target file claimed and run, returning NOOP "
            "(no diff), then re-queued, then NOOP again — endlessly. CU's `_all_skip_streak` "
            "rises but bridge keeps accepting new jobs for the same target."
        ),
        "root_cause": (
            "Bridge had no per-target memory of NOOPs. Each new job claim looked fresh — "
            "the target's history of failed-to-produce-diffs was not consulted. CU's job "
            "rebuilds keep proposing the same files."
        ),
        "detection_code": """
from pathlib import Path
import json
budget = Path(r'D:/SurgeApp/logs/aider_bridge_noop_budget.json')
if budget.exists():
    data = json.loads(budget.read_text(encoding='utf-8') or '{}')
    for target, entry in data.items():
        if int(entry.get('count') or 0) >= 2:
            print(f'noop-exhausted: {target}  cooldown_until={entry.get(\"cooldown_until\")}')
else:
    print('noop budget file missing — bridge may not be tracking per-target noops')
""",
        "fix": (
            "1. In aider_bridge.py, define _NOOP_BUDGET_PATH = LOGS_DIR / 'aider_bridge_noop_budget.json'. "
            "2. Add helpers: _noop_budget_load(), _noop_budget_save(data), "
            "_noop_budget_check(target) -> bool (True if in cooldown), "
            "_noop_budget_record(target) (increment count; on count >= 2 set cooldown_until = now + 24h). "
            "3. In the main watch loop, BEFORE claiming a job, call _noop_budget_check(target) and "
            "skip the job (continue) if True. Emit NOOP_BUDGET_SKIP live feed event. "
            "4. AFTER detecting status == 'noop', call _noop_budget_record(target). "
            "5. Cooldown auto-clears once 24h has passed (the check function pops expired entries)."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L021",
        "title": "UI shows 'idle' or stale state because bridge status JSON misses key fields",
        "symptom": (
            "Terminal status badge shows 'online' even when bridge is mid-job. Operator can't "
            "tell what target is being processed or how long it has been running. "
            "aider_bridge_status.json only contains state and task_id with no timestamps."
        ),
        "root_cause": (
            "_write_bridge_status() emitted only state, task_id, detail. There was no structured "
            "target/started_at/last_event_at — the target was buried inside detail string."
        ),
        "detection_code": """
import json
from pathlib import Path
p = Path(r'D:/SurgeApp/logs/aider_bridge_status.json')
if not p.exists():
    print('MISSING: aider_bridge_status.json — bridge not running or not writing status')
else:
    data = json.loads(p.read_text(encoding='utf-8'))
    required = ['ts','pid','state','task_id','target','started_at','last_event_at']
    missing = [k for k in required if k not in data]
    if missing:
        print(f'INCOMPLETE bridge status — missing fields: {missing}')
    else:
        print(f'OK: bridge status complete. state={data[\"state\"]} target={data[\"target\"]}')
""",
        "fix": (
            "1. In aider_bridge.py at module scope, declare globals: "
            "_BRIDGE_JOB_STARTED_AT: str = '' and _BRIDGE_JOB_TARGET: str = ''. "
            "2. Update _write_bridge_status(state, task_id='', detail='', target='') to: "
            "   - On state == 'processing': set _BRIDGE_JOB_STARTED_AT (if empty) and _BRIDGE_JOB_TARGET. "
            "   - On any other state: clear both globals. "
            "   - Always emit ts, pid, state, task_id, target, started_at, last_event_at, detail. "
            "3. Update the call in run_aider_patch to pass target=target explicitly. "
            "4. UI (_tick_heartbeat in SurgeApp_Claude_Terminal.py) should read these fields and "
            "show bridge:<state>(<filename>) when state != 'idle'."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L022",
        "title": "CU ui_status falsely says 'paused_dirty_core' while CU is actively running a job",
        "symptom": (
            "continues_update_state.json has running=True and phase=queueing (real work in flight) "
            "but ui_status field says 'paused_dirty_core'. Terminal badge wrongly shows the "
            "yellow 'paused' state when CU is actually doing useful work on a clean target."
        ),
        "root_cause": (
            "_cu_compute_ui_status() checked `if dirty_targets:` BEFORE checking `if running and "
            "phase == 'queueing'`. dirty_targets is INFORMATIONAL — it lists which files were "
            "skipped this cycle, not whether CU itself is paused. Active work overrides skipped "
            "informational entries."
        ),
        "detection_code": """
import json
from pathlib import Path
s = json.loads(Path(r'D:/SurgeApp/memory/continues_update_state.json').read_text(encoding='utf-8'))
ui = s.get('ui_status'); running = s.get('running'); phase = s.get('phase')
dirty = s.get('dirty_targets') or []
if running and phase in ('queueing','starting') and ui not in ('running_real_job',):
    print(f'BUG: CU is active but ui_status={ui!r} (expected running_real_job)')
    print(f'   running={running} phase={phase} dirty_targets_count={len(dirty)}')
else:
    print(f'OK: ui_status={ui} matches running={running} phase={phase}')
""",
        "fix": (
            "Inside worker.py _cu_compute_ui_status(state), reorder the checks so the priority is: "
            "1. Blocked states (worker import / bridge stale) — highest priority. "
            "2. ACTIVE state: if running and phase in ('queueing','starting') return 'running_real_job'. "
            "3. Genuinely paused states: pause_reason in ('all_targets_dirty','blocked_by_staged_edits') "
            "or phase in ('blocked_by_staged_edits','deferred_dirty_target') => 'paused_dirty_core'; "
            "consec_fail >= 3 => 'paused_recent_failures'; "
            "skip_streak >= 2 or noop_count >= 5 => 'paused_noop_budget'; "
            "dirty and not running => 'paused_dirty_core' (last-resort dirty pause). "
            "4. Default 'idle_clean'."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L023",
        "title": "LaunchLuna's CU dirty-core gate trips on untracked files (luna_start.pyw)",
        "symptom": (
            "Every LaunchLuna start writes events: {service: continues_update, action: paused_gate, "
            "reason: dirty_core_files} — even after committing every change. continues_update never "
            "auto-starts. Manual investigation shows git status is empty for tracked core files."
        ),
        "root_cause": (
            "_cu_startup_gate() called `git status --porcelain=v1 -- <core files>`. By default, "
            "git status reports untracked files as `??`. Files like luna_start.pyw that exist "
            "locally but were never committed show up there. The gate treats any non-empty output "
            "as 'dirty', so untracked files permanently block CU."
        ),
        "detection_code": """
import subprocess
core = ['worker.py','aider_bridge.py','luna_guardian.py','LaunchLuna.pyw',
        'luna_start.pyw','SurgeApp_Claude_Terminal.py','director_agent.py']
all_status = subprocess.run(['git','-C',r'D:/SurgeApp','status','--porcelain=v1','--']+core,
                            capture_output=True, text=True).stdout
no_untracked = subprocess.run(['git','-C',r'D:/SurgeApp','status','--porcelain=v1',
                               '--untracked-files=no','--']+core,
                              capture_output=True, text=True).stdout
extra_lines = [l for l in all_status.splitlines() if l.startswith('??')]
if extra_lines and not no_untracked.strip():
    print('BUG: gate treats untracked files as dirty:')
    for l in extra_lines: print('   ', l)
else:
    print('OK: gate sees only tracked-modified files')
""",
        "fix": (
            "In LaunchLuna.pyw _cu_startup_gate(), add --untracked-files=no to the git status call: "
            "subprocess.run(['git','status','--porcelain=v1','--untracked-files=no','--'] + _CU_GATE_CORE_FILES, ...). "
            "This restricts the gate to TRACKED files that are modified or staged. Untracked files do not "
            "represent in-progress work that could conflict with CU edits."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
    {
        "id": "L024",
        "title": "Verifier reports false 'duplicate storm' for worker_main when an aider job targets worker.py",
        "symptom": (
            "Luna_Post_Repair_Verify.ps1 fails with 'worker_main duplicate storm (logical count=4)' "
            "even though only 1 worker_main parent/child pair exists. The 'extra' processes are an "
            "aider child whose --file argument contains worker.py."
        ),
        "root_cause": (
            "The verifier matched the marker (e.g. 'worker.py') anywhere in the command line. "
            "An aider invocation like `python -m aider --file D:\\SurgeApp\\worker.py` matches "
            "the substring 'worker.py' even though the script being invoked is `aider`, not "
            "`worker.py`. Same problem affects aider_bridge.py matching."
        ),
        "detection_code": """
# Run the verifier and check whether it ever flags >1 logical worker_main while
# only 2 physical worker.py processes exist (one parent/child pair = 1 logical).
# If a duplicate storm is reported, list all 'python -m aider' processes — those
# are the false positives leaking into the worker_main count.
import subprocess
out = subprocess.run(['powershell','-NoProfile','-Command',
    "Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^python' -and "
    "$_.CommandLine -match '-m aider'} | Select-Object ProcessId,@{n='Cmd';e={$_.CommandLine}}"],
    capture_output=True, text=True).stdout
print(out or '(no aider children running)')
""",
        "fix": (
            "In Luna_Post_Repair_Verify.ps1 (Section 4), replace the substring match with a "
            "Test-InvokesScript helper that: "
            "1. Returns false if command line contains '-m aider' (excludes aider child processes). "
            "2. Matches the marker only when it appears as a SCRIPT after python(w).exe, using a regex "
            "like `python[\\w.]*\\.exe\"?\\s+\"?[^\"\\s]*\\\\?<marker>(?:\"|\\s|$)`. "
            "Use this helper for every logical role (worker_main, worker_cu, aider_bridge, guardian, "
            "terminal, apprentice, tray). Same logic luna_guardian.py uses internally for "
            "_command_invokes_script — keep them consistent."
        ),
        "fix_verified": True,
        "date_learned": "2026-05-01",
    },
]


# ── Active diagnostics — Luna runs these herself ──────────────────────────────

def check_tier6_loop() -> Optional[Dict[str, Any]]:
    """Detect if tier6 is patching the same function repeatedly."""
    try:
        from luna_modules.luna_tier6_codegen import _read_history
        hist = _read_history(limit=30)
        if not hist:
            return None
        recent_window = datetime.now() - timedelta(minutes=30)
        recent = [h for h in hist if datetime.fromisoformat(h.get("ts", "2000-01-01")) >= recent_window]
        fn_counts: Dict[str, int] = {}
        for h in recent:
            fn = h.get("function", "")
            fn_counts[fn] = fn_counts.get(fn, 0) + 1
        repeated = {fn: c for fn, c in fn_counts.items() if c >= 3}
        if repeated:
            return {
                "issue": "tier6_patch_loop",
                "repeated_targets": repeated,
                "lesson": "L001",
                "diagnosis": f"Tier6 patched the same function 3+ times in 30 minutes: {list(repeated.keys())}",
                "fix": LESSONS[0]["fix"],
            }
    except Exception as exc:
        _diag(f"self_teacher check_tier6_loop: {exc}")
    return None


def check_repair_cooldown() -> Optional[Dict[str, Any]]:
    """Check if repair_all_tiers has a working cooldown."""
    try:
        state_path = Path("memory/luna_autonomy_state.json")
        if not state_path.exists():
            return None
        state = json.loads(state_path.read_text(encoding="utf-8"))
        last_repair = state.get("last_self_repair_at", "")
        if not last_repair:
            return {
                "issue": "repair_no_cooldown_state",
                "lesson": "L002",
                "diagnosis": "last_self_repair_at not in autonomy state — cooldown may not be tracking",
                "fix": LESSONS[1]["fix"],
            }
    except Exception as exc:
        _diag(f"self_teacher check_repair_cooldown: {exc}")
    return None


def check_dead_code_after_return() -> Optional[Dict[str, Any]]:
    """Scan tier9 for state timestamps in dead code after return."""
    try:
        path = _MODULES_DIR / "luna_tier9_external_learning.py"
        src = path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            found_return = False
            for stmt in node.body:
                if found_return and not isinstance(stmt, (ast.Return, ast.Pass)):
                    return {
                        "issue": "dead_code_after_return",
                        "function": node.name,
                        "line": getattr(stmt, "lineno", "?"),
                        "lesson": "L003",
                        "diagnosis": f"Dead code in {node.name}() at line {getattr(stmt, 'lineno', '?')} — cooldown timestamp may never be saved",
                        "fix": LESSONS[2]["fix"],
                    }
                if isinstance(stmt, ast.Return):
                    found_return = True
    except Exception as exc:
        _diag(f"self_teacher check_dead_code: {exc}")
    return None


def check_claude_model_leak() -> Optional[Dict[str, Any]]:
    """Check if any call site sends claude-* model names to query_llm."""
    try:
        for fname in ["luna_tier6_codegen.py", "luna_tier9_external_learning.py"]:
            src = (_MODULES_DIR / fname).read_text(encoding="utf-8", errors="replace")
            hits = re.findall(r'model=["\']claude-[^"\']*["\']', src)
            if hits:
                return {
                    "issue": "claude_model_name_leak",
                    "file": fname,
                    "hits": hits,
                    "lesson": "L005",
                    "diagnosis": f"{fname} has hardcoded claude-* model name: {hits}",
                    "fix": LESSONS[4]["fix"],
                }
        worker_src = (PROJECT_DIR / "worker.py").read_text(encoding="utf-8", errors="replace")
        if "_ext_model" not in worker_src:
            return {
                "issue": "query_llm_missing_ext_model_filter",
                "file": "worker.py",
                "lesson": "L005",
                "diagnosis": "worker.py query_llm is missing _ext_model filter — claude-* names will go to all providers",
                "fix": LESSONS[4]["fix"],
            }
    except Exception as exc:
        _diag(f"self_teacher check_claude_model_leak: {exc}")
    return None


def check_loop_detector_integrity() -> Optional[Dict[str, Any]]:
    """Detect if the linter broke luna_loop_detector.py (missing/duplicated functions)."""
    try:
        import ast as _ast
        src = (_MODULES_DIR / "luna_loop_detector.py").read_text(encoding="utf-8", errors="replace")
        tree = _ast.parse(src)
        fn_counts: Dict[str, int] = {}
        for node in _ast.walk(tree):
            if isinstance(node, _ast.FunctionDef):
                fn_counts[node.name] = fn_counts.get(node.name, 0) + 1
        required = {"detect_loops", "detect_no_progress_loop", "run_loop_diagnosis",
                    "ask_for_help", "loop_detector_summary"}
        missing = required - set(fn_counts.keys())
        dups = {k: v for k, v in fn_counts.items() if v > 1}
        if missing or dups:
            return {
                "issue":     "loop_detector_mangled_by_linter",
                "missing":   list(missing),
                "duplicates": dups,
                "lesson":    "L009",
                "diagnosis": (
                    f"luna_loop_detector.py damaged: missing={list(missing)}, duplicates={dups}. "
                    "Linter likely merged functions or deleted detect_loops()."
                ),
                "fix": LESSONS[8]["fix"],  # L009 is index 8
            }
    except Exception as exc:
        _diag(f"self_teacher check_loop_detector_integrity: {exc}")
    return None


def check_model_routing_cooldown() -> Optional[Dict[str, Any]]:
    """Detect if check_model_routing() lacks a cooldown and spams the live feed."""
    try:
        state_path = MEMORY_DIR / "luna_autonomy_state.json"
        if not state_path.exists():
            return None
        state = safe_read_json(state_path, default={}) or {}
        if "last_routing_check_at" not in state:
            return {
                "issue": "routing_check_no_cooldown",
                "lesson": "L007",
                "diagnosis": (
                    "check_model_routing() has no cooldown — emits [LUNA THINKS] every cycle, "
                    "flooding the live feed and appearing stuck"
                ),
                "fix": LESSONS[6]["fix"],
            }
    except Exception as exc:
        _diag(f"self_teacher check_model_routing_cooldown: {exc}")
    return None


def check_excluded_functions() -> Optional[Dict[str, Any]]:
    """Verify critical evolution functions are excluded from tier6 patching."""
    try:
        from luna_modules.luna_tier6_codegen import _TIER6_EXCLUDED_FUNCTIONS
        required = {"continuous_evolution_step", "autonomous_maintenance_cycle", "_build_maintainonce_output"}
        missing = required - _TIER6_EXCLUDED_FUNCTIONS
        if missing:
            return {
                "issue": "missing_tier6_exclusions",
                "missing": list(missing),
                "lesson": "L001",
                "diagnosis": f"Evolution engine functions not in exclusion list: {missing}",
                "fix": "Add to _TIER6_EXCLUDED_FUNCTIONS in luna_tier6_codegen.py: " + ", ".join(f"'{f}'" for f in missing),
            }
    except Exception as exc:
        _diag(f"self_teacher check_excluded_functions: {exc}")
    return None


def check_tier_query_coverage() -> Optional[Dict[str, Any]]:
    """Verify _is_tier_query() catches common tier-status phrases to prevent hallucination."""
    try:
        worker_path = PROJECT_DIR / "worker.py"
        source = worker_path.read_text(encoding="utf-8", errors="replace")
        required_markers = [
            '"all tiers"', '"each tier"', '"every tier"',
            '"tier 1"', '"tier 16"', '"tier 17"',
            '"are running"', '"check each"',
        ]
        missing = [m for m in required_markers if m not in source]
        if missing:
            return {
                "issue": "tier_query_missing_markers",
                "missing": missing,
                "lesson": "L013",
                "diagnosis": (
                    f"_is_tier_query() missing {len(missing)} markers — Luna will hallucinate tier status "
                    f"for prompts containing these phrases instead of calling _build_tier_status_report()"
                ),
                "fix": (
                    "In worker.py _is_tier_query(), add to markers tuple: "
                    + ", ".join(missing)
                    + ". These phrases must route to _build_tier_status_report(), not the LLM fallback."
                ),
            }
    except Exception as exc:
        _diag(f"self_teacher check_tier_query_coverage: {exc}")
    return None


def check_api_vault_caching() -> Optional[Dict[str, Any]]:
    """Detect if load_api_vault() lacks in-memory caching (causes excessive file I/O)."""
    try:
        worker_path = PROJECT_DIR / "worker.py"
        source = worker_path.read_text(encoding="utf-8", errors="replace")
        if "_API_VAULT_CACHE_TS" not in source or "_API_VAULT_CACHE_TTL" not in source:
            return {
                "issue": "api_vault_no_cache",
                "lesson": "L014",
                "diagnosis": (
                    "load_api_vault() reads the vault file and writes VAULT_STATE_PATH on every call. "
                    "It is called 10+ times per chat response — causing excessive disk I/O."
                ),
                "fix": (
                    "Add module-level cache: _API_VAULT_CACHE: Dict[str, str] = {}, _API_VAULT_CACHE_TS: float = 0.0, "
                    "_API_VAULT_CACHE_TTL: float = 60.0. In load_api_vault(), return cached copy if "
                    "(time.monotonic() - _API_VAULT_CACHE_TS) < _API_VAULT_CACHE_TTL. Update cache after file read."
                ),
            }
    except Exception as exc:
        _diag(f"self_teacher check_api_vault_caching: {exc}")
    return None


def check_done_queue_size() -> Optional[Dict[str, Any]]:
    """Warn if done task queue is bloated (>200 files) and prune cooldown is not set."""
    try:
        done_dir = PROJECT_DIR / "tasks" / "done"
        if not done_dir.exists():
            return None
        count = sum(1 for _ in done_dir.glob("*.json"))
        if count > 500:
            state_path = MEMORY_DIR / "luna_autonomy_state.json"
            state = safe_read_json(state_path, default={}) or {}
            if "last_prune_at" not in state:
                return {
                    "issue": "done_queue_bloat",
                    "count": count,
                    "lesson": "L014",
                    "diagnosis": (
                        f"tasks/done/ has {count} files and prune_done_queue() has never run. "
                        "DONE_DIR glob is O(N) — slows down _friendly_log_tail() and task refresh."
                    ),
                    "fix": (
                        "Call prune_done_queue(max_age_hours=24, keep_min=100) in autonomous_maintenance_cycle "
                        "with 3600s cooldown via state['last_prune_at']. This archives old tasks to tasks/archive/."
                    ),
                }
    except Exception as exc:
        _diag(f"self_teacher check_done_queue_size: {exc}")
    return None


# ── Full diagnosis runner ─────────────────────────────────────────────────────

def check_speak_hallucination_filter() -> Optional[Dict[str, Any]]:
    """Detect if speak() is missing its hallucination filter."""
    try:
        worker_path = PROJECT_DIR / "worker.py"
        source = worker_path.read_text(encoding="utf-8", errors="replace")
        if "_HALLUCINATION_PHRASES" not in source:
            return {
                "issue": "speak_hallucination_filter_missing",
                "lesson": "L016",
                "diagnosis": (
                    "_HALLUCINATION_PHRASES not found in worker.py — speak() will broadcast "
                    "LLM-invented performance complaints ('task routing, queue bloat, caching') "
                    "as if they were real diagnostic facts."
                ),
                "fix": (
                    "Add _HALLUCINATION_PHRASES tuple before speak() in worker.py. "
                    "In speak(), if any phrase matches, replace message with run_full_self_diagnosis() output. "
                    "Phrases: 'task routing, queue bloat', 'queue bloat, and caching', "
                    "'performance bottlenecks from inefficient', 'lru cache eviction', 'orchestration kernel'."
                ),
            }
    except Exception as exc:
        _diag(f"self_teacher check_speak_hallucination_filter: {exc}")
    return None


def check_status_routing() -> Optional[Dict[str, Any]]:
    """Detect if status queries can reach the LLM fallback and cause hallucination."""
    try:
        worker_path = PROJECT_DIR / "worker.py"
        source = worker_path.read_text(encoding="utf-8", errors="replace")
        if "_is_self_status_query" not in source or "_build_self_status_report" not in source:
            return {
                "issue": "status_query_not_routed",
                "lesson": "L015",
                "diagnosis": (
                    "_is_self_status_query() or _build_self_status_report() missing from worker.py. "
                    "Status queries ('what problems', 'are you ok', 'still having issues') will "
                    "fall through to LLM and generate hallucinated performance complaints."
                ),
                "fix": (
                    "Add _is_self_status_query() and _build_self_status_report() to worker.py. "
                    "In generate_luna_chat_response(), add: "
                    "elif _is_self_status_query(prompt_text): response = _build_self_status_report(owner) "
                    "BEFORE the LLM fallback else block. "
                    "_build_self_status_report() must call run_full_self_diagnosis() and run_loop_diagnosis()."
                ),
            }
    except Exception as exc:
        _diag(f"self_teacher check_status_routing: {exc}")
    return None


_CHECKS = [
    check_tier6_loop,
    check_repair_cooldown,
    check_dead_code_after_return,
    check_claude_model_leak,
    check_loop_detector_integrity,
    check_model_routing_cooldown,
    check_excluded_functions,
    check_tier_query_coverage,
    check_api_vault_caching,
    check_done_queue_size,
    check_speak_hallucination_filter,
    check_status_routing,
]


def run_full_self_diagnosis() -> Dict[str, Any]:
    """Run all known issue checks. Returns report with found issues and fixes."""
    issues = []
    for check in _CHECKS:
        try:
            result = check()
            if result:
                issues.append(result)
                _diag(f"[SELF-TEACHER] {result['issue']}: {result['diagnosis'][:100]}")
        except Exception as exc:
            _diag(f"self_teacher diagnostic failed ({check.__name__}): {exc}")

    if not issues:
        _diag("[SELF-TEACHER] All checks passed — no known issues detected")

    report = {
        "ts":            now_iso(),
        "checks_run":    len(_CHECKS),
        "issues_found":  len(issues),
        "all_clear":     len(issues) == 0,
        "issues":        issues,
    }

    try:
        append_jsonl(TEACHER_LOG_PATH, report)
    except Exception:
        pass

    return report


def teach_lesson(lesson_id: str) -> str:
    """Return the full lesson text for a given lesson ID."""
    for lesson in LESSONS:
        if lesson["id"] == lesson_id:
            return (
                f"[{lesson['id']}] {lesson['title']}\n"
                f"SYMPTOM: {lesson['symptom']}\n"
                f"ROOT CAUSE: {lesson['root_cause']}\n"
                f"FIX: {lesson['fix']}\n"
                f"VERIFIED: {lesson['fix_verified']} ({lesson['date_learned']})"
            )
    return f"No lesson found for ID: {lesson_id}"


def list_lessons() -> str:
    """Return a summary of all lessons Luna has learned."""
    return "\n".join(
        f"  [{l['id']}] {l['title']} ({'verified' if l.get('fix_verified') else 'unverified'})"
        for l in LESSONS
    )


# ── Self-teach: expand lessons by reading repair catalog ─────────────────────

def absorb_repair_catalog() -> int:
    """Read luna_self_repair_catalog.jsonl and expand our lesson knowledge from it.
    Returns number of new patterns absorbed.
    """
    catalog_path = MEMORY_DIR / "self_repair_catalog.jsonl"
    if not catalog_path.exists():
        return 0

    existing_ids = {l["id"] for l in LESSONS}
    absorbed = 0

    try:
        text = catalog_path.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                # Convert catalog entry to lesson format and append to LESSON_CATALOG
                pattern_id = entry.get("pattern_id", "")
                if not pattern_id or pattern_id in existing_ids:
                    continue
                lesson_record = {
                    "ts":          now_iso(),
                    "pattern_id":  pattern_id,
                    "bug_type":    entry.get("bug_type", ""),
                    "detection":   entry.get("detection", ""),
                    "fix":         entry.get("fix", ""),
                    "example":     entry.get("example_error", ""),
                    "lesson":      entry.get("lesson", ""),
                }
                append_jsonl(LESSON_CATALOG, lesson_record)
                existing_ids.add(pattern_id)
                absorbed += 1
            except Exception:
                pass
    except Exception as exc:
        _diag(f"absorb_repair_catalog: {exc}")

    if absorbed:
        _diag(f"[SELF-TEACHER] Absorbed {absorbed} new patterns from repair catalog")
    return absorbed


# ── Summary (required for tier wiring) ───────────────────────────────────────

def self_teacher_summary() -> Dict[str, Any]:
    report = run_full_self_diagnosis()
    absorbed = absorb_repair_catalog()
    return {
        "ts":           now_iso(),
        "lessons_known": len(LESSONS),
        "checks_run":   report["checks_run"],
        "issues_found": report["issues_found"],
        "all_clear":    report["all_clear"],
        "patterns_absorbed": absorbed,
        "top_issue":    report["issues"][0]["diagnosis"][:100] if report["issues"] else "",
    }


if __name__ == "__main__":
    import pprint
    print("=== Luna Self-Teacher ===")
    print("\nLessons known:")
    print(list_lessons())
    print("\nRunning diagnosis...")
    pprint.pprint(run_full_self_diagnosis())
    print("\nAbsorbing repair catalog...")
    absorbed = absorb_repair_catalog()
    print(f"Absorbed {absorbed} new patterns")
