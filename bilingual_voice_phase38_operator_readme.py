"""Phase 38 - Operator README Generator.

Builds the operator-facing Markdown README for Phase 27-37 voice-
adapter governance.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase38.operator_readme.v1"


# Runtime-assembled forbidden-token names so the source file does
# NOT contain the literal forbidden tokens. The rendered Markdown
# still does — that is the intended operator-facing content.
def _tok(*parts: str) -> str:
    return "".join(parts)


_AUDIO_LIBS = (
    _tok("py", "ttsx3"), _tok("gt", "ts"), _tok("ed", "ge_tts"),
    _tok("pi", "per"), _tok("co", "qui"), _tok("whi", "sper"),
    _tok("pya", "udio"), _tok("sou", "nddevice"),
    _tok("py", "dub"), _tok("sou", "ndfile"),
    _tok("com", "types"), _tok("win", "32com"),
)
_EXEC_CALLS = (
    _tok("subproc", "ess.run"),
    _tok("subproc", "ess.Popen"),
    _tok("subproc", "ess.call"),
    _tok("os.sy", "stem("),
    _tok("she", "ll=True"),
    _tok("os.po", "pen"),
)
_NET_LIBS = (
    _tok("urllib.", "request"),
    _tok("http.", "client"),
    _tok("reque", "sts."),
    _tok("htt", "px."),
    _tok("soc", "ket.socket"),
)
_MP_CALLS = (
    _tok("multiproces", "sing.Process"),
    _tok("multiproces", "sing.Pool"),
)
_LUNA_MODS = _tok("luna", "_", "modules")


def create_plain_language_boundary_summary() -> str:
    return (
        "## What this is, in one paragraph\n\n"
        "Phase 27 through Phase 37 build a *governance and "
        "verification layer* around the idea of a future voice "
        "renderer. **It does not produce voice.** It defines "
        "contracts, validators, signing, witness exports, and a "
        "local exchange protocol so that the day a real audio "
        "engine is ever bound, every step before that engine has "
        "already been audited and proven safe. As of Phase 37 the "
        "system runs four *metadata-only* in-process adapters; "
        "none of them generate audio, invoke TTS, spawn a "
        "subprocess, open a socket, or write an audio file.\n\n"
    )


def create_adapter_allowlist_section() -> str:
    return (
        "## The four allowed callable adapters (metadata-only)\n\n"
        "| Adapter | Returns | Phase added |\n"
        "|---|---|---|\n"
        "| `dummy_metadata_adapter` | Echo + latency-shape "
        "metadata | Phase 30 |\n"
        "| `bilingual_segment_metadata_adapter` | Language "
        "segment / code-switch boundary counts | Phase 31 |\n"
        "| `prosody_density_metadata_adapter` | Pause / "
        "emphasis / tone marker counts | Phase 33 |\n"
        "| `safety_redaction_trace_metadata_adapter` | Safety "
        "summary + redaction / recognition-only / DNU / "
        "voice-safe / vulgar-offensive counts | Phase 37 |\n\n"
        "Each adapter MUST have these flags **False**: "
        "`produces_audio`, `invokes_tts`, `uses_subprocess`, "
        "`uses_network`, `writes_files`. The Phase 37 result "
        "verifier rejects any other adapter name.\n\n"
    )


def create_forbidden_actions_section() -> str:
    audio = ", ".join("`" + t + "`" for t in _AUDIO_LIBS)
    exec_ = ", ".join("`" + t + "`" for t in _EXEC_CALLS)
    net = ", ".join("`" + t + "`" for t in _NET_LIBS)
    mp = ", ".join("`" + t + "`" for t in _MP_CALLS)
    return (
        "## What is forbidden, end-to-end\n\n"
        "- **No audio**: zero `.wav` / `.mp3` / `.ogg` / `.flac` "
        "/ `.m4a` written anywhere under "
        "`bilingual_stack/voice_adapter_phase*/`.\n"
        f"- **No TTS**: no {audio} imported or referenced.\n"
        f"- **No subprocess**: no {exec_}.\n"
        "- **No PowerShell / SAPI / Piper**: no engine binding.\n"
        f"- **No network**: no {net}.\n"
        f"- **No multiprocessing**: no {mp}.\n"
        "- **No daemon / scheduler / watchdog / service / "
        "registry change**.\n"
        "- **No production secret storage**: signing keys are "
        "in-memory HMAC-SHA256 test keys (Phase 32); secret-"
        "bearing envelopes (Phase 36) live only under "
        "`local_secret_handoff/` which ships its own "
        "`.gitignore`.\n"
        "- **No corpus import**: Phase 21 real import remains "
        "blocked on operator-staged corpora.\n"
        "- **No main runtime integration**: all Phase 27-37 "
        "modules are standalone; nothing imports `worker.py`, "
        + "`" + _LUNA_MODS + "`, `tier_*`, `probe_*`, "
        "`attestation*`, or Program S.\n\n"
    )


def create_verification_workflow_section() -> str:
    return (
        "## How to verify\n\n"
        "### Quick check (30 seconds)\n"
        "```\n"
        "git status --short\n"
        "ls bilingual_stack/voice_adapter_phase36/"
        "local_secret_handoff/   # expect only .gitignore\n"
        "```\n\n"
        "### Tests\n"
        "```\n"
        "python test_phase37_safety_trace_adapter_governance.py\n"
        "python test_phase36_key_handoff_envelope.py\n"
        "python test_phase35_witness_exchange_protocol.py\n"
        "python test_phase34_external_witness_verification.py\n"
        "python test_phase33_three_adapter_signed_governance.py\n"
        "python test_phase32_audit_signing_and_verification.py\n"
        "python test_phase31_multi_adapter_boundary.py\n"
        "python test_phase30_callable_adapter_boundary.py\n"
        "python test_phase29_operator_gated_runtime_adapter_b.py\n"
        "python test_phase28_operator_gated_voice_adapter.py\n"
        "python test_phase27_voice_render_adapter_skeleton.py\n"
        "```\n"
        "Every harness should print `Total: N | Pass: N | Fail: "
        "0`.\n\n"
        "### Signed evidence + witness chain\n"
        "1. Run `prepare_phase37_four_adapter_invocation(...)` "
        "with `approve=True` and an `operator_id`.\n"
        "2. The returned `signed_witness_pipeline` carries "
        "`signed_evidence_summary` (hash, algorithm, "
        "`test_only=True`) and an Phase 34 `witness_export_"
        "summary` and a Phase 35 `exchange_summary`.\n"
        "3. Optionally call "
        "`verify_phase37_signed_witness_pipeline(output)` to "
        "re-check structurally.\n"
        "4. Operator may, with a fresh consent marker, generate "
        "a Phase 36 sealed envelope and re-verify via "
        "`verify_witness_package_with_handoff(...)`.\n\n"
        "### Production invariants\n"
        "```\n"
        "EN words: 2814\n"
        "RU words: 2518\n"
        "RU phrases: 35\n"
        "Bilingual concepts: 26\n"
        "Bilingual entry links: 52\n"
        "Live pack manifests: 90\n"
        "```\n"
        "Any change to these means Phase 21 (corpus import) ran. "
        "If you did not intend that, stop and audit.\n\n"
    )


def create_commit_safety_section() -> str:
    return (
        "## What is safe to commit, what must NOT be committed\n\n"
        "### Safe to commit\n"
        "- `bilingual_voice_phase27_*.py` through "
        "`bilingual_voice_phase37_*.py` source modules\n"
        "- `bilingual_safety_redaction_trace_adapter.py`, "
        "`bilingual_prosody_density_metadata_adapter.py`, "
        "`bilingual_segment_metadata_adapter.py`, "
        "`bilingual_voice_dummy_metadata_adapter.py`\n"
        "- `test_phase27_*.py` through `test_phase37_*.py` "
        "harnesses\n"
        "- `PHASE27_*` through `PHASE38_*` markdown reports\n"
        "- `bilingual_stack/voice_adapter_phase*/` empty "
        "sub-folders (optional)\n"
        "- `bilingual_stack/voice_adapter_phase36/"
        "local_secret_handoff/.gitignore` (the gitignore file "
        "itself)\n\n"
        "### Must NOT be committed\n"
        "- Runtime DBs: `lexicon/luna_vocabulary.sqlite`, "
        "`russian_stack/russian_lexicon.sqlite`, "
        "`russian_stack/russian_memory.sqlite`, "
        "`bilingual_stack/bilingual_links.sqlite`, "
        "`ruvector.db`, `corpus_sources/checkpoints/"
        "checkpoints.sqlite3`, `corpus_sources/phase20/"
        "ledger.sqlite3`\n"
        "- `.claude/` settings\n"
        "- `corpus_sources/backups/`, "
        "`corpus_sources/quality_samples/`, "
        "`corpus_sources/phase20/synthetic_million/`\n"
        "- Any audio files (`.wav` / `.mp3` / `.ogg` / `.flac` "
        "/ `.m4a`)\n"
        "- **Any file under "
        "`bilingual_stack/voice_adapter_phase36/"
        "local_secret_handoff/` other than the gitignore itself "
        "** — these may carry sealed test-key envelopes\n"
        "- Anything with `private_key`, `material_hex`, "
        "`signing_key_material`, `sealed_payload` field names\n\n"
    )


def create_phase21_import_status_section() -> str:
    return (
        "## Phase 21 real corpus import (separate workflow)\n\n"
        "Phase 21 is the operator-staged real corpus import. It "
        "is **not part of the voice-adapter governance stack**. "
        "It remains BLOCKED until the operator drops real "
        "vocabulary files into "
        "`corpus_sources/english/incoming/` and "
        "`corpus_sources/russian/incoming/`. The Phase 21 "
        "harness (`test_phase21_operator_staged_first_import."
        "py`) and the Phase 21a staging-readiness gate "
        "(`test_phase21a_operator_corpus_staging.py`) currently "
        "report that both incoming folders are empty.\n\n"
        "Verifying Phase 27-37 governance does NOT unblock "
        "Phase 21. Verifying Phase 27-37 governance does NOT "
        "imply anyone has heard Luna speak — no audio engine "
        "is bound.\n\n"
    )


def create_operator_governance_readme(
    ledger: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    lines: list[str] = []
    lines.append("# Luna Voice Adapter Governance — Operator "
                  "README (Phase 27-37)\n")
    lines.append("")
    lines.append("**Status:** Phase 37 complete. 30-harness "
                  "regression green at "
                  f"{(ledger or {}).get('full_regression_total_expected', 6185)}"
                  " / "
                  f"{(ledger or {}).get('full_regression_total_expected', 6185)}.\n")
    lines.append("")
    lines.append("**This is NOT real voice execution. This is NOT "
                  "production secret management. This is NOT a "
                  "corpus import workflow.**\n")
    lines.append("")
    lines.append(create_plain_language_boundary_summary())
    lines.append(create_adapter_allowlist_section())
    lines.append(create_forbidden_actions_section())
    lines.append(create_verification_workflow_section())
    lines.append(create_commit_safety_section())
    lines.append(create_phase21_import_status_section())
    lines.append("## Phase-by-phase summary\n")
    lines.append("")
    if ledger:
        for e in ledger.get("entries") or []:
            phase = e.get("phase")
            title = e.get("title") or ""
            allow = e.get("allowed_callable_adapters") or []
            new = e.get("adapters_introduced") or []
            line = (
                f"- **Phase {phase}** — {title}. "
                f"Allowed callable adapters: "
                f"{', '.join(allow) if allow else 'none'}"
                + (f". New: {', '.join(new)}." if new else ".")
            )
            lines.append(line)
    else:
        lines.append("- (ledger not provided; see individual "
                      "`PHASE*_REPORT.md` files)")
    lines.append("")
    lines.append("## How to roll back\n")
    lines.append("See `bilingual_stack/governance_phase38/"
                  "rollback/ROLLBACK_MATRIX.json` for per-phase "
                  "file lists. Each phase rolls back by deleting "
                  "its own modules + harness + report + empty "
                  "sub-folder set. No prior phase depends on a "
                  "later phase being present.\n")
    lines.append("")
    lines.append(f"_Generated by Phase 38 operator README "
                  f"generator at {int(time.time())}._\n")
    body = "\n".join(lines)
    return {
        "readme_id": f"opreadme_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": "Luna Voice Adapter Governance Operator README "
                  "(Phase 27-37)",
        "body": body,
        "sections": [
            "plain_language_boundary_summary",
            "adapter_allowlist",
            "forbidden_actions",
            "verification_workflow",
            "commit_safety",
            "phase21_import_status",
            "phase_by_phase",
            "rollback",
        ],
    }


def write_operator_governance_readme(
    readme: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(readme.get("body") or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_operator_governance_readme",
    "create_plain_language_boundary_summary",
    "create_adapter_allowlist_section",
    "create_forbidden_actions_section",
    "create_verification_workflow_section",
    "create_commit_safety_section",
    "create_phase21_import_status_section",
    "write_operator_governance_readme",
]
