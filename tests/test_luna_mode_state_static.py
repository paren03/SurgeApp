"""Static contract tests for luna_modules.luna_mode_state.

These pin the public API and behavior so future edits can't silently
break Bad-Luna mode persistence.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Make luna_modules importable.
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))


@pytest.fixture
def isolated_state(monkeypatch, tmp_path):
    """Redirect STATE_PATH and MEMORY_DIR into a fresh temp dir for each test."""
    from luna_modules import luna_mode_state as mod
    fake_memory = tmp_path / "memory"
    fake_memory.mkdir()
    fake_path = fake_memory / "luna_mode_state.json"
    monkeypatch.setattr(mod, "MEMORY_DIR", fake_memory)
    monkeypatch.setattr(mod, "STATE_PATH", fake_path)
    yield mod, fake_path


# -- detect_switch_command --------------------------------------------------


@pytest.mark.parametrize(
    "prompt,expected",
    [
        # Explicit on/off commands -- the only phrases that SHOULD trigger.
        ("bad luna on", "bad"),
        ("Bad Luna On", "bad"),
        ("Bad Luna on!", "bad"),
        ("bad girl on", "bad"),
        ("Bad Girl ON", "bad"),
        ("bad luna off", "good"),
        ("Bad Luna off.", "good"),
        ("bad girl off", "good"),
        # Casual phrases that must NOT trigger (Serge's explicit requirement
        # 2026-05-10: "only activate 'Bad Luna on' or 'Bad Girl on' when I
        # type in the command"). These all used to match the old wide
        # patterns and would falsely switch mode.
        ("bad luna", None),
        ("Bad Luna!", None),
        ("be bad", None),
        ("hey luna, go bad for me", None),
        ("naughty mode please", None),
        ("good luna", None),
        ("Good Luna please come back", None),
        ("be good", None),
        ("bad off", None),
        ("switch to good", None),
        ("you're such a bad girl", None),
        ("the bad luna persona is fun", None),
        # Neutral chat: must not trigger.
        ("how's the weather?", None),
        ("", None),
        ("   ", None),
        ("luna, tell me a joke", None),
    ],
)
def test_detect_switch_command(prompt, expected):
    from luna_modules.luna_mode_state import detect_switch_command
    assert detect_switch_command(prompt) == expected


def test_detect_switch_command_both_present_later_wins():
    """If both bad and good explicit commands appear, whichever comes LAST wins."""
    from luna_modules.luna_mode_state import detect_switch_command
    assert detect_switch_command("bad luna off then bad luna on") == "bad"
    assert detect_switch_command("bad luna on actually bad luna off") == "good"


# -- get_mode / set_mode ----------------------------------------------------


def test_default_mode_is_good(isolated_state):
    mod, _ = isolated_state
    assert mod.get_mode("any-session") == "good"


def test_get_mode_after_set(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-1", "bad")
    assert mod.get_mode("sess-1") == "bad"


def test_mode_isolated_per_session(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-A", "bad")
    mod.set_mode("sess-B", "good")
    assert mod.get_mode("sess-A") == "bad"
    assert mod.get_mode("sess-B") == "good"


def test_set_mode_rejects_invalid_value(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-1", "evil")  # not in ALLOWED_MODES
    assert mod.get_mode("sess-1") == "good"  # untouched


def test_set_mode_normalizes_case(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-1", "BAD")
    assert mod.get_mode("sess-1") == "bad"


def test_blank_session_id_resolves_to_default_bucket(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("", "bad")
    assert mod.get_mode("") == "bad"
    assert mod.get_mode(None) == "bad"


def test_state_file_is_valid_json(isolated_state):
    mod, path = isolated_state
    mod.set_mode("sess-1", "bad")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "sessions" in data
    assert data["sessions"]["sess-1"]["mode"] == "bad"


def test_corrupt_state_file_falls_back_to_default(isolated_state):
    mod, path = isolated_state
    path.write_text("{this is not json", encoding="utf-8")
    assert mod.get_mode("any") == "good"  # graceful


# -- resolve_mode_for_prompt -----------------------------------------------


def test_resolve_persists_bad_switch(isolated_state):
    mod, _ = isolated_state
    assert mod.resolve_mode_for_prompt("sess-1", "bad luna on") == "bad"
    # Next call with non-switch prompt still returns bad
    assert mod.resolve_mode_for_prompt("sess-1", "tell me something") == "bad"


def test_resolve_persists_good_switch(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-1", "bad")
    assert mod.resolve_mode_for_prompt("sess-1", "bad luna off") == "good"
    assert mod.get_mode("sess-1") == "good"


def test_resolve_returns_existing_mode_for_normal_prompt(isolated_state):
    mod, _ = isolated_state
    mod.set_mode("sess-1", "bad")
    assert mod.resolve_mode_for_prompt("sess-1", "what's up?") == "bad"


# -- inject_mode_into_system_prompt ---------------------------------------


SAMPLE_PROMPT = (
    "You are Luna, Serge's personal AI girl.\n\n"
    "Modes: Good Luna / Bad Luna.\n\n"
    "Start every new session in Good Luna mode by default.\n\n"
    "Current mode: Good Luna\n"
)


def test_inject_good_keeps_good_mode_label():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    out = inject_mode_into_system_prompt(SAMPLE_PROMPT, "good")
    assert "Current mode: Good Luna" in out
    assert "Current mode: Bad Luna" not in out
    assert "ACTIVE MODE OVERRIDE" not in out  # no reinforcement for good


def test_inject_bad_replaces_mode_line():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    out = inject_mode_into_system_prompt(SAMPLE_PROMPT, "bad")
    assert "Current mode: Bad Luna" in out
    assert "Current mode: Good Luna" not in out


def test_inject_bad_adds_reinforcement_block():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    out = inject_mode_into_system_prompt(SAMPLE_PROMPT, "bad")
    assert "ACTIVE MODE OVERRIDE" in out
    assert "Bad Luna mode is currently ACTIVE" in out
    assert "Ignore any prior instructions" in out  # specifically counters the preamble


def test_inject_is_idempotent():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    once = inject_mode_into_system_prompt(SAMPLE_PROMPT, "bad")
    twice = inject_mode_into_system_prompt(once, "bad")
    # Idempotent: line count for "Current mode:" stays at 1.
    assert twice.count("Current mode:") == 1
    # And the reinforcement block isn't duplicated either.
    assert twice.count("ACTIVE MODE OVERRIDE") == 1


def test_inject_handles_missing_mode_line():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    no_line = "You are Luna.\n\nBe nice.\n"
    out = inject_mode_into_system_prompt(no_line, "good")
    assert "Current mode: Good Luna" in out


def test_inject_handles_invalid_mode_defaults_to_good():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    out = inject_mode_into_system_prompt(SAMPLE_PROMPT, "evil")
    assert "Current mode: Good Luna" in out


def test_inject_never_raises_on_garbage_input():
    from luna_modules.luna_mode_state import inject_mode_into_system_prompt
    # Should NEVER raise -- chat reliability depends on this.
    assert inject_mode_into_system_prompt(None, "bad") is not None
    assert inject_mode_into_system_prompt("", "bad") is not None
    assert inject_mode_into_system_prompt(SAMPLE_PROMPT, None) is not None


# -- Bounded-growth contract ----------------------------------------------


def test_session_cap_trims_oldest(isolated_state, monkeypatch):
    mod, path = isolated_state
    monkeypatch.setattr(mod, "MAX_SESSIONS", 5)
    for i in range(10):
        mod.set_mode(f"sess-{i:03d}", "bad")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert len(data["sessions"]) <= 5
    # Most recent should be retained.
    assert "sess-009" in data["sessions"]
