"""Silent check: prompt builders are professional (no pet names) + inject vocab.

Builds prompt STRINGS only (no model load, no voice)."""
import sys

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_conversation_runtime as crt  # noqa: E402

BANNED = ["ai girl", "pet name", "baby", "handsome", "darling", "my love",
          "affectionate", "loving"]
SAMPLE = "Tell me about freedom and knowledge"
CLF = {"category": "casual", "text_chars": len(SAMPLE)}


def _check(label: str, prompt: str) -> None:
    low = prompt.lower()
    leaks = [b for b in BANNED if b in low]
    has_vocab = "bilingual dictionary" in low
    first = next((ln for ln in prompt.splitlines()
                  if ln.startswith("You are Luna")), "")
    print(f"[{label}] persona_leaks={leaks or 'NONE'}  vocab_block={has_vocab}")
    print(f"        intro: {first}")


# V1 fallback prompt (pure function)
_check("V1 _build_main_prompt", crt._build_main_prompt(
    classification=CLF, recent_turns=[], mode="good_luna", text=SAMPLE))

# Active sovereign prompt (build string only; singleton ctor is lazy re: model)
try:
    from luna_modules import cognitive_sovereign_main_runtime as smr  # noqa
    sp = smr.get_singleton()._build_prompt(
        classification=CLF, mode="good_luna", incoming=SAMPLE,
        recent_turns=[], context_pack=None)
    _check("SOVEREIGN _build_prompt", sp)
except Exception as exc:  # noqa: BLE001
    print("sovereign build error:", type(exc).__name__, exc)
