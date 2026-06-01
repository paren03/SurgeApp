"""Silent verification of the Professional-default persona (no voice, no LLM)."""
import sys
sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_personality_runtime as p  # noqa: E402

BANNED = ["baby", "honey", "handsome", "darling", "my love", "sweetie",
          "sweetheart", "babe", "sexy", "gorgeous", "daddy", "master",
          "good boy", " sir"]
SAMPLE = ("The dashboard fix is committed and the latency is down. "
          "Let me know what you want to tackle next.")
INTENTS = ["greet", "acknowledge", "reassure", "focus", "answer",
           "boot", "boot_degraded", "fallback"]


def main() -> None:
    r = p.set_mode("professional")
    print("set_mode(professional):", r.get("ok"), "-> current:",
          p.current_mode())
    print("DEFAULT_MODE:", p.DEFAULT_MODE)

    leaks = []
    for it in INTENTS:
        for _ in range(12):  # templates are random + probabilistic
            out = p.shape_for_speech(SAMPLE, intent=it).get("shaped_text", "")
            low = out.lower()
            for b in BANNED:
                if b in low:
                    leaks.append((it, b.strip(), out))
    print(f"professional pet-name leaks across {len(INTENTS)*12} samples:",
          len(leaks))
    for x in leaks[:8]:
        print("   LEAK:", x)

    print("\nswitch detection:")
    print("  'Bad Luna on'   ->", p.detect_mode_switch_in("Bad Luna on"))
    print("  'Bad Girl on'   ->", p.detect_mode_switch_in("Bad Girl on"))
    print("  'Good Luna'     ->", p.detect_mode_switch_in("Good Luna"))
    print("  'professional'  ->", p.detect_mode_switch_in("professional"))
    print("  'hey help me'   ->", p.detect_mode_switch_in("hey help me"))

    # A couple of example professional lines:
    print("\nexample professional greet:",
          repr(p.shape_for_speech("", intent="greet").get("shaped_text")))
    print("example professional ack:",
          repr(p.shape_for_speech("", intent="acknowledge").get("shaped_text")))
    print("\ncurrent_mode (unchanged by detect):", p.current_mode())
    print("VERDICT:", "PASS - no endearments in professional"
          if not leaks else f"FAIL - {len(leaks)} leaks")


if __name__ == "__main__":
    main()
