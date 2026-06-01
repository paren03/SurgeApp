"""Pre-render the fixed professional ack phrases in Serge's EN voice clone.

SILENT: uses XTTS tts_to_file (writes WAVs) and never plays audio. Produces
memory/voice_cache/cloned_acks/ack_clone_en_NN.wav + manifest.json, so the
conversation runtime can play an instant cloned ack instead of a slow live
synth. Run:
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\render_cloned_acks.py
"""
import json
import os
import sys

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_voice_xtts_adapter as xa  # noqa: E402

OUT = r"D:\SurgeApp\memory\voice_cache\cloned_acks"
EN_ACKS = ["Got it.", "On it.", "Sure.", "Understood.", "One sec.", "Will do."]


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    ad = xa.get_singleton()
    loaded = ad._ensure_model_loaded()
    print("xtts model loaded:", loaded)
    if not loaded:
        print("ABORT: model not loaded")
        return
    manifest = []
    for i, phrase in enumerate(EN_ACKS):
        fp = os.path.join(OUT, f"ack_clone_en_{i:02d}.wav")
        try:
            ad._tts.tts_to_file(text=phrase, file_path=fp,
                                speaker_wav=xa.DEFAULT_SPEAKER_REF,
                                language="en")
            ok = os.path.isfile(fp)
            print(f"rendered {phrase!r:18} -> {ok} "
                  f"({os.path.getsize(fp)//1024 if ok else 0} KB)")
            if ok:
                manifest.append({"phrase": phrase, "wav": fp, "lang": "en"})
        except Exception as exc:  # noqa: BLE001
            print(f"FAILED {phrase!r}: {type(exc).__name__}: {exc}")
    with open(os.path.join(OUT, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print("DONE:", len(manifest), "clips ->", OUT)


if __name__ == "__main__":
    main()
