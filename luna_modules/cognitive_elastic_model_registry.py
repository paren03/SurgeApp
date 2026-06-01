"""Declarative model-tier registry for the Elastic Brain. Read-only over a
JSON file. NEVER raises."""
from __future__ import annotations
import json
import os
from typing import Any, Dict, Optional

ROOT = r"D:\SurgeApp"
LIBRARY_PATH = os.path.join(ROOT, "memory", "elastic_brain", "model_library.json")
_DEFAULT_MODEL_DIR = os.path.join(ROOT, "local_models")


def load_library() -> Dict[str, Any]:
    try:
        with open(LIBRARY_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        tiers = data.get("tiers") or {}
        if not isinstance(tiers, dict):
            return {"ok": False, "tiers": {}, "error": "tiers_not_dict"}
        return {"ok": True,
                "model_dir": data.get("model_dir") or _DEFAULT_MODEL_DIR,
                "tiers": tiers}
    except FileNotFoundError:
        return {"ok": False, "tiers": {}, "error": "library_missing"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "tiers": {}, "error": f"{type(exc).__name__}"}


def get_tier(name: str) -> Optional[Dict[str, Any]]:
    try:
        lib = load_library()
        t = (lib.get("tiers") or {}).get(name)
        if not isinstance(t, dict):
            return None
        out = dict(t)
        out.setdefault("model_dir", lib.get("model_dir") or _DEFAULT_MODEL_DIR)
        out.setdefault("device", "cpu")
        out.setdefault("ngl", 0)
        out.setdefault("n_ctx", 2048)
        out["tier"] = name
        return out
    except Exception:  # noqa: BLE001
        return None


def tier_weights_present(name: str) -> bool:
    t = get_tier(name)
    if not t:
        return False
    try:
        return os.path.isfile(os.path.join(t["model_dir"], t["model_name"]))
    except Exception:  # noqa: BLE001
        return False


def report() -> Dict[str, Any]:
    lib = load_library()
    tiers = lib.get("tiers") or {}
    return {
        "module": "cognitive_elastic_model_registry",
        "library_path": LIBRARY_PATH,
        "ok": lib.get("ok", False),
        "tier_names": sorted(tiers.keys()),
        "tiers_present": {k: tier_weights_present(k) for k in tiers},
    }


__all__ = ["load_library", "get_tier", "tier_weights_present", "report"]
