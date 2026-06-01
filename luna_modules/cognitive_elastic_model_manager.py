"""Energy-aware elastic model manager. Loads a model tier on demand, keeps at
most one heavy model resident (one-hot), and unloads to free GPU/CPU memory.
The real loader uses gpt4all; a loader callable is injectable for tests.
NEVER raises from public methods."""
from __future__ import annotations
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

from luna_modules import cognitive_elastic_model_registry as _reg

ROOT = r"D:\SurgeApp"
_HEAVY_TIERS = {"M", "L", "XL", "BATCH"}   # S is light; may coexist


class _LlamaCppHandle:
    """Uniform handle around a llama_cpp.Llama so callers use the same
    .generate(prompt, max_tokens) -> str and .close() regardless of backend.
    NEVER raises."""

    def __init__(self, llm: Any):
        self._llm = llm

    def generate(self, prompt: str, max_tokens: int = 64, **kw: Any) -> str:
        try:
            out = self._llm.create_completion(
                str(prompt), max_tokens=int(max_tokens),
                temperature=float(kw.get("temp", kw.get("temperature", 0.0))))
            return out["choices"][0]["text"]
        except Exception:  # noqa: BLE001
            return ""

    def close(self) -> None:
        try:
            c = getattr(self._llm, "close", None)
            if callable(c):
                c()
        except Exception:  # noqa: BLE001
            pass


def _default_loader(tier: Dict[str, Any]):
    """Build a real model for a tier, honoring device + ngl so the energy
    manager controls GPU/CPU placement. Prefers llama-cpp-python (GPU-capable
    via n_gpu_layers); falls back to gpt4all if a tier asks for it. Returns a
    handle exposing .generate()/.close(), or None. NEVER raises."""
    path = os.path.join(tier["model_dir"], tier["model_name"])
    if not os.path.isfile(path):
        return None
    backend = str(tier.get("backend", "llama_cpp"))

    if backend == "llama_cpp":
        try:
            from llama_cpp import Llama  # GPU-capable; bundled CUDA backend
        except Exception:  # noqa: BLE001
            return None
        # eco/cpu tiers set device=cpu -> ngl 0; gpu tiers offload `ngl` layers.
        ngl = (int(tier.get("ngl", 0))
               if str(tier.get("device")) == "gpu" else 0)
        try:
            llm = Llama(model_path=path, n_gpu_layers=ngl,
                        n_ctx=int(tier.get("n_ctx", 2048)),
                        n_threads=int(tier.get("n_threads", 8)),
                        verbose=False)
            return _LlamaCppHandle(llm)
        except Exception:  # noqa: BLE001
            return None

    # Legacy gpt4all backend (CPU-only on this host) — kept as a fallback.
    try:
        import gpt4all  # local import: heavy, optional
    except Exception:  # noqa: BLE001
        return None
    kw: Dict[str, Any] = {
        "model_name": tier["model_name"],
        "model_path": tier["model_dir"],
        "allow_download": False,
        "n_threads": int(tier.get("n_threads", 8)),
    }
    if str(tier.get("device")) == "gpu":
        kw["device"] = "gpu"
        kw["ngl"] = int(tier.get("ngl", 0))
    try:
        return gpt4all.GPT4All(**kw)
    except Exception:  # noqa: BLE001
        return None


class ElasticModelManager:
    def __init__(self, loader: Optional[Callable[[Dict[str, Any]], Any]] = None):
        self._loader = loader or _default_loader
        self._lock = threading.RLock()
        self._resident_tier: Optional[str] = None
        self._llm: Any = None
        self._loaded_at: Optional[float] = None
        self._last_use: Optional[float] = None
        self._mode_override: Optional[str] = None

    # ---- introspection ---- #
    def resident_tier(self) -> Optional[str]:
        with self._lock:
            return self._resident_tier

    # ---- energy mode ---- #
    def _energy_mode(self) -> str:
        if self._mode_override in ("eco", "balanced", "performance"):
            return self._mode_override
        try:
            from luna_modules import cognitive_feature_flags as ff
            flags = ff.read_flags()
            m = str(flags.get("cognitive_elastic_energy_mode", "balanced"))
            return m if m in ("eco", "balanced", "performance") else "balanced"
        except Exception:  # noqa: BLE001
            return "balanced"

    def set_energy_mode(self, mode: str) -> Dict[str, Any]:
        if mode not in ("eco", "balanced", "performance"):
            return {"ok": False, "error": "invalid_mode"}
        with self._lock:
            self._mode_override = mode
        return {"ok": True, "mode": mode}

    def resolve_placement(self, tier: Dict[str, Any],
                          mode: str = "balanced") -> Dict[str, Any]:
        """Adjust a tier's device/ngl for the energy mode. NEVER raises.
        eco         -> force CPU (no GPU spin; lowest power)
        balanced    -> tier as configured
        performance -> tier as configured (GPU as set)"""
        try:
            out = dict(tier)
            if mode == "eco":
                out["device"] = "cpu"
                out["ngl"] = 0
            return out
        except Exception:  # noqa: BLE001
            return dict(tier)

    # ---- lifecycle ---- #
    def _close_current(self) -> None:
        if self._llm is not None:
            try:
                close = getattr(self._llm, "close", None)
                if callable(close):
                    close()
            except Exception:  # noqa: BLE001
                pass
        self._llm = None
        self._resident_tier = None
        self._loaded_at = None

    def acquire(self, tier_name: str) -> Any:
        """Ensure the tier is loaded; return the model handle (or None)."""
        try:
            with self._lock:
                if self._resident_tier == tier_name and self._llm is not None:
                    self._last_use = time.time()
                    return self._llm
                tier = _reg.get_tier(tier_name)
                if tier is None:
                    return None
                # one-hot: evict any resident heavy model before loading another
                if self._llm is not None and (
                        tier_name in _HEAVY_TIERS or
                        self._resident_tier in _HEAVY_TIERS):
                    self._close_current()
                placed = self.resolve_placement(tier, mode=self._energy_mode())
                llm = self._loader(placed)
                if llm is None:
                    return None
                self._llm = llm
                self._resident_tier = tier_name
                self._loaded_at = time.time()
                self._last_use = time.time()
                return llm
        except Exception:  # noqa: BLE001
            return None

    def release(self, tier_name: Optional[str] = None) -> Dict[str, Any]:
        try:
            with self._lock:
                if tier_name is not None and tier_name != self._resident_tier:
                    return {"ok": True, "released": False,
                            "reason": "not_resident"}
                was = self._resident_tier
                self._close_current()
                return {"ok": True, "released": bool(was), "was": was}
        except Exception:  # noqa: BLE001
            return {"ok": False, "released": False}

    def reap_if_idle(self, idle_unload_s: float = 120.0) -> bool:
        """Unload the resident model if it has been idle longer than the
        budget. Returns True if it unloaded. NEVER raises."""
        try:
            with self._lock:
                if self._llm is None or self._last_use is None:
                    return False
                if (time.time() - self._last_use) >= float(idle_unload_s):
                    self._close_current()
                    return True
                return False
        except Exception:  # noqa: BLE001
            return False

    def report(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "module": "cognitive_elastic_model_manager",
                "resident_tier": self._resident_tier,
                "loaded": self._llm is not None,
                "loaded_at": self._loaded_at,
                "last_use": self._last_use,
                "energy_mode": self._energy_mode(),
            }


_SINGLETON: Optional[ElasticModelManager] = None
_SINGLETON_LOCK = threading.Lock()


def get_manager() -> ElasticModelManager:
    global _SINGLETON
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            _SINGLETON = ElasticModelManager()
        return _SINGLETON


def report() -> Dict[str, Any]:
    try:
        return get_manager().report()
    except Exception as exc:  # noqa: BLE001
        return {"module": "cognitive_elastic_model_manager",
                "error": f"{type(exc).__name__}"}


__all__ = ["ElasticModelManager", "get_manager", "report"]
