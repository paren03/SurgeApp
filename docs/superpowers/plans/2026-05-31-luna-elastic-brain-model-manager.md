# Luna Elastic Brain — Plan 1: Model-Library Registry + Energy-Aware Manager

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Luna an energy-aware model manager that loads the right local model on demand, places it on GPU/CPU wisely, and unloads it when idle so the GPU powers down — working today with the models already in `local_models/`.

**Architecture:** A declarative JSON **registry** defines model tiers (S/M/L/XL/BATCH) with device/ngl/ctx hints. A **manager** singleton (`cognitive_elastic_model_manager.py`) loads a tier on demand via gpt4all, enforces a one-hot policy (only one heavy model resident), and runs an idle reaper that closes the model to free VRAM/RAM. A loader is **dependency-injected** so the logic is unit-testable without real weights or a GPU. All public APIs NEVER raise; flag-gated; `report()`-exposed; reversible.

**Tech Stack:** Python 3.11, `gpt4all` (existing loader API: `GPT4All(model_name, model_path, allow_download, n_threads, device, ngl)`), stdlib `threading`/`json`/`time`. Tests run under `D:\SurgeApp\.aider_venv\Scripts\python.exe`, written to run BOTH as pytest and standalone `__main__`.

This is **Plan 1 of a series** (decomposed from the spec): later plans cover the Difficulty Router, the Compressed Knowledge Vault, the operator/dashboard surface, and log-bloat hygiene. This plan stands alone and is testable on its own.

---

## File structure

- Create: `D:\SurgeApp\memory\elastic_brain\model_library.json` — tier registry (data).
- Create: `D:\SurgeApp\luna_modules\cognitive_elastic_model_registry.py` — load/validate registry.
- Create: `D:\SurgeApp\luna_modules\cognitive_elastic_model_manager.py` — energy-aware lifecycle.
- Create: `D:\SurgeApp\self_tests\test_elastic_model_registry.py` — registry tests.
- Create: `D:\SurgeApp\self_tests\test_elastic_model_manager.py` — manager tests (fake loader).
- Modify: `D:\SurgeApp\luna_modules\cognitive_feature_flags.py` — add 2 flags.

---

### Task 1: Model-library registry (data + loader)

**Files:**
- Create: `D:\SurgeApp\memory\elastic_brain\model_library.json`
- Create: `D:\SurgeApp\luna_modules\cognitive_elastic_model_registry.py`
- Test: `D:\SurgeApp\self_tests\test_elastic_model_registry.py`

- [ ] **Step 1: Write the registry data file**

Create `D:\SurgeApp\memory\elastic_brain\model_library.json` (only tiers whose weights exist today; XL/BATCH omitted until downloaded):

```json
{
  "schema": 1,
  "model_dir": "D:\\SurgeApp\\local_models",
  "tiers": {
    "S":  {"model_name": "Llama-3.2-1B-Instruct-Q4_0.gguf", "role": "fast",  "device": "cpu", "ngl": 0,  "n_ctx": 2048, "est_vram_mb": 0},
    "M":  {"model_name": "hermes3-8b-llama3.1.gguf",          "role": "daily", "device": "gpu", "ngl": 33, "n_ctx": 4096, "est_vram_mb": 5200},
    "L":  {"model_name": "qwen2.5-coder-7b-instruct.gguf",    "role": "coder", "device": "gpu", "ngl": 28, "n_ctx": 8192, "est_vram_mb": 5600}
  }
}
```

- [ ] **Step 2: Write the failing test**

Create `D:\SurgeApp\self_tests\test_elastic_model_registry.py`:

```python
import sys
sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_elastic_model_registry as reg


def test_load_returns_tiers():
    lib = reg.load_library()
    assert lib["ok"] is True
    assert set(["S", "M", "L"]).issubset(lib["tiers"].keys())


def test_get_tier_known():
    t = reg.get_tier("M")
    assert t is not None
    assert t["model_name"] == "hermes3-8b-llama3.1.gguf"
    assert t["device"] in ("cpu", "gpu")
    assert isinstance(t["ngl"], int)


def test_get_tier_unknown_returns_none():
    assert reg.get_tier("ZZZ") is None


def test_report_never_raises():
    out = reg.report()
    assert isinstance(out, dict)


if __name__ == "__main__":
    test_load_returns_tiers()
    test_get_tier_known()
    test_get_tier_unknown_returns_none()
    test_report_never_raises()
    print("REGISTRY_OK")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_registry.py"`
Expected: FAIL — `ModuleNotFoundError: cognitive_elastic_model_registry`.

- [ ] **Step 4: Write the registry module**

Create `D:\SurgeApp\luna_modules\cognitive_elastic_model_registry.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_registry.py"`
Expected: prints `REGISTRY_OK`.

- [ ] **Step 6: Commit**

```bash
git add memory/elastic_brain/model_library.json luna_modules/cognitive_elastic_model_registry.py self_tests/test_elastic_model_registry.py
git commit -m "feat(elastic-brain): model-tier registry"
```

---

### Task 2: Energy-aware manager — load/acquire with injectable loader + one-hot

**Files:**
- Create: `D:\SurgeApp\luna_modules\cognitive_elastic_model_manager.py`
- Test: `D:\SurgeApp\self_tests\test_elastic_model_manager.py`

- [ ] **Step 1: Write the failing test (fake loader — no real weights/GPU)**

Create `D:\SurgeApp\self_tests\test_elastic_model_manager.py`:

```python
import sys
sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_elastic_model_manager as mm


class _FakeLLM:
    def __init__(self, **kw):
        self.kw = kw
        self.closed = False

    def generate(self, prompt, **kw):
        return "ok:" + prompt

    def close(self):
        self.closed = True


def _fake_loader(tier):
    # mimics building a model for a tier; records the device/ngl it would use
    return _FakeLLM(model_name=tier["model_name"], device=tier["device"],
                    ngl=tier["ngl"])


def test_acquire_loads_tier():
    m = mm.ElasticModelManager(loader=_fake_loader)
    h = m.acquire("M")
    assert h is not None
    assert m.resident_tier() == "M"


def test_one_hot_evicts_previous():
    m = mm.ElasticModelManager(loader=_fake_loader)
    h1 = m.acquire("M")
    h2 = m.acquire("L")          # heavy → must evict M
    assert m.resident_tier() == "L"
    assert h1.closed is True     # previous model was closed (VRAM freed)
    assert h2.closed is False


def test_acquire_unknown_tier_returns_none():
    m = mm.ElasticModelManager(loader=_fake_loader)
    assert m.acquire("ZZZ") is None


def test_release_closes_model():
    m = mm.ElasticModelManager(loader=_fake_loader)
    h = m.acquire("M")
    m.release("M")
    assert h.closed is True
    assert m.resident_tier() is None


if __name__ == "__main__":
    test_acquire_loads_tier()
    test_one_hot_evicts_previous()
    test_acquire_unknown_tier_returns_none()
    test_release_closes_model()
    print("MANAGER_CORE_OK")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: FAIL — `ModuleNotFoundError: cognitive_elastic_model_manager`.

- [ ] **Step 3: Write the manager module (core)**

Create `D:\SurgeApp\luna_modules\cognitive_elastic_model_manager.py`:

```python
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


def _default_loader(tier: Dict[str, Any]):
    """Build a real gpt4all model for a tier. Honors device + ngl so the
    energy manager controls GPU/CPU placement. Returns the model or None."""
    try:
        import gpt4all  # local import: heavy, optional
    except Exception:  # noqa: BLE001
        return None
    path = os.path.join(tier["model_dir"], tier["model_name"])
    if not os.path.isfile(path):
        return None
    kw: Dict[str, Any] = {
        "model_name": tier["model_name"],
        "model_path": tier["model_dir"],
        "allow_download": False,
        "n_threads": int(tier.get("n_threads", 8)),
    }
    # gpt4all device: "cpu" or "gpu"; ngl = GPU layers. Pass only when GPU.
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

    def resident_tier(self) -> Optional[str]:
        with self._lock:
            return self._resident_tier

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
                llm = self._loader(tier)
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
                    return {"ok": True, "released": False, "reason": "not_resident"}
                was = self._resident_tier
                self._close_current()
                return {"ok": True, "released": bool(was), "was": was}
        except Exception:  # noqa: BLE001
            return {"ok": False, "released": False}

    def report(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "module": "cognitive_elastic_model_manager",
                "resident_tier": self._resident_tier,
                "loaded": self._llm is not None,
                "loaded_at": self._loaded_at,
                "last_use": self._last_use,
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: prints `MANAGER_CORE_OK`.

- [ ] **Step 5: Commit**

```bash
git add luna_modules/cognitive_elastic_model_manager.py self_tests/test_elastic_model_manager.py
git commit -m "feat(elastic-brain): energy-aware manager core (load/one-hot/release)"
```

---

### Task 3: Idle reaper (unload after idle → GPU powers down)

**Files:**
- Modify: `D:\SurgeApp\luna_modules\cognitive_elastic_model_manager.py`
- Test: `D:\SurgeApp\self_tests\test_elastic_model_manager.py` (add a test)

- [ ] **Step 1: Add the failing test**

Append to `test_elastic_model_manager.py` (before the `__main__` block) and add the call in `__main__`:

```python
def test_idle_reaper_unloads():
    m = mm.ElasticModelManager(loader=_fake_loader)
    h = m.acquire("M")
    # force last_use into the past, idle budget 0.0 -> should reap
    m._last_use = 0.0
    reaped = m.reap_if_idle(idle_unload_s=0.0)
    assert reaped is True
    assert h.closed is True
    assert m.resident_tier() is None
```

Add `test_idle_reaper_unloads()` to the `__main__` run list above the print.

- [ ] **Step 2: Run test to verify it fails**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: FAIL — `AttributeError: 'ElasticModelManager' object has no attribute 'reap_if_idle'`.

- [ ] **Step 3: Add `reap_if_idle` to the manager**

Insert this method into `ElasticModelManager` (after `release`):

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: prints `MANAGER_CORE_OK`.

- [ ] **Step 5: Commit**

```bash
git add luna_modules/cognitive_elastic_model_manager.py self_tests/test_elastic_model_manager.py
git commit -m "feat(elastic-brain): idle reaper unloads model to free GPU/CPU"
```

---

### Task 4: Energy modes (eco / balanced / performance) device selection

**Files:**
- Modify: `D:\SurgeApp\luna_modules\cognitive_elastic_model_manager.py`
- Test: `D:\SurgeApp\self_tests\test_elastic_model_manager.py` (add a test)

- [ ] **Step 1: Add the failing test**

Append before `__main__` and add to the run list:

```python
def test_eco_mode_forces_cpu_placement():
    # eco mode should downgrade a GPU tier to CPU placement in the resolved tier
    m = mm.ElasticModelManager(loader=_fake_loader)
    resolved = m.resolve_placement({"tier": "M", "device": "gpu", "ngl": 33,
                                    "model_name": "x", "model_dir": "d"},
                                   mode="eco")
    assert resolved["device"] == "cpu"
    assert resolved["ngl"] == 0


def test_performance_mode_keeps_gpu():
    m = mm.ElasticModelManager(loader=_fake_loader)
    resolved = m.resolve_placement({"tier": "M", "device": "gpu", "ngl": 33,
                                    "model_name": "x", "model_dir": "d"},
                                   mode="performance")
    assert resolved["device"] == "gpu"
    assert resolved["ngl"] == 33
```

- [ ] **Step 2: Run test to verify it fails**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: FAIL — `AttributeError: ... 'resolve_placement'`.

- [ ] **Step 3: Add `resolve_placement` and apply it in `acquire`**

Add this method to `ElasticModelManager`:

```python
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
```

Then in `acquire`, change the loader call to honor the mode. Replace:

```python
                llm = self._loader(tier)
```

with:

```python
                placed = self.resolve_placement(tier, mode=self._energy_mode())
                llm = self._loader(placed)
```

And add an energy-mode reader + store. In `__init__`, after `self._last_use = None` add:

```python
        self._mode_override: Optional[str] = None
```

Add this method to the class:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\test_elastic_model_manager.py"`
Expected: prints `MANAGER_CORE_OK`.

- [ ] **Step 5: Commit**

```bash
git add luna_modules/cognitive_elastic_model_manager.py self_tests/test_elastic_model_manager.py
git commit -m "feat(elastic-brain): eco/balanced/performance energy modes"
```

---

### Task 5: Feature flags (master + kill-switch)

**Files:**
- Modify: `D:\SurgeApp\luna_modules\cognitive_feature_flags.py`

- [ ] **Step 1: Find the flag-defaults block**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" -c "import sys; sys.path.insert(0,r'D:\SurgeApp'); from luna_modules import cognitive_feature_flags as ff; print('cognitive_elastic_brain_enabled' in ff.read_flags())"`
Expected: prints `False` (flag not present yet).

- [ ] **Step 2: Add the flags**

In `cognitive_feature_flags.py`, locate the dict of default flags (where existing `cognitive_*_enabled` keys are defined) and add these three entries with default values:

```python
    "cognitive_elastic_brain_enabled": False,
    "cognitive_elastic_brain_paused": False,
    "cognitive_elastic_energy_mode": "balanced",
```

(Place them alongside the other `cognitive_*` defaults, matching the file's existing formatting.)

- [ ] **Step 3: Verify the flags load**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" -c "import sys; sys.path.insert(0,r'D:\SurgeApp'); from luna_modules import cognitive_feature_flags as ff; f=ff.read_flags(); print(f.get('cognitive_elastic_brain_enabled'), f.get('cognitive_elastic_energy_mode'))"`
Expected: prints `False balanced`.

- [ ] **Step 4: Commit**

```bash
git add luna_modules/cognitive_feature_flags.py
git commit -m "feat(elastic-brain): feature flags (master, pause, energy mode)"
```

---

### Task 6: Live smoke test (real weights, opt-in) + full self-test sweep

**Files:**
- Create: `D:\SurgeApp\self_tests\smoke_elastic_manager_live.py`

- [ ] **Step 1: Write an opt-in live smoke test**

Create `D:\SurgeApp\self_tests\smoke_elastic_manager_live.py`:

```python
"""OPT-IN live test: actually loads tier M on the real backend, generates a
token, then reaps. Skips (prints SKIP) unless RUN_LIVE=1 — so it never blocks
the normal suite or requires a GPU/weights in CI."""
import os
import sys
sys.path.insert(0, r"D:\SurgeApp")


def main():
    if os.environ.get("RUN_LIVE") != "1":
        print("SKIP: set RUN_LIVE=1 to run the live elastic-manager test")
        return
    from luna_modules import cognitive_elastic_model_manager as mm
    m = mm.get_manager()
    llm = m.acquire("M")
    assert llm is not None, "tier M failed to load"
    out = llm.generate("Say OK.", max_tokens=8)
    assert isinstance(out, str) and out
    assert m.resident_tier() == "M"
    reaped = m.reap_if_idle(idle_unload_s=0.0)
    assert reaped is True
    print("ELASTIC_LIVE_OK")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it (skips by default)**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\smoke_elastic_manager_live.py"`
Expected: prints `SKIP: set RUN_LIVE=1 ...`.

- [ ] **Step 3: (Operator, optional) run live**

Run: `$env:RUN_LIVE=1; & "D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\self_tests\smoke_elastic_manager_live.py"; $env:RUN_LIVE=$null`
Expected: prints `ELASTIC_LIVE_OK` (after a real model load). If gpt4all GPU isn't available it falls back to CPU; still passes.

- [ ] **Step 4: Run the whole self-test suite to confirm no regression**

Run: `& "D:\SurgeApp\.aider_venv\Scripts\python.exe" -c "import sys; sys.path.insert(0,r'D:\SurgeApp'); from luna_modules import luna_self_improvement as si; import json; print(json.dumps(si.run_improvement_cycle(max_targets=4)))"`
Expected: the 3 new `cognitive_elastic_*` modules get smoke tests generated and pass (improved >= the new modules; flagged empty).

- [ ] **Step 5: Commit**

```bash
git add self_tests/smoke_elastic_manager_live.py
git commit -m "test(elastic-brain): opt-in live smoke test"
```

---

## Self-Review

**Spec coverage:** This plan implements spec §4.1 (model library registry) and §4.2 (energy-aware model manager: load-on-demand, one-hot, idle-unload, eco/balanced/performance) + §7 flags/kill-switch. NOT in this plan (later plans): §4.3 difficulty router, §4.4 compressed knowledge vault, §6 dashboard/operator verbs beyond report(), §10 step 6 log hygiene. That decomposition is intended.

**Placeholder scan:** No TBD/TODO; every code step has complete code; commands have expected output. The `_default_loader` device/ngl call matches gpt4all's real API (confirmed against `cognitive_sovereign_main_runtime._ensure_model_and_session`).

**Type consistency:** `get_tier()` returns a dict with `tier`/`device`/`ngl`/`model_dir`/`model_name`/`n_ctx`; the manager reads exactly those. `acquire/release/reap_if_idle/resolve_placement/set_energy_mode/report` names are consistent across tasks and tests. `resident_tier()` used consistently. Fake loader returns an object with `.generate()`/`.close()` matching what the manager calls.

**Kill-switch wiring note:** the runtime consumer (a later router plan) must check `cognitive_elastic_brain_enabled` (master) and `cognitive_elastic_brain_paused` (kill-switch) before routing through the manager; until then the manager is inert (nothing calls `acquire`), so this plan is safe to land dark.
