"""Phase 27 — Voice-Render Adapter Registry.

Local, inspectable, in-memory registry of future adapter descriptors.
No engine import, no subprocess, no audio. All built-ins dry-run only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_contract as vac


_HARD_LIST_CAP = 50


def _make(name: str, atype: str, caps: dict[str, Any]) -> dict[str, Any]:
    return vac.create_voice_adapter_descriptor(
        name, atype, capabilities=caps, dry_run=True)


def get_builtin_dry_run_adapters() -> list[dict[str, Any]]:
    return [
        _make("dry_run_basic", "dry_run_renderer", {
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": False,
            "supports_segments": True,
            "supports_prosody": False,
            "supports_pronunciation_hints": False,
            "supports_emotion": False,
            "supports_streaming": False,
        }),
        _make("dry_run_code_switch", "dry_run_renderer", {
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_segments": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
            "supports_emotion": False,
            "supports_streaming": False,
        }),
        _make("piper_shaped_dry_run", "piper_shaped", {
            "supports_languages": ["en", "ru"],
            "supports_code_switching": False,
            "supports_segments": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
            "supports_emotion": False,
            "supports_streaming": True,
        }),
        _make("sapi_shaped_dry_run", "sapi_shaped", {
            "supports_languages": ["en", "ru"],
            "supports_code_switching": False,
            "supports_segments": True,
            "supports_prosody": False,
            "supports_pronunciation_hints": False,
            "supports_emotion": False,
            "supports_streaming": False,
        }),
        _make("kokoro_shaped_dry_run", "kokoro_shaped", {
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_segments": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
            "supports_emotion": True,
            "supports_streaming": True,
        }),
        _make("local_renderer_shaped_dry_run", "local_renderer_shaped", {
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_segments": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
            "supports_emotion": False,
            "supports_streaming": False,
        }),
    ]


def _default_registry() -> dict[str, dict[str, Any]]:
    return {d["adapter_name"]: d for d in get_builtin_dry_run_adapters()}


def register_adapter_descriptor(
    descriptor: dict[str, Any],
    registry: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    if registry is None:
        registry = _default_registry()
    val = vac.validate_voice_adapter_descriptor(descriptor)
    if not val["ok"]:
        return {"ok": False, "reasons": val["reasons"], "registry": registry}
    name = descriptor.get("adapter_name")
    if not name:
        return {"ok": False, "reasons": ["missing_adapter_name"],
                "registry": registry}
    if len(registry) >= _HARD_LIST_CAP and name not in registry:
        return {"ok": False, "reasons": ["registry_cap_reached"],
                "registry": registry}
    registry[name] = dict(descriptor)
    return {"ok": True, "reasons": [], "registry": registry}


def list_registered_adapters(
    registry: Optional[dict[str, dict[str, Any]]] = None,
    limit: int = _HARD_LIST_CAP,
) -> list[dict[str, Any]]:
    if registry is None:
        registry = _default_registry()
    cap = max(1, min(int(limit or 1), _HARD_LIST_CAP))
    return list(registry.values())[:cap]


def find_adapter_by_name(
    name: str,
    registry: Optional[dict[str, dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    if registry is None:
        registry = _default_registry()
    return registry.get(str(name))


def validate_registry(
    registry: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    if registry is None:
        registry = _default_registry()
    reasons: list[str] = []
    for name, desc in registry.items():
        val = vac.validate_voice_adapter_descriptor(desc)
        if not val["ok"]:
            reasons.append(f"invalid:{name}:" + ",".join(val["reasons"]))
        elif desc.get("dry_run") is not True:
            reasons.append(f"not_dry_run:{name}")
    return {
        "ok": not reasons,
        "reasons": reasons,
        "size": len(registry),
        "cap": _HARD_LIST_CAP,
    }


def write_adapter_registry_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_builtin_dry_run_adapters",
    "register_adapter_descriptor",
    "list_registered_adapters",
    "find_adapter_by_name",
    "validate_registry",
    "write_adapter_registry_report",
]
