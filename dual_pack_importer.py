"""Dual Sovereign Pack Importer — orchestrator for Phase 13 seed packs.

Routes English (Track A) and Russian (Track B) pack files to their respective
ingestion paths, then writes a Phase-12-style `pack_manifest.json` next to
each pack regardless of language. Russian path needed a wrapper because its
ingestion module did not emit a manifest natively.

Does NOT touch Program S, tiers, probes, attestation, worker orchestration.
Does NOT integrate with Luna's main runtime.
Does NOT create daemons or recursive loops.
Streams files via the existing ingester (which itself uses chunked reading).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import english_knowledge_ingestion as en_ingest
import pack_manifest as pm
import russian_knowledge_ingestion as ru_ingest

DEFAULT_SEED_DIR = Path(__file__).resolve().parent / "seed_packs"

# Per-pack file naming convention:
#   seed_packs/en/<category>.jsonl  → English word-or-phrase pack
#   seed_packs/ru/<category>.jsonl  → Russian word-or-phrase pack
# Files named "idioms.jsonl" in either language route through the phrase
# ingestion path; everything else routes through word ingestion.

_PHRASE_FILENAMES = {"idioms.jsonl", "phrases.jsonl", "voice_personality.jsonl"}


def _classify(path: Path) -> tuple[str, str]:
    """Return (lang, kind) for a seed-pack path."""
    lang = path.parent.name.lower()
    if lang not in ("en", "ru"):
        return ("unknown", "word")
    kind = "phrase" if path.name.lower() in _PHRASE_FILENAMES else "word"
    return (lang, kind)


def _summary_for(lang: str, path: Path, kind: str, result: dict[str, Any]) -> dict[str, Any]:
    """Normalize per-pack result to a stable shape."""
    if kind == "phrase":
        added = result.get("added") or result.get("added_phrases") or 0
    else:
        added = result.get("added") or result.get("added_words") or 0
    return {
        "lang": lang,
        "kind": kind,
        "path": str(path),
        "added": int(added),
        "rejected": int(result.get("rejected", 0)),
        "duplicates": int(result.get("duplicates", 0)),
        "error": result.get("error"),
        "report_path": result.get("report_path"),
        "manifest_path": result.get("manifest_path"),
        "pack_id": result.get("pack_id"),
    }


def _emit_ru_manifest(path: Path, source: str, result: dict[str, Any],
                      coverage_categories: Iterable[str],
                      register_tags: Iterable[str],
                      safety_tags: Iterable[str]) -> str:
    """Russian ingester does not emit pack manifests natively — wrap and write."""
    added_total = (int(result.get("added", 0))
                   + int(result.get("added_words", 0))
                   + int(result.get("added_phrases", 0)))
    manifest = pm.create_pack_manifest(
        source_name=source,
        language="ru",
        coverage_categories=list(coverage_categories),
        register_tags=list(register_tags),
        safety_tags=list(safety_tags),
        domain_tags=[],
        row_count=added_total + int(result.get("rejected", 0)),
        accepted_count=added_total,
        rejected_count=int(result.get("rejected", 0)),
        duplicate_count=0,
        source_path=str(path),
        import_report_path=str(result.get("report_path") or ""),
        notes=f"russian_seed_pack source={source}",
    )
    out = Path(str(path) + ".ru_pack_manifest.json")
    pm.write_pack_manifest(manifest, out)
    return str(out)


def _scan_pack_taxonomy(path: Path) -> dict[str, list[str]]:
    """Read pack once (streaming) to collect declared coverage / register / safety."""
    cats: set[str] = set()
    regs: set[str] = set()
    safs: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict):
                    continue
                for v in obj.get("coverage_categories", []) or []:
                    if isinstance(v, str) and v.strip():
                        cats.add(v.strip())
                for v in obj.get("register_tags", []) or []:
                    if isinstance(v, str) and v.strip():
                        regs.add(v.strip())
                for v in obj.get("safety_tags", []) or []:
                    if isinstance(v, str) and v.strip():
                        safs.add(v.strip())
    except FileNotFoundError:
        return {"coverage": [], "register": [], "safety": []}
    return {
        "coverage": sorted(cats),
        "register": sorted(regs),
        "safety": sorted(safs),
    }


def import_pack(
    path: str | Path,
    source: Optional[str] = None,
    en_db: Optional[str | Path] = None,
    ru_db: Optional[str | Path] = None,
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    """Import one pack file and return a normalized summary dict."""
    p = Path(path)
    lang, kind = _classify(p)
    if lang == "unknown":
        return {"lang": "unknown", "path": str(p), "error": "unknown_language",
                "added": 0, "rejected": 0, "duplicates": 0}
    src = source or f"seed_{lang}_{p.stem}"
    if lang == "en":
        if kind == "phrase":
            result = en_ingest.ingest_phrase_list(str(p), source=src,
                                                  db_path=en_db, pack_id=pack_id)
        else:
            result = en_ingest.ingest_word_list(str(p), source=src,
                                                db_path=en_db, pack_id=pack_id)
        return _summary_for("en", p, kind, result)

    # Russian
    if kind == "phrase":
        result = ru_ingest.ingest_phrase_list(str(p), source=src, db_path=ru_db)
    else:
        result = ru_ingest.ingest_word_list(str(p), source=src, db_path=ru_db)
    tax_info = _scan_pack_taxonomy(p)
    manifest_path = _emit_ru_manifest(
        p, src, result,
        coverage_categories=tax_info["coverage"],
        register_tags=tax_info["register"],
        safety_tags=tax_info["safety"],
    )
    result["manifest_path"] = manifest_path
    return _summary_for("ru", p, kind, result)


def import_seed_directory(
    seed_dir: Optional[str | Path] = None,
    en_db: Optional[str | Path] = None,
    ru_db: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Import every .jsonl found under seed_dir/{en,ru}/. Returns full report."""
    root = Path(seed_dir) if seed_dir else DEFAULT_SEED_DIR
    if not root.exists():
        return {"error": "seed_dir_not_found", "seed_dir": str(root),
                "packs": [], "totals": {}}
    packs: list[dict[str, Any]] = []
    for lang in ("en", "ru"):
        sub = root / lang
        if not sub.exists():
            continue
        for fp in sorted(sub.glob("*.jsonl")):
            packs.append(import_pack(fp, en_db=en_db, ru_db=ru_db))
    totals = {
        "packs": len(packs),
        "en_packs": sum(1 for p in packs if p.get("lang") == "en"),
        "ru_packs": sum(1 for p in packs if p.get("lang") == "ru"),
        "added_total": sum(int(p.get("added", 0)) for p in packs),
        "rejected_total": sum(int(p.get("rejected", 0)) for p in packs),
        "duplicates_total": sum(int(p.get("duplicates", 0)) for p in packs),
        "errors": [p for p in packs if p.get("error")],
        "manifests_written": sum(1 for p in packs if p.get("manifest_path")),
    }
    return {
        "seed_dir": str(root),
        "packs": packs,
        "totals": totals,
        "generated_at": time.time(),
    }


def preview_seed_directory(
    seed_dir: Optional[str | Path] = None,
    per_pack_limit: int = 5,
) -> dict[str, Any]:
    """Preview each pack file without writing to any DB."""
    root = Path(seed_dir) if seed_dir else DEFAULT_SEED_DIR
    if not root.exists():
        return {"error": "seed_dir_not_found", "seed_dir": str(root), "previews": []}
    previews: list[dict[str, Any]] = []
    for lang in ("en", "ru"):
        sub = root / lang
        if not sub.exists():
            continue
        for fp in sorted(sub.glob("*.jsonl")):
            lang_code, kind = _classify(fp)
            if lang_code == "en":
                previews.append({
                    "lang": "en", "kind": kind, "path": str(fp),
                    **en_ingest.preview_ingestion(str(fp), limit=per_pack_limit),
                })
            else:
                previews.append({
                    "lang": "ru", "kind": kind, "path": str(fp),
                    **ru_ingest.preview_ingestion(str(fp), limit=per_pack_limit),
                })
    return {"seed_dir": str(root), "previews": previews}


__all__ = [
    "DEFAULT_SEED_DIR",
    "import_pack",
    "import_seed_directory",
    "preview_seed_directory",
]
