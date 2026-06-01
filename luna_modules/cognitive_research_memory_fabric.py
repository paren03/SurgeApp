"""Program T — Research memory fabric.

Durable structured knowledge cards distilled from ingestions and
syntheses. Each card carries:

- card_id
- captured_at_utc / last_used_at_utc
- topic (heuristic cluster)
- trust_level: high|medium|uncertain|stale|conflicting
- freshness: fresh|aging|stale
- claims: [{claim, confidence, source_ids}]
- procedures: [{step, source_ids}]
- evidence_chain: [source_id, ...] (provenance trace)
- related_card_ids: [card_id]
- usage_count

The fabric file is bounded (``MAX_CARDS``), atomic-write only, and
NEVER raises. No threads / daemons / LLM calls.

Operator flag: ``cognitive_research_memory_fabric_enabled``.
"""
from __future__ import annotations

import importlib
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

PROJECT_ROOT = r"D:\SurgeApp"
COGNITIVE_DIR = os.path.join(PROJECT_ROOT, "memory", "cognitive")
FABRIC_PATH = os.path.join(
    COGNITIVE_DIR, "research_memory_cards.json")
MAX_CARDS = 500
TOPIC_MAX_TOKENS = 3
FRESH_AFTER_DAYS = 14
STALE_AFTER_DAYS = 60


def _safe(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _enabled() -> bool:
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_research_memory_fabric_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _age_days(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        t = time.mktime(time.strptime(iso, "%Y-%m-%dT%H:%M:%SZ"))
        return max(0.0, (time.time() - t) / 86400.0)
    except Exception:  # noqa: BLE001
        return None


def _freshness_for_age(age_days: Optional[float]) -> str:
    if age_days is None:
        return "fresh"
    if age_days < FRESH_AFTER_DAYS:
        return "fresh"
    if age_days < STALE_AFTER_DAYS:
        return "aging"
    return "stale"


def _topic_from_concepts(
    concepts: List[Dict[str, Any]], hint: Optional[str] = None,
) -> str:
    if hint:
        return hint.strip().lower()[:64] or "general"
    if not concepts:
        return "general"
    top = [str(c.get("concept", "")).strip()
            for c in concepts[: TOPIC_MAX_TOKENS]]
    top = [t for t in top if t]
    return "_".join(top) or "general"


def _atomic_write(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = (f"{path}.tmp.{int(time.time() * 1000)}."
            f"{uuid.uuid4().hex[:8]}")
    with open(tmp, "w", encoding="utf-8") as fh:
        # 2026-06-01 perf: compact (no indent). mark_card_used() rewrites the
        # whole store per card (~16x/turn); pretty-printing generated ~1.18M
        # encode ops/turn (profiled). Files are machine-read (json.load is
        # whitespace-agnostic), so compact is safe and ~5x faster to write.
        json.dump(data, fh, separators=(",", ":"), default=str)
    os.replace(tmp, path)


def _load() -> Dict[str, Any]:
    if not os.path.isfile(FABRIC_PATH):
        return {"cards": [], "captured_at_utc": _now_iso()}
    try:
        with open(FABRIC_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {"cards": [], "captured_at_utc": _now_iso()}
        if "cards" not in data or not isinstance(data["cards"], list):
            data["cards"] = []
        return data
    except Exception:  # noqa: BLE001
        return {"cards": [], "captured_at_utc": _now_iso()}


def _save(data: Dict[str, Any]) -> None:
    # Bounded — evict least-recently-used when over cap.
    cards: List[Dict[str, Any]] = data.get("cards") or []
    if len(cards) > MAX_CARDS:
        cards.sort(
            key=lambda c: str(
                c.get("last_used_at_utc")
                or c.get("captured_at_utc") or ""),
            reverse=True,
        )
        cards = cards[: MAX_CARDS]
        data["cards"] = cards
    data["captured_at_utc"] = _now_iso()
    _atomic_write(FABRIC_PATH, data)


def store_card_from_ingestion(
    ingestion_record: Dict[str, Any],
    governor_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Promote an ingestion record into a research card. The trust
    governor's verdict (if supplied) takes precedence over the
    ingestion hint. NEVER raises."""
    if not _enabled():
        return {"ok": False,
                "reason": "research_memory_fabric_disabled"}
    if not isinstance(ingestion_record, dict):
        return {"ok": False, "reason": "bad_ingestion_record"}
    captured = _now_iso()
    card_id = uuid.uuid4().hex[:12]
    source_id = ingestion_record.get("source_id") or "unknown_source"
    govern = dict(governor_result or {})
    trust_level = (
        govern.get("trust_level")
        or {
            "high": "high", "medium": "medium",
            "uncertain": "uncertain", "low": "uncertain",
        }.get(ingestion_record.get("trust_hint", "medium"),
              "medium")
    )
    age = _age_days(ingestion_record.get("captured_at_utc"))
    freshness = govern.get("freshness") or _freshness_for_age(age)
    topic = _topic_from_concepts(
        ingestion_record.get("key_concepts") or [],
        ingestion_record.get("topic"),
    )
    claims = []
    for c in (ingestion_record.get("extracted_claims") or []):
        claims.append({
            "claim": c.get("claim"),
            "confidence": c.get("confidence", 0.5),
            "uncertain": bool(c.get("uncertain")),
            "source_ids": [source_id],
        })
    procedures = []
    for p in (ingestion_record.get("procedures") or []):
        procedures.append({
            "step": p.get("step"),
            "source_ids": [source_id],
        })
    card = {
        "card_id": card_id,
        "captured_at_utc": captured,
        "last_used_at_utc": captured,
        "topic": topic,
        "trust_level": trust_level,
        "freshness": freshness,
        "claims": claims,
        "procedures": procedures,
        "evidence_chain": [source_id],
        "related_card_ids": [],
        "usage_count": 0,
        "uncertainty_flags":
            list(ingestion_record.get("uncertainty_flags") or []),
        "ingestion_id": ingestion_record.get("ingestion_id"),
        "source_type": ingestion_record.get("source_type"),
        "key_concepts": ingestion_record.get("key_concepts"),
        "governor_reason": govern.get("reason"),
    }
    try:
        data = _load()
        data["cards"].append(card)
        _save(data)
        return {"ok": True, "card_id": card_id, "topic": topic,
                "trust_level": trust_level, "freshness": freshness}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def store_synthesis_card(*, topic: str, claims: List[Dict[str, Any]],
                          procedures: List[Dict[str, Any]],
                          evidence_chain: List[str],
                          trust_level: str = "medium",
                          freshness: str = "fresh",
                          contradictions:
                              Optional[List[Dict[str, Any]]] = None,
                          related_card_ids:
                              Optional[List[str]] = None,
                          notes: Optional[str] = None,
                          ) -> Dict[str, Any]:
    """Store a card produced by the synthesis layer. NEVER raises."""
    if not _enabled():
        return {"ok": False,
                "reason": "research_memory_fabric_disabled"}
    card_id = uuid.uuid4().hex[:12]
    captured = _now_iso()
    card = {
        "card_id": card_id,
        "captured_at_utc": captured,
        "last_used_at_utc": captured,
        "topic": (topic or "general").lower().strip()[:64],
        "trust_level": trust_level,
        "freshness": freshness,
        "claims": list(claims or []),
        "procedures": list(procedures or []),
        "evidence_chain": list(evidence_chain or []),
        "related_card_ids": list(related_card_ids or []),
        "contradictions": list(contradictions or []),
        "usage_count": 0,
        "uncertainty_flags":
            ["synthesis_with_contradictions"] if contradictions
            else [],
        "is_synthesis": True,
        "notes": notes,
    }
    try:
        data = _load()
        data["cards"].append(card)
        _save(data)
        return {"ok": True, "card_id": card_id,
                "topic": card["topic"],
                "trust_level": trust_level,
                "freshness": freshness}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def find_cards(*, query: str = "", topic: Optional[str] = None,
                top_k: int = 5,
                min_trust:
                    Optional[str] = None) -> List[Dict[str, Any]]:
    """Keyword/topic search across cards. NEVER raises."""
    if not _enabled():
        return []
    data = _load()
    cards: List[Dict[str, Any]] = data.get("cards") or []
    qtokens = {t for t in (query or "").lower().split() if len(t) > 2}
    trust_order = {"high": 3, "medium": 2,
                    "uncertain": 1, "stale": 0, "conflicting": 0}
    min_rank = trust_order.get((min_trust or "").lower(), -1)
    scored: List[Dict[str, Any]] = []
    for c in cards:
        if min_rank >= 0 and trust_order.get(
                c.get("trust_level", "uncertain"), 0) < min_rank:
            continue
        if topic:
            if str(c.get("topic", "")).lower() != topic.lower():
                continue
        score = 0.0
        # Topic alignment with any query token.
        for t in qtokens:
            if t in str(c.get("topic", "")).lower():
                score += 0.4
        # Concept overlap.
        for kc in (c.get("key_concepts") or []):
            cn = str(kc.get("concept", "")).lower()
            if cn in qtokens:
                score += 0.2
        # Claim text overlap.
        for cl in (c.get("claims") or []):
            ct = str(cl.get("claim", "")).lower()
            for t in qtokens:
                if t in ct:
                    score += 0.05
        # Freshness boost.
        score += {"fresh": 0.10, "aging": 0.04,
                   "stale": -0.10}.get(c.get("freshness", "fresh"), 0.0)
        # Trust boost.
        score += {"high": 0.15, "medium": 0.05,
                   "uncertain": -0.05,
                   "conflicting": -0.10,
                   "stale": -0.15}.get(c.get("trust_level"), 0.0)
        scored.append({"card": c, "score": round(score, 3)})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[: int(top_k)]


def mark_card_used(card_id: str) -> Dict[str, Any]:
    if not _enabled():
        return {"ok": False,
                "reason": "research_memory_fabric_disabled"}
    try:
        data = _load()
        for c in data.get("cards") or []:
            if c.get("card_id") == card_id:
                c["last_used_at_utc"] = _now_iso()
                c["usage_count"] = int(c.get("usage_count", 0)) + 1
                _save(data)
                return {"ok": True, "card_id": card_id,
                        "usage_count": c["usage_count"]}
        return {"ok": False, "reason": "card_not_found"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def mark_card_stale(card_id: str, reason: str = "") -> Dict[str, Any]:
    if not _enabled():
        return {"ok": False,
                "reason": "research_memory_fabric_disabled"}
    try:
        data = _load()
        for c in data.get("cards") or []:
            if c.get("card_id") == card_id:
                c["freshness"] = "stale"
                c["trust_level"] = "stale"
                flags = list(c.get("uncertainty_flags") or [])
                tag = f"marked_stale:{reason}" if reason else "marked_stale"
                if tag not in flags:
                    flags.append(tag)
                c["uncertainty_flags"] = flags
                _save(data)
                return {"ok": True, "card_id": card_id,
                        "reason": reason or "no_reason"}
        return {"ok": False, "reason": "card_not_found"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


def list_topics() -> List[Dict[str, Any]]:
    if not _enabled():
        return []
    data = _load()
    by_topic: Dict[str, Dict[str, Any]] = {}
    for c in data.get("cards") or []:
        t = str(c.get("topic") or "general")
        b = by_topic.setdefault(t, {
            "topic": t,
            "card_count": 0,
            "trust_counts": {},
            "freshness_counts": {},
        })
        b["card_count"] += 1
        tl = c.get("trust_level", "medium")
        b["trust_counts"][tl] = b["trust_counts"].get(tl, 0) + 1
        fr = c.get("freshness", "fresh")
        b["freshness_counts"][fr] = b["freshness_counts"].get(fr, 0) + 1
    return sorted(by_topic.values(),
                   key=lambda x: x["card_count"], reverse=True)


def report() -> Dict[str, Any]:
    if not _enabled():
        return {"available": False,
                "reason": "research_memory_fabric_disabled"}
    data = _load()
    cards: List[Dict[str, Any]] = data.get("cards") or []
    trust_counts: Dict[str, int] = {}
    freshness_counts: Dict[str, int] = {}
    for c in cards:
        tl = c.get("trust_level", "medium")
        trust_counts[tl] = trust_counts.get(tl, 0) + 1
        fr = c.get("freshness", "fresh")
        freshness_counts[fr] = freshness_counts.get(fr, 0) + 1
    return {
        "available": True,
        "enabled": True,
        "card_count": len(cards),
        "max_cards": MAX_CARDS,
        "trust_counts": trust_counts,
        "freshness_counts": freshness_counts,
        "topic_count": len({c.get("topic") for c in cards}),
        "fabric_path": FABRIC_PATH,
        "doctrine": [
            "durable_knowledge_cards",
            "evidence_chain_provenance",
            "trust_and_freshness_stratified",
            "bounded_lru_eviction",
            "atomic_write_only",
        ],
    }


__all__ = [
    "FABRIC_PATH", "MAX_CARDS",
    "store_card_from_ingestion", "store_synthesis_card",
    "find_cards", "mark_card_used", "mark_card_stale",
    "list_topics", "report",
]
