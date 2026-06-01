"""Luna Knowledge Vault — a content-addressed, compressed store with an index
that acts as Luna's "links". Make 1 TB of physical disk hold several TB of
*knowledge* (text), retrievable by link in typically sub-millisecond time.

Honest scope: this compresses TEXT (knowledge/logs/docs). It does NOT compress
model weights (already quantized) and is unrelated to GPU inference.

Design:
  * ingest(text) -> link        : content-address (sha256), dedup, compress,
                                  append to an append-only blob file, index it.
  * get(link) -> text | None    : index lookup (O(1)) -> seek+read -> decompress.
  * stats() -> dict             : logical vs physical bytes, ratio, dedup saved.

Codecs: stdlib only — "zlib" (fast, sub-ms) default, "lzma" (max ratio).
Everything NEVER raises. Append-only + index = crash-safe-ish and simple.
"""
from __future__ import annotations
import hashlib
import json
import lzma
import os
import threading
import zlib
from typing import Any, Dict, Optional

ROOT = r"D:\SurgeApp"
VAULT_DIR = os.path.join(ROOT, "knowledge", "vault")
BLOBS_PATH = os.path.join(VAULT_DIR, "blobs.bin")
INDEX_PATH = os.path.join(VAULT_DIR, "index.jsonl")

_CODECS = ("zlib", "lzma", "zstd")


def _compress(data: bytes, codec: str) -> bytes:
    if codec == "zstd":
        try:
            import zstandard
            # level 10: strong ratio, still fast. Frame stores content size
            # so decompress needs no out-length hint.
            return zstandard.ZstdCompressor(level=10).compress(data)
        except Exception:  # noqa: BLE001
            return zlib.compress(data, 6)   # graceful fallback
    if codec == "lzma":
        return lzma.compress(data, preset=6)
    return zlib.compress(data, 6)


def _decompress(data: bytes, codec: str) -> bytes:
    if codec == "zstd":
        import zstandard
        return zstandard.ZstdDecompressor().decompress(data)
    if codec == "lzma":
        return lzma.decompress(data)
    return zlib.decompress(data)


class KnowledgeVault:
    def __init__(self, vault_dir: str = VAULT_DIR):
        self._dir = vault_dir
        self._blobs = os.path.join(vault_dir, "blobs.bin")
        self._index_path = os.path.join(vault_dir, "index.jsonl")
        self._lock = threading.RLock()
        self._index: Dict[str, Dict[str, Any]] = {}
        self._logical_bytes = 0      # sum of raw lengths of UNIQUE chunks
        self._dedup_hits = 0
        self._dedup_saved_bytes = 0
        self._loaded = False

    # ---- persistence ---- #
    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        try:
            os.makedirs(self._dir, exist_ok=True)
            if os.path.isfile(self._index_path):
                with open(self._index_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            self._index[rec["link"]] = rec
                            self._logical_bytes += int(rec.get("raw_len", 0))
                        except Exception:  # noqa: BLE001
                            continue
        except Exception:  # noqa: BLE001
            pass
        self._loaded = True

    # ---- core API ---- #
    def ingest(self, text: str, *, codec: str = "zlib") -> Optional[str]:
        """Store text; return its link. Same text -> same link (dedup,
        stored once). NEVER raises; returns None only on hard failure."""
        try:
            if not isinstance(text, str):
                return None
            if codec not in _CODECS:
                codec = "zlib"
            with self._lock:
                self._ensure_loaded()
                raw = text.encode("utf-8", errors="replace")
                link = hashlib.sha256(raw).hexdigest()[:16]
                if link in self._index:                 # dedup
                    self._dedup_hits += 1
                    self._dedup_saved_bytes += len(raw)
                    return link
                blob = _compress(raw, codec)
                os.makedirs(self._dir, exist_ok=True)
                with open(self._blobs, "ab") as bf:
                    offset = bf.tell()
                    bf.write(blob)
                rec = {"link": link, "offset": offset, "clen": len(blob),
                       "codec": codec, "raw_len": len(raw)}
                with open(self._index_path, "a", encoding="utf-8") as ix:
                    ix.write(json.dumps(rec) + "\n")
                self._index[link] = rec
                self._logical_bytes += len(raw)
                return link
        except Exception:  # noqa: BLE001
            return None

    def get(self, link: str) -> Optional[str]:
        """Retrieve text by link. NEVER raises. None if unknown/broken."""
        try:
            with self._lock:
                self._ensure_loaded()
                rec = self._index.get(link)
                if rec is None:
                    return None
            # read outside the lock-critical region is fine; file is append-only
            with open(self._blobs, "rb") as bf:
                bf.seek(int(rec["offset"]))
                blob = bf.read(int(rec["clen"]))
            raw = _decompress(blob, rec.get("codec", "zlib"))
            return raw.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return None

    def stats(self) -> Dict[str, Any]:
        try:
            with self._lock:
                self._ensure_loaded()
                physical = 0
                try:
                    if os.path.isfile(self._blobs):
                        physical = os.path.getsize(self._blobs)
                except Exception:  # noqa: BLE001
                    physical = 0
                logical = self._logical_bytes + self._dedup_saved_bytes
                ratio = (round(logical / physical, 2)
                         if physical > 0 else 0.0)
                return {
                    "chunks": len(self._index),
                    "logical_bytes": logical,
                    "physical_bytes": physical,
                    "compression_ratio": ratio,
                    "dedup_hits": self._dedup_hits,
                    "dedup_saved_bytes": self._dedup_saved_bytes,
                    "vault_dir": self._dir,
                }
        except Exception:  # noqa: BLE001
            return {"error": "stats_failed"}


_SINGLETON: Optional[KnowledgeVault] = None
_SINGLETON_LOCK = threading.Lock()


def get_vault() -> KnowledgeVault:
    global _SINGLETON
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            _SINGLETON = KnowledgeVault()
        return _SINGLETON


def report() -> Dict[str, Any]:
    try:
        v = get_vault()
        return {"module": "cognitive_knowledge_vault", **v.stats()}
    except Exception as exc:  # noqa: BLE001
        return {"module": "cognitive_knowledge_vault",
                "error": f"{type(exc).__name__}"}


__all__ = ["KnowledgeVault", "get_vault", "report"]


if __name__ == "__main__":
    import sys
    if sys.stdout is not None:
        print(json.dumps(report(), indent=2))
