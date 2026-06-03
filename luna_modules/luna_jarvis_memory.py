"""
luna_jarvis_memory.py — semantic recall over Serge's Obsidian vault.

Uses chromadb (already installed). Its default embeddings run through ONNX /
onnxruntime — NO torch — so this cannot disturb the GPU voice stack and adds no
GPU load (CPU only). Vault notes are embedded ONCE into a local persistent index;
search then matches by MEANING, not just keywords.

Build/refresh the index with rebuild_index(); query with semantic_search().
Everything here is defensive: on any failure it returns empty / an error string
so the voice assistant never crashes because of memory.
"""

import logging
from pathlib import Path

logger = logging.getLogger("luna.jarvis_memory")

VAULT_ROOT = Path(r"C:\Users\paren\Documents\Obsidian Vault")
DB_DIR     = Path(r"D:\SurgeApp\memory\luna_vector_db")
COLLECTION = "vault"

_client = None
_collection = None


def _get_collection():
    global _client, _collection
    if _collection is not None:
        return _collection
    import chromadb
    DB_DIR.mkdir(parents=True, exist_ok=True)
    _client = chromadb.PersistentClient(path=str(DB_DIR))
    _collection = _client.get_or_create_collection(COLLECTION)
    return _collection


def _chunks(text: str, source: str, size: int = 800, overlap: int = 120):
    """Split a note into ~size-char chunks with small overlap; skip tiny ones."""
    text = text.strip()
    out, i, n = [], 0, 0
    while i < len(text):
        chunk = text[i:i + size].strip()
        if len(chunk) > 40:
            out.append((f"{source}#{n}", chunk))
            n += 1
        i += max(1, size - overlap)
    return out


def rebuild_index() -> str:
    """(Re)embed every vault note into the local semantic index. Persists to disk."""
    global _client, _collection
    try:
        _get_collection()
        try:
            _client.delete_collection(COLLECTION)
        except Exception:
            pass
        _collection = _client.get_or_create_collection(COLLECTION)
        col = _collection
    except Exception as e:
        return f"vector memory unavailable: {e}"

    ids, docs, metas = [], [], []
    for md in VAULT_ROOT.rglob("*.md"):
        try:
            text = md.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for cid, chunk in _chunks(text, md.name):
            ids.append(cid)
            docs.append(chunk)
            metas.append({"source": md.name})
    if not ids:
        return "no vault docs found to index"
    try:
        for k in range(0, len(ids), 100):
            col.add(ids=ids[k:k + 100], documents=docs[k:k + 100], metadatas=metas[k:k + 100])
    except Exception as e:
        return f"index add failed: {e}"
    return f"indexed {len(ids)} chunks from {len({m['source'] for m in metas})} vault notes"


def semantic_search(query: str, k: int = 3):
    """Return up to k semantically-matching vault chunks as '[source] text' strings."""
    try:
        col = _get_collection()
        if col.count() == 0:
            rebuild_index()
            col = _get_collection()
        res = col.query(query_texts=[query], n_results=k)
        docs = (res.get("documents") or [[]])[0]
        metas = (res.get("metadatas") or [[]])[0]
        hits = []
        for doc, meta in zip(docs, metas):
            src = (meta or {}).get("source", "?")
            hits.append("[%s] %s" % (src, doc.strip().replace("\n", " ")[:280]))
        return hits
    except Exception as e:
        logger.warning(f"semantic_search failed: {e}")
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(rebuild_index())
    for q in ["dashboard memory leak fix", "what makes Luna crash", "her voice latency"]:
        print(f"\nQ: {q}")
        for h in semantic_search(q, 2):
            print("  ", h)
