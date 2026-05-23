"""Luna bounded vocabulary store (SQLite-backed).

Standalone Luna infrastructure. Does NOT touch Program S. Every query is
bounded; no function returns unbounded results. The full database is never
loaded into memory.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

DEFAULT_LIMIT = 25
HARD_MAX_LIMIT = 200

_DEFAULT_DB_PATH = Path(__file__).resolve().parent / "lexicon" / "luna_vocabulary.sqlite"


def _resolve_db_path(db_path: Optional[str | Path] = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    override = os.environ.get("LUNA_VOCABULARY_DB")
    if override:
        return Path(override)
    return _DEFAULT_DB_PATH


def _clamp_limit(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    try:
        n = int(limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if n <= 0:
        return DEFAULT_LIMIT
    return min(n, HARD_MAX_LIMIT)


@contextmanager
def _connect(db_path: Optional[str | Path] = None):
    path = _resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
    finally:
        conn.close()


_EXTRA_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("register_tags_json",       "TEXT", "'[]'"),
    ("safety_tags_json",         "TEXT", "'[]'"),
    ("coverage_categories_json", "TEXT", "'[]'"),
    ("pack_source",              "TEXT", "''"),
    ("pack_id",                  "TEXT", "''"),
)


def _apply_migrations(conn: sqlite3.Connection) -> list[str]:
    """Additive only. Adds missing columns without touching existing data."""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(words)")}
    added: list[str] = []
    for col, ctype, default in _EXTRA_COLUMNS:
        if col not in existing:
            conn.execute(
                f"ALTER TABLE words ADD COLUMN {col} {ctype} NOT NULL DEFAULT {default}"
            )
            added.append(col)
    return added


def init_db(db_path: Optional[str | Path] = None) -> str:
    """Idempotent schema initialization. Returns the resolved DB path string."""
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS words (
                word              TEXT PRIMARY KEY,
                definition        TEXT NOT NULL DEFAULT '',
                synonyms_json     TEXT NOT NULL DEFAULT '[]',
                examples_json     TEXT NOT NULL DEFAULT '[]',
                tags_json         TEXT NOT NULL DEFAULT '[]',
                source            TEXT NOT NULL DEFAULT 'manual',
                language          TEXT NOT NULL DEFAULT 'en',
                frequency_score   REAL NOT NULL DEFAULT 0.0,
                word_level        TEXT NOT NULL DEFAULT 'plain',
                created_at        REAL NOT NULL,
                updated_at        REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_words_word_prefix
                ON words(word);
            CREATE INDEX IF NOT EXISTS idx_words_level
                ON words(word_level);
            CREATE INDEX IF NOT EXISTS idx_words_frequency
                ON words(frequency_score);
            """
        )
        _apply_migrations(conn)
    return str(_resolve_db_path(db_path))


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for json_field, plain_field in (
        ("synonyms_json", "synonyms"),
        ("examples_json", "examples"),
        ("tags_json", "tags"),
        ("register_tags_json", "register_tags"),
        ("safety_tags_json", "safety_tags"),
        ("coverage_categories_json", "coverage_categories"),
    ):
        raw = d.pop(json_field, "[]")
        try:
            d[plain_field] = json.loads(raw) if raw else []
        except json.JSONDecodeError:
            d[plain_field] = []
    d.setdefault("pack_source", "")
    d.setdefault("pack_id", "")
    return d


def _norm_strset(items: Optional[Iterable[str]]) -> str:
    return json.dumps(sorted({
        s.strip().lower() for s in (items or [])
        if isinstance(s, str) and s.strip()
    }))


def add_word(
    word: str,
    definition: str = "",
    synonyms: Optional[Iterable[str]] = None,
    examples: Optional[Iterable[str]] = None,
    tags: Optional[Iterable[str]] = None,
    source: str = "manual",
    language: str = "en",
    frequency_score: float = 0.0,
    word_level: str = "plain",
    register_tags: Optional[Iterable[str]] = None,
    safety_tags: Optional[Iterable[str]] = None,
    coverage_categories: Optional[Iterable[str]] = None,
    pack_source: str = "",
    pack_id: str = "",
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Insert or update a word. Returns the resulting row as a dict."""
    if not isinstance(word, str) or not word.strip():
        raise ValueError("word must be a non-empty string")
    w = word.strip().lower()
    now = time.time()
    syns = json.dumps(sorted({s.strip().lower() for s in (synonyms or []) if s.strip()}))
    exs = json.dumps([e for e in (examples or []) if isinstance(e, str) and e.strip()])
    tgs = json.dumps(sorted({t.strip().lower() for t in (tags or []) if t.strip()}))
    reg = _norm_strset(register_tags)
    saf = _norm_strset(safety_tags)
    cov = _norm_strset(coverage_categories)

    with _connect(db_path) as conn:
        cur = conn.execute("SELECT created_at FROM words WHERE word = ?", (w,))
        existing = cur.fetchone()
        created_at = existing["created_at"] if existing else now
        conn.execute(
            """
            INSERT INTO words(word, definition, synonyms_json, examples_json, tags_json,
                              source, language, frequency_score, word_level,
                              created_at, updated_at,
                              register_tags_json, safety_tags_json,
                              coverage_categories_json, pack_source, pack_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(word) DO UPDATE SET
                definition               = excluded.definition,
                synonyms_json            = excluded.synonyms_json,
                examples_json            = excluded.examples_json,
                tags_json                = excluded.tags_json,
                source                   = excluded.source,
                language                 = excluded.language,
                frequency_score          = excluded.frequency_score,
                word_level               = excluded.word_level,
                updated_at               = excluded.updated_at,
                register_tags_json       = excluded.register_tags_json,
                safety_tags_json         = excluded.safety_tags_json,
                coverage_categories_json = excluded.coverage_categories_json,
                pack_source              = excluded.pack_source,
                pack_id                  = excluded.pack_id
            """,
            (
                w, definition or "", syns, exs, tgs,
                source, language, float(frequency_score), word_level,
                created_at, now,
                reg, saf, cov, pack_source or "", pack_id or "",
            ),
        )
        row = conn.execute("SELECT * FROM words WHERE word = ?", (w,)).fetchone()
    return _row_to_dict(row)


def lookup_word(word: str, db_path: Optional[str | Path] = None) -> Optional[dict[str, Any]]:
    if not isinstance(word, str) or not word.strip():
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM words WHERE word = ? LIMIT 1",
            (word.strip().lower(),),
        ).fetchone()
    return _row_to_dict(row) if row else None


def search_prefix(
    prefix: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    if not isinstance(prefix, str) or not prefix.strip():
        return []
    n = _clamp_limit(limit)
    pat = prefix.strip().lower().replace("%", "").replace("_", "") + "%"
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM words WHERE word LIKE ? ORDER BY frequency_score DESC, word ASC LIMIT ?",
            (pat, n),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def search_contains(
    needle: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    if not isinstance(needle, str) or not needle.strip():
        return []
    n = _clamp_limit(limit)
    pat = "%" + needle.strip().lower().replace("%", "").replace("_", "") + "%"
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM words
               WHERE word LIKE ? OR definition LIKE ?
               ORDER BY frequency_score DESC, word ASC LIMIT ?""",
            (pat, pat, n),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def search_by_tag(
    tag: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    if not isinstance(tag, str) or not tag.strip():
        return []
    n = _clamp_limit(limit)
    t = tag.strip().lower()
    needle_a = f'"{t}"'
    pat = f'%{needle_a}%'
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM words WHERE tags_json LIKE ? ORDER BY frequency_score DESC, word ASC LIMIT ?",
            (pat, n),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_related_words(
    word: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    """Return words listed as synonyms of `word`, capped to limit."""
    base = lookup_word(word, db_path=db_path)
    if not base:
        return []
    n = _clamp_limit(limit)
    syns = base.get("synonyms") or []
    if not syns:
        return []
    syns = syns[:n]
    placeholders = ",".join("?" for _ in syns)
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM words WHERE word IN ({placeholders}) "
            f"ORDER BY frequency_score DESC, word ASC LIMIT ?",
            (*syns, n),
        ).fetchall()
    found = [_row_to_dict(r) for r in rows]
    found_words = {r["word"] for r in found}
    stubs = [
        {"word": s, "definition": "", "synonyms": [], "examples": [],
         "tags": [], "source": "synonym_ref", "language": base.get("language", "en"),
         "frequency_score": 0.0, "word_level": "plain",
         "created_at": 0.0, "updated_at": 0.0}
        for s in syns if s not in found_words
    ]
    return (found + stubs)[:n]


_ALLOWED_FILTER_COLS = {
    "word", "definition", "source", "language", "word_level",
}


def bounded_query(
    where: Optional[dict[str, Any]] = None,
    order_by: str = "frequency_score DESC, word ASC",
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    """Bounded, validated SELECT against `words`. Always returns <= HARD_MAX_LIMIT."""
    n = _clamp_limit(limit)
    clauses: list[str] = []
    params: list[Any] = []
    for col, val in (where or {}).items():
        if col not in _ALLOWED_FILTER_COLS:
            raise ValueError(f"filter column not allowed: {col!r}")
        clauses.append(f"{col} = ?")
        params.append(val)

    safe_order = order_by if all(
        token.strip().split()[0] in {
            "word", "definition", "source", "language", "word_level",
            "frequency_score", "created_at", "updated_at"
        }
        for token in order_by.split(",")
    ) else "frequency_score DESC, word ASC"

    sql = "SELECT * FROM words"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += f" ORDER BY {safe_order} LIMIT ?"
    params.append(n)

    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [_row_to_dict(r) for r in rows]


def count_words(db_path: Optional[str | Path] = None) -> int:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM words").fetchone()
    return int(row["n"]) if row else 0


def count_by_tag(tag: str, db_path: Optional[str | Path] = None) -> int:
    if not isinstance(tag, str) or not tag.strip():
        return 0
    pat = f'%"{tag.strip().lower()}"%'
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM words WHERE tags_json LIKE ?", (pat,)
        ).fetchone()
    return int(row["n"]) if row else 0


_SEED_NORMAL = [
    "hello", "goodbye", "yes", "no", "please", "thanks", "water", "food",
    "home", "work", "time", "today", "tomorrow", "friend", "family", "love",
    "like", "want", "need", "help", "talk", "walk", "run", "sit", "stand",
    "eat", "drink", "sleep", "wake", "read", "write", "learn", "teach",
    "play", "watch", "listen", "hear", "see", "look", "find", "give", "take",
    "make", "build", "hold", "open", "close", "start", "stop", "finish",
]

_SEED_TEACHER = [
    "explain", "define", "demonstrate", "illustrate", "summarize",
    "paraphrase", "analyze", "evaluate", "interpret", "compare", "contrast",
    "classify", "categorize", "organize", "sequence", "predict", "infer",
    "conclude", "hypothesize", "observe", "measure", "calculate", "estimate",
    "verify", "validate", "justify", "support", "evidence", "example",
    "concept", "theory", "principle", "method", "process", "system",
    "structure", "function", "purpose", "context", "perspective", "viewpoint",
    "assumption", "criteria", "framework", "model", "pattern", "relationship",
    "cause", "effect", "lesson",
]

_SEED_TECHNICAL = [
    "function", "variable", "constant", "parameter", "argument", "return",
    "class", "object", "method", "attribute", "property", "interface",
    "module", "package", "library", "framework", "dependency", "repository",
    "branch", "commit", "merge", "rebase", "pull", "push", "fork", "clone",
    "debug", "compile", "runtime", "syntax", "semantic", "recursion",
    "iteration", "algorithm", "complexity", "abstraction", "encapsulation",
    "inheritance", "polymorphism", "composition", "lambda", "closure",
    "callback", "promise", "async", "await", "mutex", "semaphore", "thread",
    "process",
]

_SEED_CARPENTRY = [
    "joist", "beam", "rafter", "stud", "lintel", "sheathing", "plywood",
    "drywall", "framing", "sill", "plate", "header", "trimmer", "cripple",
    "blocking", "bridging", "subfloor", "decking", "fascia", "soffit",
    "eave", "gable", "ridge", "hip", "valley", "flashing", "underlayment",
    "shingle", "truss", "purlin", "mortise", "tenon", "dado", "rabbet",
    "miter", "plumb", "level", "square", "osha", "harness", "lanyard",
    "scaffold", "guardrail", "toeboard", "hardhat", "lockout", "tagout",
    "msds", "ppe", "ladder",
]

_SEED_PROFESSIONAL = [
    "collaborate", "coordinate", "facilitate", "prioritize", "delegate",
    "execute", "implement", "oversee", "manage", "lead", "mentor", "develop",
    "optimize", "streamline", "leverage", "articulate", "communicate",
    "negotiate", "present", "propose", "recommend", "strategize", "plan",
    "schedule", "deliver", "achieve", "accomplish", "milestone", "deadline",
    "deliverable", "stakeholder", "objective", "initiative", "outcome",
    "metric", "kpi", "roi", "scope", "budget", "timeline", "resource",
    "alignment", "synergy", "accountability", "ownership", "governance",
    "compliance", "risk", "mitigation", "opportunity",
]

SEED_GROUPS: tuple[tuple[str, str, list[str]], ...] = (
    ("normal",       "plain",        _SEED_NORMAL),
    ("teacher",      "intermediate", _SEED_TEACHER),
    ("technical",    "specialized",  _SEED_TECHNICAL),
    ("carpentry",    "specialized",  _SEED_CARPENTRY),
    ("professional", "advanced",     _SEED_PROFESSIONAL),
)


def _zipf_for(word: str) -> float:
    try:
        import wordfreq
        return float(wordfreq.zipf_frequency(word, "en"))
    except Exception:
        return 0.0


def _wordnet_definition(word: str) -> tuple[str, list[str]]:
    """Return (definition, synonyms) from WordNet's first synset, or ('', [])."""
    try:
        from nltk.corpus import wordnet
        syns = wordnet.synsets(word)
        if not syns:
            return "", []
        first = syns[0]
        defn = first.definition() or ""
        lemmas: list[str] = []
        for s in syns[:3]:
            for lemma in s.lemmas():
                name = lemma.name().replace("_", " ").lower()
                if name != word and name not in lemmas:
                    lemmas.append(name)
                if len(lemmas) >= 8:
                    break
            if len(lemmas) >= 8:
                break
        return defn, lemmas
    except Exception:
        return "", []


def _level_from_zipf(zipf: float, fallback: str) -> str:
    if zipf >= 5.5:
        return "plain"
    if zipf >= 4.5:
        return "everyday"
    if zipf >= 3.5:
        return "intermediate"
    if zipf >= 2.5:
        return "advanced"
    if zipf > 0.0:
        return "rare"
    return fallback


def seed_small_dataset(db_path: Optional[str | Path] = None) -> dict[str, int]:
    """Idempotent: seeds 5 small word lists tagged by category.

    Uses wordfreq for level + frequency and WordNet for definition / synonyms
    when available; falls back gracefully if either is missing.
    Returns {category: count_added}.
    """
    init_db(db_path)
    summary: dict[str, int] = {}
    for category, default_level, words in SEED_GROUPS:
        added = 0
        for w in words:
            z = _zipf_for(w)
            defn, syns = _wordnet_definition(w)
            level = _level_from_zipf(z, default_level)
            add_word(
                w,
                definition=defn,
                synonyms=syns,
                tags=[category],
                source="seed_small_dataset",
                frequency_score=z,
                word_level=level,
                db_path=db_path,
            )
            added += 1
        summary[category] = added
    return summary
