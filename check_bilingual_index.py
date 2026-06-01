"""Read-only check: are the word columns indexed in bilingual_links.sqlite?"""
import sqlite3

URI = "file:D:/SurgeApp/bilingual_links.sqlite?mode=ro"
c = sqlite3.connect(URI, uri=True, timeout=10.0)
cur = c.cursor()
for tbl in ("english_words", "russian_words"):
    try:
        idx = cur.execute(f"PRAGMA index_list('{tbl}')").fetchall()
        print(f"{tbl} indexes: {idx}")
    except Exception as exc:
        print(f"{tbl} index_list err: {exc}")
try:
    print("EN plan:", cur.execute(
        "EXPLAIN QUERY PLAN SELECT word,definition,pos "
        "FROM english_words WHERE word=?", ("test",)).fetchall())
except Exception as exc:
    print("EN plan err:", exc)
try:
    print("RU plan:", cur.execute(
        "EXPLAIN QUERY PLAN SELECT word,definition,pos "
        "FROM russian_words WHERE word=?", ("тест",)
    ).fetchall())
except Exception as exc:
    print("RU plan err:", exc)
c.close()
