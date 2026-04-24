"""Tier-1 real tool implementations for Luna.

Upgrades from simulated stubs to live capabilities:

  web_search()           — Brave Search API (live results)
  github_api_call()      — generic GitHub REST v3 transport
  github_get_repo()      — repository metadata
  github_list_issues()   — open / closed issues
  github_create_issue()  — file new issues autonomously
  github_read_file()     — read any file from a GitHub repo
  github_search_code()   — code search across GitHub
  run_project_shell()    — sandboxed subprocess inside PROJECT_DIR
  project_read_file()    — read any file under PROJECT_DIR
  project_write_file()   — safe atomic write (py_compile gate for .py)
  list_project_files()   — glob any pattern within PROJECT_DIR

All external calls (Brave, GitHub) read credentials from ``API.txt``
via ``_read_vault()``.  No key is ever written to logs or outputs.
"""

from __future__ import annotations

import base64
import json
import os
import py_compile
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from luna_modules.luna_io import safe_read_text, safe_write_text
from luna_modules.luna_logging import _diag, now_iso
from luna_modules.luna_paths import PROJECT_DIR


# ── Credential vault ──────────────────────────────────────────────────────────

def _read_vault() -> Dict[str, str]:
    """Parse API.txt into a key→value dict (blank lines and # comments skipped).

    Also folds in os.environ so both sources are visible in one dict.
    Keys from API.txt take priority over environment variables.
    """
    vault: Dict[str, str] = {k: v for k, v in os.environ.items() if v}
    try:
        text = safe_read_text(PROJECT_DIR / "API.txt")
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if key:
                    vault[key] = val
    except Exception:
        pass
    return vault


# ── Web search ────────────────────────────────────────────────────────────────

def web_search(query: str, max_results: int = 5) -> Dict[str, Any]:
    """Query Brave Search and return structured results.

    Return value::

        {
            "ok": True,
            "query": "...",
            "provider": "brave",
            "results": [
                {"title": "...", "url": "...", "snippet": "..."},
                ...
            ],
        }

    Returns ``{"ok": False, "reason": "..."}`` on any error or missing key.
    """
    vault = _read_vault()
    key = str(vault.get("BRAVE_SEARCH_API_KEY") or "").strip()
    if not key:
        return {
            "ok": False,
            "reason": "missing BRAVE_SEARCH_API_KEY in API.txt",
            "query": query,
            "results": [],
        }
    url = "https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode({
        "q": query,
        "count": str(min(max(1, max_results), 10)),
        "search_lang": "en",
        "text_decorations": "0",
    })
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Luna/1.0",
            "X-Subscription-Token": key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as exc:
        _diag(f"web_search failed: {exc}")
        return {"ok": False, "reason": str(exc), "query": query, "results": []}

    items = (raw.get("web") or {}).get("results") or []
    results: List[Dict[str, str]] = []
    for item in items[:max_results]:
        extra = item.get("extra_snippets") or []
        snippet = item.get("description") or (extra[0] if extra else "")
        results.append({
            "title":   str(item.get("title", ""))[:200],
            "url":     str(item.get("url", "")),
            "snippet": str(snippet)[:400],
        })
    return {
        "ok": True,
        "query": query,
        "provider": "brave",
        "ts": now_iso(),
        "results": results,
    }


# ── GitHub API ────────────────────────────────────────────────────────────────

def _gh_headers() -> Dict[str, str]:
    vault = _read_vault()
    token = str(vault.get("GITHUB_TOKEN") or vault.get("LUNA_GITHUB_TOKEN") or "").strip()
    hdrs = {"Accept": "application/vnd.github+json", "User-Agent": "Luna-Command-Center"}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    return hdrs


def _gh_enabled() -> bool:
    vault = _read_vault()
    return bool(str(vault.get("GITHUB_TOKEN") or vault.get("LUNA_GITHUB_TOKEN") or "").strip())


def github_api_call(
    endpoint: str,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Make a GitHub REST v3 API call.

    Args:
        endpoint: Path after ``https://api.github.com``
                  (e.g. ``/repos/owner/repo/issues``).
        method:   HTTP verb — ``GET``, ``POST``, ``PATCH``, ``DELETE``.
        body:     Dict serialised to JSON for POST/PATCH requests.

    Returns::

        {"ok": True,  "status": 200, "data": {...}}   # success
        {"ok": False, "status": 404, "reason": "..."}  # HTTP error
        {"ok": False,              "reason": "..."}     # network error
    """
    if not _gh_enabled():
        return {"ok": False, "reason": "missing GITHUB_TOKEN in API.txt"}
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        f"https://api.github.com{endpoint}",
        data=data,
        headers=_gh_headers(),
        method=method,
    )
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return {"ok": True, "status": resp.status,
                    "data": json.loads(resp.read().decode("utf-8", errors="ignore"))}
    except urllib.error.HTTPError as exc:
        raw_b = exc.read() if hasattr(exc, "read") else b""
        err_data: Dict[str, Any] = {}
        try:
            err_data = json.loads(raw_b.decode("utf-8", errors="ignore"))
        except Exception:
            pass
        return {"ok": False, "status": exc.code, "reason": str(exc), "data": err_data}
    except Exception as exc:
        _diag(f"github_api_call {method} {endpoint} failed: {exc}")
        return {"ok": False, "reason": str(exc)}


def github_get_repo(repo: str) -> Dict[str, Any]:
    """Return key metadata for *owner/repo*."""
    r = github_api_call(f"/repos/{repo}")
    if not r.get("ok"):
        return r
    d = r["data"]
    return {
        "ok": True,
        "repo": repo,
        "description":    d.get("description", ""),
        "language":       d.get("language", ""),
        "stars":          d.get("stargazers_count", 0),
        "forks":          d.get("forks_count", 0),
        "open_issues":    d.get("open_issues_count", 0),
        "default_branch": d.get("default_branch", "main"),
        "url":            d.get("html_url", ""),
        "private":        d.get("private", False),
    }


def github_list_issues(
    repo: str,
    state: str = "open",
    limit: int = 10,
) -> Dict[str, Any]:
    """List issues for *owner/repo*."""
    r = github_api_call(f"/repos/{repo}/issues?state={state}&per_page={min(limit, 30)}")
    if not r.get("ok"):
        return r
    issues = [
        {
            "number":     item.get("number"),
            "title":      item.get("title", ""),
            "state":      item.get("state", ""),
            "url":        item.get("html_url", ""),
            "labels":     [lbl.get("name") for lbl in (item.get("labels") or [])],
            "created_at": str(item.get("created_at", ""))[:10],
        }
        for item in (r["data"] or [])
    ]
    return {"ok": True, "repo": repo, "state": state, "count": len(issues), "issues": issues}


def github_create_issue(
    repo: str,
    title: str,
    body: str,
    labels: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create a new issue in *owner/repo*."""
    payload: Dict[str, Any] = {"title": title, "body": body}
    if labels:
        payload["labels"] = labels
    r = github_api_call(f"/repos/{repo}/issues", method="POST", body=payload)
    if not r.get("ok"):
        return r
    d = r["data"]
    return {"ok": True, "number": d.get("number"), "url": d.get("html_url", ""), "title": title}


def github_read_file(repo: str, path: str, ref: str = "main") -> Dict[str, Any]:
    """Read a file from *owner/repo* at *ref* (branch/tag/sha).

    Decodes Base-64 content returned by the GitHub Contents API.
    """
    r = github_api_call(f"/repos/{repo}/contents/{path}?ref={ref}")
    if not r.get("ok"):
        return r
    d = r["data"]
    content_b64 = str(d.get("content") or "").replace("\n", "")
    try:
        content = base64.b64decode(content_b64).decode("utf-8", errors="ignore")
    except Exception:
        content = ""
    return {
        "ok":      True,
        "repo":    repo,
        "path":    path,
        "ref":     ref,
        "size":    d.get("size", 0),
        "sha":     d.get("sha", ""),
        "content": content,
        "url":     d.get("html_url", ""),
    }


def github_search_code(
    query: str,
    repo: Optional[str] = None,
    limit: int = 5,
) -> Dict[str, Any]:
    """Search code on GitHub (optionally scoped to *owner/repo*)."""
    q = f"{query} repo:{repo}" if repo else query
    r = github_api_call(
        f"/search/code?q={urllib.parse.quote(q)}&per_page={min(limit, 10)}"
    )
    if not r.get("ok"):
        return r
    items = [
        {
            "repo":  item.get("repository", {}).get("full_name", ""),
            "path":  item.get("path", ""),
            "url":   item.get("html_url", ""),
            "score": item.get("score", 0),
        }
        for item in (r["data"].get("items") or [])
    ]
    return {"ok": True, "query": query, "count": len(items), "items": items}


# ── Sandboxed shell ───────────────────────────────────────────────────────────

def run_project_shell(
    cmd: str,
    timeout: int = 30,
    cwd: Optional[str] = None,
) -> Dict[str, Any]:
    """Run *cmd* in a subprocess sandboxed to PROJECT_DIR.

    The resolved working directory must be within PROJECT_DIR; the call is
    rejected with ``{"ok": False, ...}`` otherwise.  stdout and stderr are
    captured (truncated to 4 KB / 2 KB respectively).

    Returns::

        {
            "ok": True,
            "returncode": 0,
            "stdout": "...",
            "stderr": "...",
            "cmd": "...",
        }
    """
    project_root = str(PROJECT_DIR.resolve()).lower()
    resolved_cwd = (Path(cwd).resolve() if cwd else PROJECT_DIR.resolve())
    if not str(resolved_cwd).lower().startswith(project_root):
        return {"ok": False, "reason": "cwd is outside project directory jail", "cmd": cmd}
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(resolved_cwd),
        )
        return {
            "ok":         result.returncode == 0,
            "returncode": result.returncode,
            "stdout":     result.stdout[:4000],
            "stderr":     result.stderr[:2000],
            "cmd":        cmd,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"timeout after {timeout}s", "cmd": cmd}
    except Exception as exc:
        _diag(f"run_project_shell failed: {exc}")
        return {"ok": False, "reason": str(exc), "cmd": cmd}


# ── Expanded project-level file access ────────────────────────────────────────

def _in_project_jail(path: Path) -> bool:
    try:
        return str(path.resolve()).lower().startswith(str(PROJECT_DIR.resolve()).lower())
    except Exception:
        return False


def project_read_file(path: str) -> Dict[str, Any]:
    """Read any file under PROJECT_DIR.

    Returns::

        {"ok": True, "path": "...", "size": N, "content": "..."}
    """
    try:
        file_path = Path(path).resolve()
        if not _in_project_jail(file_path):
            return {"ok": False, "reason": "path is outside project directory jail"}
        if not file_path.exists():
            return {"ok": False, "reason": "file not found", "path": path}
        content = safe_read_text(file_path)
        return {"ok": True, "path": str(file_path), "size": len(content), "content": content}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def project_write_file(
    path: str,
    content: str,
    verify_python: bool = True,
) -> Dict[str, Any]:
    """Write *content* atomically to any file under PROJECT_DIR.

    For ``.py`` files, ``py_compile`` is run on a temp copy before the real
    file is touched.  Pass ``verify_python=False`` to skip this gate for
    non-Python files you know are safe.

    Returns::

        {"ok": True, "path": "...", "size": N, "ts": "..."}
    """
    try:
        file_path = Path(path).resolve()
        if not _in_project_jail(file_path):
            return {"ok": False, "reason": "path is outside project directory jail"}
        if verify_python and file_path.suffix.lower() == ".py":
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            try:
                py_compile.compile(tmp_path, doraise=True)
            except py_compile.PyCompileError as exc:
                return {"ok": False, "reason": f"py_compile failed: {exc}", "path": path}
            finally:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except Exception:
                    pass
        safe_write_text(file_path, content)
        return {"ok": True, "path": str(file_path), "size": len(content), "ts": now_iso()}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def list_project_files(pattern: str = "**/*.py", max_results: int = 200) -> Dict[str, Any]:
    """Glob *pattern* within PROJECT_DIR and return relative paths.

    Examples::

        list_project_files("**/*.py")          # all Python files
        list_project_files("luna_modules/*.py") # just the modules
        list_project_files("memory/*.json")     # memory files
    """
    try:
        files = sorted(
            str(f.relative_to(PROJECT_DIR))
            for f in PROJECT_DIR.glob(pattern)
            if f.is_file()
        )[:max_results]
        return {"ok": True, "pattern": pattern, "count": len(files), "files": files}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
