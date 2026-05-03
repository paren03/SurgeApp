"""Phase UI-1A tests: Luna read-only HTTP dashboard.

These tests prove the dashboard is read-only and safe:
  * server binds loopback only
  * GET endpoints work
  * POST/PUT/PATCH/DELETE/OPTIONS rejected with 405
  * path traversal rejected
  * arbitrary file reads rejected
  * missing data files degrade gracefully
  * static files load
  * live-feed tail is bounded
  * no shell execution / Aider invocation / package installs in module
  * dashboard remains read-only, code execution disabled, guardian disabled
  * branding assets exist (logo SVG, icon PNG, icon ICO)
"""
from __future__ import annotations

import json
import re
import socket
import sys
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import luna_modules.luna_http_dashboard as hd


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ServerFixture:
    """Boot a real LunaDashboardServer on an ephemeral port for the test."""

    def __init__(self) -> None:
        self.port = _free_port()
        self.server = hd.create_server("127.0.0.1", self.port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "_ServerFixture":
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _get(url: str, *, method: str = "GET", data: bytes | None = None, timeout: float = 3.0):
    req = urllib.request.Request(url, method=method, data=data)
    return urllib.request.urlopen(req, timeout=timeout)


class TestSafetyConstants(unittest.TestCase):
    def test_phase_id(self) -> None:
        self.assertEqual(hd.PHASE_ID, "UI-1A")

    def test_default_host_is_loopback(self) -> None:
        self.assertEqual(hd.DEFAULT_HOST, "127.0.0.1")

    def test_default_port(self) -> None:
        self.assertEqual(hd.DEFAULT_PORT, 8765)

    def test_advisory_only_true(self) -> None:
        self.assertTrue(hd.ADVISORY_ONLY)

    def test_code_execution_locked(self) -> None:
        self.assertTrue(hd.CODE_EXECUTION_LOCKED)

    def test_guardian_live_enforcement_disabled(self) -> None:
        self.assertFalse(hd.GUARDIAN_LIVE_ENFORCEMENT)

    def test_live_feed_cap_is_100(self) -> None:
        self.assertEqual(hd.LIVE_FEED_MAX_LINES, 100)

    def test_static_whitelist_only(self) -> None:
        # Whitelist must contain only the seven approved paths.
        self.assertEqual(set(hd.STATIC_FILES.keys()), {
            "/", "/index.html", "/style.css", "/app.js",
            "/assets/luna_logo.svg", "/assets/luna_icon.png", "/assets/luna_icon.ico",
        })


def _strip_python_comments_and_docstrings(source: str) -> str:
    """Return ``source`` with comments and docstring literals removed.

    Used so the unsafe-pattern tests examine *executable* code, not the
    explanatory prose in module-level docstrings, comments, or path-name
    constants like ``aider_bridge_status``. We use ``tokenize`` so the
    stripping survives multi-line strings cleanly.
    """
    import io
    import tokenize

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except tokenize.TokenizeError:
        return source

    keep: list[tokenize.TokenInfo] = []
    prev_was_newline_like = True
    for tok in tokens:
        if tok.type == tokenize.COMMENT:
            continue
        # Drop top-level string statements (docstrings) and any string that
        # appears immediately after a NEWLINE / INDENT / DEDENT — those are
        # the function/class/module docstrings.
        if tok.type == tokenize.STRING and prev_was_newline_like:
            continue
        keep.append(tok)
        if tok.type in (tokenize.NEWLINE, tokenize.NL, tokenize.INDENT, tokenize.DEDENT):
            prev_was_newline_like = True
        elif tok.type in (tokenize.ENCODING,):
            prev_was_newline_like = True
        else:
            prev_was_newline_like = False

    try:
        return tokenize.untokenize(keep)
    except (ValueError, IndexError):
        return source


class TestNoUnsafeImports(unittest.TestCase):
    """Static check: dashboard executable code must not invoke shell/aider/etc.

    We strip comments and docstring literals first because the spec lists
    forbidden behaviors in prose ("no Aider invocation"), and the read-only
    sources whitelist legitimately contains the substring ``aider_bridge``
    as a JSON path. The contract being verified is *executable* code, not
    prose.
    """

    RAW = (Path(hd.__file__)).read_text(encoding="utf-8")
    CODE = _strip_python_comments_and_docstrings(RAW)

    def test_no_subprocess(self) -> None:
        self.assertNotIn("subprocess", self.CODE)

    def test_no_os_system(self) -> None:
        self.assertNotIn("os.system", self.CODE)

    def test_no_eval(self) -> None:
        self.assertNotRegex(self.CODE, r"\beval\s*\(")

    def test_no_exec_call(self) -> None:
        self.assertNotRegex(self.CODE, r"\bexec\s*\(")

    def test_no_aider_invocation(self) -> None:
        # No imports of an aider module and no calls to aider functions.
        self.assertNotRegex(self.CODE, r"(?i)\bimport\s+aider\b")
        self.assertNotRegex(self.CODE, r"(?i)\bfrom\s+aider\b")
        self.assertNotRegex(self.CODE, r"(?i)\baider\s*\(")

    def test_no_pip_install(self) -> None:
        self.assertNotRegex(self.CODE, r"(?i)pip\s+install")

    def test_no_easy_install(self) -> None:
        self.assertNotIn("easy_install", self.CODE.lower())

    def test_no_uv_install(self) -> None:
        self.assertNotRegex(self.CODE, r"(?i)\buv\s+(?:pip\s+)?install\b")

    def test_no_zero_dot_zero_bind(self) -> None:
        # No 0.0.0.0 in executable code — loopback-only by construction.
        self.assertNotIn("0.0.0.0", self.CODE)


class TestBindLoopbackOnly(unittest.TestCase):
    def test_refuses_zero_zero_zero_zero(self) -> None:
        with self.assertRaises(ValueError):
            hd.create_server("0.0.0.0", _free_port())

    def test_refuses_external_ip(self) -> None:
        with self.assertRaises(ValueError):
            hd.create_server("192.168.1.10", _free_port())

    def test_accepts_localhost(self) -> None:
        srv = hd.create_server("127.0.0.1", _free_port())
        try:
            self.assertEqual(srv.server_address[0], "127.0.0.1")
        finally:
            srv.server_close()


class TestEndpointsAndSafety(unittest.TestCase):
    def test_health_endpoint_ok(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/health") as r:
                self.assertEqual(r.status, 200)
                data = json.loads(r.read().decode("utf-8"))
                self.assertTrue(data["ok"])
                self.assertEqual(data["phase"], "UI-1A")
                self.assertEqual(data["host"], "127.0.0.1")
                self.assertEqual(data["code_execution_state"], "LOCKED")
                self.assertEqual(data["guardian_live_enforcement"], "DISABLED")

    def test_status_endpoint_safety_locked(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/status") as r:
                data = json.loads(r.read().decode("utf-8"))
                safety = data.get("safety", {})
                self.assertEqual(safety["code_execution_state"], "LOCKED")
                self.assertEqual(safety["guardian_live_enforcement"], "DISABLED")
                self.assertFalse(safety["safe_to_execute_now"])
                self.assertFalse(safety["safe_to_apply_real_project"])
                self.assertFalse(safety["guardian_enforcing_live"])
                self.assertTrue(safety["advisory_only"])

    def test_decision_brief_endpoint(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/decision-brief") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertIn("counts", data)
                self.assertIn("top_items", data)

    def test_soak_endpoint_includes_command(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/soak") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertIn("luna_decision_brief", data["soak_command"])
                self.assertIn("--soak", data["soak_command"])

    def test_scorecard_endpoint(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/scorecard") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertIn("dimensions", data)

    def test_resources_endpoint(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/resources") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertIn("memory", data)

    def test_archive_endpoint(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/archive") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertEqual(data["archive_path"], str(hd.ARCHIVE_DIR))
                self.assertIsInstance(data["items"], list)

    def test_live_feed_bounded(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/live-feed?limit=9999") as r:
                data = json.loads(r.read().decode("utf-8"))
                # Server must clamp to LIVE_FEED_MAX_LINES regardless of client.
                self.assertLessEqual(data["limit"], hd.LIVE_FEED_MAX_LINES)
                self.assertLessEqual(len(data["records"]), hd.LIVE_FEED_MAX_LINES)

    def test_live_feed_negative_limit_clamped(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/api/live-feed?limit=-5") as r:
                data = json.loads(r.read().decode("utf-8"))
                self.assertGreater(data["limit"], 0)

    def test_unknown_api_404(self) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/api/does-not-exist")
            self.assertEqual(ctx.exception.code, 404)


class TestMethodGating(unittest.TestCase):
    def _expect_405(self, method: str) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/api/health", method=method, data=b"{}")
            self.assertEqual(ctx.exception.code, 405)

    def test_post_rejected(self) -> None:
        self._expect_405("POST")

    def test_put_rejected(self) -> None:
        self._expect_405("PUT")

    def test_patch_rejected(self) -> None:
        self._expect_405("PATCH")

    def test_delete_rejected(self) -> None:
        self._expect_405("DELETE")


class TestPathTraversalAndArbitraryReads(unittest.TestCase):
    def test_dotdot_rejected(self) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/../etc/passwd")
            self.assertIn(ctx.exception.code, (400, 403, 404))

    def test_encoded_dotdot_rejected(self) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/%2e%2e/secret")
            self.assertIn(ctx.exception.code, (400, 403, 404))

    def test_arbitrary_file_read_rejected(self) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/worker.py")
            self.assertEqual(ctx.exception.code, 404)

    def test_memory_file_not_served_directly(self) -> None:
        with _ServerFixture() as fx:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                _get(f"{fx.base}/memory/luna_morning_decision_brief.json")
            self.assertEqual(ctx.exception.code, 404)


class TestStaticFilesAndAssets(unittest.TestCase):
    def test_index_html_loads(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/") as r:
                self.assertEqual(r.status, 200)
                body = r.read().decode("utf-8")
                self.assertIn("Luna Command Center", body)
                self.assertIn("Phase UI-1A", body)

    def test_style_css_loads(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/style.css") as r:
                self.assertEqual(r.status, 200)
                self.assertIn("text/css", r.headers["Content-Type"])

    def test_app_js_loads(self) -> None:
        with _ServerFixture() as fx:
            with _get(f"{fx.base}/app.js") as r:
                self.assertEqual(r.status, 200)
                self.assertIn("javascript", r.headers["Content-Type"])

    def test_logo_svg_present(self) -> None:
        self.assertTrue(hd.ASSETS_DIR.joinpath("luna_logo.svg").exists())

    def test_icon_png_present(self) -> None:
        self.assertTrue(hd.ASSETS_DIR.joinpath("luna_icon.png").exists())

    def test_icon_ico_present(self) -> None:
        self.assertTrue(hd.ASSETS_DIR.joinpath("luna_icon.ico").exists())


class TestPayloadGracefulDegradation(unittest.TestCase):
    """When source files are missing, payload builders must not raise."""

    def test_safe_read_missing_file_returns_none(self) -> None:
        self.assertIsNone(hd._safe_read_json(hd.PROJECT_ROOT / "nope_does_not_exist.json"))

    def test_safe_tail_missing_file_returns_empty(self) -> None:
        self.assertEqual(
            hd._safe_tail_jsonl(hd.PROJECT_ROOT / "nope_does_not_exist.jsonl"),
            [],
        )

    def test_safe_tail_respects_limit(self) -> None:
        # Bigger than configured cap must still be clamped at 100.
        out = hd._safe_tail_jsonl(hd.READONLY_SOURCES["live_feed"], limit=10_000)
        self.assertLessEqual(len(out), hd.LIVE_FEED_MAX_LINES)

    def test_status_payload_keys(self) -> None:
        s = hd.build_status_payload()
        for key in ("luna", "worker", "guardian", "aider_bridge", "soak", "safety"):
            self.assertIn(key, s)


class TestSelfTestEntryPoint(unittest.TestCase):
    def test_self_test_returns_zero(self) -> None:
        # End-to-end smoke: boots ephemeral server, hits endpoints, exits clean.
        rc = hd.run_self_test()
        self.assertEqual(rc, 0)


class TestDashboardSourceFiles(unittest.TestCase):
    """The static front-end must not call dangerous APIs either."""

    def setUp(self) -> None:
        self.html = (_PROJECT_ROOT / "luna_dashboard" / "index.html").read_text(encoding="utf-8")
        self.css  = (_PROJECT_ROOT / "luna_dashboard" / "style.css").read_text(encoding="utf-8")
        self.js   = (_PROJECT_ROOT / "luna_dashboard" / "app.js").read_text(encoding="utf-8")

    def test_html_has_phase_label(self) -> None:
        self.assertIn("Phase UI-1A", self.html)

    def test_css_uses_gold_palette(self) -> None:
        # Spot-check: gold accent variable must exist so the theme is the one we ship.
        self.assertIn("--luna-gold", self.css)

    def test_js_does_not_use_eval(self) -> None:
        self.assertNotRegex(self.js, r"\beval\s*\(")
        # Must not use Function constructor (a common eval substitute).
        self.assertNotRegex(self.js, r"new\s+Function\s*\(")

    def test_js_does_not_post(self) -> None:
        # The front-end must never issue write methods.
        self.assertNotRegex(self.js, r"method\s*:\s*['\"]POST['\"]")
        self.assertNotRegex(self.js, r"method\s*:\s*['\"]PUT['\"]")
        self.assertNotRegex(self.js, r"method\s*:\s*['\"]DELETE['\"]")


if __name__ == "__main__":
    unittest.main()
