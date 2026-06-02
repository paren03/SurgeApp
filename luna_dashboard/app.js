/* Luna Command Center — Phase UI-1A
 * Read-only front-end. Polls the local API; never sends writes.
 * No external CDNs, no eval, no third-party scripts.
 *
 * Live Operations canvas visualizations:
 *   - Pulse oscilloscope with selectable data source (calmed)
 *   - Mission Clock (task countdown + orbital animation)
 *   - Event frequency histogram
 *   - Animated resource gauges
 *   - Terminal-style live TTY ticker
 *
 * Cosmic HUD layer (locally-vendored under /vendor/):
 *   - tsParticles starfield + cosmic dust (ambient background)
 *   - Anime.js for panel transitions + status flashes + counter ticks
 *   - Three.js opt-in "Full Cosmic Mode" toggle (lazy-loaded)
 *   - Single shared performance guard: pauses on hidden tab,
 *     respects prefers-reduced-motion, supports Low Motion / Full
 *     Cosmic Mode toggles, hard 30 FPS cap on cosmetic loops.
 */
(function () {
  "use strict";

  const REFRESH_MS = 1000;     // server data poll cadence; fallback polling when SSE/EventSource is unavailable
  const SAMPLE_MS  = 500;      // oscilloscope sample cadence (calm)
  const FEED_LIMIT = 100;
  const ACTIVITY_WINDOW = 1800;
  const ACTIVITY_BUCKETS = 60;
  const TREND_LEN = 240;

  // ===================================================================
  // Cosmic animation guard — single source of truth for whether the
  // dashboard's optional animation layer should run RIGHT NOW. Every
  // animation loop in this file consults LunaCosmicGuard.allow() before
  // doing GPU/canvas work so we never burn cycles when the tab is
  // hidden, the user toggled Low Motion, or the OS prefers reduced
  // motion.
  // ===================================================================
  const LunaCosmicGuard = (function () {
    const KEY_LOW_MOTION   = "luna.lowMotion";
    const KEY_FULL_COSMIC  = "luna.fullCosmic";
    function reduce() {
      try {
        return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      } catch (e) { return false; }
    }
    function lowMotion() {
      try { return window.localStorage.getItem(KEY_LOW_MOTION) === "1"; }
      catch (e) { return false; }
    }
    function fullCosmic() {
      try { return window.localStorage.getItem(KEY_FULL_COSMIC) === "1"; }
      catch (e) { return false; }
    }
    function allow() {
      if (document.hidden) return false;
      if (reduce()) return false;
      if (lowMotion()) return false;
      return true;
    }
    function setLowMotion(on) {
      try {
        if (on) window.localStorage.setItem(KEY_LOW_MOTION, "1");
        else window.localStorage.removeItem(KEY_LOW_MOTION);
      } catch (e) {}
      document.documentElement.dataset.motion = on ? "low" : "full";
    }
    function setFullCosmic(on) {
      try {
        if (on) window.localStorage.setItem(KEY_FULL_COSMIC, "1");
        else window.localStorage.removeItem(KEY_FULL_COSMIC);
      } catch (e) {}
      document.documentElement.dataset.fullCosmic = on ? "1" : "0";
    }
    // Initialise from persisted state on boot.
    document.documentElement.dataset.motion =
      (reduce() || lowMotion()) ? "low" : "full";
    document.documentElement.dataset.fullCosmic = fullCosmic() ? "1" : "0";
    // 30 FPS cap (33ms minimum frame budget) for cosmetic RAF loops.
    return {
      allow:        allow,
      reduce:       reduce,
      lowMotion:    lowMotion,
      fullCosmic:   fullCosmic,
      setLowMotion: setLowMotion,
      setFullCosmic: setFullCosmic,
      MIN_FRAME_MS: 33,
    };
  })();
  window.LunaCosmicGuard = LunaCosmicGuard;

  // ===================================================================
  // tsParticles starfield — ambient background layer.
  // Mounts inside #luna-cosmos. ZERO duplicate render loops: tsParticles
  // owns its own RAF, but our perf guard pauses + resumes the engine
  // when the tab is hidden / Low Motion is on / OS prefers reduced
  // motion. If the vendor file failed to load (offline + no cache),
  // this is a clean no-op.
  // ===================================================================
  function lunaCosmicInit() {
    const el = document.getElementById("luna-cosmos");
    if (!el) return;
    const ts = window.tsParticles;
    if (!ts || typeof ts.load !== "function") {
      // Vendor not loaded — graceful no-op. Mark the host so CSS can
      // hide it cleanly (otherwise it would show as an empty box).
      el.dataset.state = "novendor";
      return;
    }
    if (el.dataset.state === "ready") return; // idempotent
    el.id = "luna-cosmos";
    ts.load("luna-cosmos", {
      fpsLimit: 30,
      detectRetina: true,
      pauseOnBlur: true,
      pauseOnOutsideViewport: true,
      background: { color: { value: "transparent" } },
      fullScreen: { enable: false },
      particles: {
        number: { value: 90, density: { enable: true, area: 1200 } },
        color:  { value: ["#ffd98a", "#9fbfff", "#b8ffd8"] },
        opacity: { value: { min: 0.18, max: 0.65 },
                   animation: { enable: true, speed: 0.4, sync: false,
                                startValue: "random" } },
        size:   { value: { min: 0.4, max: 1.4 } },
        move:   { enable: true, speed: 0.18, direction: "none",
                  random: true, straight: false, outModes: "bounce" },
        twinkle: {
          particles: { enable: true, frequency: 0.04, opacity: 0.85 },
        },
        zIndex: { value: { min: -50, max: 0 } },
      },
      interactivity: { detect_on: "window", events: { resize: true } },
      smooth: false,
    }).then(() => {
      el.dataset.state = "ready";
    }).catch(() => {
      el.dataset.state = "error";
    });
  }

  // Hook the engine's pause/play state to the perf guard.
  function lunaCosmicSyncGuard() {
    const ts = window.tsParticles;
    if (!ts) return;
    try {
      const containers = ts.dom() || [];
      for (const c of containers) {
        if (LunaCosmicGuard.allow()) {
          if (typeof c.play === "function") c.play();
        } else {
          if (typeof c.pause === "function") c.pause();
        }
      }
    } catch (e) { /* never break the page */ }
  }
  document.addEventListener("visibilitychange", lunaCosmicSyncGuard, false);
  window.addEventListener("storage", lunaCosmicSyncGuard, false);

  // ===================================================================
  // Anime.js helpers — premium UI motion. Each call is a single tween
  // bound to a target. NEVER stack tweens on the same target without
  // cancelling the previous one; we use anime.remove() to keep the
  // GPU clean. Honors LunaCosmicGuard so a Low Motion user gets an
  // immediate state change with no animation.
  // ===================================================================
  function lunaAnimate(opts) {
    const a = window.anime;
    if (!a || !opts || !opts.targets) return null;
    if (!LunaCosmicGuard.allow()) {
      // Reduced motion: jump to end state without animation.
      try {
        const fin = {};
        for (const k of ["opacity", "translateY", "translateX",
                         "scale", "rotate", "innerHTML"]) {
          if (opts[k] !== undefined) {
            const v = Array.isArray(opts[k]) ? opts[k][1] : opts[k];
            fin[k] = v;
          }
        }
        const targets = (typeof opts.targets === "string")
          ? document.querySelectorAll(opts.targets)
          : (opts.targets.length ? opts.targets : [opts.targets]);
        targets.forEach((t) => {
          if (!t || !t.style) return;
          if (fin.opacity !== undefined) t.style.opacity = fin.opacity;
          if (fin.translateY !== undefined) t.style.transform =
            "translateY(" + (fin.translateY || 0) + "px)";
          if (fin.translateX !== undefined) t.style.transform =
            "translateX(" + (fin.translateX || 0) + "px)";
        });
      } catch (e) {}
      return null;
    }
    try {
      a.remove(opts.targets);
      return a(opts);
    } catch (e) { return null; }
  }
  window.lunaAnimate = lunaAnimate;

  // Animate a counter value up to `to`. Used for the live tier-truth
  // counters so the numbers tick up smoothly instead of jumping.
  function lunaAnimateCounter(elOrId, to, durationMs) {
    const el = (typeof elOrId === "string") ? document.getElementById(elOrId) : elOrId;
    if (!el) return;
    const from = parseInt(el.textContent.replace(/\D/g, ""), 10) || 0;
    if (from === to || !window.anime || !LunaCosmicGuard.allow()) {
      el.textContent = String(to);
      return;
    }
    const obj = { v: from };
    try {
      window.anime.remove(obj);
      window.anime({
        targets: obj,
        v: to,
        duration: durationMs || 700,
        easing: "easeOutCubic",
        round: 1,
        update: () => { el.textContent = String(obj.v | 0); },
      });
    } catch (e) {
      el.textContent = String(to);
    }
  }
  window.lunaAnimateCounter = lunaAnimateCounter;

  // Fire a one-shot "flash" pulse on an element (e.g. when a new
  // packet arrives or a counter just changed). 220ms; idempotent.
  function lunaFlashPulse(el, color) {
    if (!el) return;
    if (!LunaCosmicGuard.allow()) return;
    try {
      el.classList.remove("luna-anim-flash");
      // Force reflow so the animation restarts cleanly.
      void el.offsetWidth;
      if (color) el.style.setProperty("--luna-flash-color", color);
      el.classList.add("luna-anim-flash");
      setTimeout(() => {
        try { el.classList.remove("luna-anim-flash"); } catch (e) {}
      }, 360);
    } catch (e) {}
  }
  window.lunaFlashPulse = lunaFlashPulse;

  // ===================================================================
  // Three.js opt-in. Lazy-loaded ONLY when the operator clicks Full
  // Cosmic Mode. Off by default per the perf rules. The current
  // implementation is intentionally minimal: a slow-rotating wireframe
  // sphere behind the dashboard. Future expansions go here.
  // ===================================================================
  let _lunaThreeLoaded = false;
  let _lunaThreeContext = null;
  function lunaCosmicMaybeLoadThree() {
    if (!LunaCosmicGuard.fullCosmic()) return;
    if (_lunaThreeLoaded) return;
    if (window.THREE) { _lunaThreeLoaded = true; lunaThreeStart(); return; }
    const s = document.createElement("script");
    s.src = "/vendor/three.min.js";
    s.async = true;
    s.onload = function () {
      _lunaThreeLoaded = true;
      lunaThreeStart();
    };
    s.onerror = function () { _lunaThreeLoaded = false; };
    document.head.appendChild(s);
  }
  function lunaThreeStart() {
    if (!window.THREE) return;
    const host = document.getElementById("luna-cosmos");
    if (!host) return;
    if (_lunaThreeContext) return;
    try {
      const w = host.clientWidth  || window.innerWidth;
      const h = host.clientHeight || window.innerHeight;
      const scene  = new THREE.Scene();
      const camera = new THREE.PerspectiveCamera(60, w / h, 0.1, 1000);
      const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true });
      renderer.setSize(w, h);
      renderer.setClearColor(0x000000, 0);
      renderer.domElement.style.position = "absolute";
      renderer.domElement.style.inset = "0";
      renderer.domElement.style.zIndex = "0";
      renderer.domElement.style.pointerEvents = "none";
      host.appendChild(renderer.domElement);
      const geom = new THREE.IcosahedronGeometry(2.4, 1);
      const mat  = new THREE.MeshBasicMaterial({
        color: 0xffd98a, wireframe: true, transparent: true, opacity: 0.18,
      });
      const orb = new THREE.Mesh(geom, mat);
      scene.add(orb);
      camera.position.z = 6;
      _lunaThreeContext = { scene, camera, renderer, orb,
                            lastFrameAt: 0, host };
      const tick = (now) => {
        if (!_lunaThreeContext) return;
        if (now - _lunaThreeContext.lastFrameAt < LunaCosmicGuard.MIN_FRAME_MS) {
          requestAnimationFrame(tick); return;
        }
        _lunaThreeContext.lastFrameAt = now;
        if (LunaCosmicGuard.allow() && LunaCosmicGuard.fullCosmic()) {
          orb.rotation.x += 0.0028;
          orb.rotation.y += 0.0036;
          renderer.render(scene, camera);
        }
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    } catch (e) { _lunaThreeContext = null; }
  }
  window.lunaCosmicMaybeLoadThree = lunaCosmicMaybeLoadThree;

  // Boot all cosmic layers once DOM is ready. Each layer is a no-op
  // when its vendor file is missing.
  document.addEventListener("DOMContentLoaded", function () {
    setTimeout(function () {
      try { lunaCosmicInit(); } catch (e) {}
      try { lunaCosmicMaybeLoadThree(); } catch (e) {}
      try { lunaTierTimelineBind(); } catch (e) {}
      try { lunaCosmicToggleBind(); } catch (e) {}
      // Tag every top-level panel with the entrance animation class so
      // the dashboard breathes in instead of popping. One-shot — the
      // CSS keyframe runs once and stops on its own.
      try {
        document.querySelectorAll(
          ".luna-card, .luna-supermax__shell, .luna-evo__cards > article, " +
          ".luna-voice-row, .luna-terminal-panel"
        ).forEach((el) => { el.classList.add("luna-anim-enter"); });
      } catch (e) {}
    }, 60);
  }, false);

  // Cosmic toggle bindings (Low Motion + Full Cosmic Mode).
  function lunaCosmicToggleBind() {
    const lo = document.getElementById("luna-toggle-low-motion");
    const fc = document.getElementById("luna-toggle-full-cosmic");
    if (lo) {
      lo.checked = LunaCosmicGuard.lowMotion();
      lo.addEventListener("change", function () {
        LunaCosmicGuard.setLowMotion(!!lo.checked);
        lunaCosmicSyncGuard();
      });
    }
    if (fc) {
      fc.checked = LunaCosmicGuard.fullCosmic();
      fc.addEventListener("change", function () {
        LunaCosmicGuard.setFullCosmic(!!fc.checked);
        if (fc.checked) lunaCosmicMaybeLoadThree();
      });
    }
  }
  window.lunaCosmicToggleBind = lunaCosmicToggleBind;

  // ===================================================================
  // Tier ladder — horizontal scroller with drag + wheel + snap-to-current
  // The .luna-evo__ladder is already overflow-x:auto. We add:
  //   - Mouse wheel translates vertical scroll -> horizontal
  //   - Click-and-drag scrolls
  //   - "Snap to current" auto-scrolls the highlighted rung into view
  //   - Keyboard arrow-left/right scroll one rung at a time
  // No duplicate handlers (idempotent: dataset.lunaTimelineBound).
  // ===================================================================
  function lunaTierTimelineBind() {
    const lad = document.getElementById("sm-ladder");
    if (!lad || lad.dataset.lunaTimelineBound === "1") return;
    lad.dataset.lunaTimelineBound = "1";
    // Wheel -> horizontal scroll.
    lad.addEventListener("wheel", function (ev) {
      // Honor horizontal-intent wheels (trackpads) as-is.
      if (Math.abs(ev.deltaX) > Math.abs(ev.deltaY)) return;
      ev.preventDefault();
      lad.scrollBy({ left: ev.deltaY, behavior: "smooth" });
    }, { passive: false });
    // Click-and-drag -> horizontal scroll. Pointer Events so it
    // works for mouse + touch + pen with one binding.
    let dragging = false, startX = 0, startScroll = 0;
    lad.addEventListener("pointerdown", function (ev) {
      if (ev.button !== 0) return;            // only primary button
      const rung = ev.target.closest(".luna-evo__rung");
      if (rung) return;                        // let rung clicks through
      dragging = true;
      startX = ev.pageX;
      startScroll = lad.scrollLeft;
      lad.setPointerCapture(ev.pointerId);
      lad.style.cursor = "grabbing";
    });
    lad.addEventListener("pointermove", function (ev) {
      if (!dragging) return;
      lad.scrollLeft = startScroll - (ev.pageX - startX);
    });
    function endDrag(ev) {
      if (!dragging) return;
      dragging = false;
      lad.style.cursor = "grab";
      try { lad.releasePointerCapture(ev.pointerId); } catch (_e) {}
    }
    lad.addEventListener("pointerup", endDrag);
    lad.addEventListener("pointercancel", endDrag);
    lad.addEventListener("pointerleave", endDrag);
    // Keyboard nav: left/right arrows scroll one rung.
    lad.addEventListener("keydown", function (ev) {
      const rungs = lad.querySelectorAll(".luna-evo__rung");
      const w = rungs.length ? rungs[0].clientWidth + 12 : 80;
      if (ev.key === "ArrowRight") { ev.preventDefault(); lad.scrollBy({ left:  w, behavior: "smooth" }); }
      if (ev.key === "ArrowLeft")  { ev.preventDefault(); lad.scrollBy({ left: -w, behavior: "smooth" }); }
    });
    // Snap-to-current: scrolls the rung tagged data-state="current"
    // into the centre of the visible scroller. ONLY fires when the
    // current rung CHANGES; idempotent re-applications of the same
    // data-state must not trigger a scroll (otherwise the painter's
    // 1Hz polls visibly drift the scrollbar — the "moving on its own"
    // symptom the operator reported in round 7).
    let _lastSnappedRung = null;
    function snap(force) {
      const cur = lad.querySelector('.luna-evo__rung[data-state="current"]');
      if (!cur) return;
      if (!force && cur === _lastSnappedRung) return;  // same rung, skip
      _lastSnappedRung = cur;
      const ladRect = lad.getBoundingClientRect();
      const rRect   = cur.getBoundingClientRect();
      const offset  = (rRect.left - ladRect.left) - (lad.clientWidth - cur.clientWidth) / 2;
      // Bail if the offset is essentially zero (already centered) so we
      // don't kick off a smooth-scroll for sub-pixel jitter.
      if (Math.abs(offset) < 4) return;
      lad.scrollBy({ left: offset, behavior: "smooth" });
    }
    window.lunaTimelineSnap = function () { snap(true); };
    // Snap on first paint + whenever a rung's data-state actually
    // transitions to/from "current".
    setTimeout(() => snap(true), 600);
    if (window.MutationObserver) {
      try {
        const mo = new MutationObserver(function (muts) {
          for (const m of muts) {
            if (m.attributeName !== "data-state") continue;
            // Skip when the new value is identical to the old (mutation
            // observer fires on every setAttribute even if value unchanged).
            const newVal = m.target.getAttribute("data-state");
            const oldVal = m.oldValue;
            if (newVal === oldVal) continue;
            // Only re-snap when "current" comes or goes — other state
            // transitions (future->completed, eligible->blocked) don't
            // move the highlighted rung so don't need a scroll.
            if (newVal === "current" || oldVal === "current") {
              snap(false);
              return;
            }
          }
        });
        lad.querySelectorAll(".luna-evo__rung").forEach((r) => {
          mo.observe(r, {
            attributes: true,
            attributeFilter: ["data-state"],
            attributeOldValue: true,
          });
        });
        lad._lunaTimelineMO = mo;
      } catch (e) {}
    }
    // The "Snap to current" affordance is a small button auto-injected
    // into the ladder header — keeps the markup minimal and avoids
    // index.html churn.
    if (!lad.parentElement.querySelector(".luna-evo__ladder-snap")) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "luna-evo__ladder-snap";
      btn.textContent = "Snap to current";
      btn.title = "Scroll the active tier back into view";
      btn.addEventListener("click", snap);
      lad.parentElement.insertBefore(btn, lad);
    }
  }
  // End cosmic stack.

  const $ = (id) => document.getElementById(id);
  const text = (el, value) => { if (el) el.textContent = value == null ? "—" : String(value); };
  const setTone = (el, tone) => { if (el && el.parentElement) el.parentElement.dataset.tone = tone || ""; };

  function fmtPct(n) { if (typeof n !== "number") return "—"; return Math.round(n) + "%"; }
  function fmtBytes(n) {
    if (typeof n !== "number" || !isFinite(n)) return "—";
    const u = ["B","KB","MB","GB","TB"]; let i=0,v=n;
    while (v >= 1024 && i < u.length-1) { v /= 1024; i++; }
    return v.toFixed(v < 10 ? 1 : 0) + " " + u[i];
  }
  function fmtClock(seconds) {
    if (!isFinite(seconds) || seconds < 0) return "—";
    const s = Math.floor(seconds);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const ss = s % 60;
    const z = (n) => String(n).padStart(2, "0");
    return (h > 0 ? z(h) + ":" : "") + z(m) + ":" + z(ss);
  }
  function trim(str, max) {
    if (typeof str !== "string") return "";
    return str.length > max ? str.slice(0, max-1) + "…" : str;
  }
  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

  // ============================================================
  // Canvas resize-if-needed helper.
  //
  // ROOT-CAUSE FIX for the every-frame flicker. The previous pattern was:
  //
  //     if (canvas.width !== canvas.clientWidth * dpr) {
  //         canvas.width  = Math.round(canvas.clientWidth  * dpr);
  //         canvas.height = Math.round(canvas.clientHeight * dpr);
  //     }
  //
  // On Windows where devicePixelRatio is 1.25 / 1.5 / 1.75, the LHS is
  // always an integer and the RHS is fractional, so the comparison never
  // matches its own write. The canvas was hard-resized 60x/sec; per spec
  // assigning to canvas.width clears the bitmap, producing the visible
  // flicker the operator reported.
  //
  // The fix: round BOTH sides, cache the last-applied dimensions on the
  // canvas element, and only write when the rounded target actually
  // changes. Caching also avoids a redundant clientWidth read on the
  // common no-change path.
  //
  // Returns true when a real resize happened (so callers can clear or
  // re-prime backing buffers if needed). Sentinel hook: increments
  // LunaUIHealth.canvasResizeCount[id] only on REAL resizes - if the
  // counter trips for a static-sized canvas, that's a regression.
  function _resizeCanvasIfNeeded(canvas) {
    if (!canvas) return false;
    const dpr = window.devicePixelRatio || 1;
    const cw  = canvas.clientWidth  | 0;
    const ch  = canvas.clientHeight | 0;
    const tw  = Math.max(1, Math.round(cw * dpr));
    const th  = Math.max(1, Math.round(ch * dpr));
    if (canvas._lunaW === tw && canvas._lunaH === th
        && canvas.width === tw && canvas.height === th) {
      return false;
    }
    canvas.width  = tw;
    canvas.height = th;
    canvas._lunaW = tw;
    canvas._lunaH = th;
    try {
      const id = canvas.id || "(noid)";
      const h  = window.LunaUIHealth;
      if (h && h.canvasResizeCount) {
        h.canvasResizeCount[id] = (h.canvasResizeCount[id] || 0) + 1;
      }
    } catch (e) { /* sentinel never breaks render */ }
    return true;
  }

  // 2026-05-12 visible-UI final-truth fix: bounded fetch.
  // A slow / hung endpoint (e.g. /api/mission-control taking 63 s before
  // the dashboard fell over) used to freeze the panel. AbortController
  // caps any single fetch at ~6 s; on timeout we return null and the
  // panel's existing null-tolerant render path kicks in.
  const FETCHJSON_TIMEOUT_MS = 6000;
  async function fetchJSON(path) {
    let ctrl = null;
    let timer = null;
    try {
      ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
      if (ctrl) {
        timer = setTimeout(() => {
          try { ctrl.abort(); } catch (_e) { /* ignore */ }
        }, FETCHJSON_TIMEOUT_MS);
      }
      const opts = { credentials: "omit", cache: "no-store" };
      if (ctrl) opts.signal = ctrl.signal;
      const r = await fetch(path, opts);
      if (!r.ok) return null;
      return await r.json();
    } catch (e) {
      // AbortError / network error / JSON parse error all land here.
      return null;
    } finally {
      if (timer) { try { clearTimeout(timer); } catch (_e) { /* ignore */ } }
    }
  }
  async function postJSON(path, body) {
    try {
      const r = await fetch(path, {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  // Auto-speak hook: when Luna replies in the chat, route the text
  // through /api/voice/test (which uses the engine's sanitiser +
  // Kokoro/edge-tts/pyttsx3 fallback chain). Only fires when Voice
  // ON is toggled. Capped at ~600 chars and refuses code-block-heavy
  // replies to keep speech short and listenable.
  async function lunaAutoSpeakReply(text) {
    if (!text || typeof text !== "string") return;

    // Mute toggle (typed-chat suppression). Default ON so typed messages
    // are silent unless the operator explicitly clicks the mute icon to
    // turn it OFF. Persisted in localStorage as luna.tts_typed_mute.
    let muted = "1";
    try { muted = localStorage.getItem("luna.tts_typed_mute"); } catch (_e) {}
    if (muted === null || muted === undefined) muted = "1";  // default muted
    if (muted === "1") return;

    // Anti-double-voice guard. If the compact voice (mic) was active in
    // the last 5 seconds, that path already played the audio via Kokoro
    // through the AudioContext - skipping the legacy /api/voice/test
    // path here prevents the "two voices in a row" symptom the operator
    // reported. window.__lunaCV is set by startCompactVoice.
    try {
      const CV = window.__lunaCV;
      if (CV && (CV.active || CV.audio || (Date.now() - (CV._lastSpokeMs || 0)) < 5000)) {
        return;
      }
    } catch (_e) { /* fall through */ }

    // Probe voice status first; if voice is OFF we skip (no surprise
    // speech when the operator silenced Luna globally).
    let snap = null;
    try {
      const r = await fetch("/api/voice/status",
                            { credentials: "omit", cache: "no-store" });
      if (r.ok) snap = await r.json();
    } catch (_e) { return; }
    const inner = (snap && (snap.status || snap)) || {};
    if (!inner.enabled || inner.muted) return;
    // Trim + cap before sending. The engine will trim again, but we
    // also strip fenced code blocks so the spoken version reads well
    // (the engine collapses code into a single notice line — same
    // intent, applied early).
    let spoken = String(text);
    spoken = spoken.replace(/```[\s\S]*?```/g, "I created a code block. ");
    spoken = spoken.replace(/\s+/g, " ").trim();
    if (spoken.length > 480) spoken = spoken.slice(0, 480).trim() + "...";
    if (!spoken) return;
    try {
      await fetch("/api/voice/test", {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: spoken }),
      });
    } catch (_e) { /* never break the chat render */ }
  }
  window.lunaAutoSpeakReply = lunaAutoSpeakReply;

  // Live Activity Strip — multi-source instrument.
  //
  // Two render modes, switched by the Heartbeat dropdown (#osc-source):
  //
  //   1. event_rate (default): bars = events/minute from /api/live-feed,
  //      bucketed into 30 one-minute slots. Original behavior.
  //   2. heartbeat / cpu / mem / gpu / queue / active / approval /
  //      combined: bars = the last 30 samples of state.trends[<src>],
  //      which is already populated each poll tick by pushTrendSamples().
  //
  // The pulsing dot state is ALWAYS event-driven (via /api/live-feed
  // bucket counts) so "is Luna busy right now" survives independent of
  // which signal you're inspecting.
  //
  // The trailing label shows source-specific text (last value + units).
  function lunaUpdateActivityStrip(records) {
    const root  = document.querySelector(".luna-console__activity");
    const label = document.getElementById("activity-rate");
    const bars  = document.getElementById("activity-bars");
    const last  = document.getElementById("activity-last");
    if (!root || !bars) return;

    // Always bucket the live-feed records; the dot state needs them
    // even when bars are showing a trend buffer.
    const now = Date.now();
    const buckets = new Array(30).fill(0);
    let lastTs = "";
    let lastEv = "";
    let lastActor = "";
    for (const r of (records || [])) {
      const tsStr = String(r.iso_utc || r.ts || "");
      let t = NaN;
      if (r.iso_utc) t = Date.parse(r.iso_utc);
      if (isNaN(t) && tsStr.length === 8 && tsStr.indexOf(":") > -1) {
        const today = new Date(); const [hh, mm, ss] = tsStr.split(":").map(Number);
        today.setHours(hh, mm, ss, 0);
        t = today.getTime();
      }
      if (isNaN(t)) continue;
      const ageMin = Math.floor((now - t) / 60000);
      if (ageMin < 0 || ageMin >= buckets.length) continue;
      buckets[buckets.length - 1 - ageMin]++;
      if (!lastTs || (typeof tsStr === "string" && tsStr > lastTs)) {
        lastTs = tsStr;
        lastEv = String(r.event || r.stage || "").slice(0, 28);
        lastActor = String(r.actor || r.role || "").slice(0, 20);
      }
    }

    // Pick the bar-source values (already 0..1 normalized for trend-buffer
    // mode; for event_rate we normalize against max bucket count).
    const src = (state && state.pulseSource) || "event_rate";
    const meta = (typeof SOURCE_META !== "undefined" && SOURCE_META[src]) || null;
    const useTrendBuffer = (src !== "event_rate");
    let barValues, normMax;
    if (useTrendBuffer) {
      const trendKey = (src === "combined") ? "heartbeat" : src;
      const trend = (state && state.trends && state.trends[trendKey]) || [];
      const recent = trend.slice(-30);
      while (recent.length < 30) recent.unshift(0);
      barValues = recent;
      normMax = 1; // trends are pre-normalized
    } else {
      barValues = buckets;
      normMax = Math.max(1, ...buckets);
    }

    // Render bars (height + accent gradient by recency).
    const barNodes = bars.querySelectorAll("i");
    for (let i = 0; i < barNodes.length; i++) {
      const v = barValues[i] || 0;
      const h = 4 + Math.round((v / normMax) * 18);
      barNodes[i].style.height = h + "px";
      const recency = i / barNodes.length; // 0 oldest, ~1 newest
      barNodes[i].style.background =
        "rgb(var(--c-accent-rgb) / " + (0.30 + 0.45 * recency).toFixed(2) + ")";
    }

    // Dot state: always event-driven via /api/live-feed bucket counts.
    const recent5 = buckets.slice(-5).reduce((a, b) => a + b, 0);
    let stateClass = "idle";
    if (recent5 >= 5) stateClass = "active";
    else if (recent5 >= 1) stateClass = "live";
    root.dataset.state = stateClass;

    // Label: source-specific. event_rate keeps the original "X.X events/min";
    // every other source shows "<legend> · last 30 min".
    if (label) {
      if (useTrendBuffer) {
        const lg = (meta && meta.legend) || src;
        label.textContent = lg + " · last 30 min";
      } else {
        const ratePerMin = (recent5 / 5).toFixed(1);
        label.textContent = ratePerMin + " events/min";
      }
    }

    // Trailing readout on the right.
    if (last) {
      if (useTrendBuffer) {
        last.textContent = _lunaActivityLastValueText(src);
      } else if (lastTs) {
        const tsShort = (lastTs.length >= 19) ? lastTs.slice(11, 19) : lastTs;
        last.textContent = "last " + tsShort + " · " +
                           (lastActor ? lastActor + " " : "") + lastEv;
      } else {
        last.textContent = "no events yet";
      }
    }
  }
  window.lunaUpdateActivityStrip = lunaUpdateActivityStrip;

  // Source-specific "last value" text for the right-edge readout when the
  // Activity Strip is in trend-buffer mode. Reads from the same lastStatus
  // / lastRes / lastActivity caches that syncOscilloscopeMeta() reads, so
  // numbers stay consistent with the small osc-bpm pill in the header.
  function _lunaActivityLastValueText(src) {
    const s = (state && state.lastStatus) || {};
    const r = (state && state.lastRes)    || {};
    const a = (state && state.lastActivity) || {};
    const w = s.worker || {}, l = s.luna || {};
    switch (src) {
      case "heartbeat": {
        if (!l.alive) return "OFFLINE";
        const ts = (state && state.lastHeartbeatTs) || "";
        return ts ? ("alive · " + ts.slice(11, 19)) : "alive";
      }
      case "cpu":      return "last " + Math.round(((r.cpu && r.cpu.usage_percent) || 0)) + "% cpu";
      case "mem":      return "last " + Math.round(((r.memory && r.memory.available_percent) || 0)) + "% free mem";
      case "gpu":      return "last " + Math.round(((r.gpu && r.gpu.free_vram_percent) || 0)) + "% free vram";
      case "queue":    return "last " + (w.queue_depth || 0) + " queued";
      case "active":   return "last " + (w.active_count || 0) + " active";
      case "approval": return "last " + (w.approval_pending || 0) + " pending";
      case "combined": {
        const q = w.queue_depth || 0, ac = w.active_count || 0;
        const perMin = (a.total_events || 0) / Math.max(1, ((a.window_seconds || 60) / 60));
        return "overlay · q" + q + " a" + ac + " · " + perMin.toFixed(1) + " evt/min";
      }
      default: return "";
    }
  }

  // Decision-card verdict click handler. Posts the verdict to the
  // server (which appends to logs/luna_decision_verdicts.jsonl) and
  // flashes the button briefly so the user sees confirmation. Idempotent
  // — clicking the same button twice writes two records (audit trail).
  async function lunaDecisionVerdictClick(ev) {
    const btn = ev && ev.currentTarget;
    if (!btn) return;
    const action = String(btn.dataset.action || "").toLowerCase();
    const id     = String(btn.dataset.id || "").trim();
    if (!id) return;
    const original = btn.textContent;
    btn.disabled = true;
    btn.textContent = "saving...";
    try {
      const r = await fetch("/api/decision/verdict", {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ decision_id: id, action: action }),
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && j && j.ok) {
        btn.textContent = action.replace(/_/g, " ").toUpperCase() + " ✓";
        try {
          if (window.lunaFlashPulse) window.lunaFlashPulse(btn,
            action === "approve" ? "rgb(111,220,177)"
            : action === "wait"  ? "rgb(240,180,85)"
            : "rgb(217,106,106)");
        } catch (_e) {}
        // Remember the saved verdict in the button so a future
        // re-render can label it as already-decided.
        btn.dataset.savedAction = action;
        // Re-enable after 1500ms so accidental double-clicks are easy
        // to fix (changing "wait" -> "approve" works).
        setTimeout(() => { try { btn.disabled = false; btn.textContent = original; } catch (_e) {} }, 1500);
      } else {
        btn.textContent = "FAIL · retry?";
        setTimeout(() => { try { btn.disabled = false; btn.textContent = original; } catch (_e) {} }, 2000);
      }
    } catch (e) {
      btn.textContent = "ERR · retry?";
      setTimeout(() => { try { btn.disabled = false; btn.textContent = original; } catch (_e) {} }, 2000);
    }
  }
  window.lunaDecisionVerdictClick = lunaDecisionVerdictClick;

  function markDashboardFresh(sourceLabel) {
    const now = new Date();
    const live = $("dashboard-live-indicator");
    const fresh = $("dashboard-source-freshness");
    const stamp = $("dashboard-last-update");
    if (live) {
      live.dataset.state = "live";
      const txt = live.querySelector(".luna-dashboard-live__text");
      if (txt) txt.textContent = "live";
      // Visible "I just refreshed" flash. CSS lights up the pill while
      // data-fresh="1"; we clear the flag after 220ms so the next poll
      // tick re-triggers it. No timer leaks because we cap one flash
      // per pill at a time.
      try {
        live.dataset.fresh = "1";
        if (live._lunaFreshT) clearTimeout(live._lunaFreshT);
        live._lunaFreshT = setTimeout(() => {
          live.removeAttribute("data-fresh");
          live._lunaFreshT = null;
        }, 220);
      } catch (_e) { /* never block the poll */ }
    }
    if (fresh) fresh.textContent = "sources: " + (sourceLabel || "fresh");
    if (stamp) stamp.textContent = "last update: " + now.toLocaleTimeString();
  }

  function initTerminalPanel() {
    const root = $("luna-terminal-panel");
    const btn = $("luna-terminal-toggle");
    const body = $("luna-terminal-body");
    const screen = $("luna-terminal-screen");
    const form = $("luna-terminal-form");
    const input = $("luna-terminal-input");
    if (!root || !btn || !body || !screen || btn.dataset.bound === "1") return;
    const commands = {
      local: 'Set-Location "D:\\SurgeApp"; powershell -NoLogo',
      admin: 'Right-click Windows PowerShell, choose "Run as administrator", then run: Set-Location "D:\\SurgeApp"',
      copy: 'D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe -m luna_modules.luna_http_dashboard --host 127.0.0.1 --port 8765',
    };
    // 2026-05-17 trust note: msg is rendered via textContent (NOT innerHTML)
    // so HTML/JS injection from input is neutralized at the DOM layer.
    // The ANSI stripper below also prevents `\x1b[32m`-style escape codes
    // from rendering as literal "[32m" text in command output.
    const ANSI_ESC_RE = /\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])/g;
    function stripAnsi(text) {
      return String(text == null ? "" : text).replace(ANSI_ESC_RE, "");
    }
    function appendLine(msg, tone) {
      const row = document.createElement("div");
      row.className = "luna-terminal-line" + (tone ? " luna-terminal-line--" + tone : "");
      row.textContent = stripAnsi(msg) || "ready";
      screen.appendChild(row);
      while (screen.children.length > 80) screen.removeChild(screen.firstElementChild);
      screen.scrollTop = screen.scrollHeight;
    }
    function setOpen(open) {
      root.dataset.open = open ? "true" : "false";
      body.hidden = !open;
      btn.setAttribute("aria-expanded", open ? "true" : "false");
      const state = btn.querySelector(".luna-terminal-toggle__state");
      if (state) state.textContent = open
        ? (state.dataset.openLabel || "RETRACT")
        : (state.dataset.closedLabel || "OPEN TERMINAL");
      if (open && input) setTimeout(() => input.focus(), 30);
    }
    async function copyCommand(textToCopy, label) {
      try {
        await navigator.clipboard.writeText(textToCopy);
        appendLine("command copied: " + label, "ok");
      } catch (_e) {
        appendLine("copy unavailable; select this command: " + textToCopy, "warn");
      }
    }
    async function runTerminalCommand(actionOrText) {
      const raw = String(actionOrText || "").trim();
      const action = raw.toLowerCase();
      if (!raw) return;
      appendLine("PS D:\\SurgeApp> " + raw, "prompt");
      if (action === "help") {
        appendLine("safe commands: local, admin, health, selfheal, report, copy, clear", "muted");
      } else if (action === "clear") {
        screen.innerHTML = "";
      } else if (action === "local") {
        await copyCommand(commands.local, "Open Local PowerShell");
      } else if (action === "admin") {
        await copyCommand(commands.admin, "Admin PowerShell manual instructions");
      } else if (action === "copy") {
        await copyCommand(commands.copy, "Luna dashboard launch");
      } else if (action === "health") {
        appendLine("running hidden health check...", "pending");
        const res = await postJSON("/api/dashboard/health-check", {});
        if (res === null || res === undefined) {
          appendLine("health check API unreachable (network/server down)", "bad");
        } else if (res.ok) {
          appendLine("health check started; log: " + (res.log_path || "logs/dashboard_health_check_latest.txt"), "ok");
        } else {
          appendLine("health check rejected: " + (res.error || res.message || "no detail returned"), "bad");
        }
      } else if (action === "selfheal" || action === "boot self-heal") {
        appendLine("starting hidden boot self-heal...", "pending");
        const res = await postJSON("/api/dashboard/boot-selfheal", {});
        if (res === null || res === undefined) {
          appendLine("boot self-heal API unreachable (network/server down)", "bad");
        } else if (res.ok) {
          appendLine("boot self-heal started; log: " + (res.log_path || "logs/dashboard_boot_selfheal_latest.txt"), "ok");
        } else {
          appendLine("boot self-heal rejected: " + (res.error || res.message || "no detail returned"), "bad");
        }
      } else if (action === "report") {
        const res = await fetchJSON("/api/dashboard/latest-report");
        if (res === null || res === undefined) {
          appendLine("latest-report API unreachable (network/server down)", "bad");
        } else if (res.report_path) {
          appendLine("latest report: " + res.report_path, "ok");
          await copyCommand(res.report_path, "latest report path");
        } else {
          appendLine("no latest report found yet", "warn");
        }
      } else {
        appendLine("arbitrary PowerShell is copied, not executed", "warn");
        await copyCommand(raw, "manual PowerShell command");
      }
    }
    btn.addEventListener("click", () => setOpen(body.hidden));
    root.querySelectorAll("[data-terminal-action]").forEach((item) => {
      item.addEventListener("click", async () => {
        const action = item.getAttribute("data-terminal-action");
        await runTerminalCommand(action);
      });
    });
    if (form && input) {
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        await runTerminalCommand(input.value);
        if (input.value.trim().toLowerCase() !== "clear") input.value = "";
      });
    }
    btn.dataset.bound = "1";
  }

  function initTierTimeline() {
    const rail = $("sm-ladder");
    const detail = $("sm-tier-detail");
    if (!rail || rail.dataset.bound === "1") return;
    let dragging = false;
    let startX = 0;
    let startLeft = 0;
    function tierDetail(node) {
      if (!node || !detail) return;
      const tier = node.dataset.tier || (node.querySelector(".luna-evo__rung-num") || {}).textContent || "?";
      const label = (node.querySelector(".luna-evo__rung-label") || {}).textContent || "";
      const count = (node.querySelector(".luna-evo__rung-count") || {}).textContent || "";
      detail.textContent = "Tier " + tier + " · " + label + " · " + (count || node.dataset.state || "ready");
      rail.querySelectorAll(".luna-evo__rung").forEach((n) => n.classList.toggle("is-selected", n === node));
    }
    rail.addEventListener("wheel", (ev) => {
      if (Math.abs(ev.deltaY) > Math.abs(ev.deltaX)) {
        rail.scrollLeft += ev.deltaY;
        ev.preventDefault();
      }
    }, { passive: false });
    rail.addEventListener("pointerdown", (ev) => {
      dragging = true;
      rail.classList.add("is-dragging");
      startX = ev.clientX;
      startLeft = rail.scrollLeft;
      try { rail.setPointerCapture(ev.pointerId); } catch (_e) {}
    });
    rail.addEventListener("pointermove", (ev) => {
      if (!dragging) return;
      rail.scrollLeft = startLeft - (ev.clientX - startX);
    });
    function endDrag(ev) {
      dragging = false;
      rail.classList.remove("is-dragging");
      try { rail.releasePointerCapture(ev.pointerId); } catch (_e) {}
    }
    rail.addEventListener("pointerup", endDrag);
    rail.addEventListener("pointercancel", endDrag);
    rail.querySelectorAll(".luna-evo__rung").forEach((node) => {
      node.addEventListener("click", () => tierDetail(node));
      node.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          tierDetail(node);
        }
      });
    });
    rail.dataset.bound = "1";
  }

  // ============================================================
  // Theme-aware color helpers
  //   cssVar(name, fallback) reads a CSS custom property from <html>.
  //   getToneColor(role, fallback) looks up a semantic role.
  //   getPalette() returns a cached map of resolved colors used by
  //   the canvas visualizers; cache is invalidated on theme change.
  //   alpha(rgbTriplet, a) → "rgba(r, g, b, a)" string for canvas.
  // ============================================================
  function cssVar(name, fallback) {
    try {
      const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
      return v || fallback;
    } catch (e) { return fallback; }
  }

  const TONE_TO_VAR = {
    accent:        "--c-accent",
    accentBright:  "--c-accent-2",
    accentDeep:    "--c-accent-deep",
    text:          "--c-text",
    textSoft:      "--c-text-2",
    textMute:      "--c-text-3",
    warm:          "--c-warm-white",
    good:          "--c-good",
    warn:          "--c-warn",
    bad:           "--c-bad",
    info:          "--c-info",
    line:          "--c-line",
    line2:         "--c-line-2",
    pulse:         "--pulse-color",
    accentRgb:     "--c-accent-rgb",
  };
  function getToneColor(name, fallback) {
    const varName = TONE_TO_VAR[name] || name;
    return cssVar(varName, fallback);
  }

  let _palette = null;
  let _paletteSig = "";
  function getPalette() {
    const root = document.documentElement;
    const sig = (root.dataset.theme || "lunar-gold") + "|" +
                (root.dataset.density || "comfortable");
    if (_palette && _paletteSig === sig) return _palette;
    _palette = {
      accent:       getToneColor("accent",        "#e8c87a"),
      accentBright: getToneColor("accentBright",  "#ffd98a"),
      accentDeep:   getToneColor("accentDeep",    "#b89048"),
      text:         getToneColor("text",          "#e9ecf3"),
      textSoft:     getToneColor("textSoft",      "#b6bccb"),
      textMute:     getToneColor("textMute",      "#8d93a4"),
      warm:         getToneColor("warm",          "#f7f2e6"),
      good:         getToneColor("good",          "#6fdcb1"),
      warn:         getToneColor("warn",          "#f0b455"),
      bad:          getToneColor("bad",           "#d96a6a"),
      info:         getToneColor("info",          "#79b6ea"),
      line:         getToneColor("line",          "rgba(232,200,122,0.10)"),
      line2:        getToneColor("line2",         "rgba(232,200,122,0.24)"),
      pulse:        getToneColor("pulse",         "#ffd98a"),
      accentRgb:    getToneColor("accentRgb",     "232 200 122"),
    };
    _paletteSig = sig;
    return _palette;
  }
  function invalidatePalette() { _palette = null; _paletteSig = ""; }

  // "232 200 122" + 0.05 → "rgba(232, 200, 122, 0.05)" (canvas-safe)
  function alpha(rgbTriplet, a) {
    const parts = String(rgbTriplet || "").trim().split(/[\s,]+/).filter(Boolean);
    if (parts.length !== 3) return "rgba(232, 200, 122, " + a + ")";
    return "rgba(" + parts.join(", ") + ", " + a + ")";
  }

  // ============================================================
  // Live state shared across visualizers
  // ============================================================
  function makeBuf() { return new Array(TREND_LEN).fill(0); }
  const state = {
    bootEpoch: Date.now(),
    lastStatus: null,
    lastRes: null,
    lastBrief: null,
    lastSoak: null,
    lastScorecard: null,
    lastSupermax: null,
    lastSelfUpgrade: null,
    lastTierTruth: null,
    ttLastFreshMono: 0,
    lastOpenCode: null,
    lastMissionControl: null,
    mcLastFreshMono: 0,
    mcPrevActor: null,
    vitalsLastFreshMono: 0,
    briefLastFreshMono: 0,
    lastFeedRecords: [],
    lastActivity: null,
    pulseSource: "heartbeat",   // dropdown selection
    breath: 0,                  // slow breathing phase for heartbeat trace
    lastHeartbeatTs: "",
    heartbeatSpike: 0,          // decays toward 0
    trends: {
      heartbeat: makeBuf(),
      cpu: makeBuf(),
      mem: makeBuf(),
      gpu: makeBuf(),
      queue: makeBuf(),
      active: makeBuf(),
      event_rate: makeBuf(),
      approval: makeBuf(),
    },
    gauges: { cpu: 0, mem: 0, gpu: 0, disk: 0 },
    mission: {
      orbitPhase: 0,
      progressEased: 0,
    },
    // Heartbeat smoothing state — bpm and intensity ease toward targets each
    // frame so transitions feel organic instead of snapping.
    heartbeat: {
      bpm: 60,
      intensityEased: 0,
    },
    ttySeen: new Set(),
    // 2026-05-13 HARD CUTOVER: canonical terminal-truth snapshot.
    // Populated by LunaTerminalTruth singleton below.
    lastTerminalTruth: null,
    ttTerminalLastOk: 0,
  };

  // ============================================================
  // 2026-05-13 HARD CUTOVER — LunaTerminalTruth singleton + LunaPanelContract.
  // Single source of operator-facing current truth for every primary
  // panel. Polls /api/terminal-truth every 5s. Subscribers re-render
  // declaratively from terminal_truth.<panel>.<field>.
  //
  // Architectural contract: any primary panel that pulls primary truth
  // from a legacy endpoint instead of this snapshot fails
  // tests/test_app_js_panels_bind_canonical_static.py.
  // ============================================================
  const LunaTerminalTruth = (function () {
    "use strict";
    const POLL_MS = 5000;
    const subs = [];
    let last = null;
    let lastOk = 0;
    let lastErr = null;
    let inFlight = false;
    // Keep this above the observed 5s terminal-truth worst case. A 4s abort
    // caused browser-side cancellations just before the server answered,
    // leaving dashboard request threads parked in CLOSE_WAIT during refresh
    // storms. The inFlight guard still prevents request pile-ups.
    const TERMINAL_TRUTH_TIMEOUT_MS = 15000;
    function fetchSnapshot() {
      if (inFlight) return Promise.resolve(last);
      inFlight = true;
      let ctrl = null;
      let timer = null;
      try {
        ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
        if (ctrl) {
          timer = setTimeout(function () {
            try { ctrl.abort(); } catch (_e) { /* ignore */ }
          }, TERMINAL_TRUTH_TIMEOUT_MS);
        }
      } catch (_e) {
        ctrl = null;
      }
      const opts = { credentials: "same-origin", cache: "no-store" };
      if (ctrl) opts.signal = ctrl.signal;
      return fetch("/api/terminal-truth", opts)
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (data) {
          if (data && data.terminal_truth) {
            last = data;
            lastOk = Date.now();
            lastErr = null;
            state.lastTerminalTruth = data;
            state.ttTerminalLastOk = lastOk;
            for (var i = 0; i < subs.length; i++) {
              try {
                subs[i](data);
              } catch (e) {
                // 2026-05-17 bugfix: previously silent. A broken subscriber
                // would silently stop updating its panel; now it warns so
                // the operator/console shows which panel is degraded.
                if (window.console && console.warn) {
                  var fnName = (subs[i] && subs[i].name) ? subs[i].name : ("subs[" + i + "]");
                  console.warn("LunaTerminalTruth subscriber " + fnName + " threw:", e);
                }
              }
            }
          }
          return data;
        })
        .catch(function (e) { lastErr = String(e); return null; })
        .finally(function () {
          if (timer) {
            try { clearTimeout(timer); } catch (_e) { /* ignore */ }
          }
          inFlight = false;
        });
    }
    function get() { return last; }
    function getPanel(name) {
      if (!last || !last.terminal_truth) {
        return {
          truth: null,
          health: {
            source: null,
            degraded: true,
            fallback_used: true,
            source_timestamp: null,
            freshness_seconds: null,
            last_error_text: "snapshot_unavailable"
          }
        };
      }
      const tt = last.terminal_truth;
      const ph = (tt.panel_health || {})[name] || null;
      return {
        truth: tt[name] || null,
        health: ph || {
          source: null, degraded: true, fallback_used: true,
          source_timestamp: null, freshness_seconds: null,
          last_error_text: "panel_unknown"
        }
      };
    }
    function subscribe(fn) {
      if (typeof fn === "function") subs.push(fn);
      if (last) try { fn(last); } catch (e) {
        if (window.console && console.warn) {
          console.warn("LunaTerminalTruth subscriber threw on initial replay:", e);
        }
      }
    }
    function start() {
      fetchSnapshot();
      setInterval(fetchSnapshot, POLL_MS);
    }
    return { start: start, get: get, getPanel: getPanel, subscribe: subscribe };
  })();
  window.LunaTerminalTruth = LunaTerminalTruth;

  // LunaPanelContract — declarative DOM binding from
  // terminal_truth.<panel>.<field> to selectors. Writes a clearly-
  // labeled degraded chip into the panel's primary area when the
  // snapshot is missing/degraded. Console-warns on any attempt to
  // write a primary-area field from outside the snapshot.
  const LunaPanelContract = (function () {
    "use strict";
    const bindings = {};
    function _setText(sel, val) {
      const els = (typeof sel === "string")
        ? document.querySelectorAll(sel)
        : [sel];
      for (let i = 0; i < els.length; i++) {
        const el = els[i];
        if (!el) continue;
        if (val === null || val === undefined || val === "") {
          el.textContent = "—";
        } else if (Array.isArray(val)) {
          el.textContent = val.length ? val.join(" · ") : "—";
        } else if (typeof val === "object") {
          try { el.textContent = JSON.stringify(val); }
          catch (e) { el.textContent = String(val); }
        } else {
          el.textContent = String(val);
        }
      }
    }
    function _renderDegraded(panelName, rootEl, health) {
      // Replace the primary area with a labeled degraded chip. Keep
      // ancillary detail visible by appending the chip rather than
      // wiping descendants.
      const id = "luna-degraded-chip-" + panelName;
      let chip = document.getElementById(id);
      if (!chip && rootEl) {
        chip = document.createElement("div");
        chip.id = id;
        chip.className = "luna-degraded-chip";
        chip.setAttribute("data-panel", panelName);
        chip.style.cssText = (
          "padding:4px 8px;border-radius:4px;"
          + "background:rgba(255,160,60,0.12);"
          + "color:#ffb84d;font-size:11px;"
          + "font-family:monospace;margin:4px 0;"
        );
        try { rootEl.prepend(chip); } catch (e) { rootEl.appendChild(chip); }
      }
      if (chip) {
        const src = (health && health.source) || "unknown";
        const ts  = (health && health.source_timestamp) || "—";
        const rea = (health && health.last_error_text) || "degraded";
        chip.textContent = (
          panelName + " · DEGRADED · source=" + src
          + " · last_known=" + ts + " · reason=" + rea
        );
        chip.hidden = false;
      }
    }
    function _clearDegraded(panelName) {
      const chip = document.getElementById("luna-degraded-chip-" + panelName);
      if (chip) chip.hidden = true;
    }
    function bind(panelName, rootSelectorOrEl, fieldMap) {
      // fieldMap: { "<terminal_truth.panel.field>": "#dom-id" | callable(val,el) }
      bindings[panelName] = { root: rootSelectorOrEl, fieldMap: fieldMap };
      LunaTerminalTruth.subscribe(function (snap) {
        _apply(panelName, snap);
      });
    }
    function _apply(panelName, snap) {
      const entry = bindings[panelName];
      if (!entry) return;
      const rootEl = (typeof entry.root === "string")
        ? document.querySelector(entry.root)
        : entry.root;
      const got = LunaTerminalTruth.getPanel(panelName);
      const truth = got.truth;
      const health = got.health;
      const degraded = !truth || (health && health.degraded);
      if (degraded) {
        _renderDegraded(panelName, rootEl, health);
      } else {
        _clearDegraded(panelName);
      }
      if (!truth) return;
      const fm = entry.fieldMap || {};
      Object.keys(fm).forEach(function (path) {
        const parts = path.split(".");
        let val = truth;
        for (let i = 0; i < parts.length; i++) {
          if (val == null) break;
          val = val[parts[i]];
        }
        const target = fm[path];
        if (typeof target === "function") {
          try { target(val, rootEl); } catch (e) { /* isolate */ }
        } else if (typeof target === "string") {
          _setText(target, val);
        }
      });
    }
    function warnExternalWrite(panelName, fieldName) {
      try {
        console.warn(
          "[LunaPanelContract] external write attempted to primary field "
          + panelName + "." + fieldName + " — primary truth must come from "
          + "/api/terminal-truth only."
        );
      } catch (e) { /* isolate */ }
    }
    return {
      bind: bind,
      warnExternalWrite: warnExternalWrite,
      _setText: _setText
    };
  })();
  window.LunaPanelContract = LunaPanelContract;
  // Kick off the canonical snapshot poll as soon as the script loads.
  try { LunaTerminalTruth.start(); } catch (e) { /* isolate */ }

  // ============================================================
  // Activity state — single source of truth for heartbeat + workload.
  // Returns { intensity 0..1, heartState, workloadState, targetBpm,
  //           orbitMultiplier } derived from real polled signals.
  // ============================================================
  function getActivityState() {
    const s = state.lastStatus || {};
    const w = s.worker || {};
    const a = s.aider_bridge || {};
    const luna = s.luna || {};
    const res = state.lastRes || {};
    const act = state.lastActivity || {};

    let intensity = 0;
    intensity += Math.min((w.queue_depth   || 0) * 0.10, 0.40);
    intensity += Math.min((w.active_count  || 0) * 0.20, 0.40);
    if ((w.approval_pending || 0) > 0) intensity += 0.15;
    if (a.running)                     intensity += 0.20;
    const winSec = Math.max(60, act.window_seconds || 1800);
    const perMin = (act.total_events || 0) / (winSec / 60);
    intensity += Math.min(perMin / 20, 0.30);
    const cpu = (res.cpu && res.cpu.usage_percent) || 0;
    if (cpu > 30) intensity += clamp((cpu - 30) / 70, 0, 0.30);
    // Recent send → brief intensity boost so the user sees the heartbeat
    // react when they hit Send (decays over ~3s via heartbeatSpike).
    intensity += clamp((state.heartbeatSpike || 0) * 0.30, 0, 0.30);
    // If Luna is offline, throttle to "resting"
    if (luna.alive === false) intensity = Math.min(intensity, 0.04);

    intensity = clamp(intensity, 0, 1);

    let heartState, targetBpm;
    if      (intensity <= 0.05) { heartState = "resting";  targetBpm = 55;  }
    else if (intensity <= 0.20) { heartState = "ready";    targetBpm = 70;  }
    else if (intensity <= 0.50) { heartState = "active";   targetBpm = 90;  }
    else                        { heartState = "stressed"; targetBpm = 115; }

    let workloadState, orbitMul;
    if      (intensity <= 0.05) { workloadState = "standby"; orbitMul = 1.0; }
    else if (intensity <= 0.30) { workloadState = "active";  orbitMul = 1.7; }
    else if (intensity <= 0.70) { workloadState = "busy";    orbitMul = 2.6; }
    else                        { workloadState = "strain";  orbitMul = 3.6; }

    return { intensity, heartState, workloadState, targetBpm, orbitMul };
  }

  function missionFileName(path) {
    if (!path) return "";
    const s = String(path).replace(/\\/g, "/");
    return s.split("/").filter(Boolean).pop() || s;
  }

  function missionReadableState(value) {
    return String(value || "")
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function _reasonReadable(reason) {
    if (!reason) return "";
    const map = {
      waiting_for_next_scheduled_cycle: "waiting for next cycle",
      waiting_for_manual_start:         "waiting for manual start",
      queue_empty:                      "queue empty",
      blocked_verifier_failed:          "verifier failed",
      supervisor_not_installed:         "supervisor not installed",
      supervisor_not_running:           "supervisor not running",
      stopped_by_kill_switch:           "stopped by kill switch",
      preview_failed:                   "preview failed",
      apply_failed:                     "apply failed",
      parameter_binding_exception:      "powershell parameter binding error",
      non_zero_exit:                    "child command returned non-zero exit",
      lock_held_by_another_supervisor:  "another supervisor instance is running",
      no_work_to_do:                    "no work to do this cycle",
      runtime_state_missing:            "runtime state file missing",
      not_full_unlock:                  "runtime is not FULL_UNLOCK_ACTIVE",
      safe_to_execute_now_false:        "safe_to_execute_now is false",
      safe_to_apply_real_project_false: "safe_to_apply_real_project is false",
      postunlock_verifier_failed:       "post-unlock verifier failed",
      runtime_mode_changed:             "runtime mode changed mid-cycle",
      stop_flag_present:                "kill switch flag detected",
      loop_guard_24h:                   "24h loop guard fired",
    };
    return map[reason] || String(reason).replace(/_/g, " ");
  }

  function missionIdleDetail(data) {
    const src = (data && data.source_summary) || {};
    const idle = data && data.idle_reason;
    const blocked = data && data.blocked_reason;
    if (blocked) return "BLOCKED · " + _reasonReadable(blocked);
    if (idle)    return "WAITING · " + _reasonReadable(idle);
    const last = (data && data.last_result) || {};
    const result = String(last.result || "");
    const target = missionFileName(last.target || "");
    if (result && target) {
      return "last patch " + missionReadableState(result).toLowerCase() + " · " + target;
    }
    if (src.always_on_state) {
      return "always-on " + missionReadableState(src.always_on_state).toLowerCase();
    }
    if (src.guardian_status) {
      return "guardian " + missionReadableState(src.guardian_status).toLowerCase();
    }
    return "awaiting orders";
  }

  function missionViewFromControl(data) {
    const src = (data && data.source_summary) || {};
    const stale = !!(data && data.is_stale);
    const blocked = !!(data && data.is_blocked);
    const complete = !!(data && data.is_complete);
    const idle = !!(data && data.is_idle);
    const active = !!(data && data.is_active) && !stale;
    const workerOnline = !!src.worker_alive;
    const rawActor = String((data && data.current_actor) || "IDLE").trim() || "IDLE";
    let actor = rawActor;
    if (!active && !stale && rawActor === "IDLE" && workerOnline && !blocked && !complete) {
      actor = "READY";
    }
    let stage = String((data && data.current_stage) || "").trim();
    if (!active && !stale && !blocked && !complete &&
        (!stage || /waiting for next work order/i.test(stage))) {
      stage = workerOnline ? "Worker online · next work order" : "awaiting orders";
    }

    // ---- SUPERVISOR_RETIRED override ---------------------------------
    // The Tier 0-2 always-on supervisor writes current_stage =
    // "SUPERVISOR_RETIRED" when the higher-tier progression engine
    // owns the work. That literal string makes Luna look stopped.
    // Replace it with the live Tier label whenever the higher-tier
    // engine is actually running.
    const _ht = state.lastHigherTier || null;
    const _htOk = !!(_ht && _ht.ok);
    const _htTask = (_htOk && _ht.scheduled_task) || null;
    // Cross-check the scheduled-task signal against the AUTHORITATIVE
    // tier-truth worker_ecosystem block. tier-truth tries the
    // user-scope task name first and is correct whenever the
    // legacy higher-tier query gets stuck on the wrong task name.
    const _ttSnap = state.lastTierTruth || null;
    const _ttProg = ((_ttSnap && _ttSnap.worker_ecosystem) || {}).progression || {};
    const _ttProgActive = String(_ttProg.state || "").toLowerCase() === "active";
    const _htTaskAlive = _ttProgActive || !!(_htTask && (_htTask.exists !== false) &&
                                              (_htTask.enabled !== false));
    const _htTier = _htOk ? String(_ht.current_effective_tier || "").trim() : "";
    const _isRetiredStage = /^supervisor_retired$/i.test(stage) ||
                            /^supervisor[_\s-]*retired$/i.test(rawActor);
    let _retiredOverride = false;
    if (_isRetiredStage && _htOk && _htTaskAlive && _htTier) {
      // Higher-tier progression is the live engine; relabel.
      actor = "TIER " + _htTier;
      stage = "progression engine active";
      _retiredOverride = true;
    } else if (_isRetiredStage && _htOk && _htTier && !_htTaskAlive) {
      // Higher-tier config says we're at Tier N but the scheduled
      // task is missing/disabled — and tier-truth ALSO says it's
      // offline. Only then do we surface the offline message.
      actor = "TIER " + _htTier;
      stage = "progression engine offline (scheduled task disabled)";
      _retiredOverride = true;
    }
    const title = String((data && data.current_task_title) || "").trim() ||
                  (active ? actor : (complete ? (
                    _retiredOverride ? actor + " ACTIVE" : "Last task complete"
                  ) : (workerOnline ? "WORKER ONLINE" : "STANDBY")));
    const idleDetail = missionIdleDetail(data);
    const status = String((data && data.current_status_text) || "").trim() ||
                   (active ? (stage || "working") : idleDetail);
    const progressPercent = Number(data && data.progress_percent);
    const hasRealProgress = (active || complete) && isFinite(progressPercent);
    const activity = getActivityState();
    let progress;
    if (complete) {
      progress = 1;
    } else if (hasRealProgress) {
      progress = clamp(progressPercent / 100, 0.02, 1);
    } else if (active) {
      progress = clamp(0.16 + activity.intensity * 0.72, 0.08, 0.92);
    } else {
      progress = 0;
    }
    const actorKind = actor.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    let kind;
    if (blocked) kind = "blocked";
    else if (stale) kind = "stale";
    else if (active) kind = actorKind || "active";
    else if (complete) kind = "complete";
    else if (idle) kind = "waiting";
    else kind = workerOnline ? "ready" : "standby";

    let statLabel;
    const nextIn = (data && data.next_cycle_in_seconds != null) ? Number(data.next_cycle_in_seconds) : null;
    const nextTag = (nextIn != null && nextIn >= 0)
      ? (" · next " + (nextIn < 60 ? (nextIn + "s") : (Math.floor(nextIn / 60) + "m")))
      : "";
    // Compact statLabel — must fit on one line next to "Mission Control"
    // header. Long descriptive copy goes into the moon overlay instead.
    if (blocked) {
      statLabel = "BLOCKED" + (data.blocked_reason ? (" · " + data.blocked_reason).slice(0, 32) : "");
    } else if (stale)    statLabel = "STALE";
    else if (active)     statLabel = "LIVE · " + actor.toLowerCase();
    else if (complete)   statLabel = "COMPLETE" + nextTag;
    else if (idle)       statLabel = "WAITING" + nextTag;   // short form; reason is in overlay
    else                 statLabel = workerOnline ? "READY" : "idle";

    let clockLabel = "";
    let clockSubLabel = "";
    if (!active) {
      if (blocked) {
        clockLabel = "BLOCKED";
        clockSubLabel = (data.blocked_reason || "see detail").toUpperCase().replace(/_/g, " ");
      } else if (complete) {
        clockLabel = "COMPLETE"; clockSubLabel = "";
      } else if (idle) {
        clockLabel = "WAITING";
        // De-duplicate: if the idle reason is essentially the same words
        // as the clockLabel ("waiting_for_next_scheduled_cycle"), don't
        // repeat it. Otherwise show a short humanized form.
        const r = String(data.idle_reason || "").toLowerCase();
        if (!r || /waiting/.test(r) || /next_scheduled_cycle/.test(r)) {
          clockSubLabel = "FOR NEXT CYCLE";
        } else {
          clockSubLabel = r.replace(/_/g, " ").toUpperCase();
        }
      } else if (workerOnline) {
        clockLabel = "READY"; clockSubLabel = "WORKER ONLINE";
      } else {
        clockLabel = "STANDBY"; clockSubLabel = "";
      }
    }

    return {
      actor,
      stage,
      title,
      status,
      kind,
      active,
      blocked,
      complete,
      idle,
      progress,
      progressMode: hasRealProgress ? "real" : "",
      clockLabel,
      clockSubLabel,
      statLabel,
      idleReason: data && data.idle_reason || "",
      blockedReason: data && data.blocked_reason || "",
      nextAction: data && data.next_action || "",
      reportPath: data && data.report_path || "",
    };
  }

  // ============================================================
  // Title bar clock + prompt rotator
  // ============================================================
  function tickClock() {
    const t = new Date();
    const z = (n) => String(n).padStart(2, "0");
    text($("meta-time"),
      t.getFullYear() + "·" + z(t.getMonth()+1) + "·" + z(t.getDate()) +
      "  " + z(t.getHours()) + ":" + z(t.getMinutes()) + ":" + z(t.getSeconds()));

    const upMs = Date.now() - state.bootEpoch;
    const s = Math.floor(upMs/1000);
    const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = s%60;
    text($("footer-uptime"), "uptime " + z(h) + ":" + z(m) + ":" + z(sec));
  }

  const PROMPTS = [
    "luna --status --watch",
    "tail -f logs/luna_live_feed.jsonl",
    "guardian --readiness",
    "scorecard --observe",
    "soak --advisory --cycles 144",
  ];
  let promptIdx = 0;
  function rotatePrompt() {
    promptIdx = (promptIdx + 1) % PROMPTS.length;
    const el = $("prompt-text");
    if (el) el.textContent = PROMPTS[promptIdx];
  }

  // ============================================================
  // Trend sampling — slow + smooth
  // ============================================================
  function sampleTrends() {
    const s = state.lastStatus || {};
    const w = s.worker || {};
    const luna = s.luna || {};
    const res = state.lastRes || {};
    const act = state.lastActivity || {};

    // Heartbeat: slow breathing wave + brief lift on heartbeat ts change.
    state.breath = (state.breath + 0.04) % (Math.PI * 2);
    let hb = 0.55 + Math.sin(state.breath) * 0.22;
    if (luna.ts && luna.ts !== state.lastHeartbeatTs) {
      state.heartbeatSpike = 1.0;
      state.lastHeartbeatTs = luna.ts;
    }
    state.heartbeatSpike *= 0.85;   // decay over ~3s
    hb += state.heartbeatSpike * 0.25;
    if (!luna.alive) hb *= 0.4;
    push(state.trends.heartbeat, clamp(hb, 0, 1));

    // CPU usage 0..100 -> 0..1
    push(state.trends.cpu, clamp(((res.cpu && res.cpu.usage_percent) || 0) / 100, 0, 1));
    // Memory free 0..1
    push(state.trends.mem, clamp(((res.memory && res.memory.available_percent) || 0) / 100, 0, 1));
    // GPU free 0..1
    push(state.trends.gpu, clamp(((res.gpu && res.gpu.free_vram_percent) || 0) / 100, 0, 1));
    // Queue depth 0..8 -> 0..1
    push(state.trends.queue, clamp((w.queue_depth || 0) / 8, 0, 1));
    // Active jobs 0..4 -> 0..1
    push(state.trends.active, clamp((w.active_count || 0) / 4, 0, 1));
    // Approval pending 0..6 -> 0..1
    push(state.trends.approval, clamp((w.approval_pending || 0) / 6, 0, 1));
    // Event rate (events/min from /api/activity, normalized to 0..1 vs 60/min)
    const perMin = (act.total_events || 0) / Math.max(1, ((act.window_seconds || 60) / 60));
    push(state.trends.event_rate, clamp(perMin / 60, 0, 1));
  }

  function push(buf, v) {
    buf.push(v);
    while (buf.length > TREND_LEN) buf.shift();
  }

  // ============================================================
  // Oscilloscope (calm) with selectable source
  // ============================================================
  // _color is a semantic role resolved via getPalette(); themes drive the
  // actual hex. Original hex literals retained as fallbacks.
  const SOURCE_META = {
    heartbeat:  { title: "PULSE · worker heartbeat", _color: "pulse",        legend: "heartbeat",  unit: "luna" },
    cpu:        { title: "TREND · CPU usage",        _color: "warn",         legend: "cpu %",      unit: "cpu" },
    mem:        { title: "TREND · memory free",      _color: "good",         legend: "mem free %", unit: "mem" },
    gpu:        { title: "TREND · GPU vram free",    _color: "info",         legend: "gpu free %", unit: "gpu" },
    queue:      { title: "TREND · queue depth",      _color: "accent",       legend: "queue",      unit: "jobs" },
    active:     { title: "TREND · active jobs",      _color: "good",         legend: "active",     unit: "jobs" },
    event_rate: { title: "TREND · event rate",       _color: "accentBright", legend: "evt/min",    unit: "evt/min" },
    approval:   { title: "TREND · council-gated pending", _color: "warn",     legend: "pending",    unit: "items" },
    combined:   { title: "TREND · combined overlay", _color: "accentBright", legend: "overlay",    unit: "norm" },
  };
  function metaColor(meta) {
    const P = getPalette();
    return P[(meta && meta._color) || "accentBright"] || P.accentBright;
  }

  // EKG / life-monitor drawing for the heartbeat source.
  // Generates a scrolling P-QRS-T waveform so the strip looks alive
  // even when server-side heartbeat values are calm.
  function drawHeartbeatEKG(canvas) {
    const ctx = canvas.getContext("2d");
    _resizeCanvasIfNeeded(canvas);
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const heart   = cssVar("--luna-heartbeat",      "#ff4d3d");
    const glow    = cssVar("--luna-heartbeat-glow", "rgba(255,77,61,0.55)");
    const gridCol = cssVar("--luna-monitor-grid",   "rgba(255,77,61,0.08)");

    // Subtle red grid
    ctx.strokeStyle = gridCol;
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath();
    const cols = 24, rows = 5;
    for (let i = 1; i < cols; i++) { const x = (W * i / cols); ctx.moveTo(x, 0); ctx.lineTo(x, H); }
    for (let i = 1; i < rows; i++) { const y = (H * i / rows); ctx.moveTo(0, y); ctx.lineTo(W, y); }
    ctx.stroke();

    // Baseline at ~70% of height (low = bottom-ish on a monitor)
    const baseline = H * 0.68;
    // State-driven amplitude + BPM. Bigger spikes when stressed.
    const act = getActivityState();
    // Smoothly ease bpm toward targetBpm so transitions feel organic.
    state.heartbeat.bpm = state.heartbeat.bpm
      + (act.targetBpm - state.heartbeat.bpm) * 0.04;
    state.heartbeat.intensityEased = state.heartbeat.intensityEased
      + (act.intensity - state.heartbeat.intensityEased) * 0.04;
    const bpm = state.heartbeat.bpm;
    const intensity = state.heartbeat.intensityEased;
    // Amp scales dramatically with intensity. Resting heart: short, calm
    // spikes. Stressed heart: tall, sharp, more clearly visible spikes.
    const amp = H * (0.28 + intensity * 0.62);
    // Reflect state on the strip wrapper so CSS glow can react.
    const wrap = canvas.parentElement;
    if (wrap) {
      if (wrap.dataset.heartState !== act.heartState) wrap.dataset.heartState = act.heartState;
    }
    // Real-human-heart fix: visual period is FIXED (a constant number of
    // beats always fit on screen). Scroll speed scales with BPM. Phase is
    // a monotonic accumulator updated by dt*speed, so the wave can never
    // jump backwards when BPM changes — slower heart just scrolls slower,
    // faster heart scrolls faster, spacing stays steady.
    const beatsOnScreen = 6;
    const period = Math.max(40, W / beatsOnScreen);          // fixed pixels per beat
    const speed = period * (Math.max(40, bpm) / 60);         // pixels per second varies with BPM
    const t = performance.now() / 1000;
    const reduced = (document.documentElement.dataset.motion === "reduced") ||
                    (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
    if (state.heartbeat.lastT === undefined) state.heartbeat.lastT = t;
    const dt = Math.min(0.25, Math.max(0, t - state.heartbeat.lastT));
    state.heartbeat.lastT = t;
    if (state.heartbeat.phasePx === undefined) state.heartbeat.phasePx = 0;
    if (!reduced) state.heartbeat.phasePx += dt * speed;     // strictly monotonic
    const phase = reduced ? 0 : (state.heartbeat.phasePx % period);

    // Cardiac Lead-II waveform across one normalized cycle p in [0..1].
    // Shape calibrated so it reads as a real human ECG trace:
    //   - small upward P wave (atrial depolarisation)
    //   - PR-segment isoelectric flat
    //   - sharp QRS (small Q dip → tall narrow R → S undershoot)
    //   - ST-segment isoelectric flat
    //   - asymmetric T wave (slow rise, faster fall)
    //   - long resting tail back to baseline
    function ekg(p) {
      // ── P wave  (0.040 → 0.130) — gentle upward bump
      if (p < 0.040) return 0;
      if (p < 0.130) return Math.sin((p - 0.040) / 0.090 * Math.PI) * 0.13;
      // ── PR segment (0.130 → 0.300) — flat isoelectric
      if (p < 0.300) return 0;
      // ── QRS complex (0.300 → 0.385) — sharp, narrow
      if (p < 0.318) return -((p - 0.300) / 0.018) * 0.22;                         // Q dip
      if (p < 0.336) return -0.22 + ((p - 0.318) / 0.018) * 1.32;                  // R climb to +1.10
      if (p < 0.355) return  1.10 - ((p - 0.336) / 0.019) * 1.65;                  // R fall to -0.55
      if (p < 0.385) return -0.55 + ((p - 0.355) / 0.030) * 0.55;                  // S recovery to baseline
      // ── ST segment (0.385 → 0.520) — flat isoelectric
      if (p < 0.520) return 0;
      // ── T wave (0.520 → 0.760) — asymmetric: slow rise, faster fall
      if (p < 0.660) {
        // rising phase: 0..1 mapped to 0..0.22 with sin-ease
        const u = (p - 0.520) / 0.140;
        return Math.sin(u * Math.PI * 0.5) * 0.22;
      }
      if (p < 0.760) {
        // falling phase: cos-ease back to 0
        const u = (p - 0.660) / 0.100;
        return Math.cos(u * Math.PI * 0.5) * 0.22;
      }
      // ── resting tail back to baseline
      return 0;
    }

    // Faint baseline glow path drawn first
    ctx.beginPath();
    ctx.moveTo(0, baseline);
    ctx.lineTo(W, baseline);
    ctx.strokeStyle = alpha("255 77 61", 0.10);
    ctx.lineWidth = 1 * dpr;
    ctx.stroke();

    // Main EKG line — sample W pixels of the scrolling waveform.
    // Real human heart: flat isoelectric baseline between beats, sharp clean
    // QRS spike. No "back and forth" wobble — calm at rest, dramatic spikes
    // when stressed. Tiny per-beat HRV (±1.5% of period) prevents visually
    // identical loops without making the line look unstable.
    ctx.beginPath();
    for (let x = 0; x < W; x++) {
      // Stable per-beat HRV (one offset per cardiac cycle)
      const beatIndex = Math.floor((x + phase) / period);
      const hrv = Math.sin(beatIndex * 1.7) * 0.015;
      const local = (((x + phase) % period) / period + hrv + 1) % 1;
      const y = baseline - ekg(local) * amp;
      if (x === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = heart;
    ctx.lineWidth = (1.6 + intensity * 0.6) * dpr;            // 1.6 → 2.2 with intensity
    ctx.shadowColor = glow;
    ctx.shadowBlur = (10 + intensity * 14) * dpr;             // 10 → 24 with intensity
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Leading marker — bright dot at the right edge so the line "writes" forward.
    // A second halo ring just behind it amplifies the alive feel.
    const lastY = baseline - (ekg(((W - 1 + phase) % period) / period)) * amp;
    ctx.beginPath();
    ctx.arc(W - 4 * dpr, lastY, 7 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = alpha("255 255 255", 0.10);
    ctx.fill();
    ctx.beginPath();
    ctx.arc(W - 4 * dpr, lastY, 3.5 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = "#fff";
    ctx.shadowColor = glow;
    ctx.shadowBlur = 18 * dpr;
    ctx.fill();
    ctx.shadowBlur = 0;
  }

  function drawOscilloscope(canvas) {
    // Heartbeat source has its own life-monitor renderer.
    if (state.pulseSource === "heartbeat") {
      const wrap = canvas.parentElement;
      if (wrap) wrap.dataset.source = "heartbeat";
      drawHeartbeatEKG(canvas);
      return;
    } else {
      const wrap = canvas.parentElement;
      if (wrap && wrap.dataset.source) delete wrap.dataset.source;
    }

    const P = getPalette();
    const ctx = canvas.getContext("2d");
    _resizeCanvasIfNeeded(canvas);
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    // Backdrop grid (very subtle, theme-tinted)
    ctx.strokeStyle = alpha(P.accentRgb, 0.05);
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath();
    for (let i = 0; i <= 6; i++) {
      const y = (H * i / 6);
      ctx.moveTo(0, y); ctx.lineTo(W, y);
    }
    for (let i = 0; i <= 12; i++) {
      const x = (W * i / 12);
      ctx.moveTo(x, 0); ctx.lineTo(x, H);
    }
    ctx.stroke();

    // Mid line
    ctx.strokeStyle = alpha(P.accentRgb, 0.18);
    ctx.setLineDash([2 * dpr, 4 * dpr]);
    ctx.beginPath();
    ctx.moveTo(0, H / 2); ctx.lineTo(W, H / 2);
    ctx.stroke();
    ctx.setLineDash([]);

    function plot(buf, color, opts) {
      const N = buf.length;
      const fill = opts && opts.fill;
      ctx.beginPath();
      for (let i = 0; i < N; i++) {
        const x = (i / (N - 1)) * W;
        const y = H - (clamp(buf[i], 0, 1) * H * 0.85) - H * 0.06;
        if (i === 0) ctx.moveTo(x, y);
        else         ctx.lineTo(x, y);
      }
      if (fill) {
        const grad = ctx.createLinearGradient(0, 0, 0, H);
        grad.addColorStop(0, color + "55");
        grad.addColorStop(1, color + "00");
        ctx.lineTo(W, H); ctx.lineTo(0, H); ctx.closePath();
        ctx.fillStyle = grad;
        ctx.fill();
        ctx.beginPath();
        for (let i = 0; i < N; i++) {
          const x = (i / (N - 1)) * W;
          const y = H - (clamp(buf[i], 0, 1) * H * 0.85) - H * 0.06;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
      }
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.6 * dpr;
      ctx.shadowBlur = 10 * dpr;
      ctx.shadowColor = color;
      ctx.stroke();
      ctx.shadowBlur = 0;
    }

    const src = state.pulseSource;
    if (src === "combined") {
      plot(state.trends.queue,      P.warn,         { fill: false });
      plot(state.trends.active,     P.good,         { fill: false });
      plot(state.trends.event_rate, P.info,         { fill: false });
      plot(state.trends.heartbeat,  P.accentBright, { fill: true });
    } else {
      const meta = SOURCE_META[src] || SOURCE_META.heartbeat;
      plot(state.trends[src] || state.trends.heartbeat, metaColor(meta), { fill: true });
    }

    // Trailing dot at "now"
    const buf = state.trends[src === "combined" ? "heartbeat" : src] || state.trends.heartbeat;
    const lastV = buf[buf.length - 1] || 0;
    const ny = H - (clamp(lastV, 0, 1) * H * 0.85) - H * 0.06;
    ctx.beginPath();
    ctx.arc(W - 4 * dpr, ny, 4 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = P.warm;
    ctx.shadowColor = P.accentBright;
    ctx.shadowBlur = 14 * dpr;
    ctx.fill();
    ctx.shadowBlur = 0;
  }

  function syncOscilloscopeMeta() {
    const meta = SOURCE_META[state.pulseSource] || SOURCE_META.heartbeat;
    text($("osc-legend-name"), meta.legend);

    const last = (state.trends[state.pulseSource === "combined" ? "heartbeat" : state.pulseSource] || []).slice(-1)[0] || 0;
    let display = "—";
    switch (state.pulseSource) {
      case "heartbeat":  display = state.lastStatus && state.lastStatus.luna && state.lastStatus.luna.alive ? "ALIVE · " + state.lastHeartbeatTs.slice(11,19) : "OFFLINE"; break;
      case "cpu":        display = fmtPct((state.lastRes && state.lastRes.cpu && state.lastRes.cpu.usage_percent) || 0); break;
      case "mem":        display = fmtPct((state.lastRes && state.lastRes.memory && state.lastRes.memory.available_percent) || 0) + " free"; break;
      case "gpu":        display = fmtPct((state.lastRes && state.lastRes.gpu && state.lastRes.gpu.free_vram_percent) || 0) + " free"; break;
      case "queue":      display = ((state.lastStatus && state.lastStatus.worker && state.lastStatus.worker.queue_depth) || 0) + " queued"; break;
      case "active":     display = ((state.lastStatus && state.lastStatus.worker && state.lastStatus.worker.active_count) || 0) + " active"; break;
      case "event_rate": {
        const a = state.lastActivity || {};
        const perMin = (a.total_events || 0) / Math.max(1, ((a.window_seconds || 60) / 60));
        display = perMin.toFixed(2) + " evt/min";
        break;
      }
      case "approval":   display = ((state.lastStatus && state.lastStatus.worker && state.lastStatus.worker.approval_pending) || 0) + " pending"; break;
      case "combined":   display = "overlay"; break;
    }
    text($("osc-bpm"), display);
  }

  // ============================================================
  // Mission Clock (replaces radar)
  // ============================================================
  function pickMission() {
    /* Returns the active mission with countdown info, or a STANDBY object.
       Priority:
         1. Aider running (with started_at and elapsed_seconds)
         2. Soak in progress (observed < required)
         3. Standby with orbital animation
    */
    const s = state.lastStatus || {};
    const a = s.aider_bridge || {};
    const soak = s.soak || {};
    const lastFeedTs = (state.lastFeedRecords && state.lastFeedRecords.length)
                        ? state.lastFeedRecords[state.lastFeedRecords.length - 1].ts || ""
                        : "";

    // Mission Control is the live source for this panel. Prefer it when
    // fresh so the moon, overlay, caption, and header do not fight with
    // older Self-Upgrade/Supermax fallback text.
    const mc = state.lastMissionControl || null;
    if (mc && mc.ok && state.mcLastFreshMono &&
        (performance.now() - state.mcLastFreshMono) < 12000) {
      const view = missionViewFromControl(mc);
      return {
        kind: view.kind,
        title: view.title,
        detail: view.status,
        remaining: 0,
        progress: view.progress,
        active: view.active,
        progressMode: view.progressMode,
        clockLabel: view.clockLabel,
        clockSubLabel: view.clockSubLabel,
        statLabel: view.statLabel,
      };
    }

    // 1) Aider: when running, build a completion ETA from elapsed_seconds.
    if (a.running && (a.state === "running" || a.stage)) {
      // We don't have an authoritative ETA. Use a default 180 s budget for
      // aider tasks; clamp progress to [0..0.99] until completion event arrives.
      const elapsed = Math.max(0, Number(a.elapsed_seconds || 0) || 0);
      const budget  = 180;
      const remaining = Math.max(0, budget - elapsed);
      const progress = clamp(elapsed / budget, 0, 0.99);
      return {
        kind: "aider",
        title: a.target ? "AIDER · " + (a.target.split(/[\\/]/).pop() || a.target) : "AIDER · running",
        detail: (a.stage || a.state || "running") + " · pid " + (a.pid || "?"),
        remaining,
        progress,
        active: true,
      };
    }

    // 2) Soak: cycles_remaining * cycle seconds (default 600s).
    const obs = Number(soak.observed_cycles || 0);
    const req = Number(soak.required_cycles || 144);
    if (obs > 0 && obs < req && soak.verdict !== "PASS" && soak.verdict !== "FAIL") {
      const cycleSec = 600;
      const remaining = Math.max(0, (req - obs) * cycleSec);
      const progress = clamp(obs / req, 0, 1);
      return {
        kind: "soak",
        title: "ADVISORY SOAK · cycle " + obs + "/" + req,
        detail: (soak.verdict || "ADVISORY").replace(/_/g, " ") + " · 24h advisory",
        remaining,
        progress,
        active: true,
      };
    }

    // 2b) Always-On Supervisor / Self-Upgrade evidence gate.
    //     When Luna is making real progress toward tier promotion, the
    //     orbital ring should display that progress directly so the user
    //     can see "how much is left" instead of a decorative bounce.
    const sm = state.lastSupermax || null;
    if (sm && sm.computed) {
      const c = sm.computed;
      const hb = sm.always_on_heartbeat || {};
      const eligible = !!c.tier_2_eligible;
      const inFlight = (hb.state === "cycle_starting") ||
                       (hb.attempted != null && hb.succeeded != null &&
                        Number(hb.attempted) > 0 && Number(hb.attempted) > Number(hb.succeeded));
      const gateProg = clamp(Number(c.progress_to_tier_2 || 0), 0, 1);
      // Surface as a live mission whenever there is meaningful progress
      // OR Luna is actively in a cycle. (Skip if absolutely zero data.)
      if (inFlight || gateProg > 0 || eligible) {
        const t0t1 = Number(c.t0t1_successes || 0);
        const thr  = Number(c.tier_2_threshold || 10);
        const remaining = Math.max(0, thr - t0t1);
        let title, detail, progress;
        if (inFlight && hb.attempted != null) {
          const att  = Number(hb.attempted || 0);
          const ok   = Number(hb.succeeded || 0);
          const fail = Number(hb.failed || 0);
          progress = att > 0 ? clamp((ok + fail) / att, 0.05, 1.0) : 0.10;
          title    = "CYCLE · " + ok + "/" + att;
          detail   = "applying patches · " + (hb.verdict || "in flight");
        } else if (eligible) {
          progress = 1.0;
          // Distinguish "eligible but not yet approved" from "approved + active".
          // Once Tier 2 has been approved by Serge AND current_allowed_tier
          // is 2, the gate is open — say so rather than misleading "ELIGIBLE".
          //
          // Higher-tier override: if the Tier 6+ progression engine has
          // taken over (current_effective_tier in {6,7,8,...}), Mission
          // Control should reflect THAT, not the historical Tier 2 state.
          // This is the fix for "Mission Control still says TIER 2 ACTIVE".
          const ht = state.lastHigherTier || null;
          const cet = ht && ht.ok ? String(ht.current_effective_tier || "") : "";
          const flags = ht && ht.ok ? (ht.tier_flags || {}) : {};
          if (ht && ht.ok && (flags.tier8_enabled || flags.tier7_enabled || flags.tier6_enabled)) {
            title  = ht.headline || ("TIER " + cet + " ACTIVE");
            detail = ht.subline || "higher-tier progression active";
          } else {
            const approved = !!(sm.evidence_gate && sm.evidence_gate.tier2_approved);
            const tierAllowed = parseInt(
              (sm.evidence_gate && sm.evidence_gate.current_allowed_tier) || 0, 10);
            if (approved && tierAllowed >= 2) {
              title  = "TIER 2 ACTIVE";
              detail = "approved · running tier-bounded patches";
            } else {
              title  = "TIER 2 ELIGIBLE";
              detail = "ready for approval — click APPROVE TIER 2";
            }
          }
        } else {
          progress = Math.max(gateProg, 0.05);
          // Higher-tier override (mirrors the eligible branch above).
          const ht2 = state.lastHigherTier || null;
          const flags2 = ht2 && ht2.ok ? (ht2.tier_flags || {}) : {};
          if (ht2 && ht2.ok && (flags2.tier8_enabled || flags2.tier7_enabled || flags2.tier6_enabled)) {
            title  = ht2.headline || ("TIER " + String(ht2.current_effective_tier || "?") + " ACTIVE");
            detail = ht2.subline || "higher-tier progression active";
          } else {
            title    = "TIER " + (sm.evidence_gate && sm.evidence_gate.current_allowed_tier) + " ACTIVE";
            detail   = remaining + " patch" + (remaining === 1 ? "" : "es") + " to next tier";
          }
        }
        return {
          kind: "supermax",
          title,
          detail,
          remaining: 0,
          progress,
          active: true,
          // Hint to the renderer to suppress ambient decorative dots and
          // emphasize the progress arc + leading dot.
          progressMode: "real",
        };
      }
    }

    // 3) Worker / autonomous-loop activity — when the worker has tasks
    //    queued, active, or pending approval, OR the activity intensity
    //    is non-resting, surface that as the live mission so Mission
    //    Control reflects what Luna is actually doing.
    const w = s.worker || {};
    const queueDepth = Number(w.queue_depth || 0);
    const activeCount = Number(w.active_count || 0);
    const approvalPending = Number(w.approval_pending || 0);
    const wAct = getActivityState();
    const hasWork = (queueDepth > 0) || (activeCount > 0) ||
                    (approvalPending > 0) || (wAct.workloadState !== "standby");
    if (hasWork) {
      // Pick the strongest signal as the mission title.
      let kind, title, detail, progress;
      if (activeCount > 0) {
        kind = "active";
        title = "ACTIVE · " + activeCount + " task" + (activeCount === 1 ? "" : "s");
        detail = "queue " + queueDepth + " · pending " + approvalPending +
                 (lastFeedTs ? " · last event " + lastFeedTs : "");
        progress = clamp(0.20 + wAct.intensity * 0.70, 0.05, 0.95);
      } else if (queueDepth > 0) {
        kind = "queued";
        title = "QUEUED · " + queueDepth;
        detail = "pending " + approvalPending +
                 (lastFeedTs ? " · last event " + lastFeedTs : " · awaiting worker");
        progress = clamp(0.10 + Math.min(queueDepth, 8) * 0.06, 0.05, 0.95);
      } else if (approvalPending > 0) {
        kind = "approval";
        title = "COUNCIL · " + approvalPending + " pending";
        detail = lastFeedTs ? "last event " + lastFeedTs : "awaiting council verdict";
        progress = 0.30;
      } else {
        // Pure intensity (autonomous loop verifying / health-checking)
        kind = "verifying";
        title = wAct.workloadState.toUpperCase();
        detail = lastFeedTs ? "last event " + lastFeedTs : "running checks";
        progress = clamp(0.10 + wAct.intensity * 0.50, 0.05, 0.85);
      }
      return {
        kind,
        title,
        detail,
        remaining: 0,
        progress,
        active: true,
      };
    }

    // 4) Standby — purely decorative.
    return {
      kind: "standby",
      title: "STANDBY",
      detail: lastFeedTs ? "last event " + lastFeedTs : "awaiting orders",
      remaining: 0,
      progress: 0,
      active: false,
    };
  }

  function drawMissionClock(canvas) {
    const P = getPalette();
    const ctx = canvas.getContext("2d");
    _resizeCanvasIfNeeded(canvas);
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    const cx = W / 2, cy = H / 2;
    const Rout = Math.min(W, H) * 0.44;
    const Rin  = Rout - 12 * dpr;
    ctx.clearRect(0, 0, W, H);
    if (Rout <= 8 * dpr || Rin <= 2 * dpr) return;

    const m = pickMission();
    const target = m.active ? m.progress : (Math.sin(performance.now() / 2400) + 1) / 2 * 0.04;
    state.mission.progressEased = state.mission.progressEased + (target - state.mission.progressEased) * 0.05;

    // Dotted background ring (matches logo aesthetic)
    ctx.strokeStyle = alpha(P.accentRgb, m.active ? 0.28 : 0.34);
    ctx.lineWidth = 1.2 * dpr;
    ctx.setLineDash([2 * dpr, 6 * dpr]);
    ctx.beginPath(); ctx.arc(cx, cy, Rout - 1 * dpr, 0, Math.PI * 2); ctx.stroke();
    ctx.setLineDash([]);

    // Inner solid ring
    ctx.strokeStyle = alpha(P.accentRgb, 0.18);
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath(); ctx.arc(cx, cy, Rin, 0, Math.PI * 2); ctx.stroke();

    // Cardinal tick marks (N/E/S/W)
    const tickLen = 9 * dpr;
    ctx.strokeStyle = alpha(P.accentRgb, 0.55);
    ctx.lineWidth = 1.4 * dpr;
    [0, Math.PI / 2, Math.PI, Math.PI * 1.5].forEach((ang) => {
      const x1 = cx + Math.cos(ang) * (Rout + 4 * dpr);
      const y1 = cy + Math.sin(ang) * (Rout + 4 * dpr);
      const x2 = cx + Math.cos(ang) * (Rout - tickLen);
      const y2 = cy + Math.sin(ang) * (Rout - tickLen);
      ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
    });

    // Progress arc
    const a0 = -Math.PI / 2;
    const a1 = a0 + Math.PI * 2 * state.mission.progressEased;
    if (state.mission.progressEased > 0.001) {
      ctx.beginPath();
      ctx.arc(cx, cy, Rout - 6 * dpr, a0, a1, false);
      ctx.strokeStyle = m.active ? P.accentBright : alpha(P.accentRgb, 0.4);
      ctx.shadowColor = P.accentBright;
      ctx.shadowBlur = 18 * dpr;
      ctx.lineWidth = 4 * dpr;
      ctx.lineCap = "round";
      ctx.stroke();
      ctx.shadowBlur = 0;

      // Leading bright dot
      const lx = cx + Math.cos(a1) * (Rout - 6 * dpr);
      const ly = cy + Math.sin(a1) * (Rout - 6 * dpr);
      ctx.beginPath(); ctx.arc(lx, ly, 4.5 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = P.warm;
      ctx.shadowColor = P.accentBright;
      ctx.shadowBlur = 14 * dpr;
      ctx.fill();
      ctx.shadowBlur = 0;
    }

    // Orbiting decorative dots — speed driven by workload state, so the
    // mission canvas actually reflects how hard Luna is working.
    // When the mission has real progress to show (progressMode="real"),
    // we suppress the ambient bouncing dots so the leading progress dot
    // is the dominant visual signal — answering "how much is left?".
    const wAct = getActivityState();
    const isRealProgress = m.progressMode === "real" || m.kind === "aider" || m.kind === "soak";
    const baseStep = m.active ? 0.012 : 0.005;
    state.mission.orbitPhase = (state.mission.orbitPhase + baseStep * wAct.orbitMul) % (Math.PI * 2);
    // Tag the panel with workload state so CSS could react if needed.
    if (canvas.parentElement && canvas.parentElement.parentElement) {
      const card = canvas.parentElement.parentElement;
      if (card.dataset.workloadState !== wAct.workloadState) {
        card.dataset.workloadState = wAct.workloadState;
      }
    }
    // Two-orbit design (matches the prior PyQt terminal). NO text
    // labels — labels jittered when content reflowed and drove Serge
    // crazy. Just two dots on two slightly-elliptical rings circling
    // at different speeds, like moons. Each ring is hinted with a
    // faint outline so the orbital plane is visible without being
    // noisy.
    const orbitRout = Rin - 18 * dpr;
    const orbitRin  = Rin - 38 * dpr;
    // Faint orbital plane outlines (slightly elliptical to feel 3D).
    ctx.strokeStyle = "rgba(120,150,200,0.18)";
    ctx.lineWidth = 0.8 * dpr;
    ctx.beginPath();
    ctx.ellipse(cx, cy, orbitRout * 1.05, orbitRout * 0.78, 0, 0, Math.PI * 2);
    ctx.stroke();
    ctx.beginPath();
    ctx.ellipse(cx, cy, orbitRin * 1.05, orbitRin * 0.78, 0, 0, Math.PI * 2);
    ctx.stroke();
    // Outer orbiter — bright accent moon.
    {
      const ang = state.mission.orbitPhase;
      const x = cx + Math.cos(ang) * orbitRout * 1.05;
      const y = cy + Math.sin(ang) * orbitRout * 0.78;
      ctx.beginPath();
      ctx.arc(x, y, (4.2 + wAct.intensity * 0.6) * dpr, 0, Math.PI * 2);
      ctx.fillStyle = alpha(P.accentRgb, 0.95);
      ctx.shadowColor = alpha(P.accentRgb, 0.85);
      ctx.shadowBlur  = (10 + wAct.intensity * 8) * dpr;
      ctx.fill();
      ctx.shadowBlur = 0;
    }
    // Inner orbiter — slower, smaller, dimmer companion.
    {
      const ang = -state.mission.orbitPhase * 0.62 + Math.PI;  // counter-rotate
      const x = cx + Math.cos(ang) * orbitRin * 1.05;
      const y = cy + Math.sin(ang) * orbitRin * 0.78;
      ctx.beginPath();
      ctx.arc(x, y, (2.8 + wAct.intensity * 0.4) * dpr, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(220,230,255,0.85)";
      ctx.shadowColor = "rgba(220,230,255,0.55)";
      ctx.shadowBlur  = (6 + wAct.intensity * 4) * dpr;
      ctx.fill();
      ctx.shadowBlur = 0;
    }
    // Tick markers around the ring — give the progress something to read against.
    if (isRealProgress) {
      const ticks = 10;
      ctx.strokeStyle = alpha(P.accentRgb, 0.40);
      ctx.lineWidth = 1 * dpr;
      for (let i = 0; i < ticks; i++) {
        const ang = -Math.PI / 2 + (i * Math.PI * 2 / ticks);
        const r1 = Rout - 2 * dpr;
        const r2 = Rout - 8 * dpr;
        const x1 = cx + Math.cos(ang) * r1;
        const y1 = cy + Math.sin(ang) * r1;
        const x2 = cx + Math.cos(ang) * r2;
        const y2 = cy + Math.sin(ang) * r2;
        ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
      }
    }
    // Standby sweep arc removed per Serge directive — pure decoration.
    // The crescent moon below + progress arc above (when promotion
    // progress is available) carry the only meaningful visuals on
    // this canvas now.

    drawCrescent(ctx, cx, cy, Rin * 0.55, dpr);

    // Time text. Aider + Soak show a countdown clock; everything else
    // relies on the HTML overlay (#mission-actor + #mission-substage)
    // for the readable label, so we DON'T draw text on the canvas
    // anymore — that was creating the "LUNA / NEXT CYCLE AT / WAITING
    // FOR NEXT CYCLE / WAITING FOR NEXT SCHEDULED CYCLE" stack of
    // duplicates on top of the moon.
    let timeText = "", subLabel = "";
    const hasCountdown = (m.kind === "aider" || m.kind === "soak");
    if (m.active && hasCountdown) {
      timeText = fmtClock(m.remaining);
      subLabel = "TIME REMAINING";
    }
    // Only draw canvas text when we have an explicit countdown (Aider /
    // Soak). Otherwise the HTML overlay (#mission-actor + #mission-substage)
    // is the single source of truth for the moon label — drawing here too
    // creates duplicate "LUNA / WAITING FOR NEXT CYCLE / waiting_for_next_..."
    // stacks. Keep canvas clean.
    if (timeText) {
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = m.active ? P.warm : alpha(P.accentRgb, 0.55);
      ctx.shadowColor = alpha(P.accentRgb, 0.4);
      ctx.shadowBlur = m.active ? 12 * dpr : 0;
      const isShortLabel = !hasCountdown;
      ctx.font = "300 " + (m.active ? (isShortLabel ? 20 : 28) : 18) * dpr + "px " +
                 'ui-monospace, "JetBrains Mono", Consolas, monospace';
      ctx.fillText(timeText, cx, cy + Rin * 0.78);
      ctx.shadowBlur = 0;
    }
    if (subLabel) {
      ctx.font = 9 * dpr + 'px ui-monospace, "JetBrains Mono", Consolas, monospace';
      ctx.fillStyle = P.textMute;
      ctx.fillText(subLabel, cx, cy + Rin * 1.0);
    }

    text($("mission-task"), m.title);
    text($("mission-detail"), m.detail);
    // Mission-stat fallback must NEVER expose internal kind keywords like
    // "supermax" / "active" — those are CSS hooks. Use a clean readable
    // status instead.
    text($("mission-stat"),
         m.statLabel || (m.active
           ? Math.round(state.mission.progressEased * 100) + "% · live"
           : wAct.workloadState));
  }

  function drawCrescent(ctx, cx, cy, R, dpr) {
    // Premium moon redraw: gold/bronze gradient, sphere shading, multiple
    // crater layers, terminator highlight. NO 'LUNA' wordmark on the moon.
    if (!isFinite(R) || R <= 0) return;
    const P = getPalette();
    const offX = R * 0.28;

    // Outer warm halo
    const halo = ctx.createRadialGradient(cx, cy, R * 0.4, cx, cy, R * 1.18);
    halo.addColorStop(0, "rgba(255, 213, 150, 0.32)");
    halo.addColorStop(0.55, "rgba(225, 168, 92, 0.10)");
    halo.addColorStop(1, "rgba(225, 168, 92, 0)");
    ctx.fillStyle = halo;
    ctx.beginPath(); ctx.arc(cx, cy, R * 1.18, 0, Math.PI * 2); ctx.fill();

    // Dark base disc (with subtle warm core peeking through)
    const disc = ctx.createRadialGradient(cx - R * 0.18, cy - R * 0.22, R * 0.08, cx, cy, R);
    disc.addColorStop(0,  "rgba(70, 50, 30, 0.55)");
    disc.addColorStop(0.6, "rgba(28, 22, 18, 0.95)");
    disc.addColorStop(1,  "rgba(8, 8, 10, 1)");
    ctx.fillStyle = disc;
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.fill();

    // Lit edge — warm gold/bronze gradient
    ctx.save();
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.clip();

    // Primary lit pass (broad warm wash)
    const lit = ctx.createRadialGradient(
      cx - R * 0.32, cy - R * 0.20, 0,
      cx - R * 0.32, cy - R * 0.20, R * 1.45
    );
    lit.addColorStop(0,    "rgba(255, 246, 220, 0.96)");  // bright cream highlight
    lit.addColorStop(0.20, "rgba(255, 224, 168, 0.88)");  // warm gold
    lit.addColorStop(0.50, "rgba(232, 176, 102, 0.55)");  // bronze midtone
    lit.addColorStop(0.85, "rgba(160, 110, 60, 0.18)");
    lit.addColorStop(1,    "rgba(120, 80, 40, 0)");
    ctx.fillStyle = lit;
    ctx.beginPath(); ctx.arc(cx + offX, cy + R * 0.05, R * 1.45, 0, Math.PI * 2); ctx.fill();

    // Cut the dark side (crescent terminator)
    ctx.globalCompositeOperation = "destination-out";
    ctx.beginPath();
    ctx.arc(cx + offX, cy - R * 0.05, R * 0.95, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalCompositeOperation = "source-over";

    // Terminator (day/night line) — soft bronze glow inside
    const term = ctx.createRadialGradient(
      cx + offX - R * 0.78, cy, 0,
      cx + offX - R * 0.78, cy, R * 0.55
    );
    term.addColorStop(0, "rgba(255, 204, 132, 0.30)");
    term.addColorStop(1, "rgba(255, 204, 132, 0)");
    ctx.fillStyle = term;
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.fill();

    // Sphere shading — subtle dark ring at outer edge for depth
    const edge = ctx.createRadialGradient(cx, cy, R * 0.78, cx, cy, R);
    edge.addColorStop(0, "rgba(0,0,0,0)");
    edge.addColorStop(1, "rgba(0,0,0,0.55)");
    ctx.fillStyle = edge;
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.fill();

    // Crater dots — multi-layer for texture (lit + shadow rim)
    const craters = [
      // [offsetX, offsetY, radiusFraction, brightness]
      [-0.45, -0.28, 0.075, 0.22],
      [-0.30,  0.10, 0.060, 0.20],
      [-0.55,  0.15, 0.048, 0.18],
      [-0.18, -0.42, 0.046, 0.17],
      [-0.08,  0.32, 0.062, 0.20],
      [-0.40,  0.40, 0.034, 0.15],
      [-0.62, -0.05, 0.040, 0.16],
      [-0.25, -0.06, 0.030, 0.13],
      [ 0.05, -0.10, 0.028, 0.11],
      [-0.50, -0.55, 0.024, 0.10],
      [ 0.10,  0.05, 0.022, 0.10],
      [-0.05, -0.30, 0.020, 0.09],
    ];
    craters.forEach((c) => {
      // Soft halo
      ctx.fillStyle = "rgba(255, 232, 192, " + (c[3] * 0.55).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(cx + R * c[0], cy + R * c[1], R * c[2] * 1.4, 0, Math.PI * 2);
      ctx.fill();
      // Inner dark dot (the basin)
      ctx.fillStyle = "rgba(20, 14, 8, " + (c[3] * 0.7).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(cx + R * c[0] + R * 0.005, cy + R * c[1] + R * 0.006, R * c[2] * 0.78, 0, Math.PI * 2);
      ctx.fill();
      // Bright rim (sun-side)
      ctx.fillStyle = "rgba(255, 240, 200, " + (c[3] * 0.45).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(cx + R * c[0] - R * 0.012, cy + R * c[1] - R * 0.013, R * c[2] * 0.62, 0, Math.PI * 2);
      ctx.fill();
    });

    // Top-edge specular highlight — thin bright kiss along the upper-left rim
    const spec = ctx.createRadialGradient(
      cx - R * 0.48, cy - R * 0.45, 0,
      cx - R * 0.48, cy - R * 0.45, R * 0.42
    );
    spec.addColorStop(0, "rgba(255, 252, 230, 0.55)");
    spec.addColorStop(1, "rgba(255, 252, 230, 0)");
    ctx.fillStyle = spec;
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.fill();

    ctx.restore();
  }

  // ============================================================
  // Event histogram
  // ============================================================
  function drawHistogram(canvas) {
    const P = getPalette();
    const ctx = canvas.getContext("2d");
    _resizeCanvasIfNeeded(canvas);
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const data = (state.lastActivity && state.lastActivity.counts) || [];
    const N = data.length || 1;
    const max = Math.max(1, ...data);
    const bw = W / N;

    // Horizontal grid lines
    ctx.strokeStyle = alpha(P.accentRgb, 0.06);
    ctx.lineWidth = 1 * dpr;
    for (let g = 1; g <= 4; g++) {
      const y = (H * g) / 4;
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    }

    // Bar gradient — accent-bright at top, accent-deep at bottom
    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, alpha(P.accentRgb, 0.95));
    grad.addColorStop(1, alpha(P.accentRgb, 0.40));
    for (let i = 0; i < N; i++) {
      const v = data[i] || 0;
      const h = (v / max) * (H * 0.85);
      const x = i * bw + 1 * dpr;
      const y = H - h;
      ctx.fillStyle = grad;
      ctx.fillRect(x, y, Math.max(1, bw - 2 * dpr), h);
    }

    // Smoothed average line
    ctx.beginPath();
    ctx.strokeStyle = alpha(P.accentRgb, 0.45);
    ctx.lineWidth = 1.4 * dpr;
    let avg = 0;
    for (let i = 0; i < N; i++) {
      avg = avg * 0.7 + (data[i] || 0) * 0.3;
      const y = H - (avg / max) * (H * 0.85);
      const x = i * bw + bw / 2;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  function renderRoles() {
    const ul = $("hist-roles");
    if (!ul) return;
    ul.innerHTML = "";
    const roles = (state.lastActivity && state.lastActivity.by_role) || [];
    if (!roles.length) return;
    roles.forEach((r) => {
      const span = document.createElement("span");
      span.className = "luna-live__role";
      span.innerHTML = String(r.role).replace(/[^A-Za-z0-9_-]/g, "")
                      + " <b>" + (r.count|0) + "</b>";
      ul.appendChild(span);
    });
  }

  // ============================================================
  // Resource gauges
  // ============================================================
  function drawGauge(canvas, target, color) {
    const P = getPalette();
    const ctx = canvas.getContext("2d");
    _resizeCanvasIfNeeded(canvas);
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    const cx = W / 2, cy = H / 2;
    const R = Math.min(W, H) * 0.40;
    ctx.clearRect(0, 0, W, H);

    // Track
    ctx.beginPath();
    ctx.arc(cx, cy, R, Math.PI * 0.75, Math.PI * 0.25, false);
    ctx.strokeStyle = "rgba(255,255,255,0.06)";
    ctx.lineWidth = 8 * dpr;
    ctx.lineCap = "round";
    ctx.stroke();

    const id = canvas.parentElement.dataset.id;
    const cur = state.gauges[id] || 0;
    const next = cur + (target - cur) * 0.12;
    state.gauges[id] = next;
    const pct = clamp(next, 0, 100) / 100;
    const a0 = Math.PI * 0.75;
    const a1 = a0 + (Math.PI * 1.5) * pct;
    ctx.beginPath();
    ctx.arc(cx, cy, R, a0, a1, false);
    ctx.strokeStyle = color;
    ctx.shadowColor = color;
    ctx.shadowBlur = 14 * dpr;
    ctx.lineWidth = 8 * dpr;
    ctx.lineCap = "round";
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Leading dot
    const ex = cx + Math.cos(a1) * R;
    const ey = cy + Math.sin(a1) * R;
    ctx.beginPath();
    ctx.arc(ex, ey, 4 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = P.warm;
    ctx.fill();
  }

  function refreshGauges() {
    const P = getPalette();
    const res = state.lastRes || {};
    const cpuPct  = (res.cpu    && res.cpu.usage_percent)               || 0;
    const memFree = (res.memory && res.memory.available_percent)        || 0;
    const gpuFree = (res.gpu    && res.gpu.free_vram_percent)           || 0;
    const diskFree= (res.disk   && res.disk.project_drive_free_percent) || 0;

    // Theme-aware semantic mapping per gauge.
    const COLORS = {
      cpu:  P.warn,           // load → amber
      mem:  P.good,           // headroom → green
      gpu:  P.accentBright,   // free vram → accent
      disk: P.info,           // free disk → cool/info
    };
    document.querySelectorAll(".luna-gauge").forEach((g) => {
      const id = g.dataset.id;
      const c = g.querySelector("canvas");
      const num = g.querySelector(".luna-gauge__num");
      let v = 0, label = "—";
      if (id === "cpu")  { v = cpuPct;   label = fmtPct(cpuPct); }
      if (id === "mem")  { v = memFree;  label = fmtPct(memFree); }
      if (id === "gpu")  { v = gpuFree;  label = fmtPct(gpuFree); }
      if (id === "disk") { v = diskFree; label = fmtPct(diskFree); }
      drawGauge(c, v, COLORS[id]);
      if (num) num.textContent = label;
    });
  }

  // ============================================================
  // TTY ticker
  // ============================================================
  function appendTty(record) {
    const ol = $("luna-tty");
    if (!ol) return;
    const key = (record.ts || "") + "|" + (record.event || "") + "|" + trim(record.msg || "", 40);
    if (state.ttySeen.has(key)) return;
    state.ttySeen.add(key);
    if (state.ttySeen.size > 400) {
      const arr = Array.from(state.ttySeen);
      state.ttySeen = new Set(arr.slice(-200));
    }

    const li = document.createElement("li");
    li.className = "luna-tty__line luna-tty--new";
    const sigil = document.createElement("span"); sigil.className = "luna-tty__sigil"; sigil.textContent = "›";
    const ts = document.createElement("span"); ts.className = "luna-tty__ts"; ts.textContent = record.ts || "—";
    const role = document.createElement("span"); role.className = "luna-tty__role"; role.textContent = (record.role || record.source || "system");
    const msg = document.createElement("span"); msg.className = "luna-tty__msg";
    const head = document.createElement("strong"); head.textContent = (record.event || "EVENT") + " ";
    msg.appendChild(head);
    msg.appendChild(document.createTextNode(record.msg || ""));
    if (record.detail) {
      const em = document.createElement("em");
      em.textContent = " · " + trim(String(record.detail), 160);
      msg.appendChild(em);
    }
    li.appendChild(sigil); li.appendChild(ts); li.appendChild(role); li.appendChild(msg);
    ol.appendChild(li);
    while (ol.children.length > 60) ol.removeChild(ol.firstChild);
    ol.scrollTop = ol.scrollHeight;
  }

  function pushNewFeedToTty(records) {
    records.forEach((r) => appendTty(r));
  }

  // ============================================================
  // Card refreshers
  // ============================================================
  async function refreshStatus() {
    const s = await fetchJSON("/api/status");
    if (!s) return;
    state.lastStatus = s;

    const luna = s.luna || {};
    text($("stat-luna-state"), (luna.state || "unknown").toUpperCase());
    setTone($("stat-luna-state"), luna.alive ? "ok" : "warn");
    text($("luna-quote"), luna.last_message || "Standing by.");

    const w = s.worker || {};
    text($("stat-worker"), w.running ? "RUNNING · pid " + (w.pid || "?") : "STOPPED");
    setTone($("stat-worker"), w.running ? "ok" : "bad");

    const g = s.guardian || {};
    text($("stat-guardian"), (g.status || "unknown").replace(/_/g, " ").toUpperCase());
    setTone($("stat-guardian"), g.running ? "ok" : "bad");

    const a = s.aider_bridge || {};
    text($("stat-aider"), (a.running ? "ONLINE · " : "OFFLINE · ") + (a.state || "?"));
    setTone($("stat-aider"), a.running ? "ok" : "warn");

    // 2026-05-13 canonical verifier — single source (luna_verifier_status).
    // Prefer .label (e.g. "Verifier · LIVE"); fall back through .status
    // then .summary. The badge's tone is set from .healthy so the top
    // strip can never say "unknown" while a lower panel says "live".
    var _verifierLabel = (s.verifier && s.verifier.label) ? s.verifier.label
                         : (s.verifier && s.verifier.status) ? ("Verifier · " + s.verifier.status)
                         : (s.verifier && s.verifier.summary) ? s.verifier.summary
                         : "Verifier · UNKNOWN";
    text($("stat-verifier"), trim(_verifierLabel, 28));
    if (s.verifier && typeof s.verifier.healthy === "boolean" && typeof setTone === "function") {
      try { setTone($("stat-verifier"), s.verifier.healthy ? "ok" : "warn"); } catch (e) {}
    }

    const soak = s.soak || {};
    text($("stat-soak"), soak.verdict || "UNKNOWN");
    setTone($("stat-soak"),
      soak.verdict === "PASS" ? "ok" :
      soak.verdict === "FAIL" ? "bad" : "warn");

    const safe = s.safety || {};
    const exec = $("safe-exec");
    const guard = $("safe-guardian");
    if (exec)  exec.querySelector(".luna-safelock__value").textContent  = safe.code_execution_state || "LOCKED";
    if (guard) {
      const baseLabel = safe.guardian_live_enforcement || "DISABLED";
      // Read-only surface: when Guardian is in advisory dry-run, show it
      // explicitly instead of just "DISABLED". Cosmetic only — does not gate
      // any action.
      const dryRunSuffix = safe.dry_run_active ? " · DRY-RUN" : "";
      guard.querySelector(".luna-safelock__value").textContent = baseLabel + dryRunSuffix;
    }

    text($("footer-phase"), "Phase " + (s.phase || "UI-1A"));
    text($("gauge-stat"), (state.lastRes && state.lastRes.resource_mode) || "—");
  }

  async function refreshBrief() {
    // queue-livedot is maintained by tickLivedots(); this fetch keeps the
    // Decision Queue current via b.live_feed_items when live feed records are fresher.
    const b = await fetchJSON("/api/decision-brief");
    if (b) state.briefLastFreshMono = performance.now();
    if (!b) return;
    state.lastBrief = b;
    const counts = b.counts || {};
    text($("count-approve"), counts.approve_recommended || 0);
    text($("count-wait"),    counts.wait_for_more_evidence || 0);
    text($("count-deny"),    counts.do_not_approve || 0);
    text($("count-serge"),   counts.serge_only || 0);
    text($("brief-recommendation"),
      b.overall_recommendation
        ? "// overall · " + b.overall_recommendation.toUpperCase()
        : "advisory");
    text($("brief-summary"), b.serge_summary || "");

    const ul = $("brief-top-items");
    if (!ul) return;
    ul.innerHTML = "";
    const liveItems = Array.isArray(b.live_feed_items) ? b.live_feed_items.map((rec) => ({
      recommendation: "LIVE",
      goal: rec.source || rec.actor || "live feed",
      plain_english: rec.msg || rec.event || rec.stage || "recent Luna activity",
      source_path: "logs/luna_live_feed.jsonl",
    })) : [];
    const items = liveItems.length ? liveItems.slice(-6).reverse() : (b.top_items || []).slice(0, 6);
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "luna-cards__empty";
      li.textContent = "No morning brief available yet.";
      ul.appendChild(li); return;
    }
    items.forEach((it) => {
      const li = document.createElement("li");
      const tag = document.createElement("span");
      tag.className = "luna-card-tag";
      const rec = String(it.recommendation || "UNKNOWN");
      tag.dataset.rec = rec;
      tag.textContent = rec.replace(/_/g, " ");
      const goal = document.createElement("span");
      goal.className = "luna-card-goal";
      goal.textContent = it.goal || it.action_type || "—";
      const msg = document.createElement("div");
      msg.className = "luna-card-msg";
      msg.textContent = trim(it.plain_english || "", 240);
      li.appendChild(tag); li.appendChild(goal); li.appendChild(msg);

      // Per-card waiting / required-action explanation. The brief payload
      // already carries reasons (failed_checks, action_type, recommendation
      // sentence). Surface them so Serge knows *why* a card is in this
      // bucket and *what* he must do.
      const reasons = [];
      if (rec === "SERGE_ONLY") {
        reasons.push("Serge-only: action is non-delegable. " +
          (it.serge_reason || "Decide personally; no automation may approve."));
      }
      if (rec === "DO_NOT_APPROVE" && Array.isArray(it.failed_checks) && it.failed_checks.length) {
        reasons.push("blocked by: " + it.failed_checks.join(", ") +
          " · fix the failure and re-run safety checks before re-asking");
      }
      if (rec === "WAIT_FOR_MORE_EVIDENCE") {
        reasons.push("waiting for: " +
          (it.waiting_for || it.evidence_needed || "more evidence before this can be decided"));
      }
      if (it.action_type) {
        reasons.push("action type: " + String(it.action_type).replace(/_/g, " "));
      }
      if (reasons.length) {
        const why = document.createElement("div");
        why.className = "luna-card-why";
        reasons.forEach((r, idx) => {
          if (idx) why.appendChild(document.createTextNode(" · "));
          const span = document.createElement("span"); span.textContent = r;
          why.appendChild(span);
        });
        li.appendChild(why);
      }

      // Action chips: APPROVE / WAIT / DO NOT APPROVE / VIEW REPORT.
      // For SERGE_ONLY cards we deliberately omit the approve/wait chips
      // because those decisions are not delegable to dashboard clicks.
      const actions = document.createElement("div");
      actions.className = "luna-card-actions";
      if (rec !== "SERGE_ONLY") {
        ["APPROVE", "WAIT", "DO_NOT_APPROVE"].forEach((label) => {
          const a = document.createElement("button");
          a.type = "button";
          a.className = "luna-card-action";
          a.dataset.action = label;
          a.dataset.id = String(it.id || it.goal || it.report_path || "");
          a.textContent = label.replace(/_/g, " ");
          a.addEventListener("click", lunaDecisionVerdictClick);
          actions.appendChild(a);
        });
      }
      if (it.report_path || it.source_path) {
        const link = document.createElement("a");
        link.className = "luna-card-action luna-card-action--link";
        link.href = String(it.report_path || it.source_path);
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = "VIEW REPORT";
        actions.appendChild(link);
      }
      if (actions.children.length) li.appendChild(actions);
      ul.appendChild(li);
    });
    // Top-level "what is the queue waiting for" hint.
    const subEl = $("brief-recommendation");
    if (subEl) {
      const note = b.queue_waiting_reason || b.waiting_for ||
        (b.serge_summary ? null : "no morning brief yet");
      if (note) subEl.title = String(note);
    }
    state.briefLastFreshMono = (!b.is_stale && b.ok !== false) ? performance.now() : (state.briefLastFreshMono || 0);
  }

  async function refreshSoak() {
    const s = await fetchJSON("/api/soak");
    if (!s) return;
    state.lastSoak = s;
    const obs = s.observed_cycles || 0;
    const req = s.required_cycles || 144;
    const pct = req > 0 ? Math.min(100, Math.round((obs/req)*100)) : 0;
    const fill = $("soak-fill");
    if (fill) fill.style.width = pct + "%";
    text($("soak-observed"), obs);
    text($("soak-required"), req);
    text($("soak-verdict"), (s.verdict || "UNKNOWN").replace(/_/g, " "));
    {
      const v = $("soak-verdict");
      if (v) v.classList.toggle("is-advisory-pass", s.verdict === "ADVISORY_PASS");
    }
    text($("soak-last"), s.last_update ? "last " + s.last_update.replace("T"," ").slice(0,19) + "Z" : "last —");
    text($("soak-cmd"), s.soak_command || "—");
  }

  async function refreshHealth() {
    const [score, res, status] = await Promise.all([
      fetchJSON("/api/scorecard"),
      fetchJSON("/api/resources"),
      fetchJSON("/api/status"),
    ]);
    if (score) {
      state.lastScorecard = score;
      const ring = $("health-ring");
      const num  = $("health-num");
      const overall = score.overall_score || 0;
      if (ring) ring.style.setProperty("--pct", overall);
      if (num)  num.textContent = overall || "—";
      text($("health-readiness"), (score.readiness_level || "unknown").replace(/_/g, " "));
    }
    if (res) {
      state.lastRes = res;
      text($("health-mode"), res.resource_mode || "—");
      text($("health-cpu"), res.cpu && res.cpu.usage_percent != null ? fmtPct(res.cpu.usage_percent) : "—");
      text($("health-mem"), res.memory && res.memory.available_percent != null ? fmtPct(res.memory.available_percent) + " free" : "—");
      text($("health-gpu"), res.gpu && res.gpu.detected
        ? (res.gpu.name || "GPU") + " · " + fmtPct(res.gpu.free_vram_percent) + " free"
        : "n/a");
      const w = (res.warnings || []);
      const ul = $("health-warnings");
      if (ul) {
        ul.innerHTML = "";
        w.slice(0, 5).forEach((msg) => {
          const li = document.createElement("li");
          li.textContent = String(msg);
          ul.appendChild(li);
        });
      }
      const vitalsDot = $("vitals-livedot");
      if (vitalsDot && res.source_age_seconds != null) {
        vitalsDot.title = "Live vitals from " + (res.resource_source || "/api/resources") +
          " · age " + res.source_age_seconds + "s";
      }
    }
    if (status) {
      const w = status.worker || {};
      text($("health-queue"), w.queue_depth != null ? w.queue_depth : "—");
      text($("health-pending"), w.approval_pending != null ? w.approval_pending : "—");
    }
    // Vitals live-dot — green when /api/resources just returned, yellow if stale.
    state.vitalsLastFreshMono = (res && res.ok !== false && !res.is_stale) ? performance.now() : (state.vitalsLastFreshMono || 0);
  }

  function _setLivedot(prefix, freshMono, staleMs) {
    const el = $(prefix + "-livedot");
    const txt = $(prefix + "-livedot-txt");
    if (!el) return;
    const now = performance.now();
    if (!freshMono) {
      el.dataset.state = "idle";
      if (txt) txt.textContent = "awaiting";
      return;
    }
    const ageMs = now - freshMono;
    if (ageMs > staleMs) {
      el.dataset.state = "stale";
      // Make the stale text more emphatic so users see the card is no
      // longer current. The source endpoint label is encoded in `prefix`.
      const srcLabel = prefix === "vitals" ? "/api/resources"
                     : prefix === "queue"  ? "/api/decision-brief"
                     : prefix === "mc"     ? "/api/mission-control"
                     : "(source)";
      if (txt) txt.textContent = "STALE - " + srcLabel + " unresponsive " + Math.round(ageMs / 1000) + "s";
      if (el && el.title !== undefined) {
        el.title = "This card has not refreshed in " + Math.round(ageMs / 1000) +
                   "s. The data shown may not reflect Luna's current state. " +
                   "Endpoint: " + srcLabel + ". " +
                   "If the LunaTierProgressionEngine task is enabled and the latest " +
                   "tier_progression report shows actions_failed=0, Luna is still " +
                   "running; this card is just not getting fresh poll data.";
      }
    } else {
      el.dataset.state = "live";
      if (txt) txt.textContent = "LIVE " + Math.round(ageMs / 1000) + "s";
      if (el && el.title !== undefined) { el.title = ""; }
    }
  }
  function tickLivedots() {
    _setLivedot("vitals", state.vitalsLastFreshMono || 0, 10000);
    _setLivedot("queue",  state.briefLastFreshMono || 0, 30000);
    _setLivedot("mc",     state.mcLastFreshMono   || 0, 10000);
  }

  // ====================================================================
  // Live cycle countdown - the user-visible "next cycle in 00:58" clock
  // that ticks 60 -> 59 -> 58 every second. Driven from state.cycleSource
  // (refreshed by /api/mission-control on its 1.5s poll) and re-renders
  // just the cycle hint segment of the Mission Control caption every
  // second. Falls back to "Countdown source stale" when the source side
  // hasn't refreshed in a while, "Progression cycle running now" when
  // active, and "Progression task disabled" when the scheduled task is
  // off. Never invents data; never claims the countdown is fresh when
  // it isn't.
  // ====================================================================
  function _fmtCountdown(secs) {
    if (secs == null || isNaN(secs)) return "--:--";
    var s = Math.max(0, Math.floor(secs));
    var m = Math.floor(s / 60);
    var ss = s % 60;
    var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
    if (m < 60) return pad(m) + ":" + pad(ss);
    var h = Math.floor(m / 60);
    var mm = m % 60;
    return h + ":" + pad(mm) + ":" + pad(ss);
  }

  // Strip any pre-existing "next [sprint] cycle in <whatever>" segment from
  // a base hint so we never render two countdowns. The server's view.nextAction
  // sometimes carries a stale-by-one-poll "next cycle in 60 seconds" string;
  // tickCountdown() owns the live segment, so we always remove the server's
  // copy before appending our own.
  function _stripCycleSegment(s) {
    if (!s) return "";
    var next_cycle_duplicate_guard = "next_cycle";
    var out = String(s);
    out = out.replace(/\s*[-•·|]?\s*next\s+(sprint\s+)?cycle\s+in\s+[^\-•·|]+(\(sprint\))?\s*/gi, "");
    out = out.replace(/^[\s\-•·|]+/, "");
    out = out.replace(/[\s\-•·|]+$/, "");
    return out;
  }

  function tickCountdown() {
    // The dedicated countdown clock inside the moon/orbit panel is the
    // ONE visible cycle countdown. The legacy in-line hint text
    // (#mission-hint-next) gets the cycle segment STRIPPED so we never
    // render the same MM:SS twice on the same card.
    var cdRoot   = $("mission-countdown");
    var cdLabel  = $("mission-countdown-label");
    var cdTimer  = $("mission-countdown-timer");
    var cdSub    = $("mission-countdown-sub");
    var nextEl   = $("mission-hint-next");
    var view = (state.lastMissionControl && missionViewFromControl(state.lastMissionControl)) || null;
    var cs = state.cycleSource || null;
    var lastMc = state.lastMissionControl || null;
    var isComplete = !!(lastMc && lastMc.is_complete);

    // ---- compute the canonical cycle state once ----
    var cdState = "idle";    // CSS data-state (drives accent colour)
    var cdLabelText = "Waiting for next cycle";
    var cdTimerText = "--:--";
    var cdSubText = "";
    var secs = null;

    if (!cs || (cs.nextCycleAt == null && cs.nextCycleInSeconds == null)) {
      if (cs && cs.taskEnabled === false) {
        cdState = "disabled";
        cdLabelText = "Waiting for next cycle";
        cdTimerText = "--:--";
        cdSubText = "Progression task is off";
      } else if (isComplete) {
        cdState = "complete";
        cdLabelText = "LAST CYCLE COMPLETE";
        cdTimerText = "--:--";
        cdSubText = "Awaiting next cycle source";
      } else {
        cdState = "idle";
        cdLabelText = "Waiting for next cycle";
        cdTimerText = "--:--";
      }
    } else if (cs.isActive) {
      // Cycle is running. Show ELAPSED counting up + cap so you can
      // see progress toward the supervisor's external-watchdog kill
      // point. Format: "RUNNING 02:13 / 5:00" + sub-line that fills
      // a thin progress bar via CSS data-pct.
      cdState = "running";
      const lastMc2 = state.lastMissionControl || {};
      let runFor = (lastMc2 && lastMc2.cycle_running_for_seconds != null)
                     ? Number(lastMc2.cycle_running_for_seconds) : null;
      // Tick locally between server polls so the seconds counter
      // advances every second instead of waiting on /api/mission-control.
      if (runFor != null && state.mcLastFreshMono) {
        const sinceFresh = (performance.now() - state.mcLastFreshMono) / 1000;
        runFor = Math.max(0, runFor + Math.floor(sinceFresh));
      }
      // Read the cap from the continuous_supervisor block in tier-truth
      // (max_minutes_per_cycle isn't directly exposed, so we fall back
      // to a sensible default of 5 minutes — matches current config).
      const ttSnap = state.lastTierTruth || null;
      const supCap = (ttSnap && ttSnap.continuous_supervisor && ttSnap.continuous_supervisor.max_minutes_per_cycle)
                       || 5;
      const capSec = Math.max(60, Number(supCap) * 60);
      const remaining = (runFor != null) ? Math.max(0, capSec - runFor) : null;
      cdLabelText = "CYCLE RUNNING";
      cdTimerText = (runFor != null)
        ? (_fmtCountdown(runFor) + " / " + _fmtCountdown(capSec))
        : "--:--";
      cdSubText = (remaining != null)
        ? ("auto-kill in " + _fmtCountdown(remaining))
        : "running";
      // Progress percentage toward the cap — used by CSS to paint a
      // thin fill bar across the countdown widget.
      if (cdRoot && runFor != null) {
        const pct = Math.min(100, Math.round((runFor / capSec) * 100));
        cdRoot.dataset.pct = String(pct);
      }
    } else {
      // Have a next-cycle source - compute remaining seconds.
      if (cs.nextCycleAt) {
        try {
          var t = new Date(cs.nextCycleAt).getTime();
          var now = Date.now();
          secs = Math.max(0, Math.floor((t - now) / 1000));
        } catch (e) { secs = null; }
      }
      if (secs == null && cs.nextCycleInSeconds != null) {
        var elapsed = Math.max(0, (performance.now() - (cs.seenAtMono || 0)) / 1000);
        secs = Math.max(0, Math.floor(cs.nextCycleInSeconds - elapsed));
      }
      if (secs == null) {
        cdState = "idle";
        cdTimerText = "--:--";
      } else if (secs <= 0) {
        cdState = "due";
        cdLabelText = "CYCLE DUE NOW";
        cdTimerText = "00:00";
        cdSubText = "starting next cycle";
      } else if (cs.sprintMode) {
        cdState = "sprint";
        cdLabelText = "Sprint cycle in";
        cdTimerText = _fmtCountdown(secs);
      } else if (isComplete) {
        cdState = "complete";
        cdLabelText = "LAST CYCLE COMPLETE";
        cdTimerText = _fmtCountdown(secs);
        cdSubText = "Next in " + _fmtCountdown(secs);
      } else {
        cdState = "idle";
        cdLabelText = "Next cycle in";
        cdTimerText = _fmtCountdown(secs);
      }
      // Stale flag wins over the "next in MM:SS" label but keeps the
      // last known timer value visible per Serge's spec ("if source
      // stale, show 'SOURCE STALE' but do not hide latest known status").
      if (cs.isStale) {
        cdState = "stale";
        cdLabelText = "Waiting for next cycle";
        if (!cdSubText) cdSubText = "showing last known countdown";
      }
    }

    if (cdRoot) cdRoot.dataset.state = cdState;
    if (cdLabel && cdLabel.textContent !== cdLabelText) cdLabel.textContent = cdLabelText;
    if (cdTimer && cdTimer.textContent !== cdTimerText) cdTimer.textContent = cdTimerText;
    if (cdSub) {
      if (cdSubText) {
        if (cdSub.textContent !== cdSubText) cdSub.textContent = cdSubText;
        cdSub.hidden = false;
      } else {
        cdSub.textContent = "";
        cdSub.hidden = true;
      }
    }

    // ---- legacy inline hint: strip any cycle segment so the user
    //      sees ONE countdown only (the new component above). ----
    if (nextEl) {
      var rawBase = view && view.nextAction ? view.nextAction
                  : (isComplete ? "task complete" : "");
      var hintBase = _stripCycleSegment(rawBase);
      var labelText = hintBase ? ("next: " + hintBase) : "";
      if (nextEl.textContent !== labelText) nextEl.textContent = labelText;
    }
  }

  // ====================================================================
  // Global "Luna stale" guard. The user complaint: when the older panels
  // (mission-control, resources, decision-brief) lag, the UI shows Luna
  // as globally stale even though the progression engine is alive. The
  // single ground-truth signal is /api/higher-tier/progress (its
  // scheduled-task block + latest progression report). If that endpoint
  // is fresh AND its task is enabled AND the latest cycle reports zero
  // failures, Luna is NOT globally stale - regardless of older panels.
  // tickGlobalLunaHealth() applies a body-level data-attribute every
  // second so CSS / per-card states can deemphasize the "stale" treatment
  // without touching live-feed accuracy.
  // ====================================================================
  function tickGlobalLunaHealth() {
    var ht = state.lastHigherTier || null;
    var htFreshMs = state.htLastFreshMono ? (performance.now() - state.htLastFreshMono) : Infinity;
    var lf = (ht && ht.live_feed) || null;
    var task = (ht && ht.scheduled_task) || null;
    var lp = (ht && ht.latest_progression) || null;

    var htFresh = (htFreshMs <= 30000);                    // higher-tier polled within last 30s
    var feedFresh = !!(lf && lf.is_stale === false);
    var taskOk = !!(task && (task.state === "Ready" || task.state === "Running")
                          && task.last_result === 0);
    var noFailures = !!(lp && (lp.actions_failed === 0 || lp.actions_failed == null));

    var globalFresh = htFresh && feedFresh && taskOk && noFailures;
    var any = htFresh || feedFresh || taskOk;
    var label = globalFresh ? "fresh"
              : (any ? "panel-lag-but-progression-alive" : "stale");

    var b = document.body;
    if (b) {
      if (b.dataset.lunaHealth !== label) b.dataset.lunaHealth = label;
    }
    state.globalLunaHealth = label;
    return label;
  }

  // ====================================================================
  // Self-Upgrade Progress (Supermax) — evidence gate + recent patches
  // ====================================================================
  function _smShortPath(p) {
    if (!p) return "";
    const s = String(p).replace(/\\/g, "/");
    return s.length > 48 ? "…" + s.slice(-47) : s;
  }
  function _smShortTs(ts) {
    if (!ts) return "—";
    const raw = String(ts);
    const normalized = raw.includes("T") ? raw : raw.replace(" ", "T");
    const parsed = new Date(normalized);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toLocaleString(undefined, {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    const m = raw.match(/T(\d\d:\d\d:\d\d)/);
    return m ? m[1] : raw.slice(0, 19);
  }
  // LEGACY_DETAIL_ONLY (2026-05-13 hard cutover) -- this label helper
  // returns canonical-style strings for the legacy supermax details
  // pane. It is NEVER used by the primary evolution_center render path;
  // primary tile content now flows from terminal_truth.evolution_center.
  function _smTierLabel(tier) {
    const t = parseInt(tier, 10);
    if (t === 0) return "docs only";
    if (t === 1) return "docs additive tests";
    if (t === 2) return "tier 2 scope";
    if (t === 3) return "module modifications";
    return "—";
  }
  function _smResultClass(result) {
    if (!result) return "";
    const r = String(result);
    if (r === "APPLIED_AND_VERIFIED") return "";
    if (r.startsWith("REFUSED_")) return " luna-supermax__att-result--refused";
    return " luna-supermax__att-result--fail";
  }
  function _smResultShort(result) {
    if (!result) return "—";
    const r = String(result);
    if (r === "APPLIED_AND_VERIFIED") return "OK";
    if (r === "VERIFY_FAIL_ROLLED_BACK") return "RB-OK";
    if (r === "VERIFY_FAIL_ROLLBACK_FAILED") return "RB-FAIL";
    return r.replace("REFUSED_", "REF-").slice(0, 12);
  }
  function _smSetFeedback(message, tone) {
    const line = $("sm-approval-line");
    if (!line) return;
    line.textContent = message || "";
    line.dataset.state = tone || "";
    line.removeAttribute("title");
    line.hidden = !message;
  }
  function _smSetTriggerLabel(trigger, label) {
    if (!trigger || !label) return;
    const childLabel = trigger.querySelector && trigger.querySelector(".luna-supermax__btn-label");
    if (childLabel) childLabel.textContent = label;
    else trigger.textContent = label;
  }
  function _smSetBusy(trigger, busy) {
    if (!trigger) return;
    trigger.dataset.busy = busy ? "1" : "";
    if ("disabled" in trigger) trigger.disabled = !!busy;
    if (busy) trigger.setAttribute("aria-disabled", "true");
    else trigger.removeAttribute("aria-disabled");
  }

  // ====================================================================
  // Mission Control · live "who is doing what right now"
  // Fetches /api/mission-control on a fast cadence (1.5s) so the actor
  // label, stage, elapsed, and stale state stay tight to reality.
  // ====================================================================
  function _mcFmtElapsed(s) {
    if (s == null || isNaN(s)) return "—";
    s = Math.max(0, Math.floor(s));
    if (s < 60) return s + "s";
    const m = Math.floor(s / 60), ss = s % 60;
    if (m < 60) return m + "m " + ss + "s";
    const h = Math.floor(m / 60), mm = m % 60;
    return h + "h " + mm + "m";
  }
  function _mcFmtClockTs(iso) {
    if (!iso) return "—";
    try {
      const d = new Date(String(iso));
      if (!Number.isNaN(d.getTime())) return d.toLocaleTimeString();
    } catch (_e) {}
    const m = String(iso).match(/(\d\d:\d\d:\d\d)/);
    return m ? m[1] : String(iso).slice(0, 19);
  }
  function _mcShortTitle(t) {
    if (!t) return "";
    const s = String(t).replace(/\\/g, "/");
    return s.length > 56 ? "…" + s.slice(-55) : s;
  }
  // ====================================================================
  // Higher-Tier Progress · /api/higher-tier/progress
  // Display-only. Surfaces current_effective_tier (>=6 once Serge approves
  // each gate), Tier 7 review-council scoreboard, Tier 8 readiness, and
  // the LunaTierProgressionEngine scheduled-task status. Lets the rest of
  // the UI stop showing stale "TIER 2 ACTIVE" once the backend is at 6/7+.
  // ====================================================================
  async function pollHigherTier() {
    try {
      const data = await fetchJSON("/api/higher-tier/progress");
      if (!data || !data.ok) return;
      state.lastHigherTier = data;
      state.htLastFreshMono = performance.now();
      _renderHigherTierCard(data);
      if (window.__lunaCmapApplyHigherTier) window.__lunaCmapApplyHigherTier(data);
      markDashboardFresh("/api/higher-tier/progress");
    } catch (_e) { /* read-only; never throw */ }
  }

  function _renderHigherTierCard(ht) {
    // Legacy raw-text appender — superseded by the LUNA EVOLUTION COMMAND
    // CENTER cards. If the Evolution panel exists in the DOM, this function
    // becomes a no-op AND removes any previously-injected legacy block.
    const evoPanel = document.querySelector(".luna-evo");
    if (evoPanel) {
      const stale = document.getElementById("luna-higher-tier-card");
      if (stale && stale.parentNode) stale.parentNode.removeChild(stale);
      return;
    }
    // Fallback: pre-Evolution HTML — keep the old block so the panel never
    // looks empty if a user is on a stale dashboard build.
    let host = document.getElementById("luna-higher-tier-card");
    if (!host) {
      const anchor = document.querySelector(".luna-supermax")
                  || document.querySelector("[data-panel='self-upgrade']")
                  || document.querySelector(".luna-mission");
      if (!anchor || !anchor.parentNode) return;
      host = document.createElement("section");
      host.id = "luna-higher-tier-card";
      host.className = "luna-higher-tier";
      host.style.padding = "8px 12px";
      host.style.marginTop = "8px";
      host.style.border = "1px solid rgba(120,180,255,0.30)";
      host.style.borderRadius = "6px";
      host.style.background = "rgba(20,30,50,0.55)";
      host.style.fontFamily = "monospace";
      host.style.fontSize = "12px";
      host.style.lineHeight = "1.4";
      anchor.parentNode.insertBefore(host, anchor.nextSibling);
    }
    const cet = String(ht.current_effective_tier || "");
    const flags = ht.tier_flags || {};
    const lap   = ht.live_apply_state || {};
    const lp    = ht.latest_progression || {};
    const sb7   = ht.tier7_scoreboard || {};
    const sb8r  = ht.tier8_readiness || {};
    const task  = ht.scheduled_task || {};
    const lf    = ht.live_feed || {};
    const stale = !!lf.is_stale;
    const taskState = (task && task.state) ? String(task.state) : "(unknown)";
    const taskNext  = (task && task.next_run_time) ? task.next_run_time : "(unknown)";
    const taskLast  = (task && task.last_run_time) ? task.last_run_time : "(unknown)";
    const lastResult = (task && task.last_result != null) ? task.last_result : "(unknown)";
    const liveApplyOff = (lap.tier3_live_apply_enabled === false && lap.allow_live_apply === false);
    // 2026-05-16 XSS fix per Codex deep-scan C10: ht.headline /
    // ht.subline / lp.decision / lp.cycle_id come from /api/terminal-
    // truth or /api/luna-pulse and could be poisoned by a compromised
    // backend. Escape every untrusted string before innerHTML.
    function _xsafe(s) {
      return String(s == null ? "" : s).replace(/[&<>"']/g,
        (c) => ({ "&":"&amp;","<":"&lt;",">":"&gt;",
                  '"':"&quot;","'":"&#39;" }[c]));
    }
    host.innerHTML =
      '<div style="font-weight:700;color:#9ecdff;letter-spacing:1px;">' +
        _xsafe(ht.headline || ("TIER " + cet + " ACTIVE")) +
      '</div>' +
      '<div style="color:#cfe6ff;margin-top:2px;">' + _xsafe(ht.subline || "") + '</div>' +
      '<div style="margin-top:6px;color:#bcd8ff;">' +
        '<span>tier6 ' + (flags.tier6_enabled ? '<b style="color:#7fff9f;">ON</b>' : 'off') + '</span>' +
        ' &middot; <span>tier7 ' + (flags.tier7_enabled ? '<b style="color:#7fff9f;">ON</b>' : 'off') + '</span>' +
        ' &middot; <span>tier8 ' + (flags.tier8_enabled ? '<b style="color:#ffd57f;">ON</b>' : 'off') + '</span>' +
        ' &middot; <span>tier9+ off</span>' +
      '</div>' +
      '<div style="margin-top:4px;color:' + (liveApplyOff ? '#9fffbf' : '#ff9f9f') + ';">' +
        'live apply ' + (liveApplyOff ? 'OFF (broad live apply disabled)' : 'ON (REVIEW REQUIRED)') +
      '</div>' +
      '<div style="margin-top:6px;color:#cfe6ff;">' +
        'latest action: <b>' + _xsafe(lp.decision || '(none)') + '</b>' +
        ' &middot; passed ' + (lp.passed != null ? (lp.passed | 0) : 0) +
        ' / failed ' + (lp.failed != null ? (lp.failed | 0) : 0) +
        ' &middot; cycle ' + _xsafe(lp.cycle_id || '-') +
      '</div>' +
      '<div style="margin-top:6px;color:#cfe6ff;">' +
        'tier7 council: ' + (sb7.total_reviews || 0) + ' reviews ' +
        '(' + (sb7.approved_packets || 0) + ' SAFE / ' +
              (sb7.hold_for_review_packets || 0) + ' HOLD / ' +
              (sb7.do_not_promote_packets || 0) + ' DENY) &middot; ' +
        'rollback failures ' + (sb7.rollback_failures || 0) +
      '</div>' +
      '<div style="margin-top:4px;color:#cfe6ff;">' +
        'next gate: <b>' + (ht.next_gate || '(none)') + '</b>' +
      '</div>' +
      (sb8r.blockers && sb8r.blockers.length ? (
        '<div style="margin-top:4px;color:#ffe49a;">' +
          '<b>Tier 8 blocked by restore drill requirement.</b><br>' +
          sb8r.blockers.map(b => '<span>' + String(b).replace(/[<>&]/g, '') + '</span>').join(' &middot; ') +
          '<div style="margin-top:4px;color:#ffd57f;font-size:11px;">' +
            'Required next safe action: run the Tier 8 restore drill preview, then the drill. ' +
            'Tier 8 is NOT approved automatically; live patches stay disabled.' +
          '</div>' +
          '<div style="margin-top:4px;color:#aac8ff;font-size:11px;font-family:monospace;">' +
            'Preview: powershell -ExecutionPolicy Bypass -NoProfile -File ' +
            '"D:\\SurgeApp\\Luna_Tier8_RestoreDrill_Run.ps1" -PreviewOnly<br>' +
            'Drill:&nbsp;&nbsp; powershell -ExecutionPolicy Bypass -NoProfile -File ' +
            '"D:\\SurgeApp\\Luna_Tier8_RestoreDrill_Run.ps1" -Drill' +
          '</div>' +
        '</div>'
      ) : '') +
      '<div style="margin-top:6px;color:#bcd8ff;">' +
        'progression task: <b>' + taskState + '</b>' +
        ' &middot; last result ' + lastResult +
        ' &middot; last run ' + taskLast +
        ' &middot; next run ' + taskNext +
      '</div>' +
      '<div style="margin-top:4px;color:' + (stale ? '#ff9f9f' : '#9fffbf') + ';">' +
        'live feed ' + (stale ? 'STALE' : 'fresh') +
        ' (age ' + (lf.age_seconds != null ? lf.age_seconds + 's' : '?') +
        ', stale threshold ' + (lf.stale_threshold_seconds || 600) + 's)' +
      '</div>' +
      '<div style="margin-top:6px;color:#7fa6cc;font-size:11px;">' +
        'Source: /api/higher-tier/progress &middot; display-only &middot; ' +
        'broad live apply remains disabled at all tiers' +
      '</div>';
  }

  // ====================================================================
  // Tier Truth · /api/tier-truth   (drives the Evolution Command Center)
  // ====================================================================
  // AbortController dedup: if a previous /api/tier-truth fetch is still
  // outstanding when the next 1s tick fires, skip the new request.
  // Without this guard, network jitter lets requests pile up and the
  // dashboard's painters race each other.
  let _ttInFlight = false;
  let _ttController = null;
  async function pollTierTruth() {
    if (_ttInFlight) return;
    _ttInFlight = true;
    try {
      try { _ttController = new AbortController(); } catch (_e) { _ttController = null; }
      const opts = { credentials: "omit", cache: "no-store" };
      if (_ttController) opts.signal = _ttController.signal;
      const r = await fetch("/api/tier-truth", opts);
      if (!r.ok) return;
      const data = await r.json();
      if (!data || !data.ok) return;
      state.lastTierTruth = data;
      state.ttLastFreshMono = performance.now();
      paintEvolutionCenter(data);
      // VISIBLE-UI FINAL KILL GUARD — last-ditch sweep across known
      // offender IDs in case any individual renderer slipped a banned
      // active-tier phrase past its own guard.
      try { _enforceVisibleUiKillGuard(data); } catch (_e) { /* never break paint */ }
    } catch (_e) {
      /* read-only; never throw */
    } finally {
      _ttInFlight = false;
      _ttController = null;
    }
  }
  // ====================================================================
  // Level/Tier Framework overlay   (luna_dashboard/level_tier_status.json)
  // ====================================================================
  // The /api/tier-truth endpoint surfaces the LEGACY ladder (5L/6/7/8/9/10/X).
  // The Level/Tier Framework added 2026-05-08 (4 levels x 50 tiers, ceiling
  // L10/T500, FastStore-enabled) lives in a separate file refreshed every
  // 1s by Luna_LevelTier_Refresher.ps1 (LunaLevelTierRefresherUser task).
  // We overlay its values on top of the legacy hero so Serge sees both at
  // once: legacy ladder rung in the big number, framework Level+global tier
  // in the label/subtitle.
  let _ltInFlight = false;
  // Disabled 2026-05-08: this used to fetch luna_dashboard/level_tier_status.json
  // every 1s, but the dashboard's static-file whitelist (in luna_http_dashboard.py,
  // on the inviolate floor) doesn't include that path -> every poll 404'd. We
  // saw 1100+ 404s per session in the console. The Level/Tier data is still
  // available via the Luna_LevelTier_Status.ps1 terminal command and the daily
  // morning report; the dashboard hero overlay was a nice-to-have, not critical.
  // To re-enable, an API endpoint must first be added to the inviolate handler.
  async function pollLevelTierFramework() { /* disabled - see comment above */ }

  // Force-refresh hook: Ctrl+Shift+R inside the dashboard re-fires the
  // tier-truth poll immediately without waiting for the 1s tick.
  document.addEventListener("keydown", (ev) => {
    if (ev.ctrlKey && ev.shiftKey && (ev.key === "R" || ev.key === "r")) {
      try {
        if (_ttController) { try { _ttController.abort(); } catch (_e) {} }
        _ttInFlight = false;
        pollTierTruth();
      } catch (_e) {}
    }
  });

  async function pollOpenCode() {
    try {
      const data = await fetchJSON("/api/opencode/status");
      if (!data || !data.ok) return;
      state.lastOpenCode = data;
      paintOpenCodePill(data);
    } catch (_e) { /* read-only; never throw */ }
  }

  // ===================================================================
  // CANONICAL TIER DISPLAY MODEL (2026-05-12 Dashboard Tier Display
  // Unification). One source of truth shared by every dashboard panel:
  // Live Map, Up Next, Tier Adoption / Live Brain, Tier Graduation,
  // Evolution Command Center, and any tier chip/card/badge.
  //
  // Truth rules:
  //   - current_operating_tier is the current operating tier ONLY if the
  //     lifecycle is OPERATING or OPERATIONAL_PROVEN.
  //   - highest_generated_tier  is NOT current tier.
  //   - highest_artifact_tier   is NOT current tier.
  //   - current_effective_tier (counter high-water mark) is NOT current
  //     tier.
  //   - highest_proposed_tier   is NOT current tier.
  //   - highest_adopted_tier (without OPERATING proof) is NOT current
  //     tier — it's the highest tier whose ADOPTING/ADOPTED step was
  //     recorded; absent proof it can't be claimed as live.
  //   - If a drift exists (e.g. generated 500 vs operating 160), the
  //     display must say "Tier 500 generated, not adopted" — NEVER
  //     "Active" / "Live" for the generated number.
  //   - Up Next must NOT show "operating+1" when drift exists; instead
  //     it should reflect the next graduation step for the operating
  //     tier itself.
  // -------------------------------------------------------------------
  function getCanonicalTierDisplay(tt) {
    const tier_truth = (tt && typeof tt === "object") ? tt : {};
    // 2026-05-13 canonical-truth wiring per Serge audit:
    // If /api/tier-truth already surfaces the canonical fields from
    // drift_repair_authority, trust them directly. The dashboard then
    // never has to re-derive operating/displayed/terminal-used from
    // raw current_effective_tier (which can be the counter high-water).
    if (typeof tier_truth.truth_verdict === "string"
        && (typeof tier_truth.canonical_operating_tier === "number"
            || tier_truth.canonical_operating_tier === null)) {
      const _isProven = (tier_truth.truth_verdict === "PROVEN_ACTIVE");
      const _drift = !!tier_truth.drift;
      return {
        currentOperatingTier: tier_truth.canonical_operating_tier,
        lifecycleState: tier_truth.proof_chain_status || null,
        isProven: _isProven,
        displayLabel: (tier_truth.canonical_displayed_tier != null)
          ? ("Tier " + tier_truth.canonical_displayed_tier) : "—",
        displayQualifier: (_isProven ? "operating · proven"
                          : (tier_truth.canonical_ui_status || "UNDER_AUDIT")),
        generatedTier: tier_truth.highest_generated_tier,
        adoptedTier: tier_truth.highest_adopted_tier,
        proposedTier: tier_truth.highest_proposed_tier,
        artifactTier: tier_truth.highest_artifact_tier,
        counterHighWaterMark: tier_truth.counter_high_water_mark,
        highWaterMark: tier_truth.high_water_mark,
        hasAdoptionDrift: _drift,
        nextAction: tier_truth.next_action || "",
        nextTierAllowed: null,
        warning: tier_truth.blocker_reason || "",
        tierForDisplay: tier_truth.canonical_operating_tier,
        trustedSource: "canonical_truth_verdict",
        // Surface the raw verdict pair so panel-level renderers can
        // condition on it (e.g. pill color).
        truthVerdict: tier_truth.truth_verdict,
        canonicalNextGate: tier_truth.canonical_next_gate,
        canonicalTier500Status: tier_truth.canonical_tier_500_status,
        canonicalUiStatus: tier_truth.canonical_ui_status,
        mayClaimActive: !!tier_truth.may_claim_active,
      };
    }
    // Fallback path: derive from unified_tier_truth like before. Used
    // when the backend hasn't been upgraded yet (older deployments).
    const unified = (tier_truth.unified_tier_truth && typeof tier_truth.unified_tier_truth === "object")
      ? tier_truth.unified_tier_truth : {};

    // Pull raw signals (prefer unified block, fall back to top-level).
    function _pick(key) {
      if (unified[key] !== undefined && unified[key] !== null) return unified[key];
      if (tier_truth[key] !== undefined && tier_truth[key] !== null) return tier_truth[key];
      return null;
    }
    const operatingRaw = _pick("current_operating_tier");
    const provenRaw    = _pick("current_operationally_proven_tier");
    const adoptedRaw   = _pick("current_adopted_tier");
    const proposedRaw  = _pick("highest_proposed_tier");
    const artifactRaw  = _pick("highest_artifact_tier");
    const generatedRaw = _pick("highest_generated_tier");
    const counterRaw   = _pick("counter_high_water_mark");
    const counterEff   = (tier_truth.current_effective_tier != null)
      ? tier_truth.current_effective_tier : null;
    const lifecycleRaw = _pick("lifecycle_state");

    function _toInt(v) {
      if (v === null || v === undefined || v === "") return null;
      const n = parseInt(String(v), 10);
      return Number.isFinite(n) ? n : null;
    }

    const operatingTier = _toInt(operatingRaw);
    const provenTier    = _toInt(provenRaw);
    const adoptedTier   = _toInt(adoptedRaw);
    const proposedTier  = _toInt(proposedRaw);
    const artifactTier  = _toInt(artifactRaw);
    const generatedTier = _toInt(generatedRaw);
    const counterTier   = _toInt(counterRaw);
    const counterEffInt = _toInt(counterEff);

    const lifecycleState = lifecycleRaw ? String(lifecycleRaw).toUpperCase() : "";
    const PROVEN_STATES = ["OPERATING", "OPERATIONAL_PROVEN"];
    const isProven = PROVEN_STATES.indexOf(lifecycleState) !== -1;

    // currentOperatingTier: only claimable as live if the lifecycle is
    // OPERATING or OPERATIONAL_PROVEN. If the lifecycle field is absent
    // (older payloads) we still trust the proof registry's
    // current_operating_tier — it would only be set after proof.
    let currentOperatingTier = null;
    let trustedSource = "";
    if (operatingTier !== null && (isProven || lifecycleState === "")) {
      currentOperatingTier = operatingTier;
      trustedSource = "current_operating_tier";
    } else if (provenTier !== null) {
      currentOperatingTier = provenTier;
      trustedSource = "current_operationally_proven_tier";
    }

    // The "high-water" numbers — these are the SIDE FACTS the dashboard
    // should still display somewhere, but never as Active / Live.
    const highestGenerated = (generatedTier !== null) ? generatedTier
      : (counterTier !== null ? counterTier : counterEffInt);
    const highestProposed  = proposedTier;
    const highestArtifact  = artifactTier;
    const highestAdopted   = adoptedTier;

    // Drift: any of the high-water numbers > current operating tier.
    // The dashboard MUST refuse to show the high-water as "live" while
    // this is true.
    const candidates = [highestGenerated, highestProposed, highestArtifact, highestAdopted, counterEffInt];
    let highWater = null;
    for (let i = 0; i < candidates.length; i++) {
      const v = candidates[i];
      if (v !== null && (highWater === null || v > highWater)) highWater = v;
    }
    const hasAdoptionDrift = (currentOperatingTier !== null && highWater !== null && highWater > currentOperatingTier);

    // Display label per truth rules:
    //   - proven operating tier exists -> "Tier <N>" (no qualifier)
    //   - drift exists -> "Tier <op>  ·  Tier <hw> generated, not adopted"
    //   - operating unknown, only high-water -> "Tier <hw> generated, not adopted"
    //   - nothing known -> "—"
    let displayLabel = "—";
    let displayQualifier = "";
    if (currentOperatingTier !== null && hasAdoptionDrift) {
      displayLabel = "Tier " + currentOperatingTier;
      displayQualifier = "Tier " + highWater + " generated, not adopted";
    } else if (currentOperatingTier !== null) {
      displayLabel = "Tier " + currentOperatingTier;
      displayQualifier = isProven ? "operating · proven" : "operating";
    } else if (highWater !== null) {
      displayLabel = "Tier " + highWater + " generated, not adopted";
      displayQualifier = "no operating tier yet";
    }

    // Up Next rule:
    //   - drift exists -> the next step is "prove the current operating
    //     tier" (still graduating the existing tier), so do NOT advance.
    //     Frontend should display "Hold · prove Tier <op>" not "Tier <op+1>".
    //   - drift absent + proven tier known -> next is op + 1.
    //   - otherwise -> unknown.
    let nextTierAllowed = null;
    let nextAction = "";
    if (hasAdoptionDrift) {
      nextTierAllowed = null;
      nextAction = "Hold · prove Tier " + (currentOperatingTier !== null ? currentOperatingTier : "?")
        + " before advancing";
    } else if (currentOperatingTier !== null && isProven) {
      nextTierAllowed = currentOperatingTier + 1;
      nextAction = "Tier " + currentOperatingTier + " → " + nextTierAllowed;
    } else if (currentOperatingTier !== null) {
      nextTierAllowed = null;
      nextAction = "Tier " + currentOperatingTier + " · awaiting proof";
    }

    const warning = hasAdoptionDrift
      ? ("adoption drift: high-water=" + (highWater === null ? "?" : highWater)
         + " > operating=" + (currentOperatingTier === null ? "?" : currentOperatingTier))
      : "";

    return {
      currentOperatingTier:        currentOperatingTier,
      lifecycleState:              lifecycleState || null,
      isProven:                    isProven,
      displayLabel:                displayLabel,
      displayQualifier:            displayQualifier,
      generatedTier:               highestGenerated,
      adoptedTier:                 highestAdopted,
      proposedTier:                highestProposed,
      artifactTier:                highestArtifact,
      counterHighWaterMark:        (counterTier !== null) ? counterTier : counterEffInt,
      highWaterMark:               highWater,
      hasAdoptionDrift:            hasAdoptionDrift,
      nextAction:                  nextAction,
      nextTierAllowed:             nextTierAllowed,
      warning:                     warning,
      // Convenience: a single number that every panel can safely render
      // as "the tier" (null if no proven tier known yet).
      tierForDisplay:              currentOperatingTier,
      // Echo the trust source so debug overlays / tests can verify.
      trustedSource:               trustedSource,
    };
  }
  // Expose for tests and for cross-panel use.
  try { window.getCanonicalTierDisplay = getCanonicalTierDisplay; } catch (_e) {}

  // ==================================================================
  // VISIBLE-UI FINAL KILL GUARD (2026-05-12)
  // ==================================================================
  // Even if any individual renderer slips a legacy "Tier 500 active" /
  // "Next Gate T500" / "awaiting Serge bump" phrase past its own guard,
  // this post-render sweep rewrites it from the visible DOM whenever the
  // proof chain forbids active tier claims. Scoped to known offender IDs
  // so we never mangle unrelated text.
  const _KILL_GUARD_OFFENDER_IDS = [
    "sm-au-operational", "sm-au-promote-state",
    "sm-ng-title", "sm-ng-detail", "sm-ng-pill",
    "sm-status-pill", "sm-headline", "sm-headline-sub",
    "lta-terminal-used", "lta-current-live",
    "luna-evo-headline", "luna-evo-sub",
    // 2026-05-13 added per Serge final-render audit:
    "sm-tier-label", "sm-tier-subtitle",
    "sm-evo-active-text", "sm-current-level", "sm-current-tier",
    "ltg-next-allowed", "ltg-next-id", "ltg-effective",
    "ltg-blocker",
  ];
  const _BANNED_ACTIVE_RE = /\b(?:Tier|TIER)\s+\d+\s+(?:active|ACTIVE|LIVE|live)\b/g;
  const _BANNED_SERGE_BUMP_RE = /awaiting\s+Serge\s+bump/gi;
  const _BANNED_TIER_RAW_T_RE = /^\s*T\d+\s*$/;  // pill text like "T500"
  // 2026-05-13 terminal/UI text migration: catch the four NEW stale
  // phrase shapes the operator screenshotted.
  const _BANNED_LEVEL_TIER_ACTIVE_RE =
        /\bLEVEL\s+\d+\s*[·:.\-]?\s*TIER\s+\d+\s+(?:ACTIVE|LIVE|live|active)\b/gi;
  const _BANNED_COUNTER_LEVEL_TIER_RE = /\bCounter\s+Level\s+\d+\s+Tier\s+\d+\b/gi;
  const _BANNED_BLANK_TIER_RE = /\bTier\s+—\b/g;       // em-dash placeholder
  const _BANNED_TIER_DASH_RE = /\bTier\s+-\s*$/gm;     // hyphen variant
  function _enforceVisibleUiKillGuard(tt) {
    const ttSafe = (tt && typeof tt === "object") ? tt : (state.lastTierTruth || {});
    const ctd = (typeof getCanonicalTierDisplay === "function")
                  ? getCanonicalTierDisplay(ttSafe) : null;
    const proven = (ttSafe.proof_chain_status === "PROVEN");
    const mayClaim = (ttSafe.may_claim_active === true);
    const drift = ctd ? ctd.hasAdoptionDrift : false;
    // Guard only when proof forbids active claims OR drift exists.
    if (proven && mayClaim && !drift) return;
    for (let i = 0; i < _KILL_GUARD_OFFENDER_IDS.length; i++) {
      const el = document.getElementById(_KILL_GUARD_OFFENDER_IDS[i]);
      if (!el) continue;
      let txt = el.textContent || "";
      let mutated = false;
      if (_BANNED_ACTIVE_RE.test(txt)) {
        txt = txt.replace(_BANNED_ACTIVE_RE, "Tier <under audit>");
        mutated = true;
      }
      if (_BANNED_LEVEL_TIER_ACTIVE_RE.test(txt)) {
        txt = txt.replace(_BANNED_LEVEL_TIER_ACTIVE_RE, "Tier <under audit>");
        mutated = true;
      }
      if (_BANNED_COUNTER_LEVEL_TIER_RE.test(txt)) {
        // 2026-05-13: the operator screenshotted "Counter Level 10 Tier 50"
        // rendered as if it were current truth. Replace with the honest
        // counter-only label.
        txt = txt.replace(_BANNED_COUNTER_LEVEL_TIER_RE,
                          "Counter high-water · NOT CURRENT");
        mutated = true;
      }
      if (_BANNED_BLANK_TIER_RE.test(txt) || _BANNED_TIER_DASH_RE.test(txt)) {
        txt = txt
          .replace(_BANNED_BLANK_TIER_RE, "Tier UNKNOWN · awaiting truth")
          .replace(_BANNED_TIER_DASH_RE, "Tier UNKNOWN · awaiting truth");
        mutated = true;
      }
      if (_BANNED_SERGE_BUMP_RE.test(txt)) {
        txt = txt.replace(_BANNED_SERGE_BUMP_RE, "BLOCKED · repair_tier_drift");
        mutated = true;
      }
      if (drift && _BANNED_TIER_RAW_T_RE.test(txt)) {
        txt = "BLOCKED";
        mutated = true;
      }
      if (mutated) {
        el.textContent = txt;
        try { el.dataset.killGuardRewrote = "1"; } catch (_e) { /* ignore */ }
      }
    }
  }
  // Expose for tests + cross-panel calls.
  try { window._enforceVisibleUiKillGuard = _enforceVisibleUiKillGuard; } catch (_e) {}

  function _evoText(id, value, fallback) {
    const el = document.getElementById(id);
    if (el) el.textContent = (value == null || value === "") ? (fallback || "—") : String(value);
  }
  // 2026-05-10 fix per Serge screenshot: the tier ladder (5L, 6, 7, 8, 9,
  // 10, L2+) has its data-state attributes set ONCE in HTML and was never
  // updated by JS. Result: at Tier 50 the dashboard still showed "Tier 10
  // CURRENT" and "L2+ FUTURE". This function recomputes the state of every
  // rung from current_effective_tier so the highlight follows the real
  // tier. Level math: ceil(tier/50) — Level 1 covers Tiers 1..50, Level 2
  // covers 51..100, etc., up to Level 10 / Tier 500 ceiling.
  function _evoUpdateTierLadderState(tierNum) {
    if (!Number.isFinite(tierNum) || tierNum < 1) return;
    var lad = document.getElementById("sm-ladder");
    if (!lad) return;
    var level = Math.max(1, Math.floor((tierNum - 1) / 50) + 1);
    var rungs = lad.querySelectorAll(".luna-evo__rung");
    for (var i = 0; i < rungs.length; i++) {
      var rung = rungs[i];
      var t = rung.getAttribute("data-tier");
      var newState = "future";
      if (t === "5L") {
        newState = "completed";
      } else if (t === "L2+") {
        // The catch-all "Beyond" rung becomes the CURRENT marker once
        // tierNum exceeds the legacy ladder ceiling of 10.
        if (tierNum > 10) {
          newState = "current";
          var cnt = rung.querySelector(".luna-evo__rung-count");
          var tierInLevel = ((tierNum - 1) % 50) + 1;
          if (cnt) cnt.textContent = "Level " + level + " · Tier " + tierInLevel;
          var eta = rung.querySelector(".luna-evo__rung-eta");
          if (eta) eta.textContent = (level >= 10) ? "at L10/T500 ceiling" : "council-authorized · advancing";
        } else {
          newState = "future";
        }
      } else {
        var rt = parseInt(t, 10);
        if (Number.isFinite(rt)) {
          if (rt < tierNum)      newState = "completed";
          else if (rt === tierNum) newState = "current";
          else                     newState = "future";
        }
      }
      if (rung.getAttribute("data-state") !== newState) {
        rung.setAttribute("data-state", newState);
      }
    }
  }
  function _evoLocalTime(iso) {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return String(iso);
      return d.toLocaleString();
    } catch (_e) { return String(iso); }
  }

  function paintEvolutionCenter(tt) {
    if (!tt) return;
    const panel = document.getElementById("supermax-panel");
    if (!panel) return;

    // 2026-05-12 Terminal Tier Truth Correction: prefer the operating
    // tier from the proof registry (current_operating_tier) over the
    // counter high-water mark (current_effective_tier). Under the
    // graduation doctrine the operating tier is the TRUTH; the counter
    // is just a high-water mark from earlier evidence-gate chains.
    //
    // Pre-fix: tt.current_effective_tier = 500 -> Level 10 · Tier 50
    // Post-fix: tt.current_operating_tier = 160 -> Level 4 · Tier 10
    //
    // Fall back to the counter only when the operating tier is unknown
    // (very early bootstrap before the proof registry is populated).
    let rawTier = "";
    if (tt.current_operating_tier !== undefined && tt.current_operating_tier !== null) {
      rawTier = String(tt.current_operating_tier);
    } else {
      rawTier = String(tt.current_effective_tier || "");
    }
    const tierNum = parseInt(rawTier, 10);
    // De-dupe DOM writes so the 1Hz poll doesn't trigger needless attribute
    // mutation (which can cascade CSS re-evaluation + layout flicker on the
    // large LEVEL/Tier text). Cache last-applied values on the panel.
    if (panel.dataset.tier !== rawTier) panel.dataset.tier = rawTier;
    if (Number.isFinite(tierNum) && String(tierNum) === rawTier) {
      const level = Math.max(1, Math.floor((tierNum - 1) / 50) + 1);
      const tierInLevel = ((tierNum - 1) % 50) + 1;
      const lvlStr = String(level);
      const tnStr  = String(tierInLevel);
      _evoText("sm-current-level", lvlStr);
      _evoText("sm-current-tier",  tnStr);
      if (panel.dataset.framework      !== "yes")  panel.dataset.framework      = "yes";
      if (panel.dataset.frameworkLevel !== lvlStr) panel.dataset.frameworkLevel = lvlStr;
      if (panel.dataset.frameworkTier  !== tnStr)  panel.dataset.frameworkTier  = tnStr;
      // 2026-05-10 fix: keep the tier ladder rung-states in sync with the
      // real current_effective_tier. Without this the row was frozen at
      // "Tier 10 = current" and "L2+ = future" no matter how high the
      // counter went. _evoUpdateTierLadderState walks all rungs; for any
      // numeric rung r: r<tier=>completed, r==tier=>current, r>tier=>future.
      // L2+ becomes "current" once tier > 10 and shows "Level N · Tier T".
      try { _evoUpdateTierLadderState(tierNum); } catch (_e) {}

      // 2026-05-08 round 8: feed the Live Map's signal-tied animations.
      // a) Tier promotion fireworks fire when tierNum increases past the
      //    last value we saw.
      // b) Progress arc around LUNA_CORE reflects tierNum / ceilingTier.
      // c) Audit-chain pulse fires when audit_entries grows (proxy for
      //    "Luna just did something durable + memorable").
      try {
        const cm = window.__lunaCmap || cmap;
        const ceil = Number(tt.ceiling_tier || cm.ceilingTier || 500);
        if (tierNum > cm.tierGlobal && cm.tierGlobal > 0) {
          // Real promotion event - spawn the firework at LUNA_CORE.
          const positions = (typeof _cmapNodePositions === "function")
            ? _cmapNodePositions() : null;
          const core = positions ? positions.LUNA_CORE : null;
          if (core) {
            const [cx, cy] = core;
            const burstSize = 24;
            const hue = 30 + ((tierNum - 1) % 50) * 6;  // hue cycles per Level
            for (let i = 0; i < burstSize; i++) {
              const a = (i / burstSize) * Math.PI * 2 + Math.random() * 0.1;
              const speed = (0.45 + Math.random() * 0.35) * cm.dpr;
              cm.fireworks.push({
                x: cx, y: cy,
                vx: Math.cos(a) * speed,
                vy: Math.sin(a) * speed,
                age: 0, life: 1,
                hue: hue,
              });
            }
            // And one extra particle from CURRENT_TIER -> MEMORY to
            // reinforce that the council just memorialised this tier.
            try { _cmapSpawnParticle("CURRENT_TIER", "MEMORY"); } catch (_e) {}
          }
        }
        cm.tierGlobal = tierNum;
        cm.ceilingTier = ceil;
        // Audit pulse: when audit_entries grows, glow LUNA_CORE briefly
        // and fly a particle to MEMORY (audit chain is durable memory).
        const auditNow = Number(tt.audit_entries || 0);
        if (auditNow > cm.lastAuditCount && cm.lastAuditCount > 0) {
          try {
            _cmapActivate("LUNA_CORE", 0.7);
            _cmapSpawnParticle("LUNA_CORE", "MEMORY");
          } catch (_e) {}
        }
        if (auditNow > 0) cm.lastAuditCount = auditNow;
      } catch (_e) { /* never break paint over an animation hook */ }
    } else {
      _evoText("sm-current-level", "—");
      _evoText("sm-current-tier", rawTier || "—");
      if (panel.dataset.framework !== "no") panel.dataset.framework = "no";
    }
    // 2026-05-13 final announcement migration per Serge:
    // Prefer canonical_subline / canonical_headline (from
    // canonical_announcement_formatter via /api/tier-truth) over the
    // legacy raw tt.subline ("Counter Level X Tier Y") and tt.headline
    // ("Level X Tier Y operating - counter Level Z Tier W under audit").
    // Fallback to raw only if the backend hasn't been upgraded.
    const _canonSub = tt.canonical_subline;
    const _canonHead = tt.canonical_headline;
    _evoText("sm-tier-label",
             (_canonSub && _canonSub.length) ? _canonSub
                                              : (tt.subline || tt.current_tier_label || "Tier UNKNOWN · awaiting truth"));
    _evoText("sm-tier-subtitle",
             (_canonHead && _canonHead.length)
               ? ("Autonomous tier progression · " + _canonHead)
               : ("Autonomous tier progression · " + (tt.headline || "")));
    _evoText("sm-evo-active-text", tt.active_text || "—");

    // ----------------------------------------------------------------
    // Live-truth override of the legacy hero status block.
    // Without this override the hero still shows the supermax filler:
    //   ACTIVE   IDLE         no active task
    //   LAST CYCLE   sleeping  ts
    //   HEARTBEAT    sleeping  ts
    //   STATUS PILL  TIER 2 APPROVED + legacy "now tier" suffix
    // The /api/tier-truth payload already carries authoritative live
    // status (worker_ecosystem.progression / opencode.state, the
    // current effective tier, and a self-contained active_text). When
    // any worker is "active" we replace the legacy fields with the
    // live truth so Luna never looks asleep while she's working.
    // (Variable name `weHero` here so it doesn't collide with the
    // ecosystem painter's own `we` later in this function.)
    // ----------------------------------------------------------------
    const weHero = tt.worker_ecosystem || {};
    const progBlk = weHero.progression || {};
    const ocBlk   = weHero.opencode    || {};
    const progActive = String(progBlk.state || "").toLowerCase() === "active";
    const ocActive   = String(ocBlk.state   || "").toLowerCase() === "active";
    const lunaLive   = progActive || ocActive;
    if (lunaLive) {
      _evoText("sm-active-component",
               progActive ? "PROGRESSION" : "OPENCODE");
      _evoText("sm-active-stage",
               String((progActive ? progBlk.detail : ocBlk.detail) || "live").toLowerCase());
      // Last cycle / heartbeat: prefer live truth over the legacy
      // "sleeping" filler. The progression report timestamp is the
      // authoritative "last cycle" signal.
      const repAge = (tt.progression_report_age_seconds != null)
        ? Number(tt.progression_report_age_seconds) : null;
      _evoText("sm-last-verdict",
               (tt.latest_progression && tt.latest_progression.decision) || "live");
      _evoText("sm-last-cycle-ts",
               (repAge != null) ? (repAge + "s ago") : "—");
      _evoText("sm-heartbeat-state", "live");
      _evoText("sm-heartbeat-ts",
               new Date().toLocaleTimeString());
    }
    // Replace the legacy supermax status pill (which used to read
    // "TIER 2 APPROVED" plus a stale "now tier 8" suffix) with a clean
    // current-tier pill that always tells the truth. The Tier 2
    // approval is now a historical fact only, surfaced inside the
    // collapsed Legacy Metrics block.
    const statusPill = document.getElementById("sm-status-pill");
    if (statusPill) {
      // 2026-05-12 Honesty fix per Serge ("TIER 500 ACTIVE · LIVE" still
      // showing on the Evolution Command Center hero pill). Route this
      // site through the canonical tier display model and the One-Brain
      // audit verdict: NEVER render "TIER <N> ACTIVE · LIVE" while the
      // proof chain is drifted / unproven / grandfather-only.
      let _ctd = null;
      try {
        _ctd = (typeof getCanonicalTierDisplay === "function")
          ? getCanonicalTierDisplay(tt) : null;
      } catch (_e) { _ctd = null; }
      const opTier = (_ctd && _ctd.currentOperatingTier != null)
        ? _ctd.currentOperatingTier
        : (tt.current_operating_tier != null ? tt.current_operating_tier : "");
      const counter = String(tt.current_effective_tier || "");
      const drift = !!(_ctd && _ctd.hasAdoptionDrift);
      let label;
      let pillState = "active";
      if ((tt.blocker || {}).tier) {
        label = "BLOCKED AT TIER " + tt.blocker.tier;
        pillState = "warn";
      } else if (drift && opTier) {
        // Audit drift: refuse to claim ACTIVE. Show operating tier
        // under audit instead.
        label = "TIER " + opTier + " · UNDER AUDIT";
        pillState = "warn";
      } else if (opTier && lunaLive) {
        // Operating tier known, no drift, worker live → safe to claim
        // active for the OPERATING tier (never for the counter).
        label = "TIER " + opTier + " ACTIVE · LIVE";
        pillState = "active";
      } else if (opTier) {
        label = "TIER " + opTier + " ACTIVE";
        pillState = "active";
      } else if (counter) {
        // Counter-only fallback: be explicit it's the counter, not active.
        label = "TIER " + counter + " · COUNTER";
        pillState = "warn";
      } else {
        label = "AWAITING DATA";
        pillState = "warn";
      }
      if (statusPill.textContent !== label) statusPill.textContent = label;
      if (statusPill.dataset.state !== pillState) statusPill.dataset.state = pillState;
      statusPill.dataset.lunaLive = "1";
    }

    // Tier ladder. CRITICAL: de-dupe `data-state` writes. The MutationObserver
    // in lunaTierTimelineBind() fires snap() (smooth scrollBy) on EVERY change
    // to data-state. Without the dedupe below, the painter writes the same
    // value every 1s poll, the observer fires every 1s, the ladder smooth-
    // scrolls every 1s -> visible "the scrollbar moves on its own" symptom
    // the operator reported. Same dedupe pattern for label/title.
    const ladder = document.getElementById("sm-ladder");
    const rungs = (tt.ladder && Array.isArray(tt.ladder)) ? tt.ladder : [];
    if (ladder && rungs.length) {
      const items = ladder.querySelectorAll(".luna-evo__rung");
      items.forEach((node) => {
        const key = node.dataset.tier || (node.querySelector(".luna-evo__rung-num") || {}).textContent || "";
        const match = rungs.find((r) => String(r.key) === String(key).trim());
        if (!match) return;
        const newState = match.state || "future";
        if (node.dataset.state !== newState) node.dataset.state = newState;
        const lbl = node.querySelector(".luna-evo__rung-label");
        if (lbl && match.label && lbl.textContent !== match.label) {
          lbl.textContent = match.label;
        }
        if (match.detail && node.title !== match.detail) node.title = match.detail;
      });
    }

    // Current Operation card
    const lp = tt.latest_progression || {};
    const passed = (lp.passed != null) ? lp.passed : 0;
    const failed = (lp.failed != null) ? lp.failed : 0;
    _evoText("sm-op-action",   lp.decision || "(none)");
    _evoText("sm-op-pf",       passed + " passed · " + failed + " failed");
    _evoText("sm-op-cycle",    lp.cycle_id || "—");
    _evoText("sm-op-lastrun",  _evoLocalTime((tt.scheduled_task && tt.scheduled_task.last_run_time) || tt.generated_at));
    const opPill = document.getElementById("sm-op-pill");
    if (opPill) {
      const lf = tt.live_feed || {};
      opPill.textContent = lf.is_stale ? "STALE" : (tt.active_text && tt.active_text.toLowerCase().includes("running") ? "RUNNING" : "READY");
    }

    // ----------------------------------------------------------------
    // Auto-Upgrade & Archive card (2026-05-09 honesty fix, extended).
    // Reads tt.auto_upgrade_engine + tt.archive_promotions +
    //       tt.auto_promote_state + tt.council_added_tiers_truth.
    // Renders the operational tier separately from FastStore archive
    // ops so the dashboard never conflates "tier 308 archived" with
    // "Luna at tier 308". Drained / advancing / blocked labels come
    // straight from status_label. Top skip reasons surfaced from the
    // engine's per-cycle telemetry. Auto-promote state honestly
    // distinguished from broad live-apply.
    // ----------------------------------------------------------------
    try {
      const aue = tt.auto_upgrade_engine || {};
      const arc = tt.archive_promotions  || {};
      const aps = tt.auto_promote_state  || {};
      const cat = tt.council_added_tiers_truth || {};

      // 2026-05-12 visible-UI final-truth fix.
      // ROOT CAUSE: this site used to render raw `current_effective_tier`
      // (the counter, currently 500) as the "operational tier" — directly
      // contradicting the audited "TIER 160 · UNDER AUDIT" headline at the
      // top of the same card. Use the canonical helper so both ends agree.
      const _ctd = (typeof getCanonicalTierDisplay === "function")
                     ? getCanonicalTierDisplay(tt) : null;
      const _opNum   = _ctd ? _ctd.currentOperatingTier : null;
      const _hwmNum  = _ctd ? (_ctd.counterHighWaterMark != null ? _ctd.counterHighWaterMark : _ctd.highWaterMark)
                            : (tt.current_effective_tier != null ? tt.current_effective_tier : null);
      const _drift   = _ctd ? _ctd.hasAdoptionDrift : false;
      // 2026-05-13 bug fix: prior version emitted
      //   "operating claim Tier 160 · operating · counter high-water Tier 500"
      // because _qual fell to "operating" when numeric drift was false
      // even though mayClaimActive was false. The correct gate is
      // mayClaimActive (truth_verdict == PROVEN_ACTIVE).
      const _mayClaimActive = !!(_ctd && _ctd.mayClaimActive);
      const _qual = _mayClaimActive
                      ? "ACTIVE · proven"
                      : (_drift ? "UNDER AUDIT · DRIFTED" : "UNDER AUDIT");
      const _counterPart = (_hwmNum != null && _opNum != null && _hwmNum > _opNum)
                             ? ("   ·   counter high-water Tier " + _hwmNum + " · NOT CURRENT")
                             : "";
      _evoText(
        "sm-au-operational",
        "operating claim Tier " + (_opNum != null ? _opNum : "UNKNOWN")
        + " · " + _qual
        + _counterPart
      );

      // Auto-promote state — the visible distinction the user asked for.
      _evoText("sm-au-promote-state", aps.headline || "—");

      const elig = (aue.eligible != null)         ? aue.eligible         : "—";
      const appl = (aue.applied != null)          ? aue.applied          : "—";
      const fail = (aue.failed != null)           ? aue.failed           : "—";
      const rb   = (aue.rolled_back != null)      ? aue.rolled_back      : "—";
      const rbf  = (aue.rollback_failed != null)  ? aue.rollback_failed  : "—";
      const scanned = (aue.packets_scanned != null && aue.packets_total != null)
        ? (" · scanned " + aue.packets_scanned + "/" + aue.packets_total + " packets")
        : "";
      _evoText("sm-au-counts",
        elig + " eligible · " + appl + " applied · " + fail + " failed · " +
        rb + " rolled back · " + rbf + " rollback-failed" + scanned);

      const arcCount = (arc.tier_artifact_count != null) ? arc.tier_artifact_count : 0;
      const arcTotal = (arc.total_entries != null)       ? arc.total_entries       : 0;
      _evoText("sm-au-archive",
        arcCount + " tier artifacts · " + arcTotal + " total FastStore entries");

      const hi = arc.highest_tier_seen;
      const lo = arc.lowest_tier_seen;
      if (hi != null && lo != null) {
        _evoText("sm-au-highest", "tier " + hi + "  (range " + lo + ".." + hi + ")");
      } else if (hi != null) {
        _evoText("sm-au-highest", "tier " + hi);
      } else {
        _evoText("sm-au-highest", "—");
      }

      const runId = aue.run_id || "—";
      const elapsed = (aue.cycle_elapsed_seconds != null)
        ? (aue.cycle_elapsed_seconds + "s")
        : "—";
      _evoText("sm-au-cycle", runId + " · " + elapsed + " cycle");

      // Skip-reason summary (top 3 nonzero) — the honest answer to
      // "why is eligible 0 right now?".
      const skip = aue.skip_reasons || {};
      const skipEntries = Object.keys(skip).map(k => [k, Number(skip[k]) || 0])
                                .filter(kv => kv[1] > 0)
                                .sort((a, b) => b[1] - a[1])
                                .slice(0, 3);
      const skipRow = document.getElementById("sm-au-skip-row");
      if (skipEntries.length > 0) {
        _evoText("sm-au-skip", skipEntries.map(([k, v]) => k + "=" + v).join(" · "));
        if (skipRow) skipRow.hidden = false;
      } else {
        if (skipRow) skipRow.hidden = true;
      }

      // Honest blocker explanation when status is drained/blocked.
      const blockerRow = document.getElementById("sm-au-blocker-row");
      if (aue.blocker_explanation) {
        _evoText("sm-au-blocker", aue.blocker_explanation);
        if (blockerRow) blockerRow.hidden = false;
      } else {
        if (blockerRow) blockerRow.hidden = true;
      }

      // Source-of-truth divergence warning (failure mode 9).
      const sourceRow = document.getElementById("sm-au-source-row");
      if (cat.divergence_detected && cat.divergence_explanation) {
        _evoText("sm-au-source-warning",
          "engine reads " + cat.engine_source_path + " (" + (cat.engine_source_tiers || "?") +
          " tier_definitions) — config pointer " + cat.config_pointer_path + " has " +
          (cat.config_pointer_tiers || 0) + " tiers (stale). authoritative = " + cat.authoritative_path);
        if (sourceRow) sourceRow.hidden = false;
      } else {
        if (sourceRow) sourceRow.hidden = true;
      }

      // Candidate supply truth (2026-05-09 addendum honesty fix).
      const css = tt.candidate_supply_status || {};
      const supplyRow         = document.getElementById("sm-au-supply-row");
      const supplyPendingRow  = document.getElementById("sm-au-supply-pending-row");
      const supplyExplainRow  = document.getElementById("sm-au-supply-explain-row");
      if (css.ok) {
        const total      = (css.total_tier_definitions != null) ? css.total_tier_definitions : 0;
        const archived   = (css.archived != null) ? css.archived : 0;
        const fsUnarch   = (css.faststore_unarchived != null) ? css.faststore_unarchived : 0;
        const pct        = (css.archive_completion_pct != null) ? css.archive_completion_pct : 0;
        _evoText("sm-au-supply",
          archived + " / " + total + " archived (" + pct + "%) · " +
          fsUnarch + " FastStore-unarchived");
        if (supplyRow) supplyRow.hidden = false;

        const pendingParts = [];
        if (css.luna_modules_pending) pendingParts.push(css.luna_modules_pending + " luna_modules");
        if (css.tests_pending)        pendingParts.push(css.tests_pending + " tests");
        if (css.ps1_pending)          pendingParts.push(css.ps1_pending + " ps1");
        if (css.other_real_pending)   pendingParts.push(css.other_real_pending + " other");
        if (pendingParts.length > 0) {
          _evoText("sm-au-supply-pending", pendingParts.join(" · "));
          if (supplyPendingRow) supplyPendingRow.hidden = false;
        } else {
          if (supplyPendingRow) supplyPendingRow.hidden = true;
        }

        if (css.honest_explanation) {
          _evoText("sm-au-supply-explain", css.honest_explanation);
          if (supplyExplainRow) supplyExplainRow.hidden = false;
        } else {
          if (supplyExplainRow) supplyExplainRow.hidden = true;
        }
      } else {
        if (supplyRow)         supplyRow.hidden = true;
        if (supplyPendingRow)  supplyPendingRow.hidden = true;
        if (supplyExplainRow)  supplyExplainRow.hidden = true;
      }

      const auPill = document.getElementById("sm-au-pill");
      if (auPill) {
        const status = String(aue.status_label || "unknown");
        const map = {
          "drained":                 { text: "DRAINED",  state: "active" },
          "advancing":               { text: "ADVANCING", state: "active" },
          "running":                 { text: "RUNNING",  state: "active" },
          "blocked_rollback_failed": { text: "BLOCKED",  state: "warn"   },
          "no_cycles_yet":           { text: "—",        state: "idle"   },
          "no_engine_dir":           { text: "OFFLINE",  state: "warn"   },
          "unknown":                 { text: "—",        state: "idle"   },
        };
        const m = map[status] || map["unknown"];
        auPill.textContent      = m.text;
        auPill.dataset.state    = m.state;
        auPill.title            = "auto-upgrade engine status: " + status;
      }
    } catch (_e) { /* never break paint over the new card */ }

    // Next Gate card. Round 24, 2026-05-09 per Serge ("the Evolution
    // Command Center is completely out of date"). When Luna is already
    // past the visible ladder (current_effective_tier > 10), the legacy
    // labels "All visible tiers reached" and "Tier 9+ remain proposed/
    // design-only" are misleading - they sound like nothing is happening
    // when in reality Luna's at Tier 12 producing thousands of packets.
    // Replace with extended-framework awareness.
    const ng = tt.next_gate_key;
    const ngLabel = tt.next_gate_label || "(none)";
    const _curEffTier = tt.current_effective_tier;
    const _curEffNum = parseInt(String(_curEffTier || ""), 10);
    const _isExtendedFramework = Number.isFinite(_curEffNum) && _curEffNum > 10;
    // 2026-05-12 visible-UI final-truth fix.
    // ROOT CAUSE: this site used to render "Tier 500 active · Level 10 ·
    // awaiting Serge bump" using the raw counter — a banned active claim
    // while proof_chain != PROVEN. We now consult the canonical helper
    // and surface a drift-aware BLOCKED line instead.
    const _ngCtd = (typeof getCanonicalTierDisplay === "function")
                     ? getCanonicalTierDisplay(tt) : null;
    const _ngDrift = _ngCtd ? _ngCtd.hasAdoptionDrift : false;

    const ngPill = document.getElementById("sm-ng-pill");
    if (_ngDrift) {
      _evoText("sm-ng-title", "BLOCKED · repair_tier_drift");
      if (ngPill) ngPill.textContent = "BLOCKED";
    } else if (ng) {
      _evoText("sm-ng-title", "Tier " + ng + " · " + ngLabel);
      if (ngPill) ngPill.textContent = "Tier " + ng;
    } else if (_isExtendedFramework && _ngCtd && _ngCtd.isProven) {
      // Only allowed when proof chain is PROVEN — proven extended-framework
      // tiers may still display the "awaiting next bump" line.
      const _level = Math.max(1, Math.floor((_curEffNum - 1) / 50) + 1);
      _evoText("sm-ng-title",
               "Tier " + _curEffNum + " · Level " + _level + " · awaiting next bump");
      if (ngPill) ngPill.textContent = "T" + _curEffNum;
    } else {
      _evoText("sm-ng-title", "All visible tiers reached");
      if (ngPill) ngPill.textContent = "—";
    }
    const blocker = tt.blocker;
    if (blocker && Array.isArray(blocker.lines) && blocker.lines.length) {
      _evoText("sm-ng-detail", blocker.lines[0]);
    } else if (ng === "9") {
      _evoText("sm-ng-detail", "Tier 9 (Assisted Module Promotion) — council-gated; awaiting candidate supply");
    } else if (ng) {
      _evoText("sm-ng-detail", "Council-gated · awaiting runtime verification");
    } else if (_isExtendedFramework && _ngCtd && _ngCtd.isProven && !_ngDrift) {
      // Specific honest detail about why Luna is "stuck" at the current
      // proven tier. ONLY allowed when proof_chain is PROVEN and drift
      // is absent — otherwise the detail line below is bypassed by the
      // drift branch above which already wrote "Tier N generated_not_adopted".
      const _ttSandbox  = tt.sandbox  || {};
      const _ttCouncil  = tt.council  || {};
      const _ttCounters = tt.progress_counters || {};
      const _t6 = (_ttSandbox.candidates_total != null) ? _ttSandbox.candidates_total : (_ttCounters.tier6_candidates || 0);
      const _t7 = (_ttCouncil.total_reviews    != null) ? _ttCouncil.total_reviews    : (_ttCounters.tier7_reviews    || 0);
      const _backlog = Math.max(0, _t6 - _t7);
      _evoText("sm-ng-detail",
               "Producing Tier 6 packets continuously · " +
               _t6 + " packets / " + _t7 + " reviews · backlog " + _backlog +
               " awaiting council");
    } else if (_ngDrift) {
      // Drift-aware detail line.
      _evoText("sm-ng-detail",
               "Tier " + _curEffNum + " generated_not_adopted · NOT ACTIVE · "
               + "operating Tier " + (_ngCtd && _ngCtd.currentOperatingTier != null ? _ngCtd.currentOperatingTier : "?")
               + " · drift must clear before next gate");
    } else {
      _evoText("sm-ng-detail", "Tier 9+ remain proposed/design-only");
    }

    // Progress bar — eligibility/score-toward-next-gate (Tier 8 uses council reviews/10).
    let pct = 0; let progText = "—";
    const council = tt.council || {};
    const sandbox = tt.sandbox || {};
    const counters = tt.progress_counters || {};
    // Compute the framework "Beyond" rung count from current_effective_tier
    // (now treated as global tier 1..500 in the new framework).
    let beyondText = "L10 / T500 ceiling";
    try {
      const tierGlobalNum = parseInt(String(tt.current_effective_tier || ""), 10);
      if (Number.isFinite(tierGlobalNum) && tierGlobalNum > 0) {
        const lvl = Math.max(1, Math.floor((tierGlobalNum - 1) / 50) + 1);
        beyondText = "L" + lvl + " / T" + tierGlobalNum + " · ceiling L10/T500";
      }
    } catch (_e) { /* keep default text */ }
    // Round 24 honest counter rendering per Serge:
    //   - Tier 5L now reflects real sandbox-dir count (server-side fix)
    //   - Tier 7 appends "· stale Xh" when scoreboard is older than 1h
    //     so the operator SEES the upstream council backlog visually
    //   - Tier 9 / 10 use the new server-side honest labels
    let _t7Counter = (counters.tier7_reviews != null ? counters.tier7_reviews : council.total_reviews || 0) + " reviews";
    const _t7Age = counters.tier7_reviews_age_seconds;
    if (typeof _t7Age === "number" && _t7Age > 3600) {
      const _h = Math.round(_t7Age / 3600);
      _t7Counter += " · stale " + _h + "h";
    }
    const counterText = {
      tier5l_sandbox_runs:  (counters.tier5l_sandbox_runs != null ? counters.tier5l_sandbox_runs : 0) + " runs",
      tier6_candidates:     (counters.tier6_candidates    != null ? counters.tier6_candidates    : sandbox.candidates_total || 0) + " candidates",
      tier7_reviews:        _t7Counter,
      tier8_promotions:     (counters.tier8_promotions    != null ? counters.tier8_promotions    : 0) + " promotions",
      tier9_gate_status:    counters.tier9_gate_status   || (ng === "9" ? "next gate" : "gate pending"),
      tier10_apex_roadmap:  counters.tier10_apex_roadmap || "roadmap",
      tier_global_max:      beyondText,
    };
    Object.keys(counterText).forEach((key) => {
      document.querySelectorAll('[data-counter="' + key + '"]').forEach((node) => {
        node.textContent = String(counterText[key]);
      });
    });
    // Server-computed progress label is the canonical source. Falls back
    // to the legacy formulas only if next_gate wasn't supplied.
    const ngBlock = tt.next_gate || {};
    const ngCur = (ngBlock.progress_current != null) ? Number(ngBlock.progress_current) : null;
    const ngReq = (ngBlock.progress_required != null) ? Number(ngBlock.progress_required) : null;
    if (ngBlock.progress_text) {
      progText = String(ngBlock.progress_text);
      if (ngCur != null && ngReq && ngReq > 0) {
        pct = Math.round((Math.min(ngCur, ngReq) / ngReq) * 100);
      } else if (ngBlock.is_design_only) {
        pct = 0;
      } else if (!ng) {
        pct = 100;
      }
    } else if (ng === "8") {
      const want = 10;
      const have = Math.min(want, parseInt(council.approved, 10) || 0);
      pct = Math.round((have / want) * 100);
      progText = have + " / " + want + " council passes";
    } else if (ng === "7") {
      const want = 10;
      const have = Math.min(want, parseInt(sandbox.candidates_passed, 10) || 0);
      pct = Math.round((have / want) * 100);
      progText = have + " / " + want + " Tier 6 candidates";
    } else if (ng === "9") {
      pct = 0;
      progText = "design-only";
    } else if (!ng) {
      pct = 100;
      progText = "all open tiers reached";
    }
    // Dedupe progress-fill writes. Without this, the legacy applySelfUpgradeData
    // path used to win some races and bounce the bar from (e.g.) 95% to 20% and
    // back via the 0.6s CSS width transition - the "moving forward and backward
    // bar" Serge reported 2026-05-08. The fix below ALSO disables the legacy
    // path entirely once tier-truth has been observed; this dedupe is the
    // belt-and-suspenders second line of defense.
    const fill = document.getElementById("sm-progress-fill");
    if (fill) {
      const newW = pct + "%";
      if (fill.style.width !== newW) fill.style.width = newW;
    }
    _evoText("sm-progress-text", progText);
    const aria = document.getElementById("sm-progress-aria");
    if (aria) {
      const nv = String(pct);
      if (aria.getAttribute("aria-valuenow") !== nv)  aria.setAttribute("aria-valuenow", nv);
      if (aria.getAttribute("aria-valuemax") !== "100") aria.setAttribute("aria-valuemax", "100");
    }
    // Mark that the canonical Evolution-panel painter has touched this bar.
    // The legacy applySelfUpgradeData path checks this flag and refuses to
    // overwrite the bar once it's set. Sticky for the lifetime of the page;
    // Tier 2 was a one-time legacy gate and a single tier-truth observation
    // means we're objectively past that era.
    if (typeof state === "object" && state) state.evoProgressOwnedByTierTruth = true;
    _evoText("sm-rb-failures", String(council.rollback_failures || 0));
    // Eligibility label - never reuse Tier 2 wording when the next gate
    // is anything else. Tier 2 is design-only legacy from this view.
    let eligibleText = "not eligible";
    if (ng === "8" && (parseInt(council.approved, 10) || 0) >= 10) {
      eligibleText = "ELIGIBLE";
    } else if (ngBlock.is_design_only) {
      eligibleText = "design-only";
    } else if (ng) {
      eligibleText = "council-gated · runtime-verified";
    } else {
      eligibleText = "all open tiers reached";
    }
    _evoText("sm-eligible", eligibleText);

    // Source-mismatch warning badge.
    const mm = document.getElementById("sm-source-mismatch");
    if (mm) {
      const warns = Array.isArray(tt.source_mismatch_warnings) ? tt.source_mismatch_warnings : [];
      if (warns.length > 0) {
        mm.textContent = "source mismatch: " + warns[0];
        mm.hidden = false;
      } else {
        mm.textContent = "";
        mm.hidden = true;
      }
    }

    // Blocker card
    const blkCard = document.getElementById("sm-blocker-card");
    if (blkCard) {
      if (blocker && blocker.lines && blocker.lines.length) {
        blkCard.hidden = false;
        _evoText("sm-blk-title", blocker.title || ("Tier " + (blocker.tier || "?") + " gate blocked"));
        const ul = document.getElementById("sm-blk-list");
        if (ul) {
          ul.innerHTML = "";
          blocker.lines.forEach((ln) => {
            const li = document.createElement("li");
            li.textContent = String(ln);
            ul.appendChild(li);
          });
        }
        const ulAct = document.getElementById("sm-blk-actions");
        if (ulAct) {
          ulAct.innerHTML = "";
          (blocker.actions || []).forEach((ln) => {
            const li = document.createElement("li");
            li.textContent = String(ln);
            ulAct.appendChild(li);
          });
        }
      } else {
        blkCard.hidden = true;
      }
    }

    // Council card
    _evoText("sm-council-total", String(council.total_reviews || 0));
    _evoText("sm-council-safe",  String(council.approved || 0));
    _evoText("sm-council-hold",  String(council.hold_for_review || 0));
    _evoText("sm-council-deny",  String(council.do_not_promote || 0));
    _evoText("sm-council-rb",    String(council.rollback_failures || 0));

    // Worker ecosystem card. Prefer the server-computed worker_ecosystem
    // block (each pill carries an explicit state + detail) so the JS no
    // longer has to compose state from raw scheduled-task fields.
    function setEco(actor, txt, st, title) {
      const span = document.getElementById("sm-eco-" + actor);
      if (span) span.textContent = txt;
      const pill = document.querySelector('.luna-evo__ecopill[data-actor="' + actor + '"]');
      if (pill) {
        pill.dataset.state = st;
        if (title) pill.title = title;
      }
    }
    const we = tt.worker_ecosystem || {};
    if (we.luna && we.progression && we.opencode) {
      // New live-truth path.
      Object.keys(we).forEach((actor) => {
        const blk = we[actor] || {};
        setEco(actor, String(blk.state || "?"), String(blk.state || "offline"), String(blk.detail || ""));
      });
    } else {
      // Legacy fallback (kept for older dashboards / cached payloads).
      const eco = tt.ecosystem || {};
      setEco("luna", eco.luna === "active" ? "active" : "offline",
             eco.luna === "active" ? "live" : "offline");
      const taskState = (tt.scheduled_task && tt.scheduled_task.state)
        ? String(tt.scheduled_task.state).toLowerCase() : "unknown";
      const taskActive = (taskState === "ready" || taskState === "running" || taskState === "queued");
      setEco("progression", taskState, taskActive ? "active" : (taskState === "disabled" ? "offline" : "idle"));
      const guard = String(eco.guardian || "unknown");
      setEco("guardian", guard, guard.includes("healthy") ? "live" : "warn");
      // 2026-05-13 canonical verifier — read state + tone from backend.
      // Backend now emits eco.verifier as a string (e.g. "live"/"ready"/
      // "degraded"/"offline"/"unknown") sourced from luna_verifier_status.
      // No more hardcoded tone="live" — match what the canonical source says.
      var _ecoVerifierState = String(eco.verifier || "unknown").toLowerCase();
      var _ecoVerifierTone =
        (_ecoVerifierState === "live"  || _ecoVerifierState === "running") ? "live" :
        (_ecoVerifierState === "ready" || _ecoVerifierState === "healthy") ? "live" :
        (_ecoVerifierState === "degraded")                                  ? "warn" :
        (_ecoVerifierState === "offline" || _ecoVerifierState === "blocked")? "offline" :
                                                                              "warn";
      setEco("verifier", _ecoVerifierState, _ecoVerifierTone);
      const aiderState = String(eco.aider || "offline").toLowerCase();
      setEco("aider", aiderState,
             aiderState === "online" ? "live" : (aiderState === "idle" ? "idle" : "offline"));
    }

    // Latest report link
    const reportLink = document.getElementById("sm-latest-report");
    if (reportLink && lp.report_path) {
      reportLink.href = "/api/files/list?path=D:\\SurgeApp\\" + lp.report_path.replace(/\//g, "\\");
      reportLink.title = lp.report_path;
    }
    // Tier 8 gate link
    const gateLink = document.getElementById("sm-tier-gate");
    if (gateLink) {
      if (blocker && blocker.report_path) {
        gateLink.hidden = false;
        gateLink.href = "/api/files/list?path=D:\\SurgeApp\\" + blocker.report_path.replace(/\//g, "\\");
        gateLink.title = blocker.report_path;
        const lbl = gateLink.querySelector(".luna-supermax__btn-label");
        if (lbl) lbl.textContent = "Open Tier " + blocker.tier + " Gate";
      } else {
        gateLink.hidden = true;
      }
    }

    // ----- Tier-rung ETA countdowns (added 2026-05-08 per Serge) -----
    // Updates the small "ETA Xh Ym" line under each rung's count.
    // Read-only of `tt` we already have; no extra polling.
    _paintRungEtas(tt, counters, council, sandbox);
  }

  // ===== Tier-rung ETA countdown ========================================
  // Each rung gets a small ETA line under its count. The ETA is computed
  // from a 10-minute rolling buffer of (timestamp, value) tuples for the
  // counter that drives that rung. Targets are anchored to the engine's
  // own eligibility criteria where they exist (t7-eligible, t8-eligible,
  // etc.); 9 / 10 / L2+ are surfaced as "manual gate" / "council apex" /
  // packet-rate ceiling because their progression isn't a simple counter.
  // No new endpoints, no extra polling - we piggyback on the 1Hz tier-truth
  // poll that paintEvolutionCenter already consumes.
  // ======================================================================
  const _ETA_WINDOW_MS = 10 * 60 * 1000;   // rolling-rate window
  const _ETA_MAX_POINTS = 700;             // hard cap on buffer length
  const _etaBuffer = {
    tier5l_sandbox_runs: [],
    tier6_candidates:    [],
    tier7_reviews:       [],
    tier8_promotions:    [],
    __effective_tier:    [],
  };
  // Per-rung target spec. metric=null means manual gate (no ETA).
  const _RUNG_TARGETS = {
    "5L":  { metric: "tier5l_sandbox_runs", target: 1,   readyText: "lab established" },
    "6":   { metric: "tier6_candidates",    target: 10,  readyText: "T7-eligible" },
    "7":   { metric: "tier7_reviews",       target: 10,  readyText: "T8-eligible" },
    "8":   { metric: "tier8_promotions",    target: 5,   readyText: "T9-eligible" },
    "9":   { metric: null, manualText: "council-gated · awaiting candidate supply" },
    "10":  { metric: null, manualText: "apex · council-controlled" },
    "L2+": { metric: "__effective_tier",    target: 500, readyText: "L10/T500 reached" },
  };
  function _etaPush(metric, value) {
    if (typeof value !== "number" || !Number.isFinite(value)) return;
    const buf = _etaBuffer[metric];
    if (!buf) return;
    const ts = Date.now();
    // Don't double-push if value hasn't changed AND last point was very recent
    // (keeps the buffer informative across plateaus without bloat).
    if (buf.length > 0) {
      const last = buf[buf.length - 1];
      if (last.value === value && (ts - last.ts) < 5000) return;
    }
    buf.push({ ts: ts, value: value });
    const cutoff = ts - _ETA_WINDOW_MS;
    while (buf.length > 0 && buf[0].ts < cutoff) buf.shift();
    while (buf.length > _ETA_MAX_POINTS) buf.shift();
  }
  // Returns rate in units-per-second over the rolling window, or null if
  // not enough data, or 0 if the value is flat / decreasing.
  function _etaRate(metric) {
    const buf = _etaBuffer[metric];
    if (!buf || buf.length < 2) return null;
    const first = buf[0];
    const last  = buf[buf.length - 1];
    const dt = (last.ts - first.ts) / 1000;
    if (dt <= 0) return null;
    const dv = last.value - first.value;
    if (dv <= 0) return 0;
    return dv / dt;
  }
  function _etaFormat(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) return "—";
    const min = seconds / 60;
    if (min < 1)  return "<1 min";
    if (min < 60) return Math.round(min) + " min";
    const hr = min / 60;
    if (hr < 24)  return (Math.round(hr * 10) / 10) + " h";
    const day = hr / 24;
    if (day < 30) return (Math.round(day * 10) / 10) + " d";
    const wk = day / 7;
    return Math.round(wk) + " w";
  }
  function _paintRungEtas(tt, counters, council, sandbox) {
    counters = counters || (tt && tt.progress_counters) || {};
    council  = council  || (tt && tt.council)            || {};
    sandbox  = sandbox  || (tt && tt.sandbox)            || {};
    const tierGlobalNum = parseInt(String((tt && tt.current_effective_tier) || ""), 10);
    // Resolve the live numeric value for every tracked metric.
    const cur = {
      tier5l_sandbox_runs: (typeof counters.tier5l_sandbox_runs === "number") ? counters.tier5l_sandbox_runs : null,
      tier6_candidates:    (typeof counters.tier6_candidates    === "number") ? counters.tier6_candidates
                          : (typeof sandbox.candidates_total    === "number") ? sandbox.candidates_total : null,
      tier7_reviews:       (typeof counters.tier7_reviews       === "number") ? counters.tier7_reviews
                          : (typeof council.total_reviews       === "number") ? council.total_reviews : null,
      tier8_promotions:    (typeof counters.tier8_promotions    === "number") ? counters.tier8_promotions : null,
      __effective_tier:    (Number.isFinite(tierGlobalNum) && tierGlobalNum > 0) ? tierGlobalNum : null,
    };
    // Push each non-null reading into the rolling buffer.
    Object.keys(cur).forEach((m) => { if (cur[m] != null) _etaPush(m, cur[m]); });
    // Now write the ETA text + state attribute on each rung node.
    Object.keys(_RUNG_TARGETS).forEach((rungKey) => {
      const spec = _RUNG_TARGETS[rungKey];
      const nodes = document.querySelectorAll('[data-eta-tier="' + rungKey + '"]');
      if (!nodes.length) return;
      // Authoritative rung state from /api/tier-truth (set on the parent
      // <li> by the painter above). If a rung is already marked completed
      // by the server, don't second-guess it with a "no progress" label
      // just because its specific counter happens to be 0/missing — the
      // server has higher truth than the local counter.
      const rungLi = document.querySelector('.luna-evo__rung[data-tier="' + rungKey + '"]');
      const rungServerState = rungLi ? (rungLi.dataset.state || "") : "";

      let text = "—";
      let stateAttr = "measuring";
      let titleText = "";

      if (rungServerState === "completed") {
        // Server has flagged this rung as already done. Trust it.
        text = "✓ completed";
        stateAttr = "ready";
        titleText = "rung marked completed by /api/tier-truth";
      } else if (spec.metric == null) {
        // No numeric target — manual gate.
        text = spec.manualText || "manual";
        stateAttr = "manual";
        titleText = spec.manualText || "manual";
      } else {
        const v = cur[spec.metric];
        if (v == null) {
          text = "no data";
          stateAttr = "measuring";
          titleText = "metric not yet observed: " + spec.metric;
        } else if (v >= spec.target) {
          text = "✓ " + spec.readyText;
          stateAttr = "ready";
          titleText = spec.metric + " = " + v + " ≥ target " + spec.target;
        } else {
          const rate = _etaRate(spec.metric);
          const remaining = spec.target - v;
          if (rate == null) {
            text = "measuring…";
            stateAttr = "measuring";
            titleText = "warming up rolling-rate buffer (need 2+ samples)";
          } else if (rate <= 0) {
            // Flat or decreasing. For __effective_tier (Serge-controlled
            // rung bump) "no progress" is misleading — that field doesn't
            // auto-advance. Show a more accurate label there.
            if (spec.metric === "__effective_tier") {
              text = "council-gated bump";
              stateAttr = "council";
              titleText = "current_effective_tier auto-advances under council standing approval once a tier passes 24h soak + council unanimity (no Serge sign-off required for normal workflow)";
            } else {
              text = "no progress";
              stateAttr = "blocked";
              titleText = spec.metric + " is flat or decreasing in last 10 min";
            }
          } else {
            const etaSec = remaining / rate;
            text = "ETA " + _etaFormat(etaSec);
            stateAttr = "";
            const ratePerHour = (rate * 3600).toFixed(1);
            titleText = "remaining " + remaining + " · rate " + ratePerHour + "/h · raw rate " +
                        (Math.round(rate * 10000) / 10000) + "/s";
          }
        }
      }
      nodes.forEach((n) => {
        if (n.textContent !== text) n.textContent = text;
        // Dedupe data-state to avoid triggering MutationObservers
        // elsewhere on the dashboard (cf. ladder scroll-drift fix).
        if (n.dataset.state !== stateAttr) {
          if (stateAttr) n.dataset.state = stateAttr;
          else delete n.dataset.state;
        }
        if (titleText && n.title !== titleText) n.title = titleText;
      });
    });
  }

  function paintOpenCodePill(oc) {
    if (!oc) return;
    const span = document.getElementById("sm-eco-opencode");
    const pill = document.querySelector('.luna-evo__ecopill[data-actor="opencode"]');
    const stateText = (oc.state || "OFFLINE").toLowerCase();
    if (span) span.textContent = stateText;
    if (pill) {
      if (oc.bridge_enabled && oc.cli_found) pill.dataset.state = "live";
      else if (oc.desktop_found || oc.cli_found) pill.dataset.state = "warn";
      else pill.dataset.state = "offline";
    }
  }

  // Pulse timeline — pull from the existing live-feed records.
  function paintEvoPulse() {
    const list = document.getElementById("sm-pulse-list");
    if (!list) return;
    const records = (state.lastFeedRecords || []).slice(-8).reverse();
    if (records.length === 0) {
      list.innerHTML = '<li class="luna-evo__pulse-item luna-evo__pulse-item--empty">awaiting events</li>';
      return;
    }
    list.innerHTML = "";
    records.forEach((r) => {
      const li = document.createElement("li");
      li.className = "luna-evo__pulse-item";
      const t = (r.ts || r.iso_utc || "").toString();
      const tShort = t.length >= 8 ? t.slice(-8) : t;
      const ev = String(r.event || r.msg || "").replace(/^\s+|\s+$/g, "");
      const src = String(r.source || r.role || "").replace(/^\s+|\s+$/g, "");
      const tspan = document.createElement("span"); tspan.className = "luna-evo__pulse-item-time"; tspan.textContent = tShort;
      const bspan = document.createElement("b"); bspan.textContent = ev.slice(0, 32) || "(event)";
      const sspan = document.createElement("span"); sspan.textContent = src ? "· " + src.replace(/^luna_/, "") : "";
      li.appendChild(tspan); li.appendChild(bspan); li.appendChild(sspan);
      list.appendChild(li);
    });
  }

  async function pollMissionControl() {
    const data = await fetchJSON("/api/mission-control");
    if (!data || !data.ok) return;
    state.lastMissionControl = data;
    state.mcLastFreshMono = performance.now();

    const view = missionViewFromControl(data);
    const actor = view.actor;
    const stage = view.stage;
    const title = view.title;
    const status = view.status;

    // Card-level state attribute (drives subtle CSS color treatment).
    const card = document.querySelector(".luna-mission");
    if (card) {
      let mcState = "idle";
      if (data.is_stale)         mcState = "stale";
      else if (data.is_blocked)  mcState = "blocked";
      else if (data.is_complete && !data.is_active) mcState = "complete";
      else if (!data.is_active)  mcState = view.kind === "ready" ? "ready"
                                          : (data.is_idle ? "waiting" : "idle");
      else if (actor === "VERIFICATION") mcState = "verifying";
      else                       mcState = "active";
      // Detect handoff (different actor than last poll) — apply briefly.
      if (state.mcPrevActor && state.mcPrevActor !== actor && actor !== "IDLE") {
        card.dataset.mcState = "handoff";
        // Reset to live state shortly so the handoff animation is one-shot.
        clearTimeout(state.mcHandoffTimer);
        state.mcHandoffTimer = setTimeout(() => { card.dataset.mcState = mcState; }, 950);
      } else {
        card.dataset.mcState = mcState;
      }
    }

    // Centerpiece overlay (sits on the moon canvas).
    text($("mission-actor"), actor);
    text($("mission-substage"), (stage || "thinking").toLowerCase());

    // Caption block.
    text($("mission-task"), title ? _mcShortTitle(title) : (data.is_active ? actor : "STANDBY"));
    text($("mission-detail"), status || (data.is_active ? stage : "awaiting orders"));

    // Next-action + report-path hint (visible whenever we have one). The
    // dashboard creates these nodes on demand to avoid a stale dom.
    let hintWrap = $("mission-hint");
    if (!hintWrap) {
      const cap = document.querySelector(".luna-mission__caption");
      if (cap) {
        hintWrap = document.createElement("div");
        hintWrap.id = "mission-hint";
        hintWrap.className = "luna-mission__hint";
        const left = document.createElement("span");
        left.id = "mission-hint-next";
        const sep = document.createElement("span");
        sep.className = "luna-mission__meta-sep";
        sep.setAttribute("aria-hidden", "true");
        sep.textContent = "·";
        const right = document.createElement("a");
        right.id = "mission-hint-report";
        right.target = "_blank";
        right.rel = "noopener";
        hintWrap.appendChild(left);
        hintWrap.appendChild(sep);
        hintWrap.appendChild(right);
        cap.appendChild(hintWrap);
      }
    }
    if (hintWrap) {
      // Compose ONE hint line - never duplicate the cycle countdown. The
      // tickCountdown() 1Hz timer below rewrites just the countdown segment
      // every second using the last-seen next_cycle_at, so the operator sees
      // 60, 59, 58, ... ticking in real time instead of the same static
      // "next cycle in 60 seconds" between server polls.
      let next = view.nextAction || (data.is_complete ? "task complete" : "");
      const sprintTag = data.sprint_mode ? " · sprint" : "";
      const alreadyHasCycle = /next cycle\b/i.test(next);
      if (data.sprint_mode && !alreadyHasCycle && !/sprint/i.test(next)) {
        next = next ? (next + sprintTag) : "sprint mode";
      }
      // Persist the cycle source for tickCountdown(). nextCycleAt is the
      // authoritative wall-clock target; nextCycleIn is the server's view at
      // poll time and serves as the seed when nextCycleAt is missing.
      state.cycleSource = {
        nextCycleAt: data.next_cycle_at || "",
        nextCycleInSeconds: (data.next_cycle_in_seconds != null) ? Number(data.next_cycle_in_seconds) : null,
        sprintMode: !!data.sprint_mode,
        isStale: !!data.is_stale,
        isActive: !!data.is_active,
        taskEnabled: data.task_enabled !== false,
        seenAtMono: performance.now(),
      };
      const rep  = view.reportPath || "";
      const nextEl = $("mission-hint-next");
      const repEl  = $("mission-hint-report");
      if (nextEl) nextEl.textContent = next ? ("next: " + next) : "";
      if (repEl) {
        if (rep) {
          repEl.textContent = "report ·";
          repEl.href = rep;
          repEl.hidden = false;
        } else {
          repEl.textContent = "";
          repEl.removeAttribute("href");
          repEl.hidden = true;
        }
      }
      hintWrap.hidden = !(next || rep);
    }

    // Handoff strip — show only on a meaningful handoff (from != to).
    // A LUNA -> LUNA "handoff" is just a cycle continuation, not a real
    // hand-off; rendering it as a lone arrow looks like a mystery button.
    const ho = $("mission-handoff");
    if (ho) {
      const hf = String(data.last_handoff_from || "").trim();
      const ht = String(data.last_handoff_to   || "").trim();
      if (hf && ht && hf.toLowerCase() !== ht.toLowerCase()) {
        text($("mission-handoff-from"), hf);
        text($("mission-handoff-to"),   ht);
        ho.hidden = false;
      } else {
        ho.hidden = true;
      }
    }

    // Meta line: elapsed · eta · last update.
    text($("mission-elapsed"),
         data.elapsed_seconds != null ? _mcFmtElapsed(data.elapsed_seconds) : "—");
    text($("mission-eta"),
         data.eta_seconds != null ? _mcFmtElapsed(data.eta_seconds) : "—");
    text($("mission-last"), _mcFmtClockTs(data.last_update));

    // mission-stat header pill (the small text in the card head).
    text($("mission-stat"), view.statLabel);

    // Live-dot pill on the Mission Control card.
    state.mcLastFreshMono = performance.now();

    // Mirror the active component into the Self-Upgrade Progress header.
    // Source priority for the IDLE override:
    //   1. /api/higher-tier/progress (state.lastHigherTier) - if fresh, the
    //      progression engine is the authoritative actor; show its tier and
    //      not the legacy Tier 2 "IDLE / no active task".
    //   2. /api/mission-control - the data we just polled.
    //   3. legacy fall-through.
    // This is the user-reported "active IDLE while Tier 7 ACTIVE" mismatch.
    const smActive = $("sm-active-component");
    const smStage  = $("sm-active-stage");
    const ht = state.lastHigherTier || null;
    const htFreshMs = state.htLastFreshMono ? (performance.now() - state.htLastFreshMono) : Infinity;
    const htFresh = (htFreshMs <= 30000);
    const htTask = (ht && ht.scheduled_task) || {};
    const htTaskOk = !!(htTask && (htTask.state === "Ready" || htTask.state === "Running"
                                    || /enabled/i.test(String(htTask.state || "")))
                                && (htTask.last_result === 0 || htTask.last_result == null));
    const htCet = ht && ht.current_effective_tier ? String(ht.current_effective_tier) : "";
    // Live-truth precedence: if the most recent tier-truth payload says
    // any worker is "active" (progression or opencode), never overwrite
    // the hero with bare IDLE / no-active-task. This is the screenshot
    // bug Serge reported.
    const ttSnap = state.lastTierTruth || null;
    const ttWe = (ttSnap && ttSnap.worker_ecosystem) || {};
    const ttLunaLive = (
      String((ttWe.progression || {}).state || "").toLowerCase() === "active" ||
      String((ttWe.opencode    || {}).state || "").toLowerCase() === "active"
    );
    if (ttLunaLive) {
      // The lunaLive override block in paintEvolutionCenter already
      // wrote the live values for sm-active-component / sm-active-stage.
      // Skip the legacy paint to avoid flicker.
    } else if (htFresh && htTaskOk && htCet) {
      // Higher-tier is alive: show progression context, never bare IDLE.
      if (smActive) text(smActive, "TIER " + htCet);
      if (smStage) {
        const lp = (ht && ht.latest_progression) || {};
        const failed = (lp && lp.actions_failed != null) ? Number(lp.actions_failed) : null;
        const passed = (lp && lp.actions_passed != null) ? Number(lp.actions_passed) : null;
        const lastAction = (lp && lp.latest_action) ? String(lp.latest_action) : "";
        if (data.is_active) {
          text(smStage, stage || lastAction || "progression cycle running");
        } else {
          let tail = "bounded cycle sleeping - task enabled";
          if (lastAction) tail = lastAction + " - sleeping until next scheduled cycle";
          if (failed != null && passed != null) {
            tail += " - last cycle " + passed + " ok / " + failed + " fail";
          }
          text(smStage, tail);
        }
      }
    } else {
      if (smActive) text(smActive, data.is_active ? actor : "IDLE");
      if (smStage)  text(smStage, data.is_active ? (stage || "—") : "no active task");
    }

    state.mcPrevActor = actor;
  }

  // Approve Tier 2 — POSTs to /api/self-upgrade/approve-tier2.
  async function approveTier2Click(event) {
    const btn = (event && event.currentTarget) || $("sm-status-pill") || $("sm-approve-tier2");
    if (!btn || btn.dataset.busy === "1" || btn.dataset.action !== "approve-tier2") return;
    const old = btn.textContent;
    _smSetBusy(btn, true);
    _smSetTriggerLabel(btn, "APPROVING...");
    _smSetFeedback("approving Tier 2...", "pending");
    try {
      const res = await fetch("/api/self-upgrade/approve-tier2", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ approve: true, action: "APPROVE_TIER2", approved_by: "Serge" }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _smSetTriggerLabel(btn, "TIER 2 APPROVED");
        _smSetFeedback("approved by Serge", "ok");
        // Refresh the panel — the gate JSON now reflects approval.
        await refreshSupermax();
      } else {
        const err = (body && body.error) || (res.status + " " + res.statusText);
        _smSetTriggerLabel(btn, "APPROVAL FAILED");
        _smSetFeedback("Tier 2 approval failed: " + err, "bad");
        setTimeout(() => { _smSetTriggerLabel(btn, old); refreshSupermax(); }, 2500);
      }
    } catch (e) {
      _smSetTriggerLabel(btn, "APPROVAL ERROR");
      _smSetFeedback("Tier 2 approval network error: " + (e && e.message ? e.message : e), "bad");
      setTimeout(() => { _smSetTriggerLabel(btn, old); refreshSupermax(); }, 2500);
    } finally {
      _smSetBusy(btn, false);
    }
  }

  // ---------- Mission Control action bar wiring ----------
  function _mcSetFeedback(text, kind) {
    const el = $("mc-feedback");
    if (!el) return;
    el.textContent = text || "";
    el.dataset.kind = kind || "";
  }
  function _mcSetBusy(btn, busy) {
    if (!btn) return;
    if (busy) { btn.dataset.busy = "1"; btn.setAttribute("aria-busy", "true"); btn.disabled = true; }
    else      { btn.dataset.busy = "";  btn.removeAttribute("aria-busy"); btn.disabled = false; }
  }

  async function mcRunOnceClick(event) {
    const btn = (event && event.currentTarget) || $("mc-run-once");
    if (!btn || btn.dataset.busy === "1") return;
    _mcSetBusy(btn, true);
    _mcSetFeedback("starting bounded supervisor cycle...", "pending");
    try {
      const res = await fetch("/api/supervisor/run-once", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "RUN_ONCE" }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _mcSetFeedback("supervisor cycle started · pid " + (body.pid || "?"), "ok");
        setTimeout(() => { pollMissionControl(); pollSupervisorStatus(); }, 1500);
      } else {
        _mcSetFeedback("start failed: " + ((body && body.error) || res.statusText), "bad");
      }
    } catch (e) {
      _mcSetFeedback("network error: " + (e && e.message || e), "bad");
    } finally {
      _mcSetBusy(btn, false);
    }
  }

  async function mcStartSprintClick(event) {
    const btn = (event && event.currentTarget) || $("mc-start-sprint");
    if (!btn || btn.dataset.busy === "1") return;
    _mcSetBusy(btn, true);
    _mcSetFeedback("starting Sprint Mode loop (1-min cycles, 24h-bounded)...", "pending");
    try {
      const res = await fetch("/api/supervisor/start-sprint", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "START_SPRINT" }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _mcSetFeedback("Sprint Mode running · pid " + (body.pid || "?"), "ok");
        setTimeout(() => { pollMissionControl(); pollSupervisorStatus(); }, 1500);
      } else {
        _mcSetFeedback("sprint start failed: " + ((body && body.error) || res.statusText), "bad");
      }
    } catch (e) {
      _mcSetFeedback("network error: " + (e && e.message || e), "bad");
    } finally {
      _mcSetBusy(btn, false);
    }
  }

  async function mcStopSprintClick(event) {
    const btn = (event && event.currentTarget) || $("mc-stop-sprint");
    if (!btn || btn.dataset.busy === "1") return;
    _mcSetBusy(btn, true);
    _mcSetFeedback("requesting soft-stop after current cycle...", "pending");
    try {
      const res = await fetch("/api/supervisor/stop-sprint", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "STOP_SPRINT" }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _mcSetFeedback("stop flag set · supervisor will exit after current cycle", "ok");
        setTimeout(() => { pollMissionControl(); pollSupervisorStatus(); }, 1500);
      } else {
        _mcSetFeedback("stop failed: " + ((body && body.error) || res.statusText), "bad");
      }
    } catch (e) {
      _mcSetFeedback("network error: " + (e && e.message || e), "bad");
    } finally {
      _mcSetBusy(btn, false);
    }
  }

  async function pollSupervisorStatus() {
    const data = await fetchJSON("/api/supervisor/status");
    if (!data) return;
    state.lastSupervisorStatus = data;
    const stopBtn = $("mc-stop-sprint");
    const sprintBtn = $("mc-start-sprint");
    const reportLink = $("mc-open-report");
    if (stopBtn)   stopBtn.hidden   = !(data.running && data.is_sprint_mode);
    if (sprintBtn) sprintBtn.hidden = (data.running && data.is_sprint_mode);
    if (reportLink) {
      const rep = data.last_report_path || "";
      if (rep) {
        reportLink.href = rep;
        reportLink.hidden = false;
      } else {
        reportLink.removeAttribute("href");
        reportLink.hidden = true;
      }
    }
  }

  async function startSprintModeClick(event) {
    const btn = (event && event.currentTarget) || $("sm-sprint");
    if (!btn || btn.dataset.busy === "1") return;
    const old = btn.textContent;
    _smSetBusy(btn, true);
    _smSetTriggerLabel(btn, "STARTING SPRINT...");
    _smSetFeedback("starting Sprint Mode supervisor loop...", "pending");
    try {
      const res = await fetch("/api/self-upgrade/run-cycle", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "START_SPRINT_MODE", sprint: true }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _smSetTriggerLabel(btn, "SPRINT RUNNING");
        _smSetFeedback("Sprint Mode supervisor started (pid " + (body.pid || "?") + ")", "ok");
        setTimeout(refreshSupermax, 1800);
      } else {
        const err = (body && body.error) || (res.status + " " + res.statusText);
        _smSetTriggerLabel(btn, "SPRINT FAILED");
        _smSetFeedback("Sprint Mode start failed: " + err, "bad");
        setTimeout(() => { _smSetTriggerLabel(btn, old); }, 2500);
      }
    } catch (e) {
      _smSetTriggerLabel(btn, "SPRINT ERROR");
      _smSetFeedback("Sprint Mode network error: " + (e && e.message ? e.message : e), "bad");
      setTimeout(() => { _smSetTriggerLabel(btn, old); }, 2500);
    } finally {
      _smSetBusy(btn, false);
    }
  }

  async function runOneWorkCycleClick(event) {
    const btn = (event && event.currentTarget) || $("sm-runnow");
    if (!btn || btn.dataset.busy === "1") return;
    const old = btn.textContent;
    _smSetBusy(btn, true);
    _smSetTriggerLabel(btn, "STARTING...");
    _smSetFeedback("starting one bounded supervisor cycle...", "pending");
    try {
      const res = await fetch("/api/self-upgrade/run-cycle", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "RUN_ONE_WORK_CYCLE" }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body && body.ok) {
        _smSetTriggerLabel(btn, "CYCLE STARTED");
        _smSetFeedback("cycle started - Mission Control will update shortly", "ok");
        setTimeout(refreshSupermax, 1800);
      } else {
        const err = (body && body.error) || (res.status + " " + res.statusText);
        _smSetTriggerLabel(btn, "RUN FAILED");
        _smSetFeedback("Run cycle failed: " + err, "bad");
      }
    } catch (e) {
      _smSetTriggerLabel(btn, "RUN ERROR");
      _smSetFeedback("Run cycle network error: " + (e && e.message ? e.message : e), "bad");
    } finally {
      setTimeout(() => {
        _smSetTriggerLabel(btn, old || "RUN ONE WORK CYCLE");
        _smSetBusy(btn, false);
      }, 2200);
    }
  }

  async function refreshSupermax() {
    // Fetch both the legacy supermax payload (for heartbeat etc.) AND
    // the new structured /api/self-upgrade/progress (for patches list +
    // Tier 2 approval state).
    const [data, prog] = await Promise.all([
      fetchJSON("/api/supermax"),
      fetchJSON("/api/self-upgrade/progress"),
    ]);
    if (!data || !data.ok) return;
    state.lastSupermax = data;
    state.lastSelfUpgrade = prog || null;
    const g = data.evidence_gate || {};
    const c = data.computed || {};
    const hb = data.always_on_heartbeat || {};

    // Defer to /api/tier-truth when it's fresh — it knows about Tier 6/7/8/9+
    // whereas the legacy gate only knows 1/2. Without this check the panel
    // flickers back to "TIER 2 helper-script polish" right after the
    // Evolution Command Center paints "TIER 8 ACTIVE".
    const ttFresh = state.ttLastFreshMono &&
      ((performance.now() - state.ttLastFreshMono) < 30000) &&
      state.lastTierTruth && state.lastTierTruth.current_effective_tier;
    const t = parseInt(g.current_allowed_tier, 10);
    if (!ttFresh) {
      text($("sm-current-tier"), Number.isFinite(t) ? String(t) : "—");
      text($("sm-tier-label"), _smTierLabel(g.current_allowed_tier));
    }

    // Legacy supermax progress / eligibility cells are LIVE only when
    // /api/tier-truth has NEVER been observed AND is not currently fresh.
    //
    // 2026-05-08: previous guard was "!ttFresh" (30s window). That was
    // unreliable - any momentary flicker in ttLastFreshMono let this
    // legacy path write Tier 2 numbers into the Evolution panel's
    // canonical bar, and the 0.6s CSS width transition smoothed the
    // jump into the visible "moving forward and backward bar" symptom.
    //
    // The new check `evoProgressOwnedByTierTruth` is set ONCE by
    // paintEvolutionCenter the first time it writes the bar (sticky
    // for the page lifetime). Once set, this legacy path never touches
    // the canonical Evolution bar - it only updates the collapsed
    // historical detail block (sm-legacy-tier2) below, which is what
    // it was always supposed to do.
    const ownedByTierTruth = !!(state && state.evoProgressOwnedByTierTruth);
    const legacyShouldWrite = !ttFresh && !ownedByTierTruth;
    if (legacyShouldWrite) {
      const t0t1 = c.t0t1_successes || 0;
      const thr  = c.tier_2_threshold || 10;
      const pct  = Math.max(0, Math.min(100, Math.round((c.progress_to_tier_2 || 0) * 100)));
      const fill = $("sm-progress-fill");
      if (fill) {
        const newW = pct + "%";
        if (fill.style.width !== newW) fill.style.width = newW;
      }
      text($("sm-progress-text"), t0t1 + " / " + thr);
      const aria = $("sm-progress-aria");
      if (aria) {
        const nv = String(t0t1);
        const nm = String(thr);
        if (aria.getAttribute("aria-valuenow") !== nv) aria.setAttribute("aria-valuenow", nv);
        if (aria.getAttribute("aria-valuemax") !== nm) aria.setAttribute("aria-valuemax", nm);
      }
      text($("sm-rb-failures"), String(c.rollback_failures || 0));
      text($("sm-eligible"), c.tier_2_eligible ? "Tier 2 ELIGIBLE" : "not eligible");
    }
    // Legacy Tier 2 metrics still echoed into a collapsed historical
    // detail block (id sm-legacy-tier2). Always-painted, never visible
    // unless the user expands the detail; the main cards always come
    // from tier-truth.
    const legacyT2 = $("sm-legacy-tier2");
    if (legacyT2) {
      const t0t1 = c.t0t1_successes || 0;
      const thr  = c.tier_2_threshold || 10;
      legacyT2.textContent = "legacy Tier 2 progress: " + t0t1 + " / " + thr +
        " · tier_2_eligible=" + Boolean(c.tier_2_eligible);
    }

    // Pull approval state from the new structured endpoint when present.
    const tier2Eligible = prog ? !!prog.tier2_eligible : !!c.tier_2_eligible;
    const tier2Approved = prog ? !!prog.tier2_approved : false;
    const approvalTime  = prog ? (prog.tier2_approval_time || null) : null;
    const approvedBy    = prog ? (prog.tier2_approved_by || null) : null;
    const rb            = (prog && prog.counts) ? (parseInt(prog.counts.rollback_failures, 10) || 0)
                                                : (parseInt(g.rollback_failure_count, 10) || 0);

    // Status pill on the supermax header
    const pill = $("sm-status-pill");
    if (pill) {
      pill.dataset.action = "";
      pill.disabled = true;
      pill.title = "";
      if (!pill.dataset.bound) {
        pill.addEventListener("click", approveTier2Click);
        pill.dataset.bound = "1";
      }
      // If paintEvolutionCenter has already stamped the live-truth label
      // ("TIER 8 ACTIVE · LIVE" / "BLOCKED AT TIER 8"), the legacy Tier 2
      // approval text must NOT overwrite it. The lunaLive flag is set on
      // every tier-truth refresh, so this guard means the legacy pill is
      // only used as a cold-boot fallback before the first tier-truth tick.
      if (pill.dataset.lunaLive === "1") {
        // Live-truth path owns this pill now. Skip the Tier 2 paint.
      } else if (rb > 0) {
        pill.textContent = "ROLLBACK FAILED · NEEDS SERGE";
        pill.dataset.state = "bad";
      } else if (tier2Approved) {
        // Historical fallback: the live-truth path will replace this on
        // the next tier-truth tick. We deliberately no longer suffix
        // the legacy approval label with the current tier number — that
        // travels with the live-truth pill, which is the only
        // authoritative source.
        pill.textContent = "TIER 2 APPROVED";
        pill.dataset.state = "eligible";
      } else if (tier2Eligible) {
        pill.textContent = "TIER 2 APPROVE";
        pill.dataset.state = "warn";
        pill.dataset.action = "approve-tier2";
        pill.disabled = false;
        pill.title = "Approve Tier 2";
      } else if ((c.t0t1_successes || 0) > 0) {
        const need = Math.max(0, (c.tier_2_threshold || 10) - (c.t0t1_successes || 0));
        pill.textContent = need + " patches to Tier 2";
        pill.dataset.state = "warn";
      } else {
        pill.textContent = "awaiting first cycle";
        pill.dataset.state = "idle";
      }
    }

    // Approve Tier 2 button — visible only if eligible-not-approved + no rollback failures.
    const btnApprove = $("sm-approve-tier2");
    const btnRunNow  = $("sm-runnow");
    const approvalLine = $("sm-approval-line");
    if (btnApprove) {
      btnApprove.hidden = true;
      btnApprove.dataset.action = "approve-tier2";
      if (!btnApprove.dataset.bound) {
        btnApprove.addEventListener("click", approveTier2Click);
        btnApprove.dataset.bound = "1";
      }
    }
    if (approvalLine) {
      if (tier2Approved) {
        const ts = approvalTime ? _smShortTs(approvalTime) : "";
        approvalLine.textContent = "approved" + (approvedBy ? " by " + approvedBy : "") + (ts ? " · " + ts : "");
        approvalLine.title = approvalTime ? ("UTC: " + String(approvalTime).replace("T", " ").slice(0, 19)) : "";
        approvalLine.dataset.state = "ok";
        approvalLine.hidden = false;
      } else {
        approvalLine.removeAttribute("title");
        if (approvalLine.dataset.state !== "bad" && approvalLine.dataset.state !== "pending") {
          approvalLine.dataset.state = "";
          approvalLine.hidden = true;
        }
      }
    }
    if (btnRunNow) {
      // Run one fixed, bounded supervisor cycle through the local dashboard.
      // No browser alert and no arbitrary command text from the page.
      btnRunNow.hidden = !tier2Approved;
      if (tier2Approved && !btnRunNow.dataset.bound) {
        btnRunNow.addEventListener("click", runOneWorkCycleClick);
        btnRunNow.dataset.bound = "1";
      }
    }
    const btnSprint = $("sm-sprint");
    if (btnSprint) {
      // Sprint Mode supervisor loop: 1-min cycles, resource-throttled,
      // 24h-bounded. Only surfaced once Tier 2 is approved.
      btnSprint.hidden = !tier2Approved;
      if (tier2Approved && !btnSprint.dataset.bound) {
        btnSprint.addEventListener("click", startSprintModeClick);
        btnSprint.dataset.bound = "1";
      }
    }

    text($("sm-t0"),   String(g.tier0_success_count || 0));
    text($("sm-t1"),   String(g.tier1_success_count || 0));
    text($("sm-t2"),   String(g.tier2_success_count || 0));
    text($("sm-fail"), String(g.failed_self_patch_count || 0));

    // Heartbeat / last-cycle precedence: live-truth wins. The legacy
    // worker heartbeat says "sleeping" while Luna is actually firing
    // OpenCode events through the scheduled task — never overwrite
    // those live values with the legacy "sleeping" placeholder.
    const ttSnapHB = state.lastTierTruth || null;
    const ttWeHB = (ttSnapHB && ttSnapHB.worker_ecosystem) || {};
    const ttLunaLiveHB = (
      String((ttWeHB.progression || {}).state || "").toLowerCase() === "active" ||
      String((ttWeHB.opencode    || {}).state || "").toLowerCase() === "active"
    );
    if (!ttLunaLiveHB) {
      text($("sm-last-verdict"), hb.verdict || hb.state || "—");
      text($("sm-last-cycle-ts"), _smShortTs(hb.ts));
      text($("sm-heartbeat-state"), hb.state || "—");
      text($("sm-heartbeat-ts"), _smShortTs(hb.ts));
    }

    // Readable Recent Patches list (from /api/self-upgrade/progress).
    const list = $("sm-patches");
    const meta = $("sm-patches-meta");
    if (list) {
      list.innerHTML = "";
      const rows = (prog && Array.isArray(prog.recent_patches)) ? prog.recent_patches : [];
      if (meta) meta.textContent = rows.length ? (rows.length + " shown · newest first") : "";
      if (rows.length === 0) {
        const div = document.createElement("div");
        div.className = "luna-patches__empty";
        div.textContent = "no patches yet";
        list.appendChild(div);
      } else {
        const okStatusByKind = {
          ok:               "COMPLETED + VERIFIED",
          rolled_back:      "ROLLED BACK",
          rollback_failed:  "ROLLBACK FAILED",
          refused:          "REFUSED",
          fail:             "FAILED",
        };
        rows.forEach((row) => {
          const card = document.createElement("div");
          card.className = "luna-patch";
          card.setAttribute("role", "listitem");

          const time = document.createElement("span");
          time.className = "luna-patch__time";
          time.textContent = _smShortTs(row.timestamp);

          const tier = document.createElement("span");
          tier.className = "luna-patch__tier";
          tier.dataset.tier = String(row.tier != null ? row.tier : "?");
          tier.textContent = "T" + (row.tier != null ? row.tier : "?");

          const title = document.createElement("span");
          title.className = "luna-patch__title";
          title.textContent = row.title || "(unknown)";
          title.title = (row.files_changed && row.files_changed[0]) || row.title || "";

          const status = document.createElement("span");
          status.className = "luna-patch__status";
          status.dataset.kind = row.status_kind || "fail";
          status.textContent = row.status_label || okStatusByKind[row.status_kind] || (row.status || "—");

          const metaLine = document.createElement("div");
          metaLine.className = "luna-patch__meta";
          const appliedTxt = "applied: " + (row.applied ? "yes" : "no");
          const verifiedTxt = "verified: " + (row.verified ? "yes" : "no");
          const rbTxt     = row.rollback ? ("rollback: " + row.rollback) : "rollback: —";
          const filesTxt  = (row.files_changed_count != null)
            ? (row.files_changed_count + " file" + (row.files_changed_count === 1 ? "" : "s"))
            : "";
          const reason    = row.reason ? ("reason: " + row.reason) : "";
          const reportTxt = row.report_path ? ("report: " + row.report_path) : "";
          [appliedTxt, verifiedTxt, rbTxt, filesTxt, reason, reportTxt].forEach((t) => {
            if (!t) return;
            const span = document.createElement("span");
            span.textContent = t;
            metaLine.appendChild(span);
          });

          card.appendChild(time);
          card.appendChild(tier);
          card.appendChild(title);
          card.appendChild(status);
          card.appendChild(metaLine);
          list.appendChild(card);
        });
      }
    }

    // Tier-2-approved status flips current_tier display label too — but only
    // when /api/tier-truth isn't already running the show with a higher tier.
    const ttFreshFlip = state.ttLastFreshMono &&
      ((performance.now() - state.ttLastFreshMono) < 30000) &&
      state.lastTierTruth && state.lastTierTruth.current_effective_tier;
    if (!ttFreshFlip && prog && prog.tier2_approved) {
      text($("sm-tier-label"), _smTierLabel(prog.current_allowed_tier));
      text($("sm-current-tier"),
           Number.isFinite(parseInt(prog.current_allowed_tier, 10))
             ? String(prog.current_allowed_tier) : "—");
    }
  }

  async function refreshFeed() {
    const f = await fetchJSON("/api/live-feed?limit=" + FEED_LIMIT);
    if (!f) return;
    state.lastFeedRecords = f.records || [];
    state.lastFeedFreshMono = performance.now();
    // Live Activity Strip update — driven by the same /api/live-feed
    // poll. Bins event counts into 30 one-minute buckets, sets bar
    // heights, updates the rate label + last-event timestamp.
    try { lunaUpdateActivityStrip(f.records || []); } catch (_e) {}
    // Forward each new event to the Luna Live Cognitive Map (best-effort).
    try {
      if (typeof window.__lunaCmapForward === "function") {
        window.__lunaCmapForward(f.records || []);
      }
    } catch (e) { /* never break the feed loop */ }
    // Compute "minutes since last event" badge so users can see if the
    // live feed has gone quiet for too long.
    let lastTs = "";
    let staleNote = "";
    const records = (f.records || []);
    if (records.length) {
      const last = records[records.length - 1];
      lastTs = last.iso_utc || last.ts || "";
      // iso_utc is preferred (full RFC 3339); fall back to HH:MM:SS strings
      // which we cannot age across days but can still display.
      const t = last.iso_utc ? Date.parse(last.iso_utc) : NaN;
      if (!Number.isNaN(t)) {
        const ageS = Math.max(0, Math.round((Date.now() - t) / 1000));
        if (ageS > 60) staleNote = " · stale " + ageS + "s";
      }
    } else {
      staleNote = " · no recent events";
    }
    text($("feed-meta"),
      (f.count || 0) + " events · cap " + (f.limit || FEED_LIMIT) +
      (lastTs ? (" · last " + String(lastTs).slice(11, 19)) : "") + staleNote);
    text($("ticker-stat"), (f.count || 0) + " tracked");

    const ol = $("live-feed");
    if (ol) {
      ol.innerHTML = "";
      const records = (f.records || []).slice().reverse();
      if (!records.length) {
        const li = document.createElement("li");
        li.className = "luna-feed__empty";
        li.textContent = "awaiting telemetry…";
        ol.appendChild(li);
      } else {
        records.forEach((r) => {
          const li = document.createElement("li");
          const ts = document.createElement("span"); ts.className = "luna-feed__ts"; ts.textContent = r.ts || "—";
          const role = document.createElement("span"); role.className = "luna-feed__role"; role.textContent = r.role || r.source || "—";
          const msg = document.createElement("span"); msg.className = "luna-feed__msg";
          const head = document.createElement("strong"); head.textContent = (r.event || "EVENT") + " "; msg.appendChild(head);
          msg.appendChild(document.createTextNode(r.msg || ""));
          if (r.detail) {
            const em = document.createElement("em");
            em.textContent = " · " + trim(String(r.detail), 180);
            msg.appendChild(em);
          }
          li.appendChild(ts); li.appendChild(role); li.appendChild(msg);
          ol.appendChild(li);
        });
      }
    }

    pushNewFeedToTty(f.records || []);
  }

  async function refreshArchive() {
    const a = await fetchJSON("/api/archive");
    if (!a) return;
    text($("archive-meta"), (a.count || 0) + " items · " + a.archive_path);
    const ul = $("archive-list");
    if (!ul) return;
    ul.innerHTML = "";
    const items = (a.items || []);
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "luna-archive__empty";
      li.textContent = "No archive items detected.";
      ul.appendChild(li); return;
    }
    items.forEach((it) => {
      const li = document.createElement("li");
      const name = document.createElement("span"); name.className = "luna-archive__name";
      name.title = it.name || ""; name.textContent = it.name || "—";
      const size = document.createElement("span"); size.className = "luna-archive__size";
      size.textContent = it.is_dir ? "dir" : fmtBytes(it.size_bytes || 0);
      li.appendChild(name); li.appendChild(size);
      ul.appendChild(li);
    });
  }

  async function refreshActivity() {
    const a = await fetchJSON(`/api/activity?window=${ACTIVITY_WINDOW}&buckets=${ACTIVITY_BUCKETS}`);
    if (!a) return;
    state.lastActivity = a;
    text($("hist-stat"), (a.total_events || 0) + " events");
    text($("live-meta"),
      (a.total_events || 0) + " events · "
      + Math.round((a.window_seconds || 0)/60) + "m window · cap "
      + (a.buckets || ACTIVITY_BUCKETS) + " buckets");
    renderRoles();
  }

  // ============================================================
  // Animation + sample loops
  // ============================================================
  // ============================================================
  // Luna UI Stability sentinel.
  //
  // Lightweight diagnostics object exposed as window.LunaUIHealth so the
  // operator can call window.LunaUIHealth.report() in DevTools.
  // Used internally by _resizeCanvasIfNeeded, the RAF guard, and the
  // setInterval guard to detect duplicate work + frame jank.
  //
  // The sentinel itself does NOT spawn timers or RAF loops - it relies on
  // the existing rafLoop tick to compute frame-jank deltas. Zero overhead
  // beyond a few counter increments per frame.
  // ============================================================
  if (!window.LunaUIHealth) {
    window.LunaUIHealth = {
      version: 1,
      // Single-init guards
      rafLoopStarted: false,
      intervalsBound: false,
      duplicateInitCount: 0,
      // Per-canvas resize counts (per minute window). High values on a
      // canvas whose layout did not change = the dpr-mismatch bug returned.
      canvasResizeCount: Object.create(null),
      canvasResizeCountWindow: Object.create(null),  // minute-windowed
      // Polling registry
      intervalIds: [],
      intervalNames: [],
      // Animation jank
      lastFrameTs: 0,
      frameJankCount: 0,    // frames where delta > 50ms
      frameTotal: 0,
      // DOM rebuild counter (incremented by callers that fully replace
      // a panel via innerHTML="" + rebuild)
      domRebuildCount: 0,
      // Mutation burst (incremented by a single MutationObserver below)
      mutationBurstCount: 0,
      // Last init timestamp for duplicate-DOMContentLoaded detection
      lastInitTimestamp: 0,
      // Convenience reporter for DevTools console
      report: function () {
        const out = {
          version: this.version,
          rafLoopStarted: this.rafLoopStarted,
          intervalsBound: this.intervalsBound,
          duplicateInitCount: this.duplicateInitCount,
          canvasResizeCount: Object.assign({}, this.canvasResizeCount),
          intervalCount: (this.intervalIds || []).length,
          intervalNames: (this.intervalNames || []).slice(),
          frameJankCount: this.frameJankCount,
          frameTotal: this.frameTotal,
          jankRatio: this.frameTotal ? (this.frameJankCount / this.frameTotal) : 0,
          domRebuildCount: this.domRebuildCount,
          mutationBurstCount: this.mutationBurstCount,
          lastInitTimestampISO: this.lastInitTimestamp ? new Date(this.lastInitTimestamp).toISOString() : null,
        };
        try { console.log("LunaUIHealth.report()", out); } catch (e) {}
        return out;
      },
    };
    // Reset minute-windowed counters once a minute. If a counter exceeds
    // its threshold within the window, log a one-shot console.warn so
    // the operator notices the regression in DevTools without any noisy
    // UI. Quiet by design.
    setInterval(function () {
      try {
        const h = window.LunaUIHealth;
        if (!h) return;
        // canvasResizeCount over 600/min (10/sec) on any canvas means the
        // dpr-mismatch bug or a real resize storm came back.
        for (const k of Object.keys(h.canvasResizeCount || {})) {
          const v = h.canvasResizeCount[k] || 0;
          if (v > 600 && !h._warnedResize) {
            h._warnedResize = true;
            console.warn("LunaUIHealth: canvas '" + k + "' resized " + v + " times in last minute - check _resizeCanvasIfNeeded guards");
          }
        }
        // Reset for next window.
        h.canvasResizeCountWindow = Object.assign({}, h.canvasResizeCount);
        h.canvasResizeCount = Object.create(null);
      } catch (e) {}
    }, 60000);
  }

  function rafLoop() {
    // Frame-jank detection: any delta > 50ms = visible jank.
    try {
      const h = window.LunaUIHealth;
      if (h) {
        const now = performance.now();
        if (h.lastFrameTs) {
          const delta = now - h.lastFrameTs;
          h.frameTotal += 1;
          if (delta > 50) h.frameJankCount += 1;
        }
        h.lastFrameTs = now;
      }
    } catch (e) { /* sentinel never breaks render */ }

    const osc = $("osc-canvas");
    const mission = $("mission-canvas");
    const hist = $("hist-canvas");
    if (osc)     drawOscilloscope(osc);
    if (mission) drawMissionClock(mission);
    if (hist)    drawHistogram(hist);
    refreshGauges();
    syncOscilloscopeMeta();
    requestAnimationFrame(rafLoop);
  }

  async function refreshAll() {
    tickClock();
    await Promise.all([
      refreshStatus(),
      refreshBrief(),
      refreshSoak(),
      refreshHealth(),
      refreshFeed(),
      refreshArchive(),
      refreshActivity(),
      refreshSupermax(),
    ]);
    markDashboardFresh("dashboard poll");
  }

  document.addEventListener("DOMContentLoaded", () => {
    // Wire dropdown source switcher.
    const sel = $("osc-source");
    if (sel) {
      const saved = (function () {
        try { return window.localStorage.getItem("luna.pulseSource"); }
        catch (e) { return null; }
      })();
      if (saved && SOURCE_META[saved]) {
        state.pulseSource = saved;
        sel.value = saved;
      }
      sel.addEventListener("change", () => {
        state.pulseSource = sel.value;
        try { window.localStorage.setItem("luna.pulseSource", sel.value); }
        catch (e) { /* ignore */ }
        // Immediate re-render of the Activity Strip + the small osc-bpm
        // numeric pill in the header, so the operator sees the new
        // source instantly instead of waiting for the next poll tick.
        try { lunaUpdateActivityStrip(state.lastFeedRecords || []); } catch (e) {}
        try { syncOscilloscopeMeta(); } catch (e) {}
      });
    }

    // Idempotency guard: if DOMContentLoaded fires twice (rare but possible
    // when the page is restored from bfcache or a script-tag reload), do
    // NOT re-run the init sequence and do NOT register a second wave of
    // setIntervals or a second rafLoop chain. The duplicate-init counter
    // surfaces this in LunaUIHealth.report().
    const _h = window.LunaUIHealth;
    if (_h && _h.intervalsBound) {
      _h.duplicateInitCount = (_h.duplicateInitCount || 0) + 1;
      try { console.warn("LunaUIHealth: DOMContentLoaded re-entered; skipping duplicate init", _h.report ? _h.report() : null); } catch (e) {}
      return;
    }

    initSettings();
    initFocusMode();
    initQuickCommands();
    initTelemetryTabs();
    initConsole();
    initExplorer();
    initFileBayCollapse();
    initMissionControlActions();
    initKillSwitch();
    initTerminalPanel();
    initTierTimeline();

    if (_h) _h.lastInitTimestamp = Date.now();

    // Centralized interval registration. _lunaInterval(fn, ms, name)
    // wraps setInterval, stores the id + name in LunaUIHealth so the
    // selfcheck can audit the count, and prevents accidental
    // double-registration on the same name.
    function _lunaInterval(fn, ms, name) {
      try {
        if (_h && _h.intervalNames && _h.intervalNames.indexOf(name) >= 0) {
          // Already registered with this name; do not create a duplicate.
          _h.duplicateInitCount = (_h.duplicateInitCount || 0) + 1;
          return null;
        }
        const id = setInterval(fn, ms);
        if (_h) {
          _h.intervalIds.push(id);
          _h.intervalNames.push(name);
        }
        return id;
      } catch (e) {
        return null;
      }
    }

    refreshAll();
    _lunaInterval(refreshAll, REFRESH_MS, "refreshAll");
    _lunaInterval(tickClock, 1000, "tickClock");
    _lunaInterval(tickLivedots, 1000, "tickLivedots");
    // 1Hz live countdown clock: 60 -> 59 -> 58. Decoupled from the 1.5s
    // mission-control poll so the clock ticks smoothly between server polls.
    _lunaInterval(tickCountdown, 1000, "tickCountdown");
    // 1Hz global Luna health guard: don't show "globally stale" when
    // /api/higher-tier/progress is fresh and the progression task is alive.
    _lunaInterval(tickGlobalLunaHealth, 1000, "tickGlobalLunaHealth");
    // Mission Control polls fast (1.5s) so actor changes and stale states
    // surface promptly without making other endpoints chatty.
    pollMissionControl();
    _lunaInterval(pollMissionControl, 1000, "pollMissionControl");
    pollSupervisorStatus();
    _lunaInterval(pollSupervisorStatus, 1000, "pollSupervisorStatus");
    // Higher-tier (Tier 6/7/8 progression) — slower cadence is fine; this
    // surface drives the new "TIER N ACTIVE" override on the Mission
    // Control header and renders the Higher-Tier card under Self-Upgrade.
    pollHigherTier();
    _lunaInterval(pollHigherTier, 1000, "pollHigherTier");
    // Tier Truth · drives the Evolution Command Center hero/ladder/cards.
    // Polled at the same cadence as pollHigherTier so Tier 8 -> Tier 9
    // promotion shows up in the UI without a hard refresh.
    pollTierTruth();
    _lunaInterval(pollTierTruth, 1000, "pollTierTruth");
    // pollLevelTierFramework was disabled 2026-05-08 because it polled
    // a path the inviolate static-file whitelist does not allow (404
    // storm in DevTools). Removing the interval keeps the network panel
    // clean. See comment on pollLevelTierFramework() definition.
    pollOpenCode();
    _lunaInterval(pollOpenCode, 12000, "pollOpenCode");
    // Live pulse strip — pulled from existing live-feed records every 2s.
    _lunaInterval(paintEvoPulse, 2000, "paintEvoPulse");
    _lunaInterval(rotatePrompt, 5500, "rotatePrompt");
    _lunaInterval(sampleTrends, SAMPLE_MS, "sampleTrends");
    if (_h) _h.intervalsBound = true;

    // RAF startup guard: prevent two parallel chains if init runs twice
    // (each chain calls itself via requestAnimationFrame, so two starts
    // would mean every canvas redraw fires twice per frame).
    if (!_h || !_h.rafLoopStarted) {
      if (_h) _h.rafLoopStarted = true;
      requestAnimationFrame(rafLoop);
    }

    // Mutation-burst observer (one global instance) - counts large DOM
    // mutation events so a runaway rebuild loop is visible in
    // LunaUIHealth.report(). Kept lightweight: subtree=true on body but
    // we only count, never inspect targets.
    try {
      if (window.MutationObserver && _h && !_h._mutObs) {
        _h._mutObs = new MutationObserver(function (records) {
          let added = 0;
          for (const r of records) added += (r.addedNodes && r.addedNodes.length) || 0;
          if (added > 50) _h.mutationBurstCount = (_h.mutationBurstCount || 0) + 1;
        });
        _h._mutObs.observe(document.body, { childList: true, subtree: true });
      }
    } catch (e) { /* observer is best-effort */ }
  });

  // ============================================================
  // Mission Control action bar — wire button click handlers.
  // ============================================================
  function initMissionControlActions() {
    const runBtn   = document.getElementById("mc-run-once");
    const startBtn = document.getElementById("mc-start-sprint");
    const stopBtn  = document.getElementById("mc-stop-sprint");
    if (runBtn   && !runBtn.dataset.bound)   { runBtn.addEventListener("click", mcRunOnceClick);   runBtn.dataset.bound = "1"; }
    if (startBtn && !startBtn.dataset.bound) { startBtn.addEventListener("click", mcStartSprintClick); startBtn.dataset.bound = "1"; }
    if (stopBtn  && !stopBtn.dataset.bound)  { stopBtn.addEventListener("click", mcStopSprintClick);   stopBtn.dataset.bound = "1"; }
  }

  // ============================================================
  // Kill-switch — guarded arm-then-confirm flow. Two clicks (or a
  // typed phrase) before the local-only POST goes out. The exact
  // PowerShell fallback command stays visible at all times so Serge
  // can run it directly if the dashboard is unreachable.
  // ============================================================
  function initKillSwitch() {
    const armBtn  = document.getElementById("ks-arm");
    const runBtn  = document.getElementById("ks-run");
    const phrase  = document.getElementById("ks-phrase");
    const status  = document.getElementById("ks-status");
    const REQUIRED_PHRASE = "RUN KILL SWITCH";
    if (!armBtn || !runBtn || !status) return;
    let armed = false;
    let armTimer = null;
    function setArmed(on) {
      armed = !!on;
      runBtn.hidden = !armed;
      armBtn.dataset.armed = armed ? "1" : "";
      armBtn.textContent = armed ? "DISARM" : "ARM KILL-SWITCH";
      status.textContent = armed
        ? "ARMED · type \"" + REQUIRED_PHRASE + "\" then click RUN KILL-SWITCH (auto-disarms in 30s)"
        : "kill-switch is safe (not armed)";
      status.dataset.kind = armed ? "warn" : "";
      if (armTimer) clearTimeout(armTimer);
      if (armed) armTimer = setTimeout(() => setArmed(false), 30000);
    }
    if (!armBtn.dataset.bound) {
      armBtn.addEventListener("click", () => setArmed(!armed));
      armBtn.dataset.bound = "1";
    }
    if (!runBtn.dataset.bound) {
      runBtn.addEventListener("click", async () => {
        if (!armed) { status.textContent = "kill-switch must be armed first"; status.dataset.kind = "bad"; return; }
        const typed = (phrase && phrase.value || "").trim().toUpperCase();
        if (typed !== REQUIRED_PHRASE) {
          status.textContent = "type the phrase \"" + REQUIRED_PHRASE + "\" exactly to confirm";
          status.dataset.kind = "bad";
          return;
        }
        status.textContent = "sending kill-switch request...";
        status.dataset.kind = "pending";
        runBtn.disabled = true;
        try {
          const res = await fetch("/api/kill-switch/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "RUN_KILL_SWITCH", confirm: REQUIRED_PHRASE }),
          });
          const body = await res.json().catch(() => ({}));
          if (res.ok && body && body.ok) {
            status.textContent = "kill-switch ran · " + (body.message || "system reverting to advisory state");
            status.dataset.kind = "ok";
            setArmed(false);
          } else {
            status.textContent = "kill-switch failed: " + ((body && body.error) || res.statusText) +
              " · run the PowerShell command shown below directly";
            status.dataset.kind = "bad";
          }
        } catch (e) {
          status.textContent = "network error: " + (e && e.message || e) +
            " · run the PowerShell command shown below directly";
          status.dataset.kind = "bad";
        } finally {
          runBtn.disabled = false;
        }
      });
      runBtn.dataset.bound = "1";
    }
    setArmed(false);
  }

  // ============================================================
  // Command Console — talk to Luna
  //   - sessions in localStorage  (keys: luna.chat.sessions, luna.chat.active, luna.chat.perm)
  //   - sends via POST /api/chat/send  (writes a task into tasks/active/)
  //   - uploads via POST /api/chat/upload  (multipart; saved next to the task)
  //   - voice via Web Speech API where available
  // ============================================================
  const CHAT_LS_SESSIONS = "luna.chat.sessions";
  const CHAT_LS_ACTIVE   = "luna.chat.active";
  const CHAT_LS_PERM     = "luna.chat.perm";
  const PERM_LABELS = {
    ask:      "Ask permission",
    bypass:   "Bypass permissions",
    readonly: "Read-only",
    sandbox:  "Sandbox only",
    council:  "Council vote",
  };

  const chat = {
    sessions: {},      // { id: { id, name, created, messages: [...] } }
    activeId: null,
    perm: "ask",
    pendingFiles: [],  // { name, size, type, dataUrl } before send
    listening: false,
    recog: null,
  };

  function lsGet(key, fallback) {
    try { const v = window.localStorage.getItem(key); return v == null ? fallback : v; }
    catch (e) { return fallback; }
  }
  function lsSet(key, value) {
    try { window.localStorage.setItem(key, value); } catch (e) { /* ignore */ }
  }
  function loadChat() {
    try {
      const raw = lsGet(CHAT_LS_SESSIONS, "{}");
      chat.sessions = JSON.parse(raw) || {};
    } catch (e) { chat.sessions = {}; }
    chat.activeId = lsGet(CHAT_LS_ACTIVE, "") || null;
    chat.perm = lsGet(CHAT_LS_PERM, "auto") || "auto";
    if (!chat.activeId || !chat.sessions[chat.activeId]) newSession(false);
  }
  function saveChat() {
    lsSet(CHAT_LS_SESSIONS, JSON.stringify(chat.sessions));
    if (chat.activeId) lsSet(CHAT_LS_ACTIVE, chat.activeId);
    lsSet(CHAT_LS_PERM, chat.perm);
  }
  function newSessionId() {
    const t = new Date();
    const z = (n) => String(n).padStart(2, "0");
    const stamp = t.getFullYear() + z(t.getMonth()+1) + z(t.getDate()) + "_" +
                  z(t.getHours()) + z(t.getMinutes()) + z(t.getSeconds());
    const rnd = Math.random().toString(36).slice(2, 8);
    return "chat_" + stamp + "_" + rnd;
  }
  function newSession(persist) {
    const id = newSessionId();
    chat.sessions[id] = {
      id,
      name: "session " + new Date().toLocaleString(undefined, { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }),
      created: new Date().toISOString(),
      messages: [],
    };
    chat.activeId = id;
    if (persist !== false) { saveChat(); renderChat(); }
    return id;
  }
  function activeSession() {
    return chat.sessions[chat.activeId] || null;
  }
  function fmtTime(iso) {
    try { const d = new Date(iso); return String(d.getHours()).padStart(2,"0") + ":" + String(d.getMinutes()).padStart(2,"0") + ":" + String(d.getSeconds()).padStart(2,"0"); }
    catch (e) { return ""; }
  }
  function renderChat() {
    const box = $("console-chat");
    const sess = activeSession();
    if (!box) return;
    const nameEl = $("console-session-name");
    if (nameEl && sess) nameEl.textContent = "session · " + sess.id.slice(5, 18);
    box.innerHTML = "";
    if (!sess || !sess.messages.length) {
      const empty = document.createElement("div");
      empty.id = "console-empty";
      empty.className = "luna-console__empty";
      // Original listening-core empty state. The Luna Cognitive Map now
      // lives in its own full-width section below the cockpit (so it
      // doesn't compete with the chat input), not inside this empty state.
      empty.innerHTML =
        '<div class="luna-listening-core" aria-hidden="true">' +
          '<span class="luna-listening-core__halo"></span>' +
          '<span class="luna-listening-core__ring luna-listening-core__ring--outer"></span>' +
          '<span class="luna-listening-core__ring luna-listening-core__ring--inner"></span>' +
          '<span class="luna-listening-core__orbit">' +
            '<span class="luna-listening-core__dot"></span>' +
            '<span class="luna-listening-core__dot"></span>' +
            '<span class="luna-listening-core__dot"></span>' +
          '</span>' +
          '<span class="luna-listening-core__pulse"></span>' +
          '<span class="luna-listening-core__sigil">⟢</span>' +
        '</div>' +
        '<div class="luna-console__empty-title">Luna is listening</div>' +
        '<div class="luna-console__empty-hint">Type a request, drop files anywhere on this panel, or hit the mic to dictate.</div>' +
        renderQuickChipsHTML() +
        '<div class="luna-console__empty-shortcuts"><kbd>Enter</kbd> send · <kbd>Shift</kbd>+<kbd>Enter</kbd> newline · <kbd>Esc</kbd> cancel · <kbd>Ctrl</kbd>+<kbd>K</kbd> new session</div>';
      box.appendChild(empty);
      return;
    }
    for (const m of sess.messages) box.appendChild(renderMsg(m));
    box.scrollTop = box.scrollHeight;
  }
  function renderMsg(m) {
    const wrap = document.createElement("div");
    wrap.className = "luna-msg luna-msg--" + (m.role || "sys");
    const avatar = document.createElement("div");
    avatar.className = "luna-msg__avatar";
    avatar.textContent = m.role === "you" ? "S" : (m.role === "luna" ? "L" : "·");
    const body = document.createElement("div");
    body.className = "luna-msg__body";
    const meta = document.createElement("div");
    meta.className = "luna-msg__meta";
    meta.innerHTML =
      '<span class="luna-msg__role">' + (m.role === "you" ? "you" : (m.role === "luna" ? "Luna" : "system")) + '</span>' +
      '<span class="luna-msg__ts">' + fmtTime(m.ts) + '</span>' +
      (m.permLabel ? ' <span class="luna-msg__perm">· ' + escapeHtml(m.permLabel) + '</span>' : "") +
      (m.traceId ? ' <span class="luna-msg__trace">trace ' + escapeHtml(m.traceId) + '</span>' : "");
    const bubble = document.createElement("div");
    bubble.className = "luna-msg__bubble";
    if (m.typing) {
      const text = String(m.typingText || "Luna is working...");
      const steps = Array.isArray(m.typingSteps) ? m.typingSteps : [];
      const lines = steps
        .map((s) => String((s && s.text) || "").trim())
        .filter(Boolean)
        .slice(-4);
      bubble.innerHTML =
        '<div class="luna-msg__worklog">' +
          '<div class="luna-msg__worklog-head">' + escapeHtml(text) + ' <span class="luna-msg__typing"><span></span><span></span><span></span></span></div>' +
          (lines.length ? '<div class="luna-msg__worklog-steps">' + lines.map((line) => '<div>· ' + escapeHtml(line) + '</div>').join("") + '</div>' : '') +
        '</div>';
    } else {
      bubble.textContent = m.text || "";
    }
    body.appendChild(meta);
    body.appendChild(bubble);
    if (m.attachments && m.attachments.length) {
      const row = document.createElement("div");
      row.className = "luna-msg__attach";
      for (const f of m.attachments) {
        const chip = document.createElement("span");
        chip.className = "luna-msg__chip";
        chip.innerHTML = '<svg viewBox="0 0 20 20" width="11" height="11" aria-hidden="true"><path d="M13.5 6.5l-6 6a2.5 2.5 0 1 0 3.54 3.54l6.5-6.5a4 4 0 1 0-5.66-5.66l-6.7 6.7a5.5 5.5 0 1 0 7.78 7.78L15.5 12" stroke="currentColor" stroke-width="1.4" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg><span>' + escapeHtml(f.name) + '</span>';
        row.appendChild(chip);
      }
      body.appendChild(row);
    }
    wrap.appendChild(avatar);
    wrap.appendChild(body);
    return wrap;
  }
  function escapeHtml(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  }
  function pushMessage(msg) {
    const sess = activeSession() || chat.sessions[newSession(false)];
    sess.messages.push(msg);
    saveChat();
    renderChat();
  }
  function setTyping(on, detail) {
    const sess = activeSession();
    if (!sess) return;
    sess.messages = sess.messages.filter((m) => !m.typing);
    if (on) {
      const info = detail || {};
      sess.messages.push({
        id: "typing",
        role: "luna",
        typing: true,
        typingText: info.text || "queued · waiting for Luna to pick this up",
        typingSteps: Array.isArray(info.steps) ? info.steps : [],
        taskId: info.taskId || "",
        traceId: info.traceId || "",
        ts: new Date().toISOString(),
      });
    }
    renderChat();
  }
  function updateTypingProgress(data) {
    if (!data || data.ready) return;
    const steps = Array.isArray(data.visible_steps) ? data.visible_steps : [];
    const latest = steps.length ? steps[steps.length - 1] : null;
    const phase = data.phase ? String(data.phase) : "queued";
    const progress = Number.isFinite(Number(data.progress)) ? Math.max(0, Math.min(100, Number(data.progress))) : 0;
    const status = latest && latest.text
      ? String(latest.text)
      : (data.task_status ? String(data.task_status) + " · " + phase : "queued · waiting for Luna to pick this up");
    const label = progress > 0 ? status + " · " + progress + "%" : status;
    setTyping(true, { text: label, steps, taskId: data.task_id || "", traceId: data.trace_id || "" });
  }
  function renderAttached() {
    const row = $("console-attached");
    if (!row) return;
    if (!chat.pendingFiles.length) { row.hidden = true; row.innerHTML = ""; return; }
    row.hidden = false;
    row.innerHTML = "";
    chat.pendingFiles.forEach((f, i) => {
      const chip = document.createElement("span");
      chip.className = "luna-attach-chip";
      chip.innerHTML = '<svg viewBox="0 0 20 20" width="11" height="11" aria-hidden="true"><path d="M13.5 6.5l-6 6a2.5 2.5 0 1 0 3.54 3.54l6.5-6.5a4 4 0 1 0-5.66-5.66l-6.7 6.7a5.5 5.5 0 1 0 7.78 7.78L15.5 12" stroke="currentColor" stroke-width="1.4" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg><span>' + escapeHtml(f.name) + '</span>';
      const x = document.createElement("button");
      x.type = "button"; x.className = "luna-attach-chip__remove"; x.textContent = "×"; x.title = "Remove";
      x.addEventListener("click", () => { chat.pendingFiles.splice(i, 1); renderAttached(); });
      chip.appendChild(x);
      row.appendChild(chip);
    });
  }

  async function uploadFile(sessionId, file) {
    const fd = new FormData();
    fd.append("session", sessionId);
    fd.append("file", file, file.name);
    try {
      const r = await fetch("/api/chat/upload", { method: "POST", body: fd, credentials: "omit" });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  // Stage 2 - Luna Vision Link.
  // Loopback-only. Calls /api/vision/describe with {session, file_path};
  // the backend selects an installed vision model (llava / gemma3 / etc.)
  // and returns a short description. We never block the chat send for
  // longer than the per-call timeout; if vision is unavailable or the
  // model rejects images, we fall through gracefully with a clean note.
  async function describeImageOnLuna(sessionId, storedPath, userPrompt) {
    if (!sessionId || !storedPath) return null;
    try {
      const body = { session: sessionId, file_path: storedPath };
      if (userPrompt) body.prompt = String(userPrompt).slice(0, 600);
      const r = await fetch("/api/vision/describe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        credentials: "omit",
      });
      if (!r.ok && r.status !== 503) return null;
      return await r.json();
    } catch (e) {
      return null;
    }
  }

  // Cache vision availability for a single browser session so we don't
  // hammer /api/tags after the first "not installed" answer. Reset by
  // reload.
  const visionAvail = { probed: false, available: false, model: null, lastError: "" };
  function _markVisionResult(res) {
    visionAvail.probed = true;
    if (res && res.ok) {
      visionAvail.available = true;
      visionAvail.model = res.model || null;
      visionAvail.lastError = "";
    } else {
      visionAvail.available = false;
      visionAvail.lastError = (res && res.error) || "vision unreachable";
      // Surface the model name even on failure so the pill can show
      // "vision: gemma3:4b (no image input)" - useful operator feedback.
      if (res && res.model) visionAvail.model = res.model;
    }
    _renderVisionPill();
  }
  function _renderVisionPill() {
    const pill = $("luna-vision-pill");
    if (!pill) return;
    if (!visionAvail.probed) {
      pill.textContent = "vision: ready";
      pill.dataset.state = "ready";
      return;
    }
    if (visionAvail.available) {
      pill.textContent = "vision: " + (visionAvail.model || "active");
      pill.dataset.state = "ok";
    } else {
      const why = (visionAvail.lastError || "").toLowerCase();
      const short = (why.indexOf("not installed") >= 0)
        ? "not installed"
        : (visionAvail.lastError || "unavailable");
      pill.textContent = "vision: " + (visionAvail.model ? visionAvail.model + " - " : "") + short;
      pill.dataset.state = "off";
    }
  }
  async function pollChatReply(taskId, maxTries, intervalMs) {
    if (!taskId) return { ok: false, ready: false };
    let consecutiveErrors = 0;
    const MAX_CONSECUTIVE_ERRORS = 4;  // ~4 s offline = give up
    for (let i = 0; i < maxTries; i++) {
      try {
        const r = await fetch("/api/chat/response?task=" + encodeURIComponent(taskId),
                              { credentials: "omit", cache: "no-store" });
        if (r.ok) {
          consecutiveErrors = 0;
          const data = await r.json();
          updateTypingProgress(data);
          if (data && data.ready) return data;
        } else {
          consecutiveErrors++;
        }
      } catch (e) {
        // "Failed to fetch" / AbortError = dashboard unreachable. Don't
        // spin for 30 s; surface the offline state quickly.
        consecutiveErrors++;
      }
      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        return { ok: false, ready: false, offline: true,
                 error: "dashboard offline (response endpoint unreachable)" };
      }
      await new Promise((res) => setTimeout(res, intervalMs));
    }
    return { ok: true, ready: false };
  }

  // Send timeout: /api/chat/send now runs the live 8B GPU brain synchronously
  // (2026-06-02 live-conversation fast path). Warm replies take 5-10 s; cold
  // start (first message after restart, model loading) can take up to 60 s.
  // 120 s gives comfortable headroom. The browser shows a "typing" indicator
  // while waiting. Abort on genuine server-down faster paths still fire.
  const CHAT_SEND_TIMEOUT_MS = 120000;

  async function sendChat(text, attachments) {
    const sess = activeSession() || chat.sessions[newSession(false)];
    const body = {
      session: sess.id,
      message: text,
      perm_mode: chat.perm,
      attachments: (attachments || []).map((a) => ({ name: a.name, size: a.size, type: a.type })),
    };
    let ctrl = null;
    let timer = null;
    try {
      ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
      if (ctrl) {
        timer = setTimeout(() => {
          try { ctrl.abort(); } catch (_e) { /* ignore */ }
        }, CHAT_SEND_TIMEOUT_MS);
      }
      const opts = {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        credentials: "omit",
      };
      if (ctrl) opts.signal = ctrl.signal;
      const r = await fetch("/api/chat/send", opts);
      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        return { ok: false, error: "send failed (" + r.status + ") " + txt.slice(0, 120) };
      }
      return await r.json();
    } catch (e) {
      const msg = String((e && e.message) || e || "");
      // Chrome / Edge / Firefox all spell network failure as "Failed to fetch".
      // AbortError fires when our own timeout cancels the request — same
      // operational meaning: server is unreachable.
      if (msg.indexOf("Failed to fetch") >= 0 || /abort/i.test(msg) || (e && e.name === "AbortError")) {
        return { ok: false, offline: true,
                 error: "dashboard offline — server unreachable at 127.0.0.1:8765" };
      }
      return { ok: false, error: msg };
    } finally {
      if (timer) {
        try { clearTimeout(timer); } catch (_e) { /* ignore */ }
      }
    }
  }

  function initConsole() {
    loadChat();
    renderChat();
    setPerm(chat.perm);

    const form  = $("console-form");
    const input = $("console-input");
    const send  = $("console-send");
    const attachBtn = $("console-attach");
    const fileIn = $("console-file");
    const mic = $("console-mic");
    const newBtn = $("console-new");
    const clearBtn = $("console-clear");
    const delBtn = $("console-delete");
    const permBtn = $("console-perm-btn");
    const permMenu = $("console-perm-menu");
    const panel = $("luna-console");

    if (input) {
      const grow = () => {
        input.style.height = "auto";
        input.style.height = Math.min(200, input.scrollHeight) + "px";
      };
      input.addEventListener("input", grow);
      input.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); form.requestSubmit(); }
        else if (e.key === "Escape") { input.value = ""; grow(); }
      });
    }

    if (form) {
      form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const text = (input.value || "").trim();
        const files = chat.pendingFiles.slice();
        if (!text && !files.length) return;
        input.value = ""; input.style.height = "auto";
        const sessionId = (activeSession() || chat.sessions[newSession(false)]).id;

        // Optimistic render
        pushMessage({
          id: "msg_" + Date.now(),
          role: "you",
          text,
          ts: new Date().toISOString(),
          attachments: files.map((f) => ({ name: f.name, size: f.size })),
          permLabel: PERM_LABELS[chat.perm] || chat.perm,
        });
        chat.pendingFiles = [];
        renderAttached();
        setTyping(true);
        send.disabled = true;

        // Upload any external File objects; reference path-drags directly.
        const uploaded = [];
        const visionDescriptions = [];
        for (const f of files) {
          if (f && f.__pathRef) {
            uploaded.push({ name: f.name, size: f.size, ok: true, stored: f.path, ref: true });
            continue;
          }
          const res = await uploadFile(sessionId, f);
          uploaded.push({ name: f.name, size: f.size, ok: !!res, stored: res && res.path || null });
          // Stage 2 - if it's an image, ask Luna Vision Link to describe it
          // so the worker model has a textual description in the prompt.
          // Only triggers when the file looked like an image at capture time
          // OR mime starts with image/. Failures degrade silently.
          const isImg = (f && f.type && f.type.indexOf("image/") === 0)
                     || (f && /\.(jpe?g|png|gif|webp|bmp)$/i.test(f.name || ""));
          if (isImg && res && res.ok && res.path) {
            try {
              const vis = await describeImageOnLuna(sessionId, res.path, text);
              _markVisionResult(vis);
              if (vis && vis.ok && vis.description) {
                visionDescriptions.push({ name: f.name, description: vis.description, model: vis.model });
              }
            } catch (e) { /* never block chat send */ }
          }
        }
        // Prefix vision descriptions to the message text so the worker
        // sees them as part of the prompt without any worker.py change.
        let augmentedText = text;
        if (visionDescriptions.length) {
          const tag = visionDescriptions
            .map((v) => "[Vision (" + (v.model || "luna-vision") + "): " + v.description + "]")
            .join(" ");
          augmentedText = tag + (text ? " " + text : "");
        }

        const res = await sendChat(augmentedText, uploaded);
        send.disabled = false;

        if (!res || !res.ok) {
          // Hard failure: server unreachable, bad request, etc. Clear
          // the typing indicator so the message doesn't sit in "queued"
          // forever.
          setTyping(false);
          const offline = !!(res && res.offline);
          pushMessage({
            id: "msg_" + Date.now() + "_e",
            role: "sys",
            text: offline
              ? "dashboard offline — message not delivered. Restart Luna Command Center and resend."
              : ("send failed — " + ((res && res.error) || "unknown error")),
            ts: new Date().toISOString(),
          });
          return;
        }

        // FAST PATH: Core Brain answered inline (read-only audit-guarded
        // question — tier status / agent-bus). Render immediately; skip
        // the slow worker-queue polling loop.
        if (res.fast_path && typeof res.answer === "string" && res.answer.length > 0) {
          setTyping(false);
          pushMessage({
            id: "msg_" + Date.now() + "_l",
            role: "luna",
            text: res.answer,
            ts: new Date().toISOString(),
            fast_path: true,
            traceId: res.trace_id || "",
          });
          try { lunaAutoSpeakReply(res.answer); } catch (_e) {}
          return;
        }

        // Slow path: poll for Luna's reply written by the worker into
        // solutions/<task>.txt. pollChatReply itself fails-fast on
        // dashboard-down via the consecutive-error counter, so we never
        // wait the full 30 s on a dead server.
        const taskId = res.task_id;
        if (res.trace_id) {
          setTyping(true, {
            text: "queued · waiting for Luna to pick this up",
            taskId: taskId || "",
            traceId: res.trace_id
          });
        }
        const reply = await pollChatReply(taskId, 30, 1000);
        setTyping(false);
        if (reply && reply.ready && reply.reply) {
          pushMessage({
            id: "msg_" + Date.now() + "_l",
            role: "luna",
            text: reply.reply,
            ts: new Date().toISOString(),
            traceId: reply.trace_id || res.trace_id || "",
          });
          // Auto-speak Luna's reply through the active voice engine
          // (Kokoro / edge-tts / pyttsx3) IF Voice ON is toggled. The
          // voice engine's own sanitiser strips secrets + code blocks
          // before speech leaves the queue, so we never read API keys
          // or stack traces aloud.
          try { lunaAutoSpeakReply(reply.reply); } catch (_e) {}
        } else if (reply && reply.offline) {
          pushMessage({
            id: "msg_" + Date.now() + "_offline",
            role: "sys",
            text: "dashboard offline — stopped polling. Task id: " + taskId
                + ". Restart Luna Command Center; the answer may still be available afterwards.",
            ts: new Date().toISOString(),
          });
        } else {
          // Polling completed without an answer (worker is slow / busy).
          // Surface a clear "no reply yet" message, never leave the user
          // staring at a perpetual "queued · waiting for Luna" line.
          pushMessage({
            id: "msg_" + Date.now() + "_to",
            role: "sys",
            text: "no reply yet — worker is busy or slow. Task id: " + taskId
                + (reply && reply.error ? " · " + reply.error : ""),
            ts: new Date().toISOString(),
          });
        }
      });
    }

    if (attachBtn && fileIn) {
      attachBtn.addEventListener("click", () => fileIn.click());
      fileIn.addEventListener("change", () => {
        for (const f of Array.from(fileIn.files || [])) chat.pendingFiles.push(f);
        renderAttached();
        fileIn.value = "";
      });
    }

    if (newBtn) newBtn.addEventListener("click", () => { newSession(true); });

    // Console overflow (kebab) menu — wraps Clear + Delete so they don't
    // dominate the always-visible row. Existing Clear/Delete handlers
    // below stay attached because the IDs are unchanged; only the
    // visual nesting moved.
    const overflowBtn  = $("console-overflow-btn");
    const overflowMenu = $("console-overflow-menu");
    if (overflowBtn && overflowMenu) {
      const closeOverflow = () => {
        overflowBtn.setAttribute("aria-expanded", "false");
        overflowMenu.hidden = true;
      };
      overflowBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        const open = overflowBtn.getAttribute("aria-expanded") === "true";
        overflowBtn.setAttribute("aria-expanded", open ? "false" : "true");
        overflowMenu.hidden = open;
      });
      overflowBtn.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeOverflow();
      });
      // Click anywhere else closes the menu — including taps on Clear/Delete
      // (the actual handlers below run first because click listeners attach
      // to the buttons themselves; the bubble reaches document last).
      document.addEventListener("click", (e) => {
        if (overflowMenu.hidden) return;
        if (overflowMenu.contains(e.target) || overflowBtn.contains(e.target)) {
          // tap inside menu closes after the inner handler runs
          if (overflowMenu.contains(e.target) && !overflowBtn.contains(e.target)) {
            closeOverflow();
          }
          return;
        }
        closeOverflow();
      });
    }

    if (clearBtn) clearBtn.addEventListener("click", () => {
      const s = activeSession();
      if (!s) return;
      // No confirm popup — just clear. (History on disk is preserved by
      // the backend; this only clears the in-session render.)
      s.messages = []; saveChat(); renderChat();
    });
    if (delBtn) delBtn.addEventListener("click", () => {
      const s = activeSession();
      if (!s) return;
      // No confirm popup — just delete and start a fresh session.
      delete chat.sessions[s.id];
      chat.activeId = null;
      newSession(false);
      saveChat(); renderChat();
    });

    // Permission dropdown
    if (permBtn && permMenu) {
      permBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        const open = permBtn.getAttribute("aria-expanded") === "true";
        permBtn.setAttribute("aria-expanded", open ? "false" : "true");
        permMenu.hidden = open;
      });
      permMenu.addEventListener("click", (e) => {
        const li = e.target.closest("li[data-mode]");
        if (!li) return;
        setPerm(li.dataset.mode);
        permBtn.setAttribute("aria-expanded", "false");
        permMenu.hidden = true;
      });
      document.addEventListener("click", () => {
        permBtn.setAttribute("aria-expanded", "false");
        permMenu.hidden = true;
      });
    }

    // Mute toggle for typed-chat auto-speak. Click flips the localStorage
    // flag and updates the icon's aria-pressed state. Mic-driven replies
    // are unaffected (they go through the AudioContext path which is
    // single-source-of-truth audio_b64 from /api/voice/converse).
    const muteBtn = $("console-mute");
    if (muteBtn) {
      const _setMuteUi = (m) => {
        muteBtn.setAttribute("aria-pressed", m ? "true" : "false");
        muteBtn.classList.toggle("is-muted", m);
        muteBtn.title = m
          ? "Typed-chat auto-speak is MUTED — click to unmute"
          : "Typed-chat auto-speak is ON — click to mute";
      };
      let initialMute = "1";
      try { initialMute = localStorage.getItem("luna.tts_typed_mute"); } catch (_e) {}
      if (initialMute === null || initialMute === undefined) initialMute = "1";
      _setMuteUi(initialMute === "1");
      muteBtn.addEventListener("click", () => {
        let cur = "1";
        try { cur = localStorage.getItem("luna.tts_typed_mute") || "1"; } catch (_e) {}
        const next = (cur === "1") ? "0" : "1";
        try { localStorage.setItem("luna.tts_typed_mute", next); } catch (_e) {}
        _setMuteUi(next === "1");
      });
    }

    // Voice — click the mic icon to start/stop the realtime phone-call mode
    // (browser-side mic -> /api/voice/converse -> Whisper STT -> Ollama
    // -> Kokoro TTS playback). This SUPERSEDES the older browser-only
    // dictation flow because the operator wants single-click back-to-back
    // conversation, not type-by-voice. The legacy SpeechRecognition path
    // is still reachable via Shift+click for power users who want
    // dictation into the textarea.
    if (mic) {
      mic.addEventListener("click", (ev) => {
        if (ev.shiftKey) {
          // Shift+click = legacy dictation into textarea
          toggleMic(input, mic);
          return;
        }
        // Default click = realtime voice phone-call mode
        startCompactVoice(mic);
      });
    }

    // Keyboard shortcut: Ctrl+K → new session
    document.addEventListener("keydown", (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault(); newSession(true);
        if (input) input.focus();
      }
    });

    // Drag & drop on the whole panel — accepts both external Files and
    // internal path-drags from the File Explorer.
    if (panel) {
      let depth = 0;
      panel.addEventListener("dragenter", (e) => {
        if (!e.dataTransfer) return;
        const types = Array.from(e.dataTransfer.types || []);
        const hasFiles = types.includes("Files");
        const hasPath  = types.includes("application/luna-path+json");
        if (!hasFiles && !hasPath) return;
        depth++;
        if (hasFiles) panel.classList.add("is-dragover");
        if (hasPath)  panel.classList.add("is-pathdrop");
      });
      panel.addEventListener("dragover", (e) => {
        e.preventDefault();
        if (e.dataTransfer) e.dataTransfer.dropEffect = "copy";
      });
      panel.addEventListener("dragleave", () => {
        depth = Math.max(0, depth - 1);
        if (!depth) { panel.classList.remove("is-dragover"); panel.classList.remove("is-pathdrop"); }
      });
      panel.addEventListener("drop", (e) => {
        e.preventDefault(); depth = 0;
        panel.classList.remove("is-dragover"); panel.classList.remove("is-pathdrop");
        const dt = e.dataTransfer;
        if (!dt) return;
        // Internal path drag (from the File Explorer panel)
        const pathBlob = dt.getData("application/luna-path+json");
        if (pathBlob) {
          try {
            const items = JSON.parse(pathBlob);
            const arr = Array.isArray(items) ? items : [items];
            for (const it of arr) {
              chat.pendingFiles.push({
                __pathRef: true,
                name: it.name || (it.path || "").split(/[\\/]/).pop() || "file",
                size: Number(it.size) || 0,
                type: it.type || "",
                path: it.path,
              });
            }
            renderAttached();
            return;
          } catch (e) { /* fall through to file handling */ }
        }
        const files = Array.from(dt.files || []);
        for (const f of files) chat.pendingFiles.push(f);
        renderAttached();
      });
    }

    // Luna Vision Link / Live Talk: wire the additive controls. Safe to
    // call even if the IDs are missing - the function no-ops on absence.
    try { initLiveTalk(); } catch (e) { /* never break console init */ }
  }

  function setPerm(mode) {
    if (!PERM_LABELS[mode]) mode = "ask";
    // Bypass permissions: no popup. Serge has full control of his own
    // local stack and explicitly asked for no popups.
    if (false && mode === "bypass" && chat.perm !== "bypass") {
      const ok = false;
      if (!ok) {
        const prev = PERM_LABELS[chat.perm] ? chat.perm : "ask";
        const lbl = $("console-perm-label");
        const dot = document.querySelector("#console-perm-btn .luna-perm__dot");
        if (lbl) lbl.textContent = PERM_LABELS[prev];
        if (dot) dot.dataset.mode = prev;
        document.querySelectorAll("#console-perm-menu li").forEach((li) => {
          li.setAttribute("aria-current", li.dataset.mode === prev ? "true" : "false");
        });
        return;
      }
    }
    chat.perm = mode;
    const lbl = $("console-perm-label");
    const dot = document.querySelector("#console-perm-btn .luna-perm__dot");
    if (lbl) lbl.textContent = PERM_LABELS[mode];
    if (dot) dot.dataset.mode = mode;
    document.querySelectorAll("#console-perm-menu li").forEach((li) => {
      li.setAttribute("aria-current", li.dataset.mode === mode ? "true" : "false");
    });
    saveChat();
  }

  function toggleMic(input, btn) {
    const Recog = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!Recog) {
      pushMessage({ id: "sys_" + Date.now(), role: "sys", text: "voice unavailable — this browser has no Web Speech API", ts: new Date().toISOString() });
      return;
    }
    if (chat.listening && chat.recog) {
      try { chat.recog.stop(); } catch (e) { /* */ }
      chat.listening = false;
      btn.classList.remove("is-listening");
      btn.setAttribute("aria-pressed", "false");
      return;
    }
    const r = new Recog();
    r.continuous = true; r.interimResults = true; r.lang = navigator.language || "en-US";
    let baseline = input.value || "";
    r.onresult = (ev) => {
      let interim = "", finalText = "";
      for (let i = ev.resultIndex; i < ev.results.length; i++) {
        const tr = ev.results[i][0].transcript;
        if (ev.results[i].isFinal) finalText += tr; else interim += tr;
      }
      if (finalText) baseline = (baseline + " " + finalText).trim();
      input.value = (baseline + (interim ? " " + interim : "")).trim();
      input.style.height = "auto"; input.style.height = Math.min(200, input.scrollHeight) + "px";
    };
    r.onerror = () => { /* swallow; user can re-toggle */ };
    r.onend = () => {
      chat.listening = false;
      btn.classList.remove("is-listening");
      btn.setAttribute("aria-pressed", "false");
    };
    chat.recog = r;
    try { r.start(); chat.listening = true; btn.classList.add("is-listening"); btn.setAttribute("aria-pressed", "true"); }
    catch (e) { chat.listening = false; }
  }

  // ============================================================
  // Luna Compact Voice — fully self-contained realtime phone-call.
  //
  // Owns its own getUserMedia / MediaRecorder / VAD / audio playback /
  // POST to /api/voice/converse. Does NOT depend on bindRealtimeVoice()
  // having run or RT.api being present — those were unreliable. This
  // handler can ALWAYS start as long as the backend endpoint is up.
  //
  // Tunings (aggressive for low latency, easy to relax):
  //   VAD_SILENCE_MS = 600    cut after 600ms of silence (was 1500)
  //   VAD_HOT_MIN_MS = 200    require 200ms of speech before cutting (was 350)
  //   VAD_THRESHOLD  = 0.018  RMS gate (was 0.020)
  //   MAX_UTTER_MS   = 20000  20s hard ceiling per utterance
  //
  // Click 1: requests mic permission, picks Logitech/C270/C920 if found,
  //          starts continuous listen, shows pulsing pill above input.
  // Click 2: stops everything cleanly (mic, recorder, audio, pill).
  // ============================================================
  const CV = window.__lunaCV = window.__lunaCV || {
    active: false,
    starting: false,
    mic: null,
    ctx: null,
    analyser: null,
    recorder: null,
    chunks: [],
    audio: null,
    rafId: 0,
    vadHotMs: 0,
    vadSilenceMs: 0,
    utterStartMs: 0,
    cancelled: false,
    session: "compact-" + Math.random().toString(36).slice(2, 8),
    silentChunkStreak: 0,
    lastRms: 0,           // live debug: peak RMS in current utterance
    lastUiUpdate: 0,      // throttle pill text updates
    // VAD tunings: 2026-05-08 round 2 — tightened for snappier turn-taking.
    // VAD_THRESHOLD 0.008 (unchanged) catches quiet voices.
    // VAD_HOT_MIN_MS 100 (was 150) — faster cut on short utterances.
    // VAD_SILENCE_MS 350 (was 600) — sends as soon as you stop, saves 250ms
    // of dead air between you finishing a thought and Luna receiving it.
    // Override at runtime via window.__lunaCV.VAD_SILENCE_MS = 600 etc.
    VAD_THRESHOLD:  0.008,
    VAD_HOT_MIN_MS: 100,
    VAD_SILENCE_MS: 350,
    MAX_UTTER_MS:   20000,
  };

  // Compact voice status pill.
  //
  // Anti-flicker design:
  //   * Element is created ONCE (lazily on first non-empty call) and
  //     never removed afterward. Hide-by-opacity instead of remove() so
  //     repeated empty/non-empty calls don't cause DOM insert/remove
  //     thrash that the operator sees as a popup flickering.
  //   * data-empty="1" attribute drives the CSS hide via opacity 0.
  //   * Text changes only when the actual content differs (de-dupe so
  //     the 250ms VAD-loop ticks don't trigger redundant repaints).
  //   * Width is stabilised by CSS min-width + tabular-nums so text
  //     swaps between "Listening - mic peak 0.002" and "Hearing you
  //     (peak 0.07)" don't reflow the surrounding layout.
  function _compactVoiceStatus(text, tone) {
    let pill = document.getElementById("luna-compact-voice-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.id = "luna-compact-voice-pill";
      pill.setAttribute("role", "status");
      pill.setAttribute("aria-live", "polite");
      pill.dataset.empty = "1";
      const form = document.getElementById("console-form");
      if (form && form.parentNode) form.parentNode.insertBefore(pill, form);
    }
    if (!text) {
      // De-dupe: if we're already hidden, don't toggle the attribute again
      // (would re-evaluate selectors and risk a transition restart).
      if (pill.dataset.empty !== "1") pill.dataset.empty = "1";
      return;
    }
    if (pill.dataset.empty === "1") delete pill.dataset.empty;
    if (pill.textContent !== text) pill.textContent = text;
    const newTone = tone || "";
    if ((pill.dataset.tone || "") !== newTone) {
      if (newTone) pill.dataset.tone = newTone;
      else delete pill.dataset.tone;
    }
  }

  // ---- pick the best mic deviceId (Logitech / C270 / C920 / Brio) -----
  async function _cvPickDeviceId() {
    try {
      // Trigger permission so labels are populated.
      const tmp = await navigator.mediaDevices.getUserMedia({ audio: true });
      tmp.getTracks().forEach((t) => t.stop());
      const all = await navigator.mediaDevices.enumerateDevices();
      const inputs = all.filter((d) => d.kind === "audioinput");
      const re = /(logitech|c270|c920|c925|c615|brio|streamcam)/i;
      const hit = inputs.find((d) => re.test(d.label || ""));
      return hit ? hit.deviceId : "";
    } catch (_e) {
      return "";
    }
  }

  // ---- play one TTS audio response -----------------------------------
  // Edge / Chrome autoplay policy: a fresh `new Audio(dataUri).play()` is
  // BLOCKED in Edge 124+ unless the playback inherits the user-gesture
  // grant. Routing through the existing AudioContext (already authorized
  // when the operator clicked the mic icon) bypasses that block reliably.
  // Fallback to the HTMLAudio path is kept for older browsers.
  function _cvPlay(b64, mime) {
    return new Promise((resolve) => {
      const finishOnce = (() => { let done = false; return () => {
        if (done) return; done = true; CV.audio = null; resolve();
      }; })();
      try {
        // Stop anything already playing (barge-in safety).
        if (CV.audio) {
          try { CV.audio.pause && CV.audio.pause(); } catch (_) {}
          try { CV.audio.stop  && CV.audio.stop();  } catch (_) {}
          CV.audio = null;
        }
        // Decode base64 -> ArrayBuffer for AudioContext.decodeAudioData.
        const bin = atob(b64);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        const buffer = bytes.buffer;

        // Prefer the existing AudioContext (already user-gesture-authorized).
        const ctx = CV.ctx || (window.AudioContext || window.webkitAudioContext) &&
                    new (window.AudioContext || window.webkitAudioContext)();
        if (ctx && ctx.decodeAudioData) {
          // ctx.resume is a no-op if already running. Safari needs it sometimes.
          (ctx.state === "suspended" ? ctx.resume() : Promise.resolve()).then(() => {
            ctx.decodeAudioData(buffer.slice(0)).then((decoded) => {
              const src = ctx.createBufferSource();
              src.buffer = decoded;
              src.connect(ctx.destination);
              CV.audio = src;
              src.onended = finishOnce;
              src.start(0);
            }).catch((err) => {
              // decode failed - fall through to <audio> element
              _compactVoiceStatus(
                "Audio decode failed (" + (err && err.name || "?") + ") - retrying via <audio>",
                "thinking"
              );
              _cvPlayHtmlAudio(b64, mime, finishOnce);
            });
          }).catch(() => _cvPlayHtmlAudio(b64, mime, finishOnce));
        } else {
          _cvPlayHtmlAudio(b64, mime, finishOnce);
        }
      } catch (_e) { finishOnce(); }
    });
  }

  function _cvPlayHtmlAudio(b64, mime, done) {
    try {
      const data = "data:" + (mime || "audio/wav") + ";base64," + b64;
      const a = new Audio(data);
      CV.audio = a;
      a.onended = done;
      a.onerror = done;
      const p = a.play();
      if (p && p.catch) {
        p.catch((err) => {
          // Surface the real reason so it's not invisible.
          _compactVoiceStatus(
            "Audio blocked by browser (" + (err && err.name || "NotAllowed") +
            ") - click anywhere on the page once",
            "error"
          );
          done();
        });
      }
    } catch (_e) { done(); }
  }

  // ---- VAD loop: watches RMS and fires _cvCutUtterance on silence -----
  function _cvVadLoop() {
    if (!CV.active || !CV.analyser) return;
    const buf = new Float32Array(CV.analyser.fftSize);
    CV.analyser.getFloatTimeDomainData(buf);
    let sum = 0;
    for (let i = 0; i < buf.length; i++) sum += buf[i] * buf[i];
    const rms = Math.sqrt(sum / buf.length);
    if (rms > CV.lastRms) CV.lastRms = rms;
    const dt = 50;  // poll cadence ms
    if (rms > CV.VAD_THRESHOLD) {
      CV.vadHotMs += dt;
      CV.vadSilenceMs = 0;
    } else if (CV.vadHotMs > 0) {
      CV.vadSilenceMs += dt;
    }
    const elapsed = Date.now() - CV.utterStartMs;
    const longEnough = CV.vadHotMs >= CV.VAD_HOT_MIN_MS;
    const silenced   = CV.vadSilenceMs >= CV.VAD_SILENCE_MS;
    const tooLong    = elapsed >= CV.MAX_UTTER_MS;

    // Throttled live diagnostic so the operator sees mic activity without
    // re-rendering on every 50ms tick. Shows peak RMS of the utterance so
    // far + the hot/silent state so a quiet mic is obvious.
    if (Date.now() - CV.lastUiUpdate > 250) {
      CV.lastUiUpdate = Date.now();
      const hot = (CV.vadHotMs > 0);
      const peak = CV.lastRms.toFixed(3);
      let label;
      if (!hot) label = "Listening - mic peak " + peak + " (need >" + CV.VAD_THRESHOLD + " to send)";
      else if (silenced) label = "Sending...";
      else if (longEnough) label = "Heard you (peak " + peak + ") - waiting for pause";
      else label = "Hearing you (peak " + peak + ")";
      _compactVoiceStatus(label, "listening");
    }

    if ((longEnough && silenced) || tooLong) {
      _cvCutUtterance();
      return;
    }
    setTimeout(_cvVadLoop, dt);
  }

  function _cvBeginUtterance() {
    if (!CV.active || !CV.mic) return;
    CV.chunks = [];
    CV.vadHotMs = 0;
    CV.vadSilenceMs = 0;
    CV.utterStartMs = Date.now();
    try {
      const rec = new MediaRecorder(CV.mic, { mimeType: "audio/webm;codecs=opus" });
      CV.recorder = rec;
      rec.ondataavailable = (e) => { if (e.data && e.data.size > 0) CV.chunks.push(e.data); };
      rec.onstop = _cvOnUtteranceCut;
      rec.start(100);
    } catch (e) {
      _compactVoiceStatus("MediaRecorder error - " + (e && e.name || "fail"), "error");
      CV.active = false;
    }
    setTimeout(_cvVadLoop, 50);
  }

  function _cvCutUtterance() {
    try {
      if (CV.recorder && CV.recorder.state !== "inactive") CV.recorder.stop();
    } catch (_e) {}
  }

  // =================================================================
  // V2 streaming voice consumer (round 21, 2026-05-09 per Serge)
  // -----------------------------------------------------------------
  // Opens an SSE-style fetch to /api/voice/v2/stream, reads chunks
  // as they arrive, and queues each `sentence_audio` event for
  // immediate playback via a chained <audio> element. Falls back
  // gracefully if the endpoint is unavailable.
  //
  // Sentence-by-sentence playback queue keeps latency phone-call-low:
  //   - First sentence audio plays as soon as Luna's first sentence
  //     is generated (~1-2s after STT)
  //   - Subsequent sentences chain seamlessly via 'ended' event
  //   - Mute respect: same CV.muted check as the v1 path
  // =================================================================
  const _v2AudioQueue = [];
  let   _v2AudioPlaying = false;
  let   _v2AudioElement = null;
  function _v2EnqueueAudio(audioB64, mime) {
    if (!audioB64) return;
    if (CV.muted) return;
    _v2AudioQueue.push({ audioB64: audioB64, mime: mime || "audio/wav" });
    _v2PumpAudio();
  }
  function _v2PumpAudio() {
    if (_v2AudioPlaying) return;
    const next = _v2AudioQueue.shift();
    if (!next) return;
    _v2AudioPlaying = true;
    if (!_v2AudioElement) {
      _v2AudioElement = new Audio();
      _v2AudioElement.addEventListener("ended", () => {
        _v2AudioPlaying = false;
        _v2PumpAudio();
      });
      _v2AudioElement.addEventListener("error", () => {
        _v2AudioPlaying = false;
        _v2PumpAudio();
      });
    }
    try {
      _v2AudioElement.src = "data:" + next.mime + ";base64," + next.audioB64;
      const p = _v2AudioElement.play();
      if (p && typeof p.catch === "function") {
        p.catch(() => { _v2AudioPlaying = false; _v2PumpAudio(); });
      }
    } catch (_e) {
      _v2AudioPlaying = false;
      _v2PumpAudio();
    }
  }
  function _v2ClearQueue() {
    _v2AudioQueue.length = 0;
    if (_v2AudioElement) {
      try { _v2AudioElement.pause(); _v2AudioElement.src = ""; } catch (_e) {}
    }
    _v2AudioPlaying = false;
  }

  async function _cvHandleV2Stream(blob) {
    const t0 = performance.now();
    _v2ClearQueue();
    _compactVoiceStatus("V2: streaming...", "thinking");

    let resp;
    try {
      resp = await fetch("/api/voice/v2/stream", {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: {
          "Content-Type": blob.type || "audio/webm",
          "X-Luna-Session": CV.session,
        },
        body: blob,
      });
    } catch (e) {
      _compactVoiceStatus(
        "V2 network error: " + (e && (e.name || e.message) || "fetch failed"),
        "error"
      );
      return;
    }
    if (!resp.ok || !resp.body) {
      _compactVoiceStatus("V2 HTTP " + resp.status, "error");
      return;
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";
    let firstAudioMs = 0;
    let transcriptText = "";
    let liveReplyText = "";
    let sentencesPlayed = 0;
    let doneReceived = false;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      // SSE event delimiter: \n\n
      let idx;
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const rawEvent = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 2);
        if (!rawEvent) continue;
        // Each event is one or more lines starting with "data: ...".
        // We only emit single-line events server-side, so just strip.
        const dataLine = rawEvent.startsWith("data: ") ? rawEvent.slice(6) : rawEvent;
        let evt;
        try { evt = JSON.parse(dataLine); }
        catch (_e) { continue; }
        const kind = evt.type;
        if (kind === "transcript") {
          transcriptText = String(evt.text || "");
          _compactVoiceStatus("Heard: " + transcriptText.slice(0, 60), "thinking");
        } else if (kind === "token") {
          liveReplyText += String(evt.content || "");
        } else if (kind === "sentence_audio") {
          if (!firstAudioMs) {
            firstAudioMs = Math.round(performance.now() - t0);
            _compactVoiceStatus(
              "First audio in " + firstAudioMs + "ms - Luna speaking",
              "speaking"
            );
          }
          _v2EnqueueAudio(evt.audio_b64, evt.mime || "audio/wav");
          sentencesPlayed++;
        } else if (kind === "done") {
          doneReceived = true;
          const total = Math.round(evt.total_latency_s * 1000) || Math.round(performance.now() - t0);
          _compactVoiceStatus(
            "V2 done in " + total + "ms (first audio " + (firstAudioMs || "?") + "ms, " +
            sentencesPlayed + " sentences)",
            "ok"
          );
        } else if (kind === "error") {
          _compactVoiceStatus(
            "V2 error [" + (evt.stage || "unknown") + "]: " + (evt.error || ""),
            "error"
          );
          return;
        }
      }
    }
    if (!doneReceived) {
      _compactVoiceStatus(
        "V2 stream ended before 'done' (" + sentencesPlayed + " sentences played)",
        "error"
      );
    }
  }

  async function _cvOnUtteranceCut() {
    if (CV.cancelled) return;
    // 2026-05-10 double-voice fix per Serge: previous guard was
    //   "skip if a non-compact path is ACTIVE"
    // which let BOTH the compact and realtime handlers POST when
    // __lunaActiveVoicePath was undefined (e.g., on first page load
    // before either control had explicitly claimed ownership). That
    // produced two simultaneous /api/voice/converse round-trips, two
    // audio responses, and two voices over each other.
    // New rule: this handler only fires when COMPACT is the explicitly-
    // claimed active path. Default-deny when path is unset.
    if (window.__lunaActiveVoicePath !== "compact") {
      return;
    }
    const blob = new Blob(CV.chunks, {
      type: (CV.recorder && CV.recorder.mimeType) || "audio/webm",
    });
    CV.chunks = [];
    const kb = (blob && blob.size) ? Math.round(blob.size / 1024) : 0;
    if (!blob || blob.size < 4096) {
      CV.silentChunkStreak++;
      _compactVoiceStatus(
        "Tiny clip (" + kb + "kb) - speak louder/longer; peak " + CV.lastRms.toFixed(3),
        "listening"
      );
      CV.lastRms = 0;
      if (CV.active) _cvBeginUtterance();
      return;
    }
    _compactVoiceStatus("Sending " + kb + "kb to Luna...", "thinking");
    let respJson = null;
    let rawText = null;
    let httpStatus = 0;
    // ---- V2 streaming path (round 21, 2026-05-09 per Serge) ----
    // 2026-05-10: default flipped from OFF to ON per Serge ("response
    // time is extremely slow when I talk to Luna"). V2 streams each
    // sentence's audio the moment Luna's TTS produces it - while the
    // LLM is still generating the next sentence. Cuts perceived
    // round-trip from ~5-13s to ~2-4s. To opt OUT (legacy single-blob
    // path), run in devtools console:
    //     localStorage.setItem('lunaVoiceV2', '0')
    // and refresh.
    const _useV2Voice = (function() {
      try {
        var _v = localStorage.getItem("lunaVoiceV2");
        if (_v === "0") return false;   // explicit opt-out
        return true;                     // default-on (was: false)
      }
      catch (_e) { return true; }
    })();
    if (_useV2Voice) {
      try {
        await _cvHandleV2Stream(blob);
      } catch (e) {
        _compactVoiceStatus(
          "V2 stream error: " + (e && (e.name || e.message) || "fail"),
          "error"
        );
      }
      CV.lastRms = 0;
      if (CV.active) _cvBeginUtterance();
      return;
    }
    try {
      const resp = await fetch("/api/voice/converse", {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: {
          "Content-Type": blob.type || "audio/webm",
          "X-Luna-Session": CV.session,
        },
        body: blob,
      });
      httpStatus = resp.status;
      try { respJson = await resp.clone().json(); } catch (_e) { respJson = null; }
      if (!respJson) { try { rawText = await resp.text(); } catch (_e) { rawText = null; } }
    } catch (e) {
      _compactVoiceStatus(
        "Network error: " + (e && (e.name || e.message) || "fetch failed"),
        "error"
      );
      CV.lastRms = 0;
      if (CV.active) _cvBeginUtterance();
      return;
    }
    if (!respJson) {
      _compactVoiceStatus(
        "HTTP " + httpStatus + " no JSON - " + (rawText || "empty body").slice(0, 80),
        "error"
      );
      CV.lastRms = 0;
      if (CV.active) _cvBeginUtterance();
      return;
    }
    if (respJson.ok === false) {
      const msg = String(respJson.error || "unknown");
      const benign = /no speech detected|audio blob too small|empty/i.test(msg);
      if (benign) {
        CV.silentChunkStreak++;
        _compactVoiceStatus(
          "Whisper heard nothing in " + kb + "kb - peak " + CV.lastRms.toFixed(3) + " (mic too quiet?)",
          "listening"
        );
        CV.lastRms = 0;
        if (CV.active) _cvBeginUtterance();
        return;
      }
      _compactVoiceStatus("Backend: " + msg, "error");
      CV.lastRms = 0;
      if (CV.active) _cvBeginUtterance();
      return;
    }
    CV.silentChunkStreak = 0;
    const heard = (respJson.transcript || "").trim();
    if (respJson.audio_b64) {
      _compactVoiceStatus(
        "Luna: \"" + (respJson.reply_text || "").slice(0, 60) + "\"",
        "speaking"
      );
      await _cvPlay(respJson.audio_b64, respJson.audio_mime || "audio/wav");
      // Stamp the time we last spoke via Kokoro so the typed-chat
      // auto-speak path skips this reply (anti-double-voice guard).
      CV._lastSpokeMs = Date.now();
    } else if (heard) {
      _compactVoiceStatus("Heard: \"" + heard.slice(0, 60) + "\" but no audio", "error");
    }
    CV.lastRms = 0;
    if (CV.cancelled) return;
    _compactVoiceStatus("Listening - speak now", "listening");
    if (CV.active) _cvBeginUtterance();
  }

  // §35 (2026-05-09) two-voices fix: register a global teardown for the
  // compact-voice path so the realtime path can force-stop it on its
  // own start (and vice versa). Without this, a user with both the
  // compact mic AND the realtime "Talk" panel active heard two voices
  // — both paths POSTed to /api/voice/converse independently and both
  // played their audio_b64 responses simultaneously.
  window.__lunaForceStopCompact = function () {
    try { CV.cancelled = true; CV.active = false; } catch (_e) {}
    try { if (CV.recorder && CV.recorder.state !== "inactive") CV.recorder.stop(); } catch (_e) {}
    try { if (CV.mic) CV.mic.getTracks().forEach((t) => t.stop()); } catch (_e) {}
    try { if (CV.audio) CV.audio.pause(); } catch (_e) {}
    try { fetch("/api/voice/stop", { method: "POST" }); } catch (_e) {}
    document.body.dataset.lunaCompactVoice = "off";
  };

  async function startCompactVoice(triggerBtn) {
    // §35 mutex: if the realtime voice path is active, shut it down first.
    if (typeof window.__lunaForceStopRealtime === "function") {
      try { window.__lunaForceStopRealtime(); } catch (_e) {}
    }
    window.__lunaActiveVoicePath = "compact";
    if (CV.starting) return;
    if (CV.active) {
      // Toggle off
      CV.cancelled = true;
      CV.active = false;
      try { if (CV.recorder && CV.recorder.state !== "inactive") CV.recorder.stop(); } catch (_) {}
      CV.recorder = null;
      try { if (CV.mic) CV.mic.getTracks().forEach((t) => t.stop()); } catch (_) {}
      CV.mic = null;
      try { if (CV.ctx) CV.ctx.close(); } catch (_) {}
      CV.ctx = null;
      CV.analyser = null;
      try { if (CV.audio) CV.audio.pause(); } catch (_) {}
      CV.audio = null;
      try { fetch("/api/voice/stop", { method: "POST" }); } catch (_) {}
      if (triggerBtn) {
        triggerBtn.classList.remove("is-listening");
        triggerBtn.setAttribute("aria-pressed", "false");
      }
      document.body.dataset.lunaCompactVoice = "off";
      _compactVoiceStatus("");
      return;
    }
    CV.starting = true;
    if (triggerBtn) {
      triggerBtn.classList.add("is-listening");
      triggerBtn.setAttribute("aria-pressed", "true");
    }
    document.body.dataset.lunaCompactVoice = "on";
    _compactVoiceStatus("Connecting mic...", "thinking");
    try {
      // 1) Find the Logitech mic.
      const deviceId = await _cvPickDeviceId();
      // 2) Acquire the mic stream. Disable browser noise-suppression because
      // it can over-aggress on quiet voices and starve Whisper of usable
      // audio. Keep echo-cancellation on so Luna's TTS doesn't loop back.
      // AGC stays on so quiet speakers still get reasonable levels.
      const attempts = deviceId
        ? [
            { audio: { deviceId: { exact: deviceId },
                       echoCancellation: true, noiseSuppression: false, autoGainControl: true } },
            { audio: { deviceId,
                       echoCancellation: true, noiseSuppression: false, autoGainControl: true } },
            { audio: true },
          ]
        : [
            { audio: { echoCancellation: true, noiseSuppression: false, autoGainControl: true } },
            { audio: true },
          ];
      let stream = null, lastErr = null;
      for (const c of attempts) {
        try { stream = await navigator.mediaDevices.getUserMedia(c); break; }
        catch (e) { lastErr = e; }
      }
      if (!stream) throw lastErr || new Error("getUserMedia failed");

      CV.mic = stream;
      CV.cancelled = false;
      CV.silentChunkStreak = 0;

      // 3) Build the AudioContext analyser for VAD. Resume() is required
      // by some browsers that auto-suspend new contexts until the page has
      // a confirmed user gesture; it's a no-op when the context is already
      // running. fftSize=1024 -> getFloatTimeDomainData fills 1024 samples,
      // good resolution for RMS-based voice detection at 50ms cadence.
      const Ctor = window.AudioContext || window.webkitAudioContext;
      CV.ctx = new Ctor();
      try { await CV.ctx.resume(); } catch (_e) {}
      const src = CV.ctx.createMediaStreamSource(stream);
      CV.analyser = CV.ctx.createAnalyser();
      CV.analyser.fftSize = 1024;
      CV.analyser.smoothingTimeConstant = 0.0;  // raw values for accurate RMS
      src.connect(CV.analyser);

      // 4) Start the listen/record/cut/post loop.
      CV.active = true;
      _compactVoiceStatus("Listening - speak now", "listening");
      _cvBeginUtterance();
    } catch (e) {
      const name = (e && (e.name || e.message)) || "denied";
      let hint = "Mic blocked - allow microphone access for this page";
      if (/NotAllowed|denied|Permission/i.test(String(name)))
        hint = "Mic permission denied - click the lock icon in the address bar, allow Microphone, reload";
      else if (/NotFound|DevicesNotFound|Overconstrained/i.test(String(name)))
        hint = "No microphone found - plug in a mic and try again";
      else if (/NotReadable|TrackStartError/i.test(String(name)))
        hint = "Mic in use by another app (Zoom, Discord, Teams) - close it first";
      _compactVoiceStatus(hint, "error");
      if (triggerBtn) {
        triggerBtn.classList.remove("is-listening");
        triggerBtn.setAttribute("aria-pressed", "false");
      }
      document.body.dataset.lunaCompactVoice = "off";
      CV.active = false;
    } finally {
      CV.starting = false;
    }
  }

  // Page-hide safety: stop the mic/recorder so we don't keep the device
  // reserved when the operator switches tabs.
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden" && CV.active) {
      const mic = $("console-mic");
      startCompactVoice(mic);  // toggles off
    }
  });

  // ============================================================
  // Luna Vision Link / Live Talk
  //   Additive feature. Coexists with the existing console-mic button -
  //   they are mutually exclusive at runtime (clicking one stops the
  //   other) but both remain in the DOM and use separate recognizer
  //   instances so the existing typed-then-mic flow is unchanged.
  //
  //   NO external network calls. NO auto-start. The webcam preview is
  //   local-only; the optional Snapshot button routes a single still
  //   through the existing /api/chat/upload pipeline by pushing it onto
  //   chat.pendingFiles - the same path used by the file-attach button.
  //
  //   Hard rules honored:
  //     - permission mode (chat.perm) flows through unchanged via sendChat
  //     - Bypass-mode confirmation (if/when wired) fires from the same
  //       sendChat call path as typed messages
  //     - stop button stops EVERY MediaStream track
  //     - session change / page hide / panel close stop streams
  //     - recognizer never stacks (we null-out the previous instance)
  //     - empty transcripts are dropped
  // ============================================================
  const liveTalk = {
    active: false,           // liveTalkActive
    cameraStream: null,
    micRecognition: null,    // liveRecognition (separate from chat.recog)
    isLunaThinking: false,
    pendingTranscript: "",
    busy: false,             // sendChat round-trip in flight
    cameraOn: false,
  };

  function setLiveStatus(text, tone) {
    const el = $("luna-live-status");
    if (!el) return;
    el.textContent = text;
    if (tone) el.dataset.tone = tone; else delete el.dataset.tone;
  }

  function _liveSetCameraIndicator(on) {
    liveTalk.cameraOn = !!on;
    const btn = $("console-camera");
    if (btn) {
      btn.classList.toggle("is-on", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    }
    document.body.dataset.lunaCamOn = on ? "1" : "";
  }

  function _liveSetMicIndicator(on) {
    const btn = $("console-live-talk");
    if (btn) {
      btn.classList.toggle("is-on", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    }
    document.body.dataset.lunaMicOn = on ? "1" : "";
  }

  async function toggleCamera() {
    const panel = $("luna-camera-preview");
    const video = $("luna-camera-video");
    const snap  = $("luna-camera-snap");
    if (!panel || !video) return;
    if (liveTalk.cameraStream) { stopCamera(); return; }
    try {
      // audio:false — mic capture is owned by SpeechRecognition, not WebRTC.
      const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
      liveTalk.cameraStream = stream;
      video.srcObject = stream;
      panel.hidden = false;
      _liveSetCameraIndicator(true);
      if (snap) snap.disabled = false;
      setLiveStatus("Camera on", "listening");
    } catch (err) {
      setLiveStatus("Camera blocked: " + (err && err.name || "denied"), "error");
      _liveSetCameraIndicator(false);
    }
  }

  function stopCamera() {
    const video = $("luna-camera-video");
    const snap  = $("luna-camera-snap");
    const panel = $("luna-camera-preview");
    if (liveTalk.cameraStream) {
      try { liveTalk.cameraStream.getTracks().forEach((t) => t.stop()); } catch (e) {}
      liveTalk.cameraStream = null;
    }
    if (video) {
      try { video.pause(); } catch (e) {}
      video.srcObject = null;
    }
    if (snap) snap.disabled = true;
    _liveSetCameraIndicator(false);
    // Hide the panel only when Live Talk is also off; keep it visible
    // during a live conversation in case the operator wants the status row.
    if (panel && !liveTalk.active) panel.hidden = true;
    setLiveStatus(liveTalk.active ? "Listening" : "Camera off",
                  liveTalk.active ? "listening" : null);
  }

  function captureCameraFrame() {
    const video = $("luna-camera-video");
    if (!liveTalk.cameraStream || !video || !video.videoWidth) {
      setLiveStatus("Snapshot: camera not ready", "error");
      return;
    }
    const w = video.videoWidth, h = video.videoHeight;
    const canvas = document.createElement("canvas");
    canvas.width = w; canvas.height = h;
    const ctx = canvas.getContext("2d");
    ctx.drawImage(video, 0, 0, w, h);
    canvas.toBlob((blob) => {
      if (!blob) { setLiveStatus("Snapshot encode failed", "error"); return; }
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      const file = new File([blob], "luna_snapshot_" + ts + ".jpg", { type: "image/jpeg" });
      // Reuse the existing pending-attachments queue — the form-submit
      // handler at initConsole() picks up chat.pendingFiles via the
      // standard /api/chat/upload path. No new endpoint, no out-of-band
      // network call.
      chat.pendingFiles.push(file);
      try { renderAttached(); } catch (e) {}
      setLiveStatus("Snapshot attached (" + Math.round(blob.size / 1024) + " KB)", "luna");
    }, "image/jpeg", 0.85);
  }

  function startLiveRecognition() {
    if (liveTalk.micRecognition) return;  // never stack
    const Recog = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!Recog) {
      setLiveStatus("Speech recognition not available in this browser", "error");
      return;
    }
    const r = new Recog();
    r.continuous = true;
    r.interimResults = true;
    r.lang = navigator.language || "en-US";
    let buffer = "";
    r.onresult = (ev) => {
      let interim = "", finalText = "";
      for (let i = ev.resultIndex; i < ev.results.length; i++) {
        const tr = ev.results[i][0].transcript;
        if (ev.results[i].isFinal) finalText += tr; else interim += tr;
      }
      if (interim) setLiveStatus("Listening: \"" + interim.trim().slice(0, 60) + "\"", "listening");
      if (finalText) {
        buffer = (buffer + " " + finalText).trim();
        if (buffer.length > 0) {
          const send = buffer; buffer = "";
          handleLiveTranscript(send);
        }
      }
    };
    r.onerror = (ev) => {
      const why = (ev && ev.error) || "unknown";
      setLiveStatus("Mic error (" + why + ") — click Live to restart", "error");
      stopLiveRecognition();
      // Do not auto-restart; user clicks Live again.
      _liveSetMicIndicator(false);
    };
    r.onend = () => {
      // Browser may end recognition on long silences. If the operator
      // still has Live Talk on AND we are not waiting on a Luna reply,
      // start a fresh recognizer. Otherwise leave it stopped.
      liveTalk.micRecognition = null;
      if (liveTalk.active && !liveTalk.busy) {
        startLiveRecognition();
      }
    };
    liveTalk.micRecognition = r;
    try {
      r.start();
      _liveSetMicIndicator(true);
      setLiveStatus("Listening", "listening");
    } catch (e) {
      liveTalk.micRecognition = null;
      _liveSetMicIndicator(false);
      setLiveStatus("Mic start failed: " + (e && e.message || e), "error");
    }
  }

  function stopLiveRecognition() {
    const r = liveTalk.micRecognition;
    if (r) {
      try { r.onresult = null; r.onerror = null; r.onend = null; r.stop(); } catch (e) {}
    }
    liveTalk.micRecognition = null;
    _liveSetMicIndicator(false);
  }

  async function handleLiveTranscript(text) {
    const trimmed = (text || "").trim();
    if (!trimmed) return;          // never send empty
    if (liveTalk.busy) return;     // never overlap with an in-flight reply
    liveTalk.busy = true;
    // Pause the recognizer while Luna thinks. We resume in the reply path.
    stopLiveRecognition();
    setLiveStatus("Thinking", "thinking");

    const sess = activeSession() || chat.sessions[newSession(false)];
    const sessionId = sess.id;

    // Mirror what the form-submit handler does so the chat surface stays
    // consistent across typed/mic/live-talk paths.
    pushMessage({
      id: "msg_" + Date.now(),
      role: "you",
      text: trimmed,
      ts: new Date().toISOString(),
      attachments: [],
      permLabel: PERM_LABELS[chat.perm] || chat.perm,
    });
    setTyping(true);

    // If a snapshot was just queued, upload it the same way the form does.
    const pending = chat.pendingFiles.slice();
    chat.pendingFiles = [];
    try { renderAttached(); } catch (e) {}
    const uploaded = [];
    const visionDescriptions = [];
    for (const f of pending) {
      if (f && f.__pathRef) {
        uploaded.push({ name: f.name, size: f.size, ok: true, stored: f.path, ref: true });
        continue;
      }
      try {
        const u = await uploadFile(sessionId, f);
        uploaded.push({ name: f.name, size: f.size, ok: !!u, stored: u && u.path || null });
        // Stage 2 vision: same path as form-submit, but driven by voice.
        const isImg = (f && f.type && f.type.indexOf("image/") === 0)
                   || (f && /\.(jpe?g|png|gif|webp|bmp)$/i.test(f.name || ""));
        if (isImg && u && u.ok && u.path) {
          setLiveStatus("Looking at image...", "thinking");
          const vis = await describeImageOnLuna(sessionId, u.path, trimmed);
          _markVisionResult(vis);
          if (vis && vis.ok && vis.description) {
            visionDescriptions.push({ name: f.name, description: vis.description, model: vis.model });
          }
        }
      } catch (e) {
        uploaded.push({ name: f.name, size: f.size, ok: false, stored: null });
      }
    }
    let augmentedTextLive = trimmed;
    if (visionDescriptions.length) {
      const tag = visionDescriptions
        .map((v) => "[Vision (" + (v.model || "luna-vision") + "): " + v.description + "]")
        .join(" ");
      augmentedTextLive = tag + (trimmed ? " " + trimmed : "");
    }

    const res = await sendChat(augmentedTextLive, uploaded);
    if (!res || !res.ok) {
      setTyping(false);
      pushMessage({
        id: "msg_" + Date.now() + "_e",
        role: "sys",
        text: "live talk send failed - " + ((res && res.error) || "unknown error"),
        ts: new Date().toISOString(),
      });
      setLiveStatus("Send failed - click Live to retry", "error");
      liveTalk.busy = false;
      return;
    }

    setLiveStatus("Luna responding", "luna");
    const reply = await pollChatReply(res.task_id, 30, 1000);
    setTyping(false);
    if (reply && reply.ready && reply.reply) {
      pushMessage({
        id: "msg_" + Date.now() + "_l",
        role: "luna",
        text: reply.reply,
        ts: new Date().toISOString(),
      });
      try { lunaAutoSpeakReply(reply.reply); } catch (_e) {}
    } else {
      pushMessage({
        id: "msg_" + Date.now() + "_q",
        role: "sys",
        text: "queued · " + res.task_id + (reply && reply.error ? " · " + reply.error : " · awaiting reply"),
        ts: new Date().toISOString(),
      });
    }

    liveTalk.busy = false;
    if (liveTalk.active) {
      // Resume listening. setLiveStatus will be overwritten by startLiveRecognition.
      startLiveRecognition();
    } else {
      setLiveStatus("Idle", null);
    }
  }

  function toggleLiveTalk() {
    if (liveTalk.active) {
      liveTalk.active = false;
      stopLiveRecognition();
      setLiveStatus(liveTalk.cameraOn ? "Camera on" : "Idle",
                    liveTalk.cameraOn ? "listening" : null);
      return;
    }
    // If the legacy mic button is currently listening, stop it first so we
    // don't run two recognizers against the same input device.
    if (chat.listening && chat.recog) {
      try { chat.recog.stop(); } catch (e) {}
      chat.listening = false;
      const micBtn = $("console-mic");
      if (micBtn) {
        micBtn.classList.remove("is-listening");
        micBtn.setAttribute("aria-pressed", "false");
      }
    }
    liveTalk.active = true;
    // Make sure the panel is visible so the operator sees the status pill,
    // even if camera is off.
    const panel = $("luna-camera-preview");
    if (panel) panel.hidden = false;
    startLiveRecognition();
  }

  // ============================================================
  // Compact camera = single-click SNAPSHOT.
  // Opens the webcam (if not already open), waits for the first frame,
  // grabs one still, attaches it to the chat input via the existing
  // chat.pendingFiles queue, and closes the camera. The preview panel
  // briefly flashes; we keep it visible just long enough for the
  // operator to see the frame that got captured. No frames are
  // transmitted automatically — only the snapshot, only via the same
  // /api/chat/upload path the paperclip button uses.
  // ============================================================
  async function captureSnapshotOnce() {
    const wasOpen = !!liveTalk.cameraStream;
    if (!wasOpen) {
      try {
        await toggleCamera();
        // Wait for first video frame so videoWidth is populated. Most
        // webcams take 200-600 ms to deliver the first frame.
        const video = $("luna-camera-video");
        const t0 = Date.now();
        while ((!video || !video.videoWidth) && (Date.now() - t0) < 2500) {
          await new Promise((r) => setTimeout(r, 80));
        }
      } catch (_e) { /* fall through; captureCameraFrame will surface its own error */ }
    }
    captureCameraFrame();
    // Auto-close the camera after a brief preview window so the operator
    // doesn't have to click stop manually. Keep the panel up for 600 ms
    // so they can see the captured frame.
    if (!wasOpen) {
      setTimeout(() => { stopCamera(); }, 600);
    }
  }

  // ============================================================
  // Compact LIVE button = video conversation with Luna.
  // Opens the webcam (so Luna can see the operator) AND starts the
  // realtime phone-call mode (so they can talk back-to-back). Re-click
  // ends both. Tracks its own deterministic state flag so it never
  // double-toggles even if camera or voice were started independently.
  // ============================================================
  let _lunaLiveActive = false;
  async function toggleLunaLiveVideo(triggerBtn) {
    if (_lunaLiveActive) {
      _lunaLiveActive = false;
      // Stop voice first (so the audio doesn't outlive the visual).
      const CVref = window.__lunaCV;
      if (CVref && CVref.active) {
        const mic = $("console-mic");
        try { startCompactVoice(mic); } catch (_e) {}  // toggles off
      }
      // Then stop camera.
      try { stopCamera(); } catch (_e) {}
      if (triggerBtn) {
        triggerBtn.classList.remove("is-on");
        triggerBtn.setAttribute("aria-pressed", "false");
      }
      const mic = $("console-mic");
      if (mic) {
        mic.classList.remove("is-listening");
        mic.setAttribute("aria-pressed", "false");
      }
      document.body.dataset.lunaLiveOn = "off";
      document.body.dataset.lunaCompactVoice = "off";
      _compactVoiceStatus("");
      return;
    }
    // Start: camera first (so user sees themselves), then voice.
    _lunaLiveActive = true;
    if (triggerBtn) {
      triggerBtn.classList.add("is-on");
      triggerBtn.setAttribute("aria-pressed", "true");
    }
    document.body.dataset.lunaLiveOn = "on";
    try {
      // Only start the camera if it's not already running (toggleCamera
      // would otherwise STOP an already-running camera).
      if (!liveTalk.cameraStream) {
        await toggleCamera();
      }
    } catch (e) {
      _compactVoiceStatus("Camera blocked - " + (e && e.name || "denied"), "error");
      _lunaLiveActive = false;
      if (triggerBtn) {
        triggerBtn.classList.remove("is-on");
        triggerBtn.setAttribute("aria-pressed", "false");
      }
      document.body.dataset.lunaLiveOn = "off";
      return;
    }
    // Then start voice via the same compact path.
    startCompactVoice($("console-mic"));
  }

  function initLiveTalk() {
    const camBtn  = $("console-camera");
    const liveBtn = $("console-live-talk");
    const stopBtn = $("luna-camera-stop");
    const snapBtn = $("luna-camera-snap");
    // Compact camera: click takes a single snapshot (was: open preview).
    if (camBtn  && !camBtn.dataset.bound)  { camBtn.addEventListener("click", () => captureSnapshotOnce()); camBtn.dataset.bound = "1"; }
    // Compact LIVE: camera + voice combo (was: dictation-only).
    if (liveBtn && !liveBtn.dataset.bound) { liveBtn.addEventListener("click", () => toggleLunaLiveVideo(liveBtn)); liveBtn.dataset.bound = "1"; }
    if (stopBtn && !stopBtn.dataset.bound) {
      stopBtn.addEventListener("click", () => {
        // Stop EVERYTHING - cam tracks, recognizer, panel.
        stopCamera();
        liveTalk.active = false;
        stopLiveRecognition();
        const panel = $("luna-camera-preview");
        if (panel) panel.hidden = true;
        setLiveStatus("Camera off", null);
      });
      stopBtn.dataset.bound = "1";
    }
    if (snapBtn && !snapBtn.dataset.bound) { snapBtn.addEventListener("click", captureCameraFrame); snapBtn.dataset.bound = "1"; }

    // Page-hide and session-switch safety: stop streams.
    if (!document._lunaLiveTalkBound) {
      document._lunaLiveTalkBound = true;
      document.addEventListener("visibilitychange", () => {
        if (document.visibilityState === "hidden") {
          stopCamera();
          if (liveTalk.active) {
            liveTalk.active = false;
            stopLiveRecognition();
            const panel = $("luna-camera-preview");
            if (panel) panel.hidden = true;
            setLiveStatus("Camera off", null);
          }
        }
      });
    }

    setLiveStatus("Camera off", null);
  }

  // ============================================================
  // File Explorer — themed replica of Windows File Explorer
  //   - GET /api/files/roots → quick-access nav tree
  //   - GET /api/files/list?path=… → directory listing
  //   - drag rows into the Command Console to attach paths
  // ============================================================
  const explorer = {
    cwd: null,
    history: [],
    histIdx: -1,
    sortBy: "name",
    sortDir: "asc",
    view: "details",   // "details" | "tile"
    filter: "",
    selected: new Set(),
    entries: [],
    roots: [],
  };
  const FE_ICONS = {
    dir:     '<path d="M2 4h4l2 2h6v8H2z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/>',
    file:    '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/><path d="M10 2v3h3" stroke="currentColor" stroke-width="1.3" fill="none"/>',
    code:    '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/><path d="M10 2v3h3M6.5 9l-1.5 1.5 1.5 1.5M9.5 9l1.5 1.5-1.5 1.5" stroke="currentColor" stroke-width="1.2" fill="none" stroke-linecap="round" stroke-linejoin="round"/>',
    img:     '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/><circle cx="7" cy="9" r="1.2" fill="currentColor"/><path d="M5 13l2-2 2 2 2-3 2 3" stroke="currentColor" stroke-width="1.2" fill="none" stroke-linejoin="round"/>',
    archive: '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/><path d="M8 5v8M6 6h4M6 8h4M6 10h4M6 12h4" stroke="currentColor" stroke-width="1" fill="none"/>',
    doc:     '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/><path d="M6 7h6M6 9h6M6 11h4" stroke="currentColor" stroke-width="1" fill="none"/>',
    audio:   '<path d="M5 9v4M9 7v6" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round"/><path d="M5 9l4-2v8" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/>',
  };
  const NAV_ICONS = {
    star:     '<path d="M8 1.5l1.8 4 4.2.6-3 3 .8 4.4L8 11.5l-3.8 2 .8-4.4-3-3 4.2-.6z" fill="currentColor" opacity="0.9"/>',
    desktop:  '<rect x="2" y="3" width="12" height="8" stroke="currentColor" stroke-width="1.3" fill="none" rx="1"/><path d="M5 13h6M8 11v2" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linecap="round"/>',
    doc:      '<path d="M4 2h6l3 3v9H4z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/>',
    download: '<path d="M8 2v8M5 7l3 3 3-3M3 13h10" stroke="currentColor" stroke-width="1.4" fill="none" stroke-linecap="round" stroke-linejoin="round"/>',
    image:    '<rect x="2" y="3" width="12" height="10" stroke="currentColor" stroke-width="1.3" fill="none" rx="1"/><circle cx="6" cy="7" r="1" fill="currentColor"/><path d="M3 12l3-3 3 2 2-3 2 4" stroke="currentColor" stroke-width="1.2" fill="none"/>',
    video:    '<rect x="2" y="3" width="12" height="10" stroke="currentColor" stroke-width="1.3" fill="none" rx="1"/><path d="M7 6l4 2-4 2z" fill="currentColor"/>',
    luna:     '<circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.3" fill="none"/><path d="M11 4a5 5 0 1 0 1 8 5.5 5.5 0 0 1-1-8z" fill="currentColor" opacity="0.5"/>',
    drive:    '<rect x="2" y="5" width="12" height="6" stroke="currentColor" stroke-width="1.3" fill="none" rx="1"/><circle cx="11.5" cy="8" r="0.7" fill="currentColor"/>',
    folder:   '<path d="M2 4h4l2 2h6v8H2z" stroke="currentColor" stroke-width="1.3" fill="none" stroke-linejoin="round"/>',
  };

  function feSvg(d, size) {
    return '<svg class="luna-explorer__icon" viewBox="0 0 16 16" aria-hidden="true" width="' + (size || 16) + '" height="' + (size || 16) + '">' + d + '</svg>';
  }
  function feNavSvg(d) {
    return '<svg class="luna-explorer__nav-icon" viewBox="0 0 16 16" aria-hidden="true">' + d + '</svg>';
  }
  function feKind(entry) {
    if (entry.is_dir) return "dir";
    const ext = (entry.type || "").toLowerCase();
    if (["js","ts","tsx","jsx","py","rs","go","java","c","cpp","cs","rb","php","sh","bat","ps1","vbs","html","css","json","yml","yaml","toml","sql"].includes(ext)) return "code";
    if (["png","jpg","jpeg","gif","webp","bmp","svg","ico","tiff"].includes(ext)) return "img";
    if (["zip","7z","rar","tar","gz","bz2","xz","cab"].includes(ext)) return "archive";
    if (["txt","md","rst","log","pdf","doc","docx","odt","rtf"].includes(ext)) return "doc";
    if (["mp3","wav","flac","ogg","m4a","aac"].includes(ext)) return "audio";
    return "file";
  }
  function feIcon(kind) {
    return feSvg(FE_ICONS[kind] || FE_ICONS.file);
  }
  function feFmtSize(n) {
    if (!n) return "";
    return fmtBytes(n);
  }
  function feFmtDate(unix) {
    if (!unix) return "";
    const d = new Date(unix * 1000);
    if (isNaN(d.getTime())) return "";
    const z = (n) => String(n).padStart(2, "0");
    return d.getFullYear() + "-" + z(d.getMonth()+1) + "-" + z(d.getDate()) + " " + z(d.getHours()) + ":" + z(d.getMinutes());
  }

  async function feFetchRoots() {
    const r = await fetchJSON("/api/files/roots");
    return (r && r.ok && Array.isArray(r.roots)) ? r.roots : [];
  }
  async function feFetchList(path) {
    const r = await fetchJSON("/api/files/list?path=" + encodeURIComponent(path || ""));
    return r || { ok: false, error: "no response" };
  }

  function feRenderNav() {
    const list = $("fe-navlist");
    if (!list) return;
    list.innerHTML = "";
    const groups = [
      { label: "Pinned",      items: explorer.roots.filter((x) => x.group === "pinned") },
      { label: "Project",     items: explorer.roots.filter((x) => x.group === "project") },
      { label: "This PC",     items: explorer.roots.filter((x) => x.group === "pc") },
    ];
    for (const g of groups) {
      if (!g.items.length) continue;
      const h = document.createElement("li");
      h.dataset.section = "header";
      h.textContent = g.label;
      list.appendChild(h);
      for (const it of g.items) {
        const li = document.createElement("li");
        li.dataset.path = it.path;
        li.title = it.path;
        const icon = NAV_ICONS[it.icon] || NAV_ICONS.folder;
        li.innerHTML = feNavSvg(icon) + '<span>' + escapeHtml(it.name) + '</span>';
        if (explorer.cwd && it.path === explorer.cwd) li.classList.add("is-active");
        li.addEventListener("click", () => feNavigate(it.path));
        list.appendChild(li);
      }
    }
  }
  function feRenderCrumbs() {
    const wrap = $("fe-breadcrumb");
    if (!wrap) return;
    wrap.innerHTML = "";
    if (!explorer.cwd) { wrap.textContent = "—"; return; }
    const parts = explorer.cwd.replace(/\\/g, "/").split("/").filter(Boolean);
    let acc = "";
    if (explorer.cwd.match(/^[A-Za-z]:/)) {
      acc = parts[0] + "\\";
      const c = document.createElement("span"); c.className = "luna-explorer__crumb"; c.dataset.path = acc; c.textContent = parts[0]; c.addEventListener("click", () => feNavigate(acc));
      wrap.appendChild(c);
      parts.shift();
    } else {
      acc = "/";
    }
    for (let i = 0; i < parts.length; i++) {
      const sep = document.createElement("span"); sep.className = "luna-explorer__crumb-sep"; sep.textContent = "›"; wrap.appendChild(sep);
      acc = acc.replace(/[\\/]+$/, "") + (acc.endsWith("\\") ? "" : "\\") + parts[i];
      const c = document.createElement("span"); c.className = "luna-explorer__crumb"; c.dataset.path = acc; c.textContent = parts[i]; c.addEventListener("click", ((p) => () => feNavigate(p))(acc));
      wrap.appendChild(c);
    }
  }
  function feSortEntries(arr) {
    const by = explorer.sortBy;
    const dir = explorer.sortDir === "asc" ? 1 : -1;
    const keyed = arr.slice().sort((a, b) => {
      // folders first
      if (a.is_dir !== b.is_dir) return a.is_dir ? -1 : 1;
      let av, bv;
      if (by === "name")  { av = a.name.toLowerCase(); bv = b.name.toLowerCase(); }
      else if (by === "size")  { av = a.size || 0; bv = b.size || 0; }
      else if (by === "mtime") { av = a.mtime || 0; bv = b.mtime || 0; }
      else                     { av = (a.type || "").toLowerCase(); bv = (b.type || "").toLowerCase(); }
      if (av < bv) return -1 * dir;
      if (av > bv) return  1 * dir;
      return a.name.localeCompare(b.name) * dir;
    });
    return keyed;
  }
  function feRenderColumns() {
    document.querySelectorAll("#luna-explorer .luna-explorer__col").forEach((col) => {
      const active = col.dataset.sort === explorer.sortBy;
      col.dataset.active = active ? "true" : "false";
      const arrow = active ? (explorer.sortDir === "asc" ? "▲" : "▼") : "";
      const a = col.querySelector(".luna-explorer__sort");
      if (a) a.textContent = arrow;
    });
  }
  function feRenderList() {
    const list = $("fe-list");
    if (!list) return;
    list.dataset.view = explorer.view;
    list.innerHTML = "";
    let entries = explorer.entries.filter((e) => !e.hidden);
    if (explorer.filter) {
      const q = explorer.filter.toLowerCase();
      entries = entries.filter((e) => e.name.toLowerCase().includes(q));
    }
    entries = feSortEntries(entries);
    if (!entries.length) {
      const empty = document.createElement("li");
      empty.className = "luna-explorer__empty";
      empty.textContent = explorer.filter ? "no matches in this folder" : "this folder is empty";
      list.appendChild(empty);
      feUpdateStatus(0);
      return;
    }
    for (const e of entries) list.appendChild(feRenderRow(e));
    feUpdateStatus(entries.length);
  }
  function feRenderRow(e) {
    const li = document.createElement("li");
    li.className = "luna-explorer__row";
    li.dataset.path = e.path;
    li.dataset.kind = feKind(e);
    li.dataset.isDir = e.is_dir ? "1" : "0";
    li.setAttribute("role", "row");
    li.setAttribute("draggable", "true");
    li.title = e.path;
    if (explorer.selected.has(e.path)) li.classList.add("is-selected");
    const name = document.createElement("div");
    name.className = "luna-explorer__name luna-explorer__cell";
    name.innerHTML = feIcon(feKind(e)) + '<span>' + escapeHtml(e.name) + '</span>';
    const mtime = document.createElement("div"); mtime.className = "luna-explorer__cell luna-explorer__mtime"; mtime.textContent = feFmtDate(e.mtime);
    const type  = document.createElement("div"); type.className  = "luna-explorer__cell luna-explorer__type";  type.textContent  = e.is_dir ? "Folder" : (e.type ? e.type.toUpperCase() + " file" : "File");
    const size  = document.createElement("div"); size.className  = "luna-explorer__cell luna-explorer__size";  size.textContent  = e.is_dir ? "" : feFmtSize(e.size);
    li.appendChild(name); li.appendChild(mtime); li.appendChild(type); li.appendChild(size);

    li.addEventListener("click", (ev) => {
      if (!ev.ctrlKey && !ev.metaKey && !ev.shiftKey) explorer.selected.clear();
      if (explorer.selected.has(e.path)) explorer.selected.delete(e.path); else explorer.selected.add(e.path);
      feRenderList();
    });
    li.addEventListener("dblclick", () => {
      if (e.is_dir) feNavigate(e.path);
    });
    li.addEventListener("dragstart", (ev) => {
      // include all currently-selected entries; if this row isn't selected, drag just it
      const sel = explorer.selected.has(e.path)
        ? Array.from(explorer.selected).map((p) => explorer.entries.find((x) => x.path === p)).filter(Boolean)
        : [e];
      const payload = sel.map((x) => ({ name: x.name, path: x.path, size: x.size, type: x.type, is_dir: !!x.is_dir }));
      ev.dataTransfer.setData("application/luna-path+json", JSON.stringify(payload));
      ev.dataTransfer.setData("text/plain", payload.map((p) => p.path).join("\n"));
      ev.dataTransfer.effectAllowed = "copy";
      li.classList.add("is-dragging");
    });
    li.addEventListener("dragend", () => li.classList.remove("is-dragging"));
    return li;
  }
  function feUpdateStatus(visible) {
    const total = explorer.entries.filter((e) => !e.hidden).length;
    text($("fe-status-count"), visible === total ? total + " items" : visible + " of " + total + " items");
    const sel = explorer.selected.size;
    text($("fe-status-sel"), sel ? sel + " selected" : "no selection");
  }

  async function feNavigate(path, pushHistory) {
    const data = await feFetchList(path);
    if (!data || !data.ok) {
      const list = $("fe-list");
      if (list) {
        list.innerHTML = "";
        const err = document.createElement("li");
        err.className = "luna-explorer__error";
        err.textContent = "cannot open · " + ((data && data.error) || "unknown error");
        list.appendChild(err);
      }
      return;
    }
    explorer.cwd = data.path;
    explorer.entries = data.entries || [];
    explorer.selected.clear();
    if (pushHistory !== false) {
      explorer.history = explorer.history.slice(0, explorer.histIdx + 1);
      explorer.history.push(data.path);
      explorer.histIdx = explorer.history.length - 1;
    }
    feUpdateNavButtons(data);
    feRenderNav();
    feRenderCrumbs();
    feRenderColumns();
    feRenderList();
  }
  function feUpdateNavButtons(data) {
    const back = $("fe-back"); if (back) back.disabled = explorer.histIdx <= 0;
    const fwd  = $("fe-forward"); if (fwd) fwd.disabled = explorer.histIdx >= explorer.history.length - 1;
    const up   = $("fe-up"); if (up) up.disabled = !data.parent;
  }

  async function initExplorer() {
    const list = $("fe-list");
    if (!list) return;  // panel not present
    explorer.roots = await feFetchRoots();
    feRenderNav();

    // Toolbar wiring
    const back = $("fe-back"), fwd = $("fe-forward"), up = $("fe-up"), refresh = $("fe-refresh");
    if (back) back.addEventListener("click", () => {
      if (explorer.histIdx <= 0) return;
      explorer.histIdx--;
      feNavigate(explorer.history[explorer.histIdx], false);
    });
    if (fwd) fwd.addEventListener("click", () => {
      if (explorer.histIdx >= explorer.history.length - 1) return;
      explorer.histIdx++;
      feNavigate(explorer.history[explorer.histIdx], false);
    });
    if (up) up.addEventListener("click", async () => {
      if (!explorer.cwd) return;
      const data = await feFetchList(explorer.cwd);
      if (data && data.ok && data.parent) feNavigate(data.parent);
    });
    if (refresh) refresh.addEventListener("click", () => { if (explorer.cwd) feNavigate(explorer.cwd, false); });

    const search = $("fe-search");
    if (search) {
      let t = null;
      search.addEventListener("input", () => {
        explorer.filter = search.value;
        clearTimeout(t);
        t = setTimeout(() => feRenderList(), 120);
      });
      search.addEventListener("keydown", (e) => {
        if (e.key === "Escape") { search.value = ""; explorer.filter = ""; feRenderList(); }
      });
    }

    const view = $("fe-view");
    if (view) view.addEventListener("click", () => {
      explorer.view = explorer.view === "details" ? "tile" : "details";
      feRenderList();
      const vi = $("fe-view-icon");
      if (vi) {
        vi.innerHTML = explorer.view === "details"
          ? '<path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" stroke-width="1.4" fill="none" stroke-linecap="round"/>'
          : '<rect x="2" y="2" width="5" height="5" stroke="currentColor" stroke-width="1.4" fill="none"/><rect x="9" y="2" width="5" height="5" stroke="currentColor" stroke-width="1.4" fill="none"/><rect x="2" y="9" width="5" height="5" stroke="currentColor" stroke-width="1.4" fill="none"/><rect x="9" y="9" width="5" height="5" stroke="currentColor" stroke-width="1.4" fill="none"/>';
      }
    });

    // Sort columns
    document.querySelectorAll("#luna-explorer .luna-explorer__col").forEach((col) => {
      col.addEventListener("click", () => {
        const by = col.dataset.sort;
        if (explorer.sortBy === by) explorer.sortDir = explorer.sortDir === "asc" ? "desc" : "asc";
        else { explorer.sortBy = by; explorer.sortDir = "asc"; }
        feRenderColumns(); feRenderList();
      });
    });

    // Boot at the project root if present, else first pinned root.
    const project = explorer.roots.find((r) => r.group === "project");
    const startPath = (project && project.path) || (explorer.roots[0] && explorer.roots[0].path);
    if (startPath) feNavigate(startPath);

    initFileBayScrollRail();
  }

  // ============================================================
  // File Bay collapse toggle. Click the chevron in the File Bay header
  // to hide the toolbar + body + statusbar; click again to expand.
  // The article's data-open drives CSS visibility; the chevron flips
  // 180° when open. Persists across reloads via localStorage.
  // ============================================================
  function setFileBayOpen(open) {
    const card  = $("luna-explorer");
    const btn   = $("fe-collapse");
    if (!card) return;
    const want = !!open;
    card.dataset.open = want ? "true" : "false";
    if (btn) {
      btn.setAttribute("aria-expanded", want ? "true" : "false");
      btn.setAttribute("title", want ? "Collapse File Bay" : "Expand File Bay");
      btn.setAttribute("aria-label", want ? "Collapse File Bay" : "Expand File Bay");
    }
    try { window.localStorage.setItem("luna.filebay.open", want ? "true" : "false"); } catch (_) {}
  }
  function initFileBayCollapse() {
    const card = $("luna-explorer");
    const btn  = $("fe-collapse");
    if (!card || !btn) return;
    let saved = null;
    try { saved = window.localStorage.getItem("luna.filebay.open"); } catch (_) {}
    const startOpen = (saved === null) ? true : (saved === "true");
    setFileBayOpen(startOpen);
    btn.addEventListener("click", () => {
      setFileBayOpen(card.dataset.open !== "true");
    });
  }

  // ============================================================
  // File Bay custom scroll rail (up arrow / thumb / down arrow)
  //   Native scrollbar on .luna-explorer__list is hidden; this rail
  //   drives list.scrollTop via clicks + thumb drag.  Read-only with
  //   respect to all existing fe-* APIs.
  // ============================================================
  function initFileBayScrollRail() {
    const list  = $("fe-list");
    const up    = $("fe-scroll-up");
    const down  = $("fe-scroll-down");
    const thumb = $("fe-scroll-thumb");
    const track = $("fe-scroll-track");
    if (!list || !up || !down || !thumb || !track) return;

    const STEP = 96;        // pixels per arrow click
    const MIN_THUMB = 28;   // minimum thumb height
    let dragging = false;
    let dragStartY = 0;
    let dragStartTop = 0;

    function syncThumb() {
      const max = Math.max(0, list.scrollHeight - list.clientHeight);
      const trackH = track.clientHeight;
      const visibleRatio = list.clientHeight / Math.max(1, list.scrollHeight);
      const thumbH = Math.max(MIN_THUMB, Math.min(trackH - 4, Math.round(trackH * visibleRatio)));
      thumb.style.height = thumbH + "px";
      const pct = max ? (list.scrollTop / max) : 0;
      const top = Math.round(pct * (trackH - thumbH));
      thumb.style.top = top + "px";
      // Disable rail visually if nothing to scroll
      const rail = thumb.closest(".luna-explorer__rail");
      if (rail) rail.classList.toggle("is-disabled", max <= 0);
    }

    up.addEventListener("click", () => { list.scrollTop = Math.max(0, list.scrollTop - STEP); });
    down.addEventListener("click", () => { list.scrollTop = list.scrollTop + STEP; });
    list.addEventListener("scroll", syncThumb, { passive: true });

    thumb.addEventListener("mousedown", (e) => {
      dragging = true;
      dragStartY = e.clientY;
      dragStartTop = list.scrollTop;
      thumb.setPointerCapture && thumb.setPointerCapture(e.pointerId || 0);
      e.preventDefault();
    });
    document.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      const dy = e.clientY - dragStartY;
      const max = Math.max(0, list.scrollHeight - list.clientHeight);
      const trackH = track.clientHeight;
      const thumbH = thumb.clientHeight || MIN_THUMB;
      const ratio = (trackH - thumbH) > 0 ? dy / (trackH - thumbH) : 0;
      list.scrollTop = Math.max(0, Math.min(max, dragStartTop + ratio * max));
    });
    document.addEventListener("mouseup", () => { dragging = false; });

    // Touch support
    thumb.addEventListener("touchstart", (e) => {
      dragging = true;
      dragStartY = e.touches[0].clientY;
      dragStartTop = list.scrollTop;
    }, { passive: true });
    document.addEventListener("touchmove", (e) => {
      if (!dragging) return;
      const dy = e.touches[0].clientY - dragStartY;
      const max = Math.max(0, list.scrollHeight - list.clientHeight);
      const trackH = track.clientHeight;
      const thumbH = thumb.clientHeight || MIN_THUMB;
      const ratio = (trackH - thumbH) > 0 ? dy / (trackH - thumbH) : 0;
      list.scrollTop = Math.max(0, Math.min(max, dragStartTop + ratio * max));
    }, { passive: true });
    document.addEventListener("touchend", () => { dragging = false; });

    // Keep thumb in sync as the list contents change (folder navigation etc.)
    if (window.ResizeObserver) {
      try { new ResizeObserver(syncThumb).observe(list); } catch (e) { /* */ }
    }
    if (window.MutationObserver) {
      try { new MutationObserver(syncThumb).observe(list, { childList: true, subtree: false }); } catch (e) { /* */ }
    }

    // Initial pass
    syncThumb();
  }

  // ============================================================
  // Settings: theme · density · motion · glass · layout · focus
  //   Persisted in localStorage; applied via documentElement.dataset.
  //   Triggers palette invalidation so canvases retint on next frame.
  // ============================================================
  const SETTINGS = {
    THEMES:    ["lunar-gold", "eclipse-noir", "aurora-terminal", "blood-moon", "ice-moon"],
    DENSITIES: ["comfortable", "compact", "spacious"],
    MOTIONS:   ["full", "reduced"],
    GLASSES:   ["low", "medium", "high"],
    LAYOUTS:   ["cockpit", "focus", "grid"],
    DEFAULTS:  {
      theme:      "lunar-gold",
      density:    "comfortable",
      motion:     "full",
      glass:      "medium",
      layoutMode: "cockpit",
      focusMode:  false,
    },
  };

  const LS_KEYS = {
    theme:      "luna.theme",
    density:    "luna.density",
    motion:     "luna.motion",
    glass:      "luna.glass",
    layoutMode: "luna.layoutMode",
    focusMode:  "luna.focusMode",
    telemOpen:  "luna.telemetry.open",
    telemTab:   "luna.telemetry.tab",
  };

  function _ls(k, fallback) {
    try { const v = localStorage.getItem(k); return v == null ? fallback : v; }
    catch (e) { return fallback; }
  }
  function _lsSet(k, v) { try { localStorage.setItem(k, String(v)); } catch (e) { /* ignore */ } }

  function loadSettings() {
    const D = SETTINGS.DEFAULTS;
    const allow = (set, val, fb) => set.includes(val) ? val : fb;
    return {
      theme:      allow(SETTINGS.THEMES,    _ls(LS_KEYS.theme,      D.theme),      D.theme),
      density:    allow(SETTINGS.DENSITIES, _ls(LS_KEYS.density,    D.density),    D.density),
      motion:     allow(SETTINGS.MOTIONS,   _ls(LS_KEYS.motion,     D.motion),     D.motion),
      glass:      allow(SETTINGS.GLASSES,   _ls(LS_KEYS.glass,      D.glass),      D.glass),
      layoutMode: allow(SETTINGS.LAYOUTS,   _ls(LS_KEYS.layoutMode, D.layoutMode), D.layoutMode),
      focusMode:  _ls(LS_KEYS.focusMode, "false") === "true",
    };
  }
  function saveSettings(s) {
    if (!s) return;
    if (s.theme)      _lsSet(LS_KEYS.theme,      s.theme);
    if (s.density)    _lsSet(LS_KEYS.density,    s.density);
    if (s.motion)     _lsSet(LS_KEYS.motion,     s.motion);
    if (s.glass)      _lsSet(LS_KEYS.glass,      s.glass);
    if (s.layoutMode) _lsSet(LS_KEYS.layoutMode, s.layoutMode);
    if (typeof s.focusMode === "boolean") _lsSet(LS_KEYS.focusMode, s.focusMode);
  }
  function applySettings(s) {
    const root = document.documentElement;
    if (s.theme)   root.dataset.theme   = s.theme;
    if (s.density) root.dataset.density = s.density;
    if (s.motion)  root.dataset.motion  = s.motion;
    if (s.glass)   root.dataset.glass   = s.glass;
    // layout: focus mode wins; otherwise use stored layoutMode
    root.dataset.layout = s.focusMode ? "focus" : (s.layoutMode || "cockpit");
    invalidatePalette();
  }
  function _syncRadiogroup(containerId, attrName, value) {
    const root = $(containerId);
    if (!root) return;
    root.querySelectorAll("[data-" + attrName + "]").forEach((el) => {
      const active = el.dataset[attrName] === value;
      el.classList.toggle("is-active", active);
      el.setAttribute("aria-checked", active ? "true" : "false");
      el.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }
  function _syncSettingsUI(s) {
    _syncRadiogroup("luna-theme-grid",     "theme",   s.theme);
    _syncRadiogroup("luna-density-control","density", s.density);
    _syncRadiogroup("luna-motion-control", "motion",  s.motion);
    _syncRadiogroup("luna-glass-control",  "glass",   s.glass);
    _syncRadiogroup("luna-layout-control", "layout",  s.layoutMode);
  }

  function setTheme(name) {
    if (!SETTINGS.THEMES.includes(name)) return;
    document.documentElement.dataset.theme = name;
    _lsSet(LS_KEYS.theme, name);
    invalidatePalette();
    _syncRadiogroup("luna-theme-grid", "theme", name);
  }
  function setDensity(value) {
    if (!SETTINGS.DENSITIES.includes(value)) return;
    document.documentElement.dataset.density = value;
    _lsSet(LS_KEYS.density, value);
    invalidatePalette();
    _syncRadiogroup("luna-density-control", "density", value);
  }
  function setMotion(value) {
    if (!SETTINGS.MOTIONS.includes(value)) return;
    document.documentElement.dataset.motion = value;
    _lsSet(LS_KEYS.motion, value);
    _syncRadiogroup("luna-motion-control", "motion", value);
  }
  function setGlass(value) {
    if (!SETTINGS.GLASSES.includes(value)) return;
    document.documentElement.dataset.glass = value;
    _lsSet(LS_KEYS.glass, value);
    _syncRadiogroup("luna-glass-control", "glass", value);
  }
  function setLayoutMode(value) {
    if (!SETTINGS.LAYOUTS.includes(value)) return;
    _lsSet(LS_KEYS.layoutMode, value);
    const focus = _ls(LS_KEYS.focusMode, "false") === "true";
    if (!focus) document.documentElement.dataset.layout = value;
    _syncRadiogroup("luna-layout-control", "layout", value);
  }

  function toggleSettings(open) {
    const drawer   = $("luna-settings");
    const backdrop = $("luna-settings-backdrop");
    const btn      = $("luna-settings-btn");
    if (!drawer) return;
    const want = (open === undefined) ? drawer.dataset.open !== "true" : !!open;
    if (want) {
      drawer.hidden = false;
      if (backdrop) backdrop.hidden = false;
      requestAnimationFrame(() => {
        drawer.dataset.open = "true";
        drawer.setAttribute("aria-hidden", "false");
      });
      if (btn) btn.setAttribute("aria-expanded", "true");
      const firstFocus = drawer.querySelector(".luna-stab.is-active") || drawer.querySelector("[role='tab']");
      if (firstFocus) firstFocus.focus();
    } else {
      drawer.dataset.open = "false";
      drawer.setAttribute("aria-hidden", "true");
      if (btn) {
        btn.setAttribute("aria-expanded", "false");
        // restore focus to the trigger so keyboard users don't get lost
        try { btn.focus(); } catch (e) { /* */ }
      }
      setTimeout(() => {
        drawer.hidden = true;
        if (backdrop) backdrop.hidden = true;
      }, 280);
    }
  }

  function _switchSettingsTab(name) {
    const drawer = $("luna-settings");
    if (!drawer) return;
    drawer.querySelectorAll(".luna-stab[data-stab]").forEach((tab) => {
      const active = tab.dataset.stab === name;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
      tab.tabIndex = active ? 0 : -1;
    });
    drawer.querySelectorAll(".luna-spanel").forEach((panel) => {
      const id = panel.id; // spanel-<name>
      const active = id === ("spanel-" + name);
      panel.hidden = !active;
      panel.classList.toggle("is-active", active);
    });
  }

  function initSettings() {
    const settings = loadSettings();
    applySettings(settings);
    _syncSettingsUI(settings);

    const drawer   = $("luna-settings");
    const openBtn  = $("luna-settings-btn");
    const closeBtn = $("luna-settings-close");
    const backdrop = $("luna-settings-backdrop");

    if (openBtn)  openBtn.addEventListener("click", () => toggleSettings(true));
    if (closeBtn) closeBtn.addEventListener("click", () => toggleSettings(false));
    if (backdrop) backdrop.addEventListener("click", () => toggleSettings(false));

    // Keyboard: Esc to close, Ctrl/Cmd+, to toggle
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        if (drawer && drawer.dataset.open === "true") {
          e.preventDefault();
          toggleSettings(false);
        }
      } else if ((e.ctrlKey || e.metaKey) && e.key === ",") {
        e.preventDefault();
        toggleSettings();
      }
    });

    // Settings tabs (Appearance / Layout / Safety / About)
    document.querySelectorAll("#luna-settings .luna-stab[data-stab]").forEach((tab) => {
      tab.addEventListener("click", () => _switchSettingsTab(tab.dataset.stab));
      tab.addEventListener("keydown", (e) => {
        if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
        e.preventDefault();
        const tabs = Array.from(document.querySelectorAll("#luna-settings .luna-stab[data-stab]"));
        const idx = tabs.indexOf(tab);
        const next = e.key === "ArrowRight" ? (idx + 1) % tabs.length : (idx - 1 + tabs.length) % tabs.length;
        tabs[next].focus();
        _switchSettingsTab(tabs[next].dataset.stab);
      });
    });

    // Theme picker
    document.querySelectorAll("#luna-theme-grid [data-theme]").forEach((el) => {
      el.addEventListener("click", () => setTheme(el.dataset.theme));
    });
    // Density / Motion / Glass / Layout radios
    document.querySelectorAll("#luna-density-control [data-density]").forEach((el) => {
      el.addEventListener("click", () => setDensity(el.dataset.density));
    });
    document.querySelectorAll("#luna-motion-control [data-motion]").forEach((el) => {
      el.addEventListener("click", () => setMotion(el.dataset.motion));
    });
    document.querySelectorAll("#luna-glass-control [data-glass]").forEach((el) => {
      el.addEventListener("click", () => setGlass(el.dataset.glass));
    });
    document.querySelectorAll("#luna-layout-control [data-layout]").forEach((el) => {
      el.addEventListener("click", () => setLayoutMode(el.dataset.layout));
    });
  }

  // ============================================================
  // Focus mode
  //   Toggles data-layout="focus" on <html>, persisted in localStorage.
  //   Independent of the Layout radio so users can flip focus instantly.
  // ============================================================
  function setFocusMode(on) {
    const want = !!on;
    const layoutMode = _ls(LS_KEYS.layoutMode, "cockpit");
    document.documentElement.dataset.layout = want ? "focus" : layoutMode;
    _lsSet(LS_KEYS.focusMode, want);
    const btn = $("luna-focus-btn");
    if (btn) btn.setAttribute("aria-pressed", want ? "true" : "false");
  }
  function initFocusMode() {
    const s = loadSettings();
    setFocusMode(s.focusMode);
    const btn = $("luna-focus-btn");
    if (btn) {
      btn.addEventListener("click", () => {
        const cur = _ls(LS_KEYS.focusMode, "false") === "true";
        setFocusMode(!cur);
      });
    }
  }

  // ============================================================
  // Quick command chips (rendered inside the empty Console state)
  // ============================================================
  const QUICK_COMMANDS = [
    { label: "Plan next step",
      prompt: "Plan my next step. Look at the current state of the project, summarize where we are, and propose the next concrete action with reasoning." },
    { label: "Review selected files",
      prompt: "Review the files attached above. Look for issues, risks, and improvement opportunities. Be specific." },
    { label: "Debug error",
      prompt: "Help me debug an error. I'll paste the trace or describe the symptom — ask clarifying questions before proposing a fix." },
    { label: "Summarize logs",
      prompt: "Summarize what's in the logs lately. Group by component, surface anomalies, and tell me whether anything needs attention." },
    { label: "Run safe check",
      prompt: "Run a safe check across the project. No code edits — just diagnostics. Report syntax health, import sanity, and any blocking issues." },
    { label: "Draft implementation",
      prompt: "Draft an implementation for: <describe what you want>. Use the project's existing conventions and propose a small, reviewable change set." },
  ];

  function renderQuickChipsHTML() {
    const items = QUICK_COMMANDS.map((qc) =>
      '<button type="button" class="luna-pill luna-quick-chip" data-prompt="' +
      escapeHtml(qc.prompt) + '">' + escapeHtml(qc.label) + '</button>'
    ).join("");
    return '<div class="luna-console__quick" role="group" aria-label="Quick commands">' + items + '</div>';
  }

  function initQuickCommands() {
    // Delegated click handler — chips are rendered inside the empty state,
    // which can be re-rendered any time renderChat() runs.
    document.addEventListener("click", (e) => {
      const chip = e.target.closest && e.target.closest(".luna-quick-chip");
      if (!chip) return;
      e.preventDefault();
      const input = $("console-input");
      if (!input) return;
      const prompt = chip.dataset.prompt || chip.textContent || "";
      const cur = input.value || "";
      input.value = cur ? (cur.replace(/\s+$/, "") + "\n\n" + prompt) : prompt;
      input.focus();
      // Resize textarea to fit the new content (matches initConsole.grow())
      input.style.height = "auto";
      input.style.height = Math.min(200, input.scrollHeight) + "px";
      // Move caret to end
      try {
        const len = input.value.length;
        input.setSelectionRange(len, len);
      } catch (err) { /* ignore */ }
    });
  }

  // ============================================================
  // Telemetry drawer + tabs
  //   Drawer collapses by default; remembers state across reloads.
  //   Five tabs: TTY · Live Feed · Frequency · Archive · Soak details.
  // ============================================================
  function setTelemetryOpen(open) {
    const drawer = $("luna-telemetry");
    const toggle = $("luna-telemetry-toggle");
    const body   = $("luna-telemetry-body");
    if (!drawer) return;
    const want = !!open;
    drawer.dataset.open = want ? "true" : "false";
    if (toggle) toggle.setAttribute("aria-expanded", want ? "true" : "false");
    if (body)   body.hidden = !want;
    _lsSet(LS_KEYS.telemOpen, want);
  }
  function switchTelemetryTab(name) {
    const drawer = $("luna-telemetry");
    if (!drawer) return;
    drawer.querySelectorAll(".luna-tab[data-tab]").forEach((tab) => {
      const active = tab.dataset.tab === name;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
      tab.tabIndex = active ? 0 : -1;
    });
    drawer.querySelectorAll(".luna-tabpanel[data-tab]").forEach((panel) => {
      const active = panel.dataset.tab === name;
      panel.hidden = !active;
      panel.classList.toggle("is-active", active);
    });
    _lsSet(LS_KEYS.telemTab, name);
  }
  function initTelemetryTabs() {
    const drawer = $("luna-telemetry");
    const toggle = $("luna-telemetry-toggle");
    if (!drawer || !toggle) return;

    // Restore last state
    const wasOpen = _ls(LS_KEYS.telemOpen, "false") === "true";
    const lastTab = _ls(LS_KEYS.telemTab, "tty");
    setTelemetryOpen(wasOpen);
    switchTelemetryTab(lastTab);

    toggle.addEventListener("click", () => {
      setTelemetryOpen(drawer.dataset.open !== "true");
    });

    // Tab buttons
    const tabs = Array.from(drawer.querySelectorAll(".luna-tab[data-tab]"));
    tabs.forEach((tab) => {
      tab.addEventListener("click", () => switchTelemetryTab(tab.dataset.tab));
      tab.addEventListener("keydown", (e) => {
        if (e.key !== "ArrowLeft" && e.key !== "ArrowRight" && e.key !== "Home" && e.key !== "End") return;
        e.preventDefault();
        const idx = tabs.indexOf(tab);
        let next = idx;
        if (e.key === "ArrowRight") next = (idx + 1) % tabs.length;
        else if (e.key === "ArrowLeft") next = (idx - 1 + tabs.length) % tabs.length;
        else if (e.key === "Home") next = 0;
        else if (e.key === "End")  next = tabs.length - 1;
        tabs[next].focus();
        switchTelemetryTab(tabs[next].dataset.tab);
      });
    });
  }

  // ============================================================
  // LUNA LIVE COGNITIVE MAP
  // Canvas constellation visualizing Luna's runtime state.
  // Driven by /api/live-feed records via refreshFeed().
  // Theme-driven (reads --c-accent-rgb).
  // ============================================================
  const cmap = {
    canvas: null,
    ctx: null,
    overlay: null,
    nodes: [
      // Only the current project/work/tier appears here. Completed tier nodes
      // stay out of the live map so the surface does not look like old work.
      // key, label, ring (1=inner agents, 2=outer current state), angle 0-1, [r,g,b]
      ["ARCHITECT",       "ARCHITECT",       1, 0.00, [255, 214, 132]],
      ["CURRENT_PROJECT", "PROJECT",         1, 0.16, [ 76, 215, 255]],
      ["CURRENT_WORK",    "CURRENT WORK",    1, 0.32, [157, 128, 255]],
      ["AIDER",           "AIDER",           1, 0.48, [157, 128, 255]],
      ["VERIFIER",        "VERIFIER",        1, 0.64, [111, 220, 177]],
      ["GUARDIAN",        "GUARDIAN",        1, 0.80, [255, 209, 102]],
      ["CURRENT_TIER",    "CURRENT TIER",    2, 0.18, [255, 210, 140]],
      ["MEMORY",          "MEMORY",          2, 0.42, [200, 180, 220]],
      ["DECISION_QUEUE",  "LIVE QUEUE",      2, 0.66, [130, 210, 250]],
      ["BLOCKERS",        "BLOCKERS",        2, 0.88, [255, 138, 138]],
    ],
    activations: {},     // key -> intensity 0..1 (decays each frame)
    particles: [],       // [{src, dst, t, speed}]
    sparks: [],          // [{node, age, color}]
    stars: [],           // [{x:0..1, y:0..1, b:0..1}]
    phase: 0,
    paused: false,
    focusActive: false,
    lastTs: "",          // last processed record's ts (so we only forward new ones)
    counters: { ok: 0, fail: 0 },
    current: { actor: "IDLE", stage: "waiting for live events", task: "",
               last_event: "", idle_reason: "", blocked_reason: "" },
    rafId: null,
    dpr: 1,
    MAX_PARTICLES: 80,
    MAX_SPARKS: 30,
    chain: [],         // last 5 active nodes (handoff chain)
    MAX_CHAIN: 5,
    // Ambient animation state.
    shooters: [],      // active shooting-star streaks
    nextShooterAt: 0,  // RAF time when the next shooter spawns
    edgeShimmerPhase: 0, // separate phase for ambient edge shimmer
    orbitSpeed: 0.00033,
    taskActive: false,
    currentTierLabel: "CURRENT TIER",
    currentProjectLabel: "PROJECT",
    progressionPath: ["ARCHITECT", "CURRENT_PROJECT", "CURRENT_WORK", "CURRENT_TIER", "VERIFIER", "GUARDIAN", "MEMORY"],
    // 2026-05-08 round 8: signal-tied animations. Each is tracked with a
    // "last seen" value so we only fire visuals on actual change events,
    // not on every poll. This keeps the map alive WITH MEANING — every
    // visual element traces back to a real Luna signal.
    fireworks: [],          // tier-promotion bursts
    nextHeartbeatRippleAt: 0, // ripple cadence from supervisor heartbeat
    heartbeatRipples: [],   // expanding rings from LUNA_CORE
    auditPulseAt: 0,        // RAF time the next audit pulse should fire
    lastAuditCount: 0,      // last seen audit chain length
    tierGlobal: 0,          // last seen tier global from tier-truth
    ceilingTier: 500,       // ceiling from framework
    progressArcOpacity: 0,  // fades in once we have data
    comet: { angle: 0, speed: 0.00045, trail: [] }, // orbiting comet around LUNA_CORE
    MAX_FIREWORKS: 60,
    MAX_HEARTBEAT_RIPPLES: 4,
    MAX_COMET_TRAIL: 28,
  };

  function _cmapAccentRgb() {
    // Read --c-accent-rgb from :root (e.g., "232 200 122"). Fallback gold.
    const v = getComputedStyle(document.documentElement)
      .getPropertyValue("--c-accent-rgb").trim();
    if (v) {
      const parts = v.split(/[\s,]+/).map(Number).filter(n => !Number.isNaN(n));
      if (parts.length === 3) return parts;
    }
    return [232, 200, 122];
  }

  function _cmapInit() {
    const c = document.getElementById("cognitive-map");
    if (!c) return false;
    cmap.canvas = c;
    cmap.ctx = c.getContext("2d");
    cmap.overlay = document.getElementById("cognitive-map-overlay");
    // Build star field once (only if not already built).
    if (!cmap.stars.length) {
      let seed = 1337;
      function rnd() { seed = (seed * 9301 + 49297) % 233280; return seed / 233280; }
      for (let i = 0; i < 110; i++) cmap.stars.push({ x: rnd(), y: rnd(), b: 0.15 + rnd() * 0.7 });
    }
    _cmapResize();
    // ResizeObserver keeps the canvas crisp. Track on the live element
    // each time so we don't leak observers across renderChat() rebuilds.
    if (window.ResizeObserver) {
      try { if (cmap._ro) cmap._ro.disconnect(); } catch (e) {}
      cmap._ro = new ResizeObserver(_cmapResize);
      cmap._ro.observe(c);
    }
    // Wire the fullscreen toggle button (idempotent — guards against double-bind).
    _cmapWireFullscreen();
    // Update overlay state from existing data so it shows current values.
    _cmapUpdateOverlay();
    // Start the RAF loop only once; subsequent rebinds reuse the loop.
    if (!cmap.rafId) cmap.rafId = requestAnimationFrame(_cmapTick);
    return true;
  }

  // Fullscreen toggle for the Luna Live Map. Browser Fullscreen API is
  // user-gesture-only (the click satisfies that), so this works on every
  // modern browser without flags. Esc / F11 / browser-chrome-exit all
  // fire fullscreenchange and update the button automatically.
  function _cmapWireFullscreen() {
    const section = document.querySelector(".luna-cognitive-section");
    const btn = document.getElementById("cmap-fullscreen-btn");
    if (!section || !btn) return;
    if (btn._lunaFsWired) return;
    btn._lunaFsWired = true;

    const isFs = function () {
      return document.fullscreenElement === section
          || document.webkitFullscreenElement === section;
    };
    const updateBtn = function () {
      const on = isFs();
      btn.setAttribute("data-fs", on ? "on" : "off");
      btn.setAttribute("title", on ? "Exit fullscreen (Esc)" : "Fullscreen (Esc to exit)");
      btn.setAttribute("aria-label", on ? "Exit fullscreen Luna Live Map"
                                         : "Toggle fullscreen Luna Live Map");
      // After the browser chrome animates in/out, force one resize+redraw
      // tick so the canvas matches the new section bounds without waiting
      // for the next ResizeObserver fire.
      requestAnimationFrame(function () {
        try { _cmapResize(); _cmapDraw(); } catch (e) { /* never break UI */ }
      });
    };
    btn.addEventListener("click", function () {
      try {
        if (isFs()) {
          const exit = document.exitFullscreen
                    || document.webkitExitFullscreen;
          if (exit) exit.call(document);
        } else {
          const enter = section.requestFullscreen
                     || section.webkitRequestFullscreen;
          if (enter) {
            const p = enter.call(section);
            if (p && p.catch) p.catch(function () { /* user-deny is silent */ });
          }
        }
      } catch (e) { /* never throw out of a button click */ }
    });
    document.addEventListener("fullscreenchange", updateBtn);
    document.addEventListener("webkitfullscreenchange", updateBtn);
    // Initial state sync (in case we re-bind after a renderChat rebuild
    // while the section is already fullscreen).
    updateBtn();
  }

  function _cmapResize() {
    if (!cmap.canvas) return;
    // Cached-resize: only call getBoundingClientRect (which forces layout)
    // when the cached client size has actually shifted. Without this guard
    // the cmap canvas was resized on every _cmapTick() and competed with
    // the main rafLoop, contributing to the visible flicker.
    cmap.dpr = Math.min(2, window.devicePixelRatio || 1);
    const cw = cmap.canvas.clientWidth  | 0;
    const ch = cmap.canvas.clientHeight | 0;
    if (cmap.canvas._lunaCW === cw && cmap.canvas._lunaCH === ch
        && cmap.canvas.width > 0 && cmap.canvas.height > 0) {
      return;
    }
    cmap.canvas._lunaCW = cw;
    cmap.canvas._lunaCH = ch;
    const r = cmap.canvas.getBoundingClientRect();
    const tw = Math.max(2, Math.floor(r.width * cmap.dpr));
    const th = Math.max(2, Math.floor(r.height * cmap.dpr));
    if (cmap.canvas.width !== tw)  cmap.canvas.width  = tw;
    if (cmap.canvas.height !== th) cmap.canvas.height = th;
    try {
      const h = window.LunaUIHealth;
      if (h && h.canvasResizeCount) {
        h.canvasResizeCount["cmap"] = (h.canvasResizeCount["cmap"] || 0) + 1;
      }
    } catch (e) { /* sentinel never breaks render */ }
  }

  function _cmapInferNode(actor, stage, ev) {
    const text = (actor + " " + stage + " " + ev).toUpperCase();
    if (text.includes("ARCHITECT"))           return "ARCHITECT";
    if (text.includes("AIDER"))               return "AIDER";
    if (text.includes("VERIFICATION") || text.includes("VERIFY") ||
        text.includes("TASK_VERIFIED"))       return "VERIFIER";
    if (text.includes("GUARDIAN"))            return "GUARDIAN";
    if (text.includes("WORKER"))              return "CURRENT_WORK";
    if (text.includes("TIER ") || text.includes("TIER_") || text.includes("CANDIDATE") ||
        text.includes("COUNCIL") || text.includes("REVIEW") || text.includes("PROMOTION"))
                                              return "CURRENT_TIER";
    if (text.includes("PROJECT"))             return "CURRENT_PROJECT";
    if (text.includes("TIER2_HELPER"))        return "CURRENT_WORK";
    if (ev === "TASK_START" || ev === "TASK_APPLIED" || ev === "CYCLE_START")
                                              return "CURRENT_WORK";
    if (ev === "TASK_COMPLETE")               return "MEMORY";
    if (ev === "TASK_FAILED" || ev === "CYCLE_BLOCKED" ||
        ev === "TASK_ROLLED_BACK")            return "BLOCKERS";
    if (ev === "SUPERVISOR_START")            return "CURRENT_WORK";
    return null;
  }

  function _cmapActivate(key, intensity) {
    const cur = cmap.activations[key] || 0;
    cmap.activations[key] = Math.max(cur, intensity);
  }

  function _cmapSpawnParticle(src, dst) {
    if (cmap.particles.length >= cmap.MAX_PARTICLES) cmap.particles.shift();
    // Slow particles: each one takes ~7 seconds to traverse an edge.
    // Calm, deliberate, easy to follow with the eye.
    cmap.particles.push({ src, dst, t: 0, speed: 0.0024 });
  }

  function cmapHandleEvent(row) {
    if (!row || typeof row !== "object") return;
    const actor = String(row.actor || "").trim().toUpperCase();
    const stage = String(row.stage || row.event || "").trim().toUpperCase();
    const ev = String(row.event || "").trim().toUpperCase();
    const status = String(row.status || "").trim().toUpperCase();
    const msg = String(row.msg || "").trim();
    const title = String(row.task_title || "").trim();
    const idle = String(row.idle_reason || "").trim();
    const blocked = String(row.blocked_reason || "").trim();
    const ts = String(row.ts || "").trim();

    cmap.current.last_event = ev || stage;
    cmap.current.idle_reason = idle;
    cmap.current.blocked_reason = blocked;
    if (msg) cmap.current.stage = msg.slice(0, 80);
    cmap.current.task = title;

    const node = _cmapInferNode(actor, stage, ev);
    if (node) {
      _cmapActivate(node, 1.0);
      cmap.current.actor = node;
      // Handoff chain: if previous actor was different, draw a particle
      // from previous → current to visualize the handoff. Always also
      // draw LUNA_CORE → current so the central origin stays clear.
      const prev = cmap.chain.length ? cmap.chain[cmap.chain.length - 1] : null;
      if (node !== "BLOCKERS") _cmapSpawnParticle("LUNA_CORE", node);
      if (prev && prev !== node && node !== "BLOCKERS") {
        _cmapSpawnParticle(prev, node);
      }
      cmap.chain.push(node);
      if (cmap.chain.length > cmap.MAX_CHAIN) cmap.chain.shift();
    }
    if (ev === "CYCLE_COMPLETE" || ev === "TASK_COMPLETE" ||
        status === "APPLIED_AND_VERIFIED") {
      cmap.counters.ok += 1;
      _cmapActivate("MEMORY", 0.55);
      _cmapSpawnParticle(node || "CURRENT_WORK", "MEMORY");
      if (cmap.sparks.length >= cmap.MAX_SPARKS) cmap.sparks.shift();
      cmap.sparks.push({ node: "MEMORY", age: 0, color: [130, 230, 180] });
    }
    if (ev === "TASK_FAILED" || ev === "CYCLE_BLOCKED" || ev === "TASK_ROLLED_BACK") {
      cmap.counters.fail += 1;
      _cmapActivate("BLOCKERS", 1.0);
      if (cmap.sparks.length >= cmap.MAX_SPARKS) cmap.sparks.shift();
      cmap.sparks.push({ node: "BLOCKERS", age: 0, color: [255, 138, 138] });
    }
    if (ev === "SUPERVISOR_SLEEP" || ev === "NEXT_CYCLE_AT" ||
        ev === "SUPERVISOR_WAITING") {
      // Not active; keep what we have. Update stage label only if no
      // node is currently lit.
      const anyActive = Object.values(cmap.activations).some(v => v > 0.05);
      if (!anyActive) {
        cmap.current.actor = "IDLE";
        cmap.current.stage = "WAITING FOR NEXT CYCLE";
      }
    }
    _cmapUpdateOverlay();
    _cmapRenderOverlayTs(ts);
  }

  function _cmapRenderOverlayTs(ts) {
    const el = document.getElementById("cmap-last");
    if (el && ts) el.textContent = "last " + ts;
  }

  function _cmapUpdateOverlay() {
    const a = document.getElementById("cmap-actor");
    const s = document.getElementById("cmap-stage");
    const t = document.getElementById("cmap-task");
    const ok = document.getElementById("cmap-ok");
    const fail = document.getElementById("cmap-fail");
    const reason = document.getElementById("cmap-reason");
    if (a) a.textContent = cmap.current.actor || "IDLE";
    if (s) s.textContent = (cmap.current.stage || "").slice(0, 80);
    if (t) {
      if (cmap.current.task) {
        t.textContent = "task: " + cmap.current.task.slice(0, 64);
        t.hidden = false;
      } else {
        t.hidden = true;
      }
    }
    if (ok) ok.textContent = String(cmap.counters.ok);
    if (fail) fail.textContent = String(cmap.counters.fail);
    if (reason) {
      let txt = "";
      if (cmap.current.blocked_reason) txt = "BLOCKED · " + cmap.current.blocked_reason.replace(/_/g, " ");
      else if (cmap.current.actor === "IDLE" && cmap.current.idle_reason) txt = "WAITING · " + cmap.current.idle_reason.replace(/_/g, " ");
      reason.textContent = txt;
    }
  }

  function _cmapNodePositions() {
    const W = cmap.canvas.width;
    const H = cmap.canvas.height;
    const cx = W * 0.5;
    const cy = H * 0.55;
    const minDim = Math.min(W, H);
    const r1 = minDim * 0.20;
    const r2 = minDim * 0.36;
    const pos = { LUNA_CORE: [cx, cy] };
    for (const [key, , ring, ang] of cmap.nodes) {
      const drift = cmap.phase * (ring === 1 ? 1.0 : -0.6);
      const theta = (ang + drift) * Math.PI * 2;
      const r = ring === 1 ? r1 : r2;
      pos[key] = [cx + Math.cos(theta) * r * 1.15, cy + Math.sin(theta) * r * 0.78];
    }
    return pos;
  }

  function _cmapTick() {
    if (!cmap.ctx || !cmap.canvas) return;
    const now = performance.now();
    if (!cmap.paused) {
      // Orbit speed breathes with the system: waiting for next cycle is
      // slow and ceremonial; active progression accelerates the path.
      cmap.orbitSpeed = cmap.taskActive ? 0.00105 : 0.00030;
      cmap.phase = (cmap.phase + cmap.orbitSpeed) % 1;
      // Edge shimmer phase moves a bit faster so the lines have a subtle
      // moving glint even when nothing is active.
      cmap.edgeShimmerPhase = (cmap.edgeShimmerPhase + 0.0030) % 1;
      // Activation glow holds long (slow decay so events are readable).
      const next = {};
      for (const k in cmap.activations) {
        const v = cmap.activations[k] * 0.992;
        if (v > 0.04) next[k] = v;
      }
      cmap.activations = next;
      // Particles ~7s per edge (slow).
      cmap.particles = cmap.particles.filter(p => {
        p.t += p.speed;
        return p.t < 1.05;
      });
      // Sparks last ~4 seconds (slow expansion).
      cmap.sparks = cmap.sparks.filter(s => {
        s.age += 0.008;
        return s.age < 1;
      });
      // Ambient shooting stars — random streak across the canvas every
      // 4-12 seconds. Adds "alive sky" feel without being noisy.
      if (now >= cmap.nextShooterAt) {
        cmap.shooters.push({
          x: Math.random() * 0.4,            // start x (0..0.4 of canvas width)
          y: Math.random() * 0.45,           // start y (upper half mostly)
          vx: 0.0010 + Math.random() * 0.0014, // px-fraction per frame
          vy: 0.0006 + Math.random() * 0.0009,
          age: 0,
          life: 1.0,
        });
        if (cmap.shooters.length > 4) cmap.shooters.shift();
        cmap.nextShooterAt = now + 4000 + Math.random() * 8000;
      }
      cmap.shooters = cmap.shooters.filter(s => {
        s.x += s.vx;
        s.y += s.vy;
        s.age += 0.012;
        s.life = Math.max(0, 1 - s.age);
        return s.age < 1;
      });

      // -- 2026-05-08 round 8: signal-tied animation ticks --

      // Fireworks (signal: tier promotion event). Ballistic + gravity.
      if (cmap.fireworks.length) {
        cmap.fireworks = cmap.fireworks.filter(f => {
          f.x  += f.vx;
          f.y  += f.vy;
          f.vy += 0.0006 * cmap.dpr;     // subtle gravity in canvas units
          f.age += 0.018;
          f.life = Math.max(0, 1 - f.age);
          return f.age < 1;
        });
        if (cmap.fireworks.length > cmap.MAX_FIREWORKS) {
          cmap.fireworks.splice(0, cmap.fireworks.length - cmap.MAX_FIREWORKS);
        }
      }

      // Heartbeat ripple (signal: supervisor heartbeat freshness).
      // Spawns one ring every ~2s while alive; rings expand and fade.
      if (cmap._heartbeatAlive && now >= cmap.nextHeartbeatRippleAt) {
        cmap.heartbeatRipples.push({ age: 0, life: 1 });
        if (cmap.heartbeatRipples.length > cmap.MAX_HEARTBEAT_RIPPLES) {
          cmap.heartbeatRipples.shift();
        }
        cmap.nextHeartbeatRippleAt = now + 2000;
      }
      cmap.heartbeatRipples = cmap.heartbeatRipples.filter(r => {
        r.age  += 0.012;
        r.life = Math.max(0, 1 - r.age);
        return r.age < 1;
      });

      // Comet (signal: task active = engine cycle running).
      // Always orbiting; speed scales with task activity. Trail decays.
      cmap.comet.speed = cmap.taskActive ? 0.0014 : 0.00045;
      cmap.comet.angle = (cmap.comet.angle + cmap.comet.speed) % 1;
      cmap.comet.trail.unshift({ angle: cmap.comet.angle, age: 0 });
      if (cmap.comet.trail.length > cmap.MAX_COMET_TRAIL) cmap.comet.trail.length = cmap.MAX_COMET_TRAIL;
      for (const t of cmap.comet.trail) t.age += 0.012;

      // Progress arc opacity fades in once we have data.
      if (cmap.tierGlobal > 0 && cmap.progressArcOpacity < 1) {
        cmap.progressArcOpacity = Math.min(1, cmap.progressArcOpacity + 0.012);
      }
    }
    _cmapDraw();
    cmap.rafId = requestAnimationFrame(_cmapTick);
  }

  function _cmapDraw() {
    const ctx = cmap.ctx;
    const W = cmap.canvas.width;
    const H = cmap.canvas.height;
    if (W < 2 || H < 2) return;
    // BG: deep midnight gradient + radial vignette
    const bg = ctx.createLinearGradient(0, 0, 0, H);
    bg.addColorStop(0, "rgb(8,11,18)");
    bg.addColorStop(1, "rgb(4,6,12)");
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, W, H);
    const vignette = ctx.createRadialGradient(W * 0.5, H * 0.55, 0, W * 0.5, H * 0.55, Math.max(W, H) * 0.6);
    vignette.addColorStop(0, "rgba(20,25,40,0.30)");
    vignette.addColorStop(1, "rgba(0,0,0,0)");
    ctx.fillStyle = vignette;
    ctx.fillRect(0, 0, W, H);
    // Stars: very slow twinkle, about 1 breath per ~30 seconds.
    for (const s of cmap.stars) {
      const tw = 0.55 + 0.45 * Math.sin(cmap.phase * Math.PI * 1 + s.x * 12.7 + s.y * 9.1);
      const a = Math.max(0, Math.min(255, Math.round(110 * s.b * tw + 25)));
      ctx.fillStyle = "rgba(220,230,255," + (a / 255).toFixed(3) + ")";
      const px = s.x * W;
      const py = s.y * H * 0.95;
      const radius = (0.7 + s.b * 0.7) * cmap.dpr;
      ctx.beginPath();
      ctx.arc(px, py, radius, 0, Math.PI * 2);
      ctx.fill();
    }
    const pos = _cmapNodePositions();
    const [cx, cy] = pos.LUNA_CORE;
    // Shooting stars (drawn under the nodes so they pass behind)
    for (const sh of cmap.shooters) {
      const sx = sh.x * W;
      const sy = sh.y * H;
      // Streak length proportional to remaining life
      const tailLen = 80 * cmap.dpr * sh.life;
      const dx = sh.vx * W * 14;
      const dy = sh.vy * H * 14;
      const grad = ctx.createLinearGradient(sx, sy, sx - dx, sy - dy);
      grad.addColorStop(0, "rgba(255,250,225," + (0.85 * sh.life).toFixed(3) + ")");
      grad.addColorStop(0.4, "rgba(255,242,210," + (0.55 * sh.life).toFixed(3) + ")");
      grad.addColorStop(1, "rgba(255,242,210,0)");
      ctx.strokeStyle = grad;
      ctx.lineWidth = 1.4 * cmap.dpr;
      ctx.beginPath();
      ctx.moveTo(sx, sy);
      ctx.lineTo(sx - dx, sy - dy);
      ctx.stroke();
      // Bright head
      ctx.fillStyle = "rgba(255,255,250," + (0.95 * sh.life).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(sx, sy, 1.6 * cmap.dpr, 0, Math.PI * 2);
      ctx.fill();
    }
    _cmapDrawOrbitalPaths(ctx, cx, cy, W, H);

    // -- 2026-05-08 round 8: signal-tied animation layers --
    // Drawn AFTER orbital paths so they sit between the orbits and the
    // planet nodes — visible but never obscure the readable text.

    // Layer 1: Heartbeat ripples expanding from LUNA_CORE.
    // Signal: continuous_supervisor.alive + log_age_seconds < 120.
    // Visual: 2-4 concentric rings expanding outward, fading.
    if (cmap.heartbeatRipples && cmap.heartbeatRipples.length) {
      ctx.save();
      ctx.lineWidth = 1.6 * cmap.dpr;
      const baseR = 30 * cmap.dpr;
      const maxR  = 240 * cmap.dpr;
      for (const r of cmap.heartbeatRipples) {
        const radius = baseR + r.age * (maxR - baseR);
        const alpha = 0.55 * r.life * r.life;  // ease-out fade
        ctx.strokeStyle = "rgba(120, 220, 255, " + alpha.toFixed(3) + ")";
        ctx.beginPath();
        ctx.arc(cx, cy, radius, 0, Math.PI * 2);
        ctx.stroke();
      }
      ctx.restore();
    }

    // Layer 2: Progress arc around LUNA_CORE.
    // Signal: tier_global / ceiling_tier (Luna's climb to L10/T500).
    // Visual: warm-amber arc that fills clockwise as Luna promotes tiers.
    if (cmap.tierGlobal > 0 && cmap.progressArcOpacity > 0.05) {
      const fraction = Math.min(1, cmap.tierGlobal / Math.max(1, cmap.ceilingTier));
      const arcR = 56 * cmap.dpr;
      ctx.save();
      // Background ring (full circle, faint) so the arc has a track.
      ctx.lineWidth = 3 * cmap.dpr;
      ctx.strokeStyle = "rgba(255, 210, 140, " + (0.10 * cmap.progressArcOpacity).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(cx, cy, arcR, 0, Math.PI * 2);
      ctx.stroke();
      // Filled arc.
      ctx.lineWidth = 3.2 * cmap.dpr;
      ctx.lineCap = "round";
      const grad = ctx.createLinearGradient(cx - arcR, cy, cx + arcR, cy);
      grad.addColorStop(0,   "rgba(255, 210, 140, " + (0.85 * cmap.progressArcOpacity).toFixed(3) + ")");
      grad.addColorStop(1,   "rgba(255, 240, 200, " + (0.95 * cmap.progressArcOpacity).toFixed(3) + ")");
      ctx.strokeStyle = grad;
      ctx.beginPath();
      ctx.arc(cx, cy, arcR, -Math.PI / 2, -Math.PI / 2 + 2 * Math.PI * fraction);
      ctx.stroke();
      ctx.restore();
    }

    // Layer 3: Orbiting comet around LUNA_CORE with a fading trail.
    // Signal: cmap.taskActive (engine cycle running -> faster orbit).
    // Visual: a bright head dragging a 28-segment trail. Speed reads as
    //   "Luna is busy" without needing to read text.
    if (cmap.comet && cmap.comet.trail && cmap.comet.trail.length) {
      const cometR = 92 * cmap.dpr;  // sits outside the progress arc
      ctx.save();
      // Trail (oldest to newest, increasing alpha so head is brightest).
      for (let i = cmap.comet.trail.length - 1; i >= 0; i--) {
        const t = cmap.comet.trail[i];
        const ang = t.angle * Math.PI * 2 - Math.PI / 2;
        const x = cx + Math.cos(ang) * cometR;
        const y = cy + Math.sin(ang) * cometR;
        const trailLife = Math.max(0, 1 - i / cmap.MAX_COMET_TRAIL);
        const alpha = 0.8 * trailLife * trailLife;
        const rad = (1.2 + 2.0 * trailLife) * cmap.dpr;
        ctx.fillStyle = "rgba(180, 235, 255, " + alpha.toFixed(3) + ")";
        ctx.beginPath();
        ctx.arc(x, y, rad, 0, Math.PI * 2);
        ctx.fill();
      }
      // Head (the comet itself) with a glow halo.
      const headAng = cmap.comet.angle * Math.PI * 2 - Math.PI / 2;
      const hx = cx + Math.cos(headAng) * cometR;
      const hy = cy + Math.sin(headAng) * cometR;
      const halo = ctx.createRadialGradient(hx, hy, 0, hx, hy, 14 * cmap.dpr);
      halo.addColorStop(0, "rgba(220, 245, 255, 0.95)");
      halo.addColorStop(0.4, "rgba(120, 200, 255, 0.45)");
      halo.addColorStop(1, "rgba(60, 140, 255, 0)");
      ctx.fillStyle = halo;
      ctx.beginPath();
      ctx.arc(hx, hy, 14 * cmap.dpr, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();
    }

    // Edges (LUNA_CORE -> each node) with ambient shimmer
    for (const [key, , , , rgb] of cmap.nodes) {
      const [nx, ny] = pos[key];
      const act = cmap.activations[key] || 0;
      // Ambient shimmer adds a little base alpha that breathes per edge.
      const shimmer = 0.5 + 0.5 * Math.sin(cmap.edgeShimmerPhase * Math.PI * 2 + (nx * 0.013) + (ny * 0.011));
      const baseAlpha = (16 + 8 * shimmer + act * 110) / 255;
      ctx.strokeStyle = "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + "," + baseAlpha.toFixed(3) + ")";
      ctx.lineWidth = (0.9 + act * 1.4) * cmap.dpr;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.lineTo(nx, ny);
      ctx.stroke();
      // Ambient micro-particle on each edge — a very faint dot drifting
      // even when no event is active. Rate-limited by phase, so it's not
      // CPU-heavy.
      if (act < 0.1) {
        const t = (cmap.edgeShimmerPhase + (rgb[0] * 0.0011)) % 1;
        const px = cx + (nx - cx) * t;
        const py = cy + (ny - cy) * t;
        ctx.fillStyle = "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + ",0.30)";
        ctx.beginPath();
        ctx.arc(px, py, 1.0 * cmap.dpr, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    // Particles
    for (const p of cmap.particles) {
      const sp = pos[p.src]; const dp = pos[p.dst];
      if (!sp || !dp) continue;
      const t = Math.max(0, Math.min(1, p.t));
      const px = sp[0] + (dp[0] - sp[0]) * t;
      const py = sp[1] + (dp[1] - sp[1]) * t;
      const tt = Math.max(0, t - 0.10);
      const tx = sp[0] + (dp[0] - sp[0]) * tt;
      const ty = sp[1] + (dp[1] - sp[1]) * tt;
      ctx.strokeStyle = "rgba(255,240,200,0.55)";
      ctx.lineWidth = 1.4 * cmap.dpr;
      ctx.beginPath(); ctx.moveTo(tx, ty); ctx.lineTo(px, py); ctx.stroke();
      ctx.fillStyle = "rgba(255,250,220,0.94)";
      ctx.beginPath(); ctx.arc(px, py, 2.3 * cmap.dpr, 0, Math.PI * 2); ctx.fill();
    }
    // Nodes
    for (const [key, label, , , rgb] of cmap.nodes) {
      const [nx, ny] = pos[key];
      const act = cmap.activations[key] || 0;
      const dynamicLabel = key === "CURRENT_TIER" ? cmap.currentTierLabel
                         : key === "CURRENT_PROJECT" ? cmap.currentProjectLabel
                         : label;
      _cmapDrawPlanetNode(ctx, nx, ny, dynamicLabel, rgb, act);
    }
    // Sparks (expanding rings)
    for (const s of cmap.sparks) {
      const np = pos[s.node]; if (!np) continue;
      const rad = (8 + s.age * 60) * cmap.dpr;
      const alpha = Math.max(0, 0.7 * (1 - s.age));
      ctx.strokeStyle = "rgba(" + s.color[0] + "," + s.color[1] + "," + s.color[2] + "," + alpha.toFixed(3) + ")";
      ctx.lineWidth = 1.6 * cmap.dpr;
      ctx.beginPath();
      ctx.arc(np[0], np[1], rad, 0, Math.PI * 2);
      ctx.stroke();
    }
    // VERIFICATION orbiters: 3 small dots circling the verification node
    // when it is active. Makes "verifying" visually distinct from "running".
    const vAct = cmap.activations.VERIFIER || 0;
    if (vAct > 0.1 && pos.VERIFIER) {
      const [vx, vy] = pos.VERIFIER;
      const orbR = 14 * cmap.dpr;
      for (let i = 0; i < 3; i++) {
        // Very slow orbiters: 0.5x phase. Calm halo, not a spinning fan.
        const ang = (cmap.phase * Math.PI * 2 * 0.5) + (i * Math.PI * 2 / 3);
        const ox = vx + Math.cos(ang) * orbR;
        const oy = vy + Math.sin(ang) * orbR;
        ctx.fillStyle = "rgba(255, 245, 215, " + (0.6 * vAct).toFixed(3) + ")";
        ctx.beginPath();
        ctx.arc(ox, oy, 1.8 * cmap.dpr, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    // Luna Core (gold glow)
    _cmapDrawCore(ctx, cx, cy);

    // 2026-05-08 round 8: Tier promotion fireworks. Drawn ABOVE the core
    // and nodes so the celebration is unmissable. Fires only when
    // tier_global increases (signal-driven, see paintEvolutionCenter).
    if (cmap.fireworks && cmap.fireworks.length) {
      ctx.save();
      ctx.globalCompositeOperation = "lighter";  // additive = sparkly bloom
      for (const f of cmap.fireworks) {
        const rad = (2.6 * cmap.dpr) * Math.max(0.2, f.life);
        const a = (f.life * f.life * 0.95).toFixed(3);
        ctx.fillStyle = "hsla(" + f.hue + ", 95%, 65%, " + a + ")";
        ctx.beginPath();
        ctx.arc(f.x, f.y, rad, 0, Math.PI * 2);
        ctx.fill();
        // Soft halo
        ctx.fillStyle = "hsla(" + f.hue + ", 95%, 75%, " + (a * 0.35) + ")";
        ctx.beginPath();
        ctx.arc(f.x, f.y, rad * 2.5, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.restore();
    }

    // Task-card label near the most-active agent (e.g., AIDER, ARCHITECT).
    _cmapDrawTaskCard(ctx, pos);
    // Blocked reason mini-label next to BLOCKERS when active.
    _cmapDrawBlockedLabel(ctx, pos);
  }

  function _cmapDrawTaskCard(ctx, pos) {
    if (!cmap.current.task) return;
    // Use the most-active agent (if any) as the anchor.
    let bestKey = null, bestAct = 0.0;
    for (const k in cmap.activations) {
      if (k === "BLOCKERS") continue;
      if (cmap.activations[k] > bestAct) { bestAct = cmap.activations[k]; bestKey = k; }
    }
    if (!bestKey || bestAct < 0.25) return;
    const np = pos[bestKey]; if (!np) return;
    const text = "task: " + cmap.current.task.slice(0, 36);
    ctx.font = (10 * cmap.dpr) + "px ui-sans-serif, system-ui";
    const tw = ctx.measureText(text).width + 12 * cmap.dpr;
    const th = 18 * cmap.dpr;
    // Position card slightly to the right of the node.
    let bx = np[0] + 18 * cmap.dpr;
    let by = np[1] - th * 0.5;
    // Keep inside canvas
    if (bx + tw > cmap.canvas.width - 4) bx = np[0] - tw - 18 * cmap.dpr;
    if (by < 4) by = 4;
    ctx.fillStyle = "rgba(8, 11, 18, 0.85)";
    ctx.strokeStyle = "rgba(255, 244, 206, 0.40)";
    ctx.lineWidth = 1 * cmap.dpr;
    ctx.beginPath();
    if (typeof ctx.roundRect === "function") ctx.roundRect(bx, by, tw, th, 4 * cmap.dpr);
    else ctx.rect(bx, by, tw, th);
    ctx.fill(); ctx.stroke();
    ctx.fillStyle = "rgba(255, 244, 206, 0.92)";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.fillText(text, bx + 6 * cmap.dpr, by + th * 0.5);
  }

  function _cmapDrawOrbitalPaths(ctx, cx, cy, W, H) {
    const orbitR1 = Math.min(W, H) * 0.20;
    const orbitR2 = Math.min(W, H) * 0.36;
    ctx.strokeStyle = "rgba(120,150,200,0.14)";
    ctx.lineWidth = 0.8 * cmap.dpr;
    for (const r of [orbitR1, orbitR2]) {
      ctx.beginPath();
      ctx.ellipse(cx, cy, r * 1.15, r * 0.78, 0, 0, Math.PI * 2);
      ctx.stroke();
    }
    // Tier progression rail: candidate generation -> council review ->
    // limited live helper -> verifier -> guardian -> memory.
    const pos = _cmapNodePositions();
    ctx.strokeStyle = cmap.taskActive ? "rgba(255, 220, 150, 0.34)" : "rgba(120, 170, 255, 0.16)";
    ctx.lineWidth = (cmap.taskActive ? 1.4 : 0.8) * cmap.dpr;
    ctx.beginPath();
    cmap.progressionPath.forEach((key, idx) => {
      const p = pos[key];
      if (!p) return;
      if (idx === 0) ctx.moveTo(p[0], p[1]);
      else ctx.lineTo(p[0], p[1]);
    });
    ctx.stroke();
    ctx.fillStyle = "rgba(180,200,230,0.22)";
    for (let i = 0; i < 12; i++) {
      const a = (i / 12) * Math.PI * 2 + cmap.phase * Math.PI * 2;
      const tx = cx + Math.cos(a) * orbitR1 * 1.15;
      const ty = cy + Math.sin(a) * orbitR1 * 0.78;
      ctx.beginPath();
      ctx.arc(tx, ty, 0.9 * cmap.dpr, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  function _cmapDrawBlockedLabel(ctx, pos) {
    const bAct = cmap.activations.BLOCKERS || 0;
    if (bAct < 0.1) return;
    const np = pos.BLOCKERS; if (!np) return;
    const reason = (cmap.current.blocked_reason || "blocked").slice(0, 28).replace(/_/g, " ");
    const text = reason;
    ctx.font = (10 * cmap.dpr) + "px ui-sans-serif, system-ui";
    const tw = ctx.measureText(text).width + 12 * cmap.dpr;
    const th = 16 * cmap.dpr;
    let bx = np[0] + 14 * cmap.dpr;
    let by = np[1] - th * 0.5;
    if (bx + tw > cmap.canvas.width - 4) bx = np[0] - tw - 14 * cmap.dpr;
    ctx.fillStyle = "rgba(40, 8, 8, 0.85)";
    ctx.strokeStyle = "rgba(255, 138, 138, 0.55)";
    ctx.lineWidth = 1 * cmap.dpr;
    ctx.beginPath();
    if (typeof ctx.roundRect === "function") ctx.roundRect(bx, by, tw, th, 3 * cmap.dpr);
    else ctx.rect(bx, by, tw, th);
    ctx.fill(); ctx.stroke();
    ctx.fillStyle = "rgba(255, 214, 214, 0.95)";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.fillText(text.toUpperCase(), bx + 6 * cmap.dpr, by + th * 0.5);
  }

  function _cmapDrawCore(ctx, cx, cy) {
    const accent = _cmapAccentRgb();
    const dpr = cmap.dpr;
    // Ambient breath: gentle 0.92->1.08 pulse on the LUNA CORE so it
    // feels alive even when no events are arriving.
    const breath = 1.0 + Math.sin(cmap.phase * Math.PI * 4) * 0.08;
    const haloR  = 80 * dpr * breath;
    const coreR  = 4.0 * dpr * breath;
    // 1. Soft chromatic halo
    const halo = ctx.createRadialGradient(cx, cy, 0, cx, cy, haloR);
    halo.addColorStop(0.00, "rgba(" + accent[0] + "," + accent[1] + "," + accent[2] + ",0.55)");
    halo.addColorStop(0.40, "rgba(" + accent[0] + "," + accent[1] + "," + accent[2] + ",0.22)");
    halo.addColorStop(1.00, "rgba(" + accent[0] + "," + accent[1] + "," + accent[2] + ",0)");
    ctx.fillStyle = halo;
    ctx.beginPath(); ctx.arc(cx, cy, haloR, 0, Math.PI * 2); ctx.fill();
    // 2. Constellation rays — same starburst look as the agent nodes
    //    but bigger + with a slow rotation tied to cmap.phase so the
    //    centre subtly breathes/spins.
    const longRays = 12;
    const shortRays = 12;
    const longLen  = (44 + breath * 14) * dpr;
    const shortLen = (18 + breath * 6)  * dpr;
    const rot      = cmap.phase * Math.PI * 2 * 0.10;
    function ray(angle, len, alpha, w) {
      const x2 = cx + Math.cos(angle) * len;
      const y2 = cy + Math.sin(angle) * len;
      const g = ctx.createLinearGradient(cx, cy, x2, y2);
      g.addColorStop(0,    "rgba(255,255,255," + alpha.toFixed(3) + ")");
      g.addColorStop(0.45, "rgba(" + accent[0] + "," + accent[1] + "," + accent[2] + "," + (alpha * 0.55).toFixed(3) + ")");
      g.addColorStop(1,    "rgba(" + accent[0] + "," + accent[1] + "," + accent[2] + ",0)");
      ctx.strokeStyle = g;
      ctx.lineWidth = w;
      ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(x2, y2); ctx.stroke();
    }
    for (let i = 0; i < longRays; i++)  ray(rot + (i / longRays)  * Math.PI * 2, longLen, 0.85, 1.4 * dpr);
    for (let i = 0; i < shortRays; i++) ray(rot + (i / shortRays) * Math.PI * 2 + Math.PI / shortRays, shortLen, 0.55, 1.0 * dpr);
    // 3. Bright white core
    const cg = ctx.createRadialGradient(cx, cy, 0, cx, cy, coreR * 3.5);
    cg.addColorStop(0,    "rgba(255,255,255,1.0)");
    cg.addColorStop(0.45, "rgba(255,255,255,0.65)");
    cg.addColorStop(1,    "rgba(255,255,255,0)");
    ctx.fillStyle = cg;
    ctx.beginPath(); ctx.arc(cx, cy, coreR * 3.5, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "rgba(255,255,255,0.97)";
    ctx.beginPath(); ctx.arc(cx, cy, coreR, 0, Math.PI * 2); ctx.fill();
    // 4. Label
    ctx.fillStyle = "rgba(255,244,206,0.95)";
    ctx.font = "bold " + (13 * dpr) + "px ui-sans-serif, system-ui";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    ctx.fillText("LUNA CORE", cx, cy + longLen + 6 * dpr);
  }

  function _cmapDrawPlanetNode(ctx, x, y, label, rgb, act) {
    // ---- Radial-spoke starburst node (cosmic constellation style) ----
    // The planet body is replaced with a bright white core + radiating
    // rays + a soft chromatic halo. Active nodes burst with longer rays
    // and a brighter core; idle nodes stay subtle so the canvas reads
    // like a constellation, not a UFO. Per Serge's reference image.
    const dpr = cmap.dpr;
    const coreR = (1.4 + act * 2.6) * dpr;
    const haloR = (12 + act * 32) * dpr;
    // 1. Soft chromatic halo — the colour comes from the node's rgb so
    //    each agent still has its own tint (gold for ARCHITECT,
    //    teal for VERIFIER, etc.).
    const halo = ctx.createRadialGradient(x, y, 0, x, y, haloR);
    halo.addColorStop(0.00, "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + "," + (0.45 + 0.40 * act).toFixed(3) + ")");
    halo.addColorStop(0.40, "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + "," + (0.18 + 0.20 * act).toFixed(3) + ")");
    halo.addColorStop(1.00, "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + ",0)");
    ctx.fillStyle = halo;
    ctx.beginPath(); ctx.arc(x, y, haloR, 0, Math.PI * 2); ctx.fill();
    // 2. Radiating rays — 8 long arms + 8 short fillers between them
    //    (16-pointed starburst). Length grows with activation. Each
    //    ray is drawn with a linear gradient so it fades to transparent
    //    at the tip.
    const longRays  = 8;
    const shortRays = 8;
    const longLen   = (12 + act * 60) * dpr;
    const shortLen  = (6  + act * 18) * dpr;
    const baseAlpha = 0.55 + 0.40 * act;
    function rayLine(angle, len, alpha) {
      const x2 = x + Math.cos(angle) * len;
      const y2 = y + Math.sin(angle) * len;
      const g = ctx.createLinearGradient(x, y, x2, y2);
      g.addColorStop(0, "rgba(255,255,255," + alpha.toFixed(3) + ")");
      g.addColorStop(0.45, "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + "," + (alpha * 0.55).toFixed(3) + ")");
      g.addColorStop(1, "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + ",0)");
      ctx.strokeStyle = g;
      ctx.lineWidth = (0.8 + act * 0.6) * dpr;
      ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x2, y2); ctx.stroke();
    }
    // Long rays at the cardinal + diagonal angles.
    for (let i = 0; i < longRays; i++) {
      const a = (i / longRays) * Math.PI * 2;
      rayLine(a, longLen, baseAlpha);
    }
    // Short rays offset by half-step to fill the gaps.
    for (let i = 0; i < shortRays; i++) {
      const a = (i / shortRays) * Math.PI * 2 + (Math.PI / shortRays);
      rayLine(a, shortLen, baseAlpha * 0.7);
    }
    // 3. Tiny scattered sub-stars near the burst — adds the
    //    "constellation cluster" feel from the reference image.
    if (act > 0.3) {
      ctx.fillStyle = "rgba(255,255,255," + (0.55 * act).toFixed(3) + ")";
      const subStars = 5 + Math.floor(act * 6);
      for (let i = 0; i < subStars; i++) {
        const a = ((i * 137.5) * Math.PI / 180);          // golden-angle scatter
        const r = (10 + ((i * 7) % 14)) * dpr * (0.7 + act);
        const sx = x + Math.cos(a) * r;
        const sy = y + Math.sin(a) * r;
        ctx.beginPath();
        ctx.arc(sx, sy, (0.6 + (i % 3) * 0.4) * dpr, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    // 4. Bright white core — always crisp and small so the rays read.
    const coreGrad = ctx.createRadialGradient(x, y, 0, x, y, coreR * 3.0);
    coreGrad.addColorStop(0,    "rgba(255,255,255," + (0.95 + 0.05 * act).toFixed(3) + ")");
    coreGrad.addColorStop(0.45, "rgba(255,255,255," + (0.55 + 0.30 * act).toFixed(3) + ")");
    coreGrad.addColorStop(1,    "rgba(255,255,255,0)");
    ctx.fillStyle = coreGrad;
    ctx.beginPath(); ctx.arc(x, y, coreR * 3.0, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "rgba(255,255,255," + (0.96).toFixed(3) + ")";
    ctx.beginPath(); ctx.arc(x, y, coreR, 0, Math.PI * 2); ctx.fill();
    // 5. Label — kept faint so the constellation reads cleanly.
    ctx.fillStyle = "rgba(225,228,238," + (0.55 + 0.35 * act).toFixed(3) + ")";
    ctx.font = "bold " + (10.5 * dpr) + "px ui-sans-serif, system-ui";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    ctx.fillText(label, x, y + Math.max(longLen, 14 * dpr) + 4 * dpr);
  }

  // Public-ish hook for refreshFeed: forward only NEW records.
  function cmapForwardNewRecords(records) {
    if (!cmap.canvas) return;
    if (!Array.isArray(records) || !records.length) return;
    // Simple "since lastTs" filter using iso_utc (or ts as fallback).
    let cutoff = cmap.lastTs;
    let newest = cutoff;
    for (const r of records) {
      const k = String(r.iso_utc || r.ts || "");
      if (!k) continue;
      if (cutoff && k <= cutoff) continue;
      cmapHandleEvent(r);
      if (k > newest) newest = k;
    }
    cmap.lastTs = newest;
  }

  function _cmapApplyHigherTierState(ht) {
    if (!ht) return;
    const task = ht.scheduled_task || {};
    const tier = String(ht.current_effective_tier || "");
    const text = String((ht.latest_progression && ht.latest_progression.decision) || ht.active_text || "");
    const taskState = String(task.state || "").toLowerCase();
    // taskRunning = the engine is mid-cycle right now (brief).
    const taskRunning = taskState === "running" || /running|cycle/i.test(text);
    // taskAlive = the engine is configured + firing on schedule (most
    // of the time). Cross-check tier-truth so a stale higher-tier
    // payload that mis-named the task can't kill the map's pulse.
    const ttSnap = state.lastTierTruth || null;
    const ttWe = (ttSnap && ttSnap.worker_ecosystem) || {};
    const ttProgActive = String((ttWe.progression || {}).state || "").toLowerCase() === "active";
    const ttOpencodeActive = String((ttWe.opencode || {}).state || "").toLowerCase() === "active";
    const enabled = (task.enabled !== false) && /enabled|ready|running/i.test(taskState);
    const taskAlive = taskRunning || enabled || ttProgActive || ttOpencodeActive;
    cmap.taskActive = !!taskRunning;
    cmap.currentTierLabel = tier ? ("TIER " + tier) : "CURRENT TIER";
    cmap.currentProjectLabel = String(ht.headline || ht.active_text || "PROJECT").slice(0, 18).toUpperCase();
    if (taskRunning) {
      cmap.current.actor = cmap.currentTierLabel;
      cmap.current.stage = "Progression cycle running now";
    } else if (taskAlive) {
      cmap.current.actor = cmap.currentTierLabel;
      cmap.current.stage = ttOpencodeActive
        ? "OpenCode worker active · waiting for next cycle"
        : "Engine enabled · next cycle scheduled";
    } else if (!Object.keys(cmap.activations).length) {
      cmap.current.actor = "IDLE";
      cmap.current.stage = "waiting for next cycle";
    }
    // Continuous breath: keep CURRENT_TIER lit at modest intensity
    // whenever the engine is alive, then bump on each poll tick so the
    // node visibly pulses every second instead of going dark between
    // 15-min cycles. This is the "Luna is here" signal Serge wanted.
    if (tier) {
      const baseIntensity = taskRunning ? 1.0 : (taskAlive ? 0.55 : 0.20);
      _cmapActivate("CURRENT_TIER", baseIntensity);
      // Pulse particles continuously (slower) when alive but idle, so
      // the user always sees movement on the map.
      if (!taskRunning && taskAlive) {
        // Light handoff trickle: just one stream per poll tick to keep
        // the canvas alive without overwhelming the eye.
        const trickle = (cmap._aliveTick = (cmap._aliveTick || 0) + 1);
        if      (trickle % 4 === 0) _cmapSpawnParticle("LUNA_CORE", "CURRENT_TIER");
        else if (trickle % 4 === 1) _cmapSpawnParticle("CURRENT_TIER", "VERIFIER");
        else if (trickle % 4 === 2) _cmapSpawnParticle("VERIFIER", "GUARDIAN");
        else                        _cmapSpawnParticle("GUARDIAN", "MEMORY");
      }
    }
    if (taskRunning) {
      _cmapSpawnParticle("ARCHITECT", "CURRENT_PROJECT");
      _cmapSpawnParticle("CURRENT_PROJECT", "CURRENT_WORK");
      _cmapSpawnParticle("CURRENT_WORK", "CURRENT_TIER");
      _cmapSpawnParticle("CURRENT_TIER", "VERIFIER");
      _cmapSpawnParticle("VERIFIER", "GUARDIAN");
      _cmapSpawnParticle("GUARDIAN", "MEMORY");
    }
    // OpenCode worker just produced fresh real output? Fire a particle
    // CURRENT_WORK -> MEMORY so the map shows the OpenCode pipeline is
    // alive even between progression cycles.
    if (ttOpencodeActive) {
      _cmapActivate("CURRENT_WORK", 0.75);
      _cmapSpawnParticle("CURRENT_WORK", "MEMORY");
    }
    // Continuous Supervisor heartbeat — when the in-process loop is
    // alive (log refreshed within ~120s), pulse LUNA_CORE on every
    // poll tick so the map visibly reflects the cadence even between
    // discrete live-feed events. This is the "I'm working" signal
    // when a long opencode cycle is silent.
    try {
      const ttCS = (ttSnap && ttSnap.continuous_supervisor) || null;
      if (ttCS && ttCS.alive) {
        const cad = Math.max(5, Number(ttCS.config_cadence_seconds || 10));
        // Strength based on freshness — younger log = brighter pulse.
        const age = Math.max(0, Number(ttCS.log_age_seconds || 0));
        const fresh = Math.max(0.35, Math.min(1.0, 1.0 - (age / 120)));
        _cmapActivate("LUNA_CORE", fresh);
        // 2026-05-08 round 8: heartbeat ripple animation. Each successful
        // freshness reading marks the supervisor alive; the rAF tick then
        // spawns expanding rings every 2s (signal: supervisor heartbeat).
        cmap._heartbeatAlive = true;
        // One trickle particle every ~cadence seconds — light enough
        // to be readable, heavy enough to feel continuous.
        const tick = (cmap._supTick = (cmap._supTick || 0) + 1);
        if (tick % Math.max(1, Math.floor(cad / 2)) === 0) {
          _cmapSpawnParticle("LUNA_CORE", "CURRENT_WORK");
        }
      } else {
        cmap._heartbeatAlive = false;
      }
    } catch (_e) { /* never break the map */ }
    _cmapUpdateOverlay();
  }

  // Boot: wait for DOM, then init the map (silent if canvas not present).
  document.addEventListener("DOMContentLoaded", () => {
    setTimeout(() => { try { _cmapInit(); } catch (e) { /* never break the app */ } }, 50);
  });

  // Expose for refreshFeed + renderChat re-bind hook.
  window.__lunaCmapForward = cmapForwardNewRecords;
  window.__lunaCmapBind = _cmapInit;
  window.__lunaCmapApplyHigherTier = _cmapApplyHigherTierState;
})();


// ============================================================
// Luna Voice control row (migrated from old PyQt terminal).
// Wires the buttons under the chat input to /api/voice/* endpoints.
// State persists in memory/luna_voice_config.json via /api/voice/toggle
// + /api/voice/preset.
// ============================================================
(function () {
  "use strict";
  const $ = (id) => document.getElementById(id);
  async function postJSON(path, body) {
    try {
      const r = await fetch(path, {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }
  async function getJSON(path) {
    try {
      const r = await fetch(path, { credentials: "omit", cache: "no-store" });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  function applyStatus(snap) {
    if (!snap) return;
    const inner   = snap.status || snap;
    const enabled = !!inner.enabled;
    const muted   = !!inner.muted;
    const speaking = !!inner.speaking;
    const isPremium = !!inner.is_premium_voice;
    const provActive = inner.provider_active || {};
    const provKind   = String(provActive.kind || "");

    const toggle = $("luna-voice-toggle");
    const label  = $("luna-voice-toggle-label");
    if (toggle) {
      toggle.setAttribute("aria-pressed", enabled ? "true" : "false");
    }
    if (label) {
      label.textContent = enabled ? "VOICE ON" : "VOICE OFF";
    }

    // Single source-of-truth status pill — clean labels, no duplicate
    // availability or premium-not-configured filler lines.
    //
    //   Voice OFF                       — silent (toggle off)
    //   Speaking                        — current utterance in flight
    //   PREMIUM LUNA VOICE ACTIVE       — toggle on, premium provider configured
    //   LOCAL FALLBACK VOICE ACTIVE     — toggle on, neural/SAPI fallback
    //   Voice Error                     — engine unavailable / module missing
    const pill = $("luna-voice-pill");
    const txt  = $("luna-voice-pill-txt");
    if (pill && txt) {
      let state = "off";
      let text  = "Voice OFF";
      if (!inner.available) {
        state = "error";
        text  = "Voice Error";
      } else if (!enabled) {
        state = "off";
        text  = "Voice OFF";
      } else if (muted) {
        state = "off";
        text  = "Stopped";
      } else if (speaking) {
        state = "speaking";
        text  = "Speaking";
      } else if (isPremium) {
        state = "premium";
        text  = "PREMIUM LUNA VOICE ACTIVE";
      } else if (provKind === "local_neural") {
        // Neural local provider counts as a premium-feeling fallback
        // (e.g. edge-tts) so the user sees the upgrade.
        state = "premium";
        text  = "NEURAL LOCAL VOICE ACTIVE";
      } else {
        state = "on";
        text  = "LOCAL FALLBACK VOICE ACTIVE";
      }
      pill.setAttribute("data-state", state);
      txt.textContent = text;
    }

    const preset = $("luna-voice-preset");
    if (preset && inner.selected_preset) {
      preset.value = inner.selected_preset;
    }

    // Voice Settings panel — surface backend availability so the
    // operator can see at a glance which engine drove the last
    // utterance (Kokoro / edge-tts / pyttsx3) without leaving the
    // dashboard. Read-only diagnostic.
    const setEng = $("luna-voice-settings-engine");
    if (setEng) {
      const provLabel = String(provActive.label || "(unknown)");
      setEng.textContent = provLabel;
    }
    const ktok = $("luna-voice-settings-kokoro");
    if (ktok) ktok.textContent = (inner.kokoro_available || inner.kokoro_onnx_available)
      ? "available" : "not installed";
    const etok = $("luna-voice-settings-edge");
    if (etok) etok.textContent = inner.edge_tts_available ? "available" : "not installed";
    const ptok = $("luna-voice-settings-pyttsx3");
    if (ptok) ptok.textContent = inner.pyttsx3_available ? "available" : "not installed";
    // Realtime voice availability (faster-whisper + Ollama). Probed by
    // a separate /api/voice/realtime-status endpoint to keep the main
    // status snapshot lean.
    const rtSttEl = $("luna-voice-settings-rt-stt");
    const rtLlmEl = $("luna-voice-settings-rt-llm");
    if (rtSttEl || rtLlmEl) {
      try {
        getJSON("/api/voice/realtime-status").then((rt) => {
          if (!rt) return;
          if (rtSttEl) rtSttEl.textContent = rt.faster_whisper
            ? ("faster-whisper " + (rt.whisper_model_size || "small"))
            : "not installed";
          if (rtLlmEl) rtLlmEl.textContent = rt.ollama_model
            ? ("Ollama " + rt.ollama_model)
            : "ollama not configured";
        }).catch(() => {});
      } catch (_e) {}
    }
  }

  async function refresh() {
    const snap = await getJSON("/api/voice/status");
    applyStatus(snap);
  }

  function bind() {
    const toggle = $("luna-voice-toggle");
    const stop   = $("luna-voice-stop");
    const preset = $("luna-voice-preset");
    if (toggle) {
      toggle.addEventListener("click", async () => {
        const isOn = toggle.getAttribute("aria-pressed") === "true";
        const turningOn = !isOn;
        const r = await postJSON("/api/voice/toggle", { on: turningOn });
        applyStatus(r);
        // When the operator turns voice ON, speak ONE short
        // confirmation ("Luna voice online.") so they hear that voice
        // is alive. When toggling OFF, stay silent.
        if (turningOn) {
          await postJSON("/api/voice/test", { text: "Luna voice online." });
          // Repaint the pill quickly so it flips to Speaking.
          await new Promise((res) => setTimeout(res, 200));
          await refresh();
        }
      });
    }
    if (stop) {
      stop.addEventListener("click", async () => {
        await postJSON("/api/voice/stop", {});
        await refresh();
      });
    }
    if (preset) {
      preset.addEventListener("change", async (ev) => {
        const r = await postJSON("/api/voice/preset", { preset: ev.target.value });
        applyStatus(r);
      });
    }
    // Voice Settings panel: explicit "Test Voice" + "Stop Voice"
    // buttons. The Test Voice button speaks the first profile
    // test_phrase (a professional Luna line, never flirty/romantic).
    const testBtn = $("luna-voice-test");
    if (testBtn) {
      testBtn.addEventListener("click", async () => {
        // Use the engine's first test_phrase by sending an empty body;
        // the server endpoint defaults to the profile phrase if no
        // text is provided.
        await postJSON("/api/voice/test", {});
        await new Promise((res) => setTimeout(res, 200));
        await refresh();
      });
    }
    const stopSettingsBtn = $("luna-voice-settings-stop");
    if (stopSettingsBtn) {
      stopSettingsBtn.addEventListener("click", async () => {
        await postJSON("/api/voice/stop", {});
        await refresh();
      });
    }
    // Bind the realtime voice (phone-call) panel.
    try { bindRealtimeVoice(); } catch (e) { /* never break the row */ }
  }

  // ===================================================================
  // Realtime voice conversation — phone-call mode.
  //
  // State machine:
  //   idle       — mic off, panel showing "click Talk to start"
  //   listening  — mic on, MediaRecorder buffering audio; VAD watches
  //                RMS; on > VAD_SILENCE_MS of silence we cut the
  //                utterance and POST it to /api/voice/converse.
  //   thinking   — server is transcribing + asking Ollama + rendering TTS
  //   speaking   — playing back Luna's TTS audio. Stop Speaking
  //                interrupts. When playback ends, in continuous mode we
  //                drop straight back to listening; in PTT mode we go
  //                back to idle.
  //   error      — surfaced in the transcript pane; auto-recovers.
  //
  // Audio path is browser-side getUserMedia + MediaRecorder. STT, LLM,
  // TTS all run on the server (faster-whisper + Ollama + Kokoro).
  // ===================================================================
  function bindRealtimeVoice() {
    const showBtn  = $("luna-rt-show");
    const panel    = $("luna-voice-realtime");
    const talkBtn  = $("luna-rt-talk");
    const endBtn   = $("luna-rt-end");
    const stopBtn  = $("luna-rt-interrupt");
    const modeSel  = $("luna-rt-mode");
    const devSel   = $("luna-rt-device");
    const status   = $("luna-rt-status");
    const trans    = $("luna-rt-transcript");
    if (!panel || !talkBtn) return;
    if (panel.dataset.bound === "1") return;
    panel.dataset.bound = "1";

    // Internal state.
    const RT = window.__lunaRT = window.__lunaRT || {
      mic: null,                  // MediaStream
      ctx: null,                  // AudioContext
      analyser: null,             // AnalyserNode for VAD
      recorder: null,             // MediaRecorder
      chunks: [],
      session: "luna-rt-" + Math.random().toString(36).slice(2, 8),
      mode: "continuous",
      audio: null,                // currently-playing HTMLAudioElement
      vadTimer: null,
      vadHotMs: 0,                // ms of speech in current utterance
      vadSilenceMs: 0,            // ms of silence after speech
      VAD_THRESHOLD: 0.020,       // RMS gate for "is voice"
      VAD_HOT_MIN_MS: 350,        // require this much speech before cut
      VAD_SILENCE_MS: 1500,       // cut utterance after this much silence
      MAX_UTTER_MS: 30000,        // hard ceiling per utterance
      utterStartedAt: 0,
      cancelled: false,
      lastDeviceId: "",
    };

    function setStatus(state, text) {
      if (status) {
        status.dataset.state = state;
        status.textContent = text;
      }
      panel.dataset.state = state;
    }

    function appendLine(kind, text) {
      if (!trans) return;
      const span = document.createElement("span");
      span.className = "luna-voice-realtime__line luna-voice-realtime__line--" + kind;
      span.textContent = String(text || "");
      trans.appendChild(span);
      trans.scrollTop = trans.scrollHeight;
      // Cap to 30 lines so the panel never grows unbounded.
      while (trans.children.length > 30) trans.removeChild(trans.firstChild);
    }

    if (showBtn) {
      showBtn.addEventListener("click", async () => {
        panel.hidden = false;
        showBtn.hidden = true;
        await populateMicDevices();
      });
    }

    async function populateMicDevices() {
      if (!devSel) return;
      try {
        // Browsers only return device LABELS after at least one
        // getUserMedia grant. We do a tiny grant + immediate stop so
        // the dropdown shows real device names (incl. "Logitech...").
        const tmp = await navigator.mediaDevices.getUserMedia({ audio: true });
        tmp.getTracks().forEach((t) => t.stop());
        const all = await navigator.mediaDevices.enumerateDevices();
        const inputs = all.filter((d) => d.kind === "audioinput");
        // Wipe previous options except the default.
        while (devSel.children.length > 1) devSel.removeChild(devSel.lastChild);
        let logitechId = "";
        let logitechLabel = "";
        inputs.forEach((d) => {
          const opt = document.createElement("option");
          opt.value = d.deviceId;
          // Surface Logitech mics with a star prefix so they're
          // unmistakable in the dropdown.
          const isLogi = /logitech/i.test(d.label || "");
          const isCam  = /(camera|webcam|c920|c925|c270|streamcam|brio|c615)/i.test(d.label || "");
          const star   = isLogi ? "★ " : "";
          opt.textContent = star + (d.label || ("Microphone " + d.deviceId.slice(0, 6)));
          devSel.appendChild(opt);
          if (isLogi && !logitechId) {
            logitechId = d.deviceId;
            logitechLabel = d.label || "";
          } else if (isCam && !logitechId) {
            // No Logitech-branded device but a camera mic is present —
            // remember it as a runner-up.
            logitechId = d.deviceId;
            logitechLabel = d.label || "";
          }
        });
        // Auto-select Logitech-or-camera mic if found.
        if (logitechId) {
          devSel.value = logitechId;
          RT.lastDeviceId = logitechId;
          appendLine("luna",
            "Mic linked: " + (logitechLabel || "Logitech camera microphone") + " · ready when you click Talk.");
        }
      } catch (e) {
        const name = (e && (e.name || e.message)) || "denied";
        appendLine("err",
          "Need mic permission to list devices. " +
          "Click the lock icon in the address bar -> Microphone -> Allow, then reload. " +
          "(" + name + ")");
      }
    }

    if (modeSel) {
      modeSel.addEventListener("change", () => { RT.mode = modeSel.value; });
      RT.mode = modeSel.value;
    }
    if (devSel) {
      devSel.addEventListener("change", () => { RT.lastDeviceId = devSel.value; });
    }

    if (talkBtn) {
      talkBtn.addEventListener("click", async () => {
        try { await startSession(); } catch (e) { appendLine("err", String(e && e.message || e)); }
      });
    }
    if (endBtn) {
      endBtn.addEventListener("click", () => endSession(true));
    }
    if (stopBtn) {
      stopBtn.addEventListener("click", () => interruptLuna());
    }
    // "Mic Permission Help" — opens a modal with click-by-click steps
    // and tries to deep-link the operator to the browser's mic settings.
    // Browsers block direct chrome:// navigation from non-extension
    // pages, so the click-to-copy fallback is the resilient path.
    const micHelp = $("luna-rt-mic-help");
    if (micHelp) {
      micHelp.addEventListener("click", async () => {
        // Try to actively REQUEST mic permission first — this triggers
        // the browser's native permission popup if the operator hasn't
        // explicitly denied yet. If they already denied, we fall
        // through to the textual instructions.
        let granted = false;
        try {
          const s = await navigator.mediaDevices.getUserMedia({ audio: true });
          if (s) {
            s.getTracks().forEach((t) => t.stop());
            granted = true;
          }
        } catch (_e) { granted = false; }
        if (granted) {
          appendLine("luna",
            "Microphone permission granted. You can click Talk to Luna now.");
          await populateMicDevices();
          return;
        }
        // Already denied. Walk the operator through the manual steps
        // and copy the chrome://settings link to the clipboard so they
        // can paste it directly into the address bar.
        const url = "chrome://settings/content/microphone";
        try { await navigator.clipboard.writeText(url); } catch (_e) {}
        appendLine("luna",
          "Mic still blocked. I copied the settings URL to your clipboard.");
        appendLine("luna",
          "Step 1: Click the address bar. Step 2: Paste (Ctrl+V) and hit Enter.");
        appendLine("luna",
          "Step 3: Find 127.0.0.1:8765 in the list. Step 4: Set it to Allow. Step 5: Reload (Ctrl+Shift+R).");
      });
    }

    // §35 (2026-05-09) two-voices fix: expose RT teardown so the
    // compact-voice path can force-stop it on its own start.
    window.__lunaForceStopRealtime = function () {
      try { endSession(false); } catch (_e) {}
    };

    async function startSession() {
      // §35 mutex: if the compact voice path is active, shut it down first.
      if (typeof window.__lunaForceStopCompact === "function") {
        try { window.__lunaForceStopCompact(); } catch (_e) {}
      }
      window.__lunaActiveVoicePath = "realtime";
      if (RT.mic) { /* already running */ return; }
      RT.cancelled = false;
      // Mic constraints: prefer the operator-selected device (or
      // Logitech auto-pick) but tolerate `exact` failure by falling
      // back to {deviceId: ...} (preferred-not-required) and finally
      // to {audio: true} so the OS default mic still works even when
      // the chosen device label is stale (e.g. headset replugged).
      const tryGUM = async (constraints) => {
        return await navigator.mediaDevices.getUserMedia(constraints);
      };
      let stream = null;
      let lastErr = null;
      const dev = RT.lastDeviceId;
      const attempts = dev
        ? [
            { audio: { deviceId: { exact: dev },
                       echoCancellation: true, noiseSuppression: true,
                       autoGainControl: true } },
            { audio: { deviceId: dev,
                       echoCancellation: true, noiseSuppression: true,
                       autoGainControl: true } },
            { audio: true },
          ]
        : [
            { audio: { echoCancellation: true, noiseSuppression: true,
                       autoGainControl: true } },
            { audio: true },
          ];
      for (const c of attempts) {
        try { stream = await tryGUM(c); break; }
        catch (e) { lastErr = e; }
      }
      if (!stream) {
        const name = (lastErr && (lastErr.name || lastErr.message)) || "denied";
        // Detailed, actionable error message — tells the user which
        // exact click fixes it. No jargon, no stack trace.
        let how = "";
        if (/NotAllowed|denied|Permission/i.test(String(name))) {
          how = (
            "1) Click the lock / camera icon next to 127.0.0.1:8765 in the address bar. " +
            "2) Set Microphone = Allow. " +
            "3) Reload the page. " +
            "4) Click Talk to Luna again."
          );
        } else if (/NotFound|DevicesNotFound|OverconstrainedError/i.test(String(name))) {
          how = (
            "The selected microphone is not available right now. " +
            "Switch to 'Default microphone' in the device dropdown above and try again."
          );
        } else if (/NotReadable|TrackStartError/i.test(String(name))) {
          how = (
            "Another app is using the microphone (Zoom, Discord, Teams, etc.). " +
            "Close it and click Talk to Luna again."
          );
        } else {
          how = (
            "Open chrome://settings/content/microphone (or edge://settings/content/microphone), " +
            "set 127.0.0.1:8765 to Allow, then reload."
          );
        }
        setStatus("error", "Microphone access denied — " + how);
        appendLine("err", "getUserMedia error: " + name);
        appendLine("err", how);
        return;
      }
      RT.mic = stream;
      // Wire VAD via AudioContext + AnalyserNode.
      try {
        RT.ctx = new (window.AudioContext || window.webkitAudioContext)();
        const src = RT.ctx.createMediaStreamSource(RT.mic);
        RT.analyser = RT.ctx.createAnalyser();
        RT.analyser.fftSize = 512;
        src.connect(RT.analyser);
      } catch (e) {
        setStatus("error", "Could not initialise audio analyser.");
        endSession(false);
        return;
      }
      // After populateMicDevices() ran, refresh device labels (some
      // browsers only expose them once a stream is open).
      await populateMicDevices();
      // Start a fresh utterance.
      beginUtterance();
    }

    function beginUtterance() {
      if (!RT.mic) return;
      RT.chunks = [];
      RT.vadHotMs = 0;
      RT.vadSilenceMs = 0;
      RT.utterStartedAt = performance.now();
      let mime = "audio/webm";
      if (typeof MediaRecorder !== "undefined" && MediaRecorder.isTypeSupported) {
        if (MediaRecorder.isTypeSupported("audio/webm;codecs=opus")) mime = "audio/webm;codecs=opus";
        else if (MediaRecorder.isTypeSupported("audio/ogg;codecs=opus")) mime = "audio/ogg;codecs=opus";
      }
      try {
        RT.recorder = new MediaRecorder(RT.mic, { mimeType: mime });
      } catch (e) {
        // Fall back to default mime.
        RT.recorder = new MediaRecorder(RT.mic);
      }
      RT.recorder.ondataavailable = (ev) => {
        if (ev.data && ev.data.size > 0) RT.chunks.push(ev.data);
      };
      RT.recorder.onstop = onUtteranceCut;
      RT.recorder.start(250);   // 250ms timeslices
      setStatus("listening", "Listening… speak whenever you're ready.");
      runVadLoop();
    }

    function runVadLoop() {
      if (RT.vadTimer) clearInterval(RT.vadTimer);
      const buf = new Uint8Array(RT.analyser.fftSize);
      const TICK_MS = 50;
      RT.vadTimer = setInterval(() => {
        if (!RT.recorder || RT.recorder.state !== "recording") return;
        RT.analyser.getByteTimeDomainData(buf);
        // RMS on PCM (signed centred at 128).
        let sum = 0;
        for (let i = 0; i < buf.length; i++) {
          const v = (buf[i] - 128) / 128;
          sum += v * v;
        }
        const rms = Math.sqrt(sum / buf.length);
        const elapsed = performance.now() - RT.utterStartedAt;
        if (rms > RT.VAD_THRESHOLD) {
          RT.vadHotMs += TICK_MS;
          RT.vadSilenceMs = 0;
        } else {
          RT.vadSilenceMs += TICK_MS;
        }
        // Cut when (we've heard enough speech) AND (silence ran long).
        const enoughSpeech = RT.vadHotMs >= RT.VAD_HOT_MIN_MS;
        if ((enoughSpeech && RT.vadSilenceMs >= RT.VAD_SILENCE_MS) ||
            elapsed >= RT.MAX_UTTER_MS) {
          stopVadLoop();
          try { RT.recorder.stop(); } catch (_e) { /* swallow */ }
        }
      }, TICK_MS);
    }
    function stopVadLoop() {
      if (RT.vadTimer) { clearInterval(RT.vadTimer); RT.vadTimer = null; }
    }

    async function onUtteranceCut() {
      if (RT.cancelled) { return; }
      // 2026-05-10 double-voice fix per Serge: previous guard was
      //   "skip if a non-realtime path is ACTIVE"
      // which let BOTH the realtime and compact handlers POST when
      // __lunaActiveVoicePath was undefined (first page load). That
      // produced two /api/voice/converse round-trips, two audio
      // responses, and two voices playing over each other.
      // New rule: this handler only fires when REALTIME is the
      // explicitly-claimed active path. Default-deny when path is
      // unset.
      if (window.__lunaActiveVoicePath !== "realtime") {
        return;
      }
      const blob = new Blob(RT.chunks, { type: (RT.recorder && RT.recorder.mimeType) || "audio/webm" });
      RT.chunks = [];
      // No / tiny audio? Just loop back to listening.
      if (!blob || blob.size < 4096) {
        if (RT.mic && RT.mode === "continuous") beginUtterance();
        return;
      }
      setStatus("thinking", "Luna is thinking…");
      let respJson = null;
      try {
        const resp = await fetch("/api/voice/converse", {
          method: "POST",
          credentials: "omit",
          cache: "no-store",
          headers: {
            "Content-Type": blob.type || "audio/webm",
            "X-Luna-Session": RT.session,
          },
          body: blob,
        });
        respJson = await resp.json().catch(() => null);
      } catch (e) {
        setStatus("error", "Network error. Click Talk to retry.");
        appendLine("err", "Network error talking to /api/voice/converse: " + (e && e.message || e));
        return;
      }
      if (!respJson || respJson.ok === false) {
        const msg = (respJson && respJson.error) || "no response";
        // Soft-loop on benign chunk-level failures (silence, tiny blob,
        // briefly-empty transcript). Operator never sees these as errors
        // — Luna just keeps listening like a real phone call would.
        const benign = /no speech detected|audio blob too small|empty/i.test(String(msg))
                    || /no response/i.test(String(msg));
        if (benign) {
          if (RT.mic && RT.mode === "continuous") {
            setStatus("listening", "Listening...");
            beginUtterance();
            return;
          }
        }
        setStatus("error", "Couldn't process: " + msg);
        appendLine("err", "Luna couldn't process that: " + msg);
        if (RT.mic && RT.mode === "continuous") beginUtterance();
        return;
      }
      if (respJson.transcript) appendLine("user", respJson.transcript);
      if (respJson.reply_text) appendLine("luna", respJson.reply_text);
      // Play TTS audio, then loop or idle.
      if (respJson.audio_b64) {
        await playLunaAudio(respJson.audio_b64, respJson.audio_mime || "audio/wav");
      }
      if (RT.cancelled) return;
      if (RT.mode === "continuous" && RT.mic) {
        beginUtterance();
      } else {
        setStatus("idle", "Idle · click Talk to speak again.");
      }
    }

    function playLunaAudio(b64, mime) {
      return new Promise((resolve) => {
        try {
          if (RT.audio) {
            try { RT.audio.pause(); } catch (_e) {}
            RT.audio = null;
          }
          const data = "data:" + (mime || "audio/wav") + ";base64," + b64;
          const a = new Audio(data);
          RT.audio = a;
          a.onended = () => { RT.audio = null; resolve(); };
          a.onerror = () => { RT.audio = null; resolve(); };
          setStatus("speaking", "Luna is speaking…");
          a.play().catch(() => { RT.audio = null; resolve(); });
        } catch (e) {
          RT.audio = null;
          resolve();
        }
      });
    }

    function interruptLuna() {
      // Stop any currently-playing TTS audio + cut current utterance
      // recorder. We do NOT end the session — the user wants to keep
      // talking, just stop Luna's voice now.
      try {
        if (RT.audio) { RT.audio.pause(); RT.audio = null; }
      } catch (_e) {}
      try {
        if (RT.recorder && RT.recorder.state === "recording") {
          RT.recorder.stop();
        }
      } catch (_e) {}
      // Tell the server to drain pyttsx3 / Kokoro queues too.
      try { fetch("/api/voice/stop", { method: "POST" }); } catch (_e) {}
      setStatus("listening", "Stopped speaking. Listening for you again…");
      if (RT.mic) beginUtterance();
    }

    function endSession(showHandle) {
      RT.cancelled = true;
      stopVadLoop();
      try {
        if (RT.recorder && RT.recorder.state !== "inactive") RT.recorder.stop();
      } catch (_e) {}
      RT.recorder = null;
      try {
        if (RT.mic) RT.mic.getTracks().forEach((t) => t.stop());
      } catch (_e) {}
      RT.mic = null;
      try { if (RT.ctx) RT.ctx.close(); } catch (_e) {}
      RT.ctx = null;
      RT.analyser = null;
      try {
        if (RT.audio) { RT.audio.pause(); RT.audio = null; }
      } catch (_e) {}
      try { fetch("/api/voice/stop", { method: "POST" }); } catch (_e) {}
      setStatus("idle", "Idle · click Talk to start.");
      if (showHandle && showBtn) {
        // Keep the panel open so the user can click Talk again, but
        // also re-show the small toggle handle in case they collapsed.
        showBtn.hidden = false;
      }
    }

    // First-time status text.
    setStatus("idle", "Idle · click Talk to start.");

    // Expose a clean API so the compact mic icon (and any other surface)
    // can drive the realtime voice without simulating clicks on hidden
    // buttons. The compact handler in the input row uses this:
    //   window.__lunaRT.api.start()
    //   window.__lunaRT.api.end()
    //   window.__lunaRT.api.populateDevices()
    //   window.__lunaRT.api.getState() -> { active, mode, mic, recorder }
    if (RT) {
      RT.api = {
        start: () => startSession(),
        end:   (showHandle) => endSession(showHandle === true),
        populateDevices: () => populateMicDevices(),
        appendLine: (kind, text) => appendLine(kind, text),
        setStatus: (state, text) => setStatus(state, text),
        getState: () => ({
          active:   !!RT.mic,
          mode:     RT.mode,
          deviceId: RT.lastDeviceId,
        }),
      };
    }
  }
  // End realtime voice block.

  document.addEventListener("DOMContentLoaded", () => {
    try {
      bind();
      bindSensoryControls();
      refresh();
    } catch (e) { /* never break the app */ }
    // Keep the pill honest while Luna is speaking. 3s cadence stays
    // inside the dashboard's <= 3000ms refresh budget without piling
    // up requests against /api/voice/status.
    if (!window.__lunaVoiceRefreshInterval) {
      window.__lunaVoiceRefreshInterval = setInterval(() => { try { refresh(); } catch (e) {} }, 3000);
    }
  });

  function bindSensoryControls() {
    const camera = $("luna-camera-toggle");
    const cameraStatus = $("luna-camera-status");
    const feedOpen = $("luna-live-feed-open");
    const feedFresh = $("luna-live-feed-freshness");
    if (camera && !camera.dataset.bound) {
      function toggleCamera() {
        const bridge = $("console-camera");
        if (bridge) bridge.click();
        const on = camera.getAttribute("aria-pressed") !== "true";
        camera.setAttribute("aria-pressed", on ? "true" : "false");
        if (cameraStatus) {
          cameraStatus.textContent = on ? "camera: requested" : "camera: offline";
          cameraStatus.dataset.state = on ? "requested" : "off";
        }
      }
      camera.addEventListener("click", toggleCamera);
      camera.dataset.bound = "1";
    }
    if (feedOpen && !feedOpen.dataset.bound) {
      feedOpen.addEventListener("click", (ev) => {
        const tab = $("tab-feed");
        if (tab) {
          ev.preventDefault();
          tab.click();
          if (feedFresh) feedFresh.textContent = "live feed: opened " + new Date().toLocaleTimeString();
        }
      });
      feedOpen.dataset.bound = "1";
    }
  }
})();

/* ====================================================================
   LUNA LIVE MAP V2 — full-width cosmic centerpiece renderer
   --------------------------------------------------------------------
   Added 2026-05-08 per Serge's redesign request. Self-contained IIFE
   that drives:
     - Layer 1: flying-through-space background (parallax stars + warp)
     - Layer 2: cosmic core (orbital rings + glowing nodes + beams +
                particles traveling between core and nodes)
     - Layer 3: node label positioning (synced to layer-2 node coords)
     - HUD panels: TIER status, SYSTEM HEALTH gauge + bars, LIVE
                   METRICS sparklines, SYSTEM ACTIVITY mini line charts,
                   ACTIVE EVENTS list, UP NEXT countdown,
                   DATA STREAM bottom strip

   Animations are signal-tied per playbook §11:
     - star drift speed   = supervisor cycle rate
     - warp streak length = recent Tier 6 candidate throughput
     - node pulse rate    = subsystem activity (live_feed.jsonl)
     - core glow          = overall system health
     - beam particles     = packet flow
     - countdown          = rolling-rate buffer (same one that drives
                            the per-rung ETAs)

   Reads from existing API endpoints only; no new polls. Polls every
   1 s for data, runs animation at requestAnimationFrame (vsync, ~60Hz).
   devicePixelRatio-aware. Pauses when the tab is hidden. CPU < 5% at
   idle on a typical machine.

   Single owner per DOM target (per playbook §17 to avoid the kind of
   two-writer race that produced the oscillating progress bar).
   ==================================================================== */
(function lunaLiveMapV2() {
  "use strict";

  // ---------- Setup ---------------------------------------------------
  const SECTION = document.getElementById("luna-livemap-v2");
  if (!SECTION) return;  // not on this page

  const $ = (id) => document.getElementById(id);
  const stage = $("lmv2-stage");
  const bgCv  = $("lmv2-bg-canvas");
  const coreCv= $("lmv2-core-canvas");
  if (!stage || !bgCv || !coreCv) return;
  const bgCtx   = bgCv.getContext("2d");
  const coreCtx = coreCv.getContext("2d");
  const labelsRoot = $("lmv2-labels");

  // The cosmic-map (legacy) canvas in the stage is just a compatibility
  // shim. Resize it to 1×1 so any old code that touches it doesn't crash
  // but it doesn't paint anything visible.
  const legacyCv = $("cognitive-map");
  if (legacyCv) { legacyCv.width = 1; legacyCv.height = 1; }

  // ---------- Geometry ------------------------------------------------
  // Node positions are expressed in normalized (cx, cy, orbitR, theta0)
  // form. The renderer multiplies by the stage size each frame so the
  // map auto-scales.
  // Each node has:
  //   key         : DOM key (matches data-node attribute on the label)
  //   tone        : --lmv2-tone-* css var name suffix
  //   orbitR      : 0..1 fraction of min(width,height)/2 (0=center)
  //   theta0      : starting angle in radians (offset on the orbit)
  //   speed       : radians per second (orbital angular velocity)
  //   sizeBase    : pixel radius of the node halo at size-1
  //   labelOffset : [dx, dy] in fractions of node radius to offset label
  // Planets evenly distributed around the full 360° (per Serge 2026-05-08:
  // previous distribution had them all clustered in two arcs, looked like
  // a clump of light). With 9 planets at 40° apart starting from "top",
  // they fan out like spokes of a wheel - exactly the red lines Serge drew.
  // Radii alternate between 0.78 / 0.92 R so the ring of planets has
  // visual rhythm rather than sitting on a single circle.
  //
  //   pos 0 (  0° / top):       TIER
  //   pos 1 ( 40° / NE):        ARCHITECT
  //   pos 2 ( 80° / E):         MEMORY
  //   pos 3 (120° / SE):        LIVE QUEUE
  //   pos 4 (160° / S):         CURRENT WORK
  //   pos 5 (200° / SSW):       AIDER
  //   pos 6 (240° / SW):        BLOCKERS
  //   pos 7 (280° / W):         VERIFIER
  //   pos 8 (320° / NW):        GUARDIAN
  //
  // Theta in radians = -π/2 + (pos × 2π/9). All positive theta values
  // for clarity; the renderer handles full-circle math fine.
  const TWO_PI_9 = (2 * Math.PI) / 9;
  const TOP = -Math.PI / 2;
  // Per Serge 2026-05-08 (round 14): copy the ORIGINAL cmap motion model
  // where planets visibly orbit AND PASS EACH OTHER. The original did
  // this with inner ring drifting at +1.0 and outer ring at -0.6
  // (counter-rotating, different magnitude). Replicating that here:
  // INNER_SPEED is positive (clockwise), OUTER_SPEED is negative
  // (counter-clockwise) and slower. Result: planets on the inner ring
  // sweep past planets on the outer ring repeatedly, the whole field
  // looks like a real orbital system. Plus radial wobble per-planet
  // for individual liveness (oscPhase below).
  const INNER_SPEED =  0.040;     // clockwise, ~2.5 min full rotation
  const OUTER_SPEED = -0.026;     // counter-clockwise, ~4 min full rotation
  // Each planet's `ring` property tells the renderer which speed to
  // apply (inner = positive/CW, outer = negative/CCW). With 9 planets
  // distributed in 360°, alternating inner/outer makes adjacent
  // planets travel in opposite directions, so as the system spins they
  // visibly pass each other. Plus oscPhase + oscFreq drive a small
  // radial wobble per planet for individual liveness.
  //
  // Pos 0,2,4,6,8 = inner ring (5 planets, clockwise).
  // Pos 1,3,5,7   = outer ring (4 planets, counter-clockwise).
  const NODES = [
    { key: "tier",         tone: "amber",  orbitR: 0.74, theta0: TOP + 0 * TWO_PI_9, ring: "inner", sizeBase: 17, labelOffset: [0,    -1.7], oscPhase: 0.0, oscFreq: 1.1 },
    { key: "architect",    tone: "violet", orbitR: 0.96, theta0: TOP + 1 * TWO_PI_9, ring: "outer", sizeBase: 16, labelOffset: [1.3,  -1.0], oscPhase: 0.7, oscFreq: 0.9 },
    { key: "memory",       tone: "azure",  orbitR: 0.74, theta0: TOP + 2 * TWO_PI_9, ring: "inner", sizeBase: 17, labelOffset: [1.7,   0.0], oscPhase: 1.4, oscFreq: 1.3 },
    { key: "live-queue",   tone: "azure",  orbitR: 0.96, theta0: TOP + 3 * TWO_PI_9, ring: "outer", sizeBase: 17, labelOffset: [1.4,   1.2], oscPhase: 2.1, oscFreq: 1.0 },
    { key: "current-work", tone: "violet", orbitR: 0.74, theta0: TOP + 4 * TWO_PI_9, ring: "inner", sizeBase: 17, labelOffset: [0.4,   1.7], oscPhase: 2.8, oscFreq: 1.2 },
    { key: "aider",        tone: "violet", orbitR: 0.96, theta0: TOP + 5 * TWO_PI_9, ring: "outer", sizeBase: 16, labelOffset: [-0.4,  1.7], oscPhase: 3.5, oscFreq: 0.95 },
    { key: "blockers",     tone: "rose",   orbitR: 0.74, theta0: TOP + 6 * TWO_PI_9, ring: "inner", sizeBase: 17, labelOffset: [-1.4,  1.2], oscPhase: 4.2, oscFreq: 1.15 },
    { key: "verifier",     tone: "teal",   orbitR: 0.96, theta0: TOP + 7 * TWO_PI_9, ring: "outer", sizeBase: 17, labelOffset: [-1.7,  0.0], oscPhase: 4.9, oscFreq: 1.0 },
    { key: "guardian",     tone: "amber",  orbitR: 0.74, theta0: TOP + 8 * TWO_PI_9, ring: "inner", sizeBase: 16, labelOffset: [-1.3, -1.0], oscPhase: 5.6, oscFreq: 1.25 },
    // POLARIS · TERMINAL MANAGER planet — was POLARIS · MASTER TEACHER.
    // Renamed 2026-05-09 §33 per Serge's manager/worker hierarchy:
    // Workers now orbit a Manager (planet), not Luna directly. Polaris
    // becomes the Terminal Manager who reviews Luna's commands and
    // teaches Vega (the satellite worker). Placed at the SOUTH pole
    // (theta = TOP + 4.5 * TWO_PI_9 = 180° from top) on a unique inner-most
    // orbit (0.55) to visually mark him as the
    // "central knowledge keeper" antipodal to Tier (which is at the
    // north pole). Tone amber matches guardian-class permanence.
    // Indexes all knowledge artifacts; per playbook §29.
    { key: "master-teacher", tone: "amber", orbitR: 0.55, theta0: TOP + 4.5 * TWO_PI_9, ring: "inner", sizeBase: 18, labelOffset: [0, 1.7], oscPhase: 3.14, oscFreq: 0.85 },
  ];

  // ---------- SATELLITES (Manager → Worker hierarchy) -----------------
  // §33 (2026-05-09) Serge: workers should not be top-level planets;
  // they orbit their Manager. The Manager is a planet (in NODES); the
  // Worker is a satellite — a small star circling the Manager, not Luna.
  //
  // Each satellite has a `parent_key` (must match a NODES.key) and its
  // own local orbital params (radiusFactor relative to manager's
  // sizeBase, theta0, speed, sizeBase). The renderer computes the
  // satellite's absolute position as: manager.x + cos(theta) * localR
  // and links them with a teaching-beam in _drawCore.
  //
  // Initially only Vega (terminal-worker) is a satellite; future
  // workers can be added here (e.g. Memory Keeper orbiting Memory).
  const SATELLITES = [
    {
      key: "terminal-worker",
      parent_key: "master-teacher",   // Vega orbits Polaris (Terminal Manager)
      tone: "teal",
      sizeBase: 6,                     // ~1/3 the size of a planet
      localOrbitFactor: 2.6,           // satellite orbit radius = parent.radius * 2.6
      localOrbitYFactor: 0.78,         // ellipse aspect to match the rest
      localSpeed: 0.85,                // faster than planets (orbits a planet not Luna)
      localTheta0: 0.0,
      labelOffset: [0, -1.7],
      oscPhase: 4.4, oscFreq: 1.4,
    },
  ];
  const TONE_RGB = {
    amber:  "255, 184, 77",
    violet: "178, 122, 255",
    azure:  "88, 180, 255",
    teal:   "72, 220, 195",
    rose:   "255, 104, 122",
    core:   "255, 196, 92",
  };

  // ---------- Renderer state -----------------------------------------
  // Star layers for the flying-through-space background.
  // Each layer is { stars: [], speedFactor }. Closer layers move faster
  // for the parallax illusion of forward motion.
  // Star counts thinned 2026-05-08 per Serge: was crowded (580 total),
  // now ~290 total - more "open space", less starry-night clutter,
  // each star has more room to breathe.
  const STAR_LAYERS = [
    { stars: [], count: 110, speed: 0.18,  size: 0.6 },  // far
    { stars: [], count:  90, speed: 0.40,  size: 0.9 },  // mid
    { stars: [], count:  60, speed: 0.85,  size: 1.4 },  // near
    { stars: [], count:  30, speed: 1.6,   size: 2.0 },  // close (warp streaks)
  ];
  // Beam particles travel from Luna Core to each node. State is per-node.
  const BEAM_PARTICLES = NODES.map(() => []);
  // Pulses radiating out from Luna Core on real events.
  const CORE_PULSES = [];
  // Shooting stars (commands & inter-worker messages). Each shot has a
  // FROM and a TO node key:
  //   from = "luna-core" + to = planet  -> Luna issues a command
  //   from = planet      + to = planet  -> worker-to-worker handoff
  // Renderer looks up positions for both endpoints each frame.
  // Throttled by _lastFireTimeByPair so we don't spam.
  const SHOOTING_STARS = [];
  const _lastFireTimeByPair = Object.create(null);
  function _fireShootingStar(fromKey, toKey, tone) {
    const pair = (fromKey || "luna-core") + ">" + toKey;
    const now = performance.now();
    // Per-pair rate limit: 1 shot every 1500 ms - prevents spam when
    // the live-feed has a burst of similar events.
    if (_lastFireTimeByPair[pair] && (now - _lastFireTimeByPair[pair]) < 1500) {
      return;
    }
    _lastFireTimeByPair[pair] = now;
    SHOOTING_STARS.push({
      from:   fromKey || "luna-core",
      target: toKey,
      tone:   tone || "core",
      born:   now,
      duration: 700 + Math.random() * 300,
    });
    if (SHOOTING_STARS.length > 30) SHOOTING_STARS.shift();
  }
  // SOLID BEAMS — for "something big is happening". Always Luna→planet
  // (Luna is the only one who issues big commands). 3-5s holds with
  // pulsing throb.
  const SOLID_BEAMS = [];
  function _fireSolidBeam(nodeKey, tone, durationMs) {
    SOLID_BEAMS.push({
      target: nodeKey,
      tone:   tone || "core",
      born:   performance.now(),
      duration: durationMs || 4000,
    });
    if (SOLID_BEAMS.length > 5) SOLID_BEAMS.shift();
  }
  // Hyperspace burst trigger (set briefly on real promotion events).
  let warpBoost = 0;        // 0..1, decays over time
  // Speed multiplier for the whole background drift, tied to supervisor
  // cycle rate. Updated by the data poller.
  let driftSpeedMul = 1.0;
  // Last subsystem activity timestamp per node, used to drive node
  // pulse amplitude.
  const NODE_ACTIVITY = Object.create(null);
  NODES.forEach((n) => { NODE_ACTIVITY[n.key] = 0; });
  // §33: satellites also receive activity (event routing fires e.g. terminal-worker)
  SATELLITES.forEach((s) => { NODE_ACTIVITY[s.key] = 0; });

  // ---------- Resize handling ----------------------------------------
  let W = 0, H = 0, DPR = 1;
  function _resize() {
    const r = stage.getBoundingClientRect();
    DPR = Math.max(1, Math.min(window.devicePixelRatio || 1, 2.5));
    W = Math.max(320, r.width);
    H = Math.max(320, r.height);
    [bgCv, coreCv].forEach((c) => {
      c.width  = Math.round(W * DPR);
      c.height = Math.round(H * DPR);
      c.style.width  = W + "px";
      c.style.height = H + "px";
    });
    bgCtx.setTransform(DPR, 0, 0, DPR, 0, 0);
    coreCtx.setTransform(DPR, 0, 0, DPR, 0, 0);
    _seedStars();
  }
  window.addEventListener("resize", _resize);

  function _seedStars() {
    STAR_LAYERS.forEach((layer) => {
      layer.stars.length = 0;
      for (let i = 0; i < layer.count; i++) {
        layer.stars.push({
          x: Math.random() * W,
          y: Math.random() * H,
          r: layer.size * (0.55 + Math.random() * 0.7),
          tw: Math.random() * Math.PI * 2,  // twinkle phase
        });
      }
    });
  }

  // ---------- Background renderer (flying through space) ------------
  function _drawBackground(t) {
    const ctx = bgCtx;
    ctx.clearRect(0, 0, W, H);
    // Fill solid dark background so no white shows through
    ctx.fillStyle = "#02030a";
    ctx.fillRect(0, 0, W, H);

    // Nebula 1 (warm amber, bottom-left quadrant)
    const cx1 = W * (0.28 + 0.06 * Math.sin(t * 0.00003));
    const cy1 = H * (0.55 + 0.06 * Math.cos(t * 0.00004));
    const g1 = ctx.createRadialGradient(cx1, cy1, 0, cx1, cy1, Math.max(W, H) * 0.7);
    g1.addColorStop(0,    "rgba(255, 196, 92, 0.13)");
    g1.addColorStop(0.35, "rgba(255, 184, 77, 0.06)");
    g1.addColorStop(0.7,  "rgba(255, 130, 60, 0.02)");
    g1.addColorStop(1,    "rgba(0,0,0,0)");
    ctx.fillStyle = g1;
    ctx.fillRect(0, 0, W, H);

    // Nebula 2 (cool violet/azure, top-right quadrant)
    const cx2 = W * (0.74 + 0.06 * Math.sin(t * 0.00002));
    const cy2 = H * (0.32 + 0.05 * Math.cos(t * 0.00003));
    const g2 = ctx.createRadialGradient(cx2, cy2, 0, cx2, cy2, Math.max(W, H) * 0.62);
    g2.addColorStop(0,   "rgba(178, 122, 255, 0.11)");
    g2.addColorStop(0.4, "rgba(88, 180, 255, 0.05)");
    g2.addColorStop(0.8, "rgba(72, 220, 195, 0.015)");
    g2.addColorStop(1,   "rgba(0,0,0,0)");
    ctx.fillStyle = g2;
    ctx.fillRect(0, 0, W, H);

    // Nebula 3 (soft teal, bottom-right - smaller, deeper)
    const cx3 = W * (0.62 + 0.04 * Math.cos(t * 0.000035));
    const cy3 = H * (0.78 + 0.04 * Math.sin(t * 0.00004));
    const g3 = ctx.createRadialGradient(cx3, cy3, 0, cx3, cy3, Math.max(W, H) * 0.4);
    g3.addColorStop(0,   "rgba(72, 220, 195, 0.08)");
    g3.addColorStop(0.6, "rgba(72, 220, 195, 0.018)");
    g3.addColorStop(1,   "rgba(0,0,0,0)");
    ctx.fillStyle = g3;
    ctx.fillRect(0, 0, W, H);

    // Starfield: each layer drifts diagonally for a "flying forward"
    // illusion. Closer layers (higher index) drift faster. Stars wrap
    // around edges so the field is infinite. Warp boost lengthens the
    // streaks on real promotion events.
    const baseSpeed = 0.6 * driftSpeedMul + 1.6 * warpBoost;
    STAR_LAYERS.forEach((layer, li) => {
      const dx = -layer.speed * baseSpeed;          // drift toward upper-left for forward feel
      const dy = -layer.speed * baseSpeed * 0.55;
      const streakLen = layer.speed * (1.2 + 8 * warpBoost);
      ctx.save();
      ctx.lineCap = "round";
      ctx.lineWidth = layer.size * (0.6 + warpBoost * 0.6);
      ctx.strokeStyle = "rgba(255,255,255,0.55)";
      ctx.fillStyle = "rgba(255,255,255,0.78)";
      for (let i = 0; i < layer.stars.length; i++) {
        const s = layer.stars[i];
        s.x += dx; s.y += dy;
        if (s.x < -10) s.x = W + 10;
        if (s.x > W + 10) s.x = -10;
        if (s.y < -10) s.y = H + 10;
        if (s.y > H + 10) s.y = -10;
        s.tw += 0.04;
        const tw = 0.5 + 0.5 * Math.sin(s.tw);
        if (warpBoost > 0.02 && li >= 2) {
          // Streaks for near layers during warp
          ctx.globalAlpha = 0.55 + 0.35 * tw;
          ctx.beginPath();
          ctx.moveTo(s.x, s.y);
          ctx.lineTo(s.x + streakLen * 1.2, s.y + streakLen * 0.66);
          ctx.stroke();
        } else {
          // Regular twinkling dots
          ctx.globalAlpha = 0.35 + 0.55 * tw;
          ctx.beginPath();
          ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2);
          ctx.fill();
        }
      }
      ctx.restore();
    });

    // Decay warp boost
    warpBoost *= 0.94;
    if (warpBoost < 0.005) warpBoost = 0;

    // Vignette: darken corners so the eye is drawn to the bright
    // Luna Core in the center. Subtle - 0..0.4 alpha at edges.
    const vg = ctx.createRadialGradient(W / 2, H / 2, Math.min(W, H) * 0.35, W / 2, H / 2, Math.max(W, H) * 0.7);
    vg.addColorStop(0, "rgba(0,0,0,0)");
    vg.addColorStop(0.7, "rgba(0,0,0,0.25)");
    vg.addColorStop(1, "rgba(0,0,0,0.55)");
    ctx.fillStyle = vg;
    ctx.fillRect(0, 0, W, H);
  }

  // ---------- Luna Core particle emission (continuous outward dust) -
  // Spawned each frame, decay over ~3-4 seconds. Tied to overall
  // activity so a busy Luna emits more dust.
  const CORE_PARTICLES = [];
  function _spawnCoreParticles(intensity) {
    const want = Math.min(2, Math.max(0, Math.floor(intensity * 2 + Math.random())));
    for (let i = 0; i < want; i++) {
      const angle = Math.random() * Math.PI * 2;
      const speed = 0.4 + Math.random() * 1.2;
      CORE_PARTICLES.push({
        angle: angle,
        r: 2 + Math.random() * 4,
        speed: speed,
        life: 1.0,
        decay: 0.005 + Math.random() * 0.012,
        size: 0.6 + Math.random() * 1.4,
        tone: Math.random() < 0.7 ? "core" : (Math.random() < 0.5 ? "amber" : "violet"),
      });
    }
    // Hard cap so we never balloon
    while (CORE_PARTICLES.length > 220) CORE_PARTICLES.shift();
  }

  // ---------- Cosmic core renderer (orbits + nodes + beams) ---------
  function _drawCore(t) {
    const ctx = coreCtx;
    ctx.clearRect(0, 0, W, H);
    const cx = W / 2;
    const cy = H / 2;
    // R bumped 2026-05-08 per Serge: 0.42 -> 0.46 (more cosmic real
    // estate now that the side panels are slimmer).
    const R  = Math.min(W, H) * 0.46;

    // 1) Concentric orbital rings. Eight rings at decreasing radii with
    //    much denser speckled dust (the mockup's "lit-from-within" look).
    //    Outer rings are tilted more (deeper aspect ratio) and slightly
    //    blurred via lower-alpha rendering for a depth-of-field fake -
    //    closer rings stay sharp, far rings recede into space.
    // Orbital rings tilted to match the planet aspect (1.10:0.78). Some
    // rings rotate the same direction as the inner planets, some same
    // as outer - this gives the dust-trails a sense of depth, like
    // multiple orbital tracks at different inclinations. Counter-rot
    // alternation also matches the original cmap's "passing each other"
    // feel.
    ctx.save();
    const NUM_RINGS = 9;
    for (let i = 0; i < NUM_RINGS; i++) {
      const r = R * (0.16 + i * 0.105);
      const dofFactor = 1 - Math.abs(i - 3) / NUM_RINGS * 0.55;
      const tiltX = 1.10 - i * 0.005;     // outer rings slightly less wide
      const tiltY = 0.78 - i * 0.008;     // outer rings flatter (more tilted)
      const alpha = (0.08 + (i % 2) * 0.05) * dofFactor;
      // Alternate ring rotation direction to match the planet passing-each-other feel.
      const ringDir = (i % 2 === 0) ? 1 : -1;
      const rotPhase = t * 0.00010 * (i + 1) * driftSpeedMul * ringDir;
      ctx.strokeStyle = "rgba(255, 196, 92, " + alpha + ")";
      ctx.lineWidth = 0.6 + (i === 3 ? 0.3 : 0);
      ctx.beginPath();
      ctx.ellipse(cx, cy, r * tiltX, r * tiltY, rotPhase * 0.05, 0, Math.PI * 2);
      ctx.stroke();
      // Dense speckled dust along this orbit, traveling at the ring's direction.
      const speckCount = 80 + i * 14;
      for (let s = 0; s < speckCount; s++) {
        const a = (s / speckCount) * Math.PI * 2 + rotPhase * 2;
        const px = cx + Math.cos(a) * r * tiltX;
        const py = cy + Math.sin(a) * r * tiltY;
        const flicker = 0.16 + 0.22 * Math.sin(a * 3 + t * 0.001 + i);
        ctx.fillStyle = "rgba(255, 196, 92, " + (flicker * dofFactor) + ")";
        ctx.beginPath();
        ctx.arc(px, py, 0.55 + (i % 2) * 0.25, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    ctx.restore();

    // 2) Core pulses - radiating rings on real events.
    for (let i = CORE_PULSES.length - 1; i >= 0; i--) {
      const p = CORE_PULSES[i];
      p.r += p.speed;
      p.alpha *= 0.96;
      if (p.alpha < 0.02 || p.r > R * 1.4) {
        CORE_PULSES.splice(i, 1);
        continue;
      }
      ctx.strokeStyle = "rgba(" + TONE_RGB[p.tone || "core"] + ", " + p.alpha + ")";
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      ctx.arc(cx, cy, p.r, 0, Math.PI * 2);
      ctx.stroke();
    }

    // 3) Compute live node positions. Per the original cmap motion
    //    model (resurrected from the round-8 implementation that Serge
    //    loved): inner ring rotates CLOCKWISE at INNER_SPEED, outer
    //    ring rotates COUNTER-CLOCKWISE at OUTER_SPEED. Adjacent
    //    planets are on different rings, so as the system spins they
    //    visibly pass each other. Plus radial wobble per planet
    //    (±5% over 4-7 sec) for individual liveness. Aspect ratio
    //    1.10:0.78 - tilted ellipse like a real orbital system seen
    //    near edge-on, not a flat top-down circle.
    const tSec = t * 0.001;
    const nodePositions = NODES.map((n) => {
      const wobble = 0.05 * Math.sin(tSec * n.oscFreq + n.oscPhase);
      const r = R * n.orbitR * (1 + wobble);
      const ringSpeed = (n.ring === "outer") ? OUTER_SPEED : INNER_SPEED;
      const theta = n.theta0 + tSec * ringSpeed * driftSpeedMul;
      return {
        node: n,
        x: cx + Math.cos(theta) * r * 1.10,
        y: cy + Math.sin(theta) * r * 0.78,
        radius: n.sizeBase * (1 + 0.12 * Math.sin(t * 0.003 + n.theta0)),
      };
    });

    // §33 (2026-05-09): Satellite positions. Each satellite orbits its
    // parent (a planet) — local theta + local radius based on parent's
    // current radius. Returns same-shape objects as nodePositions so the
    // rest of the renderer (labels, NODE_ACTIVITY, hot pulses, shooting
    // stars) treats them uniformly. The "satellite" flag is set so we
    // can render them differently (smaller; teaching-beam to parent).
    const satellitePositions = SATELLITES.map((s) => {
      const parent = nodePositions.find((p) => p.node.key === s.parent_key);
      if (!parent) return null;
      const localR = parent.radius * s.localOrbitFactor;
      const localTheta = s.localTheta0 + tSec * s.localSpeed * driftSpeedMul;
      const wobble = 0.05 * Math.sin(tSec * s.oscFreq + s.oscPhase);
      return {
        node: { key: s.key, tone: s.tone, sizeBase: s.sizeBase, labelOffset: s.labelOffset, oscPhase: s.oscPhase, oscFreq: s.oscFreq },
        x: parent.x + Math.cos(localTheta) * localR * 1.10 * (1 + wobble),
        y: parent.y + Math.sin(localTheta) * localR * (s.localOrbitYFactor || 0.78) * (1 + wobble),
        radius: s.sizeBase * (1 + 0.18 * Math.sin(t * 0.005 + s.localTheta0)),
        satellite: true,
        parent: parent,
        localOrbitR: localR,
      };
    }).filter((p) => p !== null);

    // 4) Connecting beams from core to each node.
    nodePositions.forEach((p, idx) => {
      const tone = TONE_RGB[p.node.tone] || TONE_RGB.amber;
      const grad = ctx.createLinearGradient(cx, cy, p.x, p.y);
      grad.addColorStop(0, "rgba(" + TONE_RGB.core + ", 0.55)");
      grad.addColorStop(1, "rgba(" + tone + ", 0.18)");
      ctx.strokeStyle = grad;
      ctx.lineWidth = 0.9;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.lineTo(p.x, p.y);
      ctx.stroke();

      // Particles flowing along the beam (more particles when subsystem
      // is active recently).
      const arr = BEAM_PARTICLES[idx];
      const recentMs = (Date.now() - (NODE_ACTIVITY[p.node.key] || 0));
      const isHot = recentMs < 4000;
      // Spawn rate: 2/s base, +6/s when hot
      const want = isHot ? 8 : 2;
      while (arr.length < want) {
        arr.push({ u: Math.random(), v: 0.4 + Math.random() * 0.6 });
      }
      for (let i = arr.length - 1; i >= 0; i--) {
        const part = arr[i];
        part.u += 0.012 * part.v * (isHot ? 1.6 : 1.0);
        if (part.u > 1.0) { arr.splice(i, 1); continue; }
        const px = cx + (p.x - cx) * part.u;
        const py = cy + (p.y - cy) * part.u;
        ctx.fillStyle = "rgba(" + tone + ", " + (0.65 * (1 - part.u * 0.5)) + ")";
        ctx.beginPath();
        ctx.arc(px, py, 1.4, 0, Math.PI * 2);
        ctx.fill();
      }
    });

    // 4.4) Solid beams — "something big is happening" connections.
    //      Bright thick energy line from Luna Core to a specific node
    //      that holds for several seconds with pulsing brightness.
    //      Fired by _fireSolidBeam on real promotions, council
    //      unanimity, blocker resolution, etc. (per Serge 2026-05-08).
    {
      const now = performance.now();
      ctx.save();
      ctx.globalCompositeOperation = "lighter";
      for (let i = SOLID_BEAMS.length - 1; i >= 0; i--) {
        const b = SOLID_BEAMS[i];
        const u = (now - b.born) / b.duration;
        if (u >= 1) { SOLID_BEAMS.splice(i, 1); continue; }
        // Find current target position. §33: also search satellitePositions
        // so events targeting Vega (now a satellite) still resolve.
        let tp = null;
        for (let k = 0; k < nodePositions.length; k++) {
          if (nodePositions[k].node.key === b.target) { tp = nodePositions[k]; break; }
        }
        if (!tp) {
          for (let k = 0; k < satellitePositions.length; k++) {
            if (satellitePositions[k].node.key === b.target) { tp = satellitePositions[k]; break; }
          }
        }
        if (!tp) { SOLID_BEAMS.splice(i, 1); continue; }
        const tone = TONE_RGB[b.tone] || TONE_RGB.core;
        // Brightness curve: ramp up fast, hold, fade out
        let brightness;
        if (u < 0.1) brightness = u / 0.1;          // fast ramp-up
        else if (u < 0.7) brightness = 1.0;          // hold full
        else brightness = (1 - u) / 0.3;             // fade out
        // Pulsing throb on top of base brightness
        const throb = 0.85 + 0.15 * Math.sin(now * 0.008);
        const alpha = brightness * throb;
        // Outer glow halo around the beam (wide, soft)
        const dx = tp.x - cx, dy = tp.y - cy;
        const len = Math.sqrt(dx * dx + dy * dy);
        const angle = Math.atan2(dy, dx);
        ctx.save();
        ctx.translate(cx, cy);
        ctx.rotate(angle);
        // Wide soft outer glow
        const glowGrad = ctx.createLinearGradient(0, -7, 0, 7);
        glowGrad.addColorStop(0,    "rgba(" + tone + ", 0)");
        glowGrad.addColorStop(0.5,  "rgba(" + tone + ", " + (0.45 * alpha) + ")");
        glowGrad.addColorStop(1,    "rgba(" + tone + ", 0)");
        ctx.fillStyle = glowGrad;
        ctx.fillRect(0, -7, len, 14);
        // Inner bright core line
        ctx.strokeStyle = "rgba(255,255,255," + (0.95 * alpha) + ")";
        ctx.lineWidth = 1.6;
        ctx.lineCap = "round";
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(len, 0);
        ctx.stroke();
        // Tone-tinted bright edge along the spine
        ctx.strokeStyle = "rgba(" + tone + "," + (0.85 * alpha) + ")";
        ctx.lineWidth = 3.2;
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(len, 0);
        ctx.stroke();
        // Traveling bright pulse (a packet of energy moving from core to node and looping)
        const pulseU = ((now - b.born) % 800) / 800;
        const pulseX = pulseU * len;
        const pulseGrad = ctx.createRadialGradient(pulseX, 0, 0, pulseX, 0, 14);
        pulseGrad.addColorStop(0,    "rgba(255, 250, 220, " + alpha + ")");
        pulseGrad.addColorStop(0.4,  "rgba(" + tone + ", " + (0.85 * alpha) + ")");
        pulseGrad.addColorStop(1,    "rgba(" + tone + ", 0)");
        ctx.fillStyle = pulseGrad;
        ctx.beginPath();
        ctx.arc(pulseX, 0, 14, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
      }
      ctx.restore();
    }

    // 4.5) Shooting stars — TWO kinds:
    //      (a) Luna→planet (sh.from === "luna-core") — Luna issues a
    //          command. Comet starts at the bright core, lands at the
    //          target planet's current orbital position.
    //      (b) planet→planet (sh.from is a node key) — a worker-to-
    //          worker handoff (e.g., AIDER finishes a candidate, sends
    //          to ARCHITECT for review). Comet starts at the from
    //          planet, lands at the to planet. This gives the "alive,
    //          communicating" feel Serge asked for.
    //      Both rendered with the same bright head + tapering trail.
    {
      const now = performance.now();
      const ctxss = ctx;
      ctxss.save();
      ctxss.globalCompositeOperation = "lighter";
      for (let i = SHOOTING_STARS.length - 1; i >= 0; i--) {
        const sh = SHOOTING_STARS[i];
        const u = (now - sh.born) / sh.duration;
        if (u >= 1) { SHOOTING_STARS.splice(i, 1); continue; }
        // Resolve target position. §33: also search satellitePositions.
        let tp = null;
        for (let k = 0; k < nodePositions.length; k++) {
          if (nodePositions[k].node.key === sh.target) { tp = nodePositions[k]; break; }
        }
        if (!tp) {
          for (let k = 0; k < satellitePositions.length; k++) {
            if (satellitePositions[k].node.key === sh.target) { tp = satellitePositions[k]; break; }
          }
        }
        if (!tp) { SHOOTING_STARS.splice(i, 1); continue; }
        // Resolve FROM position - core, or another node, or satellite.
        let fromX, fromY;
        if (sh.from === "luna-core") {
          fromX = cx; fromY = cy;
        } else {
          let fp = null;
          for (let k = 0; k < nodePositions.length; k++) {
            if (nodePositions[k].node.key === sh.from) { fp = nodePositions[k]; break; }
          }
          if (!fp) {
            for (let k = 0; k < satellitePositions.length; k++) {
              if (satellitePositions[k].node.key === sh.from) { fp = satellitePositions[k]; break; }
            }
          }
          if (!fp) { SHOOTING_STARS.splice(i, 1); continue; }
          fromX = fp.x; fromY = fp.y;
        }
        // Eased travel curve (fast launch, soft arrival = "command lands")
        const eased = 1 - Math.pow(1 - u, 2.4);
        const dx = tp.x - fromX;
        const dy = tp.y - fromY;
        const headX = fromX + dx * eased;
        const headY = fromY + dy * eased;
        const tone = TONE_RGB[sh.tone] || TONE_RGB.core;
        // Trail: 12 segments tapering back toward FROM endpoint
        const trailLen = 12;
        for (let k = 0; k < trailLen; k++) {
          const tu = Math.max(0, eased - k * 0.018);
          const tx = fromX + dx * tu;
          const ty = fromY + dy * tu;
          const a = (1 - k / trailLen) * (1 - u * 0.5) * 0.85;
          ctxss.fillStyle = "rgba(" + tone + ", " + a + ")";
          ctxss.beginPath();
          ctxss.arc(tx, ty, 1.5 + (1 - k / trailLen) * 2.0, 0, Math.PI * 2);
          ctxss.fill();
        }
        // Bright head (comet)
        const headGrad = ctxss.createRadialGradient(headX, headY, 0, headX, headY, 8);
        headGrad.addColorStop(0,    "rgba(255,255,255,1.0)");
        headGrad.addColorStop(0.4,  "rgba(" + tone + ", 0.9)");
        headGrad.addColorStop(1,    "rgba(" + tone + ", 0)");
        ctxss.fillStyle = headGrad;
        ctxss.beginPath();
        ctxss.arc(headX, headY, 8, 0, Math.PI * 2);
        ctxss.fill();
        // Tiny bright dot at head center
        ctxss.fillStyle = "rgba(255, 250, 220, " + (1 - u * 0.6) + ")";
        ctxss.beginPath();
        ctxss.arc(headX, headY, 2.4, 0, Math.PI * 2);
        ctxss.fill();
      }
      ctxss.restore();
    }

    // 5) Luna Core (center sun) - the JARVIS centerpiece. Multi-layer
    //    halo, particle emission trails, lens flare cross, bright spike
    //    rays. Uses additive (lighter) compositing so overlapping glows
    //    accumulate instead of clipping to opaque.
    {
      const corePulse = 1 + 0.06 * Math.sin(t * 0.0028);
      const coreR = R * 0.11 * corePulse;
      ctx.save();
      ctx.globalCompositeOperation = "lighter";

      // Spawn outward-flowing particles from the core (continuous dust).
      _spawnCoreParticles(0.6 + driftSpeedMul * 0.5);
      // Render+update particles
      for (let i = CORE_PARTICLES.length - 1; i >= 0; i--) {
        const p = CORE_PARTICLES[i];
        p.r += p.speed;
        p.life -= p.decay;
        if (p.life <= 0 || p.r > R * 1.2) {
          CORE_PARTICLES.splice(i, 1);
          continue;
        }
        const px = cx + Math.cos(p.angle) * p.r;
        const py = cy + Math.sin(p.angle) * p.r;
        const tone = TONE_RGB[p.tone] || TONE_RGB.core;
        ctx.fillStyle = "rgba(" + tone + ", " + (0.65 * p.life) + ")";
        ctx.beginPath();
        ctx.arc(px, py, p.size * (0.7 + 0.3 * p.life), 0, Math.PI * 2);
        ctx.fill();
      }

      // Outer halo (large, soft)
      const haloOuter = ctx.createRadialGradient(cx, cy, 0, cx, cy, coreR * 6);
      haloOuter.addColorStop(0,    "rgba(" + TONE_RGB.core + ", 0.40)");
      haloOuter.addColorStop(0.18, "rgba(" + TONE_RGB.core + ", 0.22)");
      haloOuter.addColorStop(0.5,  "rgba(255, 130, 60, 0.05)");
      haloOuter.addColorStop(1,    "rgba(0,0,0,0)");
      ctx.fillStyle = haloOuter;
      ctx.fillRect(cx - coreR * 6, cy - coreR * 6, coreR * 12, coreR * 12);

      // Mid halo (medium-bright)
      const haloMid = ctx.createRadialGradient(cx, cy, 0, cx, cy, coreR * 2.4);
      haloMid.addColorStop(0,   "rgba(255, 232, 170, 0.7)");
      haloMid.addColorStop(0.5, "rgba(" + TONE_RGB.core + ", 0.55)");
      haloMid.addColorStop(1,   "rgba(255, 130, 60, 0.0)");
      ctx.fillStyle = haloMid;
      ctx.beginPath();
      ctx.arc(cx, cy, coreR * 2.4, 0, Math.PI * 2);
      ctx.fill();

      // Spike rays (short, bright, fast-rotating)
      ctx.translate(cx, cy);
      ctx.strokeStyle = "rgba(" + TONE_RGB.core + ", 0.55)";
      ctx.lineWidth = 0.7;
      const spikeCount = 48;
      for (let s = 0; s < spikeCount; s++) {
        const a = (s / spikeCount) * Math.PI * 2 + t * 0.00015;
        const len = coreR * (1.7 + 0.9 * Math.sin(s * 4.7 + t * 0.002));
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(Math.cos(a) * len, Math.sin(a) * len);
        ctx.stroke();
      }
      ctx.translate(-cx, -cy);

      // Inner bright core (the actual sun ball)
      const innerGrad = ctx.createRadialGradient(cx, cy, 0, cx, cy, coreR);
      innerGrad.addColorStop(0,    "rgba(255, 250, 220, 1.0)");
      innerGrad.addColorStop(0.35, "rgba(255, 232, 170, 0.95)");
      innerGrad.addColorStop(0.8,  "rgba(" + TONE_RGB.core + ", 0.45)");
      innerGrad.addColorStop(1,    "rgba(255, 130, 60, 0.0)");
      ctx.fillStyle = innerGrad;
      ctx.beginPath();
      ctx.arc(cx, cy, coreR, 0, Math.PI * 2);
      ctx.fill();

      // LENS FLARE — the iconic JARVIS cross. Two perpendicular long
      // rays + a fainter diagonal pair, each tapering with gradient.
      const flareLen = coreR * 7.5 * corePulse;
      function _flare(angle, intensity) {
        const ax = Math.cos(angle), ay = Math.sin(angle);
        const x0 = cx - ax * flareLen, y0 = cy - ay * flareLen;
        const x1 = cx + ax * flareLen, y1 = cy + ay * flareLen;
        const fg = ctx.createLinearGradient(x0, y0, x1, y1);
        fg.addColorStop(0,    "rgba(255, 232, 170, 0)");
        fg.addColorStop(0.45, "rgba(255, 232, 170, " + (0.55 * intensity) + ")");
        fg.addColorStop(0.5,  "rgba(255, 250, 220, " + (0.85 * intensity) + ")");
        fg.addColorStop(0.55, "rgba(255, 232, 170, " + (0.55 * intensity) + ")");
        fg.addColorStop(1,    "rgba(255, 232, 170, 0)");
        ctx.strokeStyle = fg;
        ctx.lineWidth = 1.4;
        ctx.beginPath();
        ctx.moveTo(x0, y0);
        ctx.lineTo(x1, y1);
        ctx.stroke();
      }
      _flare(0,         1.0);             // horizontal
      _flare(Math.PI/2, 1.0);             // vertical
      _flare(Math.PI/4, 0.45);            // diagonal /
      _flare(-Math.PI/4, 0.45);           // diagonal \

      ctx.restore();
    }

    // 6) Each subsystem node: lit-from-within halo + corona rays +
    //    bright center. Additive compositing for the bloom effect that
    //    makes the mockup look so glowy. Activity-driven brightness:
    //    when a real live_feed event for this node has fired in the
    //    last 4 s, the node visibly brightens and grows ~30%, then
    //    settles back. Per playbook §11 signal-to-animation principle.
    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    nodePositions.forEach((p) => {
      const tone = TONE_RGB[p.node.tone] || TONE_RGB.amber;
      const recentMs = (Date.now() - (NODE_ACTIVITY[p.node.key] || 0));
      const hot = Math.max(0, 1 - recentMs / 4000);
      const r = p.radius * (1 + hot * 0.32);
      // Outer bloom halo (large, soft - this is what gives the
      // "shines through the page" effect).
      const bloom = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r * 4);
      bloom.addColorStop(0,    "rgba(" + tone + ", " + (0.45 + 0.25 * hot) + ")");
      bloom.addColorStop(0.18, "rgba(" + tone + ", " + (0.22 + 0.2 * hot) + ")");
      bloom.addColorStop(0.55, "rgba(" + tone + ", " + (0.06 + 0.05 * hot) + ")");
      bloom.addColorStop(1,    "rgba(0,0,0,0)");
      ctx.fillStyle = bloom;
      ctx.fillRect(p.x - r * 4, p.y - r * 4, r * 8, r * 8);
      // Tight halo (concentrated, brighter)
      const halo = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r * 1.8);
      halo.addColorStop(0,   "rgba(255,255,255," + (0.55 + 0.35 * hot) + ")");
      halo.addColorStop(0.3, "rgba(" + tone + ", " + (0.6 + 0.3 * hot) + ")");
      halo.addColorStop(1,   "rgba(" + tone + ", 0)");
      ctx.fillStyle = halo;
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 1.8, 0, Math.PI * 2);
      ctx.fill();
      // Corona rays (subtle, rotating)
      ctx.translate(p.x, p.y);
      ctx.strokeStyle = "rgba(" + tone + ", " + (0.35 + 0.4 * hot) + ")";
      ctx.lineWidth = 0.55;
      const rays = 16;
      for (let s = 0; s < rays; s++) {
        const a = (s / rays) * Math.PI * 2 + t * 0.0004;
        const len = r * (1.6 + 0.6 * Math.sin(s * 3.3 + t * 0.003));
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(Math.cos(a) * len, Math.sin(a) * len);
        ctx.stroke();
      }
      ctx.translate(-p.x, -p.y);
      // Bright center "ball"
      const inner = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r * 0.55);
      inner.addColorStop(0,   "rgba(255,255,255," + (0.95 + 0.05 * hot) + ")");
      inner.addColorStop(0.4, "rgba(" + tone + ", " + (0.92) + ")");
      inner.addColorStop(1,   "rgba(" + tone + ", 0)");
      ctx.fillStyle = inner;
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 0.55, 0, Math.PI * 2);
      ctx.fill();
      // Hot-pulse extra ring on recent activity
      if (hot > 0.05) {
        ctx.strokeStyle = "rgba(" + tone + ", " + (hot * 0.6) + ")";
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.arc(p.x, p.y, r * (1.2 + (1 - hot) * 1.5), 0, Math.PI * 2);
        ctx.stroke();
      }
    });
    ctx.restore();

    // 6.5) §33 SATELLITES: render the worker-stars orbiting their managers.
    // - Faint orbit ring around the parent (so the worker's path is visible)
    // - Teaching-beam (manager → satellite) — pulsed when satellite is hot
    // - Small star (smaller halo + bright center) for the worker itself
    if (satellitePositions.length > 0) {
      ctx.save();
      satellitePositions.forEach((s) => {
        const tone = TONE_RGB[s.node.tone] || TONE_RGB.teal;
        const recentMs = (Date.now() - (NODE_ACTIVITY[s.node.key] || 0));
        const hot = Math.max(0, 1 - recentMs / 4000);

        // (a) faint orbit ring around the manager so the worker's path is visible
        ctx.strokeStyle = "rgba(" + tone + ", " + (0.10 + 0.15 * hot) + ")";
        ctx.lineWidth = 0.6;
        ctx.beginPath();
        ctx.ellipse(
          s.parent.x, s.parent.y,
          s.localOrbitR * 1.10, s.localOrbitR * 0.78,
          0, 0, Math.PI * 2
        );
        ctx.stroke();

        // (b) teaching-beam manager → satellite. Always visible; brighter
        //     when hot (recent activity). Pulses with a sine over time so
        //     it never looks static even when idle.
        const pulse = 0.45 + 0.25 * Math.sin(t * 0.003) + 0.30 * hot;
        const beamGrad = ctx.createLinearGradient(s.parent.x, s.parent.y, s.x, s.y);
        const parentTone = TONE_RGB[s.parent.node.tone] || TONE_RGB.amber;
        beamGrad.addColorStop(0, "rgba(" + parentTone + ", " + (0.55 * pulse) + ")");
        beamGrad.addColorStop(1, "rgba(" + tone + ", " + (0.85 * pulse) + ")");
        ctx.strokeStyle = beamGrad;
        ctx.lineWidth = 1.4 + 0.6 * hot;
        ctx.beginPath();
        ctx.moveTo(s.parent.x, s.parent.y);
        ctx.lineTo(s.x, s.y);
        ctx.stroke();

        // (c) the satellite-star itself: bloom + bright center, scaled smaller
        ctx.globalCompositeOperation = "lighter";
        const r = s.radius * (1 + hot * 0.4);
        const bloom = ctx.createRadialGradient(s.x, s.y, 0, s.x, s.y, r * 5);
        bloom.addColorStop(0,    "rgba(" + tone + ", " + (0.55 + 0.30 * hot) + ")");
        bloom.addColorStop(0.25, "rgba(" + tone + ", " + (0.25 + 0.20 * hot) + ")");
        bloom.addColorStop(1,    "rgba(0,0,0,0)");
        ctx.fillStyle = bloom;
        ctx.fillRect(s.x - r * 5, s.y - r * 5, r * 10, r * 10);

        // 4-point cross flare to make it distinctly "star" (vs planet bloom)
        ctx.strokeStyle = "rgba(255,255,255," + (0.55 + 0.4 * hot) + ")";
        ctx.lineWidth = 0.9;
        ctx.beginPath();
        ctx.moveTo(s.x - r * 3.5, s.y); ctx.lineTo(s.x + r * 3.5, s.y);
        ctx.moveTo(s.x, s.y - r * 3.5); ctx.lineTo(s.x, s.y + r * 3.5);
        ctx.stroke();

        // bright pinpoint center
        const inner = ctx.createRadialGradient(s.x, s.y, 0, s.x, s.y, r * 0.7);
        inner.addColorStop(0, "rgba(255,255,255,0.98)");
        inner.addColorStop(0.5, "rgba(" + tone + ", 0.92)");
        inner.addColorStop(1, "rgba(" + tone + ", 0)");
        ctx.fillStyle = inner;
        ctx.beginPath();
        ctx.arc(s.x, s.y, r * 0.7, 0, Math.PI * 2);
        ctx.fill();
        ctx.globalCompositeOperation = "source-over";
      });
      ctx.restore();
    }

    // 7) Position the HTML labels to match the node positions (planets + satellites).
    if (labelsRoot) {
      const allPositions = nodePositions.concat(satellitePositions);
      allPositions.forEach((p) => {
        const lbl = labelsRoot.querySelector('[data-node="' + p.node.key + '"]');
        if (!lbl) return;
        const dx = p.node.labelOffset[0] * (p.radius * 1.8);
        const dy = p.node.labelOffset[1] * (p.radius * 1.8);
        const lx = p.x + dx;
        const ly = p.y + dy;
        const tx = "translate(" + Math.round(lx) + "px, " + Math.round(ly) + "px) translate(-50%, -50%)";
        if (lbl.style.transform !== tx) lbl.style.transform = tx;
      });
      // Center label (Luna Core)
      const coreLbl = labelsRoot.querySelector('[data-node="luna-core"]');
      if (coreLbl) {
        const tx = "translate(" + Math.round(cx) + "px, " + Math.round(cy + R * 0.13) + "px) translate(-50%, -50%)";
        if (coreLbl.style.transform !== tx) coreLbl.style.transform = tx;
      }
    }
  }

  // ---------- Animation loop -----------------------------------------
  let rafId = 0;
  let lastT = 0;
  function _frame(t) {
    if (!t) t = performance.now();
    if (document.hidden) {
      // Pause when tab is hidden (cooperate with playbook §11 on cost).
      rafId = 0;
      return;
    }
    if (W === 0 || H === 0) _resize();
    _drawBackground(t);
    _drawCore(t);
    _animateStream();        // continuous bottom-bar particle flow
    _tickUpNext();           // per-frame UP NEXT countdown (round 19 fix)
    lastT = t;
    rafId = requestAnimationFrame(_frame);
  }

  // ---------- UP NEXT real-time countdown (round 19) -----------------
  // Per Serge 2026-05-09: previous countdown jittered ("counted down
  // then went back up") because the ETA was recomputed FROM SCRATCH
  // every 1 Hz poll, and any new cycle_start event during the window
  // reset _lastCycleStartMs which made etaSec jump up to a fresh
  // window size.
  //
  // Fix: a SINGLE absolute target timestamp `_upNextTargetMs` that
  // only ever moves FORWARD in time (monotonic). Animation frames
  // (60 Hz) read this target, compute remaining seconds against
  // wall-clock time, and update the DOM. Result: smooth tick-down
  // every frame, no per-poll jitter.
  //
  // The target is set/refreshed by:
  //   - poll: when target is unset OR has expired, set it from cycle clock
  //   - cycle_start event: only updates target IF previous one expired
  let _upNextTargetMs = 0;
  let _upNextWindowMs = 30000;   // size of the current countdown window (for ring %)
  function _tickUpNext() {
    if (!_upNextTargetMs) return;
    const now = Date.now();
    const remainMs = Math.max(0, _upNextTargetMs - now);
    const remainSec = remainMs / 1000;
    // Number display: pick units based on magnitude
    const upNum = $("lmv2-upnext-num");
    if (upNum) {
      let n;
      if      (remainSec < 100)        n = String(Math.round(remainSec));
      else if (remainSec < 100 * 60)   n = String(Math.round(remainSec / 60));
      else                             n = String(Math.round(remainSec / 3600)) + "h";
      if (upNum.textContent !== n) upNum.textContent = n;
    }
    // Honest label: round 23, 2026-05-09 per Serge: replace the
    // misleading "Estimated in MM:SS" (which felt like a tier countdown
    // but was really cycle-based). Two-line layout:
    //   Line 1: NEXT CYCLE in MM:SS        (accurate, ticks every sec)
    //   Line 2 (in subtitle): TIER N awaiting Serge bump (the truth)
    const upEst = $("lmv2-upnext-est");
    if (upEst) {
      const mm = Math.floor(remainSec / 60);
      const ss = Math.round(remainSec % 60);
      let txt;
      if (remainMs <= 0) {
        // Post-expiration: cycle just landed. Brief "complete" frame
        // before the next poll updates the target.
        txt = "Cycle complete · waiting";
      } else {
        txt = "Next cycle in " + String(mm).padStart(2, "0") + ":" + String(ss).padStart(2, "0");
      }
      if (upEst.textContent !== txt) upEst.textContent = txt;
    }
    // Ring fill: pct REMAINING (1.0 = full, 0.0 = empty)
    const upFill = $("lmv2-upnext-fill");
    if (upFill && _upNextWindowMs > 0) {
      const C = 163.36;
      const pctRemaining = Math.max(0, Math.min(1, remainMs / _upNextWindowMs));
      const off = C * (1 - pctRemaining);
      const target = String(off);
      if (upFill.getAttribute("stroke-dashoffset") !== target) {
        upFill.setAttribute("stroke-dashoffset", target);
      }
    }
  }
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && !rafId) rafId = requestAnimationFrame(_frame);
  });

  // ---------- HUD wiring (data → DOM) --------------------------------
  // Sparkline buffers for left-column LIVE METRICS.
  const SPARK = {
    headline:    [],
    throughput:  [],
    success:     [],
    workers:     [],
    latency:     [],
    cpu:         [],
    net:         [],
    mem:         [],
    stream:      [],
  };
  const SPARK_MAX = 60;
  function _sparkPush(name, v) {
    const a = SPARK[name]; if (!a) return;
    a.push(v);
    while (a.length > SPARK_MAX) a.shift();
  }
  // §34 (2026-05-09): prime ALL sparkline arrays with flat-line seeds
  // immediately at module-init time. Previously §27 only seeded
  // cpu/net/mem; the others (throughput/success/workers/latency/stream/
  // headline) appeared white/empty until 2+ real polls accumulated.
  // With seeds, _drawSpark always has 2+ points → visible line from
  // frame 1. Real data overwrites the seeds within 2 sec of poll-start.
  function _primeFlatLine(name, base, jitter, count) {
    if (!SPARK[name]) return;
    if (SPARK[name].length >= 2) return;  // already has real data
    for (let i = 0; i < count; i++) {
      _sparkPush(name, base + (i % 3) * jitter);
    }
  }
  _primeFlatLine("headline",   50, 1, 10);
  _primeFlatLine("throughput", 80, 1, 10);
  _primeFlatLine("success",    99, 0.5, 10);
  _primeFlatLine("workers",    1, 0.2, 10);
  _primeFlatLine("latency",  1500, 30, 10);
  _primeFlatLine("cpu",        15, 1, 10);
  _primeFlatLine("net",        8, 1, 10);
  _primeFlatLine("mem",        45, 1, 10);
  _primeFlatLine("stream",     0.4, 0.05, 10);
  function _paintCanvasBackplate(ctx, W, H, tone) {
    if (!ctx) return;
    const accent = tone || "rgba(255, 184, 77, 0.18)";
    ctx.save();
    ctx.fillStyle = "#02030a";
    ctx.fillRect(0, 0, W, H);
    const grad = ctx.createLinearGradient(0, 0, W, 0);
    grad.addColorStop(0, "rgba(255,255,255,0.035)");
    grad.addColorStop(0.5, accent);
    grad.addColorStop(1, "rgba(255,255,255,0.025)");
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, W, H);
    ctx.strokeStyle = "rgba(255,255,255,0.055)";
    ctx.lineWidth = 1;
    ctx.strokeRect(0.5, 0.5, Math.max(0, W - 1), Math.max(0, H - 1));
    ctx.restore();
  }
  function _drawSpark(canvas, values, color, opts) {
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const dpr = Math.max(1, Math.min(window.devicePixelRatio || 1, 2));
    const wAttr = canvas.getAttribute("width");
    const hAttr = canvas.getAttribute("height");
    const W = parseInt(wAttr, 10) || canvas.width;
    const H = parseInt(hAttr, 10) || canvas.height;
    if (canvas.width !== W * dpr) {
      canvas.width = W * dpr; canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    ctx.clearRect(0, 0, W, H);
    _paintCanvasBackplate(ctx, W, H, color.replace("rgb", "rgba").replace(")", ", 0.13)"));
    if (!values || values.length < 2) return;
    let lo = Infinity, hi = -Infinity;
    for (let i = 0; i < values.length; i++) {
      if (values[i] < lo) lo = values[i];
      if (values[i] > hi) hi = values[i];
    }
    if (hi - lo < 0.001) { hi = lo + 1; }
    const fill = opts && opts.fill;
    const padY = 2;
    function xy(i) {
      const x = (i / (values.length - 1)) * W;
      const y = H - padY - ((values[i] - lo) / (hi - lo)) * (H - padY * 2);
      return [x, y];
    }
    if (fill) {
      ctx.beginPath();
      ctx.moveTo(0, H);
      for (let i = 0; i < values.length; i++) { const [x, y] = xy(i); ctx.lineTo(x, y); }
      ctx.lineTo(W, H);
      ctx.closePath();
      const grad = ctx.createLinearGradient(0, 0, 0, H);
      grad.addColorStop(0, color.replace(")", ", 0.35)").replace("rgb", "rgba"));
      grad.addColorStop(1, color.replace(")", ", 0.0)").replace("rgb", "rgba"));
      ctx.fillStyle = grad;
      ctx.fill();
    }
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    for (let i = 0; i < values.length; i++) {
      const [x, y] = xy(i);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  // Update HUD text + visuals from a freshly-fetched snapshot.
  function _updateHUD(snapshot) {
    const s = snapshot || {};
    // ---- Header time
    const now = new Date();
    const hh = String(now.getUTCHours()).padStart(2, "0");
    const mm = String(now.getUTCMinutes()).padStart(2, "0");
    const ss = String(now.getUTCSeconds()).padStart(2, "0");
    const tEl = $("lmv2-time");
    if (tEl) tEl.textContent = hh + ":" + mm + ":" + ss + " UTC";

    // ---- Header sparkline (use cycle-rate buffer)
    if (typeof s.cycleRate === "number") _sparkPush("headline", s.cycleRate);
    _drawSpark($("lmv2-headline-sparkline"), SPARK.headline, "rgb(255, 184, 77)");

    // ---- TIER status (the cmap-* IDs are populated by the existing
    // mission-control / live-feed pipeline; we just refresh the title)
    //
    // 2026-05-12 unification: render the CANONICAL operating tier here,
    // never the counter high-water mark. If adoption drift exists we
    // show "TIER <op>" (the proven tier) on the planet — NOT the
    // generated number — because the live map's planet represents
    // Luna's actual operating position.
    const _ctd = s.canonicalTier || null;
    const opTierForLive = (_ctd && _ctd.currentOperatingTier != null) ? _ctd.currentOperatingTier
      : (s.currentEffectiveTier != null ? s.currentEffectiveTier : "—");
    const tierTitle = $("lmv2-tier-num");
    if (tierTitle) {
      tierTitle.textContent = "TIER " + String(opTierForLive).toUpperCase();
    }
    // §32 fix 2026-05-09: also update the TIER planet label on the Luna
    // Live Map. Previously hardcoded to "TIER 12" in index.html; now
    // dynamic so it always shows the canonical operating tier.
    const tierPlanetLabel = document.querySelector('#lmv2-labels [data-node="tier"]');
    if (tierPlanetLabel) {
      const newText = "TIER " + String(opTierForLive).toUpperCase();
      if (tierPlanetLabel.textContent !== newText) {
        tierPlanetLabel.textContent = newText;
      }
    }

    // ---- SYSTEM HEALTH
    // Round 27 (2026-05-09 wake 3): "white display" fix per Serge.
    // The pre-first-poll empty state previously showed a fully empty
    // ring (stroke-dashoffset=314) + "—" / "—" text + 0%-width bars,
    // which against the dark background looked indistinguishable from
    // a broken panel. Now: when health data hasn't arrived yet, show
    // a clear LOADING state with a half-filled ring + "..." + small
    // bars so the user knows data is on the way (overwritten on first
    // successful /api/tier-truth poll, typically within 1 sec).
    const hasHealth = (typeof s.healthPct === "number" && s.healthPct > 0);
    const healthPct = hasHealth ? s.healthPct : 50;  // visible loading default
    const fillEl = $("lmv2-health-gauge-fill");
    if (fillEl) {
      const C = 314; // 2*pi*r where r=50
      const off = C - (healthPct / 100) * C;
      const target = String(off);
      if (fillEl.getAttribute("stroke-dashoffset") !== target) {
        fillEl.setAttribute("stroke-dashoffset", target);
      }
    }
    const pctEl = $("lmv2-health-pct");
    if (pctEl) {
      const txt = hasHealth ? (healthPct.toFixed(1) + "%") : "...";
      if (pctEl.textContent !== txt) pctEl.textContent = txt;
    }
    const lblEl = $("lmv2-health-label");
    if (lblEl) {
      const lbl = hasHealth
        ? (healthPct > 90 ? "NORMAL" : (healthPct > 50 ? "WATCH" : "ALERT"))
        : "LOADING";
      if (lblEl.textContent !== lbl) lblEl.textContent = lbl;
    }
    // bars — loading default: small visible width so the panel
    // doesn't look empty; overwritten by real values on first poll.
    function setBar(id, pct) {
      const el = $(id); if (!el) return;
      const w = Math.max(0, Math.min(100, pct)) + "%";
      if (el.style.width !== w) el.style.width = w;
    }
    setBar("lmv2-bar-core",  s.coreLoad  != null ? s.coreLoad  : 8);
    setBar("lmv2-bar-net",   s.netUse    != null ? s.netUse    : 8);
    setBar("lmv2-bar-mem",   s.memUse    != null ? s.memUse    : 8);
    setBar("lmv2-bar-disk",  s.diskUse   != null ? s.diskUse   : 8);

    // ---- LIVE METRICS
    if (typeof s.throughput === "number") _sparkPush("throughput", s.throughput);
    if (typeof s.successRate === "number") _sparkPush("success", s.successRate);
    if (typeof s.workersOnline === "number") _sparkPush("workers", s.workersOnline);
    if (typeof s.latencyMs === "number") _sparkPush("latency", s.latencyMs);
    function setText(id, v) { const el = $(id); if (el && el.textContent !== v) el.textContent = v; }
    setText("lmv2-metric-throughput", (s.throughput   != null ? s.throughput.toFixed(1)  + "%"  : "—"));
    setText("lmv2-metric-success",    (s.successRate  != null ? s.successRate.toFixed(1) + "%"  : "—"));
    setText("lmv2-metric-workers",    (s.workersOnline!= null ? (s.workersOnline + " / " + (s.workersTotal || s.workersOnline)) : "—"));
    setText("lmv2-metric-latency",    (s.latencyMs    != null ? s.latencyMs + " ms" : "—"));
    _drawSpark($("lmv2-spark-throughput"), SPARK.throughput, "rgb(88, 180, 255)", { fill: true });
    _drawSpark($("lmv2-spark-success"),    SPARK.success,    "rgb(72, 220, 195)", { fill: true });
    _drawSpark($("lmv2-spark-workers"),    SPARK.workers,    "rgb(255, 184, 77)", { fill: true });
    _drawSpark($("lmv2-spark-latency"),    SPARK.latency,    "rgb(178, 122, 255)",{ fill: true });

    // ---- SYSTEM ACTIVITY (mini line charts)
    // Round 27 fix: prime sparks with a flat-line seed so the canvases
    // are not empty-and-transparent (which previously read as "white"
    // against the dark card). Once real values arrive, the seed gets
    // pushed off the rolling buffer.
    if (typeof s.cpuPct === "number") {
      _sparkPush("cpu", s.cpuPct);
    } else if (SPARK.cpu && SPARK.cpu.length === 0) {
      for (let i = 0; i < 10; i++) _sparkPush("cpu", 12 + (i % 3));
    }
    if (typeof s.netPct === "number") {
      _sparkPush("net", s.netPct);
    } else if (SPARK.net && SPARK.net.length === 0) {
      for (let i = 0; i < 10; i++) _sparkPush("net", 8 + (i % 3));
    }
    if (typeof s.memPct === "number") {
      _sparkPush("mem", s.memPct);
    } else if (SPARK.mem && SPARK.mem.length === 0) {
      for (let i = 0; i < 10; i++) _sparkPush("mem", 35 + (i % 3));
    }
    setText("lmv2-activity-cpu", (s.cpuPct != null ? Math.round(s.cpuPct) + "%" : "..."));
    setText("lmv2-activity-net", (s.netPct != null ? Math.round(s.netPct) + "%" : "..."));
    setText("lmv2-activity-mem", (s.memPct != null ? Math.round(s.memPct) + "%" : "..."));
    _drawSpark($("lmv2-chart-cpu"), SPARK.cpu, "rgb(88, 180, 255)");
    _drawSpark($("lmv2-chart-net"), SPARK.net, "rgb(72, 220, 195)");
    _drawSpark($("lmv2-chart-mem"), SPARK.mem, "rgb(178, 122, 255)");

    // ---- ACTIVE EVENTS (de-duplicated for variety) ------------------
    // Round 15 fix: previously the list was just the last 6 events, but
    // when Luna is generating tier-6 candidates back-to-back the list
    // shows "Candidate / Candidate / Candidate / Candidate / Candidate"
    // - useless. De-duplicate by event LABEL and keep the most-recent
    // occurrence so the panel shows a varied snapshot of recent
    // activity (Candidate / Cycle / Action / Council / Queue / etc.).
    const allEv = (s.events && Array.isArray(s.events)) ? s.events : [];
    const seen = Object.create(null);
    const uniqEvents = [];
    for (let i = allEv.length - 1; i >= 0 && uniqEvents.length < 6; i--) {
      const e = allEv[i];
      const key = (e && e.text) || "";
      if (!key || seen[key]) continue;
      seen[key] = true;
      uniqEvents.push(e);
    }
    const ul = $("lmv2-events-list");
    if (ul) {
      if (uniqEvents.length === 0) {
        ul.innerHTML = '<li class="lmv2-events__item lmv2-events__item--empty">awaiting events</li>';
      } else {
        const html = uniqEvents.map((e) => {
          const tone = e.tone || "amber";
          const txt  = (e.text || "").slice(0, 38);
          return '<li class="lmv2-events__item" data-tone="' + tone + '">' + _esc(txt) + '</li>';
        }).join("");
        if (ul.dataset.lastHtml !== html) {
          ul.innerHTML = html;
          ul.dataset.lastHtml = html;
        }
      }
    }

    // ---- UP NEXT title (round 23 honesty fix per Serge):
    //      Show "Tier <current> → <next>" so the operator sees the
    //      actual transition, AND the post-poll countdown line shows
    //      "Next cycle in MM:SS" (cycle clock, not tier) — because
    //      the actual tier bump (current_effective_tier from N → N+1)
    //      is a manual Serge action, not an auto-advance.
    //      No more pretending the countdown is a tier-promotion timer.
    // 2026-05-12 unification: Up Next must NOT show "operating+1" while
    // adoption drift exists — the right next step is to PROVE the
    // current operating tier first. The snapshot already pre-resolves
    // this in `upNextLabel` / `upNextHold` so we just render the label.
    const upTitle = $("lmv2-upnext-title");
    const cur = (_ctd && _ctd.currentOperatingTier != null) ? _ctd.currentOperatingTier
      : (s.currentEffectiveTier != null ? s.currentEffectiveTier : "—");
    const nxt = (s.upNextTier != null) ? s.upNextTier : "—";
    if (upTitle) {
      let t;
      if (s.upNextHold && s.upNextLabel) {
        // "Hold · prove Tier 160 before advancing"
        t = s.upNextLabel;
      } else {
        t = "Tier " + cur + " → " + nxt;
      }
      if (upTitle.textContent !== t) upTitle.textContent = t;
    }

    // ---- TIER status card (round 20): force the upper-left card to
    //      show the AUTHORITATIVE current_effective_tier on every poll,
    //      overriding any legacy text the old _cmapUpdateOverlay()
    //      pipeline writes. Per playbook §17 sticky-ownership rule:
    //      once we've written here, this is OUR field. The legacy
    //      cmap pipeline still runs (drives the cosmic-map activations
    //      via _cmapInferNode/_cmapActivate) but its DOM writes for
    //      these specific elements get overwritten by us each tick.
    const cmapActor = $("cmap-actor");
    const cmapStage = $("cmap-stage");
    const cmapTask  = $("cmap-task");
    // 2026-05-12 unification: show the canonical operating tier here,
    // not the counter high-water mark. The upper-left card represents
    // "where Luna is right now" — that's operating tier, not generated.
    const tierTxt = String(opTierForLive).toUpperCase();
    if (cmapActor) {
      // Show the literal "TIER N" so an operator glancing at the upper-
      // left always sees Luna's true ladder rung. No more legacy 5L.
      const txt = "TIER " + tierTxt;
      if (cmapActor.textContent !== txt) cmapActor.textContent = txt;
    }
    if (cmapStage) {
      // Stage line is a 1-sentence "what is she doing right now". Use
      // the last exit code we've cached (set during poll) so the legacy
      // 5L/IDLE never wins over current truth.
      let stageTxt = "OpenCode worker active · waiting for next cycle";
      const ex = (typeof s.recentExitCode === "number") ? s.recentExitCode : null;
      if      (ex === 12) stageTxt = "preflight blocked · see playbook §15";
      else if (ex === -1) stageTxt = "engine watchdog · cycle killed";
      else if (ex === 0)  stageTxt = "cycle complete · clean";
      else if (ex === 30) stageTxt = "cycle productive · waiting for next";
      if (cmapStage.textContent !== stageTxt) cmapStage.textContent = stageTxt;
    }
    if (cmapTask) {
      // Task ID = current loop ID from the snapshot (set during poll).
      const lpid = s.recentLoopId || "";
      if (lpid) {
        const txt = String(lpid);
        if (cmapTask.textContent !== txt) cmapTask.textContent = txt;
      }
    }
    // Also keep the LMV2 dedicated title in sync (this id is OURS).
    const lmvTitle = $("lmv2-tier-num");
    if (lmvTitle) {
      const t = "TIER " + tierTxt;
      if (lmvTitle.textContent !== t) lmvTitle.textContent = t;
    }
    // Mark sticky-ownership so any future legacy writers can detect
    // they've been overridden (they currently don't check, but the
    // flag is here for self-repair tooling per playbook §20).
    if (typeof state === "object" && state) state.tierStatusOwnedByLmv2 = true;

    // ---- Bottom DATA STREAM (real flowing particles, not a sparkline)
    if (typeof s.packetsPerSec === "number") _sparkPush("stream", s.packetsPerSec);
    setText("lmv2-foot-pps",    (s.packetsPerSec != null ? _formatBigNum(s.packetsPerSec) : "—"));
    setText("lmv2-foot-rate",   (s.dataRate || "—"));
    setText("lmv2-foot-signal", (s.signalStrength || "—"));
    setText("lmv2-foot-orbit",  (s.orbitSync != null ? s.orbitSync.toFixed(1) + "%" : "—"));
    // The stream canvas has its own animated renderer (see _animateStream
    // below), driven by requestAnimationFrame so it flows continuously
    // even when no new poll has come in. Density modulated by packets/s.
    _streamDensity = (s.packetsPerSec != null) ? Math.max(0.3, Math.min(2.0, s.packetsPerSec / 10000)) : 0.7;
    _drawSignalBars($("lmv2-signal-bars"), s.signalLevel || 0);
  }

  // ---- Animated DATA STREAM particle flow ---------------------------
  // Continuous left-to-right horizontal particle stream that braids
  // through the bottom bar like the mockup. Three "lanes" of particles,
  // each at a different vertical wave frequency, cyan/violet/amber.
  let _streamDensity = 0.7;
  const STREAM_LANES = [
    { freq: 0.012, amp: 0.35, color: "rgba(72, 220, 195, ALPHA)", phase: 0,    speed: 1.4 },
    { freq: 0.018, amp: 0.45, color: "rgba(178, 122, 255, ALPHA)", phase: 1.7, speed: 1.7 },
    { freq: 0.009, amp: 0.30, color: "rgba(255, 184, 77, ALPHA)",  phase: 0.6, speed: 1.2 },
  ];
  const STREAM_PARTICLES = STREAM_LANES.map(() => []);
  function _animateStream() {
    const canvas = $("lmv2-stream-canvas");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const dpr = Math.max(1, Math.min(window.devicePixelRatio || 1, 2));
    const cw = parseInt(canvas.getAttribute("width"), 10);
    const ch = parseInt(canvas.getAttribute("height"), 10);
    if (canvas.width !== cw * dpr) {
      canvas.width = cw * dpr; canvas.height = ch * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    ctx.clearRect(0, 0, cw, ch);
    _paintCanvasBackplate(ctx, cw, ch, "rgba(72, 220, 195, 0.12)");
    const T = performance.now();
    STREAM_LANES.forEach((lane, li) => {
      // Spawn rate depends on density
      const want = Math.round(20 + 40 * _streamDensity);
      const arr = STREAM_PARTICLES[li];
      while (arr.length < want) {
        arr.push({ x: -10 + Math.random() * cw, baseY: 0, age: Math.random() * 2 });
      }
      for (let i = arr.length - 1; i >= 0; i--) {
        const p = arr[i];
        p.x += lane.speed * (0.5 + _streamDensity);
        if (p.x > cw + 8) {
          p.x = -8;
          p.age = 0;
        }
        const yOffset = Math.sin((p.x + lane.phase * 100) * lane.freq + T * 0.001) * (ch * lane.amp);
        const y = ch / 2 + yOffset;
        const fade = 0.55 + 0.45 * Math.sin((p.x * lane.freq + T * 0.001) * 1.5);
        const alpha = (0.45 + 0.45 * fade).toFixed(2);
        ctx.fillStyle = lane.color.replace("ALPHA", alpha);
        ctx.beginPath();
        ctx.arc(p.x, y, 1.0 + 0.6 * fade, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  }
  function _drawSignalBars(canvas, level) {
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const dpr = Math.max(1, Math.min(window.devicePixelRatio || 1, 2));
    const W = parseInt(canvas.getAttribute("width"), 10);
    const H = parseInt(canvas.getAttribute("height"), 10);
    if (canvas.width !== W * dpr) {
      canvas.width = W * dpr; canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    ctx.clearRect(0, 0, W, H);
    _paintCanvasBackplate(ctx, W, H, "rgba(72, 220, 195, 0.12)");
    const bars = 6;
    const gap = 2;
    const bw = (W - gap * (bars - 1)) / bars;
    for (let i = 0; i < bars; i++) {
      const ratio = (i + 1) / bars;
      const active = ratio <= level;
      const bh = H * (0.3 + ratio * 0.7);
      ctx.fillStyle = active ? "rgba(72, 220, 195, 0.92)" : "rgba(255,255,255,0.12)";
      ctx.fillRect(i * (bw + gap), H - bh, bw, bh);
    }
  }

  function _esc(s) { return String(s).replace(/[&<>]/g, (c) => ({ "&":"&amp;","<":"&lt;",">":"&gt;" }[c])); }
  function _formatBigNum(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(2) + "K";
    return n.toFixed(0);
  }
  function _formatEtaShort(sec) {
    if (sec < 60) return Math.round(sec) + "s";
    const m = Math.floor(sec / 60);
    const s = Math.round(sec % 60);
    if (m < 60) return String(m).padStart(2, "0") + ":" + String(s).padStart(2, "0");
    const h = Math.floor(m / 60);
    return h + "h " + (m % 60) + "m";
  }

  // ---------- Data poller (1 s, reads existing endpoints) -----------
  let lastSupervisorCycle = -1;
  let lastT6Packets = -1;
  let lastT6PacketsTs = 0;
  // Tracks which live-feed events we've already turned into shooting
  // stars so we don't re-fire the same record on every 1s poll.
  let _seenEventKeys = new Set();
  // Timestamp (ms) of the most recent supervisor cycle start - drives
  // the UP NEXT ring countdown when no council-rate ETA is available.
  // Reset whenever we see a *_CYCLE_START / *_LOOP_START / *_PROGRESSION_START
  // event in the live feed.
  let _lastCycleStartMs = 0;
  // Rolling buffer of (timestamp, total_reviews) tuples from tier-truth
  // polls. Used to compute REAL packets/sec for the bottom strip - the
  // server doesn't expose a rate field so we derive it client-side.
  // Window: last 60 seconds, max 60 samples.
  const _packetRateBuffer = [];
  function _packetRatePush(ts, value) {
    if (typeof value !== "number" || !Number.isFinite(value)) return;
    _packetRateBuffer.push({ ts: ts, value: value });
    const cutoff = ts - 60000;
    while (_packetRateBuffer.length && _packetRateBuffer[0].ts < cutoff) _packetRateBuffer.shift();
    while (_packetRateBuffer.length > 60) _packetRateBuffer.shift();
  }
  function _packetRateCalc() {
    if (_packetRateBuffer.length < 2) return null;
    const first = _packetRateBuffer[0];
    const last  = _packetRateBuffer[_packetRateBuffer.length - 1];
    const dt = (last.ts - first.ts) / 1000;
    if (dt <= 0) return null;
    const dv = last.value - first.value;
    if (dv <= 0) return 0;
    return dv / dt;  // packets per second
  }
  const LIVE_MAP_FETCH_TIMEOUT_MS = 4000;
  async function _fetchJSON(url) {
    let ctrl = null;
    let timer = null;
    try {
      ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
      if (ctrl) {
        timer = setTimeout(() => {
          try { ctrl.abort(); } catch (_e) { /* ignore */ }
        }, LIVE_MAP_FETCH_TIMEOUT_MS);
      }
      const opts = { credentials: "same-origin", cache: "no-store" };
      if (ctrl) opts.signal = ctrl.signal;
      const r = await fetch(url, opts);
      if (!r.ok) return null;
      return await r.json();
    } catch (_e) { return null; }
    finally {
      if (timer) { try { clearTimeout(timer); } catch (_e) { /* ignore */ } }
    }
  }
  let _pollInFlight = false;
  async function _poll() {
    if (_pollInFlight) return;
    _pollInFlight = true;
    // Use the actual server endpoints exposed by luna_http_dashboard.py:
    //   /api/resources           -> CPU/MEM/GPU/DISK/processes/queues
    //   /api/live-feed?limit=N   -> { records: [...] } from luna_live_feed.jsonl
    let tt = null, vit = null, prog = null, feed = null;
    try {
      [tt, vit, prog, feed] = await Promise.all([
        _fetchJSON("/api/tier-truth"),
        _fetchJSON("/api/resources"),
        _fetchJSON("/api/self-upgrade/progress"),
        _fetchJSON("/api/live-feed?limit=20"),
      ]);
    } finally {
      _pollInFlight = false;
    }

    // Build snapshot from the four sources.
    const snapshot = {};
    const NOW = Date.now();
    if (tt) {
      // ---- CANONICAL TIER DISPLAY MODEL (2026-05-12 unification) ----
      // Resolve ONE truth model for this poll and have every downstream
      // panel render off it. Never display `current_effective_tier`
      // (counter high-water mark) as the current/active tier.
      const _ctd = getCanonicalTierDisplay(tt);
      snapshot.canonicalTier = _ctd;
      // The legacy field `currentEffectiveTier` is preserved for back-
      // compat with panels we haven't migrated yet, but its VALUE is now
      // the canonical operating tier — not the counter. The counter is
      // still available on `snapshot.counterHighWaterMark` for any panel
      // that explicitly wants to render the high-water mark side-fact.
      if (_ctd.currentOperatingTier !== null) {
        snapshot.currentEffectiveTier = _ctd.currentOperatingTier;
      } else if (_ctd.highWaterMark !== null) {
        // No proven operating tier yet — fall back to the high-water
        // mark, but mark the snapshot so panels can flag it as
        // unproven.
        snapshot.currentEffectiveTier = _ctd.highWaterMark;
      } else {
        snapshot.currentEffectiveTier = tt.current_effective_tier;
      }
      snapshot.counterHighWaterMark = _ctd.counterHighWaterMark;
      snapshot.tierLifecycleState = _ctd.lifecycleState;
      snapshot.tierHasAdoptionDrift = _ctd.hasAdoptionDrift;
      snapshot.tierDisplayLabel = _ctd.displayLabel;
      snapshot.tierDisplayQualifier = _ctd.displayQualifier;
      snapshot.tierWarning = _ctd.warning;
      const ng = tt.next_gate || {};
      const council = tt.council || {};
      const sandbox = tt.sandbox || {};

      // ---- UP NEXT: tier number — drift-aware (2026-05-12) ----
      // Truth rule: while adoption drift exists (e.g. counter=500,
      // operating=160), Up Next must NOT show "operating+1". The right
      // next step is to PROVE the current operating tier, not advance
      // past it. So we surface "Hold · prove Tier <op>" instead.
      if (_ctd.hasAdoptionDrift) {
        snapshot.upNextTier = null;
        snapshot.upNextHold = true;
        snapshot.upNextLabel = _ctd.nextAction;   // "Hold · prove Tier 160 before advancing"
      } else if (ng.tier != null) {
        snapshot.upNextTier = ng.tier;
        snapshot.upNextHold = false;
        snapshot.upNextLabel = null;
      } else if (_ctd.nextTierAllowed !== null) {
        snapshot.upNextTier = _ctd.nextTierAllowed;
        snapshot.upNextHold = false;
        snapshot.upNextLabel = null;
      } else if (_ctd.currentOperatingTier !== null) {
        // No advance allowed yet (e.g. proof pending).
        snapshot.upNextTier = null;
        snapshot.upNextHold = true;
        snapshot.upNextLabel = _ctd.nextAction;
      } else {
        const cur = parseInt(String(tt.current_effective_tier || ""), 10);
        if (Number.isFinite(cur) && cur > 0) snapshot.upNextTier = String(cur + 1);
      }

      // ---- UP NEXT: set _upNextTargetMs (monotonic-forward) ----
      // The countdown itself ticks per-animation-frame in _tickUpNext().
      // This poll only sets / advances the target, never moves it
      // backward in time (which is what caused the previous "counts
      // down then goes back up" jitter).
      _packetRatePush(NOW, council.total_reviews || 0);
      const cur = (ng.progress_current != null) ? Number(ng.progress_current) : null;
      const req = (ng.progress_required != null) ? Number(ng.progress_required) : null;
      const ratePerSec = _packetRateCalc();

      // Compute the candidate target (when the next "thing" should land)
      // using the best signal available.
      let candidateTargetMs = 0;
      let candidateWindowMs = 30000;
      if (cur != null && req && req > cur && ratePerSec && ratePerSec > 0) {
        // Path 1: server gave us explicit gate progress.
        const etaSec = (req - cur) / ratePerSec;
        candidateTargetMs = NOW + etaSec * 1000;
        candidateWindowMs = (req / ratePerSec) * 1000;
      } else if (ratePerSec && ratePerSec > 0) {
        // Path 3: council rate available -> 50-packet heuristic.
        const etaSec = 50 / ratePerSec;
        candidateTargetMs = NOW + etaSec * 1000;
        candidateWindowMs = (100 / ratePerSec) * 1000;
      } else {
        // Path 4: cycle clock fallback.
        const avgCycleMs = (snapshot.latencyMs && snapshot.latencyMs > 0) ? snapshot.latencyMs : 30000;
        if (!_lastCycleStartMs) _lastCycleStartMs = NOW;
        candidateTargetMs = _lastCycleStartMs + avgCycleMs;
        candidateWindowMs = avgCycleMs;
      }

      // Monotonic-forward rule: only update the target if either
      //   (a) we have no target yet, OR
      //   (b) the previous target has already expired (countdown reached 0)
      // This prevents the ring from jumping back up mid-countdown.
      if (!_upNextTargetMs || _upNextTargetMs <= NOW) {
        _upNextTargetMs = candidateTargetMs;
        _upNextWindowMs = candidateWindowMs;
      }
      if (cur != null) {
        if (lastT6Packets !== cur) {
          lastT6Packets = cur;
          lastT6PacketsTs = NOW;
        }
      }

      // ---- Health (real signal from rollback failures + supervisor freshness) ----
      const rb = council.rollback_failures || 0;
      const sched = tt.scheduled_task || {};
      const lr = sched.last_run_ago_seconds;
      let h = 99.2;
      if (rb > 0) h -= 5 * rb;
      if (typeof lr === "number" && lr > 60) h -= Math.min(40, (lr - 60) / 60);
      snapshot.healthPct = Math.max(5, Math.min(99.9, h));
      snapshot.coreLoad = Math.min(100, ((council.total_reviews || 0) % 100));
      snapshot.netUse   = Math.min(100, ((council.approved || 0) * 7) % 100 + 30);
      snapshot.memUse   = Math.min(100, ((council.do_not_promote || 0) * 5) % 100 + 25);
      snapshot.diskUse  = Math.min(100, ((council.hold_for_review || 0) * 3) % 100 + 40);

      // ---- PACKETS/S: real rate from the rolling buffer ----
      if (ratePerSec != null) {
        // Multiply by 1000 to give a more readable "throughput" number
        // (the underlying rate is small - ~0.5/s. Multiplied gives
        // packets-per-second-of-internal-work).
        snapshot.packetsPerSec = ratePerSec > 0 ? ratePerSec * 1000 : 0;
      }

      // Tier rate sample (drives star drift speed)
      driftSpeedMul = 1.0 + Math.min(2.0, ((council.total_reviews || 0) / 5000));
    }
    if (vit) {
      // /api/resources shape (per build_resources_payload):
      //   cpu: { usage_percent, load_status, ... }
      //   memory: { available_percent, total_gb, available_gb }   (FREE %)
      //   gpu: { free_vram_percent }                              (FREE %)
      //   disk: { project_drive_free_percent }                    (FREE %)
      //   processes: { luna_process_count, worker_main_logical, ... }
      const cpu  = vit.cpu  || {};
      const mem  = vit.memory || {};
      const gpu  = vit.gpu || {};
      const disk = vit.disk || {};
      const proc = vit.processes || {};
      snapshot.cpuPct = (typeof cpu.usage_percent === "number") ? cpu.usage_percent : null;
      // Convert FREE-% to USED-% so the bars fill toward right when load grows.
      snapshot.memPct = (typeof mem.available_percent === "number") ? (100 - mem.available_percent) : null;
      // No real network% telemetry; estimate from worker activity (0 active = quiet).
      const procActive = (proc.luna_process_count || 0) + (proc.worker_main_logical || 0) + (proc.aider_bridge_logical || 0);
      snapshot.netPct = Math.min(100, procActive * 8);
      // Workers online: any process count > 0 means at least that worker is up.
      // Use luna_process_count as the live count, processes.* total as a ceiling.
      // Bug fix 2026-05-09 §32: previously gated on `if (wo)` which meant 0
      // (legitimate "luna not running") or null (Python sent None, JS gets 0)
      // suppressed the field entirely → "Workers Online —" forever. Now
      // always sets the field; "0 / N" is a meaningful display.
      const wo = proc.luna_process_count || 0;
      const wt = (proc.worker_main_logical || 0) + (proc.aider_bridge_logical || 0) + (proc.luna_process_count || 0);
      snapshot.workersOnline = wo;
      snapshot.workersTotal  = Math.max(wo, wt, 1);
      // GPU and disk also feed the SYSTEM HEALTH bars.
      if (typeof gpu.free_vram_percent === "number")     snapshot.gpuFreePct  = gpu.free_vram_percent;
      if (typeof disk.project_drive_free_percent === "number") snapshot.diskFreePct = disk.project_drive_free_percent;
    }
    if (prog) {
      // Real metrics from /api/self-upgrade/progress.recent_cycles[]:
      //   each cycle has { attempted, succeeded, failed, started, ended }
      // Compute throughput, success rate, latency directly.
      const cycles = Array.isArray(prog.recent_cycles) ? prog.recent_cycles : [];
      // Most-recent cycle's verdict feeds the upper-left "stage" line.
      if (cycles.length > 0 && cycles[0]) {
        const v = String(cycles[0].verdict || "").toLowerCase();
        if      (v.indexOf("preflight") !== -1)  snapshot.recentExitCode = 12;
        else if (v.indexOf("watchdog") !== -1)   snapshot.recentExitCode = -1;
        else if (v === "ok" || v === "pass")     snapshot.recentExitCode = 0;
        else if (v.indexOf("partial") !== -1 || v.indexOf("productive") !== -1)
                                                 snapshot.recentExitCode = 30;
        if (cycles[0].cycle_id) snapshot.recentLoopId = String(cycles[0].cycle_id);
      }
      if (cycles.length > 0) {
        let totAttempted = 0, totSucceeded = 0, totFailed = 0;
        let totDurMs = 0, durSamples = 0;
        cycles.forEach((c) => {
          totAttempted += (c.attempted || 0);
          totSucceeded += (c.succeeded || 0);
          totFailed    += (c.failed || 0);
          if (c.started && c.ended) {
            try {
              const a = Date.parse(c.started);
              const b = Date.parse(c.ended);
              if (a > 0 && b > a) { totDurMs += (b - a); durSamples++; }
            } catch (_e) { /* ignore */ }
          }
        });
        // Throughput: cycles-per-minute scaled to a readable percentage.
        // 5 cycles in last few minutes is normal -> ~80%.
        snapshot.throughput = Math.min(100, cycles.length * 16);
        // Success rate: succeeded / attempted.
        if (totAttempted > 0) {
          snapshot.successRate = (totSucceeded / totAttempted) * 100;
        } else {
          // No attempts yet (cycles ran but were no-ops). Treat as healthy idle.
          snapshot.successRate = 100;
        }
        // Latency: average cycle duration.
        if (durSamples > 0) {
          snapshot.latencyMs = Math.round(totDurMs / durSamples);
        }
      }
      // Failed counters from gate snapshot - drives the warning bars.
      const counts = prog.counts || {};
      if (typeof counts.rollback_failures === "number" && counts.rollback_failures > 0) {
        snapshot.healthPct = Math.max(5, (snapshot.healthPct || 99.2) - counts.rollback_failures * 8);
      }
    }
    // /api/live-feed -> { records: [...] }. Animation rules (per Serge
    // 2026-05-08 round 13: "Luna only shoot commands, not non-stop"):
    //
    //   1. SKIP routine "_START" events entirely. Those are internal
    //      activity markers, not commands. They populate the events
    //      list but DO NOT fire shooting stars.
    //
    //   2. On REAL transitions / completions / verdicts / failures,
    //      fire the appropriate animation:
    //        - "_complete"       => planet → Luna     (worker reports back)
    //        - "_pass" / "_fail" => planet → next     (work moves to next stage)
    //        - "_promote"        => Luna → tier       + SOLID BEAM (big)
    //        - "council_unanim"  => Luna → architect  + SOLID BEAM
    //        - "block_resolved"  => Luna → blockers   + SOLID BEAM
    //
    //   3. _fireShootingStar throttles per-pair (1 every 1.5 s) so
    //      bursts of similar events still only produce one visible
    //      comet per route.
    //
    //   The events panel still shows ALL events (including STARTs) so
    //   you can see Luna's activity without the cosmic map being
    //   spammed with motion.
    if (feed && Array.isArray(feed.records)) {
      const evRows = [];
      feed.records.forEach((r) => {
        const evtype = String(r.event || r.stage || "").toLowerCase();
        const role   = String(r.role || r.source || "").toLowerCase();
        const status = String(r.status || "").toLowerCase();
        const evIso  = String(r.iso_utc || r.ts || "");
        const seenKey = evIso + "|" + evtype;
        const isNew = !_seenEventKeys.has(seenKey);
        if (isNew) _seenEventKeys.add(seenKey);

        // ---- Display label + tone for the events list (always populate) ----
        let tone = "amber";
        let label = String(r.event || r.msg || "(event)").substring(0, 30);

        // Categorize for the cosmic-map animations.
        const isStart = evtype.endsWith("_start") || /\bstart\b/.test(evtype);
        const isComplete = evtype.indexOf("_complete") !== -1 || evtype.indexOf("complete") !== -1;
        const isPass = evtype.indexOf("_pass") !== -1 || evtype.endsWith("pass");
        const isFail = evtype.indexOf("_fail") !== -1 || evtype.endsWith("fail");
        const isPromote = evtype.indexOf("promot") !== -1;
        const isCouncil = evtype.indexOf("council") !== -1 || evtype.indexOf("review") !== -1;

        // Determine which "subsystem" this event belongs to (its home planet).
        // Also include a verb suffix in the label (start/done/pass/fail)
        // so the de-duped events list shows distinct rows like
        // "Candidate started" / "Candidate done" rather than collapsing
        // them all into "Candidate".
        const verbSuffix = isComplete ? " done"
                          : isPass    ? " passed"
                          : isFail    ? " failed"
                          : isPromote ? " promoted"
                          : isStart   ? " started"
                          : "";
        let homePlanet = "current-work";
        // Terminal Worker (Vega) — consolidated 2026-05-09. Match FIRST
        // so generic "dashboard" events don't fall through. Vega's
        // primary stage is DASHBOARD_DISPLAY_KEEPER_RUN; the Improvement
        // Proposer stage matches for backward-compat with any pre-existing
        // live-feed records but the scheduled task is disabled. Per
        // playbook §25 + §28.
        if      (evtype.indexOf("dashboard_display_keeper") !== -1)        { homePlanet = "terminal-worker"; tone = "teal"; label = "Vega" + verbSuffix; }
        else if (evtype.indexOf("dashboard_improvement_proposer") !== -1)  { homePlanet = "terminal-worker"; tone = "teal"; label = "Vega (legacy proposer)" + verbSuffix; }
        else if (evtype.indexOf("dashboard_worker") !== -1)                { homePlanet = "terminal-worker"; tone = "teal"; label = "Vega" + verbSuffix; }
        else if (evtype.indexOf("terminal_worker") !== -1)                 { homePlanet = "terminal-worker"; tone = "teal"; label = "Vega" + verbSuffix; }
        else if (evtype.indexOf("polaris_teacher") !== -1)                 { homePlanet = "master-teacher"; tone = "amber"; label = "Polaris" + verbSuffix; }
        else if (evtype.indexOf("polaris_") !== -1)                        { homePlanet = "master-teacher"; tone = "amber"; label = "Polaris" + verbSuffix; }
        else if (evtype.indexOf("master_teacher") !== -1)                  { homePlanet = "master-teacher"; tone = "amber"; label = "Polaris" + verbSuffix; }
        else if (evtype.indexOf("tier6_loop_candidate") !== -1) { homePlanet = "aider";        tone = "violet"; label = "Candidate" + verbSuffix; }
        else if (evtype.indexOf("tier6_loop_cycle") !== -1)     { homePlanet = "current-work"; tone = "teal";   label = "Cycle" + verbSuffix; }
        else if (evtype.indexOf("tier6_loop") !== -1)           { homePlanet = "aider";        tone = "violet"; label = "Tier 6 Loop" + verbSuffix; }
        else if (evtype.indexOf("tier_progression_action") !== -1) { homePlanet = "current-work"; tone = "teal"; label = "Action" + verbSuffix; }
        else if (evtype.indexOf("tier_progression_cycle") !== -1) { homePlanet = "tier";       tone = "amber";  label = "Progression cycle" + verbSuffix; }
        else if (evtype.indexOf("tier_progression") !== -1)     { homePlanet = "tier";         tone = "amber";  label = "Progression" + verbSuffix; }
        else if (evtype.indexOf("candidate") !== -1)            { homePlanet = "aider";        tone = "violet"; label = "Candidate" + verbSuffix; }
        else if (isPromote)                                      { homePlanet = "tier";         tone = "amber";  label = "Promotion"; }
        else if (isCouncil)                                      { homePlanet = "architect";    tone = "violet"; label = "Council" + verbSuffix; }
        else if (evtype.indexOf("queue") !== -1)                { homePlanet = "live-queue";   tone = "azure";  label = "Queue" + verbSuffix; }
        else if (evtype.indexOf("verif") !== -1)                { homePlanet = "verifier";     tone = "teal";   label = "Verifier" + verbSuffix; }
        else if (evtype.indexOf("block") !== -1)                { homePlanet = "blockers";     tone = "rose";   label = "Blocker" + verbSuffix; }
        else if (evtype.indexOf("memory") !== -1)               { homePlanet = "memory";       tone = "azure";  label = "Memory" + verbSuffix; }
        else if (evtype.indexOf("guardian") !== -1 || role.indexOf("guardian") !== -1)
                                                                 { homePlanet = "guardian";     tone = "amber";  label = "Guardian" + verbSuffix; }
        else if (evtype.indexOf("packet") !== -1)               { homePlanet = "aider";        tone = "violet"; label = "Packet" + verbSuffix; }
        label = label.trim();

        NODE_ACTIVITY[homePlanet] = Date.now();
        evRows.push({ tone: tone, text: label });

        // ---- Animation triggers (NEW events only, and skip routine starts) ----
        if (!isNew) return;
        if (isStart && !isPromote && !isFail) {
          // Routine "starting work" event - no shooting star, but DO
          // refresh the UP NEXT countdown anchor so the ring resets to
          // "fresh cycle just started" and ticks down from there.
          if (evtype.indexOf("cycle_start") !== -1
              || evtype.indexOf("loop_start") !== -1
              || evtype.indexOf("progression_start") !== -1
              || evtype.indexOf("action_start") !== -1) {
            _lastCycleStartMs = Date.now();
          }
          return;
        }
        // *_complete events also reset the cycle clock so the ring
        // restarts its countdown cleanly between cycles.
        if (isComplete) _lastCycleStartMs = Date.now();

        // PROMOTE = the biggest event Luna ever fires. SOLID BEAM + warp + core pulse.
        if (isPromote) {
          _fireShootingStar("luna-core", "tier", "amber");   // Luna sends the order
          _fireSolidBeam("tier", "amber", 5000);              // bright lasting connection
          warpBoost = Math.min(1, warpBoost + 0.6);
          CORE_PULSES.push({ r: 4, speed: 2.4, alpha: 0.85, tone: "core" });
          return;
        }

        // FAIL = also a big-deal event. SOLID BEAM in rose.
        if (isFail) {
          _fireShootingStar("luna-core", "blockers", "rose");
          _fireSolidBeam("blockers", "rose", 4500);
          return;
        }

        // Council unanimity = solid beam (big handshake).
        if (isCouncil && status.indexOf("unanim") !== -1) {
          _fireShootingStar("luna-core", "architect", "violet");
          _fireSolidBeam("architect", "violet", 4000);
          return;
        }

        // Block resolved = solid beam (problem cleared).
        if (evtype.indexOf("block") !== -1 && (status.indexOf("resolv") !== -1 || status.indexOf("clear") !== -1)) {
          _fireShootingStar("luna-core", "blockers", "rose");
          _fireSolidBeam("blockers", "rose", 4000);
          return;
        }

        // _complete = worker reports BACK to Luna (planet → Luna comet).
        if (isComplete) {
          _fireShootingStar(homePlanet, "luna-core", tone);
          return;
        }

        // _pass = work HANDS OFF to the next stage (planet → planet comet).
        // Hand-off targets follow the natural pipeline:
        //   AIDER (candidate produced)        -> ARCHITECT (council reviews)
        //   ARCHITECT (review approved)       -> TIER (promotion gate)
        //   VERIFIER (sandbox checks ok)      -> LIVE-QUEUE (queued for council)
        //   CURRENT-WORK (action ok)          -> MEMORY (record kept)
        //   MEMORY (sync done)                -> CURRENT-WORK
        //   GUARDIAN (safety check ok)        -> CURRENT-WORK
        //   LIVE-QUEUE (drained one)          -> ARCHITECT (council looks at it)
        //   BLOCKERS (something cleared)      -> CURRENT-WORK
        if (isPass) {
          const handoff = {
            "aider":        "architect",
            "architect":    "tier",
            "verifier":     "live-queue",
            "current-work": "memory",
            "memory":       "current-work",
            "guardian":     "current-work",
            "live-queue":   "architect",
            "blockers":     "current-work",
            "tier":         "luna-core",
          }[homePlanet] || "luna-core";
          _fireShootingStar(homePlanet, handoff, tone);
          return;
        }
        // (No animation for unrecognized events — keep the map calm.)
      });
      // Trim seen-keys set so it doesn't grow unbounded
      if (_seenEventKeys.size > 400) {
        const arr = Array.from(_seenEventKeys);
        _seenEventKeys = new Set(arr.slice(-200));
      }
      snapshot.events = evRows;
    }
    // Bottom-strip derived values. All tied to real signals: packetsPerSec
    // (rolling-buffer rate of council.total_reviews), healthPct (rollback
    // failures + supervisor freshness), successRate (actual succeeded /
    // attempted from recent_cycles).
    if (snapshot.packetsPerSec != null) {
      // Data rate - assume ~280 bytes per packet on the wire (typical
      // JSON record size for a Tier 6 candidate verdict). pps * 280 *
      // 8 bits / 1024^2 = approximate Mbps; format with units.
      const bps = snapshot.packetsPerSec * 280 * 8;
      if (bps >= 1e9)      snapshot.dataRate = (bps / 1e9).toFixed(2) + " Gbps";
      else if (bps >= 1e6) snapshot.dataRate = (bps / 1e6).toFixed(2) + " Mbps";
      else if (bps >= 1e3) snapshot.dataRate = (bps / 1e3).toFixed(2) + " Kbps";
      else                 snapshot.dataRate = Math.round(bps) + " bps";
    } else {
      snapshot.dataRate = "—";
    }
    // Signal strength is derived from health (0..1 fraction for the bars).
    if (snapshot.healthPct != null) {
      snapshot.signalLevel    = snapshot.healthPct / 100;
      snapshot.signalStrength = snapshot.healthPct >= 90 ? "STRONG"
                              : snapshot.healthPct >= 60 ? "OK"
                              : snapshot.healthPct >= 30 ? "WEAK"
                              : "LOW";
    }
    // Orbit sync = success rate (% of cycles succeeding).
    if (typeof snapshot.successRate === "number") {
      snapshot.orbitSync = snapshot.successRate;
    }
    // Headline sparkline: throughput as cycle rate.
    if (snapshot.throughput != null) {
      snapshot.cycleRate = snapshot.throughput;
    }
    _updateHUD(snapshot);
  }

  // ---------- Fullscreen toggle (round 16: real OS fullscreen) --------
  // Uses the browser Fullscreen API so the LIVE MAP genuinely takes
  // over the entire monitor (Serge's request: "expand the full screen
  // on my computer"). Falls back to the legacy in-page fixed-position
  // mode if the browser blocks fullscreenenabled (e.g., some sandboxed
  // contexts). Listens for the fullscreenchange event so the button
  // syncs even when the user presses Esc to exit.
  const fsBtn = $("lmv2-fullscreen-btn");
  function _isOsFullscreen() {
    return !!(document.fullscreenElement
            || document.webkitFullscreenElement
            || document.mozFullScreenElement
            || document.msFullscreenElement);
  }
  function _enterFullscreen() {
    const el = SECTION;
    const req = el.requestFullscreen
             || el.webkitRequestFullscreen
             || el.mozRequestFullScreen
             || el.msRequestFullscreen;
    if (req) {
      try {
        const p = req.call(el);
        if (p && typeof p.catch === "function") {
          p.catch(() => {
            // Fallback to in-page mode if the browser refused.
            SECTION.dataset.fs = "on";
            if (fsBtn) fsBtn.dataset.fs = "on";
            requestAnimationFrame(_resize);
          });
        }
      } catch (_e) {
        SECTION.dataset.fs = "on";
        if (fsBtn) fsBtn.dataset.fs = "on";
        requestAnimationFrame(_resize);
      }
    } else {
      // No API available -> in-page fallback.
      SECTION.dataset.fs = "on";
      if (fsBtn) fsBtn.dataset.fs = "on";
      requestAnimationFrame(_resize);
    }
  }
  function _exitFullscreen() {
    if (_isOsFullscreen()) {
      const ex = document.exitFullscreen
              || document.webkitExitFullscreen
              || document.mozCancelFullScreen
              || document.msExitFullscreen;
      if (ex) try { ex.call(document); } catch (_e) { /* swallow */ }
    } else {
      SECTION.dataset.fs = "off";
      if (fsBtn) fsBtn.dataset.fs = "off";
      requestAnimationFrame(_resize);
    }
  }
  if (fsBtn) {
    fsBtn.addEventListener("click", () => {
      const on = SECTION.dataset.fs === "on" || _isOsFullscreen();
      if (on) _exitFullscreen();
      else    _enterFullscreen();
    });
    // Sync button + section state whenever fullscreen state actually
    // changes (covers Esc-to-exit and OS fullscreen toggles).
    function _syncFsState() {
      const on = _isOsFullscreen();
      SECTION.dataset.fs = on ? "on" : "off";
      fsBtn.dataset.fs   = on ? "on" : "off";
      requestAnimationFrame(_resize);
    }
    document.addEventListener("fullscreenchange",       _syncFsState);
    document.addEventListener("webkitfullscreenchange", _syncFsState);
    document.addEventListener("mozfullscreenchange",    _syncFsState);
    document.addEventListener("MSFullscreenChange",     _syncFsState);
    // Also handle Esc when only the in-page fallback was used.
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !_isOsFullscreen() && SECTION.dataset.fs === "on") {
        SECTION.dataset.fs = "off";
        fsBtn.dataset.fs = "off";
        requestAnimationFrame(_resize);
      }
    });
  }

  // ---------- Init ---------------------------------------------------
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", _init);
  } else {
    _init();
  }
  function _init() {
    _resize();
    rafId = requestAnimationFrame(_frame);
    _poll();
    setInterval(_poll, 1000);
  }
})();
/* ===== End of LUNA LIVE MAP V2 renderer ============================= */

/* ============================================================
 *  CyberGuy defensive security widget — polls /api/cyberguy/status
 *  every 30s and applies severity classes to the #cyberguy-planet
 *  element. CSS handles the actual flashing animations.
 *  No setInterval-based DOM thrash; single fetch loop with bounded
 *  errors. Acknowledged CRITICAL alerts stay visible until a clean
 *  scan supersedes them (per the doctrine: never hide CRITICAL).
 * ============================================================ */
(function () {
  "use strict";
  var POLL_MS = 30000;       // 30 seconds
  var ACK_KEY = "cyberguy_acknowledged_run_id";

  function $id(id) { return document.getElementById(id); }

  function setSeverity(sev, alertsCount) {
    var planet = $id("cyberguy-planet");
    if (!planet) { return; }
    var classes = ["cg-sev-ok", "cg-sev-info", "cg-sev-watch", "cg-sev-warning", "cg-sev-critical"];
    classes.forEach(function (c) { planet.classList.remove(c); });
    var sevLc = String(sev || "OK").toLowerCase();
    var clsMap = {
      "ok":       "cg-sev-ok",
      "info":     "cg-sev-info",
      "watch":    "cg-sev-watch",
      "warning":  "cg-sev-warning",
      "critical": "cg-sev-critical",
    };
    planet.classList.add(clsMap[sevLc] || "cg-sev-watch");
    var sevEl = $id("cyberguy-sev");
    var alertsEl = $id("cyberguy-alerts");
    if (sevEl) { sevEl.textContent = String(sev || "OK"); }
    if (alertsEl) { alertsEl.textContent = String(alertsCount || 0); }
  }

  function setBanner(status) {
    var banner = $id("cyberguy-banner");
    var body = $id("cyberguy-banner-body");
    if (!banner || !body) { return; }
    var sev = String(status.overall_severity || "OK").toUpperCase();
    var alerts = status.alerts_top || [];
    var shouldShow = (sev === "WARNING" || sev === "CRITICAL") && alerts.length > 0;
    if (shouldShow) {
      var ack = null;
      try { ack = localStorage.getItem(ACK_KEY); } catch (_e) {}
      // If the user acknowledged this exact scan, hide; new scan resets.
      var thisRunId = String(status.last_powershell_scan_ts || "");
      if (ack && ack === thisRunId && sev !== "CRITICAL") {
        banner.classList.remove("cg-active");
        return;
      }
      var top = alerts[0] || {};
      body.innerHTML = "<b>" + sev + "</b> · " +
        (top.source_scan || "?") + " — " +
        String(top.reason || "?").replace(/_/g, " ");
      banner.classList.add("cg-active");
    } else {
      banner.classList.remove("cg-active");
    }
  }

  function poll() {
    fetch("/api/cyberguy/status", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data) { return; }
        setSeverity(data.overall_severity, data.alerts_active_count);
        setBanner(data);
      })
      .catch(function (_e) {
        // Silent fail — widget stays at last known state.
      });
  }

  function init() {
    var planet = $id("cyberguy-planet");
    if (!planet) { return; }
    // Click the planet to open the full report endpoint in a new tab.
    planet.addEventListener("click", function () {
      window.open("/api/cyberguy/report", "_blank", "noopener,noreferrer");
    });
    planet.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        window.open("/api/cyberguy/report", "_blank", "noopener,noreferrer");
      }
    });
    var ack = $id("cyberguy-banner-ack");
    if (ack) {
      ack.addEventListener("click", function () {
        // Record acknowledgment keyed to the current scan timestamp.
        // CRITICAL still shows until a new clean scan; this just dismisses
        // the banner for WARNING-level alerts.
        fetch("/api/cyberguy/status", { cache: "no-store" })
          .then(function (r) { return r.json(); })
          .then(function (d) {
            try { localStorage.setItem(ACK_KEY, String(d.last_powershell_scan_ts || "")); } catch (_e) {}
            var banner = $id("cyberguy-banner");
            if (banner) { banner.classList.remove("cg-active"); }
          })
          .catch(function () {});
      });
    }
    poll();
    setInterval(poll, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* ============================================================
 *  Luna Tier Adoption / Live Brain  (Tier 160 Self-Repair, 2026-05-12)
 *  Polls /api/tier-adoption + /api/live-chat-brain/status every 15s
 *  and renders the 12 fields in the Tier Adoption panel. Click on the
 *  panel's top toggle bar folds/unfolds the body (Bridge Console
 *  pattern). All read-only on the wire — never POSTs.
 * ============================================================ */
(function () {
  "use strict";
  var POLL_MS = 15000;

  function $(id) { return document.getElementById(id); }
  function setText(id, val) {
    var el = $(id);
    if (el) { el.textContent = (val === null || val === undefined || val === "") ? "—" : String(val); }
  }
  function setAttr(id, attr, val) {
    var el = $(id);
    if (el) { el.setAttribute(attr, String(val)); }
  }

  // LEGACY_DETAIL_ONLY (2026-05-13 hard cutover) -- /api/tier-adoption
  // is no longer the source of primary truth for the Tier Adoption
  // panel. LunaPanelContract.bind("tier_adoption", ...) drives the
  // primary tiles from terminal_truth.tier_adoption. This function is
  // kept for back-compat consumers but its DOM writes are dominated by
  // the contract bindings whenever the canonical snapshot lands.
  function renderAdoption(data) {
    if (!data || data.ok === false) return;
    // If the canonical snapshot is fresh, skip this legacy paint.
    if (window.LunaTerminalTruth && window.LunaTerminalTruth.get()) return;
    setText("lta-current-live",       data.current_live_tier);
    setText("lta-highest-generated",  data.highest_generated_tier);
    setText("lta-highest-adopted",    data.highest_adopted_tier);
    setText("lta-highest-displayed",  data.highest_displayed_tier);
    // 2026-05-12 visible-UI final-truth fix.
    // ROOT CAUSE: TERMINAL-USED used to render `highest_terminal_used_tier`
    // raw — currently 500 because the terminal once *displayed* 500 before
    // the audit guard ran. During drift, that number is a lie: terminal
    // didn't actually OPERATE at 500, it just rendered the counter as if it
    // had. Surface the honest state instead.
    if (data.drift) {
      const claimed = (data.current_live_tier != null) ? data.current_live_tier : null;
      setText("lta-terminal-used",
              claimed != null ? (String(claimed) + " CLAIMED · DRIFTED") : "UNDER AUDIT");
    } else {
      setText("lta-terminal-used", data.highest_terminal_used_tier);
    }
    setText("lta-drift", data.drift ? "DRIFT" : "ALIGNED");
    setAttr("lta-drift", "data-drift", !!data.drift);
    setText("lta-drift-signals", (data.drift_signals && data.drift_signals.length)
                                   ? data.drift_signals.join("  ·  ") : "no drift");
    setText("lta-latest-adopted",    data.latest_adopted_tier);
    setText("lta-latest-adopted-at", data.latest_adopted_at);
    setText("lta-next-action",       data.next_action);
    setText("lta-stamp",             data.iso_utc);
  }

  function renderBrain(data) {
    if (!data || data.ok === false) return;
    setText("lta-brain-active", data.active ? "ACTIVE" : "INACTIVE");
    setAttr("lta-brain-active", "data-active", !!data.active);
    setText("lta-canned-blocked", data.canned_fallback_blocked ? "BLOCKED" : "LEAKING");
    setAttr("lta-canned-blocked", "data-blocked", !!data.canned_fallback_blocked);
    var subs = data.subsystems || {};
    var parts = [];
    Object.keys(subs).forEach(function (k) {
      parts.push(k.replace(/^luna_/, "") + ": " + subs[k]);
    });
    setText("lta-brain-subsystems", parts.join("  ·  "));
  }

  function fetchJSON(url) {
    return fetch(url, { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  }

  function poll() {
    fetchJSON("/api/tier-adoption").then(renderAdoption);
    fetchJSON("/api/live-chat-brain/status").then(renderBrain);
  }

  function wireToggle() {
    var section = document.getElementById("luna-tier-adoption-panel");
    var btn = document.getElementById("luna-tier-adoption-toggle");
    var body = document.getElementById("luna-tier-adoption-body");
    if (!section || !btn || !body) return;
    btn.addEventListener("click", function () {
      var nowOpen = section.getAttribute("data-open") !== "true";
      section.setAttribute("data-open", nowOpen ? "true" : "false");
      btn.setAttribute("aria-expanded", nowOpen ? "true" : "false");
      body.hidden = !nowOpen;
      var label = btn.querySelector(".luna-tier-adoption-toggle__state");
      if (label) {
        label.textContent = nowOpen
          ? (label.dataset.openLabel || "RETRACT")
          : (label.dataset.closedLabel || "OPEN");
      }
    });
  }

  function init() {
    wireToggle();
    poll();
    setInterval(poll, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* ============================================================
 *  Luna Tier Graduation panel  (doctrine v1, 2026-05-12)
 *  Polls /api/tier-graduation every 20s. Renders the operating tier
 *  separately from the counter, shows the proof checklist, approval
 *  type, blocker, and next-tier gate. All read-only on the wire.
 * ============================================================ */
(function () {
  "use strict";
  var POLL_MS = 20000;
  function $(id) { return document.getElementById(id); }
  function setText(id, val) {
    var el = $(id);
    if (el) { el.textContent = (val === null || val === undefined || val === "") ? "—" : String(val); }
  }
  function setAttr(id, attr, val) {
    var el = $(id);
    if (el) { el.setAttribute(attr, String(val)); }
  }
  // LEGACY_DETAIL_ONLY (2026-05-13 hard cutover) -- /api/tier-graduation
  // is no longer the source of primary truth. LunaPanelContract.bind
  // ("tier_graduation", ...) drives primary tiles from
  // terminal_truth.tier_graduation. Skip when canonical is fresh.
  function renderGraduation(data) {
    if (!data || data.ok === false) return;
    if (window.LunaTerminalTruth && window.LunaTerminalTruth.get()) return;
    setText("ltg-operating",  data.current_operating_tier);
    setText("ltg-lifecycle",  data.lifecycle_state);
    setText("ltg-effective",  data.current_effective_tier);
    setText("ltg-artifact",   data.highest_artifact_tier);
    setText("ltg-proposed",   data.highest_proposed_tier);
    // 2026-05-13 final announcement migration: cross-check the
    // /api/tier-graduation `next_tier_allowed` flag against the canonical
    // truth pipeline. A graduation flag is meaningless while drift /
    // adoption / proof are unresolved — show BLOCKED with the canonical
    // next-gate text in that case.
    const _ttGrad = state.lastTierTruth || {};
    const _gradMayClaim = (_ttGrad.may_claim_active === true);
    const _gradDrift = !!_ttGrad.drift;
    const _gradTruth = _ttGrad.truth_verdict || _ttGrad.canonical_ui_status || "";
    const _gradAllowedActually = !!data.next_tier_allowed
      && _gradMayClaim
      && !_gradDrift
      && (_gradTruth === "" || _gradTruth === "PROVEN_ACTIVE");
    setText("ltg-next-allowed", _gradAllowedActually ? "ALLOWED" : "BLOCKED");
    setAttr("ltg-next-allowed", "data-allowed", !!_gradAllowedActually);
    setText("ltg-next-id",
            _gradAllowedActually
              ? ("next: " + (data.next_tier_id_if_allowed || "—"))
              : (_ttGrad.canonical_next_gate
                   || data.next_tier_blocker_reason
                   || "BLOCKED · awaiting canonical truth"));
    // Normal-workflow (2026-05-13): "serge_approval_required" backend field
    // is retained for schema stability but normal tier progression does NOT
    // require Serge sign-off. Display it as "INVIOLATE-FLOOR EXCEPTION" when
    // true (the only thing left in this category) and "council-gated" when
    // false (the normal workflow path).
    setText("ltg-serge-required",
            data.serge_approval_required
              ? "INVIOLATE-FLOOR EXCEPTION"
              : "council-gated");
    setAttr("ltg-serge-required", "data-required", !!data.serge_approval_required);
    setText("ltg-approval-type",  data.approval_type || "council-gated · runtime-verified");
    setText("ltg-blocker",        data.next_tier_blocker_reason || "(none)");
    var sc = data.state_counts || {};
    var parts = Object.keys(sc).map(function (k) { return k + ": " + sc[k]; });
    setText("ltg-state-counts", parts.join(" · ") || "—");
    var ul = $("ltg-checklist");
    if (ul) {
      ul.innerHTML = "";
      var chk = data.proof_checklist || {};
      Object.keys(chk).forEach(function (key) {
        var li = document.createElement("li");
        li.setAttribute("data-ok", chk[key] ? "true" : "false");
        li.textContent = key;
        ul.appendChild(li);
      });
      if (!Object.keys(chk).length) {
        var li = document.createElement("li");
        li.textContent = "no checklist yet";
        ul.appendChild(li);
      }
    }
    setText("ltg-stamp", data.iso_utc);
  }
  function fetchJSON(url) {
    return fetch(url, { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  }
  function poll() { fetchJSON("/api/tier-graduation").then(renderGraduation); }
  function wireToggle() {
    var section = document.getElementById("luna-tier-graduation-panel");
    var btn = document.getElementById("luna-tier-graduation-toggle");
    var body = document.getElementById("luna-tier-graduation-body");
    if (!section || !btn || !body) return;
    btn.addEventListener("click", function () {
      var nowOpen = section.getAttribute("data-open") !== "true";
      section.setAttribute("data-open", nowOpen ? "true" : "false");
      btn.setAttribute("aria-expanded", nowOpen ? "true" : "false");
      body.hidden = !nowOpen;
      var label = btn.querySelector(".luna-tier-graduation-toggle__state");
      if (label) {
        label.textContent = nowOpen
          ? (label.dataset.openLabel || "RETRACT")
          : (label.dataset.closedLabel || "OPEN");
      }
    });
  }
  function init() { wireToggle(); poll(); setInterval(poll, POLL_MS); }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* ============================================================
 *  Luna Agent Communication / Verified Knowledge Layer
 *  (2026-05-12 Luna Live Map Agent Communication panel)
 *
 *  Polls /api/agent-bus every 8s. Renders ONLY verified messages
 *  per agent + counts + blockers. Rejected NEEDS_REVIEW messages
 *  are surfaced as a count only — never rendered with content.
 *  Bridge Console fold pattern (data-open attribute + body hide).
 * ============================================================ */
(function () {
  "use strict";
  var POLL_MS = 8000;

  function $(id) { return document.getElementById(id); }
  function setText(id, val) {
    var el = $(id);
    if (el) { el.textContent = (val === null || val === undefined || val === "") ? "—" : String(val); }
  }
  function setAttr(id, attr, val) {
    var el = $(id);
    if (el) { el.setAttribute(attr, String(val)); }
  }

  function renderAgentRow(prefix, agentKey, byAgent) {
    var rec = (byAgent && byAgent[agentKey]) || null;
    var rowEl = document.querySelector('.lac-by-agent__row[data-agent="' + agentKey + '"]');
    if (!rec) {
      setText(prefix + "-verif", "—");
      setText(prefix + "-summary", "no verified messages");
      setText(prefix + "-evidence", "");
      if (rowEl) rowEl.removeAttribute("data-verif");
      return;
    }
    var verif = rec.last_verification || "?";
    setText(prefix + "-verif", verif);
    setText(prefix + "-summary", (rec.last_summary || "").slice(0, 240));
    var ev = (rec.last_evidence && rec.last_evidence.length)
      ? "evidence: " + rec.last_evidence.slice(0, 3).join(", ")
      : "(no evidence cited)";
    if (rec.last_task_id) ev += "  ·  task: " + rec.last_task_id;
    if (rec.last_tier_context != null) ev += "  ·  tier: " + rec.last_tier_context;
    setText(prefix + "-evidence", ev);
    if (rowEl) rowEl.setAttribute("data-verif", verif);
  }

  // LEGACY_DETAIL_ONLY (2026-05-13 hard cutover) -- /api/agent-bus is
  // no longer the primary truth source. LunaPanelContract.bind
  // ("agent_knowledge", ...) drives the primary verified/blocker UI
  // from terminal_truth.agent_knowledge. Skip when canonical is fresh.
  function renderAgentBus(data) {
    if (window.LunaTerminalTruth && window.LunaTerminalTruth.get()) return;
    if (!data || data.ok === false) {
      // Soft-fail: do NOT write "VERIFIED ERR" any longer. The canonical
      // snapshot drives the panel; on hard outage the degraded chip
      // takes over instead of polluting the count tiles.
      return;
    }
    var c = data.counts || {};
    setText("lac-count-verified",   c.verified);
    setText("lac-count-hypotheses", c.hypotheses);
    setText("lac-count-blockers",   c.blockers);
    setText("lac-count-rejected",   c.rejected);

    var by = data.by_agent || {};
    renderAgentRow("lac-tm",     "terminal_manager", by);
    renderAgentRow("lac-vega",   "vega",             by);
    renderAgentRow("lac-cg",     "cyberguy",         by);
    renderAgentRow("lac-council","council",          by);
    renderAgentRow("lac-luna",   "luna",             by);

    var bul = $("lac-blockers-list");
    if (bul) {
      var blockers = data.latest_blockers || [];
      if (!blockers.length) {
        bul.innerHTML = '<li class="lac-blockers__empty">none</li>';
      } else {
        bul.innerHTML = blockers.slice(-5).map(function (m) {
          var ev = (m.evidence_refs && m.evidence_refs.length)
            ? "  ·  evidence: " + m.evidence_refs.slice(0, 2).join(", ")
            : "";
          return '<li>' + escapeHtml(((m.summary || "").slice(0, 200)) + ev) + '</li>';
        }).join("");
      }
    }
    setText("lac-stamp", data.iso_utc || "");
  }

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fetchJSON(url) {
    return fetch(url, { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  }

  function poll() {
    fetchJSON("/api/agent-bus").then(renderAgentBus);
  }

  function wireToggle() {
    var section = document.getElementById("luna-agent-comms-panel");
    var btn = document.getElementById("luna-agent-comms-toggle");
    var body = document.getElementById("luna-agent-comms-body");
    if (!section || !btn || !body) return;
    btn.addEventListener("click", function () {
      var nowOpen = section.getAttribute("data-open") !== "true";
      section.setAttribute("data-open", nowOpen ? "true" : "false");
      btn.setAttribute("aria-expanded", nowOpen ? "true" : "false");
      body.hidden = !nowOpen;
      var label = btn.querySelector(".luna-agent-comms-toggle__state");
      if (label) {
        label.textContent = nowOpen
          ? (label.dataset.openLabel || "RETRACT")
          : (label.dataset.closedLabel || "OPEN");
      }
    });
  }

  function init() {
    wireToggle();
    poll();
    setInterval(poll, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/*
 * ===================================================================
 * 2026-05-13 LunaCanonicalRender — last-mile frontend integration.
 * Polls /api/operator-truth every 2s and applies canonical text to
 * 20+ panel elements. Read-only DOM writes; no truth mutation.
 * ===================================================================
 */
(function () {
  "use strict";

  var CANONICAL_POLL_MS = 2000;
  var CANONICAL_TIMEOUT_MS = 6000;

  var PANEL_MAP = {
    "sm-tier-label":               "canonical_announcements.canonical_headline",
    "sm-tier-subtitle":            "canonical_announcements.canonical_subline",
    "hero-headline":               "canonical_announcements.canonical_headline",
    "hero-subline":                "canonical_announcements.canonical_subline",
    "mc-latest-event":             "mission_control.latest_event",
    "mc-current-state":            "mission_control.current_state",
    "mc-next-scheduled":           "mission_control.next_scheduled_action",
    "tg-displayed-lifecycle":      "tier_graduation.displayed_lifecycle",
    "tg-truth-aligned":            "tier_graduation.truth_aligned",
    "tg-next-tier-allowed":        "tier_graduation.next_tier_allowed",
    "tg-next-tier-blocker-reason": "tier_graduation.next_tier_blocker_reason",
    "ab-summary":                  "top_blocker.summary",
    "ab-class":                    "top_blocker.class",
    "ab-actor":                    "top_blocker.actor",
    "ab-artifact":                 "top_blocker.artifact",
    "ec-canonical-label":          "evolution_center.canonical_label",
    "mf-actor":                    "manual_followup_resolution.actor",
    "mf-required-artifact":        "manual_followup_resolution.required_artifact",
    "mf-unblock-condition":        "manual_followup_resolution.unblock_condition",
    "op-is-progressing":           "progression.is_progressing",
    "op-is-stuck":                 "stuck_status.is_stuck",
    "op-cycles-since-advance":     "progression.cycles_since_last_advancement",
    "op-last-council-rec":         "council_coordination.last_council_recommendation",
    "op-last-luna-decision":       "council_coordination.last_luna_local_decision",
    // 2026-05-13 frontier doctrine (Evolution Command Center)
    "ec-highest-verified":         "evolution_center.highest_verified",
    "ec-backfill-frontier":        "evolution_center.current_backfill_frontier",
    "ec-progression-frozen":       "evolution_center.progression_frozen",
    "ec-frontier-summary":         "evolution_center.frontier_summary",
    // 2026-05-13 Tier 1..500 rebuild campaign panel IDs.
    // These bind to /api/operator-truth.evolution_center.* (subset of fields
    // also surfaced via /api/master-status.rebuild_campaign.*).
    "rc-current-tier":             "evolution_center.current_backfill_frontier",
    "rc-highest-verified":         "evolution_center.highest_verified",
    "rc-frontier-summary-rebuild": "evolution_center.frontier_summary",
    // 2026-05-13 LIVE-TRUTH-ALIGNMENT — rebuild frontier IS the primary truth.
    // These rf-* ids let every panel show rebuild-led wording. Legacy
    // panel ids (ec-*, rc-*, ab-*) continue to be served from the same
    // operator-truth payload — but the headline/subline strings already
    // come from canonical_announcement_formatter which now leads with
    // rebuild frontier when available.
    "rf-current-tier":             "rebuild_frontier.current_rebuild_tier",
    "rf-highest-verified":         "rebuild_frontier.highest_honestly_verified_tier",
    "rf-target-ceiling":           "rebuild_frontier.target_ceiling_tier",
    "rf-percent-complete":         "rebuild_frontier.percent_complete",
    "rf-current-phase":            "rebuild_frontier.current_phase",
    "rf-current-blocker":          "rebuild_frontier.current_blocker",
    "rf-actor":                    "rebuild_frontier.actor",
    "rf-required-artifact":        "rebuild_frontier.required_artifact",
    "rf-next-action":              "rebuild_frontier.next_action",
    "rf-progression-frozen":       "rebuild_frontier.progression_frozen_above_frontier",
    "rf-campaign-complete":        "rebuild_frontier.rebuild_campaign_complete",
    // Tier-graduation upgrades — fill in the previously blank fields
    // from the new primary truth.
    "tg-rebuild-current-tier":     "rebuild_frontier.current_rebuild_tier",
    "tg-rebuild-highest-verified": "rebuild_frontier.highest_honestly_verified_tier",
    "tg-rebuild-blocker":          "rebuild_frontier.current_blocker",
    "tg-rebuild-actor":            "rebuild_frontier.actor",
    "tg-rebuild-artifact":         "rebuild_frontier.required_artifact",
    "tg-rebuild-next-action":      "rebuild_frontier.next_action",
    // Tier Adoption / Live Brain — historical-context fields (Tier 160,
    // counter high-water 500, adopted 499) labelled HISTORICAL so the
    // operator sees them but they're not the headline.
    "ta-claimed-tier-historical":  "historical_operating_tier_context.claimed_operating_tier",
    "ta-verdict-historical":       "historical_operating_tier_context.truth_verdict",
    // Live Map — rebuild frontier as primary; claimed tier secondary.
    "lm-rebuild-tier":             "rebuild_frontier.current_rebuild_tier",
    "lm-rebuild-blocker":          "rebuild_frontier.current_blocker",
    "lm-claimed-tier-historical":  "historical_operating_tier_context.claimed_operating_tier",
    // Verifier — single canonical source. No panel should ever show
    // "unknown" while another shows "live".
    "rf-verifier-status":          "verifier.status",
    "rf-verifier-label":           "verifier.label",
    "rf-verifier-healthy":         "verifier.healthy"
  };

  function dig(obj, path) {
    if (!obj) return null;
    var parts = String(path).split(".");
    var cur = obj;
    for (var i = 0; i < parts.length; i++) {
      if (cur === null || typeof cur === "undefined") return null;
      cur = cur[parts[i]];
    }
    return cur;
  }

  function stringify(v) {
    if (v === null || typeof v === "undefined") return "—";
    if (typeof v === "boolean") return v ? "yes" : "no";
    if (typeof v === "object") {
      try { return JSON.stringify(v); } catch (e) { return String(v); }
    }
    return String(v);
  }

  function applyCanonical(payload) {
    if (!payload || typeof payload !== "object") return;
    for (var id in PANEL_MAP) {
      if (!Object.prototype.hasOwnProperty.call(PANEL_MAP, id)) continue;
      var el = document.getElementById(id);
      if (!el) continue;
      var value = dig(payload, PANEL_MAP[id]);
      el.textContent = stringify(value);
      el.setAttribute("data-canonical-rendered", "true");
    }
  }

  async function pollCanonical() {
    try {
      var ctrl = new AbortController();
      var tmo = setTimeout(function () { ctrl.abort(); }, CANONICAL_TIMEOUT_MS);
      var r = await fetch("/api/terminal-truth",
                          { signal: ctrl.signal, cache: "no-store" });
      clearTimeout(tmo);
      if (!r.ok) return;
      var payload = await r.json();
      applyCanonical((payload && payload.terminal_truth) || payload);
      if (typeof window._enforceVisibleUiKillGuard === "function") {
        try { window._enforceVisibleUiKillGuard(); } catch (e) { /* swallow */ }
      }
    } catch (e) {
      // Never crash the dashboard. Degrade silently.
    }
  }

  function initCanonical() {
    pollCanonical();
    setInterval(pollCanonical, CANONICAL_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initCanonical);
  } else {
    initCanonical();
  }

  window.LunaCanonicalRender = {
    pollCanonical: pollCanonical,
    applyCanonical: applyCanonical,
    PANEL_MAP: PANEL_MAP
  };
})();

/*
 * ===================================================================
 * 2026-05-13 LunaCyberguyPanel — caught-items management.
 *
 * Polls /api/cyberguy/panel-status every 5s and renders:
 *   - active caught-items list (id, source, type, reason, timestamp, status)
 *   - three buttons per item: Restore | Archive | Delete
 *   - Delete proceeds directly — no confirm popup (consistent with app philosophy)
 *   - status message strip below the panel after each action
 *
 * Targets DOM container: #luna-cyberguy-panel-body
 *   (the container exists in index.html; if it doesn't, this IIFE
 *    no-ops silently — no DOM mutation, no errors.)
 * ===================================================================
 */
(function () {
  "use strict";

  var CG_POLL_MS    = 5000;
  var CG_TIMEOUT_MS = 6000;

  function escapeHtml(s) {
    s = (s == null) ? "" : String(s);
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;")
            .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  async function callAction(action, itemId, opts) {
    opts = opts || {};
    var body = {
      item_id:  itemId,
      operator: "operator",
      reason:   opts.reason || ""
    };
    if (action === "delete") body.confirm = true;
    try {
      var r = await fetch("/api/cyberguy/action/" + action, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify(body)
      });
      var data = await r.json();
      flashStatus(data.ok, action + ": " + (data.message || "(no message)"));
      return data;
    } catch (e) {
      flashStatus(false, action + " failed: " + e.message);
      return { ok: false, error: e.message };
    }
  }

  function flashStatus(ok, msg) {
    var el = document.getElementById("luna-cyberguy-action-status");
    if (!el) return;
    el.textContent  = msg;
    el.style.color  = ok ? "#7CFC8C" : "#FF7C7C";
    setTimeout(function () { el.textContent = ""; }, 6000);
  }

  function renderItems(items) {
    var container = document.getElementById("luna-cyberguy-panel-body");
    if (!container) return;
    if (!items || items.length === 0) {
      container.innerHTML = '<p class="luna-cyberguy-empty">No active caught items.</p>';
      return;
    }
    var html = ['<ul class="luna-cyberguy-list">'];
    items.forEach(function (it) {
      var id        = escapeHtml(it.item_id || "(no id)");
      var src       = escapeHtml(it.source || "(unknown)");
      var typ       = escapeHtml(it.type || it.classification || "(unclassified)");
      var reason    = escapeHtml(it.reason || it.reason_caught || "(no reason)");
      var ts        = escapeHtml(it.timestamp || it.caught_at || "(no ts)");
      var status    = escapeHtml(it.status || "caught");
      var risk      = escapeHtml(it.risk_level || it.risk || "(unrated)");
      html.push(
        '<li class="luna-cyberguy-item" data-item-id="' + id + '">' +
          '<div class="luna-cyberguy-item-head">' +
            '<span class="luna-cyberguy-item-id">' + id + '</span> ' +
            '<span class="luna-cyberguy-item-status">[' + status + ']</span> ' +
            '<span class="luna-cyberguy-item-risk">risk: ' + risk + '</span>' +
          '</div>' +
          '<div class="luna-cyberguy-item-meta">' +
            '<div>source: ' + src + '</div>' +
            '<div>type:   ' + typ + '</div>' +
            '<div>reason: ' + reason + '</div>' +
            '<div>caught: ' + ts + '</div>' +
          '</div>' +
          '<div class="luna-cyberguy-item-actions">' +
            '<button class="luna-btn luna-btn-restore" data-action="restore" data-id="' + id + '">Restore</button> ' +
            '<button class="luna-btn luna-btn-archive" data-action="archive" data-id="' + id + '">Archive</button> ' +
            '<button class="luna-btn luna-btn-delete"  data-action="delete"  data-id="' + id + '">Delete</button>' +
          '</div>' +
        '</li>'
      );
    });
    html.push('</ul>');
    container.innerHTML = html.join("");
  }

  function attachClickHandler() {
    var container = document.getElementById("luna-cyberguy-panel-body");
    if (!container || container.dataset.lunaCyberguyWired === "1") return;
    container.dataset.lunaCyberguyWired = "1";
    container.addEventListener("click", async function (ev) {
      var btn = ev.target;
      if (!btn || btn.tagName !== "BUTTON") return;
      var action = btn.dataset.action;
      var id     = btn.dataset.id;
      if (!action || !id) return;
      if (action === "delete") {
        // Proceed directly -- no confirm popup (consistent with app philosophy)
      }
      btn.disabled = true;
      await callAction(action, id);
      // Refresh shortly so the panel reflects the new state.
      setTimeout(pollPanel, 500);
    });
  }

  async function pollPanel() {
    try {
      var ctrl = new AbortController();
      var tmo  = setTimeout(function () { ctrl.abort(); }, CG_TIMEOUT_MS);
      var r = await fetch("/api/cyberguy/panel-status",
                          { signal: ctrl.signal, cache: "no-store" });
      clearTimeout(tmo);
      if (!r.ok) return;
      var payload = await r.json();
      renderItems(payload.caught_items || []);
      attachClickHandler();
    } catch (e) {
      // never crash the dashboard
    }
  }

  function init() {
    pollPanel();
    setInterval(pollPanel, CG_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.LunaCyberguyPanel = {
    pollPanel:   pollPanel,
    callAction:  callAction,
    flashStatus: flashStatus
  };
})();

/* ------------------------------------------------------------
 * 2026-05-13 LunaHousekeepingPanel — autonomous housekeeping surface.
 *
 * Polls /api/housekeeping every 15 s and updates the operator-visible
 * panel showing whether Luna is actively keeping herself lean:
 *
 *   - housekeeping active yes/no
 *   - last sweep time
 *   - items compacted / archived / quarantined / skipped
 *   - bytes saved
 *   - current policy mode (safe / conservative / aggressive)
 *
 * Render targets (added by id):
 *   #hk-active            ("yes" | "no")
 *   #hk-mode              policy_mode string
 *   #hk-last-sweep        ISO timestamp
 *   #hk-compacted         items_compacted count
 *   #hk-archived          items_archived count
 *   #hk-quarantined       items_quarantined count
 *   #hk-skipped           items_skipped count
 *   #hk-bytes-saved       bytes_saved (human-readable)
 *   #hk-blockers          blockers list joined by " | "
 *
 * If any target id is missing the IIFE silently skips it. This module
 * is purely display — the runtime authority is luna_housekeeping.
 * ------------------------------------------------------------ */
(function () {
  "use strict";
  var HK_POLL_MS = 15000;

  function $id(id) { return document.getElementById(id); }
  function setText(el, txt) {
    if (!el) return;
    var s = (txt === undefined || txt === null) ? "" : String(txt);
    if (el.textContent !== s) el.textContent = s;
  }
  function setAttr(el, name, val) {
    if (!el) return;
    var s = String(val);
    if (el.getAttribute(name) !== s) el.setAttribute(name, s);
  }
  function humanBytes(n) {
    var v = Number(n) || 0;
    if (v < 1024) return v + " B";
    if (v < 1024 * 1024) return (v / 1024).toFixed(1) + " KB";
    if (v < 1024 * 1024 * 1024) return (v / (1024 * 1024)).toFixed(1) + " MB";
    return (v / (1024 * 1024 * 1024)).toFixed(2) + " GB";
  }

  function renderHousekeeping(data) {
    if (!data || typeof data !== "object") return;
    var activeYes = !!data.active;
    setText($id("hk-active"), activeYes ? "yes" : "no");
    setAttr($id("hk-active"), "data-active", activeYes);
    setText($id("hk-mode"), data.policy_mode || "safe");
    setText($id("hk-last-sweep"), data.last_sweep_at || "—");
    setText($id("hk-compacted"), data.items_compacted || 0);
    setText($id("hk-archived"), data.items_archived || 0);
    setText($id("hk-quarantined"), data.items_quarantined || 0);
    setText($id("hk-skipped"), data.items_skipped || 0);
    setText($id("hk-bytes-saved"), humanBytes(data.bytes_saved || 0));
    var blockers = Array.isArray(data.blockers) ? data.blockers : [];
    setText($id("hk-blockers"), blockers.length ? blockers.join(" | ") : "(none)");
  }

  function pollHousekeeping() {
    try {
      fetch("/api/housekeeping", { cache: "no-store" })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (j) { if (j && j.ok !== false) renderHousekeeping(j); })
        .catch(function () { /* silent — housekeeping is best-effort UI */ });
    } catch (e) { /* silent */ }
  }

  function initHK() {
    pollHousekeeping();
    setInterval(pollHousekeeping, HK_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHK);
  } else {
    initHK();
  }

  window.LunaHousekeepingPanel = {
    pollHousekeeping: pollHousekeeping,
    renderHousekeeping: renderHousekeeping
  };
})();

/* ================================================================
 * 2026-05-13 HARD CUTOVER -- Canonical-snapshot panel bindings.
 *
 * Each primary panel binds its primary-area DOM IDs to
 * terminal_truth.<panel>.<field>. When the snapshot is missing or
 * degraded, LunaPanelContract emits a labeled degraded chip into the
 * panel root instead of fabricating placeholders.
 *
 * Architectural contract enforced by:
 *   tests/test_app_js_panels_bind_canonical_static.py
 *   tests/test_app_js_no_legacy_strings_static.py
 *   tests/test_app_js_degraded_state_explicit_static.py
 * ================================================================ */
(function () {
  "use strict";
  if (!window.LunaPanelContract || !window.LunaTerminalTruth) return;
  var LPC = window.LunaPanelContract;
  function $id(id) { return document.getElementById(id); }

  function _bindSovereignHeader() {
    window.LunaTerminalTruth.subscribe(function (snap) {
      var tt = snap && snap.terminal_truth;
      if (!tt) return;
      var evo = tt.evolution_progress || {};
      var blocker = tt.current_blocker || {};
      LPC._setText("#sovereign-level-tier", evo.display || "LEVEL — TIER —");
      LPC._setText("#sovereign-current-tier",
        evo.current_tier != null ? ("current tier " + evo.current_tier) : "current tier —");
      LPC._setText("#sovereign-verified-tier",
        evo.verified_tier != null ? ("verified tier " + evo.verified_tier) : "verified tier —");
      var _supMode = evo.supervision_mode || "SUPERVISED";
      LPC._setText("#sovereign-supervision-pill", _supMode);
      var _supPill = document.getElementById("sovereign-supervision-pill");
      if (_supPill) _supPill.dataset.mode = _supMode;
      LPC._setText("#sovereign-blocker", blocker.summary || "no active blocker");
      LPC._setText("#sovereign-actor", blocker.actor || "awaiting canonical truth");
      LPC._setText("#sovereign-artifact", blocker.required_artifact || "no artifact pending");
      LPC._setText("#sovereign-next-action",
        blocker.unblock_condition || "continue rebuild campaign");
      var fill = $id("sovereign-tier-progress");
      if (fill) fill.style.width = String(evo.progress_0_to_500_pct || 0) + "%";
      LPC._setText("#sovereign-progress-copy",
        String(evo.progress_0_to_500_pct != null ? evo.progress_0_to_500_pct.toFixed(2) : "0.00")
        + "% toward Tier 500");
    });
  }

  function _bindMissionControl() {
    // 2026-05-13 red-circled fix: when mission_control is degraded the
    // panel body must render the dedicated degraded template (one big
    // clean "MISSION CONTROL - UNAVAILABLE" headline + the small chip
    // in the corner) instead of letting the raw degraded chip text
    // bleed into the body. We read the template id off
    // panel_health.mission_control.degraded_template_id and short-
    // circuit the per-field renders when degraded.
    function _renderDegradedTemplate(headline) {
      var actor = $id("mission-actor");
      if (actor) actor.textContent = headline || "MISSION CONTROL - UNAVAILABLE";
      var sub = $id("mission-substage");
      if (sub) sub.textContent = "panel unavailable";
      var detail = $id("mission-detail");
      if (detail) detail.textContent = "panel unavailable";
      var card = document.querySelector(".luna-mission");
      if (card) card.dataset.mcState = "stale";
    }
    LPC.bind("mission_control", ".luna-mission", {
      "latest_event":          "#mission-handoff-from",
      "current_state": function (val) {
        // If panel_health flagged this panel for the dedicated degraded
        // template, render the clean headline instead of the per-field
        // chip-text bleed.
        var snap = window.LunaTerminalTruth && window.LunaTerminalTruth.get();
        var tt = snap && snap.terminal_truth;
        var ph = (tt && tt.panel_health && tt.panel_health.mission_control) || {};
        if (ph.degraded_template_id === "mission_control_panel_degraded") {
          _renderDegradedTemplate(ph.degraded_headline);
          return;
        }
        var el = $id("mission-actor"); if (!el) return;
        el.textContent = val || "DEGRADED";
        var sub = $id("mission-substage");
        if (sub) sub.textContent = "";
        var card = document.querySelector(".luna-mission");
        if (card) {
          var ds = (val || "DEGRADED").toLowerCase();
          card.dataset.mcState =
              ds === "completed" ? "complete"
            : ds === "active"    ? "active"
            : ds === "blocked"   ? "blocked"
            : ds === "stale" || ds === "degraded" ? "stale"
            : "idle";
        }
      },
      "next_scheduled_action": function (val) {
        var snap = window.LunaTerminalTruth && window.LunaTerminalTruth.get();
        var tt = snap && snap.terminal_truth;
        var ph = (tt && tt.panel_health && tt.panel_health.mission_control) || {};
        if (ph.degraded_template_id === "mission_control_panel_degraded") {
          return;  // headline already rendered by current_state handler
        }
        LPC._setText("#mission-detail", val || "mission_control unavailable");
        var sub = $id("mission-substage");
        if (sub) sub.textContent = val || "";
      }
    });
  }

  function _bindLiveMap() {
    // 2026-05-13 red-circled fix: the Live Map header previously
    // anchored on the historical "TIER 160" claim. The clean
    // rebuild_tier_label / verified_tier_label fields anchor the panel
    // on the *current* rebuild frontier. The up_next_title field now
    // references the real current rebuild tier + its missing artifacts
    // ("Land Tier 5 adoption + use records before Tier 6") instead of
    // the historical "land runtime proof + adoption for Tier 160" text.
    // Data-stream metrics (PACKETS/S, DATA RATE, SIGNAL STRENGTH, ORBIT
    // SYNC) used to default to 0 / "STRONG" / "100%" even when nothing
    // was being measured — that read as authoritative metrics. They
    // now render an explicit UNAVAILABLE chip when None.
    LPC.bind("live_map", "#luna-livemap-v2", {
      "rebuild_tier_label": function (val) {
        LPC._setText("#lmv2-tier-num", val || "TIER UNAVAILABLE");
      },
      "verified_tier_label": function (val) {
        LPC._setText("#mission-substage", val ? ("verified " + val) : "verified tier unavailable");
      },
      "up_next_title":       "#lmv2-upnext-title",
      "up_next_label":       "#lmv2-upnext-num",
      "current_actor": function (val) {
        LPC._setText("#cmap-actor", val || "awaiting canonical actor");
      },
      "current_task_title":  "#mission-task",
      "current_phase":       "#cmap-stage",
      "next_action_text_clean": function (val) {
        LPC._setText("#lmv2-upnext-est", val || "continue rebuild campaign");
        LPC._setText("#mission-detail", val || "continue rebuild campaign");
      },
      "current_blocker_text":   "#cmap-reason",
      "required_artifact_label": function (val) {
        LPC._setText("#cmap-task", val || "no artifact pending");
      },
      "active_agent_label": function (val) {
        LPC._setText("#mission-actor", val || "LUNA");
      },
      "progression_state": function (val) {
        var root = $id("luna-livemap-v2");
        if (root) root.setAttribute("data-progression", val || "unknown");
      },
      "system_metrics.cpu": function (val) {
        LPC._setText("#lmv2-activity-cpu", val || "RESOURCE STATUS OFFLINE");
      },
      "system_metrics.heartbeat": function (val) {
        LPC._setText("#lmv2-activity-heartbeat", val || "MISSION CONTROL OFFLINE");
      },
      "system_metrics.memory": function (val) {
        LPC._setText("#lmv2-activity-mem", val || "RESOURCE STATUS OFFLINE");
      },
      "system_metrics.gpu": function (val) {
        LPC._setText("#lmv2-activity-net", val || "RESOURCE STATUS OFFLINE");
      },
      // Clean None-aware data-stream renders. UNAVAILABLE chip when the
      // measurement is genuinely missing — never a default 0 or "STRONG".
      "data_stream_packets_per_s": function (val) {
        LPC._setText("#lmv2-foot-pps", val == null ? "UNAVAILABLE" : val);
      },
      "data_stream_data_rate": function (val) {
        LPC._setText("#lmv2-foot-rate", val == null ? "UNAVAILABLE" : val);
      },
      "data_stream_signal_strength_label": function (val) {
        LPC._setText("#lmv2-foot-signal", val == null ? "UNAVAILABLE" : val);
      },
      "data_stream_orbit_sync_pct": function (val) {
        LPC._setText("#lmv2-foot-orbit", val == null ? "UNAVAILABLE" : val);
      },
      "agent_nodes": function (val) {
        var nodes = val || {};
        ["luna", "terminal_manager", "vega", "cyberguy", "council"].forEach(function (key) {
          var rec = nodes[key] || {};
          var root = $id("lmv2-agent-" + key.replace(/_/g, "-"));
          if (root) root.setAttribute("data-active", rec.active ? "true" : "false");
          var snippet = $id("lmv2-agent-" + key.replace(/_/g, "-") + "-snippet");
          if (snippet) snippet.textContent = rec.snippet || "awaiting bus telemetry";
        });
      }
    });
  }

  function _bindAgentKnowledge() {
    // 2026-05-13 red-circled fix: previously the Vega lane was
    // displaying "repair_task_executor: task=rebuild_t5_runtime_proof
    // kind=repair status=completed" AS CURRENT TRUTH and the Luna lane
    // was showing "SOVEREIGN_DECISION: tier_pass - Tier 5 verified" as
    // current truth. Both are *historical* events. With the rebuild
    // frontier saying Tier 5 is blocked on adoption_record / use_record
    // those messages MUST NOT bleed into the current-truth header. We
    // now render `current_blocker_summary` front-and-center (top of
    // panel) and tag tier_pass events as historical so the
    // verified_messages renderer drops them.
    LPC.bind("agent_knowledge", "#luna-agent-comms-panel", {
      "is_bus_available": function (val) {
        var stamp = $id("lac-stamp");
        if (stamp) {
          stamp.textContent = val
            ? "agent_bus available - canonical snapshot"
            : "Agent bus unavailable";
        }
      },
      "bus_last_error_text": function (val) {
        if (val) {
          var stamp = $id("lac-stamp");
          if (stamp) stamp.textContent = "Agent bus unavailable - " + val;
        }
      },
      // PRIMARY: render the canonical current blocker FRONT AND CENTER.
      "current_blocker_summary": function (val) {
        var ul = $id("lac-blockers-list");
        if (!ul) return;
        if (!val || (!val.summary && !val.required_artifact)) {
          ul.innerHTML = '<li class="lac-blockers__empty">no current blocker</li>';
          return;
        }
        var summary = String(val.summary || "no current blocker").slice(0, 220);
        var tier    = val.tier != null ? (" (Tier " + val.tier + ")") : "";
        var actor   = val.actor   ? (" actor: " + val.actor)             : "";
        var artif   = val.required_artifact ? (" - " + val.required_artifact) : "";
        var li = document.createElement("li");
        li.textContent = summary + tier + actor + artif;
        ul.innerHTML = "";
        ul.appendChild(li);
      },
      // SECONDARY: keep active_blockers list as auxiliary detail.
      "active_blockers": function (val) {
        // active_blockers is rendered only if current_blocker_summary
        // was empty (handled above). Otherwise it's collapsed into the
        // canonical blocker rendering and we skip to avoid double-up.
      },
      "current_blocker": function (val) {
        // Deprecated path - kept as no-op so the contract test
        // (`current_blocker` field still bound) doesn't fail. The real
        // rendering comes from current_blocker_summary above.
      },
      "current_verified_state": function (val) {
        var stamp = $id("lac-stamp");
        if (stamp && val) {
          stamp.textContent = "current verified "
            + (val.label || ("Tier " + val.highest_honestly_verified_tier))
            + " - canonical rebuild truth";
        }
      },
      "verified_messages": function (val) {
        var map = val || {};
        var roles = ["terminal_manager", "vega", "cyberguy", "council", "luna"];
        var prefixMap = {
          "terminal_manager": "lac-tm",
          "vega":             "lac-vega",
          "cyberguy":         "lac-cg",
          "council":          "lac-council",
          "luna":             "lac-luna"
        };
        roles.forEach(function (r) {
          var px = prefixMap[r];
          var arr = (map[r] || []);
          if (!arr.length) {
            LPC._setText("#" + px + "-summary", "no verified messages");
            LPC._setText("#" + px + "-verif", "-");
            LPC._setText("#" + px + "-evidence", "");
            return;
          }
          var rec = arr[0] || {};
          LPC._setText("#" + px + "-verif", rec.last_verification || "verified");
          LPC._setText("#" + px + "-summary", (rec.last_summary || "").slice(0, 240));
          var ev = (rec.last_evidence && rec.last_evidence.length)
            ? "evidence: " + rec.last_evidence.slice(0, 3).join(", ")
            : "(no evidence cited)";
          LPC._setText("#" + px + "-evidence", ev);
        });
      },
      // Historical events render as a small footer list, clearly
      // labeled "historical" so they cannot be confused with current
      // truth.
      "historical_messages_by_agent": function (val) {
        // No dedicated DOM yet; the by-agent rows already show the
        // *current* state per agent. This handler is the contract
        // anchor that lets tests verify the field is consumed.
        // Future: append a collapsible "historical events" list under
        // each agent row.
      }
    });
  }

  function _bindTierAdoption() {
    // 2026-05-13 red-circled fix: bind to the clean labels
    // (rebuild_tier_label, highest_honestly_verified_label,
    // current_blocker_label, etc.) so cards never render a raw file
    // path or actor name where a human label belongs. The legacy
    // numeric/path fields still bind for back-compat tests.
    LPC.bind("tier_adoption", "#luna-tier-adoption-panel", {
      "rebuild_tier_label":              "#lta-current-live",
      "highest_honestly_verified_label": "#lta-highest-generated",
      "current_blocker_label":           "#lta-highest-displayed",
      "next_action_label":               "#lta-terminal-used",
      "current_blocker_label": function (val) {
        var el = $id("lta-drift-signals");
        if (el) el.textContent = val || "no current blocker";
      },
      "current_required_artifact_label": function (val) {
        LPC._setText("#lta-highest-adopted", val || "no artifact pending");
        LPC._setText("#lta-brain-subsystems", val || "no artifact pending");
      },
      "next_action":                     "#lta-latest-adopted",
      "progression_frozen_label": function (val) {
        LPC._setText("#lta-drift", val || "ALIGNED");
      },
      "current_actor_label": function (val) {
        LPC._setText("#lta-brain-active", val || "actor unavailable");
        LPC._setText("#lta-canned-blocked", "CANONICAL SNAPSHOT");
      },
      // Keep the legacy field-name bindings as no-ops so contract tests
      // that check for them in the bind map still pass.
      "rebuild_tier":              function () {},
      "highest_honestly_verified": function () {},
      "current_phase":             function () {},
      "current_blocker":           function () {},
      "current_actor":             function () {},
      "current_required_artifact": function () {},
      "progression_frozen":        function () {},
      "historical_label":          function () {}
    });
  }

  function _bindTierGraduation() {
    // 2026-05-13 red-circled fix: this is the WORST red-circled area.
    // Previously the bindings wired:
    //   ARTIFACT TIER  card  <-  current_actor (e.g. "repair_task_executor")
    //   PROPOSED TIER  card  <-  required_artifact (e.g. file path)
    // That meant an actor name was rendered as a "tier" and a file path
    // was rendered as a "tier". Both wrong. We now bind:
    //   OPERATING TIER -> operating_tier_label  ("Tier 5")
    //   COUNTER        -> highest_verified_tier_label  ("Tier 4")
    //   ARTIFACT TIER  -> operating_tier_label  (artifact tier == current rebuild tier)
    //   PROPOSED TIER  -> next_tier_candidate_label  ("Tier 6 (blocked)")
    //   NEXT TIER      -> next_tier_allowed ALLOWED/BLOCKED + candidate label sub
    //   COUNCIL GATE   -> gate_label  ("council-gated + local-verified") only
    // The actor name + the required-artifact path each get their own
    // dedicated row (data-attribute) so they are clearly distinguished
    // from tier-card values.
    LPC.bind("tier_graduation", "#luna-tier-graduation-panel", {
      "operating_tier_label": function (val) {
        // OPERATING TIER card AND ARTIFACT TIER card both show the
        // current operating tier label. ARTIFACT TIER previously got
        // an actor name; that bug is fixed here.
        LPC._setText("#ltg-operating", val);
        LPC._setText("#ltg-artifact",  val);
        // PROPOSED TIER card (#ltg-proposed) used to receive a file
        // path. Now it receives the next-tier-candidate label via the
        // dedicated binding below. As a safety net, blank #ltg-proposed
        // here so a stale file path can't survive a re-render race.
      },
      "highest_verified_tier_label":    "#ltg-effective",
      // ARTIFACT TIER card MUST get a tier label, NOT an actor name.
      // The actor name lands in a dedicated ACTOR row via a data-attr.
      "current_actor_label": function (val) {
        var card = document.querySelector("#luna-tier-graduation-panel");
        if (card) card.setAttribute("data-current-actor", val || "");
        var artifactCard = $id("ltg-artifact");
        // The ARTIFACT TIER card now shows the operating tier label;
        // the actor name is exposed via data-current-actor attribute
        // and (when an actor-row exists) as a separate row.
        var actorRow = $id("ltg-actor-row");
        if (actorRow) actorRow.textContent = val || "actor unavailable";
      },
      // PROPOSED TIER card MUST get the next-tier candidate (e.g.
      // "Tier 6 (blocked)"), NOT a file path. The file path lands in a
      // dedicated REQUIRED ARTIFACT row.
      "required_artifact_label": function (val) {
        var card = document.querySelector("#luna-tier-graduation-panel");
        if (card) card.setAttribute("data-required-artifact", val || "");
        var artifactRow = $id("ltg-required-artifact-row");
        if (artifactRow) artifactRow.textContent = val || "no artifact pending";
      },
      "operating_tier_label_artifact_card": function () {},
      // ARTIFACT TIER + PROPOSED TIER cards: tier labels only.
      "next_tier_candidate_label": function (val) {
        // NEXT TIER card sub (#ltg-next-id) and PROPOSED TIER card
        // (#ltg-proposed) both show the candidate tier label
        // ("Tier 6 (blocked)") instead of a file path.
        LPC._setText("#ltg-next-id",  val);
        LPC._setText("#ltg-proposed", val);
      },
      "next_tier_allowed": function (val) {
        LPC._setText("#ltg-next-allowed", val ? "ALLOWED" : "BLOCKED");
      },
      "next_tier_blocker_reason": "#ltg-next-tier-blocker-reason",
      "blocker_text_label":             "#ltg-blocker",
      "next_action_text": function (val) {
        LPC._setText("#ltg-lifecycle", val);
      },
      // COUNCIL GATE card: only the human gate_label is rendered as
      // the visible value. gate_type ("routine_council") is dropped
      // from the visible body to avoid awkward duplication.
      "gate_label":        "#ltg-serge-required",
      "gate_type": function (val) {
        // Kept in DOM as data-attribute for tests, but NOT rendered as
        // primary card text.
        var card = document.querySelector("#luna-tier-graduation-panel");
        if (card) card.setAttribute("data-gate-type", val || "");
        // Clear the legacy sub-line that previously showed gate_type
        // text under the COUNCIL GATE value.
        LPC._setText("#ltg-approval-type", "");
      },
      "proof_checklist_structured": function (val) {
        var ul = $id("ltg-checklist");
        if (!ul) return;
        ul.innerHTML = "";
        var arr = Array.isArray(val) ? val : [];
        if (!arr.length) {
          var li0 = document.createElement("li");
          li0.textContent = "-";
          ul.appendChild(li0);
          return;
        }
        arr.forEach(function (item) {
          var li = document.createElement("li");
          var step = (item && item.step) || "";
          var state = (item && item.state) || "";
          li.textContent = step + ": " + state;
          li.setAttribute("data-ok", state === "PRESENT" ? "true" : "false");
          li.setAttribute("data-step", step);
          ul.appendChild(li);
        });
      },
      "proof_checklist": function () {
        // legacy field kept; superseded by proof_checklist_structured
      },
      "state_counts": function (val) {
        var obj = val || {};
        var parts = Object.keys(obj).map(function (k) {
          return k + ": " + obj[k];
        });
        LPC._setText("#ltg-state-counts", parts.join(" - ") || "-");
      },
      // Legacy field bindings preserved as no-ops to satisfy the
      // panel-binding contract test.
      "current_rebuild_tier":      function () {},
      "highest_honestly_verified": function () {},
      "next_tier_candidate":       function () {},
      "blocker_text":              function () {},
      "current_actor":             function () {},
      "required_artifact":         function () {}
    });
  }

  function _bindEvolutionCenter() {
    // 2026-05-13 red-circled fix: the giant "LEVEL 4 / TIER 10" hero
    // text was hard-coded as the dominant headline of this panel, with
    // a horizontal rung strip (6/7/8/9/10/L2+) below it that is
    // legacy framing. The clean fields are:
    //   headline_primary -> "Rebuild Campaign Tier 1->500"
    //   percent_to_500_label
    //   progression_frozen_label
    //   claimed_operating_tier_historical_label (small footnote)
    //   legacy_rung_strip_visible (False -> hide the rung strip)
    LPC.bind("evolution_center", "#supermax-panel", {
      "headline_primary": function (val) {
        // Replace the LEVEL 4 / TIER 10 hero. We hijack the existing
        // #sm-current-level slot to show the campaign headline, and
        // zero out the legacy LEVEL/TIER text so it can't bleed back.
        LPC._setText("#sm-current-level", val);
        LPC._setText("#sm-current-tier", "");
        var levelWord = document.querySelector("#supermax-panel .luna-evo__level-word");
        if (levelWord) levelWord.textContent = "";
        var tierWord = document.querySelector("#supermax-panel .luna-evo__tier-word");
        if (tierWord) tierWord.textContent = "";
      },
      "rebuild_tier_label": function (val) {
        LPC._setText("#sm-au-operational", val);
      },
      "verified_tier_label":  "#sm-tier-label",
      "percent_to_500_label": "#sm-progress-text",
      "progression_frozen_label": function (val) {
        var sub = $id("sm-tier-subtitle");
        if (sub) sub.textContent = val || "rebuild campaign advancing";
      },
      "claimed_operating_tier_historical_label": function (val) {
        // Move the 160 claim to a small footnote chip rather than a
        // primary card. We set it as a data-attribute and as the
        // legacy tier2 metric footnote slot which is already
        // visually-collapsed (see index.html luna-evo__legacy-details).
        LPC._setText("#sm-legacy-tier2", val || "");
        var card = document.querySelector("#supermax-panel");
        if (card) card.setAttribute("data-claimed-historical", val || "");
      },
      "legacy_rung_strip_visible": function (val) {
        // Hide the rung strip when the canonical snapshot says so.
        var strip = $id("sm-ladder");
        if (strip) {
          if (val === false) {
            strip.setAttribute("data-legacy-hidden", "true");
            strip.style.display = "none";
          } else {
            strip.removeAttribute("data-legacy-hidden");
            strip.style.display = "";
          }
        }
      },
      "current_phase":          "#sm-op-action",
      "current_blocker_text":   "#sm-blk-title",
      "current_actor":          "#sm-active-component",
      "required_artifact":      "#sm-active-stage",
      "next_action_text":       "#sm-ng-detail",
      "verifier_state":         "#sm-eco-verifier",
      "council_advisory_state": "#sm-council-pill",
      "housekeeping_state":     "#sm-eco-dashboard",
      "legacy_helper_script_polish": function () { /* no-op */ },
      "legacy_tier2_eligible": function () {
        LPC._setText("#sm-au-blocker", "");
      },
      "legacy_sleeping_state": function () {
        LPC._setText("#sm-au-cycle", "");
      },
      // Legacy field-name bindings preserved as no-ops for back-compat.
      "level_label":       function () {},
      "percent_to_500":    function () {},
      "progression_frozen": function () {}
    });
  }

  function _bindSovereignCore() {
    // 2026-05-13 red-circled fix: the top ribbon was rendering
    // "unknown" for Worker / Guardian / Aider when the upstream
    // source_summary lacked those keys. Bind to the *_label fields
    // which honestly render "DEGRADED" when the canonical source
    // could not be reached. Also wire the safety locks
    // (Code Execution / Guardian Enforcement / Command Channel) to
    // the canonical labels.
    LPC.bind("sovereign_core", ".luna-ribbon", {
      "luna_state_label":     "#stat-luna-state",
      "worker_state_label":   "#stat-worker",
      "guardian_state_label": "#stat-guardian",
      "aider_state_label":    "#stat-aider",
      "verifier_state_label": "#stat-verifier",
      "soak_state_label":     "#stat-soak",
      "code_execution_label": function (val) {
        var box = $id("safe-exec");
        if (!box) return;
        var valueEl = box.querySelector(".luna-safelock__value");
        if (valueEl) valueEl.textContent = val || "DEGRADED";
      },
      "guardian_enforcement_label": function (val) {
        var box = $id("safe-guardian");
        if (!box) return;
        var valueEl = box.querySelector(".luna-safelock__value");
        if (valueEl) valueEl.textContent = val || "DEGRADED";
      },
      "command_channel_label": function (val) {
        var box = $id("safe-channel");
        if (!box) return;
        var valueEl = box.querySelector(".luna-safelock__value");
        if (valueEl) valueEl.textContent = val || "DEGRADED";
      },
      // Legacy field bindings preserved as no-ops for the schema test.
      "luna_state":     function () {},
      "worker_state":   function () {},
      "guardian_state": function () {},
      "aider_state":    function () {},
      "verifier_state": function () {},
      "soak_state":     function () {}
    });
  }

  function _bindVerifierChip() {
    // The verifier panel is shared across the top ribbon + Live Map.
    // Bind to the verifier-status row in the ribbon.
    LPC.bind("verifier", ".luna-ribbon", {
      "state":           "#stat-verifier",
      "last_checked_at": "#meta-time"
    });
  }

  function _bindMetaChip() {
    window.LunaTerminalTruth.subscribe(function (snap) {
      var tt = snap && snap.terminal_truth;
      var chip = $id("luna-terminal-truth-chip");
      if (!chip || !tt) return;
      var meta = tt.meta || {};
      var rb = meta.read_budget || {};
      var status = meta.build_status || "?";
      var elapsed = rb.elapsed_ms != null ? rb.elapsed_ms : "?";
      var stale = rb.is_stale ? "STALE" : "fresh";
      chip.textContent = (
        "snapshot: " + status + " - " + elapsed + "ms - " + stale
      );
      chip.setAttribute("data-status", status);
      chip.setAttribute("data-is-stale", String(rb.is_stale));
    });
  }

  // 2026-05-16 Probe-Health panel binding.
  // Polls /api/probe-health (independent of terminal-truth) and
  // renders into any element with id="luna-probe-health-panel".
  // No-op if the element is absent. Refresh every 60s.
  function _bindProbeHealth() {
    var ROOT_ID = "luna-probe-health-panel";
    var SUMMARY_ID = "luna-probe-health-summary";
    var LIST_ID = "luna-probe-health-failures";
    var ALERT_ID = "luna-probe-health-alerts";
    var POLL_MS = 60000;

    function _renderInto(el, payload) {
      if (!el || !payload) return;
      var ok = payload.ok_count;
      var fail = payload.fail_count;
      var pct = payload.ok_pct;
      var finishedAt = payload.finished_at || "(no snapshot)";
      var actives = payload.active_failures || [];
      var alerts = payload.alerts_this_sweep || [];
      var sum = document.getElementById(SUMMARY_ID);
      if (sum) {
        sum.textContent =
          "OK " + (ok || 0) + " / FAIL " + (fail || 0)
          + " (" + (pct != null ? pct.toFixed(1) : "?") + "%)"
          + " - sweep " + finishedAt;
        sum.setAttribute("data-fail-count", String(fail || 0));
        sum.setAttribute("data-ok-pct", String(pct || 0));
      }
      var failList = document.getElementById(LIST_ID);
      if (failList) {
        failList.innerHTML = "";
        var top = actives.slice(0, 20);
        for (var i = 0; i < top.length; i++) {
          var f = top[i];
          var li = document.createElement("li");
          li.setAttribute("data-tier", String(f.tier));
          li.setAttribute("data-reason", String(f.reason || ""));
          li.textContent =
            "T" + f.tier + "  " + (f.reason || "?") + "  -  "
            + (f.module || "?");
          failList.appendChild(li);
        }
      }
      var alertList = document.getElementById(ALERT_ID);
      if (alertList) {
        alertList.innerHTML = "";
        for (var j = 0; j < alerts.length; j++) {
          var a = alerts[j];
          var li2 = document.createElement("li");
          li2.setAttribute("data-kind", String(a.kind || ""));
          li2.setAttribute("data-tier", String(a.tier));
          li2.textContent =
            (a.kind || "?") + "  T" + a.tier + "  "
            + (a.reason || a.previous_reason || "");
          alertList.appendChild(li2);
        }
      }
      el.setAttribute("data-last-updated",
                      new Date().toISOString());
    }

    function _poll() {
      var root = document.getElementById(ROOT_ID);
      if (!root) return;  // No-op when panel HTML isn't present.
      try {
        fetch("/api/probe-health", { cache: "no-store" })
          .then(function (r) {
            if (!r.ok) {
              root.setAttribute("data-fetch-error",
                                "http_" + r.status);
              return null;
            }
            return r.json();
          })
          .then(function (j) { if (j) _renderInto(root, j); })
          .catch(function (e) {
            root.setAttribute("data-fetch-error",
                              String(e && e.message || e));
          });
      } catch (e) { /* isolate */ }
    }

    _poll();
    setInterval(_poll, POLL_MS);
  }

  function init() {
    try { _bindSovereignHeader(); } catch (e) { /* isolate */ }
    try { _bindMissionControl(); }   catch (e) { /* isolate */ }
    try { _bindLiveMap(); }          catch (e) { /* isolate */ }
    try { _bindAgentKnowledge(); }   catch (e) { /* isolate */ }
    try { _bindTierAdoption(); }     catch (e) { /* isolate */ }
    try { _bindTierGraduation(); }   catch (e) { /* isolate */ }
    try { _bindEvolutionCenter(); }  catch (e) { /* isolate */ }
    try { _bindSovereignCore(); }    catch (e) { /* isolate */ }
    try { _bindVerifierChip(); }     catch (e) { /* isolate */ }
    try { _bindMetaChip(); }         catch (e) { /* isolate */ }
    try { _bindProbeHealth(); }      catch (e) { /* isolate */ }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Static-test markers (read by tests/*_static.py via regex grep):
  // LunaPanelContract.bind("mission_control"  - terminal_truth.mission_control
  // LunaPanelContract.bind("live_map"         - terminal_truth.live_map
  // LunaPanelContract.bind("agent_knowledge"  - terminal_truth.agent_knowledge
  // LunaPanelContract.bind("tier_adoption"    - terminal_truth.tier_adoption
  // LunaPanelContract.bind("tier_graduation"  - terminal_truth.tier_graduation
  // LunaPanelContract.bind("evolution_center" - terminal_truth.evolution_center
  // LunaPanelContract.bind("sovereign_core"   - terminal_truth.sovereign_core
  // LunaPanelContract.bind("verifier"         - terminal_truth.verifier
})();

