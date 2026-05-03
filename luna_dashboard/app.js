/* Luna Command Center — Phase UI-1A
 * Read-only front-end. Polls the local API; never sends writes.
 * No external CDNs, no eval, no third-party scripts.
 *
 * Live Operations canvas visualizations:
 *   - Pulse oscilloscope (worker heartbeat / queue / activity)
 *   - Service mesh radar (worker / guardian / aider / ollama nodes)
 *   - Event frequency histogram (last 30 min, 60 buckets)
 *   - Animated resource gauges (CPU / MEM / GPU / DISK)
 *   - Terminal-style live TTY ticker
 */
(function () {
  "use strict";

  const REFRESH_MS = 6000;
  const FEED_LIMIT = 100;
  const ACTIVITY_WINDOW = 1800;
  const ACTIVITY_BUCKETS = 60;

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
  function trim(str, max) {
    if (typeof str !== "string") return "";
    return str.length > max ? str.slice(0, max-1) + "…" : str;
  }
  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

  async function fetchJSON(path) {
    try {
      const r = await fetch(path, { credentials: "omit", cache: "no-store" });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  // ============================================================
  // Live state shared across visualizers
  // ============================================================
  const state = {
    bootEpoch: Date.now(),
    lastStatus: null,
    lastRes: null,
    lastBrief: null,
    lastSoak: null,
    lastScorecard: null,
    lastFeedRecords: [],
    lastActivity: null,
    pulse: {
      buf: new Array(240).fill(0),
      queueBuf: new Array(240).fill(0),
      activeBuf: new Array(240).fill(0),
      tickPhase: 0,
      lastHeartbeatTs: "",
    },
    services: [
      { id: "worker",   label: "WORKER",   on: false, pulse: 0 },
      { id: "guardian", label: "GUARDIAN", on: false, pulse: 0 },
      { id: "aider",    label: "AIDER",    on: false, pulse: 0 },
      { id: "ollama",   label: "OLLAMA",   on: false, pulse: 0 },
      { id: "soak",     label: "SOAK",     on: false, pulse: 0 },
    ],
    gauges: { cpu: 0, mem: 0, gpu: 0, disk: 0 },
    ttySeen: new Set(),
  };

  // ============================================================
  // Title bar clock + prompt rotator
  // ============================================================
  function tickClock() {
    const t = new Date();
    const z = (n) => String(n).padStart(2, "0");
    text($("meta-time"),
      t.getFullYear() + "·" + z(t.getMonth()+1) + "·" + z(t.getDate()) +
      "  " + z(t.getHours()) + ":" + z(t.getMinutes()) + ":" + z(t.getSeconds()));

    // Footer uptime
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
  // Oscilloscope (worker pulse)
  // ============================================================
  function drawOscilloscope(canvas) {
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    // Backdrop grid
    ctx.strokeStyle = "rgba(232,200,122,0.06)";
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath();
    for (let i = 0; i <= 8; i++) {
      const y = (H * i / 8);
      ctx.moveTo(0, y); ctx.lineTo(W, y);
    }
    for (let i = 0; i <= 16; i++) {
      const x = (W * i / 16);
      ctx.moveTo(x, 0); ctx.lineTo(x, H);
    }
    ctx.stroke();

    // Plot helper
    function plot(buf, color, alpha, glow) {
      const N = buf.length;
      ctx.beginPath();
      for (let i = 0; i < N; i++) {
        const x = (i / (N - 1)) * W;
        const y = H - (clamp(buf[i], 0, 1) * H * 0.85) - H * 0.05;
        if (i === 0) ctx.moveTo(x, y);
        else         ctx.lineTo(x, y);
      }
      ctx.strokeStyle = color;
      ctx.globalAlpha = alpha;
      ctx.lineWidth = 1.5 * dpr;
      if (glow) {
        ctx.shadowBlur = 12 * dpr;
        ctx.shadowColor = color;
      } else {
        ctx.shadowBlur = 0;
      }
      ctx.stroke();
      ctx.globalAlpha = 1;
      ctx.shadowBlur = 0;
    }

    plot(state.pulse.queueBuf,  "#f0b455", 0.45, false);
    plot(state.pulse.activeBuf, "#6fdcb1", 0.55, false);
    plot(state.pulse.buf,        "#ffd98a", 0.95, true);

    // "Now" cursor
    ctx.strokeStyle = "rgba(232,200,122,0.45)";
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath(); ctx.moveTo(W - 0.5, 0); ctx.lineTo(W - 0.5, H); ctx.stroke();
  }

  function pushPulse() {
    // Tick phase: a smooth sine wave that briefly spikes when a heartbeat
    // change is detected. The two sub-buffers track queue depth and active
    // jobs as steady bars beneath the pulse.
    const s = state.lastStatus || {};
    const w = s.worker || {};
    const luna = s.luna || {};
    const guardian = s.guardian || {};

    const aliveBoost = luna.alive ? 0.55 : 0.18;
    const phase = state.pulse.tickPhase;
    state.pulse.tickPhase = (phase + 0.18) % (Math.PI * 2);
    let v = aliveBoost + Math.sin(phase) * 0.18 + Math.sin(phase * 2.3) * 0.07;

    // Heartbeat ts changed since last frame? spike.
    if (luna.ts && luna.ts !== state.pulse.lastHeartbeatTs) {
      v += 0.30;
      state.pulse.lastHeartbeatTs = luna.ts;
    }
    if (guardian.kill_switch_present) v -= 0.4;

    state.pulse.buf.push(clamp(v, 0, 1));
    state.pulse.buf.shift();

    const q = clamp((w.queue_depth || 0) / 8, 0, 1);
    state.pulse.queueBuf.push(q);
    state.pulse.queueBuf.shift();

    const a = clamp((w.active_count || 0) / 4, 0, 1);
    state.pulse.activeBuf.push(a);
    state.pulse.activeBuf.shift();
  }

  // ============================================================
  // Service mesh radar
  // ============================================================
  function drawRadar(canvas) {
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
    const W = canvas.width, H = canvas.height;
    const cx = W / 2, cy = H / 2;
    const R = Math.min(W, H) * 0.42;
    ctx.clearRect(0, 0, W, H);

    // Concentric rings
    for (let k = 1; k <= 4; k++) {
      ctx.beginPath();
      ctx.arc(cx, cy, (R * k) / 4, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(232,200,122,${0.06 + k * 0.02})`;
      ctx.lineWidth = 1 * dpr;
      ctx.stroke();
    }

    // Sweep arm
    const t = (performance.now() / 4500) * Math.PI * 2;
    const grad = ctx.createConicGradient ? ctx.createConicGradient(t, cx, cy) : null;
    if (grad) {
      grad.addColorStop(0,    "rgba(232,200,122,0.40)");
      grad.addColorStop(0.04, "rgba(232,200,122,0.10)");
      grad.addColorStop(0.30, "rgba(232,200,122,0.0)");
      grad.addColorStop(1,    "rgba(232,200,122,0.0)");
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.arc(cx, cy, R, 0, Math.PI * 2);
      ctx.fill();
    }

    // Center core
    ctx.beginPath();
    ctx.arc(cx, cy, 6 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = "#fff2cc";
    ctx.shadowColor = "#ffd98a";
    ctx.shadowBlur = 16 * dpr;
    ctx.fill();
    ctx.shadowBlur = 0;
    ctx.strokeStyle = "rgba(255,217,138,0.6)";
    ctx.lineWidth = 1 * dpr;
    ctx.beginPath(); ctx.arc(cx, cy, 14 * dpr, 0, Math.PI * 2); ctx.stroke();

    // Service nodes around the ring
    const N = state.services.length;
    state.services.forEach((svc, i) => {
      const angle = -Math.PI / 2 + (i / N) * Math.PI * 2;
      const nx = cx + Math.cos(angle) * R * 0.78;
      const ny = cy + Math.sin(angle) * R * 0.78;
      const tone = svc.on ? "#6fdcb1" : "#d96a6a";

      // Connection line to center
      ctx.strokeStyle = svc.on ? "rgba(111,220,177,0.25)" : "rgba(217,106,106,0.18)";
      ctx.lineWidth = 1 * dpr;
      ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(nx, ny); ctx.stroke();

      // Pulse rings (decay over time)
      svc.pulse = Math.max(0, svc.pulse - 0.02);
      if (svc.pulse > 0) {
        const pr = (1 - svc.pulse) * 28 * dpr + 8 * dpr;
        ctx.beginPath();
        ctx.arc(nx, ny, pr, 0, Math.PI * 2);
        ctx.strokeStyle = `rgba(255,217,138,${svc.pulse * 0.6})`;
        ctx.lineWidth = 1.5 * dpr;
        ctx.stroke();
      }

      // Node dot
      ctx.beginPath();
      ctx.arc(nx, ny, 7 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = tone;
      ctx.shadowColor = tone;
      ctx.shadowBlur = 10 * dpr;
      ctx.fill();
      ctx.shadowBlur = 0;
      ctx.strokeStyle = "rgba(255,255,255,0.4)";
      ctx.lineWidth = 1 * dpr;
      ctx.stroke();

      // Label
      ctx.fillStyle = svc.on ? "#f7f2e6" : "#8d93a4";
      ctx.font = `${10 * dpr}px JetBrains Mono, Consolas, monospace`;
      ctx.textAlign = "center";
      ctx.fillText(svc.label, nx, ny + (16 + 8) * dpr);
    });
  }

  function refreshServiceStates() {
    const s = state.lastStatus || {};
    const map = {
      worker:   !!(s.worker && s.worker.running),
      guardian: !!(s.guardian && s.guardian.running),
      aider:    !!(s.aider_bridge && s.aider_bridge.running),
      ollama:   !!((state.lastRes && state.lastRes.ollama && state.lastRes.ollama.api_reachable)),
      soak:     !!(s.soak && (s.soak.verdict === "PASS" || s.soak.verdict === "RUNNING" || (s.soak.observed_cycles||0) > 0)),
    };
    state.services.forEach((svc) => {
      const wasOn = svc.on;
      svc.on = !!map[svc.id];
      if (svc.on && !wasOn) svc.pulse = 1.0;
    });
    // Also pulse the worker on every successful status refresh
    const w = state.services.find((s) => s.id === "worker");
    if (w && w.on) w.pulse = Math.max(w.pulse, 0.6);
  }

  // ============================================================
  // Event histogram
  // ============================================================
  function drawHistogram(canvas) {
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const data = (state.lastActivity && state.lastActivity.counts) || [];
    const N = data.length || 1;
    const max = Math.max(1, ...data);
    const bw = W / N;

    // Baseline grid
    ctx.strokeStyle = "rgba(232,200,122,0.06)";
    ctx.lineWidth = 1 * dpr;
    for (let g = 1; g <= 4; g++) {
      const y = (H * g) / 4;
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    }

    // Bars
    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, "rgba(255,217,138,0.95)");
    grad.addColorStop(1, "rgba(184,144,72,0.45)");
    for (let i = 0; i < N; i++) {
      const v = data[i] || 0;
      const h = (v / max) * (H * 0.85);
      const x = i * bw + 1 * dpr;
      const y = H - h;
      ctx.fillStyle = grad;
      ctx.fillRect(x, y, Math.max(1, bw - 2 * dpr), h);
    }

    // Trailing average line
    ctx.beginPath();
    ctx.strokeStyle = "rgba(232,200,122,0.45)";
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
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
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

    // Animated fill
    const id = canvas.parentElement.dataset.id;
    const cur = state.gauges[id] || 0;
    const next = cur + (target - cur) * 0.18;
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

    // Tick at end
    const ex = cx + Math.cos(a1) * R;
    const ey = cy + Math.sin(a1) * R;
    ctx.beginPath();
    ctx.arc(ex, ey, 4 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = "#fff2cc";
    ctx.fill();
  }

  function refreshGauges() {
    const res = state.lastRes || {};
    const cpuPct = (res.cpu && res.cpu.usage_percent) || 0;
    const memFree = (res.memory && res.memory.available_percent) || 0;
    const gpuFree = (res.gpu && res.gpu.free_vram_percent) || 0;
    const diskFree = (res.disk && res.disk.project_drive_free_percent) || 0;

    const COLORS = {
      cpu:  "#f0b455",
      mem:  "#6fdcb1",
      gpu:  "#ffd98a",
      disk: "#b8d8a9",
    };
    document.querySelectorAll(".luna-gauge").forEach((g) => {
      const id = g.dataset.id;
      const c = g.querySelector("canvas");
      const num = g.querySelector(".luna-gauge__num");
      let v = 0, label = "—";
      if (id === "cpu")  { v = cpuPct;  label = fmtPct(cpuPct); }
      if (id === "mem")  { v = memFree; label = fmtPct(memFree); }
      if (id === "gpu")  { v = gpuFree; label = fmtPct(gpuFree); }
      if (id === "disk") { v = diskFree; label = fmtPct(diskFree); }
      drawGauge(c, v, COLORS[id]);
      if (num) num.textContent = label;
    });
  }

  // ============================================================
  // TTY ticker (live event stream, terminal style)
  // ============================================================
  function appendTty(record) {
    const ol = $("luna-tty");
    if (!ol) return;
    const key = (record.ts || "") + "|" + (record.event || "") + "|" + trim(record.msg || "", 40);
    if (state.ttySeen.has(key)) return;
    state.ttySeen.add(key);
    if (state.ttySeen.size > 400) {
      // Trim oldest entries from the set so it doesn't grow without bound.
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
    // Records already most-recent-last. Append unseen ones in order.
    records.forEach((r) => appendTty(r));
  }

  // ============================================================
  // Existing card refresh (status, brief, soak, etc.)
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

    text($("stat-verifier"), (s.verifier && s.verifier.summary) ? trim(s.verifier.summary, 22) : "ADVISORY");

    const soak = s.soak || {};
    text($("stat-soak"), soak.verdict || "UNKNOWN");
    setTone($("stat-soak"),
      soak.verdict === "PASS" ? "ok" :
      soak.verdict === "FAIL" ? "bad" : "warn");

    const safe = s.safety || {};
    const exec = $("safe-exec");
    const guard = $("safe-guardian");
    if (exec)  exec.querySelector(".luna-safelock__value").textContent  = safe.code_execution_state || "LOCKED";
    if (guard) guard.querySelector(".luna-safelock__value").textContent = safe.guardian_live_enforcement || "DISABLED";

    text($("footer-phase"), "Phase " + (s.phase || "UI-1A"));
    text($("radar-stat"), (state.services.filter(x => x.on).length) + "/" + state.services.length + " online");
    text($("gauge-stat"), (state.lastRes && state.lastRes.resource_mode) || "—");
  }

  async function refreshBrief() {
    const b = await fetchJSON("/api/decision-brief");
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
    const items = (b.top_items || []).slice(0, 6);
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
      tag.dataset.rec = (it.recommendation || "UNKNOWN");
      tag.textContent = (it.recommendation || "UNKNOWN").replace(/_/g, " ");
      const goal = document.createElement("span");
      goal.className = "luna-card-goal";
      goal.textContent = it.goal || it.action_type || "—";
      const msg = document.createElement("div");
      msg.className = "luna-card-msg";
      msg.textContent = trim(it.plain_english || "", 240);
      li.appendChild(tag); li.appendChild(goal); li.appendChild(msg);
      ul.appendChild(li);
    });
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
    }
    if (status) {
      const w = status.worker || {};
      text($("health-queue"), w.queue_depth != null ? w.queue_depth : "—");
      text($("health-pending"), w.approval_pending != null ? w.approval_pending : "—");
    }
  }

  async function refreshFeed() {
    const f = await fetchJSON("/api/live-feed?limit=" + FEED_LIMIT);
    if (!f) return;
    state.lastFeedRecords = f.records || [];
    text($("feed-meta"), (f.count || 0) + " events · cap " + (f.limit || FEED_LIMIT));
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

    // TTY ticker — append unseen events in chronological order.
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

    // Update OSC headline in Hz-ish units (events/min)
    const perMin = (a.total_events || 0) / Math.max(1, (a.window_seconds || 60)/60);
    text($("osc-bpm"), perMin.toFixed(2) + " evt/min");
  }

  // ============================================================
  // Animation loop (always 60 fps; data refreshes are async)
  // ============================================================
  function rafLoop() {
    pushPulse();
    refreshServiceStates();
    const osc = $("osc-canvas");
    const radar = $("radar-canvas");
    const hist = $("hist-canvas");
    if (osc)   drawOscilloscope(osc);
    if (radar) drawRadar(radar);
    if (hist)  drawHistogram(hist);
    refreshGauges();
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
    ]);
  }

  document.addEventListener("DOMContentLoaded", () => {
    refreshAll();
    setInterval(refreshAll, REFRESH_MS);
    setInterval(tickClock, 1000);
    setInterval(rotatePrompt, 5500);
    requestAnimationFrame(rafLoop);
  });
})();
