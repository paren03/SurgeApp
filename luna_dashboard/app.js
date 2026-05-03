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
 */
(function () {
  "use strict";

  const REFRESH_MS = 6000;     // server data poll cadence
  const SAMPLE_MS  = 500;      // oscilloscope sample cadence (calm)
  const FEED_LIMIT = 100;
  const ACTIVITY_WINDOW = 1800;
  const ACTIVITY_BUCKETS = 60;
  const TREND_LEN = 240;

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
  function makeBuf() { return new Array(TREND_LEN).fill(0); }
  const state = {
    bootEpoch: Date.now(),
    lastStatus: null,
    lastRes: null,
    lastBrief: null,
    lastSoak: null,
    lastScorecard: null,
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
  const SOURCE_META = {
    heartbeat:  { title: "PULSE · worker heartbeat", color: "#ffd98a", legend: "heartbeat",  unit: "luna" },
    cpu:        { title: "TREND · CPU usage",        color: "#f0b455", legend: "cpu %",      unit: "cpu" },
    mem:        { title: "TREND · memory free",      color: "#6fdcb1", legend: "mem free %", unit: "mem" },
    gpu:        { title: "TREND · GPU vram free",    color: "#b8d8a9", legend: "gpu free %", unit: "gpu" },
    queue:      { title: "TREND · queue depth",      color: "#e8c87a", legend: "queue",      unit: "jobs" },
    active:     { title: "TREND · active jobs",      color: "#6fdcb1", legend: "active",     unit: "jobs" },
    event_rate: { title: "TREND · event rate",       color: "#ffd98a", legend: "evt/min",    unit: "evt/min" },
    approval:   { title: "TREND · approval pending", color: "#d96a6a", legend: "pending",    unit: "items" },
    combined:   { title: "TREND · combined overlay", color: "#ffd98a", legend: "overlay",    unit: "norm" },
  };

  function drawOscilloscope(canvas) {
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    // Backdrop grid (very subtle)
    ctx.strokeStyle = "rgba(232,200,122,0.05)";
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
    ctx.strokeStyle = "rgba(232,200,122,0.18)";
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
        // Re-stroke without close path
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
      plot(state.trends.queue,     "#f0b455", { fill: false });
      plot(state.trends.active,    "#6fdcb1", { fill: false });
      plot(state.trends.event_rate,"#b8d8a9", { fill: false });
      plot(state.trends.heartbeat, "#ffd98a", { fill: true });
    } else {
      const buf = state.trends[src] || state.trends.heartbeat;
      const color = (SOURCE_META[src] || SOURCE_META.heartbeat).color;
      plot(buf, color, { fill: true });
    }

    // Trailing dot at "now"
    const buf = state.trends[src === "combined" ? "heartbeat" : src] || state.trends.heartbeat;
    const lastV = buf[buf.length - 1] || 0;
    const ny = H - (clamp(lastV, 0, 1) * H * 0.85) - H * 0.06;
    ctx.beginPath();
    ctx.arc(W - 4 * dpr, ny, 4 * dpr, 0, Math.PI * 2);
    ctx.fillStyle = "#fff2cc";
    ctx.shadowColor = "#ffd98a";
    ctx.shadowBlur = 14 * dpr;
    ctx.fill();
    ctx.shadowBlur = 0;
  }

  function syncOscilloscopeMeta() {
    const meta = SOURCE_META[state.pulseSource] || SOURCE_META.heartbeat;
    text($("osc-title"), meta.title);
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

    // 3) Standby — purely decorative.
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
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    if (canvas.width !== canvas.clientWidth * dpr) {
      canvas.width = Math.max(1, Math.round(canvas.clientWidth * dpr));
      canvas.height = Math.max(1, Math.round(canvas.clientHeight * dpr));
    }
    const W = canvas.width, H = canvas.height;
    const cx = W / 2, cy = H / 2;
    const Rout = Math.min(W, H) * 0.44;
    const Rin  = Rout - 12 * dpr;
    ctx.clearRect(0, 0, W, H);

    const m = pickMission();
    // Smooth progress easing
    const target = m.active ? m.progress : (Math.sin(performance.now() / 2400) + 1) / 2 * 0.04;
    state.mission.progressEased = state.mission.progressEased + (target - state.mission.progressEased) * 0.05;

    // Dotted background ring (matches new logo aesthetic)
    ctx.strokeStyle = "rgba(232,200,122,0.18)";
    ctx.lineWidth = 1 * dpr;
    ctx.setLineDash([2 * dpr, 6 * dpr]);
    ctx.beginPath(); ctx.arc(cx, cy, Rout - 1 * dpr, 0, Math.PI * 2); ctx.stroke();
    ctx.setLineDash([]);

    // Inner solid ring
    ctx.strokeStyle = "rgba(232,200,122,0.10)";
    ctx.beginPath(); ctx.arc(cx, cy, Rin, 0, Math.PI * 2); ctx.stroke();

    // Cardinal tick marks (N/E/S/W) — matches user's new logo
    const tickLen = 8 * dpr;
    ctx.strokeStyle = "rgba(232,200,122,0.45)";
    ctx.lineWidth = 1.2 * dpr;
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
      const grad = ctx.createConicGradient ? ctx.createConicGradient(a0, cx, cy) : null;
      ctx.beginPath();
      ctx.arc(cx, cy, Rout - 6 * dpr, a0, a1, false);
      ctx.strokeStyle = m.active ? "#ffd98a" : "rgba(232,200,122,0.4)";
      ctx.shadowColor = "#ffd98a";
      ctx.shadowBlur = 18 * dpr;
      ctx.lineWidth = 4 * dpr;
      ctx.lineCap = "round";
      ctx.stroke();
      ctx.shadowBlur = 0;

      // Leading bright dot
      const lx = cx + Math.cos(a1) * (Rout - 6 * dpr);
      const ly = cy + Math.sin(a1) * (Rout - 6 * dpr);
      ctx.beginPath(); ctx.arc(lx, ly, 4.5 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = "#fff2cc";
      ctx.shadowColor = "#ffd98a";
      ctx.shadowBlur = 14 * dpr;
      ctx.fill();
      ctx.shadowBlur = 0;
    }

    // Orbiting decorative dots — speed depends on active state
    state.mission.orbitPhase = (state.mission.orbitPhase + (m.active ? 0.012 : 0.004)) % (Math.PI * 2);
    const orbitR = Rin - 18 * dpr;
    const dots = m.active ? 5 : 3;
    for (let i = 0; i < dots; i++) {
      const ang = state.mission.orbitPhase + (i * Math.PI * 2 / dots);
      const x = cx + Math.cos(ang) * orbitR;
      const y = cy + Math.sin(ang) * orbitR;
      const a = 0.3 + 0.7 * (i / dots);
      ctx.beginPath();
      ctx.arc(x, y, 2.5 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(255,217,138," + a + ")";
      ctx.fill();
    }

    // Center crescent — keeps brand identity inside the clock
    drawCrescent(ctx, cx, cy, Rin * 0.55, dpr);

    // Time text — large, centered
    const timeText = m.active ? fmtClock(m.remaining) : "STANDBY";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillStyle = m.active ? "#fff2cc" : "rgba(247,242,230,0.75)";
    ctx.shadowColor = "rgba(232,200,122,0.4)";
    ctx.shadowBlur = m.active ? 12 * dpr : 0;
    ctx.font = (m.active ? "300 " : "300 ") + (m.active ? 28 : 18) * dpr + "px JetBrains Mono, Consolas, monospace";
    ctx.fillText(timeText, cx, cy + Rin * 0.78);
    ctx.shadowBlur = 0;

    // Sublabel under time
    ctx.font = 9 * dpr + "px JetBrains Mono, Consolas, monospace";
    ctx.fillStyle = "rgba(141,147,164,0.85)";
    ctx.fillText(m.active ? "TIME REMAINING" : "AWAITING TASK", cx, cy + Rin * 1.0);

    // Update the side caption
    text($("mission-task"), m.title);
    text($("mission-detail"), m.detail);
    text($("mission-stat"),
         m.active ? Math.round(state.mission.progressEased * 100) + "% · " + m.kind
                  : "idle · " + (state.lastStatus ? "luna " + ((state.lastStatus.luna||{}).state || "?") : "—"));
  }

  function drawCrescent(ctx, cx, cy, R, dpr) {
    // Subtle decorative crescent — gold rim + dark side, scaled to R
    const offX = R * 0.28;

    // Outer halo
    const halo = ctx.createRadialGradient(cx, cy, R * 0.4, cx, cy, R * 1.1);
    halo.addColorStop(0, "rgba(232,200,122,0.25)");
    halo.addColorStop(1, "rgba(232,200,122,0)");
    ctx.fillStyle = halo;
    ctx.beginPath(); ctx.arc(cx, cy, R * 1.1, 0, Math.PI * 2); ctx.fill();

    // Full disc (dark)
    const disc = ctx.createRadialGradient(cx - R * 0.2, cy - R * 0.25, R * 0.1, cx, cy, R);
    disc.addColorStop(0, "#3a2e1a");
    disc.addColorStop(1, "#0a0a0e");
    ctx.fillStyle = disc;
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.fill();

    // Crescent lit edge — gold gradient
    ctx.save();
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.clip();
    const lit = ctx.createRadialGradient(cx - R * 0.3, cy - R * 0.2, 0, cx - R * 0.3, cy - R * 0.2, R * 1.4);
    lit.addColorStop(0, "rgba(255,242,204,0.95)");
    lit.addColorStop(0.45, "rgba(232,200,122,0.65)");
    lit.addColorStop(1, "rgba(120, 80, 40, 0)");
    ctx.fillStyle = lit;
    ctx.beginPath(); ctx.arc(cx + offX, cy + R * 0.05, R * 1.4, 0, Math.PI * 2); ctx.fill();

    // Cut the dark side of the crescent
    ctx.globalCompositeOperation = "destination-out";
    ctx.beginPath();
    ctx.arc(cx + offX * 1.0, cy - R * 0.05, R * 0.95, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalCompositeOperation = "source-over";

    // Tiny crater-like dots for texture
    ctx.fillStyle = "rgba(255,242,204,0.18)";
    [[ -0.45, -0.25, 0.06], [ -0.30, 0.10, 0.05], [ -0.55, 0.15, 0.04],
     [ -0.20, -0.40, 0.04], [ -0.10,  0.30, 0.05]].forEach((c) => {
      ctx.beginPath();
      ctx.arc(cx + R * c[0], cy + R * c[1], R * c[2], 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.restore();
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

    ctx.strokeStyle = "rgba(232,200,122,0.06)";
    ctx.lineWidth = 1 * dpr;
    for (let g = 1; g <= 4; g++) {
      const y = (H * g) / 4;
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    }

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
  function rafLoop() {
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
    ]);
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
      });
    }

    refreshAll();
    setInterval(refreshAll, REFRESH_MS);
    setInterval(tickClock, 1000);
    setInterval(rotatePrompt, 5500);
    setInterval(sampleTrends, SAMPLE_MS);  // calm 2 Hz sampling
    requestAnimationFrame(rafLoop);
  });
})();
