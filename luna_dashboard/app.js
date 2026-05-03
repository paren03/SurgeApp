/* Luna Command Center — Phase UI-1A
 * Read-only front-end. Polls the local API; never sends writes.
 * No external CDNs, no eval, no third-party scripts.
 */
(function () {
  "use strict";

  const REFRESH_MS = 8000;
  const FEED_LIMIT = 100;

  const $ = (id) => document.getElementById(id);
  const text = (el, value) => { if (el) el.textContent = value == null ? "—" : String(value); };
  const setTone = (el, tone) => { if (el) el.parentElement.dataset.tone = tone || ""; };

  function fmtBool(v) { return v ? "ONLINE" : "OFFLINE"; }
  function fmtPct(n)  { if (typeof n !== "number") return "—"; return Math.round(n) + "%"; }
  function fmtBytes(n) {
    if (typeof n !== "number" || !isFinite(n)) return "—";
    const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0; let v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return v.toFixed(v < 10 ? 1 : 0) + " " + u[i];
  }
  function trim(str, max) {
    if (typeof str !== "string") return "";
    return str.length > max ? str.slice(0, max - 1) + "…" : str;
  }

  async function fetchJSON(path) {
    try {
      const r = await fetch(path, { credentials: "omit", cache: "no-store" });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  function tickClock() {
    const t = new Date();
    const z = (n) => String(n).padStart(2, "0");
    text($("meta-time"),
         t.getFullYear() + "·" + z(t.getMonth() + 1) + "·" + z(t.getDate()) +
         "  " + z(t.getHours()) + ":" + z(t.getMinutes()) + ":" + z(t.getSeconds()));
  }

  // --------------- /api/status -> hero --------------------------------
  async function refreshStatus() {
    const s = await fetchJSON("/api/status");
    if (!s) return;
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

    // Safety locks always reflect server contract.
    const safe = s.safety || {};
    const exec = $("safe-exec");
    const guard = $("safe-guardian");
    if (exec)  exec.querySelector(".luna-safelock__value").textContent  = safe.code_execution_state || "LOCKED";
    if (guard) guard.querySelector(".luna-safelock__value").textContent = safe.guardian_live_enforcement || "DISABLED";

    text($("footer-phase"), "Phase " + (s.phase || "UI-1A"));
  }

  // --------------- /api/decision-brief -> Decision Center -------------
  async function refreshBrief() {
    const b = await fetchJSON("/api/decision-brief");
    if (!b) return;
    const counts = b.counts || {};
    text($("count-approve"), counts.approve_recommended || 0);
    text($("count-wait"),    counts.wait_for_more_evidence || 0);
    text($("count-deny"),    counts.do_not_approve || 0);
    text($("count-serge"),   counts.serge_only || 0);
    text($("brief-recommendation"),
         b.overall_recommendation
           ? "Overall · " + b.overall_recommendation.toUpperCase()
           : "Advisory");
    text($("brief-summary"), b.serge_summary || "");

    const ul = $("brief-top-items");
    if (!ul) return;
    ul.innerHTML = "";
    const items = (b.top_items || []).slice(0, 6);
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "luna-cards__empty";
      li.textContent = "No morning brief available yet.";
      ul.appendChild(li);
      return;
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

  // --------------- /api/soak -> Advisory Soak -------------------------
  async function refreshSoak() {
    const s = await fetchJSON("/api/soak");
    if (!s) return;
    const obs = s.observed_cycles || 0;
    const req = s.required_cycles || 144;
    const pct = req > 0 ? Math.min(100, Math.round((obs / req) * 100)) : 0;
    const fill = $("soak-fill");
    if (fill) fill.style.width = pct + "%";
    text($("soak-observed"), obs);
    text($("soak-required"), req);
    text($("soak-verdict"), (s.verdict || "UNKNOWN").replace(/_/g, " "));
    text($("soak-last"), s.last_update ? "last " + s.last_update.replace("T", " ").slice(0, 19) + "Z" : "last —");
    text($("soak-cmd"), s.soak_command || "—");
  }

  // --------------- /api/scorecard + /api/resources -> System Health ---
  async function refreshHealth() {
    const [score, res, status] = await Promise.all([
      fetchJSON("/api/scorecard"),
      fetchJSON("/api/resources"),
      fetchJSON("/api/status"),
    ]);
    if (score) {
      const ring = $("health-ring");
      const num  = $("health-num");
      const overall = score.overall_score || 0;
      if (ring) ring.style.setProperty("--pct", overall);
      if (num)  num.textContent = overall || "—";
      text($("health-readiness"),
           (score.readiness_level || "unknown").replace(/_/g, " "));
    }
    if (res) {
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

  // --------------- /api/live-feed -------------------------------------
  async function refreshFeed() {
    const f = await fetchJSON("/api/live-feed?limit=" + FEED_LIMIT);
    if (!f) return;
    text($("feed-meta"),
         (f.count || 0) + " events · cap " + (f.limit || FEED_LIMIT));
    const ol = $("live-feed");
    if (!ol) return;
    ol.innerHTML = "";
    const records = (f.records || []).slice().reverse();
    if (!records.length) {
      const li = document.createElement("li");
      li.className = "luna-feed__empty";
      li.textContent = "Awaiting telemetry…";
      ol.appendChild(li);
      return;
    }
    records.forEach((r) => {
      const li = document.createElement("li");
      const ts = document.createElement("span");
      ts.className = "luna-feed__ts";
      ts.textContent = r.ts || "—";
      const role = document.createElement("span");
      role.className = "luna-feed__role";
      role.textContent = r.role || r.source || "—";
      const msg = document.createElement("span");
      msg.className = "luna-feed__msg";
      const head = document.createElement("strong");
      head.textContent = (r.event || "EVENT") + " ";
      msg.appendChild(head);
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

  // --------------- /api/archive ---------------------------------------
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
      ul.appendChild(li);
      return;
    }
    items.forEach((it) => {
      const li = document.createElement("li");
      const name = document.createElement("span");
      name.className = "luna-archive__name";
      name.title = it.name || "";
      name.textContent = it.name || "—";
      const size = document.createElement("span");
      size.className = "luna-archive__size";
      size.textContent = it.is_dir ? "dir" : fmtBytes(it.size_bytes || 0);
      li.appendChild(name); li.appendChild(size);
      ul.appendChild(li);
    });
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
    ]);
  }

  document.addEventListener("DOMContentLoaded", () => {
    refreshAll();
    setInterval(refreshAll, REFRESH_MS);
    setInterval(tickClock, 1000);
  });
})();
