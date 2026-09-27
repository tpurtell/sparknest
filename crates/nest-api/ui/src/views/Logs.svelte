<script lang="ts">
  import { type LogLine } from "../lib/api";
  import { app, route, followLogs } from "../lib/state.svelte";

  const KEEP = 5000;
  let host = $state(route.params.get("host") ?? "");
  let level = $state("info");
  let q = $state(route.params.get("q") ?? "");
  let applied = $state(route.params.get("q") ?? "");
  let follow = $state(true);
  let lines = $state<LogLine[]>([]);
  let connected = $state(false);

  // Live: over the page's socket, recent lines first, then each new one.
  $effect(() => {
    const q: Record<string, unknown> = { level, limit: 1000 };
    if (host) q.host = host;
    if (applied.trim()) q.q = applied.trim();
    if (!follow) return;
    lines = [];
    connected = false;
    return followLogs(q, (fresh) => {
      connected = true;
      if (!fresh.length) return;
      lines = [...fresh.reverse(), ...lines].slice(0, KEEP);
    });
  });
  const t = (ms: number) => new Date(ms).toLocaleTimeString([], { hour12: false }) + "." + String(ms % 1000).padStart(3, "0");
</script>

<div class="stack">
  <div class="panel pad row">
    <h2>Logs</h2>
    <select bind:value={host}><option value="">all hosts</option>{#each app.status?.nodes ?? [] as n}<option>{n.name}</option>{/each}</select>
    <select bind:value={level}>{#each ["error", "warn", "info", "debug"] as l}<option>{l}</option>{/each}</select>
    <input type="search" placeholder="filter: a path, a job, an error… (Enter)" bind:value={q} onchange={() => (applied = q)} style="flex:1;min-width:180px" />
    <label class="check small"><input type="checkbox" bind:checked={follow} /> live</label>
    <span class="dot {follow && connected ? 'ok' : 'bad'}" title={follow ? (connected ? "streaming" : "connecting") : "paused"}></span>
  </div>
  <div class="panel logs mono">
    {#each lines as l (l.host + ":" + l.ts_ms + ":" + (l.seq ?? 0))}
      <div class="ln {l.level.toLowerCase()}"><span class="ts">{t(l.ts_ms)}</span><span class="h">{l.host}</span><span class="lv">{l.level}</span>
        <span class="msg"><span class="tg">{l.target.replace(/^nest_|^sparknestd::?/, "")}</span> {l.message}</span></div>
    {:else}<div class="empty">{follow ? "Waiting for lines…" : "Paused"}</div>{/each}
  </div>
</div>

<style>
  .logs { padding: 8px 0; font-size: 12px; max-height: 75vh; overflow: auto; }
  .ln { display: grid; grid-template-columns: 96px 70px 48px 1fr; gap: 10px; padding: 3px 14px; border-bottom: 1px solid rgba(90, 170, 255, 0.05); animation: rise 0.25s ease-out; }
  .ln:hover { background: rgba(56, 232, 255, 0.04); }
  .ts, .tg { color: var(--faint); }
  .h { color: var(--spark); overflow: hidden; text-overflow: ellipsis; }
  .lv { color: var(--muted); }
  .warn .lv { color: var(--warn); }
  .error .lv, .error .msg { color: var(--bad); }
  .msg { word-break: break-word; white-space: pre-wrap; }
  @media (max-width: 899px) { .ln { grid-template-columns: 1fr; gap: 0; } .ln .ts::after { content: " "; } }
</style>
