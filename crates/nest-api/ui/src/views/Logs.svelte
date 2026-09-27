<script lang="ts">
  import { get, type LogLine } from "../lib/api";
  import { app, route, singleFlight } from "../lib/state.svelte";

  let host = $state(route.params.get("host") ?? "");
  let level = $state("info");
  let q = $state(route.params.get("q") ?? "");
  let follow = $state(true);
  let lines = $state<LogLine[]>([]);
  let unreachable = $state<string[]>([]);
  async function load() {
    const p = new URLSearchParams({ level, limit: "1000" });
    if (host) p.set("host", host);
    if (q.trim()) p.set("q", q.trim());
    const v = await get<{ lines: LogLine[]; unreachable: string[] }>("/v1/logs?" + p);
    lines = v.lines.slice().reverse();
    unreachable = v.unreachable;
  }
  const refresh = singleFlight(load);
  $effect(() => {
    void host;
    void level;
    load();
  });
  $effect(() => {
    void app.tick;
    if (follow) refresh();
  });
  const t = (ms: number) => new Date(ms).toLocaleTimeString([], { hour12: false }) + "." + String(ms % 1000).padStart(3, "0");
</script>

<div class="stack">
  <div class="panel pad row">
    <h2>Logs</h2>
    <select bind:value={host}><option value="">all hosts</option>{#each app.status?.nodes ?? [] as n}<option>{n.name}</option>{/each}</select>
    <select bind:value={level}>{#each ["error", "warn", "info", "debug"] as l}<option>{l}</option>{/each}</select>
    <input type="search" placeholder="filter: a path, a job, an error…" bind:value={q} onkeydown={(e) => e.key === "Enter" && load()} style="flex:1;min-width:180px" />
    <label class="check small"><input type="checkbox" bind:checked={follow} /> follow</label>
  </div>
  {#each unreachable as u}<div class="small" style="color:var(--bad)">{u}</div>{/each}
  <div class="panel logs mono">
    {#each lines as l, i (i + ":" + l.ts_ms)}
      <div class="ln {l.level.toLowerCase()}"><span class="ts">{t(l.ts_ms)}</span><span class="h">{l.host}</span><span class="lv">{l.level}</span>
        <span class="msg"><span class="tg">{l.target.replace(/^nest_|^sparknestd::?/, "")}</span> {l.message}</span></div>
    {:else}<div class="empty">No matching lines</div>{/each}
  </div>
</div>

<style>
  .logs { padding: 8px 0; font-size: 12px; max-height: 75vh; overflow: auto; }
  .ln { display: grid; grid-template-columns: 96px 70px 48px 1fr; gap: 10px; padding: 3px 14px; border-bottom: 1px solid rgba(90, 170, 255, 0.05); }
  .ln:hover { background: rgba(56, 232, 255, 0.04); }
  .ts, .tg { color: var(--faint); }
  .h { color: var(--spark); overflow: hidden; text-overflow: ellipsis; }
  .lv { color: var(--muted); }
  .warn .lv { color: var(--warn); }
  .error .lv, .error .msg { color: var(--bad); }
  .msg { word-break: break-word; white-space: pre-wrap; }
  @media (max-width: 899px) { .ln { grid-template-columns: 1fr; gap: 0; } .ln .ts::after { content: " "; } }
</style>
