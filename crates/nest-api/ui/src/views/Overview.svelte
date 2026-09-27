<script lang="ts">
  import Constellation from "../components/Constellation.svelte";
  import JobStrip from "../components/JobStrip.svelte";
  import Icon from "../components/Icon.svelte";
  import { app, go } from "../lib/state.svelte";
  import { human, pct, rate } from "../lib/format";

  const hosts = $derived(app.status?.nodes ?? []);
  const held = $derived(hosts.reduce((a, h) => a + (h.info?.object_bytes ?? 0), 0));
  const free = $derived(hosts.reduce((a, h) => a + (h.info?.free_bytes ?? 0), 0));
  const cap = $derived(hosts.reduce((a, h) => a + (h.info?.total_bytes ?? 0), 0));
  const archived = $derived(app.stores.reduce((a, s) => a + (s.gateways.find(([, h]) => h.healthy)?.[1].object_bytes ?? 0), 0));
  const flow = $derived(Object.values(app.rates).reduce((a, r) => a + r.read, 0));
  const direct = $derived(Object.values(app.rates).reduce((a, r) => a + r.local, 0));
  // Spread-read rates a host measured: from its disk and from other hosts.
  const spread = (i: NonNullable<(typeof hosts)[number]["info"]>) => ({
    disk: (i.io ?? []).filter((s) => s.source === "Local").reduce((a, s) => a + s.bytes_per_s, 0),
    net: (i.io ?? []).filter((s) => s.source !== "Local").reduce((a, s) => a + s.bytes_per_s, 0),
  });
</script>

<div class="stack">
  <div class="tiles">
    <div class="tile panel"><div class="caps">On hosts</div><div class="big">{human(held)}</div><div class="small muted">all copies, {hosts.length} hosts</div></div>
    <div class="tile panel"><div class="caps">In archives</div><div class="big">{human(archived)}</div><div class="small muted">{app.stores.length} archive store{app.stores.length === 1 ? "" : "s"}</div></div>
    <div class="tile panel"><div class="caps">Free on hosts</div><div class="big">{human(free)}</div><div class="small muted">of {human(cap)} ({pct(free, cap).toFixed(0)}%)</div></div>
    <div class="tile panel" class:hot={flow > 1e6}><div class="caps">Fabric now</div><div class="big">{rate(flow)}</div><div class="small muted">reads between hosts</div></div>
    <div class="tile panel" class:hot={direct > 1e6}><div class="caps">Tracked direct</div><div class="big">{rate(direct)}</div><div class="small muted">hosts reading their own disks</div></div>
  </div>

  <JobStrip />

  <div class="panel glowline sky">
    <div class="skyhead row"><h2>Cluster</h2><span class="small muted">ring = disk: <span class="k spark"></span>sparknest <span class="k other"></span>other data · click a host to see what it holds</span></div>
    <Constellation />
  </div>

  <div class="grid hosts">
    {#each hosts as h (h.name)}
      {@const i = h.info}
      <button class="host panel" onclick={() => go("space", "", { scope: h.name })}>
        <div class="row"><span class="dot {i?.serving ? 'ok' : 'bad'}"></span><b>{h.name}</b>
          {#if app.status?.leader === h.node}<span class="badge warn">leader</span>{/if}
          <span class="spacer"></span><span class="tiny muted mono">{i ? human(i.free_bytes) + " free" : "down"}</span></div>
        {#if i}
          <div class="cap">
            <i class="other" style="width:{pct(i.total_bytes - i.free_bytes - i.object_bytes, i.total_bytes)}%"></i>
            <i class="ours" style="width:{pct(i.object_bytes, i.total_bytes)}%"></i>
          </div>
          <div class="row tiny muted mono"><span>{human(i.object_bytes)} held · {i.objects} files</span><span class="spacer"></span>
            {#if spread(i).disk + spread(i).net > 1e6}<span class="live" title="spread reads, last 10 s">disk {rate(spread(i).disk)} · net {rate(spread(i).net)}</span>
            {:else if app.rates[h.name]?.read > 1e5}<span class="live">↓{rate(app.rates[h.name].read)}</span>{/if}
            {#if !(spread(i).disk > 1e6) && app.rates[h.name]?.local > 1e5}<span class="live" title="read from its own disk">disk {rate(app.rates[h.name].local)}</span>{/if}
            {#if app.rates[h.name]?.served > 1e5}<span class="live">↑{rate(app.rates[h.name].served)}</span>{/if}</div>
        {/if}
      </button>
    {/each}
  </div>

  {#if app.stores.length}
    <h2 class="sec"><Icon name="archive" /> Archive stores</h2>
    <div class="grid hosts">
      {#each app.stores as s (s.name)}
        {@const g = s.gateways.find(([, h]) => h.healthy)}
        <button class="host panel" onclick={() => go("space", "", { scope: s.name })}>
          <div class="row"><span class="dot {g ? 'ok' : 'bad'}"></span><b>{s.name}</b><span class="spacer"></span>
            <span class="tiny muted mono">{g ? human(g[1].free_bytes) + " free" : "unreachable"}</span></div>
          {#if g}
            <div class="cap">
              <i class="other" style="width:{pct(g[1].total_bytes - g[1].free_bytes - g[1].object_bytes, g[1].total_bytes)}%"></i>
              <i class="ours" style="width:{pct(g[1].object_bytes, g[1].total_bytes)}%"></i>
            </div>
          {/if}
          <div class="tiny muted mono ell">{s.path} · via {s.gateways.filter(([, h]) => h.healthy).map(([n]) => n).join(", ") || "no gateway"}</div>
        </button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .tiles { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
  .tile { padding: 14px 16px; }
  .tile.hot { border-color: rgba(56, 232, 255, 0.5); box-shadow: 0 0 30px rgba(56, 232, 255, 0.2); }
  .big { font-size: clamp(20px, 2.6vw, 30px); font-weight: 650; font-family: var(--mono); letter-spacing: -0.02em; background: linear-gradient(90deg, #f1fbff, #8fe9ff); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .sky { padding: 14px 16px 6px; overflow: hidden; }
  .skyhead { justify-content: space-between; }
  .k { display: inline-block; width: 10px; height: 3px; border-radius: 2px; vertical-align: middle; margin: 0 3px 0 8px; }
  .k.spark { background: var(--spark); box-shadow: 0 0 6px var(--spark); }
  .k.other { background: #3b4d6b; }
  .hosts { grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 10px; }
  .host { text-align: left; padding: 12px 14px; display: flex; flex-direction: column; gap: 8px; cursor: pointer; transition: border-color 0.15s, box-shadow 0.15s; }
  .host:hover { border-color: var(--line-hi); box-shadow: var(--glow); }
  .cap { position: relative; height: 7px; border-radius: 99px; background: rgba(90, 140, 220, 0.12); overflow: hidden; display: flex; }
  .cap i { display: block; height: 100%; }
  .cap .other { background: #34445f; }
  .cap .ours { background: linear-gradient(90deg, var(--spark-2), var(--spark)); box-shadow: 0 0 10px rgba(56, 232, 255, 0.7); }
  .live { color: var(--spark); }
  .sec { display: flex; gap: 8px; align-items: center; margin-top: 6px; }
  .ell { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  @media (max-width: 899px) {
    .tiles { grid-template-columns: repeat(2, 1fr); }
    .tiles > :last-child:nth-child(odd) { grid-column: span 2; }
  }
</style>
