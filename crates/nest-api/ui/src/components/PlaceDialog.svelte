<script lang="ts">
  // Where a model (or any selection) should live: on several fabric hosts
  // with a replica factor (1/hosts spreads it, 1 copies it everywhere),
  // gathered on one host, as full copies on off-grid hosts (on the LAN
  // only, ADR-047), or in an archive. Placements are planned and shown as an
  // editable plan before anything moves.
  import { get, post, type Plan, type Readiness, type HostUsage } from "../lib/api";
  import { app, startJob, singleFlight, places } from "../lib/state.svelte";
  import { human } from "../lib/format";
  import { placing } from "../lib/ui.svelte";
  import { portal } from "../lib/portal";
  import PlanPreview from "./PlanPreview.svelte";

  type Mode = "multi" | "single" | "offgrid" | "archive";
  let mode = $state<Mode>("multi");
  let multi = $state<string[]>([]);
  let single = $state("");
  let picked = $state<string[]>([]);
  let store = $state("");
  let keepLive = $state(false);
  let factor = $state(1);
  let bySpeed = $state(false);
  // Off-grid: one copy of each file over the hosts picked (default), or a
  // full copy on each.
  let spreadOff = $state(true);
  let plan = $state<Plan | null>(null);
  let err = $state("");
  let busy = $state(false);
  let stats = $state<{ hosts: Readiness[]; usage: Record<string, HostUsage> } | null>(null);

  // Fabric places and off-grid ones, in the cluster's order: each host,
  // then its extra disks, each a place of its own (ADR-073).
  const all = $derived(places());
  const place = (h: string) => all.find((p) => p.name === h);
  const hosts = $derived(all.filter((p) => p.up && !p.offgrid).map((p) => p.name));
  const offgrid = $derived(all.filter((p) => p.offgrid).map((p) => p.name));
  const full = (h: string) => {
    const r = stats?.hosts.find((x) => x.host === h);
    return !!r && r.bytes > 0 && r.missing_bytes === 0;
  };
  // Usage is a host's: a disk shows its host's reads.
  const read = (h: string) => {
    const u = stats?.usage?.[place(h)?.host ?? h];
    return u ? u.local_bytes + u.net_bytes : 0;
  };
  const held = (h: string) => {
    const r = stats?.hosts.find((x) => x.host === h);
    return r ? r.bytes - r.missing_bytes : 0;
  };
  const free = (h: string) => place(h)?.free ?? 0;
  const disk = (h: string) => place(h)?.read_bps ?? 0;
  // Relative use of this selection: the most-reading host a full bar, the
  // least an empty one.
  const useBar = $derived.by(() => {
    const v = hosts.map(read);
    const lo = Math.min(...v, 0);
    const hi = Math.max(...v, 0);
    return (h: string) => (hi > lo ? (read(h) - lo) / (hi - lo) : 0);
  });
  // The most efficient single host: reads it most, then holds the most.
  const best = $derived([...hosts].sort((a, b) => read(b) - read(a) || held(b) - held(a))[0] ?? "");

  let opened = "";
  $effect(() => {
    if (!placing.open || opened === placing.selector) return;
    opened = placing.selector;
    mode = "multi";
    multi = [...hosts];
    picked = [];
    // One copy of each file to start: a spread (TJ almost always wants it).
    factor = 1 / Math.max(1, hosts.length);
    bySpeed = false;
    spreadOff = true;
    plan = null;
    err = "";
    stats = null;
    store = app.stores[0]?.name ?? offgrid[0] ?? "";
    keepLive = false;
    if (placing.selector.startsWith("hf")) {
      get<{ hosts: Readiness[]; usage: Record<string, HostUsage> }>("/v1/hf/detail?selector=" + encodeURIComponent(placing.selector))
        .then((d) => {
          stats = d;
          single = best;
          picked = offgrid.filter(full);
        })
        .catch(() => {});
    }
    single = best;
  });

  const H = $derived(Math.max(1, multi.length));
  const k = $derived(Math.max(1, Math.min(H, Math.round(factor * H))));
  // Snap the factor to whole copies when the host count changes.
  $effect(() => {
    void H;
    factor = Math.max(1 / H, Math.min(1, Math.round(factor * H) / H));
  });

  let timer: ReturnType<typeof setTimeout> | undefined;
  const makeOnce = singleFlight(async () => {
    if (!placing.open || mode === "archive") return;
    const targets = mode === "multi" ? multi : mode === "offgrid" ? picked : single ? [single] : [];
    if (!targets.length && mode !== "offgrid") {
      plan = null;
      return;
    }
    busy = true;
    try {
      plan = await post<Plan>(
        "/v1/plans",
        mode === "offgrid"
          ? { goal: "offgrid", selector: placing.selector, hosts: targets, spread: spreadOff && targets.length > 1 }
          : {
              goal: "place",
              selector: placing.selector,
              hosts: targets,
              factor: mode === "multi" ? k / H : 1,
              by_speed: mode === "multi" && bySpeed && k < H,
            },
      );
      err = "";
    } catch (e) {
      err = (e as Error).message;
      plan = null;
    }
    busy = false;
  });
  $effect(() => {
    void mode;
    void multi.length;
    void picked.length;
    void single;
    void k;
    void bySpeed;
    void spreadOff;
    if (!placing.open) return;
    clearTimeout(timer);
    timer = setTimeout(makeOnce, 300);
  });

  function close() {
    placing.open = false;
    opened = "";
    plan = null;
  }
  const toggle = (h: string) => (multi = multi.includes(h) ? multi.filter((x) => x !== h) : hosts.filter((x) => x === h || multi.includes(x)));
  const pick = (h: string) => (picked = picked.includes(h) ? picked.filter((x) => x !== h) : offgrid.filter((x) => x === h || picked.includes(x)));
  const isHost = (name: string) => offgrid.includes(name);

  async function archive() {
    const label = placing.label;
    if (keepLive) await startJob("/v1/replicate", { selector: placing.selector, hosts: [store] }, `Copying ${label} to ${store}`);
    else await startJob("/v1/offload", { selector: placing.selector, store }, `Offloading ${label} to ${store}`);
    close();
  }
</script>

<svelte:window onkeydown={(e) => placing.open && e.key === "Escape" && close()} />
{#if placing.open}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="bg" use:portal onclick={(e) => e.target === e.currentTarget && close()}>
    <div class="dlg panel glowline" role="dialog" aria-modal="true" aria-label="Place {placing.label}">
      <div class="row">
        <h3>Place <span class="spark">{placing.label}</span></h3>
        <span class="spacer"></span>
        <button class="btn ghost sm" onclick={close}>Close</button>
      </div>
      <div class="modes">
        <button class="mode" class:on={mode === "multi"} onclick={() => (mode = "multi")}><b>Multi host</b><span class="tiny muted">spread or replicate</span></button>
        <button class="mode" class:on={mode === "single"} onclick={() => (mode = "single")}><b>Single host</b><span class="tiny muted">gather it on one</span></button>
        <button class="mode" class:on={mode === "offgrid"} disabled={!offgrid.length} onclick={() => (mode = "offgrid")}><b>Off-grid</b><span class="tiny muted">full copies off the fabric</span></button>
        <button class="mode" class:on={mode === "archive"} disabled={!app.stores.length && !offgrid.length} onclick={() => (mode = "archive")}><b>Archive</b><span class="tiny muted">offload it</span></button>
      </div>

      {#if mode === "offgrid"}
        <div class="tiles">
          {#each offgrid as h (h)}
            <button class="tile" class:on={picked.includes(h)} onclick={() => pick(h)}>
              <div class="row"><b>{h}</b>{#if full(h)}<span class="badge">holds it</span>{/if}</div>
              <span class="tiny muted mono">{held(h) ? `holds ${human(held(h))}` : "holds none"}</span>
              <span class="tiny muted mono">{human(free(h))} free</span>
              <span class="use" title="{human(read(h))} read here in 30 days"><i style="width:{useBar(h) * 100}%"></i></span>
            </button>
          {/each}
        </div>
        <label class="check small" class:off={picked.length < 2}>
          <input type="checkbox" bind:checked={spreadOff} disabled={picked.length < 2} />
          spread
          <span class="tiny muted">{picked.length < 2 ? "pick two or more to spread" : spreadOff ? `one copy of each file over the ${picked.length} hosts, about ${Math.round(100 / picked.length)}% each` : "a full copy on each host picked"}</span>
        </label>
        <div class="tiny muted legend">{spreadOff && picked.length > 1 ? "Each file lives on one host picked; the others read it from there over the LAN." : "Each host picked keeps a full copy and reads it from its own disk."} Copies on hosts left unpicked go (never the last one). Fabric hosts are left as they are.</div>
        {#if err}<div class="small" style="color:var(--bad)">{err}</div>{/if}
        {#if plan}
          <div class="preview" class:dim={busy}>
            <div class="row"><h3>Plan</h3><span class="small muted">uncheck anything to leave it as it is; nothing moves until you apply</span></div>
            <PlanPreview bind:plan onapplied={close} />
          </div>
        {:else if busy}<div class="small muted">planning…</div>{/if}
      {:else if mode !== "archive"}
        <div class="tiles">
          {#each hosts as h (h)}
            {@const on = mode === "multi" ? multi.includes(h) : single === h}
            <button class="tile" class:on onclick={() => (mode === "multi" ? toggle(h) : (single = h))}>
              <div class="row"><b>{h}</b>{#if mode === "single" && h === best}<span class="badge spark">best</span>{/if}</div>
              <span class="tiny muted mono">{held(h) ? `holds ${human(held(h))}` : "holds none"}</span>
              <span class="tiny muted mono">{human(free(h))} free</span>
              {#if mode === "multi" && disk(h)}<span class="tiny muted mono">disk {human(disk(h))}/s</span>{/if}
              <span class="use" title="{human(read(h))} read here in 30 days"><i style="width:{useBar(h) * 100}%"></i></span>
            </button>
          {/each}
        </div>
        <div class="tiny muted legend">bar: how much each host read it (most = full){Object.keys(app.disks).length ? " · a host's extra disks are places of their own" : ""}</div>
        {#if mode === "multi"}
          <div class="factor">
            <div class="row">
              <span class="caps">Replicas</span>
              <b class="mono">{k} of {H}</b>
              <span class="small muted">{k === 1 && H > 1 ? "spread: one copy of each file" : k === H ? "every file on every host" : `${k} copies of each file`}</span>
            </div>
            <input type="range" min={1 / H} max="1" step={1 / H} bind:value={factor} disabled={H < 2} aria-label="replica factor" />
            <div class="row tiny muted ends"><span>1/{H} · spread</span><span class="spacer"></span><span>1 · everywhere</span></div>
            <label class="check small" class:off={k === H}>
              <input type="checkbox" bind:checked={bySpeed} disabled={k === H} />
              share by disk speed
              <span class="tiny muted">{k === H ? "every host holds everything" : "faster disks hold more, so a push load finishes on every host together"}</span>
            </label>
          </div>
        {/if}
        {#if err}<div class="small" style="color:var(--bad)">{err}</div>{/if}
        {#if plan}
          <div class="preview" class:dim={busy}>
            <div class="row"><h3>Plan</h3><span class="small muted">uncheck anything to leave it as it is; nothing moves until you apply</span></div>
            <PlanPreview bind:plan onapplied={close} />
          </div>
        {:else if busy}<div class="small muted">planning…</div>{/if}
      {:else}
        <div class="tiles">
          {#each app.stores as s (s.name)}
            {@const g = s.gateways.find(([, x]) => x.healthy)?.[1]}
            <button class="tile" class:on={store === s.name} disabled={!g} onclick={() => (store = s.name)}>
              <b>⧉ {s.name}</b>
              <span class="tiny muted mono">{g ? `${human(g.free_bytes)} free` : "unreachable"}</span>
            </button>
          {/each}
          {#each offgrid as h (h)}
            <button class="tile" class:on={store === h} onclick={() => (store = h)}>
              <b>⌂ {h}</b>
              <span class="tiny muted mono">off-grid · {human(free(h))} free</span>
            </button>
          {/each}
        </div>
        <label class="check small"><input type="checkbox" bind:checked={keepLive} /> keep the copies on fabric hosts too</label>
        <div class="tiny muted">
          {#if isHost(store)}
            {keepLive ? `Copies it onto ${store}; fabric hosts keep theirs.` : `Copies it onto ${store}, then removes the fabric hosts' copies: ${store} keeps it, and the others read it from there over the LAN.`}
          {:else}
            {keepLive ? "Copies it into the archive; hosts keep theirs." : "Copies it into the archive, then removes the fabric hosts' copies. Reads stream through a gateway; place it back on hosts any time."}
          {/if}
        </div>
        <div class="row end"><button class="btn primary" disabled={!store} onclick={archive}>{keepLive ? "Copy" : "Offload"} to {store}</button></div>
      {/if}
    </div>
  </div>
{/if}

<style>
  .bg { position: fixed; inset: 0; z-index: 75; grid-template-columns: minmax(0, 1fr); background: rgba(1, 4, 12, 0.5); display: grid; place-items: center; padding: 16px; backdrop-filter: blur(2px); }
  .dlg { width: min(860px, 100%); max-height: calc(100vh - 32px); overflow: auto; scrollbar-gutter: stable; padding: 20px; display: flex; flex-direction: column; gap: 14px; background: rgba(8, 16, 34, 0.82); animation: rise 0.2s ease-out; }
  h3 { margin: 0; overflow-wrap: anywhere; }
  .spark { color: var(--spark); }
  .modes { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }
  .mode { display: flex; flex-direction: column; gap: 2px; align-items: flex-start; padding: 10px 14px; border-radius: 12px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.45); cursor: pointer; text-align: left; }
  .mode:hover:not(:disabled) { border-color: var(--line-hi); }
  .mode.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .mode:disabled { opacity: 0.4; cursor: default; }
  .tiles { display: grid; grid-template-columns: repeat(auto-fill, minmax(118px, 1fr)); gap: 8px; }
  .tile { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 9px 11px 0; border-radius: 10px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.45); cursor: pointer; text-align: left; overflow: hidden; }
  .tile .row { gap: 6px; }
  .tile:hover:not(:disabled) { border-color: var(--line-hi); }
  .tile.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .tile:disabled { opacity: 0.4; cursor: default; }
  .use { align-self: stretch; height: 5px; margin: 7px -11px 0; background: rgba(90, 140, 220, 0.12); position: relative; }
  .use i { position: absolute; inset: 0 auto 0 0; background: linear-gradient(90deg, var(--spark-2), var(--spark)); box-shadow: 0 0 8px rgba(56, 232, 255, 0.6); }
  .legend { margin-top: -6px; }
  .factor { display: flex; flex-direction: column; gap: 6px; }
  .factor input[type="range"] { width: 100%; accent-color: var(--spark); }
  .factor input[type="checkbox"] { accent-color: var(--spark); }
  .ends { gap: 0; }
  .factor .check { flex-wrap: wrap; }
  .check.off { opacity: 0.5; }
  .preview { display: flex; flex-direction: column; gap: 10px; border-top: 1px solid var(--line); padding-top: 12px; }
  .dim { opacity: 0.6; transition: opacity 0.2s; }
  .end { justify-content: flex-end; }
  @media (max-width: 760px) { .modes { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 420px) { .modes { grid-template-columns: 1fr; } }
</style>
