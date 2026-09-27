<script lang="ts">
  import { get, type Readiness, type TreeNode, type HostUsage } from "../lib/api";
  import { app, runningJobs, startJob, go, singleFlight } from "../lib/state.svelte";
  import { human, ago, selLabel, splitRepo } from "../lib/format";
  import { copyTo, keepOn, removeFrom, nodeMenu, deleteModel } from "../lib/actions";
  import { openPlace } from "../lib/ui.svelte";
  import Treemap from "../components/Treemap.svelte";
  import Icon from "../components/Icon.svelte";

  let { selector }: { selector: string } = $props();

  interface Detail {
    repo: string;
    kind: string;
    selector: string;
    path: string;
    hosts: Readiness[];
    tree: TreeNode;
    revisions: { commit: string; refs: string[] }[];
    rules: { name: string; hosts: string[]; auto: boolean }[];
    usage: Record<string, HostUsage>;
    file_usage: Record<string, Record<string, HostUsage>>;
  }
  let d = $state<Detail | null>(null);
  let err = $state("");
  async function load() {
    try {
      d = await get<Detail>("/v1/hf/detail?selector=" + encodeURIComponent(selector));
      err = "";
    } catch (e) {
      err = (e as Error).message;
    }
  }
  const refresh = singleFlight(load);
  $effect(() => {
    void selector;
    void app.changed;
    refresh();
  });

  const inflight = $derived(
    new Set(
      runningJobs()
        .filter((j) => j.what.endsWith(" " + selector) || j.what.includes(" " + selector + " "))
        .flatMap((j) => Object.entries(j.hosts).filter(([, p]) => !p.finished).map(([h]) => h)),
    ),
  );
  // The block under the pointer, described below the treemap.
  let hover = $state<{ n: TreeNode; path: string[] } | null>(null);
  const hoverUse = $derived.by(() => {
    const f = hover?.n.file;
    const fu = f !== undefined && d ? d.file_usage[String(f)] : undefined;
    if (!fu) return null;
    const last = Math.max(0, ...Object.values(fu).map((x) => x.last_open_ms));
    const net = Object.values(fu).reduce((a, x) => a + x.net_bytes, 0);
    return { last, net, where: Object.entries(fu).sort((a, b) => b[1].last_open_ms - a[1].last_open_ms)[0]?.[0] };
  });
  const pctOf = (h: Readiness) => (h.ready ? 100 : Math.min(99, Math.floor(h.bytes ? (100 * (h.bytes - h.missing_bytes)) / h.bytes : 0)));
  const isStore = (name: string) => app.stores.some((s) => s.name === name);
</script>

{#if err}<p style="color:var(--bad)">{err}</p>{/if}
{#if d}
  <div class="stack">
    <div>
      <div class="caps">{d.kind}</div>
      <h2 class="title"><span class="muted">{splitRepo(d.repo)[0]}</span>{splitRepo(d.repo)[1]}</h2>
      <div class="row small muted mono">
        <span>{human(d.tree.bytes)}</span><span>·</span><span>{d.tree.files} files</span><span>·</span><span class="path">{d.path}</span>
      </div>
    </div>

    <div class="row">
      <button class="btn primary" onclick={() => openPlace(selector, d!.repo)} title="Spread, replicate, gather on one host, or archive"><Icon name="target" size={15} /> Place…</button>
      <button class="btn" onclick={() => copyTo(selector)}><Icon name="copy" size={15} /> Copy to…</button>
      <button class="btn" onclick={() => keepOn(selector)}><Icon name="pin" size={15} /> Keep on…</button>
      <button class="btn danger" onclick={() => removeFrom(selector)}><Icon name="trash" size={15} /> Remove from…</button>
      <button class="btn danger" onclick={() => deleteModel(selector)} title="Delete the repo from the hub with hf: every copy, every host"><Icon name="trash" size={15} /> Delete model</button>
    </div>

    {#if d.revisions.length || d.rules.length}
      <div class="row">
        {#each d.revisions as r}
          <span class="badge spark mono" title={r.commit}>{r.commit.slice(0, 10)}{r.refs.length ? " ← " + r.refs.join(", ") : ""}</span>
        {/each}
        {#each d.rules as r}
          <span class="badge ok" title="rule {r.name}">rule {r.name} → {r.hosts.join(", ")}{r.auto ? " · auto" : ""}</span>
        {/each}
      </div>
    {/if}

    <section>
      <h3 class="sec">Where it is</h3>
      <div class="hosts">
        {#each d.hosts as h (h.host)}
          {@const p = pctOf(h)}
          {@const u = d.usage[h.host]}
          {@const fly = inflight.has(h.host)}
          <div class="host panel" class:full={h.ready} class:fly>
            <div class="row hrow">
              <b class="hname">{isStore(h.host) ? "⧉ " : ""}{h.host}</b>
              <span class="mono small" class:spark={h.ready}>{h.ready ? "complete" : p + "%"}</span>
              <span class="spacer"></span>
              {#if fly}<span class="tiny spark">copying…</span>
              {:else if !h.ready}<button class="btn sm ghost ib" title="Copy the rest here" onclick={() => startJob("/v1/replicate", { selector, hosts: [h.host] }, `Copying ${selLabel(selector)} to ${h.host}`)}><Icon name="copy" size={13} /></button>{/if}
              {#if p > 0}<button class="btn sm ghost ib" title="Remove from {h.host}" onclick={() => removeFrom(selector, [h.host])}><Icon name="trash" size={13} /></button>{/if}
            </div>
            <div class="bar" class:live={fly}><i style="width:{p}%"></i></div>
            <div class="tiny muted hline" title={u?.opens ? `opened ${ago(u.last_open_ms)}, ${u.opens}×` : ""}>
              {#if !h.ready}{h.files - h.missing_files}/{h.files} · {human(h.missing_bytes)} missing{#if u?.opens} · {/if}{/if}{#if u?.opens}opened {ago(u.last_open_ms)}{#if u.net_bytes} · {human(u.net_bytes)} net{/if}{:else if h.ready && !isStore(h.host)}not opened in 30 days{/if}
            </div>
          </div>
        {/each}
      </div>
    </section>

    <section>
      <h3 class="sec">Files</h3>
      <Treemap root={d.tree} height="clamp(220px, calc(100vh - 600px), 600px)" tip={false} onhover={(h) => (hover = h)}
        oncontext={(n, x, y) => nodeMenu(n, x, y, null)} />
      <div class="info">
        {#if hover}
          <div class="tiny faint ipath">{hover.path.slice(0, -1).join(" › ")}</div>
          <div class="row irow">
            <b class="iname">{hover.n.name}</b>
            <span class="mono small spark">{human(hover.n.bytes)}</span>
            {#if hover.n.files > 1}<span class="mono small muted">{hover.n.files.toLocaleString()} files</span>{/if}
            <span class="spacer"></span>
            <div class="dots">{#each d.hosts as h}<span class="hd" class:on={hover.n.hosts?.includes(h.host)} class:arch={isStore(h.host)} title={h.host}></span>{/each}</div>
          </div>
          <div class="tiny muted">
            {hover.n.hosts?.length ? `on ${hover.n.hosts.join(", ")}` : "no copy"}
            {#if hoverUse?.last} · opened {ago(hoverUse.last)}{hoverUse.where ? ` on ${hoverUse.where}` : ""}{hoverUse.net ? ` · ${human(hoverUse.net)} over the network` : ""}{/if}
            · {hover.n.children?.length || hover.n.truncated ? "click to zoom in" : "click for actions"}
          </div>
        {:else}
          <div class="tiny muted hint">Point at a block for its details · click a folder to zoom in, <b>Out</b> above to zoom back · right-click for actions</div>
        {/if}
      </div>
    </section>
    <div class="row"><button class="btn ghost sm" onclick={() => go("space", "", { shape: "models" })}><Icon name="space" size={14} /> See it among everything</button></div>
  </div>
{/if}

<style>
  .title { font-size: 22px; margin: 2px 0 4px; word-break: break-word; }
  .path { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 380px; }
  .sec { margin-bottom: 10px; color: var(--muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.12em; font-size: 11px; }
  .hosts { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: 6px; }
  .host { padding: 7px 10px 8px; display: flex; flex-direction: column; gap: 5px; }
  .hrow { flex-wrap: nowrap; gap: 6px; min-height: 24px; }
  .hname { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
  .ib { padding: 3px 6px; }
  .hline { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-height: 15px; }
  .host.full { border-color: rgba(56, 232, 255, 0.4); box-shadow: 0 0 18px rgba(56, 232, 255, 0.12); }
  .host.fly { border-color: var(--spark); }
  .spark { color: var(--spark); }
  .info { min-height: 64px; margin-top: 8px; padding: 8px 12px; border-radius: 10px; border: 1px solid var(--line); background: rgba(4, 12, 28, 0.35); display: flex; flex-direction: column; gap: 3px; }
  .ipath, .iname { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .iname { min-width: 0; max-width: 60%; }
  .irow { flex-wrap: nowrap; gap: 8px; }
  .hint { margin: auto 0; }
  .dots { display: flex; gap: 3px; flex-wrap: wrap; max-width: 220px; }
  .hd { width: 9px; height: 9px; border-radius: 3px; background: rgba(90, 140, 220, 0.15); }
  .hd.on { background: var(--spark); box-shadow: 0 0 6px var(--spark); }
  .hd.arch.on { background: var(--violet); box-shadow: 0 0 6px var(--violet); }
</style>
