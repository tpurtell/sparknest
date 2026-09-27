<script lang="ts">
  import { get, type Readiness, type TreeNode, type HostUsage } from "../lib/api";
  import { app, runningJobs, startJob, go } from "../lib/state.svelte";
  import { human, ago, selLabel, splitRepo } from "../lib/format";
  import { copyTo, offload, keepOn, removeFrom, nodeMenu } from "../lib/actions";
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
  $effect(() => {
    void selector;
    void app.tick;
    load();
  });

  const inflight = $derived(
    new Set(
      runningJobs()
        .filter((j) => j.what.endsWith(" " + selector) || j.what.includes(" " + selector + " "))
        .flatMap((j) => Object.entries(j.hosts).filter(([, p]) => !p.finished).map(([h]) => h)),
    ),
  );
  function leaves(n: TreeNode, prefix = ""): { name: string; n: TreeNode }[] {
    if (!n.children?.length) return [{ name: prefix + n.name, n }];
    return n.children.flatMap((c) => leaves(c, n.kind === "repo" ? "" : prefix + n.name + "/"));
  }
  const files = $derived(d ? (d.tree.children ?? []).flatMap((c) => leaves(c)).sort((a, b) => b.n.bytes - a.n.bytes) : []);
  const pctOf = (h: Readiness) => (h.ready ? 100 : Math.min(99, Math.floor(h.bytes ? (100 * (h.bytes - h.missing_bytes)) / h.bytes : 0)));
  const isStore = (name: string) => app.stores.some((s) => s.name === name);
  let showAll = $state(false);
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
      <button class="btn primary" onclick={() => copyTo(selector, ["@all"])}><Icon name="sparkle" size={15} /> Copy to every host</button>
      <button class="btn" onclick={() => copyTo(selector)}><Icon name="copy" size={15} /> Copy to…</button>
      <button class="btn" onclick={() => keepOn(selector)}><Icon name="pin" size={15} /> Keep on…</button>
      <button class="btn" onclick={() => offload(selector)} disabled={!app.stores.length}><Icon name="archive" size={15} /> Offload…</button>
      <button class="btn danger" onclick={() => removeFrom(selector)}><Icon name="trash" size={15} /> Remove from…</button>
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
            <div class="row"><b>{isStore(h.host) ? "⧉ " : ""}{h.host}</b><span class="spacer"></span>
              <span class="mono small" class:spark={h.ready}>{h.ready ? "complete" : p + "%"}</span></div>
            <div class="bar" class:live={fly}><i style="width:{p}%"></i></div>
            <div class="tiny muted">
              {#if !h.ready}{h.files - h.missing_files}/{h.files} files · {human(h.missing_bytes)} missing<br />{/if}
              {#if u?.opens}opened {ago(u.last_open_ms)} · {u.opens}×{#if u.net_bytes} · {human(u.net_bytes)} over the network{/if}
              {:else if !isStore(h.host)}not opened in 30 days{/if}
            </div>
            <div class="row">
              {#if fly}<span class="tiny spark">copying…</span>
              {:else if !h.ready}<button class="btn sm" onclick={() => startJob("/v1/replicate", { selector, hosts: [h.host] }, `Copying ${selLabel(selector)} to ${h.host}`)}>Copy here</button>{/if}
              {#if p > 0}<button class="btn sm ghost" onclick={() => removeFrom(selector, [h.host])}>Remove</button>{/if}
            </div>
          </div>
        {/each}
      </div>
    </section>

    <section>
      <h3 class="sec">Files</h3>
      <Treemap root={d.tree} height="280px" crumbs={false} oncontext={(n, x, y) => nodeMenu(n, x, y, null)} />
      <table class="t files">
        <thead><tr><th>File</th><th class="num">Size</th><th>On</th><th>Use</th></tr></thead>
        <tbody>
          {#each showAll ? files : files.slice(0, 40) as f (f.name + (f.n.file ?? ""))}
            {@const fu = f.n.file ? d.file_usage[String(f.n.file)] : undefined}
            {@const last = fu ? Math.max(0, ...Object.values(fu).map((x) => x.last_open_ms)) : 0}
            <tr>
              <td class="fname">{f.name}</td>
              <td class="num mono">{human(f.n.bytes)}</td>
              <td><div class="dots">{#each d.hosts as h}<span class="hd" class:on={f.n.hosts?.includes(h.host)} class:arch={isStore(h.host)} title={h.host}></span>{/each}</div></td>
              <td class="tiny muted">{last ? ago(last) : ""}</td>
            </tr>
          {/each}
        </tbody>
      </table>
      {#if files.length > 40 && !showAll}<button class="btn sm ghost" onclick={() => (showAll = true)}>Show all {files.length}</button>{/if}
    </section>
    <div class="row"><button class="btn ghost sm" onclick={() => go("space", "", { shape: "models" })}><Icon name="space" size={14} /> See it among everything</button></div>
  </div>
{/if}

<style>
  .title { font-size: 22px; margin: 2px 0 4px; word-break: break-word; }
  .path { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 380px; }
  .sec { margin-bottom: 10px; color: var(--muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.12em; font-size: 11px; }
  .hosts { display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap: 8px; }
  .host { padding: 10px 12px; display: flex; flex-direction: column; gap: 7px; }
  .host.full { border-color: rgba(56, 232, 255, 0.4); box-shadow: 0 0 18px rgba(56, 232, 255, 0.12); }
  .host.fly { border-color: var(--spark); }
  .spark { color: var(--spark); }
  .files { margin-top: 10px; }
  .fname { word-break: break-all; font-size: 13px; }
  .dots { display: flex; gap: 3px; flex-wrap: wrap; max-width: 220px; }
  .hd { width: 9px; height: 9px; border-radius: 3px; background: rgba(90, 140, 220, 0.15); }
  .hd.on { background: var(--spark); box-shadow: 0 0 6px var(--spark); }
  .hd.arch.on { background: var(--violet); box-shadow: 0 0 6px var(--violet); }
</style>
