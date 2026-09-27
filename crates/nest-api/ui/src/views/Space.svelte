<script lang="ts">
  import Treemap from "../components/Treemap.svelte";
  import { get, type TreeNode, type SpaceStore } from "../lib/api";
  import { app, route, go, runningJobs, singleFlight } from "../lib/state.svelte";
  import { human, pct } from "../lib/format";
  import { nodeMenu } from "../lib/actions";

  // Scope, weight, shape and free space live in the URL so links and the
  // overview's host clicks land here set up.
  const scope = $derived(route.params.get("scope") ?? "all");
  const weight = $derived(route.params.get("weight") ?? "logical");
  const shape = $derived(route.params.get("shape") ?? "models");
  const withFree = $derived(route.params.get("free") === "1");

  const set = (k: string, v: string) => {
    const p = Object.fromEntries(route.params);
    p[k] = v;
    go("space", "", p);
  };

  let tree = $state<TreeNode | null>(null);
  let stores = $state<SpaceStore[]>([]);
  let err = $state("");
  let loading = $state(false);

  async function load() {
    loading = true;
    try {
      const q = new URLSearchParams({ weight, shape, depth: shape === "path" ? "5" : "6" });
      if (scope !== "all") q.set("scope", scope);
      const v = await get<{ tree: TreeNode; stores: SpaceStore[] }>("/v1/space/tree?" + q);
      tree = v.tree;
      stores = v.stores;
      err = "";
    } catch (e) {
      err = (e as Error).message;
    }
    loading = false;
  }
  const refresh = singleFlight(load);
  $effect(() => {
    void scope;
    void weight;
    void shape;
    load();
  });
  // Refresh quietly while copies are running.
  $effect(() => {
    void app.changed;
    refresh();
  });

  const root = $derived.by<TreeNode | null>(() => {
    if (!tree) return null;
    const name = scope === "all" ? "everything" : scope;
    const base: TreeNode = { ...tree, name };
    if (!withFree || !stores.length) return base;
    const free: TreeNode[] = stores.map((s) => ({ name: `free on ${s.name}`, kind: "free" as const, bytes: s.free, files: 0 }));
    const freeNode: TreeNode =
      free.length === 1 ? free[0] : { name: "free space", kind: "group", bytes: free.reduce((a, f) => a + f.bytes, 0), files: 0, children: free };
    return {
      name,
      kind: "group",
      bytes: base.bytes + freeNode.bytes,
      files: base.files,
      children: [...(base.children ?? []), freeNode],
    };
  });

  async function deeper(n: TreeNode): Promise<TreeNode | null> {
    if (!n.path) return null;
    const q = new URLSearchParams({ weight, shape: "path", root: n.path, depth: "5" });
    if (scope !== "all") q.set("scope", scope);
    return (await get<{ tree: TreeNode }>("/v1/space/tree?" + q)).tree;
  }

  // Blocks with a copy in flight toward this scope shimmer.
  const busySel = $derived(
    new Set(
      runningJobs()
        .filter((j) => scope === "all" || j.hosts[scope])
        .map((j) => j.what.replace(/^(replicate|offload) /, "").split(" ")[0]),
    ),
  );
  const busy = (n: TreeNode) => !!n.selector && busySel.has(n.selector);

  const scopes = $derived([
    { name: "all", label: "Whole cluster", kind: "all" },
    ...(app.status?.nodes ?? []).map((n) => ({ name: n.name, label: n.name, kind: "host" })),
    ...app.stores.map((s) => ({ name: s.name, label: s.name, kind: "archive" })),
  ]);
  const held = $derived(stores.reduce((a, s) => a + s.held, 0));
  const cap = $derived(stores.reduce((a, s) => a + s.total, 0));
  const free = $derived(stores.reduce((a, s) => a + s.free, 0));
</script>

<div class="stack">
  <div class="panel pad controls">
    <div class="row">
      <h2>Space</h2>
      <span class="small muted">{scope === "all" ? "everything sparknest holds" : `what ${scope} holds`}{loading ? " · loading…" : ""}</span>
    </div>
    <div class="chips scopes">
      {#each scopes as s}
        <button class="chip" class:on={scope === s.name} onclick={() => set("scope", s.name)}>
          {#if s.kind === "archive"}⧉ {/if}{s.label}
        </button>
      {/each}
    </div>
    <div class="row opts">
      <div class="chips">
        <button class="chip" class:on={shape === "models"} onclick={() => set("shape", "models")}>By model</button>
        <button class="chip" class:on={shape === "path"} onclick={() => set("shape", "path")}>By path</button>
      </div>
      {#if scope === "all"}
        <div class="chips">
          <button class="chip" class:on={weight === "logical"} onclick={() => set("weight", "logical")} title="Each file once">Size</button>
          <button class="chip" class:on={weight === "copies"} onclick={() => set("weight", "copies")} title="Size times copies: what it costs the cluster">Cost with copies</button>
        </div>
      {/if}
      <label class="check small"><input type="checkbox" checked={withFree} onchange={(e) => set("free", e.currentTarget.checked ? "1" : "0")} /> show free space</label>
      <span class="spacer"></span>
      {#if cap}
        <span class="mono small muted">{human(held)} held · {human(free)} free of {human(cap)}</span>
        <div class="cap"><i class="other" style="width:{pct(cap - free - held, cap)}%"></i><i class="ours" style="width:{pct(held, cap)}%"></i></div>
      {/if}
    </div>
  </div>

  {#if err}<div class="panel pad" style="color:var(--bad)">{err}</div>{/if}
  {#if root}
    {#if root.bytes > 0}
      <div class="panel pad">
        <Treemap {root} {busy} onload={deeper}
          oncontext={(n, x, y) => nodeMenu(n, x, y, scope === "all" ? null : scope)} />
      </div>
    {:else}
      <div class="panel empty">Nothing here yet.</div>
    {/if}
  {/if}
</div>

<style>
  .controls { display: flex; flex-direction: column; gap: 12px; }
  .opts { gap: 14px; }
  .cap { width: 160px; height: 7px; border-radius: 99px; background: rgba(90, 140, 220, 0.12); overflow: hidden; display: flex; }
  .cap i { display: block; height: 100%; }
  .cap .other { background: #34445f; }
  .cap .ours { background: linear-gradient(90deg, var(--spark-2), var(--spark)); box-shadow: 0 0 10px rgba(56, 232, 255, 0.7); }
  @media (max-width: 899px) {
    .scopes { flex-wrap: nowrap; overflow-x: auto; padding-bottom: 4px; }
    .cap { width: 100%; }
  }
</style>
