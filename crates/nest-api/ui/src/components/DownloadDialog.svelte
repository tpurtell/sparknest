<script lang="ts">
  // Download a repo from the Hub: search (hf's model/dataset listing, most
  // downloaded first) or paste org/name, see its size (hf's dry run), then
  // pick where as in Place (ADR-075): over several fabric places (each file
  // lands on one, evenly), on one place, over off-grid hosts, or in an
  // archive store (ADR-055; the request goes to this node, so only archives
  // it is a gateway of are offered). A host's extra disks are places of
  // their own. One copy of each file: the Hub is fetched once. A repo with
  // branches (turboderp's quants live one per branch) offers them; each
  // downloaded branch is its own model.
  import { get } from "../lib/api";
  import { app, startJob, places } from "../lib/state.svelte";
  import { human } from "../lib/format";
  import { portal } from "../lib/portal";

  let { open = $bindable(false) }: { open: boolean } = $props();

  type Hit = { id: string; downloads?: number; likes?: number; pipeline_tag?: string };
  let kind = $state<"model" | "dataset">("model");
  let q = $state("");
  let results = $state<Hit[]>([]);
  let searching = $state(false);
  let repo = $state("");
  type Ref = { name: string; sha: string; cached: "current" | "older" | "" };
  let refs = $state<Ref[]>([]);
  let branch = $state("");
  // What is downloaded: org/name, or org/name@branch off the default one.
  const target = $derived(branch && branch !== "main" ? `${repo}@${branch}` : repo);
  let size = $state<{ files: number; missing_files: number; missing_bytes: number } | null>(null);
  let sizeErr = $state("");
  type Mode = "multi" | "single" | "offgrid" | "archive";
  let mode = $state<Mode>("multi");
  let single = $state("");
  let multi = $state<string[]>([]);
  let picked = $state<string[]>([]);
  let archive = $state("");

  // Places in the cluster's order, as in Place: each host, then its extra
  // disks; fabric ones and off-grid ones apart.
  const serving = (h: string) => !!app.status?.nodes.find((n) => n.name === h)?.info?.serving;
  const all = $derived(places().filter((p) => p.up && serving(p.host)));
  const fabric = $derived(all.filter((p) => !p.offgrid));
  const offgrid = $derived(all.filter((p) => p.offgrid));
  const hosts = $derived(fabric.filter((p) => !p.disk));
  const roomiest = $derived([...fabric].sort((a, b) => b.free - a.free)[0]?.name ?? "");
  $effect(() => {
    if (open && !fabric.some((p) => p.name === single) && fabric.length) single = roomiest;
  });
  // A spread starts on the fabric hosts' own disks; extra disks and
  // off-grid hosts only when picked.
  $effect(() => {
    if (open && !multi.length && hosts.length) multi = hosts.map((p) => p.name);
  });
  // Archives this node reaches, healthy, with their free space here.
  const me = $derived(app.status?.nodes.find((n) => n.node === app.status?.me)?.name ?? "");
  const archives = $derived(
    app.stores
      .map((s) => ({ name: s.name, gw: s.gateways.find(([g]) => g === me)?.[1] }))
      .filter((s) => s.gw?.healthy)
      .map((s) => ({ name: s.name, free: s.gw?.free_bytes ?? 0 })),
  );
  $effect(() => {
    if (open && !archives.some((a) => a.name === archive) && archives.length) archive = archives[0].name;
  });
  const toggle = (h: string) => (multi = multi.includes(h) ? multi.filter((x) => x !== h) : fabric.map((x) => x.name).filter((x) => x === h || multi.includes(x)));
  const pick = (h: string) => (picked = picked.includes(h) ? picked.filter((x) => x !== h) : offgrid.map((x) => x.name).filter((x) => x === h || picked.includes(x)));
  // The places the download goes to (archive aside).
  const targets = $derived(mode === "multi" ? multi : mode === "single" ? (single ? [single] : []) : mode === "offgrid" ? picked : []);

  // A pasted repo id or huggingface.co URL is offered as is; a link into a
  // branch (…/tree/<branch>) or org/name@branch picks that branch.
  const pasted = $derived.by(() => {
    let t = q.trim().replace(/[?#].*$/, "").replace(/^https?:\/\/huggingface\.co\/(datasets\/|models\/)?/, "").replace(/\/+$/, "");
    let b = "";
    const tree = /^([^/]+\/[^/]+)\/(?:tree|blob|resolve)\/([^/]+)/.exec(t);
    if (tree) [t, b] = [tree[1], decodeURIComponent(tree[2])];
    else if (t.includes("@")) [t, b] = [t.slice(0, t.indexOf("@")), t.slice(t.indexOf("@") + 1)];
    return /^[\w.-]+(\/[\w.-]+)?$/.test(t) && t.includes("/") ? { id: t, branch: b } : null;
  });

  let timer: ReturnType<typeof setTimeout> | undefined;
  $effect(() => {
    const text = q.trim();
    const k = kind;
    clearTimeout(timer);
    if (text.length < 2) {
      results = [];
      return;
    }
    timer = setTimeout(async () => {
      searching = true;
      try {
        const r = await get<{ results: Hit[] }>(`/v1/hf/search?q=${encodeURIComponent(text)}&kind=${k}&limit=15`);
        if (q.trim() === text) results = r.results ?? [];
      } catch {
        results = [];
      } finally {
        searching = false;
      }
    }, 350);
  });

  function choose(id: string, want = "") {
    repo = id;
    refs = [];
    branch = want;
    sizeUp();
    get<{ branches: Ref[] }>(`/v1/hf/refs?repo=${encodeURIComponent(id)}&kind=${kind}`)
      .then((r) => {
        if (repo !== id) return;
        refs = r.branches ?? [];
        // A repo whose main holds only a README (one quant per branch)
        // still starts on main; the picker shows the rest.
        if (!branch && refs.length) branch = refs.some((b) => b.name === "main") ? "main" : refs[0].name;
      })
      .catch(() => {});
  }
  function sizeUp() {
    const t = target;
    size = null;
    sizeErr = "";
    get<{ files: number; missing_files: number; missing_bytes: number }>(`/v1/hf/size?repo=${encodeURIComponent(t)}&kind=${kind}`)
      .then((s) => {
        if (target === t) size = s;
      })
      .catch((e) => {
        if (target === t) sizeErr = (e as Error).message;
      });
  }
  function pickBranch(b: string) {
    if (b === branch) return;
    branch = b;
    sizeUp();
  }

  const free = $derived(
    mode === "archive"
      ? (archives.find((a) => a.name === archive)?.free ?? 0)
      : all.filter((p) => targets.includes(p.name)).reduce((a, p) => a + p.free, 0),
  );
  const fits = $derived(!size || free > size.missing_bytes);

  function close() {
    open = false;
    q = "";
    repo = "";
    refs = [];
    branch = "";
    results = [];
    size = null;
    sizeErr = "";
    multi = [];
    picked = [];
  }

  async function download() {
    const t = target;
    if (mode === "archive") await startJob("/v1/hf/download", { repo: t, kind, archive }, `Downloading ${t} into ${archive}`);
    else if (targets.length === 1) await startJob("/v1/hf/download", { repo: t, kind, host: targets[0] }, `Downloading ${t} to ${targets[0]}`);
    else {
      const everyone = mode === "multi" && multi.length === hosts.length && multi.every((m) => hosts.some((h) => h.name === m));
      await startJob("/v1/hf/download", { repo: t, kind, hosts: everyone ? [] : targets }, `Downloading ${t} over ${everyone ? "every host" : targets.join(", ")}`);
    }
    close();
  }
</script>

<svelte:window onkeydown={(e) => open && e.key === "Escape" && close()} />
{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="bg" use:portal onclick={(e) => e.target === e.currentTarget && close()}>
    <div class="dlg panel glowline" role="dialog" aria-modal="true" aria-label="Download from the Hub">
      <div class="row">
        <h3>Download from the Hub</h3>
        <span class="spacer"></span>
        <div class="chips">
          <button class="chip" class:on={kind === "model"} onclick={() => ((kind = "model"), (repo = ""), (refs = []))}>Model</button>
          <button class="chip" class:on={kind === "dataset"} onclick={() => ((kind = "dataset"), (repo = ""), (refs = []))}>Dataset</button>
        </div>
      </div>
      <!-- svelte-ignore a11y_autofocus -->
      <input type="search" placeholder="search, or paste org/name" bind:value={q} autofocus />
      <div class="hits">
        {#if pasted && (pasted.branch || !results.some((r) => r.id === pasted.id))}
          <button class="hit" class:on={repo === pasted.id && (!pasted.branch || branch === pasted.branch)} onclick={() => choose(pasted.id, pasted.branch)}><b>{pasted.id}{pasted.branch ? "@" + pasted.branch : ""}</b><span class="tiny muted">as typed</span></button>
        {/if}
        {#each results as r (r.id)}
          <button class="hit" class:on={repo === r.id} onclick={() => choose(r.id)}>
            <b>{r.id}</b>
            <span class="tiny muted mono">{r.pipeline_tag ?? ""}{r.downloads ? ` · ${r.downloads.toLocaleString()} downloads` : ""}</span>
          </button>
        {/each}
        {#if searching}<div class="tiny muted">searching…</div>
        {:else if q.trim().length >= 2 && !results.length && !pasted}<div class="tiny muted">no matches</div>{/if}
      </div>
      {#if repo}
        <div class="pick">
          {#if refs.length > 1}
            <div class="row small">
              <span class="muted">branch</span>
              <select value={branch} onchange={(e) => pickBranch((e.currentTarget as HTMLSelectElement).value)}>
                {#each refs as b (b.name)}<option value={b.name}>{b.name}{b.cached === "current" ? " · downloaded" : b.cached === "older" ? " · older copy here" : ""}</option>{/each}
              </select>
            </div>
          {/if}
          <div class="small">
            <b>{target}</b>:
            {#if size}{size.missing_files === size.files ? `${size.files} files, ${human(size.missing_bytes)}` : size.missing_files ? `${size.missing_files} of ${size.files} files missing, ${human(size.missing_bytes)}` : "already complete in the cluster"}{:else if sizeErr}<span style="color:var(--bad)">{sizeErr}</span>{:else}<span class="muted">sizing…</span>{/if}
          </div>
          <div class="modes">
            <button class="mode" class:on={mode === "multi"} onclick={() => (mode = "multi")}><b>Multi host</b><span class="tiny muted">spread over them</span></button>
            <button class="mode" class:on={mode === "single"} onclick={() => (mode = "single")}><b>Single host</b><span class="tiny muted">all on one</span></button>
            <button class="mode" class:on={mode === "offgrid"} disabled={!offgrid.length} onclick={() => (mode = "offgrid")}><b>Off-grid</b><span class="tiny muted">off the fabric</span></button>
            <button class="mode" class:on={mode === "archive"} disabled={!archives.length} onclick={() => (mode = "archive")}><b>Archive</b><span class="tiny muted">into a store</span></button>
          </div>
          {#if mode === "archive"}
            <div class="tiles">
              {#each archives as a (a.name)}
                <button class="tile" class:on={archive === a.name} onclick={() => (archive = a.name)}>
                  <b>⧉ {a.name}</b>
                  <span class="tiny muted mono">{human(a.free)} free</span>
                </button>
              {/each}
            </div>
          {:else}
            <div class="tiles">
              {#each mode === "offgrid" ? offgrid : fabric as p (p.name)}
                {@const on = targets.includes(p.name)}
                <button class="tile" class:on onclick={() => (mode === "multi" ? toggle(p.name) : mode === "offgrid" ? pick(p.name) : (single = p.name))}>
                  <b>{mode === "offgrid" ? "⌂ " : ""}{p.name}</b>
                  <span class="tiny muted mono">{human(p.free)} free</span>
                  {#if p.disk}<span class="tiny muted mono">a disk of {p.host}</span>{/if}
                </button>
              {/each}
            </div>
          {/if}
          {#if !fits}<span class="tiny" style="color:var(--bad)">does not fit there</span>{/if}
          <div class="tiny muted">
            {mode === "archive"
              ? `Every file goes into the archive, through ${me}; read through it until copied onto hosts.`
              : targets.length > 1
                ? `Each file goes to one of these ${targets.length}, evenly by size; four download at a time.`
                : "Every file goes here."}
            {all.some((p) => p.disk && targets.includes(p.name)) ? "A file for a host's extra disk lands on its own disk first, then moves over." : ""}
            Only what no host has is fetched; what the cluster already holds stays where it is.
          </div>
        </div>
      {/if}
      <div class="row end">
        <button class="btn ghost" onclick={close}>Cancel</button>
        <button class="btn primary" disabled={!repo || (mode === "archive" ? !archive : !targets.length) || !fits || !!sizeErr || size?.missing_files === 0} onclick={download}>Download</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .bg { position: fixed; inset: 0; z-index: 85; background: rgba(1, 4, 12, 0.6); display: grid; grid-template-columns: minmax(0, 1fr); place-items: center; padding: 16px; backdrop-filter: blur(3px); }
  .dlg { width: min(720px, 100%); max-height: calc(100dvh - 32px); overflow: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; background: rgba(8, 16, 34, 0.94); animation: rise 0.2s ease-out; }
  h3 { margin: 0; overflow-wrap: anywhere; }
  input[type="search"] { width: 100%; }
  .hits { display: flex; flex-direction: column; gap: 4px; max-height: 40vh; overflow: auto; scrollbar-gutter: stable; }
  .hit { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 8px 10px; border-radius: 8px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.55); cursor: pointer; text-align: left; }
  .hit:hover { border-color: var(--line-hi); }
  .hit.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .pick { display: flex; flex-direction: column; gap: 6px; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--line); }
  select { max-width: 100%; }
  .modes { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }
  .mode { display: flex; flex-direction: column; gap: 2px; align-items: flex-start; padding: 8px 12px; border-radius: 12px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.45); cursor: pointer; text-align: left; }
  .mode:hover:not(:disabled) { border-color: var(--line-hi); }
  .mode.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .mode:disabled { opacity: 0.4; cursor: default; }
  .tiles { display: grid; grid-template-columns: repeat(auto-fill, minmax(118px, 1fr)); gap: 8px; }
  .tile { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 9px 11px; border-radius: 10px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.45); cursor: pointer; text-align: left; overflow: hidden; }
  .tile:hover:not(:disabled) { border-color: var(--line-hi); }
  .tile.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  @media (max-width: 560px) { .modes { grid-template-columns: repeat(2, 1fr); } }
  .end { justify-content: flex-end; }
</style>
