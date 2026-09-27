<script lang="ts">
  // Download a repo from the Hub to a host: search (hf's model/dataset
  // listing, most downloaded first) or paste org/name, see its size (hf's
  // dry run), pick the host (most free space first).
  import { get } from "../lib/api";
  import { app, startJob } from "../lib/state.svelte";
  import { human } from "../lib/format";

  let { open = $bindable(false) }: { open: boolean } = $props();

  type Hit = { id: string; downloads?: number; likes?: number; pipeline_tag?: string };
  let kind = $state<"model" | "dataset">("model");
  let q = $state("");
  let results = $state<Hit[]>([]);
  let searching = $state(false);
  let repo = $state("");
  let size = $state<{ files: number; bytes: number } | null>(null);
  let sizeErr = $state("");
  let host = $state("");

  const hosts = $derived(
    (app.status?.nodes ?? [])
      .filter((n) => n.info?.serving)
      .map((n) => ({ name: n.name, free: n.info?.free_bytes ?? 0 }))
      .sort((a, b) => b.free - a.free),
  );
  $effect(() => {
    if (open && !hosts.some((h) => h.name === host) && hosts.length) host = hosts[0].name;
  });

  // A pasted repo id or huggingface.co URL is offered as is.
  const pasted = $derived.by(() => {
    const t = q.trim().replace(/^https?:\/\/huggingface\.co\/(datasets\/)?/, "").replace(/\/+$/, "");
    return /^[\w.-]+(\/[\w.-]+)?$/.test(t) && t.includes("/") ? t : "";
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

  function choose(id: string) {
    repo = id;
    size = null;
    sizeErr = "";
    get<{ files: number; bytes: number }>(`/v1/hf/size?repo=${encodeURIComponent(id)}&kind=${kind}`)
      .then((s) => {
        if (repo === id) size = s;
      })
      .catch((e) => {
        if (repo === id) sizeErr = (e as Error).message;
      });
  }

  const free = $derived(hosts.find((h) => h.name === host)?.free ?? 0);
  const fits = $derived(!size || free > size.bytes);

  function close() {
    open = false;
    q = "";
    repo = "";
    results = [];
    size = null;
    sizeErr = "";
  }

  async function download() {
    await startJob("/v1/hf/download", { repo, kind, host }, `Downloading ${repo} to ${host}`);
    close();
  }
</script>

<svelte:window onkeydown={(e) => open && e.key === "Escape" && close()} />
{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="bg" onclick={(e) => e.target === e.currentTarget && close()}>
    <div class="dlg panel glowline" role="dialog" aria-modal="true" aria-label="Download from the Hub">
      <div class="row">
        <h3>Download from the Hub</h3>
        <span class="spacer"></span>
        <div class="chips">
          <button class="chip" class:on={kind === "model"} onclick={() => ((kind = "model"), (repo = ""))}>Model</button>
          <button class="chip" class:on={kind === "dataset"} onclick={() => ((kind = "dataset"), (repo = ""))}>Dataset</button>
        </div>
      </div>
      <!-- svelte-ignore a11y_autofocus -->
      <input type="search" placeholder="search, or paste org/name" bind:value={q} autofocus />
      <div class="hits">
        {#if pasted && !results.some((r) => r.id === pasted)}
          <button class="hit" class:on={repo === pasted} onclick={() => choose(pasted)}><b>{pasted}</b><span class="tiny muted">as typed</span></button>
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
          <div class="small">
            <b>{repo}</b>:
            {#if size}{size.files} files, {human(size.bytes)}{:else if sizeErr}<span style="color:var(--bad)">{sizeErr}</span>{:else}<span class="muted">sizing…</span>{/if}
          </div>
          <div class="row small">
            <span class="muted">to</span>
            <select bind:value={host}>
              {#each hosts as h}<option value={h.name}>{h.name} ({human(h.free)} free)</option>{/each}
            </select>
            {#if !fits}<span class="tiny" style="color:var(--bad)">does not fit there</span>{/if}
          </div>
          <div class="tiny muted">what the cluster already has of it is copied there first; hf fetches the rest</div>
        </div>
      {/if}
      <div class="row end">
        <button class="btn ghost" onclick={close}>Cancel</button>
        <button class="btn primary" disabled={!repo || !host || !fits || !!sizeErr} onclick={download}>Download</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .bg { position: fixed; inset: 0; z-index: 85; background: rgba(1, 4, 12, 0.6); display: grid; place-items: center; padding: 16px; backdrop-filter: blur(3px); }
  .dlg { width: min(560px, 100%); padding: 20px; display: flex; flex-direction: column; gap: 12px; background: rgba(8, 16, 34, 0.94); animation: rise 0.2s ease-out; }
  h3 { margin: 0; }
  input[type="search"] { width: 100%; }
  .hits { display: flex; flex-direction: column; gap: 4px; max-height: 40vh; overflow: auto; }
  .hit { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 8px 10px; border-radius: 8px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.55); cursor: pointer; text-align: left; }
  .hit:hover { border-color: var(--line-hi); }
  .hit.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .pick { display: flex; flex-direction: column; gap: 6px; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--line); }
  select { max-width: 100%; }
  .end { justify-content: flex-end; }
</style>
