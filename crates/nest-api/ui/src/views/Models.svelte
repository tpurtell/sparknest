<script module lang="ts">
  // What each repo lacks cluster-wide (hf's dry run against the hub cache),
  // kept across visits; rechecked after ten minutes or a download of it.
  type Missing = { files: number; missing_files: number; missing_bytes: number; at: number };
  const missing = $state<Record<string, Missing>>({});
</script>

<script lang="ts">
  import { get, type Repo, type Readiness } from "../lib/api";
  import { app, route, go, targets, runningJobs, startJob, singleFlight } from "../lib/state.svelte";
  import { human, ago, selLabel, splitRepo } from "../lib/format";
  import { removeFrom } from "../lib/actions";
  import JobStrip from "../components/JobStrip.svelte";
  import Drawer from "../components/Drawer.svelte";
  import ModelDetail from "./ModelDetail.svelte";
  import Icon from "../components/Icon.svelte";
  import DownloadDialog from "../components/DownloadDialog.svelte";
  import { pick, openPlace } from "../lib/ui.svelte";

  let downloading = $state(false);

  const key = (r: Repo) => `${r.kind}:${r.repo}`;
  const lacking = (r: Repo) => missing[key(r)]?.missing_files ?? 0;
  const checkMissing = singleFlight(async () => {
    const stale = repos.filter((r) => !r.error && !r.writing && !(Date.now() - (missing[key(r)]?.at ?? 0) < 600_000));
    const next = async () => {
      for (let r = stale.shift(); r; r = stale.shift()) {
        try {
          const s = await get<Omit<Missing, "at">>(`/v1/hf/size?repo=${encodeURIComponent(r.repo)}&kind=${r.kind}`);
          missing[key(r)] = { ...s, at: Date.now() };
        } catch {
          // Unknown to the Hub (or offline): nothing to finish.
          missing[key(r)] = { files: 0, missing_files: 0, missing_bytes: 0, at: Date.now() };
        }
      }
    };
    await Promise.all([next(), next(), next()]);
  });
  // A download of a repo that finishes changes what it lacks.
  let downloadsBefore = new Set<string>();
  $effect(() => {
    const now = new Set(runningJobs().map((j) => /^hf download (\S+)/.exec(j.what)?.[1]).filter((x): x is string => !!x));
    const done = [...downloadsBefore].filter((x) => !now.has(x));
    downloadsBefore = now;
    if (!done.length) return;
    for (const k of Object.keys(missing)) if (done.includes(k.slice(k.indexOf(":") + 1))) delete missing[k];
    checkMissing();
  });

  // Finishing downloads only what no host has, with hf, on the host chosen;
  // nothing the cluster holds moves.
  async function finishOn(r: Repo) {
    const m = missing[key(r)];
    const held = (h: Readiness) => h.bytes - h.missing_bytes;
    const free = (h: string) => app.status?.nodes.find((n) => n.name === h)?.info?.free_bytes ?? 0;
    const ALL = "every host";
    const opts = [
      { name: ALL, kind: "spread", note: "each file on one host, evenly" },
      ...(r.hosts ?? [])
        .filter((h) => !app.stores.some((s) => s.name === h.host))
        .sort((a, b) => free(b.host) - free(a.host))
        .map((h) => ({ name: h.host, kind: "host", note: `${human(free(h.host))} free${held(h) ? ` · holds ${human(held(h))}` : ""}` })),
    ];
    const got = await pick(
      `Finish ${r.repo} on…`,
      opts,
      false,
      `hf downloads the ${m?.missing_files ?? ""} files (${human(m?.missing_bytes ?? 0)}) no host has, spread over every host or onto the one you pick. Nothing already in the cluster moves.`,
    );
    if (!got?.[0]) return;
    if (got[0] === ALL) await startJob("/v1/hf/download", { repo: r.repo, kind: r.kind, hosts: [] }, `Finishing ${r.repo} over every host`);
    else await startJob("/v1/hf/download", { repo: r.repo, kind: r.kind, host: got[0] }, `Finishing ${r.repo} on ${got[0]}`);
  }

  let repos = $state<Repo[]>([]);
  let hub = $state("");
  let err = $state("");
  let q = $state("");
  let kind = $state<"all" | "model" | "dataset">("all");
  let sort = $state<"size" | "name" | "used">("size");

  async function load() {
    try {
      const v = await get<{ hub: string; repos: Repo[] }>("/v1/hf");
      repos = v.repos;
      hub = v.hub;
      err = "";
      checkMissing();
    } catch (e) {
      err = (e as Error).message;
    }
  }
  const refresh = singleFlight(load);
  $effect(() => {
    void app.changed;
    refresh();
  });
  // Usage (who opened what) moves without metadata changes.
  $effect(() => {
    const t = setInterval(refresh, 15_000);
    return () => clearInterval(t);
  });

  const cols = $derived(targets());
  // Copies in flight: selector + target.
  const inflight = $derived.by(() => {
    const m = new Map<string, number>();
    for (const j of runningJobs()) {
      const sel = /^(?:replicate|offload) (\S+)/.exec(j.what)?.[1];
      if (!sel) continue;
      for (const [h, p] of Object.entries(j.hosts)) if (!p.finished) m.set(sel + " " + h, p.total_bytes ? p.done_bytes / p.total_bytes : 0);
    }
    return m;
  });

  const shown = $derived.by(() => {
    const needle = q.trim().toLowerCase();
    const list = repos.filter((r) => (kind === "all" || r.kind === kind) && (!needle || r.repo.toLowerCase().includes(needle)));
    return list.sort((a, b) =>
      sort === "name" ? a.repo.localeCompare(b.repo) : sort === "used" ? (b.last_open_ms ?? 0) - (a.last_open_ms ?? 0) : b.bytes - a.bytes,
    );
  });

  // % of bytes present; never 100 unless complete.
  function pctOf(h: Readiness) {
    if (h.ready) return 100;
    const p = h.bytes ? (100 * (h.bytes - h.missing_bytes)) / h.bytes : 0;
    return Math.min(99, Math.floor(p));
  }

  async function cell(r: Repo, h: Readiness) {
    if (inflight.has(r.selector + " " + h.host)) return;
    // Complete: offer to remove (asks first). Missing or partial: copy now.
    if (h.ready) return removeFrom(r.selector, [h.host]);
    await startJob("/v1/replicate", { selector: r.selector, hosts: [h.host] }, `Copying ${r.repo} to ${h.host}`);
  }

  const detail = $derived(route.view === "models" && route.arg ? route.arg : "");
  const cellW = $derived(Math.max(20, Math.min(46, Math.floor(720 / Math.max(1, cols.length)))));
</script>

<DownloadDialog bind:open={downloading} />
<div class="stack">
  <div class="panel pad head">
    <div class="row">
      <h2>Models</h2>
      <span class="small muted">{shown.length} repo{shown.length === 1 ? "" : "s"} in {hub}</span>
      <span class="spacer"></span>
      <input type="search" placeholder="filter…" bind:value={q} style="width:200px" />
      <button class="btn primary" onclick={() => (downloading = true)}><Icon name="download" size={15} /> Download…</button>
    </div>
    <div class="row">
      <div class="chips">
        {#each [["all", "All"], ["model", "Models"], ["dataset", "Datasets"]] as [k, l]}
          <button class="chip" class:on={kind === k} onclick={() => (kind = k as typeof kind)}>{l}</button>
        {/each}
      </div>
      <div class="chips">
        {#each [["size", "Largest"], ["used", "Recently used"], ["name", "Name"]] as [k, l]}
          <button class="chip" class:on={sort === k} onclick={() => (sort = k as typeof sort)}>{l}</button>
        {/each}
      </div>
      <span class="spacer"></span>
      <span class="tiny muted legend"><i class="lg full"></i>complete <i class="lg part"></i>partial <i class="lg none"></i>absent · click a cell to copy there, a complete one to remove</span>
    </div>
  </div>

  <JobStrip />
  {#if err}<div class="panel pad" style="color:var(--bad)">{err}</div>{/if}

  {#if shown.length}
    <div class="colhead" style="--cw:{cellW}px">
      <div></div>
      <div class="cells">
        {#each cols as c}<div class="ch" title={c.name}><span>{c.kind === "store" ? "⧉ " : ""}{c.name}</span></div>{/each}
      </div>
    </div>
  {/if}

  <div class="list" style="--cw:{cellW}px">
    {#each shown as r (r.selector)}
      {@const byHost = Object.fromEntries(r.hosts?.map((h) => [h.host, h]) ?? [])}
      {@const lastHost = Object.entries(r.usage ?? {}).sort((a, b) => b[1].last_open_ms - a[1].last_open_ms)[0]}
      <div class="repo panel">
        <div class="namebox">
        <button class="name" onclick={() => go("models", r.selector)}>
          <div class="title">
            <span class="org">{splitRepo(r.repo)[0]}</span><b>{splitRepo(r.repo)[1]}</b>
            {#if r.kind === "dataset"}<span class="badge">dataset</span>{/if}
            {#if r.writing}<span class="badge warn">{r.writing} writing</span>{/if}
          </div>
          <div class="meta tiny muted mono">
            {human(r.bytes)} · {r.files} files{r.revisions > 1 ? ` · ${r.revisions} revisions` : ""}
            · {r.last_open_ms ? `used ${ago(r.last_open_ms)}${lastHost ? " on " + lastHost[0] : ""}` : "unused in 30 days"}
          </div>
        </button>
        {#if !r.error && !r.writing && lacking(r)}
          <button class="btn sm finish" onclick={() => finishOn(r)} title="{lacking(r)} files ({human(missing[key(r)].missing_bytes)}) were never downloaded: fetch them with hf on a host"><Icon name="download" size={13} /> Finish</button>
        {/if}
        </div>
        {#if r.error}
          <div class="muted small">{r.error}</div>
        {:else}
          <div class="cells">
            {#each cols as c}
              {@const h = byHost[c.name]}
              {#if h}
                {@const p = pctOf(h)}
                {@const fly = inflight.get(r.selector + " " + c.name)}
                {@const u = r.usage?.[c.name]}
                {@const shownP = fly !== undefined ? Math.max(p, Math.min(99, Math.floor(fly * 100))) : p}
                <button class="cell" class:full={h.ready} class:part={!h.ready && p > 0} class:fly={fly !== undefined}
                  style="--p:{shownP}%"
                  title="{c.name}: {h.ready ? 'complete' : `${p}% of bytes (${h.files - h.missing_files}/${h.files} files; ${human(h.missing_bytes)} missing)`}{u?.last_open_ms ? ` · opened ${ago(u.last_open_ms)}` : ''}{u?.net_bytes ? ` · ${human(u.net_bytes)} read over the network` : ''}"
                  onclick={() => cell(r, h)}>
                  <i></i>
                  <span class="lbl">{h.ready ? "✓" : shownP > 0 ? shownP : ""}</span>
                  <span class="mob tiny">{c.name.slice(0, 3)}</span>
                </button>
              {:else}
                <span class="cell void"></span>
              {/if}
            {/each}
          </div>
          <div class="acts">
            <button class="btn sm" onclick={() => openPlace(r.selector, r.repo)} title="Place: spread or replicate over hosts, gather on one, or archive"><Icon name="target" size={14} /> Place…</button>
            <button class="btn sm ghost" onclick={() => go("models", r.selector)} title="Details"><Icon name="info" size={14} /></button>
          </div>
        {/if}
      </div>
    {:else}
      {#if !err}<div class="panel empty">No Hugging Face repos yet. Point <code>HF_HOME</code> at <code>{hub.replace(/\/hub$/, "")}</code> under the mount, or <code>nest hf import</code> an existing cache.</div>{/if}
    {/each}
  </div>
</div>

<Drawer open={!!detail} onclose={() => go("models")} wide>
  {#if detail}<ModelDetail selector={detail} />{/if}
</Drawer>

<style>
  .head { display: flex; flex-direction: column; gap: 10px; }
  .legend { display: inline-flex; align-items: center; gap: 4px; }
  .lg { display: inline-block; width: 10px; height: 10px; border-radius: 3px; margin-left: 6px; border: 1px solid var(--line); }
  .lg.full { background: rgba(56, 232, 255, 0.55); box-shadow: 0 0 6px var(--spark); }
  .lg.part { background: linear-gradient(0deg, rgba(79, 141, 255, 0.7) 50%, transparent 50%); }
  .list { display: flex; flex-direction: column; gap: 8px; }
  .colhead, .repo { display: grid; grid-template-columns: minmax(220px, 1fr) auto 164px; gap: 14px; align-items: center; }
  .colhead { padding: 0 14px; grid-template-columns: minmax(220px, 1fr) auto 164px; }
  .colhead::after { content: ""; }
  .cells { display: flex; gap: 4px; }
  .ch { width: var(--cw); height: 64px; position: relative; }
  .ch span { position: absolute; left: 50%; bottom: 2px; transform-origin: left bottom; transform: rotate(-50deg); white-space: nowrap; font-size: 11px; color: var(--muted); }
  .repo { padding: 10px 14px; transition: border-color 0.15s; }
  .namebox { display: flex; align-items: center; gap: 8px; min-width: 0; }
  .namebox .name { min-width: 0; flex: 1; }
  .finish { flex: none; color: var(--warn, #ffd36e); border-color: rgba(255, 211, 110, 0.4); }
  .repo:hover { border-color: rgba(110, 220, 255, 0.3); }
  .name { background: none; border: 0; text-align: left; cursor: pointer; min-width: 0; padding: 0; }
  .title { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 14px; }
  .name:hover b { color: var(--spark); }
  .org { color: var(--muted); }
  .meta { margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cell { width: var(--cw); height: 30px; border-radius: 7px; border: 1px solid rgba(90, 170, 255, 0.18); background: rgba(8, 18, 38, 0.8); position: relative; overflow: hidden; cursor: pointer; padding: 0; transition: transform 0.1s, border-color 0.15s; }
  .cell:hover { border-color: var(--spark); transform: translateY(-1px); }
  .cell i { position: absolute; left: 0; right: 0; bottom: 0; height: var(--p); background: linear-gradient(0deg, rgba(79, 141, 255, 0.75), rgba(56, 232, 255, 0.55)); transition: height 0.6s ease; }
  .cell.full { border-color: rgba(120, 240, 255, 0.9); box-shadow: 0 0 14px rgba(56, 232, 255, 0.55), inset 0 0 12px rgba(160, 250, 255, 0.35); }
  .cell.full i { background: linear-gradient(0deg, #1f8bff, #38e8ff 70%, #b8f7ff); }
  .cell.full .lbl { color: #021018; text-shadow: none; font-weight: 700; }
  .cell.fly { border-color: var(--spark); }
  .cell.fly i { animation: pulse 1s ease-in-out infinite; }
  .cell.fly::after { content: ""; position: absolute; inset: 0; background: linear-gradient(0deg, transparent, rgba(255, 255, 255, 0.25), transparent); animation: rise-sweep 1.2s linear infinite; }
  @keyframes rise-sweep { from { transform: translateY(100%); } to { transform: translateY(-100%); } }
  .cell.void { cursor: default; border-style: dashed; opacity: 0.3; }
  .lbl { position: relative; font-size: 11px; font-family: var(--mono); color: #eafaff; text-shadow: 0 0 6px rgba(0, 0, 0, 0.9); }
  .mob { display: none; }
  .acts { display: flex; gap: 4px; justify-content: flex-end; }
  code { color: var(--spark); }
  @media (max-width: 1100px) {
    .colhead { display: none; }
    .repo { grid-template-columns: 1fr auto; }
    .repo .cells { grid-column: 1 / -1; grid-row: 2; flex-wrap: wrap; }
    .acts { grid-row: 1; grid-column: 2; }
    .cell { height: 38px; display: flex; flex-direction: column; align-items: center; justify-content: center; }
    .mob { display: block; position: relative; color: var(--muted); font-size: 9px; }
  }
</style>
