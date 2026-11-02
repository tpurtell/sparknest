<script lang="ts">
  import { app, cancelJob, go, jobDone, jobTotal } from "../lib/state.svelte";
  import { human, duration, selLabel, selSummary } from "../lib/format";
  import { showPaths } from "../lib/ui.svelte";
  import type { Job } from "../lib/api";
  import Icon from "../components/Icon.svelte";

  /** "replicate <selector>": the verb and the selection, summarized. */
  function label(j: Job) {
    const m = /^(replicate|offload) (.*)$/.exec(j.what);
    if (!m) return { verb: "", title: selLabel(j.what), detail: "", sel: "" };
    return { verb: m[1], sel: m[2], ...selSummary(m[2], j.summary) };
  }
  const details = (j: Job, sel: string) =>
    showPaths(j.what.split(" ")[0] + " · job " + j.id, `${sel}${j.summary?.models?.length ? `\nmodels: ${j.summary.models.join(", ")}` : ""}`, j.summary?.paths ?? []);
  const jobs = $derived(app.jobs.slice().sort((a, b) => b.started_ms - a.started_ms || b.id - a.id));
</script>

<div class="stack">
  <div class="row"><h2>Jobs</h2><span class="small muted">on every host since its daemon started</span></div>
  {#each jobs as j (j.id)}
    {@const done = jobDone(j)}
    {@const total = jobTotal(j)}
    {@const secs = ((j.finished_ms ?? Date.now()) - j.started_ms) / 1000}
    {@const l = label(j)}
    <div class="job panel" class:live={!j.finished}>
      <div class="row">
        <span class="state" class:run={!j.finished} class:bad={j.finished && j.error && !j.cancelled}>
          {!j.finished ? (j.cancelled ? "cancelling" : "running") : j.cancelled ? "cancelled" : j.error ? "failed" : "done"}
        </span>
        {#if l.verb}<span class="tiny muted">{l.verb}</span>{/if}
        <b class="what" title={j.what}>{l.title}</b>
        {#if l.detail}<span class="tiny muted">{l.detail}</span>{/if}
        {#if j.node}<span class="tiny muted">on {j.node}</span>{/if}
        {#if l.sel}<button class="btn sm ghost" onclick={() => details(j, l.sel)}>Details</button>{/if}
        <span class="spacer"></span>
        {#if j.started_ms}<span class="tiny muted mono">{new Date(j.started_ms).toLocaleTimeString()} · {duration(secs * 1000)}{j.finished ? (done && secs > 0.5 ? ` · ${human(done / secs)}/s` : "") : (app.jobRates[j.id] ?? 0) >= 1 ? ` · ${human(app.jobRates[j.id])}/s` : ""}</span>{/if}
        {#if !j.finished && !j.cancelled}<button class="btn sm danger" onclick={() => cancelJob(j.id)}><Icon name="stop" size={13} /> Cancel</button>{/if}
      </div>
      {#if j.error && !j.cancelled}<div class="small" style="color:var(--bad)">{j.error}</div>{/if}
      <div class="hosts">
        {#each Object.entries(j.hosts) as [h, p]}
          <div class="h">
            <div class="row small"><b>{h}</b><span class="spacer"></span><span class="mono tiny muted">{p.done_files}/{p.total_files} · {human(p.done_bytes + (p.partial_bytes ?? 0))} / {human(p.total_bytes)}</span></div>
            <div class="bar" class:live={!p.finished}><i style="width:{p.total_bytes ? (100 * (p.done_bytes + (p.partial_bytes ?? 0))) / p.total_bytes : p.finished ? 100 : 0}%"></i></div>
            {#if p.failed.length}
              <details><summary class="tiny" style="color:var(--bad)">{p.failed.length} not done</summary>
                {#each p.failed as [, why]}
                  <div class="tiny fail">{why} <button class="btn sm ghost" onclick={() => go("logs", "", { q: why.split(": ")[0], host: h })}>logs</button></div>
                {/each}
              </details>
            {/if}
          </div>
        {/each}
      </div>
      {#if j.notes?.length}
        <div class="notes">{#each j.notes as n}<div class="small" class:bad={/: failed/.test(n)}>{n}</div>{/each}</div>
      {/if}
      {#if total}<div class="tiny muted mono">{human(done)} of {human(total)}</div>{/if}
    </div>
  {:else}
    <div class="panel empty">No jobs yet.</div>
  {/each}
</div>

<style>
  .job { padding: 12px 14px; display: flex; flex-direction: column; gap: 10px; }
  .job.live { border-color: rgba(56, 232, 255, 0.35); }
  .what { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
  .state { font-size: 11px; text-transform: uppercase; letter-spacing: 0.1em; padding: 2px 8px; border-radius: 99px; border: 1px solid var(--line); color: var(--muted); }
  .state.run { color: var(--spark); border-color: rgba(56, 232, 255, 0.5); animation: pulse 1.6s infinite; }
  .state.bad { color: var(--bad); border-color: rgba(255, 79, 123, 0.5); }
  .hosts { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 10px; }
  .h { display: flex; flex-direction: column; gap: 5px; }
  .fail { color: #ffb3c5; word-break: break-all; }
  .notes { display: flex; flex-direction: column; gap: 2px; padding: 6px 10px; border-left: 2px solid var(--line-hi); }
  .notes .bad { color: #ffb3c5; }
</style>
