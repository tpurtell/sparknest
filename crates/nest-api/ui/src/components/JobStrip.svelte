<script lang="ts">
  import { app, runningJobs, cancelJob, jobDone, jobTotal } from "../lib/state.svelte";
  import { human, duration, selLabel, selSummary } from "../lib/format";
  import Icon from "./Icon.svelte";
  const jobs = $derived(runningJobs());
</script>

{#if jobs.length}
  <div class="strip">
    {#each jobs as j (j.id)}
      {@const done = jobDone(j)}
      {@const total = jobTotal(j)}
      {@const secs = (Date.now() - j.started_ms) / 1000}
      {@const r = app.jobRates[j.id] ?? 0}
      {@const sel = j.what.replace(/^(replicate|offload) /, "")}
      {@const sum = sel !== j.what ? selSummary(sel, j.summary) : { title: selLabel(j.what), detail: "" }}
      <div class="job panel">
        <Icon name="bolt" size={16} />
        <div class="body">
          <div class="what" title={j.what}>{sum.title}
            <span class="muted tiny">{j.what.split(" ")[0]} → {Object.keys(j.hosts).join(", ") || "starting"}{sum.detail ? ` · ${sum.detail}` : ""}</span></div>
          <div class="bar live"><i style="width:{total ? (100 * done) / total : 0}%"></i></div>
          <div class="tiny muted mono">{total ? `${Math.floor((100 * done) / total)}% · ` : ""}{human(done)} / {human(total)} · {duration(secs * 1000)}{r >= 1 ? ` · ${human(r)}/s` : ""}{r >= 1 && total > done ? ` · ${duration(((total - done) / r) * 1000)} left` : ""}</div>
        </div>
        {#if j.cancelled}<span class="tiny muted">cancelling…</span>
        {:else}<button class="btn sm danger" onclick={() => cancelJob(j.id)}>Cancel</button>{/if}
      </div>
    {/each}
  </div>
{/if}

<style>
  .strip { display: grid; gap: 8px; grid-template-columns: repeat(auto-fill, minmax(min(320px, 100%), 1fr)); }
  .job { min-width: 0; display: flex; gap: 12px; align-items: center; padding: 10px 14px; color: var(--spark); border-color: rgba(56, 232, 255, 0.35); }
  .body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 5px; color: var(--text); }
  .what { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  @media (max-width: 520px) { .strip { grid-template-columns: 1fr; } }
</style>
