<script lang="ts">
  import { runningJobs, cancelJob } from "../lib/state.svelte";
  import { human, duration, selLabel } from "../lib/format";
  import Icon from "./Icon.svelte";
  const jobs = $derived(runningJobs());
  const sum = (j: (typeof jobs)[number], k: "done_bytes" | "total_bytes") =>
    Object.values(j.hosts).reduce((a, p) => a + p[k], 0);
</script>

{#if jobs.length}
  <div class="strip">
    {#each jobs as j (j.id)}
      {@const done = sum(j, "done_bytes")}
      {@const total = sum(j, "total_bytes")}
      {@const secs = (Date.now() - j.started_ms) / 1000}
      <div class="job panel">
        <Icon name="bolt" size={16} />
        <div class="body">
          <div class="what">{selLabel(j.what.replace(/^(replicate|offload) /, ""))}
            <span class="muted tiny">{j.what.split(" ")[0]} → {Object.keys(j.hosts).join(", ") || "starting"}</span></div>
          <div class="bar live"><i style="width:{total ? (100 * done) / total : 0}%"></i></div>
          <div class="tiny muted mono">{human(done)} / {human(total)} · {duration(secs * 1000)}{secs > 1 && done ? ` · ${human(done / secs)}/s` : ""}</div>
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
