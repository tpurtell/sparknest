<script lang="ts">
  import { uploads, clearFinished } from "../lib/files.svelte";
  import { human } from "../lib/format";
  const busy = $derived(uploads.filter((u) => u.state === "sending" || u.state === "queued"));
  const total = $derived(uploads.reduce((a, u) => a + u.size, 0));
  const sent = $derived(uploads.reduce((a, u) => a + u.sent, 0));
  let open = $state(true);
</script>

{#if uploads.length}
  <div class="tray panel glowline">
    <button class="head row" onclick={() => (open = !open)}>
      <b>{busy.length ? `Uploading ${busy.length}` : "Uploads"}</b>
      <span class="spacer"></span>
      <span class="mono tiny muted">{human(sent)} / {human(total)}</span>
    </button>
    <div class="bar" class:live={busy.length > 0}><i style="width:{total ? (100 * sent) / total : 100}%"></i></div>
    {#if open}
      <div class="list">
        {#each uploads.slice(-50) as u (u.id)}
          <div class="u">
            <span class="n" title={u.name}>{u.name}</span>
            {#if u.state === "sending"}
              <span class="mono tiny">{Math.floor((100 * u.sent) / Math.max(1, u.size))}%</span>
              <button class="btn sm ghost" onclick={() => u.xhr?.abort()}>✕</button>
            {:else if u.state === "failed"}<span class="tiny bad" title={u.error}>{u.error}</span>
            {:else if u.state === "done"}<span class="tiny ok">✓</span>
            {:else}<span class="tiny muted">queued</span>{/if}
          </div>
        {/each}
      </div>
      {#if !busy.length}<button class="btn sm ghost" onclick={clearFinished}>Clear</button>{/if}
    {/if}
  </div>
{/if}

<style>
  .tray { position: fixed; left: 230px; bottom: 18px; z-index: 70; width: min(380px, calc(100vw - 24px)); padding: 10px 12px; display: flex; flex-direction: column; gap: 8px; background: rgba(6, 13, 30, 0.94); }
  .head { background: none; border: 0; padding: 0; cursor: pointer; }
  .list { max-height: 220px; overflow: auto; display: flex; flex-direction: column; gap: 4px; }
  .u { display: flex; gap: 8px; align-items: center; font-size: 12px; }
  .n { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ok { color: var(--ok); }
  .bad { color: var(--bad); max-width: 140px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  @media (max-width: 899px) { .tray { left: 12px; bottom: 12px; } }
</style>
