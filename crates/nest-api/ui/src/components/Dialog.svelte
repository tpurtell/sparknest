<script lang="ts">
  import { dialog, finish } from "../lib/ui.svelte";
  let chosen = $state<string[]>([]);
  $effect(() => {
    if (dialog.open) chosen = [];
  });
  const toggle = (n: string, multi: boolean) => {
    if (!multi) return finish([n]);
    chosen = chosen.includes(n) ? chosen.filter((x) => x !== n) : [...chosen, n];
  };
</script>

<svelte:window onkeydown={(e) => dialog.open && e.key === "Escape" && finish(null)} />
{#if dialog.open && dialog.d}
  {@const d = dialog.d}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="bg" onclick={(e) => e.target === e.currentTarget && finish(null)}>
    <div class="dlg panel glowline" role="dialog" aria-modal="true" aria-label={d.title}>
      <h3>{d.title}</h3>
      {#if d.body}<p class="muted">{d.body}</p>{/if}
      {#if d.kind === "pick"}
        <div class="opts">
          {#each d.options as o}
            <button class="opt" class:on={chosen.includes(o.name)} onclick={() => toggle(o.name, d.multi)}>
              <span class="k tiny">{o.kind}</span><b>{o.name}</b>
              {#if o.note}<span class="tiny muted">{o.note}</span>{/if}
            </button>
          {/each}
        </div>
      {/if}
      <div class="row end">
        <button class="btn ghost" onclick={() => finish(null)}>Cancel</button>
        {#if d.kind === "confirm"}
          <button class="btn {d.danger ? 'danger' : 'primary'}" onclick={() => finish(true)}>{d.ok}</button>
        {:else if d.multi}
          <button class="btn primary" disabled={!chosen.length} onclick={() => finish(chosen)}>Go</button>
        {/if}
      </div>
    </div>
  </div>
{/if}

<style>
  .bg { position: fixed; inset: 0; z-index: 85; background: rgba(1, 4, 12, 0.6); display: grid; place-items: center; padding: 16px; backdrop-filter: blur(3px); }
  .dlg { width: min(460px, 100%); padding: 20px; animation: rise 0.2s ease-out; background: rgba(8, 16, 34, 0.94); }
  p { margin: 8px 0 0; }
  .opts { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 8px; margin-top: 14px; max-height: 50vh; overflow: auto; }
  .opt { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--line); background: rgba(12, 26, 52, 0.55); cursor: pointer; text-align: left; }
  .opt:hover { border-color: var(--line-hi); }
  .opt.on { border-color: var(--spark); box-shadow: var(--glow); background: rgba(56, 232, 255, 0.1); }
  .k { color: var(--faint); text-transform: uppercase; letter-spacing: 0.1em; }
  .end { justify-content: flex-end; margin-top: 18px; }
</style>
