<script lang="ts">
  import { menu, closeMenu } from "../lib/ui.svelte";
  import Icon from "./Icon.svelte";
  let el = $state<HTMLDivElement>();
  let pos = $state({ x: 0, y: 0 });
  $effect(() => {
    if (!menu.open || !el) return;
    // Keep on screen.
    const r = el.getBoundingClientRect();
    pos = {
      x: Math.max(8, Math.min(menu.x, innerWidth - r.width - 8)),
      y: Math.max(8, Math.min(menu.y, innerHeight - r.height - 8)),
    };
  });
</script>

<svelte:window onkeydown={(e) => e.key === "Escape" && closeMenu()} />
{#if menu.open}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="scrim" onclick={closeMenu} oncontextmenu={(e) => { e.preventDefault(); closeMenu(); }}></div>
  <div class="menu panel glowline" bind:this={el} style="left:{pos.x}px;top:{pos.y}px" role="menu">
    <div class="head">
      <div class="title">{menu.title}</div>
      {#if menu.sub}<div class="tiny muted">{menu.sub}</div>{/if}
    </div>
    {#each menu.items as it}
      {#if it.sep}<div class="sep"></div>
      {:else}
        <button class="item" class:danger={it.danger} disabled={it.disabled} role="menuitem"
          onclick={() => { closeMenu(); it.run?.(); }}>
          <Icon name={it.icon ?? "bolt"} size={16} />
          <span>{it.label}</span>
          {#if it.hint}<span class="hint tiny">{it.hint}</span>{/if}
        </button>
      {/if}
    {/each}
  </div>
{/if}

<style>
  .scrim { position: fixed; inset: 0; z-index: 80; }
  .menu { position: fixed; z-index: 81; min-width: 230px; max-width: min(360px, calc(100vw - 16px)); padding: 6px; animation: rise 0.14s ease-out; background: rgba(8, 16, 34, 0.92); }
  .head { padding: 8px 10px 8px; border-bottom: 1px solid var(--line); margin-bottom: 4px; }
  .title { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .item { display: flex; align-items: center; gap: 10px; width: 100%; padding: 8px 10px; border: 0; background: none; border-radius: 8px; cursor: pointer; text-align: left; color: var(--text); }
  .item:hover:not(:disabled) { background: rgba(56, 232, 255, 0.1); color: var(--spark); }
  .item:disabled { opacity: 0.4; cursor: default; }
  .item.danger { color: #ffb3c5; }
  .item.danger:hover { background: rgba(255, 79, 123, 0.12); color: var(--bad); }
  .hint { margin-left: auto; color: var(--faint); }
  .sep { height: 1px; background: var(--line); margin: 4px 6px; }
</style>
