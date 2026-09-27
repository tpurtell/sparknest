<script lang="ts">
  import type { Snippet } from "svelte";
  import Icon from "./Icon.svelte";
  let { open, onclose, children, wide = false }: { open: boolean; onclose: () => void; children: Snippet; wide?: boolean } = $props();
</script>

<svelte:window onkeydown={(e) => open && e.key === "Escape" && onclose()} />
{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
  <div class="scrim" onclick={onclose}></div>
  <aside class="drawer panel glowline" class:wide>
    <button class="close btn ghost sm" onclick={onclose} aria-label="Close"><Icon name="x" size={16} /></button>
    {@render children()}
  </aside>
{/if}

<style>
  .scrim { position: fixed; inset: 0; background: rgba(1, 4, 12, 0.45); z-index: 60; animation: fade 0.2s; }
  .drawer { position: fixed; top: 10px; right: 10px; bottom: 10px; width: min(680px, calc(100vw - 20px)); z-index: 61; overflow: auto; padding: 22px; background: rgba(6, 13, 30, 0.93); animation: slide 0.28s cubic-bezier(0.2, 0.8, 0.2, 1); }
  .drawer.wide { width: min(980px, calc(100vw - 20px)); }
  .close { position: absolute; top: 12px; right: 12px; }
  @keyframes slide { from { transform: translateX(40px); opacity: 0; } to { transform: none; opacity: 1; } }
  @keyframes fade { from { opacity: 0; } }
  @media (max-width: 899px) { .drawer { top: 0; right: 0; bottom: 0; width: 100vw; border-radius: 0; padding: 16px 14px 90px; } }
</style>
