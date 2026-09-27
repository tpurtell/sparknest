<script lang="ts">
  import type { Snippet } from "svelte";
  import Icon from "./Icon.svelte";
  import { portal } from "../lib/portal";
  let { open, onclose, children, wide = false }: { open: boolean; onclose: () => void; children: Snippet; wide?: boolean } = $props();
</script>

<svelte:window onkeydown={(e) => open && e.key === "Escape" && onclose()} />
{#if open}
  <div class="layer" use:portal>
    <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
    <div class="scrim" onclick={onclose}></div>
    <aside class="drawer panel glowline" class:wide>
      <button class="close btn ghost sm" onclick={onclose} aria-label="Close"><Icon name="x" size={16} /></button>
      {@render children()}
    </aside>
  </div>
{/if}

<style>
  /* A window floating in the middle of the screen, never taller than it:
     its content scrolls inside. */
  .layer { position: fixed; inset: 0; z-index: 60; display: grid; place-items: center; padding: 16px; }
  .scrim { position: absolute; inset: 0; background: rgba(1, 4, 12, 0.45); animation: fade 0.2s; }
  .drawer { position: relative; width: min(680px, 100%); max-height: 100%; overflow: auto; padding: 22px; background: rgba(6, 13, 30, 0.8); animation: pop 0.24s cubic-bezier(0.2, 0.8, 0.2, 1); }
  .drawer.wide { width: min(1040px, 100%); }
  .close { position: absolute; top: 12px; right: 12px; z-index: 2; }
  @keyframes pop { from { transform: scale(0.97) translateY(10px); opacity: 0; } to { transform: none; opacity: 1; } }
  @keyframes fade { from { opacity: 0; } }
  @media (max-width: 899px) { .layer { padding: 0; } .drawer { width: 100vw; height: 100%; border-radius: 0; padding: 16px 14px 40px; } .drawer.wide { width: 100vw; } }
</style>
