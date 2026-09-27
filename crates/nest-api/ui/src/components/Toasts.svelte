<script lang="ts">
  import { toasts } from "../lib/state.svelte";
</script>

<div class="toasts" aria-live="polite">
  {#each toasts as t (t.id)}
    <div class="toast panel" class:err={t.err}>{t.msg}</div>
  {/each}
</div>

<style>
  .toasts { position: fixed; right: 18px; bottom: 18px; z-index: 90; display: flex; flex-direction: column; gap: 8px; max-width: min(420px, calc(100vw - 36px)); }
  .toast { padding: 11px 15px; border-color: rgba(56, 232, 255, 0.45); box-shadow: 0 0 26px rgba(56, 232, 255, 0.25); animation: rise 0.25s ease-out; }
  .toast.err { border-color: rgba(255, 79, 123, 0.6); box-shadow: 0 0 26px rgba(255, 79, 123, 0.25); }
  @media (max-width: 899px) { .toasts { bottom: 12px; right: 12px; left: 12px; max-width: none; } }
</style>
