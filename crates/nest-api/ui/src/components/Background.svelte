<script lang="ts">
  import { onMount } from "svelte";
  import { startBackground } from "../gl/background";
  import { app, reducedMotion } from "../lib/state.svelte";
  let canvas: HTMLCanvasElement;
  // Busier cluster, livelier filaments.
  const energy = () => {
    const total = Object.values(app.rates).reduce((a, r) => a + r.read + r.served, 0);
    return Math.min(1, Math.log10(1 + total / 1e6) / 4);
  };
  onMount(() => startBackground(canvas, energy, reducedMotion()));
</script>

<canvas bind:this={canvas} aria-hidden="true"></canvas>

<style>
  canvas { position: fixed; inset: 0; width: 100vw; height: 100vh; z-index: -1; display: block; }
</style>
