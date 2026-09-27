<script lang="ts">
  import { setToken } from "../lib/api";
  import { app, startPolling } from "../lib/state.svelte";
  let tok = $state("");
  const go = () => {
    setToken(tok);
    app.authed = true;
    startPolling();
  };
</script>

<div class="wrap">
  <div class="panel glowline pad card rise">
    <div class="logo">⚡ sparknest</div>
    <p class="muted">Run <code>nest ui</code> on any node for a link that signs you in, or paste its token.</p>
    <form class="row" onsubmit={(e) => { e.preventDefault(); go(); }}>
      <input bind:value={tok} placeholder="token" style="flex:1" autocomplete="off" />
      <button class="btn primary" type="submit" disabled={!tok.trim()}>Connect</button>
    </form>
  </div>
</div>

<style>
  .wrap { min-height: 100vh; display: grid; place-items: center; padding: 16px; }
  .card { width: min(440px, 100%); }
  .logo { font-size: 26px; font-weight: 700; letter-spacing: 0.04em; background: linear-gradient(90deg, var(--spark), #9cc4ff); -webkit-background-clip: text; background-clip: text; color: transparent; margin-bottom: 8px; }
  code { color: var(--spark); }
</style>
