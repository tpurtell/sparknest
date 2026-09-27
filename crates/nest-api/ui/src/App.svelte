<script lang="ts">
  import { onMount } from "svelte";
  import { app, route, startPolling, runningJobs, leaderName, go, type View } from "./lib/state.svelte";
  import { human } from "./lib/format";
  import Background from "./components/Background.svelte";
  import Icon from "./components/Icon.svelte";
  import Toasts from "./components/Toasts.svelte";
  import ContextMenu from "./components/ContextMenu.svelte";
  import Dialog from "./components/Dialog.svelte";
  import Login from "./components/Login.svelte";
  import Overview from "./views/Overview.svelte";
  import Models from "./views/Models.svelte";
  import Space from "./views/Space.svelte";
  import Plans from "./views/Plans.svelte";
  import Jobs from "./views/Jobs.svelte";
  import Rules from "./views/Rules.svelte";
  import Files from "./views/Files.svelte";
  import Logs from "./views/Logs.svelte";

  const main: { v: View; label: string; icon: string }[] = [
    { v: "overview", label: "Overview", icon: "overview" },
    { v: "models", label: "Models", icon: "models" },
    { v: "space", label: "Space", icon: "space" },
    { v: "plans", label: "Plans", icon: "plans" },
    { v: "jobs", label: "Jobs", icon: "jobs" },
  ];
  const extra: { v: View; label: string; icon: string }[] = [
    { v: "rules", label: "Rules", icon: "rules" },
    { v: "files", label: "Files", icon: "files" },
    { v: "logs", label: "Logs", icon: "logs" },
  ];
  let moreOpen = $state(false);

  onMount(() => {
    if (app.authed) startPolling();
  });

  const serving = $derived(app.status?.nodes.filter((n) => n.info?.serving).length ?? 0);
  const total = $derived(app.status?.nodes.length ?? 0);
  const running = $derived(runningJobs());
  const flow = $derived(Object.values(app.rates).reduce((a, r) => a + r.read, 0));
</script>

<Background />
{#if !app.authed}
  <Login />
{:else}
  <div class="shell">
    <aside class="side">
      <button class="brand" onclick={() => go("overview")}>
        <span class="bolt"><Icon name="bolt" size={20} /></span>
        <span>sparknest</span>
      </button>
      <nav>
        {#each [...main, ...extra] as n}
          <a href="#/{n.v}" class:on={route.view === n.v}>
            <Icon name={n.icon} />
            <span>{n.label}</span>
            {#if n.v === "jobs" && running.length}<span class="count">{running.length}</span>{/if}
          </a>
        {/each}
      </nav>
      <div class="side-foot tiny faint">v{app.status?.nodes[0]?.info?.version ?? ""}</div>
    </aside>

    <div class="content">
      <header class="top">
        <div class="pills">
          <span class="pill"><span class="dot {serving === total && total ? 'ok' : 'bad'}"></span>{serving}/{total} serving</span>
          {#if leaderName()}<span class="pill hide-s">leader <b>{leaderName()}</b></span>{/if}
          {#if flow > 1e5}<span class="pill live"><Icon name="bolt" size={13} />{human(flow)}/s</span>{/if}
          {#if running.length}
            <a class="pill live" href="#/jobs"><span class="dot spark"></span>{running.length} job{running.length > 1 ? "s" : ""}</a>
          {/if}
          {#if app.error}<span class="pill err">{app.error}</span>{/if}
        </div>
      </header>
      <main>
        {#key route.view}
          <div class="view rise">
            {#if route.view === "overview"}<Overview />
            {:else if route.view === "models"}<Models />
            {:else if route.view === "space"}<Space />
            {:else if route.view === "plans"}<Plans />
            {:else if route.view === "jobs"}<Jobs />
            {:else if route.view === "rules"}<Rules />
            {:else if route.view === "files"}<Files />
            {:else if route.view === "logs"}<Logs />
            {/if}
          </div>
        {/key}
      </main>
    </div>

    <nav class="tabs panel">
      {#each main as n}
        <a href="#/{n.v}" class:on={route.view === n.v}>
          <Icon name={n.icon} size={20} />
          <span>{n.label}</span>
          {#if n.v === "jobs" && running.length}<span class="count">{running.length}</span>{/if}
        </a>
      {/each}
      <button class:on={extra.some((e) => e.v === route.view)} onclick={() => (moreOpen = !moreOpen)}>
        <Icon name="menu" size={20} /><span>More</span>
      </button>
      {#if moreOpen}
        <div class="more panel">
          {#each extra as n}
            <a href="#/{n.v}" onclick={() => (moreOpen = false)}><Icon name={n.icon} /> {n.label}</a>
          {/each}
        </div>
      {/if}
    </nav>
  </div>
{/if}
<Toasts />
<ContextMenu />
<Dialog />

<style>
  .shell { display: flex; min-height: 100vh; }
  .side { position: sticky; top: 0; height: 100vh; width: 212px; flex: none; padding: 18px 12px; display: flex; flex-direction: column; gap: 18px; border-right: 1px solid var(--line); background: linear-gradient(180deg, rgba(4, 10, 24, 0.75), rgba(4, 10, 24, 0.35)); backdrop-filter: blur(10px); }
  .brand { display: flex; align-items: center; gap: 10px; font-size: 19px; font-weight: 700; letter-spacing: 0.03em; background: none; border: 0; cursor: pointer; padding: 4px 8px; }
  .brand span:last-child { background: linear-gradient(90deg, #e9f8ff, var(--spark)); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .bolt { display: grid; place-items: center; width: 32px; height: 32px; border-radius: 10px; color: var(--spark); background: radial-gradient(circle, rgba(56, 232, 255, 0.25), transparent 70%); box-shadow: 0 0 22px rgba(56, 232, 255, 0.35); animation: flick 4s infinite; }
  @keyframes flick { 0%, 92%, 100% { opacity: 1; } 93% { opacity: 0.4; } 95% { opacity: 1; } 96% { opacity: 0.6; } }
  .side nav { display: flex; flex-direction: column; gap: 2px; }
  .side nav a { display: flex; align-items: center; gap: 12px; padding: 9px 12px; border-radius: 10px; color: var(--muted); position: relative; transition: all 0.15s; }
  .side nav a:hover { color: var(--text); background: rgba(56, 232, 255, 0.05); }
  .side nav a.on { color: var(--text); background: linear-gradient(90deg, rgba(56, 232, 255, 0.16), transparent); }
  .side nav a.on::before { content: ""; position: absolute; left: -12px; top: 8px; bottom: 8px; width: 3px; border-radius: 3px; background: var(--spark); box-shadow: 0 0 12px var(--spark); }
  .count { margin-left: auto; font-size: 11px; padding: 0 7px; border-radius: 99px; background: var(--spark); color: #021018; font-weight: 700; }
  .side-foot { margin-top: auto; padding: 0 12px; }
  .content { flex: 1; min-width: 0; display: flex; flex-direction: column; }
  .top { position: sticky; top: 0; z-index: 20; padding: 12px 22px; display: flex; justify-content: flex-end; }
  .pills { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
  .pill { display: inline-flex; align-items: center; gap: 7px; padding: 5px 12px; border-radius: 999px; font-size: 12px; border: 1px solid var(--line); background: rgba(6, 14, 32, 0.7); backdrop-filter: blur(8px); color: var(--muted); }
  .pill b { color: var(--text); font-weight: 600; }
  .pill.live { color: var(--spark); border-color: rgba(56, 232, 255, 0.4); box-shadow: 0 0 16px rgba(56, 232, 255, 0.15); }
  .pill.err { color: var(--bad); border-color: rgba(255, 79, 123, 0.5); }
  main { padding: 4px 22px 40px; flex: 1; min-width: 0; }
  .view { max-width: 1600px; margin: 0 auto; }
  .tabs { display: none; }
  @media (max-width: 899px) {
    .side { display: none; }
    .top { padding: 10px 12px; justify-content: flex-start; }
    .hide-s { display: none; }
    main { padding: 2px 12px 96px; }
    .tabs { display: flex; flex-direction: row; position: fixed; left: 8px; right: 8px; bottom: max(8px, env(safe-area-inset-bottom)); z-index: 50; justify-content: space-around; padding: 6px; border-radius: 18px; background: rgba(6, 13, 30, 0.9); }
    .tabs a, .tabs button { flex: 1; display: flex; flex-direction: column; align-items: center; gap: 2px; font-size: 10px; color: var(--muted); padding: 6px 2px; border-radius: 12px; background: none; border: 0; position: relative; }
    .tabs .on { color: var(--spark); background: rgba(56, 232, 255, 0.1); }
    .tabs .count { position: absolute; top: 2px; right: 18%; }
    .more { position: absolute; right: 6px; bottom: 70px; display: flex; flex-direction: column; padding: 6px; min-width: 160px; background: rgba(6, 13, 30, 0.96); }
    .more a { display: flex; flex-direction: row; gap: 10px; font-size: 14px; padding: 10px 12px; color: var(--text); }
  }
</style>
