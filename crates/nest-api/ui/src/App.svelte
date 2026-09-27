<script lang="ts">
  import { onMount } from "svelte";
  import { app, route, startLive, runningJobs, leaderName, go, type View } from "./lib/state.svelte";
  import { human } from "./lib/format";
  import Background from "./components/Background.svelte";
  import Icon from "./components/Icon.svelte";
  import Toasts from "./components/Toasts.svelte";
  import ContextMenu from "./components/ContextMenu.svelte";
  import Dialog from "./components/Dialog.svelte";
  import PlaceDialog from "./components/PlaceDialog.svelte";
  import UploadTray from "./components/UploadTray.svelte";
  import Login from "./components/Login.svelte";
  import Overview from "./views/Overview.svelte";
  import Models from "./views/Models.svelte";
  import Space from "./views/Space.svelte";
  import Plans from "./views/Plans.svelte";
  import Jobs from "./views/Jobs.svelte";
  import Rules from "./views/Rules.svelte";
  import Files from "./views/Files.svelte";
  import Logs from "./views/Logs.svelte";
  import Io from "./views/Io.svelte";

  const main: { v: View; label: string; icon: string }[] = [
    { v: "overview", label: "Overview", icon: "overview" },
    { v: "models", label: "Models", icon: "models" },
    { v: "space", label: "Space", icon: "space" },
    { v: "plans", label: "Plans", icon: "plans" },
    { v: "io", label: "I/O", icon: "io" },
    { v: "jobs", label: "Jobs", icon: "jobs" },
  ];
  const extra: { v: View; label: string; icon: string }[] = [
    { v: "rules", label: "Rules", icon: "rules" },
    { v: "files", label: "Files", icon: "files" },
    { v: "logs", label: "Logs", icon: "logs" },
  ];
  let moreOpen = $state(false);
  // Any navigation closes the menu.
  $effect(() => {
    void route.view;
    void route.arg;
    moreOpen = false;
  });

  onMount(() => {
    if (app.authed) startLive();
  });

  const serving = $derived(app.status?.nodes.filter((n) => n.info?.serving).length ?? 0);
  const total = $derived(app.status?.nodes.length ?? 0);
  const running = $derived(runningJobs());
  const flow = $derived(Object.values(app.rates).reduce((a, r) => a + r.read, 0));
</script>

<!-- Nothing in the app is dragged natively: a click-drag on a nav link
     started the browser's link drag, and releasing it on the same link could
     leave the page swallowing input. (Only drags starting in the page are
     stopped; the app takes no drops.) -->
<svelte:window ondragstart={(e) => e.preventDefault()} />
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
          <a href="#/{n.v}" class:on={route.view === n.v} draggable="false">
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
        <button class="burger btn ghost" onclick={() => (moreOpen = !moreOpen)} aria-label="Menu" aria-expanded={moreOpen}>
          <Icon name={moreOpen ? "x" : "menu"} size={20} />
        </button>
        <button class="brand mbrand" onclick={() => { moreOpen = false; go("overview"); }}>
          <span class="bolt"><Icon name="bolt" size={16} /></span><span>sparknest</span>
        </button>
        <div class="pills">
          <span class="pill"><span class="dot {serving === total && total ? 'ok' : 'bad'}"></span>{serving}/{total} serving</span>
          {#if leaderName()}<span class="pill hide-s">leader <b>{leaderName()}</b></span>{/if}
          {#if flow > 1e5}<span class="pill live hide-s"><Icon name="bolt" size={13} />{human(flow)}/s</span>{/if}
          {#if running.length}
            <a class="pill live" href="#/jobs"><span class="dot spark"></span>{running.length} job{running.length > 1 ? "s" : ""}</a>
          {/if}
          {#if app.slow}<span class="pill err" title="A request to this node has been waiting that long">API slow · {app.slow}s</span>{/if}
          {#if app.error}<span class="pill err hide-s">{app.error}</span>{/if}
        </div>
        {#if moreOpen}
          <!-- svelte-ignore a11y_no_static_element_interactions, a11y_click_events_have_key_events -->
          <div class="mscrim" onclick={() => (moreOpen = false)}></div>
          <nav class="mnav">
            {#each [...main, ...extra] as n}
              <a href="#/{n.v}" class:on={route.view === n.v} onclick={() => (moreOpen = false)}>
                <Icon name={n.icon} /> <span>{n.label}</span>
                {#if n.v === "jobs" && running.length}<span class="count">{running.length}</span>{/if}
              </a>
            {/each}
          </nav>
        {/if}
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
            {:else if route.view === "io"}<Io />
            {/if}
          </div>
        {/key}
      </main>
    </div>

  </div>
{/if}
<Toasts />
<UploadTray />
<ContextMenu />
<PlaceDialog />
<Dialog />

<style>
  .shell { display: flex; min-height: 100vh; }
  .side { position: sticky; top: 0; height: 100vh; width: 212px; flex: none; padding: 18px 12px; display: flex; flex-direction: column; gap: 18px; border-right: 1px solid var(--line); background: linear-gradient(180deg, rgba(4, 10, 24, 0.75), rgba(4, 10, 24, 0.35)); backdrop-filter: blur(10px); }
  .brand { display: flex; align-items: center; gap: 10px; font-size: 19px; font-weight: 700; letter-spacing: 0.03em; background: none; border: 0; cursor: pointer; padding: 4px 8px; }
  .brand span:last-child { background: linear-gradient(90deg, #e9f8ff, var(--spark)); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .bolt { display: grid; place-items: center; width: 32px; height: 32px; border-radius: 10px; color: var(--spark); background: radial-gradient(circle, rgba(56, 232, 255, 0.25), transparent 70%); box-shadow: 0 0 22px rgba(56, 232, 255, 0.35); animation: flick 4s infinite; }
  @keyframes flick { 0%, 92%, 100% { opacity: 1; } 93% { opacity: 0.4; } 95% { opacity: 1; } 96% { opacity: 0.6; } }
  .side nav { display: flex; flex-direction: column; gap: 2px; }
  .side nav a, .mnav a { user-select: none; -webkit-user-drag: none; }
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
  .burger, .mbrand, .mnav, .mscrim { display: none; }
  @media (max-width: 899px) {
    .side { display: none; }
    .top { padding: 8px 10px; justify-content: flex-start; align-items: center; gap: 6px; background: rgba(3, 8, 20, 0.82); backdrop-filter: blur(12px); border-bottom: 1px solid var(--line); }
    .burger { display: inline-flex; padding: 6px 8px; }
    .mbrand { display: flex; font-size: 16px; padding: 2px 4px; gap: 8px; }
    .mbrand .bolt { width: 26px; height: 26px; }
    .pills { margin-left: auto; flex-wrap: nowrap; }
    .pill { padding: 4px 9px; }
    .hide-s { display: none; }
    main { padding: 10px 12px 28px; }
    .mscrim { display: block; position: fixed; inset: 0; top: 52px; z-index: 29; background: rgba(1, 4, 12, 0.5); }
    .mnav { display: grid; grid-template-columns: 1fr 1fr; gap: 4px; position: absolute; top: calc(100% + 6px); left: 8px; right: 8px; z-index: 30; padding: 8px; background: #071022; border: 1px solid var(--line-hi); border-radius: var(--radius); box-shadow: 0 18px 50px rgba(0, 0, 0, 0.6), 0 0 30px rgba(56, 232, 255, 0.12); animation: rise 0.18s ease-out; }
    .mnav a { display: flex; align-items: center; gap: 10px; padding: 11px 12px; border-radius: 10px; color: var(--muted); position: relative; }
    .mnav a.on { color: var(--spark); background: rgba(56, 232, 255, 0.1); }
  }
</style>
